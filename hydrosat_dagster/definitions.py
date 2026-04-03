import json
import os
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

from dagster import Definitions, Failure, RunFailureSensorContext, job, op, run_failure_sensor


def build_failure_message(job_name: str, run_id: str, failure_message: str) -> str:
    """Format a consistent alert body for Alertmanager-routed job failures."""
    return (
        f"Dagster job failure detected.\n"
        f"job_name={job_name}\n"
        f"run_id={run_id}\n"
        f"failed_at_utc={datetime.now(timezone.utc).isoformat()}\n"
        f"message={failure_message}"
    )


def build_alertmanager_payload(job_name: str, run_id: str, failure_message: str) -> list[dict]:
    """Build the minimal Alertmanager v2 payload for a failed Dagster run."""
    failed_at = datetime.now(timezone.utc).isoformat()
    return [
        {
            "labels": {
                "alertname": "DagsterJobFailed",
                "severity": "critical",
                "service": "dagster",
                "job_name": job_name,
                "run_id": run_id,
            },
            "annotations": {
                "summary": f"Dagster job failed: {job_name}",
                "description": failure_message,
                "message": build_failure_message(job_name, run_id, failure_message),
            },
            "startsAt": failed_at,
        }
    ]


@op(config_schema={"should_fail": bool})
def extract_satellite_window(context):
    """Create a small synthetic payload so the demo job is easy to validate end to end."""
    payload = {
        "batch_id": str(uuid4()),
        "window_start": datetime.now(timezone.utc).isoformat(),
        "should_fail": context.op_config["should_fail"],
    }
    context.log.info("Prepared dummy ingest payload: %s", json.dumps(payload))
    return payload


@op
def load_into_stub_warehouse(context, payload):
    """Simulate the load step and allow intentional failure for alert-path verification."""
    if payload["should_fail"]:
        raise Failure("Intentional failure to validate run-failure alerting.")

    context.log.info("Simulated successful warehouse load for batch %s", payload["batch_id"])


@job
def hydrosat_demo_job():
    load_into_stub_warehouse(extract_satellite_window())


@run_failure_sensor(name="alertmanager_job_failure_alert", monitored_jobs=[hydrosat_demo_job], minimum_interval_seconds=30)
def alertmanager_job_failure_alert(context: RunFailureSensorContext):
    """Forward Dagster job failures to Alertmanager so alert routing stays centralized."""
    alertmanager_url = os.getenv("ALERTMANAGER_URL", "")

    if not alertmanager_url:
        context.log.warning("ALERTMANAGER_URL is unset; skipping Alertmanager publish.")
        return

    failure_message = context.failure_event.message if context.failure_event else "Run failed"
    payload = build_alertmanager_payload(
        job_name=context.dagster_run.job_name,
        run_id=context.dagster_run.run_id,
        failure_message=failure_message,
    )

    request = Request(
        alertmanager_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        # Dagster sensors should fail loudly here so broken alert delivery is visible in the run context.
        with urlopen(request, timeout=10) as response:
            if response.status >= 300:
                raise Failure(f"Alertmanager returned unexpected status code {response.status}")
    except (HTTPError, URLError) as exc:
        raise Failure(f"Failed to publish Dagster alert to Alertmanager: {exc}") from exc

    context.log.info("Published Alertmanager failure alert for run %s", context.dagster_run.run_id)


defs = Definitions(
    jobs=[hydrosat_demo_job],
    sensors=[alertmanager_job_failure_alert],
)
