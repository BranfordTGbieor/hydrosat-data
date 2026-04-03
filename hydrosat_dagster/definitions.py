import json
import os
from datetime import UTC, date, datetime
from pathlib import Path
from statistics import mean
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

import boto3
from dagster import Definitions, Failure, RunFailureSensorContext, job, op, run_failure_sensor

SAMPLE_SATELLITE_OBSERVATIONS = [
    {
        "scene_id": "scene-001",
        "tile_id": "T31UFQ",
        "captured_at": "2026-04-01T10:15:00Z",
        "surface_temp_c": 27.4,
        "ndvi": 0.82,
        "cloud_cover_pct": 4.1,
    },
    {
        "scene_id": "scene-002",
        "tile_id": "T31UFQ",
        "captured_at": "2026-04-01T10:20:00Z",
        "surface_temp_c": 28.1,
        "ndvi": 0.79,
        "cloud_cover_pct": 6.5,
    },
    {
        "scene_id": "scene-003",
        "tile_id": "T31UGQ",
        "captured_at": "2026-04-01T10:32:00Z",
        "surface_temp_c": 24.8,
        "ndvi": 0.88,
        "cloud_cover_pct": 1.9,
    },
    {
        "scene_id": "scene-004",
        "tile_id": "T31UGQ",
        "captured_at": "2026-04-01T10:37:00Z",
        "surface_temp_c": 25.6,
        "ndvi": 0.86,
        "cloud_cover_pct": 2.3,
    },
]


def _lake_root() -> Path:
    """Use a local filesystem root that mirrors the intended S3 raw/staging/curated layout."""
    return Path(os.getenv("HYDROSAT_DATA_LAKE_ROOT", "/tmp/hydrosat-data-lake"))


def _lake_bucket() -> str:
    return os.getenv("HYDROSAT_DATA_LAKE_BUCKET", "").strip()


def _lake_prefix() -> str:
    return os.getenv("HYDROSAT_DATA_LAKE_PREFIX", "hydrosat").strip("/")


def _partition_date(value: str | None) -> str:
    if value:
        date.fromisoformat(value)
        return value
    return date.today().isoformat()


def _s3_client():
    return boto3.client("s3")


def _lake_uri(*parts: str) -> str:
    relative_path = "/".join(part.strip("/") for part in parts if part)
    bucket = _lake_bucket()
    prefix = _lake_prefix()

    if bucket:
        prefix_path = f"{prefix}/{relative_path}" if prefix else relative_path
        return f"s3://{bucket}/{prefix_path}"

    return str(_lake_root() / relative_path)


def _split_s3_uri(uri: str) -> tuple[str, str]:
    without_scheme = uri.removeprefix("s3://")
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


def _write_text(uri: str, text: str, content_type: str) -> None:
    if uri.startswith("s3://"):
        bucket, key = _split_s3_uri(uri)
        _s3_client().put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"), ContentType=content_type)
        return

    path = Path(uri)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _read_text(uri: str) -> str:
    if uri.startswith("s3://"):
        bucket, key = _split_s3_uri(uri)
        response = _s3_client().get_object(Bucket=bucket, Key=key)
        return response["Body"].read().decode("utf-8")

    return Path(uri).read_text(encoding="utf-8")


def _jsonl_write(uri: str, records: list[dict]) -> None:
    _write_text(uri, "".join(f"{json.dumps(record)}\n" for record in records), "application/x-ndjson")


def _jsonl_read(uri: str) -> list[dict]:
    return [json.loads(line) for line in _read_text(uri).splitlines() if line.strip()]


def build_failure_message(job_name: str, run_id: str, failure_message: str) -> str:
    """Format a consistent alert body for Alertmanager-routed job failures."""
    return (
        f"Dagster job failure detected.\n"
        f"job_name={job_name}\n"
        f"run_id={run_id}\n"
        f"failed_at_utc={datetime.now(UTC).isoformat()}\n"
        f"message={failure_message}"
    )


def build_alertmanager_payload(job_name: str, run_id: str, failure_message: str) -> list[dict]:
    """Build the minimal Alertmanager v2 payload for a failed Dagster run."""
    failed_at = datetime.now(UTC).isoformat()
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


@op(config_schema={"batch_date": str, "should_fail": bool})
def extract_satellite_observations(context) -> dict:
    """Simulate a Python-driven extract into the raw layer."""
    partition_date = _partition_date(context.op_config["batch_date"])
    batch_id = str(uuid4())
    raw_path = _lake_uri("raw", "satellite_observations", f"ingest_date={partition_date}", f"{batch_id}.jsonl")

    raw_records = []
    for record in SAMPLE_SATELLITE_OBSERVATIONS:
        raw_record = {
            "batch_id": batch_id,
            "ingested_at": datetime.now(UTC).isoformat(),
            "record_source": "sample_satellite_feed",
            "should_fail": context.op_config["should_fail"],
            **record,
        }
        raw_records.append(raw_record)

    _jsonl_write(raw_path, raw_records)
    context.log.info("Wrote %s raw observations to %s", len(raw_records), raw_path)

    return {
        "batch_id": batch_id,
        "partition_date": partition_date,
        "record_count": len(raw_records),
        "raw_path": str(raw_path),
        "should_fail": context.op_config["should_fail"],
    }


@op
def stage_satellite_observations(context, raw_batch: dict) -> dict:
    """Clean and enrich raw records into a staging-friendly shape."""
    raw_records = _jsonl_read(raw_batch["raw_path"])
    staged_path = _lake_uri(
        "staging",
        "satellite_observations",
        f"ingest_date={raw_batch['partition_date']}",
        f"{raw_batch['batch_id']}.jsonl",
    )

    staged_records = []
    for record in raw_records:
        staged_records.append(
            {
                "batch_id": record["batch_id"],
                "partition_date": raw_batch["partition_date"],
                "scene_id": record["scene_id"],
                "tile_id": record["tile_id"],
                "captured_at": record["captured_at"],
                "surface_temp_c": round(float(record["surface_temp_c"]), 2),
                "ndvi": round(float(record["ndvi"]), 3),
                "cloud_cover_pct": round(float(record["cloud_cover_pct"]), 2),
                "quality_band": "high" if float(record["cloud_cover_pct"]) < 5 else "moderate",
                "vegetation_band": "dense" if float(record["ndvi"]) >= 0.8 else "medium",
                "should_fail": record["should_fail"],
            }
        )

    _jsonl_write(staged_path, staged_records)
    context.log.info("Wrote %s staged observations to %s", len(staged_records), staged_path)

    return {
        "batch_id": raw_batch["batch_id"],
        "partition_date": raw_batch["partition_date"],
        "record_count": len(staged_records),
        "staged_path": str(staged_path),
        "should_fail": raw_batch["should_fail"],
    }


@op
def curate_tile_summary(context, staged_batch: dict) -> dict:
    """Aggregate staged records into a curated tile-level summary."""
    staged_records = _jsonl_read(staged_batch["staged_path"])
    if staged_batch["should_fail"]:
        raise Failure("Intentional failure to validate run-failure alerting.")

    by_tile: dict[str, list[dict]] = {}
    for record in staged_records:
        by_tile.setdefault(record["tile_id"], []).append(record)

    curated_records = []
    for tile_id, records in sorted(by_tile.items()):
        curated_records.append(
            {
                "batch_id": staged_batch["batch_id"],
                "partition_date": staged_batch["partition_date"],
                "tile_id": tile_id,
                "observation_count": len(records),
                "avg_surface_temp_c": round(mean(record["surface_temp_c"] for record in records), 2),
                "max_ndvi": max(record["ndvi"] for record in records),
                "quality_band_breakdown": {
                    band: sum(1 for record in records if record["quality_band"] == band)
                    for band in sorted({record["quality_band"] for record in records})
                },
            }
        )

    curated_path = _lake_uri(
        "curated",
        "tile_summary",
        f"partition_date={staged_batch['partition_date']}",
        f"{staged_batch['batch_id']}.json",
    )
    _write_text(curated_path, json.dumps(curated_records, indent=2), "application/json")
    context.log.info("Wrote %s curated tile summaries to %s", len(curated_records), curated_path)

    return {
        "batch_id": staged_batch["batch_id"],
        "partition_date": staged_batch["partition_date"],
        "curated_path": str(curated_path),
        "tile_count": len(curated_records),
    }


@job
def hydrosat_lakehouse_job():
    curate_tile_summary(stage_satellite_observations(extract_satellite_observations()))


@run_failure_sensor(name="alertmanager_job_failure_alert", monitored_jobs=[hydrosat_lakehouse_job], minimum_interval_seconds=30)
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
        with urlopen(request, timeout=10) as response:
            if response.status >= 300:
                raise Failure(f"Alertmanager returned unexpected status code {response.status}")
    except (HTTPError, URLError) as exc:
        raise Failure(f"Failed to publish Dagster alert to Alertmanager: {exc}") from exc

    context.log.info("Published Alertmanager failure alert for run %s", context.dagster_run.run_id)


defs = Definitions(
    jobs=[hydrosat_lakehouse_job],
    sensors=[alertmanager_job_failure_alert],
)
