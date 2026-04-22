import json
import os
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

import boto3
from dagster import (
    Definitions,
    Failure,
    RunFailureSensorContext,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    job,
    op,
    run_failure_sensor,
    schedule,
    sensor,
)

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


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _lake_root() -> Path:
    """Use a local filesystem root that mirrors the intended S3 raw/staging/curated layout."""
    return Path(os.getenv("SIGHT_POC_DATA_LAKE_ROOT", "/tmp/sight-poc-data-lake"))


def _lake_bucket() -> str:
    return os.getenv("SIGHT_POC_DATA_LAKE_BUCKET", "").strip()


def _lake_prefix() -> str:
    return os.getenv("SIGHT_POC_DATA_LAKE_PREFIX", "sight-poc").strip("/")


def _dbt_project_dir() -> Path:
    return _repo_root() / "dbt"


def _dbt_duckdb_path() -> Path:
    override = os.getenv("SIGHT_POC_DBT_DUCKDB_PATH", "")
    if override:
        return Path(override)
    return _lake_root() / "_dbt" / "sight_poc.duckdb"


def _runtime_home_dir() -> Path:
    override = os.getenv("SIGHT_POC_RUNTIME_HOME", "").strip()
    if override:
        return Path(override)

    dagster_home = os.getenv("DAGSTER_HOME", "").strip()
    if dagster_home:
        return Path(dagster_home).parent

    return Path("/tmp/sight-poc-home")


def _dbt_command_timeout_seconds() -> int:
    override = os.getenv("SIGHT_POC_DBT_TIMEOUT_SECONDS", "").strip()
    if override:
        return int(override)
    return 180


def _partition_date(value: str | None) -> str:
    if value:
        date.fromisoformat(value)
        return value
    return date.today().isoformat()


def _operational_partition_date() -> str:
    return datetime.now(UTC).date().isoformat()


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
        _s3_client().put_object(
            Bucket=bucket, Key=key, Body=text.encode("utf-8"), ContentType=content_type
        )
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
    _write_text(
        uri, "".join(f"{json.dumps(record)}\n" for record in records), "application/x-ndjson"
    )


def _jsonl_read(uri: str) -> list[dict]:
    return [json.loads(line) for line in _read_text(uri).splitlines() if line.strip()]


def _json_read(uri: str) -> list[dict]:
    return json.loads(_read_text(uri))


def _curated_partition_prefix(partition_date: str) -> str:
    return _lake_uri("curated", "tile_summary", f"partition_date={partition_date}")


def _partition_has_curated_output(partition_date: str) -> bool:
    partition_prefix = _curated_partition_prefix(partition_date)

    if partition_prefix.startswith("s3://"):
        bucket, key_prefix = _split_s3_uri(partition_prefix)
        response = _s3_client().list_objects_v2(
            Bucket=bucket, Prefix=f"{key_prefix.rstrip('/')}/", MaxKeys=1
        )
        return bool(response.get("Contents"))

    partition_path = Path(partition_prefix)
    return partition_path.exists() and any(partition_path.glob("*.json"))


def _ensure_local_parent_dir(uri: str) -> None:
    if uri.startswith("s3://"):
        return
    Path(uri).parent.mkdir(parents=True, exist_ok=True)


def _dbt_work_dir(batch_id: str, partition_date: str) -> Path:
    return _runtime_home_dir() / "dbt-work" / partition_date / batch_id


def _stage_local_copy(source_uri: str, destination_path: Path, content_type: str) -> str:
    _write_text(str(destination_path), _read_text(source_uri), content_type)
    return str(destination_path)


def _publish_local_copy(source_path: Path, destination_uri: str, content_type: str) -> None:
    _write_text(destination_uri, source_path.read_text(encoding="utf-8"), content_type)


def _run_dbt_command(command: list[str], env: dict[str, str]) -> None:
    timeout_seconds = _dbt_command_timeout_seconds()
    try:
        process = subprocess.run(
            command,
            cwd=_dbt_project_dir(),
            env=env,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise Failure(
            f"dbt command timed out after {timeout_seconds} seconds: {' '.join(command)}\n"
            f"stdout:\n{exc.stdout or ''}\n"
            f"stderr:\n{exc.stderr or ''}"
        ) from exc

    if process.returncode != 0:
        raise Failure(
            f"dbt command failed: {' '.join(command)}\nstdout:\n{process.stdout}\nstderr:\n{process.stderr}"
        )


def _dbt_cli_command(*args: str) -> list[str]:
    return [sys.executable, "-m", "dbt.cli.main", *args]


def build_failure_message(job_name: str, run_id: str, failure_message: str) -> str:
    """Format a consistent failure message for the optional Alertmanager compatibility path."""
    return (
        f"Dagster job failure detected.\n"
        f"job_name={job_name}\n"
        f"run_id={run_id}\n"
        f"failed_at_utc={datetime.now(UTC).isoformat()}\n"
        f"message={failure_message}"
    )


def build_alertmanager_payload(job_name: str, run_id: str, failure_message: str) -> list[dict]:
    """Build the minimal Alertmanager v2 payload for optional external alert routing."""
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


def build_lakehouse_run_config(batch_date: str, should_fail: bool = False) -> dict:
    return {
        "ops": {
            "extract_satellite_observations": {
                "config": {
                    "batch_date": batch_date,
                    "should_fail": should_fail,
                }
            }
        }
    }


@op(config_schema={"batch_date": str, "should_fail": bool})
def extract_satellite_observations(context) -> dict:
    """Simulate a Python-driven extract into the raw layer."""
    partition_date = _partition_date(context.op_config["batch_date"])
    batch_id = str(uuid4())
    raw_path = _lake_uri(
        "raw", "satellite_observations", f"ingest_date={partition_date}", f"{batch_id}.jsonl"
    )

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
def transform_with_dbt(context, raw_batch: dict) -> dict:
    """Run dbt transforms for staging and curated layers, then export results back into the lake layout."""
    staged_path = _lake_uri(
        "staging",
        "satellite_observations",
        f"ingest_date={raw_batch['partition_date']}",
        f"{raw_batch['batch_id']}.jsonl",
    )
    curated_path = _lake_uri(
        "curated",
        "tile_summary",
        f"partition_date={raw_batch['partition_date']}",
        f"{raw_batch['batch_id']}.json",
    )

    work_dir = _dbt_work_dir(raw_batch["batch_id"], raw_batch["partition_date"])
    work_dir.mkdir(parents=True, exist_ok=True)

    raw_work_path = work_dir / "raw.jsonl"
    staged_work_path = work_dir / "staging.jsonl"
    curated_work_path = work_dir / "curated.json"

    dbt_raw_uri = _stage_local_copy(raw_batch["raw_path"], raw_work_path, "application/x-ndjson")
    dbt_staging_uri = str(staged_work_path)
    dbt_curated_uri = str(curated_work_path)

    duckdb_path = _dbt_duckdb_path()
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    _ensure_local_parent_dir(staged_path)
    _ensure_local_parent_dir(curated_path)

    env = {
        **os.environ,
        "SIGHT_POC_RAW_URI": dbt_raw_uri,
        "SIGHT_POC_STAGING_URI": dbt_staging_uri,
        "SIGHT_POC_CURATED_URI": dbt_curated_uri,
        "SIGHT_POC_BATCH_DATE": raw_batch["partition_date"],
        "SIGHT_POC_DUCKDB_PATH": str(duckdb_path),
    }
    runtime_home = _runtime_home_dir()
    runtime_home.mkdir(parents=True, exist_ok=True)
    cache_dir = runtime_home / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    env["HOME"] = str(runtime_home)
    env["XDG_CACHE_HOME"] = str(cache_dir)

    _run_dbt_command(
        _dbt_cli_command(
            "build",
            "--project-dir",
            str(_dbt_project_dir()),
            "--profiles-dir",
            str(_dbt_project_dir()),
            "--target",
            "sight_poc",
        ),
        env,
    )
    _run_dbt_command(
        _dbt_cli_command(
            "run-operation",
            "export_lake_outputs",
            "--project-dir",
            str(_dbt_project_dir()),
            "--profiles-dir",
            str(_dbt_project_dir()),
            "--target",
            "sight_poc",
        ),
        env,
    )

    _publish_local_copy(staged_work_path, staged_path, "application/x-ndjson")
    _publish_local_copy(curated_work_path, curated_path, "application/json")

    staged_records = _jsonl_read(str(staged_work_path))
    curated_records = _json_read(str(curated_work_path))

    if raw_batch["should_fail"]:
        raise Failure("Intentional failure to validate run-failure alerting.")

    context.log.info("dbt exported %s staged observations to %s", len(staged_records), staged_path)
    context.log.info(
        "dbt exported %s curated tile summaries to %s", len(curated_records), curated_path
    )

    return {
        "batch_id": raw_batch["batch_id"],
        "partition_date": raw_batch["partition_date"],
        "staged_path": staged_path,
        "curated_path": curated_path,
        "staged_record_count": len(staged_records),
        "tile_count": len(curated_records),
    }


@job
def sight_poc_lakehouse_job():
    transform_with_dbt(extract_satellite_observations())


@schedule(
    cron_schedule="0 3 * * *",
    execution_timezone="UTC",
    job=sight_poc_lakehouse_job,
    name="daily_lakehouse_schedule",
)
def daily_lakehouse_schedule(_context):
    """Run the lakehouse pipeline once per UTC day for the current partition date."""
    return build_lakehouse_run_config(batch_date=_operational_partition_date())


@sensor(
    name="lakehouse_partition_recovery_sensor",
    minimum_interval_seconds=300,
    job=sight_poc_lakehouse_job,
)
def lakehouse_partition_recovery_sensor(_context: SensorEvaluationContext):
    """Trigger the daily partition if the expected curated output is still missing."""
    partition_date = _operational_partition_date()

    if _partition_has_curated_output(partition_date):
        return SkipReason(f"Curated partition for {partition_date} already exists.")

    return RunRequest(
        run_key=f"lakehouse-recovery-{partition_date}",
        run_config=build_lakehouse_run_config(batch_date=partition_date),
        tags={
            "sight-poc/partition_date": partition_date,
            "sight-poc/trigger": "recovery-sensor",
        },
    )


@run_failure_sensor(
    name="alertmanager_job_failure_alert",
    monitored_jobs=[sight_poc_lakehouse_job],
    minimum_interval_seconds=30,
)
def alertmanager_job_failure_alert(context: RunFailureSensorContext):
    """Optionally forward Dagster job failures to Alertmanager when ALERTMANAGER_URL is configured."""
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
    jobs=[sight_poc_lakehouse_job],
    schedules=[daily_lakehouse_schedule],
    sensors=[alertmanager_job_failure_alert, lakehouse_partition_recovery_sensor],
)
