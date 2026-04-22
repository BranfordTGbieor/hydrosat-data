import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from urllib.error import URLError

from dagster import RunRequest, SkipReason

import sight_poc_dagster.definitions as definitions
from sight_poc_dagster.definitions import (
    SAMPLE_SATELLITE_OBSERVATIONS,
    build_alertmanager_payload,
    build_failure_message,
    build_lakehouse_run_config,
    daily_lakehouse_schedule,
    lakehouse_partition_recovery_sensor,
    sight_poc_lakehouse_job,
)


def lakehouse_run_config(should_fail: bool) -> dict:
    return build_lakehouse_run_config(batch_date="2026-04-01", should_fail=should_fail)


def fake_dbt_runner(command: list[str], env: dict[str, str]) -> None:
    raw_path = env["SIGHT_POC_RAW_URI"]
    staged_path = env["SIGHT_POC_STAGING_URI"]
    curated_path = env["SIGHT_POC_CURATED_URI"]

    if "run-operation" not in command:
        return

    with open(raw_path, encoding="utf-8") as handle:
        raw_records = [json.loads(line) for line in handle if line.strip()]

    staged_records = []
    by_tile: dict[str, list[dict]] = {}
    for record in raw_records:
        staged_record = {
            "batch_id": record["batch_id"],
            "partition_date": env["SIGHT_POC_BATCH_DATE"],
            "scene_id": record["scene_id"],
            "tile_id": record["tile_id"],
            "captured_at": record["captured_at"],
            "surface_temp_c": round(float(record["surface_temp_c"]), 2),
            "ndvi": round(float(record["ndvi"]), 3),
            "cloud_cover_pct": round(float(record["cloud_cover_pct"]), 2),
            "quality_band": "high" if float(record["cloud_cover_pct"]) < 5 else "moderate",
            "vegetation_band": "dense" if float(record["ndvi"]) >= 0.8 else "medium",
        }
        staged_records.append(staged_record)
        by_tile.setdefault(staged_record["tile_id"], []).append(staged_record)

    Path(staged_path).parent.mkdir(parents=True, exist_ok=True)
    with open(staged_path, "w", encoding="utf-8") as handle:
        for record in staged_records:
            handle.write(json.dumps(record) + "\n")

    curated_records = []
    for tile_id, records in sorted(by_tile.items()):
        curated_records.append(
            {
                "batch_id": records[0]["batch_id"],
                "partition_date": env["SIGHT_POC_BATCH_DATE"],
                "tile_id": tile_id,
                "observation_count": len(records),
                "avg_surface_temp_c": round(
                    sum(record["surface_temp_c"] for record in records) / len(records), 2
                ),
                "max_ndvi": max(record["ndvi"] for record in records),
                "quality_band_breakdown": {
                    "high": sum(1 for record in records if record["quality_band"] == "high"),
                    "moderate": sum(
                        1 for record in records if record["quality_band"] == "moderate"
                    ),
                },
            }
        )

    Path(curated_path).parent.mkdir(parents=True, exist_ok=True)
    with open(curated_path, "w", encoding="utf-8") as handle:
        json.dump(curated_records, handle)


def test_lakehouse_job_writes_raw_dbt_staging_and_curated_layers(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGHT_POC_DATA_LAKE_ROOT", str(tmp_path))
    monkeypatch.setattr("sight_poc_dagster.definitions._run_dbt_command", fake_dbt_runner)

    result = sight_poc_lakehouse_job.execute_in_process(
        run_config=lakehouse_run_config(False),
        raise_on_error=False,
    )

    assert result.success is True

    raw_files = list((tmp_path / "raw").rglob("*.jsonl"))
    staging_files = list((tmp_path / "staging").rglob("*.jsonl"))
    curated_files = list((tmp_path / "curated").rglob("*.json"))

    assert len(raw_files) == 1
    assert len(staging_files) == 1
    assert len(curated_files) == 1

    raw_records = [
        json.loads(line) for line in raw_files[0].read_text(encoding="utf-8").splitlines()
    ]
    staged_records = [
        json.loads(line) for line in staging_files[0].read_text(encoding="utf-8").splitlines()
    ]
    curated_records = json.loads(curated_files[0].read_text(encoding="utf-8"))

    assert len(raw_records) == len(SAMPLE_SATELLITE_OBSERVATIONS)
    assert len(staged_records) == len(SAMPLE_SATELLITE_OBSERVATIONS)
    assert {record["tile_id"] for record in curated_records} == {"T31UFQ", "T31UGQ"}


def test_lakehouse_job_fails_when_quality_gate_is_configured_to_fail(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGHT_POC_DATA_LAKE_ROOT", str(tmp_path))
    monkeypatch.setattr("sight_poc_dagster.definitions._run_dbt_command", fake_dbt_runner)

    result = sight_poc_lakehouse_job.execute_in_process(
        run_config=lakehouse_run_config(True),
        raise_on_error=False,
    )

    assert result.success is False
    assert list((tmp_path / "raw").rglob("*.jsonl"))
    assert list((tmp_path / "staging").rglob("*.jsonl"))
    assert list((tmp_path / "curated").rglob("*.json"))


def test_failure_message_contains_core_context():
    message = build_failure_message(
        job_name="sight_poc_lakehouse_job",
        run_id="abc123",
        failure_message="boom",
    )

    assert "job_name=sight_poc_lakehouse_job" in message
    assert "run_id=abc123" in message
    assert "message=boom" in message


def test_alertmanager_payload_contains_expected_labels_and_annotations():
    payload = build_alertmanager_payload(
        job_name="sight_poc_lakehouse_job",
        run_id="abc123",
        failure_message="boom",
    )

    alert = payload[0]

    assert alert["labels"]["alertname"] == "DagsterJobFailed"
    assert alert["labels"]["severity"] == "critical"
    assert alert["labels"]["job_name"] == "sight_poc_lakehouse_job"
    assert alert["labels"]["run_id"] == "abc123"
    assert alert["annotations"]["summary"] == "Dagster job failed: sight_poc_lakehouse_job"
    assert alert["annotations"]["description"] == "boom"


def test_daily_lakehouse_schedule_targets_current_partition(monkeypatch):
    monkeypatch.setattr(
        "sight_poc_dagster.definitions._operational_partition_date", lambda: "2026-04-02"
    )

    run_request = daily_lakehouse_schedule(None)

    assert run_request == build_lakehouse_run_config(batch_date="2026-04-02")


def test_recovery_sensor_requests_run_when_curated_partition_is_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGHT_POC_DATA_LAKE_ROOT", str(tmp_path))
    monkeypatch.setattr(
        "sight_poc_dagster.definitions._operational_partition_date", lambda: "2026-04-03"
    )

    result = lakehouse_partition_recovery_sensor(None)

    assert isinstance(result, RunRequest)
    assert result.run_key == "lakehouse-recovery-2026-04-03"
    assert result.run_config == build_lakehouse_run_config(batch_date="2026-04-03")
    assert result.tags["sight-poc/trigger"] == "recovery-sensor"


def test_recovery_sensor_skips_when_curated_partition_exists(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGHT_POC_DATA_LAKE_ROOT", str(tmp_path))
    monkeypatch.setattr(
        "sight_poc_dagster.definitions._operational_partition_date", lambda: "2026-04-03"
    )

    curated_dir = tmp_path / "curated" / "tile_summary" / "partition_date=2026-04-03"
    curated_dir.mkdir(parents=True, exist_ok=True)
    (curated_dir / "batch-1.json").write_text("[]", encoding="utf-8")

    result = lakehouse_partition_recovery_sensor(None)

    assert isinstance(result, SkipReason)
    assert "2026-04-03" in result.skip_message


def test_helper_environment_overrides_and_uri_generation(monkeypatch):
    monkeypatch.setenv("SIGHT_POC_DBT_DUCKDB_PATH", "/tmp/custom.duckdb")
    monkeypatch.setenv("SIGHT_POC_RUNTIME_HOME", "/tmp/runtime-home")
    monkeypatch.setenv("SIGHT_POC_DBT_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("SIGHT_POC_DATA_LAKE_BUCKET", "demo-bucket")
    monkeypatch.setenv("SIGHT_POC_DATA_LAKE_PREFIX", "demo-prefix")

    assert definitions._dbt_duckdb_path() == Path("/tmp/custom.duckdb")
    assert definitions._runtime_home_dir() == Path("/tmp/runtime-home")
    assert definitions._dbt_command_timeout_seconds() == 45
    assert (
        definitions._lake_uri("raw", "records.jsonl")
        == "s3://demo-bucket/demo-prefix/raw/records.jsonl"
    )
    assert definitions._split_s3_uri("s3://demo-bucket/demo-prefix/raw/records.jsonl") == (
        "demo-bucket",
        "demo-prefix/raw/records.jsonl",
    )


def test_local_text_and_json_helpers(tmp_path):
    text_path = tmp_path / "nested" / "sample.txt"
    definitions._write_text(str(text_path), "hello", "text/plain")
    assert definitions._read_text(str(text_path)) == "hello"

    jsonl_path = tmp_path / "nested" / "sample.jsonl"
    records = [{"a": 1}, {"a": 2}]
    definitions._jsonl_write(str(jsonl_path), records)
    assert definitions._jsonl_read(str(jsonl_path)) == records

    json_path = tmp_path / "nested" / "sample.json"
    json_path.write_text(json.dumps(records), encoding="utf-8")
    assert definitions._json_read(str(json_path)) == records

    created_path = tmp_path / "another" / "dir" / "file.txt"
    definitions._ensure_local_parent_dir(str(created_path))
    assert created_path.parent.exists()


def test_s3_helpers_use_client(monkeypatch):
    stored: dict[str, object] = {}

    class FakeBody:
        def read(self):
            return b"payload"

    class FakeS3:
        def put_object(self, **kwargs):
            stored["put"] = kwargs

        def get_object(self, **kwargs):
            stored["get"] = kwargs
            return {"Body": FakeBody()}

        def list_objects_v2(self, **kwargs):
            stored["list"] = kwargs
            return {"Contents": [{"Key": "one"}]}

    monkeypatch.setattr(definitions, "_s3_client", lambda: FakeS3())
    monkeypatch.setenv("SIGHT_POC_DATA_LAKE_BUCKET", "bucket")
    monkeypatch.setenv("SIGHT_POC_DATA_LAKE_PREFIX", "prefix")

    definitions._write_text("s3://bucket/key.txt", "payload", "text/plain")
    assert stored["put"]["Bucket"] == "bucket"
    assert stored["put"]["Key"] == "key.txt"

    assert definitions._read_text("s3://bucket/key.txt") == "payload"
    assert stored["get"] == {"Bucket": "bucket", "Key": "key.txt"}
    assert definitions._partition_has_curated_output("2026-04-01") is True
    assert stored["list"]["Bucket"] == "bucket"


def test_run_dbt_command_raises_on_timeout(monkeypatch):
    def fake_run(*args, **kwargs):
        exc = subprocess.TimeoutExpired(cmd=["dbt", "build"], timeout=15, output="out")
        exc.stderr = "err"
        raise exc

    monkeypatch.setattr(definitions, "_dbt_command_timeout_seconds", lambda: 15)
    monkeypatch.setattr(subprocess, "run", fake_run)

    try:
        definitions._run_dbt_command(["dbt", "build"], {})
    except Exception as exc:
        assert "timed out after 15 seconds" in str(exc)
    else:
        raise AssertionError("Expected dbt timeout failure")


def test_run_dbt_command_raises_on_non_zero_exit(monkeypatch):
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=2, stdout="bad out", stderr="bad err"),
    )

    try:
        definitions._run_dbt_command(["dbt", "build"], {})
    except Exception as exc:
        assert "dbt command failed" in str(exc)
        assert "bad err" in str(exc)
    else:
        raise AssertionError("Expected dbt non-zero failure")


def _fake_failure_context(message: str = "Run failed"):
    return SimpleNamespace(
        dagster_run=SimpleNamespace(job_name="sight_poc_lakehouse_job", run_id="run-123"),
        failure_event=SimpleNamespace(message=message),
        log=SimpleNamespace(
            warning=lambda *args, **kwargs: None,
            info=lambda *args, **kwargs: None,
        ),
    )


def test_alertmanager_sensor_skips_when_url_unset(monkeypatch):
    recorded = []
    monkeypatch.delenv("ALERTMANAGER_URL", raising=False)

    context = SimpleNamespace(log=SimpleNamespace(warning=lambda message: recorded.append(message)))
    definitions.alertmanager_job_failure_alert._run_status_sensor_fn(context)

    assert recorded == ["ALERTMANAGER_URL is unset; skipping Alertmanager publish."]


def test_alertmanager_sensor_publishes_when_url_set(monkeypatch):
    monkeypatch.setenv("ALERTMANAGER_URL", "https://alerts.example")
    published = []

    class FakeResponse:
        status = 200

        def __enter__(self):
            published.append("entered")
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(definitions, "urlopen", lambda request, timeout: FakeResponse())
    context = _fake_failure_context("boom")

    definitions.alertmanager_job_failure_alert._run_status_sensor_fn(context)
    assert published == ["entered"]


def test_alertmanager_sensor_raises_when_publish_fails(monkeypatch):
    monkeypatch.setenv("ALERTMANAGER_URL", "https://alerts.example")
    monkeypatch.setattr(
        definitions,
        "urlopen",
        lambda request, timeout: (_ for _ in ()).throw(URLError("boom")),
    )
    context = _fake_failure_context("boom")

    try:
        definitions.alertmanager_job_failure_alert._run_status_sensor_fn(context)
    except Exception as exc:
        assert "Failed to publish Dagster alert to Alertmanager" in str(exc)
    else:
        raise AssertionError("Expected Alertmanager publish failure")
