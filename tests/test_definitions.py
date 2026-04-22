import json
from pathlib import Path

from sight_poc_dagster.definitions import (
    SAMPLE_SATELLITE_OBSERVATIONS,
    build_alertmanager_payload,
    build_failure_message,
    build_lakehouse_run_config,
    daily_lakehouse_schedule,
    sight_poc_lakehouse_job,
    lakehouse_partition_recovery_sensor,
)
from dagster import RunRequest, SkipReason


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
