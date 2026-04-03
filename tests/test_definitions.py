import json
from pathlib import Path

from hydrosat_dagster.definitions import (
    SAMPLE_SATELLITE_OBSERVATIONS,
    build_alertmanager_payload,
    build_failure_message,
    hydrosat_lakehouse_job,
)


def lakehouse_run_config(should_fail: bool) -> dict:
    return {
        "ops": {
            "extract_satellite_observations": {
                "config": {
                    "batch_date": "2026-04-01",
                    "should_fail": should_fail,
                }
            }
        }
    }


def test_lakehouse_job_writes_raw_staging_and_curated_layers(tmp_path, monkeypatch):
    monkeypatch.setenv("HYDROSAT_DATA_LAKE_ROOT", str(tmp_path))

    result = hydrosat_lakehouse_job.execute_in_process(
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

    raw_records = [json.loads(line) for line in raw_files[0].read_text(encoding="utf-8").splitlines()]
    staged_records = [json.loads(line) for line in staging_files[0].read_text(encoding="utf-8").splitlines()]
    curated_records = json.loads(curated_files[0].read_text(encoding="utf-8"))

    assert len(raw_records) == len(SAMPLE_SATELLITE_OBSERVATIONS)
    assert len(staged_records) == len(SAMPLE_SATELLITE_OBSERVATIONS)
    assert {record["tile_id"] for record in curated_records} == {"T31UFQ", "T31UGQ"}


def test_lakehouse_job_fails_when_curated_step_is_configured_to_fail(tmp_path, monkeypatch):
    monkeypatch.setenv("HYDROSAT_DATA_LAKE_ROOT", str(tmp_path))

    result = hydrosat_lakehouse_job.execute_in_process(
        run_config=lakehouse_run_config(True),
        raise_on_error=False,
    )

    assert result.success is False
    assert list((tmp_path / "raw").rglob("*.jsonl"))
    assert list((tmp_path / "staging").rglob("*.jsonl"))
    assert not list((tmp_path / "curated").rglob("*.json"))


def test_failure_message_contains_core_context():
    message = build_failure_message(
        job_name="hydrosat_lakehouse_job",
        run_id="abc123",
        failure_message="boom",
    )

    assert "job_name=hydrosat_lakehouse_job" in message
    assert "run_id=abc123" in message
    assert "message=boom" in message


def test_alertmanager_payload_contains_expected_labels_and_annotations():
    payload = build_alertmanager_payload(
        job_name="hydrosat_lakehouse_job",
        run_id="abc123",
        failure_message="boom",
    )

    alert = payload[0]

    assert alert["labels"]["alertname"] == "DagsterJobFailed"
    assert alert["labels"]["severity"] == "critical"
    assert alert["labels"]["job_name"] == "hydrosat_lakehouse_job"
    assert alert["labels"]["run_id"] == "abc123"
    assert alert["annotations"]["summary"] == "Dagster job failed: hydrosat_lakehouse_job"
    assert alert["annotations"]["description"] == "boom"
