from hydrosat_dagster.definitions import (
    build_alertmanager_payload,
    build_failure_message,
    hydrosat_demo_job,
)


def demo_run_config(should_fail: bool) -> dict:
    return {
        "ops": {
            "extract_satellite_window": {
                "config": {
                    "should_fail": should_fail,
                }
            }
        }
    }


def test_demo_job_succeeds_when_should_fail_is_false():
    result = hydrosat_demo_job.execute_in_process(
        run_config=demo_run_config(False),
        raise_on_error=False,
    )

    assert result.success is True


def test_demo_job_fails_when_should_fail_is_true():
    result = hydrosat_demo_job.execute_in_process(
        run_config=demo_run_config(True),
        raise_on_error=False,
    )

    assert result.success is False


def test_failure_message_contains_core_context():
    message = build_failure_message(
        job_name="hydrosat_demo_job",
        run_id="abc123",
        failure_message="boom",
    )

    assert "job_name=hydrosat_demo_job" in message
    assert "run_id=abc123" in message
    assert "message=boom" in message


def test_alertmanager_payload_contains_expected_labels_and_annotations():
    payload = build_alertmanager_payload(
        job_name="hydrosat_demo_job",
        run_id="abc123",
        failure_message="boom",
    )

    alert = payload[0]

    assert alert["labels"]["alertname"] == "DagsterJobFailed"
    assert alert["labels"]["severity"] == "critical"
    assert alert["labels"]["job_name"] == "hydrosat_demo_job"
    assert alert["labels"]["run_id"] == "abc123"
    assert alert["annotations"]["summary"] == "Dagster job failed: hydrosat_demo_job"
    assert alert["annotations"]["description"] == "boom"
