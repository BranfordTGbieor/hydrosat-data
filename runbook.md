# Sight PoC Data Validation Runbook

This runbook validates the `sight-poc-data` repository from local code quality through Docker image publishing and Dagster runtime behavior.

Use this document when you want to prove that:

- the Dagster package installs cleanly
- unit tests cover the success and failure paths
- the layered lakehouse sample pipeline behaves as expected
- the pipeline can target local storage now and an S3-backed lake in-cluster later
- dbt is responsible for the staging and curated transforms
- the daily schedule and recovery sensor behave as expected
- the optional Alertmanager compatibility payload shape is correct
- the Docker image builds locally
- the Docker Hub release workflow is ready to publish
- the built image can be consumed by `sight-poc-infra`

This runbook assumes:

- repo root is `sight-poc-data/`
- Python 3.12 is available
- Docker is available locally
- optionally, Docker Hub credentials are available

## 1. Validation Order

Run these sections in order:

1. Local environment setup
2. Static package validation
3. Unit test validation
4. Direct Dagster job execution
5. Schedule and recovery sensor validation
6. Raw, staging, and curated data validation
7. dbt project validation
8. Optional Alertmanager compatibility validation
9. Container build validation
10. Docker Hub release workflow validation
11. Integration handoff validation for `sight-poc-infra`
12. S3-backed runtime validation

## 2. Local Environment Setup

### 2.1 Component: Python Environment

Commands:

```bash
cd /home/branford-t-gbieor/Desktop/gbieor/applications/exercises/sight-poc/sight-poc-data
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Expected success:

- package and dev dependencies install without resolution errors

Failure signs:

- missing compiler/build dependencies
- version conflicts during dependency installation

## 3. Static Package Validation

### 3.1 Component: Package Importability

Commands:

```bash
python -m compileall sight_poc_dagster tests
python -c "from sight_poc_dagster.definitions import defs; print(defs)"
```

Expected success:

- `compileall` exits `0`
- import command prints a Dagster `Definitions` object

Failure signs:

- syntax errors
- missing imports
- broken package metadata

## 4. Unit Test Validation

### 4.1 Component: Test Suite

Files:

- [sight_poc_dagster/definitions.py](/home/branford-t-gbieor/Desktop/gbieor/applications/exercises/sight-poc/sight-poc-data/sight_poc_dagster/definitions.py)
- [tests/test_definitions.py](/home/branford-t-gbieor/Desktop/gbieor/applications/exercises/sight-poc/sight-poc-data/tests/test_definitions.py)

Commands:

```bash
pytest -q
```

Expected success:

- all tests pass
- success-path test confirms `sight_poc_lakehouse_job` returns `success=True`
- failure-path test confirms `sight_poc_lakehouse_job` returns `success=False`
- schedule test confirms the current UTC partition is injected into run config
- recovery-sensor tests confirm a missing curated partition yields a `RunRequest` and an existing partition yields a `SkipReason`
- payload-format tests confirm the optional Alertmanager compatibility body contains the expected labels and annotations

Failure signs:

- any assertion failures in message or payload structure
- Dagster import errors
- test environment package issues

## 5. Direct Dagster Job Execution

### 5.1 Component: Success Path

Sample run config:

```json
{
  "ops": {
    "extract_satellite_observations": {
      "config": {
        "batch_date": "2026-04-01",
        "should_fail": false
      }
    }
  }
}
```

Commands:

```bash
python - <<'PY'
from sight_poc_dagster.definitions import sight_poc_lakehouse_job

run_config = {
    "ops": {
        "extract_satellite_observations": {
            "config": {"batch_date": "2026-04-01", "should_fail": False}
        }
    }
}

result = sight_poc_lakehouse_job.execute_in_process(run_config=run_config, raise_on_error=False)
print("success=", result.success)
PY
```

Expected success:

- terminal prints `success= True`
- logs show writes to `raw`, plus dbt-driven exports into `staging` and `curated`

Failure signs:

- result is `False`
- unexpected exception in extract, staging, or curated steps

### 5.2 Component: Failure Path

Sample run config:

```json
{
  "ops": {
    "extract_satellite_observations": {
      "config": {
        "batch_date": "2026-04-01",
        "should_fail": true
      }
    }
  }
}
```

Commands:

```bash
python - <<'PY'
from sight_poc_dagster.definitions import sight_poc_lakehouse_job

run_config = {
    "ops": {
        "extract_satellite_observations": {
            "config": {"batch_date": "2026-04-01", "should_fail": True}
        }
    }
}

result = sight_poc_lakehouse_job.execute_in_process(run_config=run_config, raise_on_error=False)
print("success=", result.success)
PY
```

Expected success:

- terminal prints `success= False`
- failure reason matches `Intentional failure to validate run-failure alerting.`

Failure signs:

- result is `True`
- failure occurs for the wrong reason

## 6. Schedule and Recovery Sensor Validation

### 6.1 Component: Daily Dagster Schedule

Commands:

```bash
python - <<'PY'
from sight_poc_dagster.definitions import daily_lakehouse_schedule

run_config = daily_lakehouse_schedule(None)
print(run_config)
PY
```

Expected success:

- output contains `extract_satellite_observations`
- output contains `should_fail: False`
- output contains today's UTC date in `batch_date`

Failure signs:

- missing `batch_date`
- `should_fail` defaults incorrectly
- import or schedule-construction errors

### 6.2 Component: Partition Recovery Sensor RunRequest

Commands:

```bash
export SIGHT_POC_DATA_LAKE_ROOT=/tmp/sight-poc-data-lake-sensor
rm -rf "${SIGHT_POC_DATA_LAKE_ROOT}"

python - <<'PY'
from sight_poc_dagster.definitions import lakehouse_partition_recovery_sensor

result = lakehouse_partition_recovery_sensor(None)
print(type(result).__name__)
print(getattr(result, "run_key", None))
print(getattr(result, "run_config", None))
PY
```

Expected success:

- evaluation returns `RunRequest`
- `run_key` starts with `lakehouse-recovery-`
- run config targets today's UTC partition

Failure signs:

- returns `SkipReason` when the curated partition does not exist
- returns malformed run config

### 6.3 Component: Partition Recovery Sensor SkipReason

Commands:

```bash
export SIGHT_POC_DATA_LAKE_ROOT=/tmp/sight-poc-data-lake-sensor
mkdir -p "${SIGHT_POC_DATA_LAKE_ROOT}/curated/tile_summary/partition_date=$(date -u +%F)"
printf '[]' > "${SIGHT_POC_DATA_LAKE_ROOT}/curated/tile_summary/partition_date=$(date -u +%F)/existing-batch.json"

python - <<'PY'
from sight_poc_dagster.definitions import lakehouse_partition_recovery_sensor

result = lakehouse_partition_recovery_sensor(None)
print(type(result).__name__)
print(getattr(result, "skip_message", None))
PY
```

Expected success:

- evaluation returns `SkipReason`
- skip message mentions the current UTC partition

Failure signs:

- sensor requests a duplicate run even though curated output already exists
- missing skip message

## 7. Raw, Staging, and Curated Data Validation

### 7.1 Component: Layered Output Layout

Commands:

```bash
export SIGHT_POC_DATA_LAKE_ROOT=/tmp/sight-poc-data-lake
rm -rf "${SIGHT_POC_DATA_LAKE_ROOT}"

python - <<'PY'
from sight_poc_dagster.definitions import sight_poc_lakehouse_job

run_config = {
    "ops": {
        "extract_satellite_observations": {
            "config": {"batch_date": "2026-04-01", "should_fail": False}
        }
    }
}

result = sight_poc_lakehouse_job.execute_in_process(run_config=run_config, raise_on_error=False)
print("success=", result.success)
PY

find "${SIGHT_POC_DATA_LAKE_ROOT}" -type f | sort
```

Expected success:

- one file exists under `raw/`
- one file exists under `staging/`
- one file exists under `curated/`

Failure signs:

- missing directories
- curated file absent on the success path

### 7.2 Component: Curated Summary Content

Commands:

```bash
python - <<'PY'
from pathlib import Path
import json
import os

root = Path(os.environ["SIGHT_POC_DATA_LAKE_ROOT"])
curated_file = next((root / "curated").rglob("*.json"))
print(curated_file)
print(json.dumps(json.loads(curated_file.read_text()), indent=2))
PY
```

Expected success:

- curated output contains tile summaries for `T31UFQ` and `T31UGQ`
- each record includes:
  - `observation_count`
  - `avg_surface_temp_c`
  - `max_ndvi`
  - `quality_band_breakdown`

Failure signs:

- malformed JSON
- missing aggregate fields

### 7.3 Component: Storage Mode Configuration

Commands:

```bash
python - <<'PY'
import os
print("SIGHT_POC_DATA_LAKE_ROOT=", os.getenv("SIGHT_POC_DATA_LAKE_ROOT", "/tmp/sight-poc-data-lake"))
print("SIGHT_POC_DATA_LAKE_BUCKET=", os.getenv("SIGHT_POC_DATA_LAKE_BUCKET", ""))
print("SIGHT_POC_DATA_LAKE_PREFIX=", os.getenv("SIGHT_POC_DATA_LAKE_PREFIX", "sight-poc"))
PY
```

Expected success:

- local runs can rely only on `SIGHT_POC_DATA_LAKE_ROOT`
- cluster runs can switch to S3 by setting `SIGHT_POC_DATA_LAKE_BUCKET`
- `SIGHT_POC_DATA_LAKE_PREFIX` defaults to `sight-poc`

Failure signs:

- bucket expected in-cluster but unset
- prefix not aligned with the expected raw/staging/curated layout

## 8. dbt Project Validation

### 8.1 Component: dbt Project Layout

Files:

- `dbt/dbt_project.yml`
- `dbt/profiles.yml`
- `dbt/models/staging/stg_satellite_observations.sql`
- `dbt/models/curated/cur_satellite_tile_summary.sql`
- `dbt/macros/export_lake_outputs.sql`

Commands:

```bash
find dbt -maxdepth 3 -type f | sort
```

Expected success:

- the dbt project contains project config, profile config, staging model, curated model, and export macro

Failure signs:

- missing model files
- missing `profiles.yml`

### 8.2 Component: dbt Environment Surface

Commands:

```bash
python - <<'PY'
import os
print("SIGHT_POC_RAW_URI=", os.getenv("SIGHT_POC_RAW_URI", "set-by-dagster"))
print("SIGHT_POC_STAGING_URI=", os.getenv("SIGHT_POC_STAGING_URI", "set-by-dagster"))
print("SIGHT_POC_CURATED_URI=", os.getenv("SIGHT_POC_CURATED_URI", "set-by-dagster"))
print("SIGHT_POC_DUCKDB_PATH=", os.getenv("SIGHT_POC_DUCKDB_PATH", "/tmp/sight-poc-data-lake/_dbt/sight-poc.duckdb"))
PY
```

Expected success:

- Dagster sets these values before invoking dbt
- DuckDB path points at a writable local path

Failure signs:

- dbt receives no raw/staging/curated URIs
- DuckDB path points at a non-writable location

## 9. Optional Alertmanager Compatibility Validation

### 9.1 Component: Failure Message Formatter

Commands:

```bash
python - <<'PY'
from sight_poc_dagster.definitions import build_failure_message
print(build_failure_message("sight_poc_lakehouse_job", "abc123", "boom"))
PY
```

Expected success:

- output includes:
  - `job_name=sight_poc_lakehouse_job`
  - `run_id=abc123`
  - `message=boom`

Failure signs:

- missing contextual fields
- malformed string body

### 9.2 Component: Alertmanager Compatibility Payload Builder

Commands:

```bash
python - <<'PY'
from sight_poc_dagster.definitions import build_alertmanager_payload
import json
payload = build_alertmanager_payload("sight_poc_lakehouse_job", "abc123", "boom")
print(json.dumps(payload, indent=2))
PY
```

Expected success:

- payload is a list containing one alert object
- labels include:
  - `alertname=DagsterJobFailed`
  - `severity=critical`
  - `job_name=sight_poc_lakehouse_job`
  - `run_id=abc123`
- annotations include:
  - `summary=Dagster job failed: sight_poc_lakehouse_job`
  - `description=boom`

Failure signs:

- payload not JSON-serializable
- missing labels required by downstream Alertmanager routing

### 9.3 Component: Alert Delivery Behavior When URL Is Missing

Commands:

```bash
python - <<'PY'
import os
from sight_poc_dagster.definitions import alertmanager_job_failure_alert
print("Sensor loaded:", alertmanager_job_failure_alert.name)
print("ALERTMANAGER_URL:", os.getenv("ALERTMANAGER_URL", ""))
PY
```

Expected success:

- sensor loads
- absence of `ALERTMANAGER_URL` is acceptable for local development

Failure signs:

- import or sensor construction errors

## 10. Container Build Validation

### 10.1 Component: Docker Image Build

Commands:

```bash
docker build -t sight-poc-dagster:local .
docker image inspect sight-poc-dagster:local --format '{{.Id}}'
```

Expected success:

- image builds successfully
- `docker image inspect` prints an image id

Failure signs:

- build context issues
- missing files in the image build
- dependency installation failures inside the Docker build

### 10.2 Component: Container Runtime Smoke Test

Commands:

```bash
docker run --rm sight-poc-dagster:local python -m compileall sight_poc_dagster
docker run --rm sight-poc-dagster:local python -c "from sight_poc_dagster.definitions import defs; print(defs)"
```

Expected success:

- both commands exit `0`

Failure signs:

- package import failure inside container
- wrong working directory or missing installed package

## 11. Docker Hub Release Workflow Validation

### 11.1 Component: GitHub Actions Publish Workflow

File:

- [ci.yml](/home/branford-t-gbieor/Desktop/gbieor/applications/exercises/sight-poc/sight-poc-data/.github/workflows/ci.yml)

Required GitHub configuration:

- secret `DOCKERHUB_USERNAME`
- secret `DOCKERHUB_TOKEN`
- variable `DOCKERHUB_REPOSITORY`
- secret `SIGHT_POC_INFRA_REPO_TOKEN`

Recommended sample value:

```text
DOCKERHUB_REPOSITORY=<your-dockerhub-user>/sight-poc-dagster
```

Trigger paths:

- push to `main`
- push a tag like `v0.1.0`

Expected success:

- `main` pushes publish `latest`
- version tag pushes publish the same immutable tag
- version tag pushes notify `sight-poc-infra` for promotion

Failure signs:

- authentication failures during `docker/login-action`
- missing GitHub secret or variable
- Docker Hub repository typo

### 11.2 Component: Manual Local Docker Hub Push

Commands:

```bash
export DOCKERHUB_REPOSITORY=<your-dockerhub-user>/sight-poc-dagster
export IMAGE_TAG=v0.1.0

docker login
docker build -t ${DOCKERHUB_REPOSITORY}:${IMAGE_TAG} .
docker push ${DOCKERHUB_REPOSITORY}:${IMAGE_TAG}
```

Expected success:

- pushed image tag is visible on Docker Hub

Failure signs:

- unauthorized push
- repository not found
- rate limit or connectivity issues

## 12. Integration Handoff to Infra

### 12.1 Component: Image Tag Handoff

After publishing a version tag, `sight-poc-data` dispatches a promotion event and `sight-poc-infra` updates GitOps values automatically.

Sample value:

```yaml
image:
  repository: docker.io/<your-dockerhub-user>/sight-poc-dagster
  tag: v0.1.0
```

File to update in infra:

- [values-gitops.yaml](/home/branford-t-gbieor/Desktop/gbieor/applications/exercises/sight-poc/sight-poc-infra/helm/dagster/values-gitops.yaml)

Expected success:

- `sight-poc-infra` updates `helm/dagster/values-gitops.yaml`
- Argo CD in `sight-poc-infra` can reconcile the new image tag

Failure signs:

- image tag exists in Docker Hub but Kubernetes cannot pull it
- wrong registry/repository string in infra values

## 13. S3-Backed Runtime Validation

### 13.1 Component: Bucket-Backed Lake Layout

This requires the S3 bucket and Dagster IRSA role from `sight-poc-infra`.

Commands:

```bash
export SIGHT_POC_DATA_LAKE_BUCKET=<bucket-from-sight-poc-infra>
export SIGHT_POC_DATA_LAKE_PREFIX=sight-poc
unset SIGHT_POC_DATA_LAKE_ROOT
```

Then rerun the success-path execution from section `5.1` in an environment that has AWS credentials or the expected IRSA role.

Expected success:

- raw, staging, and curated outputs are written to:
  - `s3://<bucket>/sight-poc/raw/...`
  - `s3://<bucket>/sight-poc/staging/...`
  - `s3://<bucket>/sight-poc/curated/...`

Failure signs:

- `AccessDenied` from S3
- bucket not found
- wrong service account IAM role in-cluster

## 14. Completion Criteria

You can treat data-repo validation as complete when all of the following are true:

- package installs successfully
- compile and test steps pass locally
- the layered lakehouse job succeeds on the success path and fails on the intentional failure path
- the daily schedule and recovery sensor behave correctly against missing and existing curated partitions
- dbt project files and environment wiring are valid
- optional Alertmanager compatibility payload shape is correct
- Docker image builds locally
- Docker Hub push works manually or via GitHub Actions
- published image coordinates are promoted into `sight-poc-infra`
