# Hydrosat Data Validation Runbook

This runbook validates the `hydrosat-data` repository from local code quality through Docker image publishing and Dagster runtime behavior.

Use this document when you want to prove that:

- the Dagster package installs cleanly
- unit tests cover the success and failure paths
- the demo job behaves as expected
- the Alertmanager payload shape is correct
- the Docker image builds locally
- the Docker Hub release workflow is ready to publish
- the built image can be consumed by `hydrosat-infra`

This runbook assumes:

- repo root is `hydrosat-data/`
- Python 3.12 is available
- Docker is available locally
- optionally, Docker Hub credentials are available

## 1. Validation Order

Run these sections in order:

1. Local environment setup
2. Static package validation
3. Unit test validation
4. Direct Dagster job execution
5. Alert payload validation
6. Container build validation
7. Docker Hub release workflow validation
8. Integration handoff validation for `hydrosat-infra`

## 2. Local Environment Setup

### 2.1 Component: Python Environment

Commands:

```bash
cd /home/branford-t-gbieor/Desktop/gbieor/applications/exercises/hydrosat/hydrosat-data
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e ".[dev]"
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
python -m compileall hydrosat_dagster tests
python -c "from hydrosat_dagster.definitions import defs; print(defs)"
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

- [hydrosat_dagster/definitions.py](/home/branford-t-gbieor/Desktop/gbieor/applications/exercises/hydrosat/hydrosat-data/hydrosat_dagster/definitions.py)
- [tests/test_definitions.py](/home/branford-t-gbieor/Desktop/gbieor/applications/exercises/hydrosat/hydrosat-data/tests/test_definitions.py)

Commands:

```bash
pytest -q
```

Expected success:

- all tests pass
- success-path test confirms `hydrosat_demo_job` returns `success=True`
- failure-path test confirms `hydrosat_demo_job` returns `success=False`
- payload-format tests confirm the Alertmanager body contains the expected labels and annotations

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
    "extract_satellite_window": {
      "config": {
        "should_fail": false
      }
    }
  }
}
```

Commands:

```bash
python - <<'PY'
from hydrosat_dagster.definitions import hydrosat_demo_job

run_config = {
    "ops": {
        "extract_satellite_window": {
            "config": {"should_fail": False}
        }
    }
}

result = hydrosat_demo_job.execute_in_process(run_config=run_config, raise_on_error=False)
print("success=", result.success)
PY
```

Expected success:

- terminal prints `success= True`
- logs show a dummy ingest payload and simulated warehouse load

Failure signs:

- result is `False`
- unexpected exception in `extract_satellite_window` or `load_into_stub_warehouse`

### 5.2 Component: Failure Path

Sample run config:

```json
{
  "ops": {
    "extract_satellite_window": {
      "config": {
        "should_fail": true
      }
    }
  }
}
```

Commands:

```bash
python - <<'PY'
from hydrosat_dagster.definitions import hydrosat_demo_job

run_config = {
    "ops": {
        "extract_satellite_window": {
            "config": {"should_fail": True}
        }
    }
}

result = hydrosat_demo_job.execute_in_process(run_config=run_config, raise_on_error=False)
print("success=", result.success)
PY
```

Expected success:

- terminal prints `success= False`
- failure reason matches `Intentional failure to validate run-failure alerting.`

Failure signs:

- result is `True`
- failure occurs for the wrong reason

## 6. Alert Payload Validation

### 6.1 Component: Failure Message Formatter

Commands:

```bash
python - <<'PY'
from hydrosat_dagster.definitions import build_failure_message
print(build_failure_message("hydrosat_demo_job", "abc123", "boom"))
PY
```

Expected success:

- output includes:
  - `job_name=hydrosat_demo_job`
  - `run_id=abc123`
  - `message=boom`

Failure signs:

- missing contextual fields
- malformed string body

### 6.2 Component: Alertmanager Payload Builder

Commands:

```bash
python - <<'PY'
from hydrosat_dagster.definitions import build_alertmanager_payload
import json
payload = build_alertmanager_payload("hydrosat_demo_job", "abc123", "boom")
print(json.dumps(payload, indent=2))
PY
```

Expected success:

- payload is a list containing one alert object
- labels include:
  - `alertname=DagsterJobFailed`
  - `severity=critical`
  - `job_name=hydrosat_demo_job`
  - `run_id=abc123`
- annotations include:
  - `summary=Dagster job failed: hydrosat_demo_job`
  - `description=boom`

Failure signs:

- payload not JSON-serializable
- missing labels required by Alertmanager routing

### 6.3 Component: Alert Delivery Behavior When URL Is Missing

Commands:

```bash
python - <<'PY'
import os
from hydrosat_dagster.definitions import alertmanager_job_failure_alert
print("Sensor loaded:", alertmanager_job_failure_alert.name)
print("ALERTMANAGER_URL:", os.getenv("ALERTMANAGER_URL", ""))
PY
```

Expected success:

- sensor loads
- absence of `ALERTMANAGER_URL` is acceptable for local development

Failure signs:

- import or sensor construction errors

## 7. Container Build Validation

### 7.1 Component: Docker Image Build

Commands:

```bash
docker build -t hydrosat-dagster:local .
docker image inspect hydrosat-dagster:local --format '{{.Id}}'
```

Expected success:

- image builds successfully
- `docker image inspect` prints an image id

Failure signs:

- build context issues
- missing files in the image build
- dependency installation failures inside the Docker build

### 7.2 Component: Container Runtime Smoke Test

Commands:

```bash
docker run --rm hydrosat-dagster:local python -m compileall hydrosat_dagster
docker run --rm hydrosat-dagster:local python -c "from hydrosat_dagster.definitions import defs; print(defs)"
```

Expected success:

- both commands exit `0`

Failure signs:

- package import failure inside container
- wrong working directory or missing installed package

## 8. Docker Hub Release Workflow Validation

### 8.1 Component: GitHub Actions Release Workflow

File:

- [release.yml](/home/branford-t-gbieor/Desktop/gbieor/applications/exercises/hydrosat/hydrosat-data/.github/workflows/release.yml)

Required GitHub configuration:

- secret `DOCKERHUB_USERNAME`
- secret `DOCKERHUB_TOKEN`
- variable `DOCKERHUB_REPOSITORY`

Recommended sample value:

```text
DOCKERHUB_REPOSITORY=<your-dockerhub-user>/hydrosat-dagster
```

Trigger paths:

- push a tag like `v0.1.0`
- or run `workflow_dispatch`

Expected success:

- workflow checks out code
- logs in to Docker Hub
- builds image from `Dockerfile`
- pushes `${DOCKERHUB_REPOSITORY}:v0.1.0` on tag push

Failure signs:

- authentication failures during `docker/login-action`
- missing GitHub secret or variable
- Docker Hub repository typo

### 8.2 Component: Manual Local Docker Hub Push

Commands:

```bash
export DOCKERHUB_REPOSITORY=<your-dockerhub-user>/hydrosat-dagster
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

## 9. Integration Handoff to Infra

### 9.1 Component: Image Tag Handoff

After publishing, copy the promoted image coordinates into `hydrosat-infra`.

Sample value:

```yaml
image:
  repository: docker.io/<your-dockerhub-user>/hydrosat-dagster
  tag: v0.1.0
```

File to update in infra:

- [values-gitops.yaml](/home/branford-t-gbieor/Desktop/gbieor/applications/exercises/hydrosat/hydrosat-infra/helm/dagster/values-gitops.yaml)

Expected success:

- Argo CD in `hydrosat-infra` can reconcile the new image tag

Failure signs:

- image tag exists in Docker Hub but Kubernetes cannot pull it
- wrong registry/repository string in infra values

## 10. Completion Criteria

You can treat data-repo validation as complete when all of the following are true:

- package installs successfully
- compile and test steps pass locally
- demo job succeeds on the success path and fails on the intentional failure path
- Alertmanager payload shape is correct
- Docker image builds locally
- Docker Hub push works manually or via GitHub Actions
- published image coordinates are ready to hand off to `hydrosat-infra`
