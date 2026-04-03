# Hydrosat Data

![Dagster](https://img.shields.io/badge/Dagster-Orchestration-5C6AC4?logo=dagster&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Image%20Build-2496ED?logo=docker&logoColor=white)
![Docker%20Hub](https://img.shields.io/badge/Docker%20Hub-Release-1D63ED?logo=docker&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-App%20CI-2088FF?logo=githubactions&logoColor=white)

Dagster application repository for the Hydrosat platform.

This repo owns:

- Dagster jobs, ops, and sensors
- unit tests
- the Docker image build
- application CI

The infrastructure, Helm chart, Argo CD applications, and environment promotion flow live in the separate `hydrosat-infra` repository.

## Layout

| Path | Purpose |
| --- | --- |
| `hydrosat_dagster/` | Dagster package |
| `tests/` | Application tests |
| `Dockerfile` | Runtime image build |
| `pyproject.toml` | Python package metadata |
| `.github/workflows/ci.yml` | Application CI workflow |

## Local Development

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

## Container Build

```bash
docker build -t hydrosat-dagster:local .
```

## Image Publishing

Image publishing is handled directly in the application CI workflow. Configure:

- `DOCKERHUB_USERNAME`
- `DOCKERHUB_TOKEN`
- `DOCKERHUB_REPOSITORY`

Publish flow:

1. pushes from `main` publish `latest`
2. pushes of tags like `v0.1.0` publish immutable version tags
3. pull requests and non-release branches still build the image but do not push it
4. the promoted image tag is then handed to the separate infra repo for GitOps rollout
