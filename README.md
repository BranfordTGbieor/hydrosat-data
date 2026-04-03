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

## Sample Pipeline

The current sample project is a small lakehouse-style pipeline orchestrated by Dagster:

1. Python extraction writes raw satellite observation records
2. a staging step cleans and enriches those records
3. a curated step produces tile-level analytical summaries

The filesystem layout mirrors the intended S3 layout we will keep using later:

- `raw/satellite_observations/ingest_date=YYYY-MM-DD/...`
- `staging/satellite_observations/ingest_date=YYYY-MM-DD/...`
- `curated/tile_summary/partition_date=YYYY-MM-DD/...`

For local validation, those layers are written under `HYDROSAT_DATA_LAKE_ROOT`, which defaults to `/tmp/hydrosat-data-lake`.

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
- `HYDROSAT_INFRA_REPO_TOKEN`

Publish flow:

1. pushes from `main` publish `latest`
2. pushes of tags like `v0.1.0` publish immutable version tags
3. release-tag pushes notify `hydrosat-infra` to promote that exact tag into GitOps values
4. pull requests and non-release branches still build the image but do not push it
