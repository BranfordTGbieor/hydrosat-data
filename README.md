# Sight PoC Data

![Dagster](https://img.shields.io/badge/Dagster-Orchestration-5C6AC4?logo=dagster&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Image%20Build-2496ED?logo=docker&logoColor=white)
![Docker%20Hub](https://img.shields.io/badge/Docker%20Hub-Release-1D63ED?logo=docker&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-App%20CI-2088FF?logo=githubactions&logoColor=white)

Data application repository for the Sight PoC platform.

This repo still runs on Dagster today. The portfolio backlog includes replacing that orchestration layer with Airflow, but the current baseline focuses first on runtime/tooling consistency and reusable delivery hygiene.

This repo owns:

- Dagster jobs, ops, and sensors
- Dagster schedules and recovery sensors
- unit tests
- the Docker image build
- application CI

The infrastructure, Helm chart, Argo CD applications, and environment promotion flow live in the separate infra repository.

## Layout

| Path | Purpose |
| --- | --- |
| `sight_poc_dagster/` | Dagster package |
| `tests/` | Application tests |
| `Dockerfile` | Runtime image build |
| `pyproject.toml` | Python package metadata |
| `.github/workflows/ci.yml` | Application CI workflow |

## Sample Pipeline

The current sample project is a small lakehouse-style pipeline orchestrated by Dagster:

1. Python extraction writes raw satellite observation records
2. dbt transforms clean and enrich those records into `staging`
3. dbt produces curated tile-level analytical summaries in `curated`
4. a daily Dagster schedule targets the current UTC partition
5. a recovery sensor requests the same partition if the curated layer is still missing

The filesystem layout mirrors the intended S3 layout we will keep using later:

- `raw/satellite_observations/ingest_date=YYYY-MM-DD/...`
- `staging/satellite_observations/ingest_date=YYYY-MM-DD/...`
- `curated/tile_summary/partition_date=YYYY-MM-DD/...`

For local validation, those layers are written under `SIGHT_POC_DATA_LAKE_ROOT`, which defaults to `/tmp/sight-poc-data-lake`.
When `SIGHT_POC_DATA_LAKE_BUCKET` is set, the same layer layout is written to S3 by using `SIGHT_POC_DATA_LAKE_PREFIX` as the top-level prefix.

## Architecture

<img src="utils/images/data-pipeline.png" alt="Sight PoC data pipeline diagram" width="1100" />

Source: [utils/mermaid/data-pipeline.mmd](./utils/mermaid/data-pipeline.mmd)

Storage modes:

- local development uses `SIGHT_POC_DATA_LAKE_ROOT`
- cluster execution uses `SIGHT_POC_DATA_LAKE_BUCKET`
- both modes preserve the same raw, staging, and curated layer layout

dbt execution model:

- Dagster writes `raw`
- Dagster invokes the bundled dbt project with `dbt-duckdb`
- dbt reads `raw`, materializes transformations in DuckDB, and exports `staging` and `curated` back into the lake layout

Operational behavior:

- `daily_lakehouse_schedule` runs the pipeline at `03:00 UTC`
- `lakehouse_partition_recovery_sensor` checks whether `curated/tile_summary/partition_date=<today>/` already exists
- the recovery sensor only requests a run when the expected curated partition is absent
- the code location still includes an optional Alertmanager-compatible failure sensor, but the default deployed alerting path for this exercise is Grafana Cloud alerting managed from `sight-poc-infra`

## Local Development

```bash
uv sync --all-extras
uv run ruff format --check .
uv run ruff check .
uv run pytest
```

## Running the Sample Job

Minimal local run config:

```yaml
ops:
  extract_satellite_observations:
    config:
      batch_date: "2026-04-07"
      should_fail: false
```

Use `should_fail: true` to simulate a controlled Dagster job failure for observability and alert-validation work.

## Container Build

```bash
docker build -t sight-poc-dagster:local .
```

## Image Publishing

Image publishing is handled directly in the application CI workflow. Configure:

- `DOCKERHUB_USERNAME`
- `DOCKERHUB_TOKEN`
- `DOCKERHUB_REPOSITORY`
- `SIGHT_POC_INFRA_REPO_TOKEN`

Publish flow:

1. pushes from `main` publish `latest`
2. pushes of tags like `v0.1.0` publish immutable version tags
3. release-tag pushes notify `sight-poc-infra` to promote that exact tag into GitOps values
4. pull requests and non-release branches still build the image but do not push it

## Submission Hygiene

- Python 3.12 is the supported runtime across `pyproject.toml`, Docker, CI, and local development.
- `uv`, `ruff`, `pytest`, and coverage are part of the default engineering baseline.
- `.editorconfig` and `pre-commit` are included so formatting and linting expectations are explicit.

## AI Assistance Disclosure

This repository was authored and manually reviewed by Branford T. Gbieor with AI assistance used for drafting, refactoring, and documentation support. The design choices, validation steps, and final committed changes are intentionally reviewed and owned by the author.
