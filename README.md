# Hydrosat Data

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

## Release Direction

The intended next step is to add a release workflow that:

1. builds the image
2. tags it for Docker Hub
3. pushes it to the configured Docker Hub repository
4. hands the promoted image tag to the separate infra repo for GitOps rollout
