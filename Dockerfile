FROM python:3.12.9-slim-bookworm

ARG UV_VERSION=0.6.17

ENV PYTHONUNBUFFERED=1
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV HOME=/opt/dagster
ENV XDG_CACHE_HOME=/opt/dagster/.cache
ENV UV_SYSTEM_PYTHON=1

WORKDIR /opt/dagster/app

RUN python -m pip install --no-cache-dir "uv==${UV_VERSION}"

COPY . /opt/dagster/app

RUN uv pip install --system --no-cache-dir . \
    && addgroup --system dagster \
    && adduser --system --uid 10001 --ingroup dagster dagster \
    && mkdir -p "${DAGSTER_HOME}" "${HOME}" "${XDG_CACHE_HOME}" \
    && python -c "import duckdb; con = duckdb.connect('/tmp/sight-poc-bootstrap.duckdb'); [con.install_extension(ext) or con.load_extension(ext) for ext in ('httpfs', 'parquet', 'json')]; con.close()" \
    && rm -f /tmp/sight-poc-bootstrap.duckdb \
    && chown -R dagster:dagster /opt/dagster

USER dagster

EXPOSE 3000 4000
