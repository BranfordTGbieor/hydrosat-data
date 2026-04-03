FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
ENV DAGSTER_HOME=/opt/dagster/dagster_home

WORKDIR /opt/dagster/app

COPY . /opt/dagster/app

RUN pip install --no-cache-dir .
RUN addgroup --system dagster \
    && adduser --system --uid 10001 --ingroup dagster dagster \
    && mkdir -p "${DAGSTER_HOME}" \
    && chown -R dagster:dagster /opt/dagster

USER dagster

EXPOSE 3000 4000
