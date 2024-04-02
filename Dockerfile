FROM python:3.10.12-slim

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONUNBUFFERED 1

ENV AIRFLOW_HOME /app/airflow

WORKDIR $AIRFLOW_HOME

COPY scripts scripts

RUN chmod +x scripts/entrypoint.sh

COPY pyproject.toml poetry.lock ./

RUN pip3 install --upgrade --no-cache-dir pip \
  && pip3 install poetry \
  && poetry install --only main
