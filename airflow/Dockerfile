# syntax=docker/dockerfile:1.7
FROM apache/airflow:2.9.3-python3.11

ARG AIRFLOW_CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt

USER root
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
USER airflow

COPY airflow/requirements.txt /requirements.txt
COPY airflow/dbt-requirements.txt /dbt-requirements.txt
RUN --mount=type=cache,target=/home/airflow/.cache/pip \
    pip install --constraint "${AIRFLOW_CONSTRAINTS_URL}" -r /requirements.txt
RUN python -m venv /home/airflow/dbt-venv
RUN --mount=type=cache,target=/home/airflow/.cache/pip \
    /home/airflow/dbt-venv/bin/pip install -r /dbt-requirements.txt
ENV PATH="/home/airflow/dbt-venv/bin:${PATH}"
COPY pipeline /opt/airflow/pipeline
COPY warehouse /opt/airflow/warehouse
