FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
USER airflow

COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
COPY pipeline /opt/airflow/pipeline
