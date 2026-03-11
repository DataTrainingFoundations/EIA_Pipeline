FROM apache/airflow:2.9.3-python3.8

USER root
RUN apt-get update && apt-get install -y curl default-jre-headless procps && rm -rf /var/lib/apt/lists/*
COPY scripts/bootstrap.sh /opt/airflow/scripts/bootstrap.sh
RUN chmod +x /opt/airflow/scripts/bootstrap.sh
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
