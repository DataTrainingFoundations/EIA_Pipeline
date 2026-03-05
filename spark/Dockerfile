FROM python:3.11-slim

WORKDIR /opt/spark-app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY jobs /opt/spark-app/jobs