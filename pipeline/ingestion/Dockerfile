# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

WORKDIR /app

COPY pipeline/ingestion/requirements.txt ./requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt

COPY pipeline /app/pipeline
CMD ["python", "-m", "pipeline.ingestion"]
