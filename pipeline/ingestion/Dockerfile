FROM python:3.11-slim

WORKDIR /app

COPY pipeline/ingestion/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline /app/pipeline
CMD ["python", "-m", "pipeline.ingestion"]
