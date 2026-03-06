FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src /app/src
CMD ["sh", "-c", "python -m src.fetch_eia & python -m src.publish_kafka"]
