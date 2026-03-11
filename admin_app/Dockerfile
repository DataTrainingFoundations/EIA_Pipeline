FROM python:3.11-slim

WORKDIR /workspace/admin_app

COPY admin_app/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY admin_app /workspace/admin_app
COPY ingestion/src/dataset_registry.yml /workspace/ingestion/src/dataset_registry.yml

CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
