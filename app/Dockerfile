# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

WORKDIR /app

COPY app/requirements.txt ./requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt

COPY app/data_access.py .
COPY app/data_access_shared.py .
COPY app/data_access_summary.py .
COPY app/data_access_generation.py .
COPY app/data_access_demand.py .
COPY app/data_access_sales.py .
COPY app/data_access_operational.py .
COPY app/data_access_operational_sales.py .
COPY app/ui_utils.py .
COPY app/respondent_geo.py .
COPY app/streamlit_app.py .
COPY app/pages/ ./pages/
COPY pipeline /app/pipeline

CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
