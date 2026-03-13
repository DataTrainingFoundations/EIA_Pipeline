FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Data access modules
COPY data_access.py .
COPY data_access_shared.py .
COPY data_access_summary.py .
COPY data_access_generation.py .
COPY data_access_demand.py .

# Utilities
COPY ui_utils.py .
COPY respondent_geo.py .

# Main app entry point
COPY streamlit_app.py .

# Dashboard pages
COPY pages/ ./pages/

CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
