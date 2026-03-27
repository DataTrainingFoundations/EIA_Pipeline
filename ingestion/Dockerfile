FROM python:3.11-slim
 
WORKDIR /app
 
# snowflake-snowpark-python pulls in pyarrow, pandas, and the Snowflake
# connector — everything needed to write DataFrames directly to Snowflake.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
 
COPY src /app/src
CMD ["python", "-m", "src.fetch_eia"]
