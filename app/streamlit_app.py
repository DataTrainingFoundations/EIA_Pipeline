import os
import pandas as pd
import psycopg2
import streamlit as st

st.title("EIA Analytics Skeleton")

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    dbname=os.getenv("POSTGRES_DB", "platform"),
    user=os.getenv("POSTGRES_USER", "platform"),
    password=os.getenv("POSTGRES_PASSWORD", "platform"),
)

st.write("Connected to Postgres")

query = "select now() as server_time"
df = pd.read_sql(query, conn)
st.dataframe(df)