## Run

Create an env file.

cp .env.example .env

create database and schema on snowflake

in snowflake sql:
    CREATE DATABASE EIA_PIPELINE;
    CREATE SCHEMA RAW;
    CREATE SCHEMA SILVER;

make sure to fill out new snowflake variables

Start the platform.

docker compose up -d --build

Open UIs.

Airflow http://localhost:28080  
Airflow login: `admin` / `admin`
login for airflow is broken, once its up run this command in terminal:
    docker compose exec airflow airflow users reset-password --username admin --password admin
Spark master UI http://localhost:28088  
Postgres http://localhost:25432  
Streamlit http://localhost:28501
