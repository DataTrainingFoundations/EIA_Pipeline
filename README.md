## Run

Create an env file.

cp .env.example .env

Start the platform.

docker compose up -d --build

Open UIs.

Airflow http://localhost:28080  
Airflow login: `admin` / `admin`  
MinIO http://localhost:29001  
MinIO login: `minioadmin` / `minioadmin123`  
Spark master UI http://localhost:28088  
Kafka broker http://localhost:29092  
Kafka from containers: `kafka:9092`  
Kafka from host machine: `localhost:29092`  
Postgres http://localhost:25432  
Streamlit http://localhost:28501
