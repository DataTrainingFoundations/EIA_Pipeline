from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="eia_pipeline_skeleton",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args={"retries": 2},
    tags=["eia", "skeleton"],
):
    ingest = BashOperator(
        task_id="ingest_to_kafka",
        bash_command="echo 'Placeholder: trigger ingestion container or run python module'",
    )

    bronze = BashOperator(
        task_id="spark_bronze_streaming",
        bash_command="echo 'Placeholder: spark-submit streaming job on spark-master'",
    )

    silver = BashOperator(
        task_id="spark_silver_batch",
        bash_command="echo 'Placeholder: spark-submit silver job'",
    )

    gold = BashOperator(
        task_id="spark_gold_batch",
        bash_command="echo 'Placeholder: spark-submit gold job'",
    )

    platinum = BashOperator(
        task_id="spark_platinum_batch",
        bash_command="echo 'Placeholder: spark-submit platinum job'",
    )

    ingest >> bronze >> silver >> gold >> platinum