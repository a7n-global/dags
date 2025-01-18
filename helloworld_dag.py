from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="hello_world_test",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
    catchup=False
) as dag:
    EmptyOperator(task_id="dummy")
