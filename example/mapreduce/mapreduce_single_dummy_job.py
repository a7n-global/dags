# This job is used to simulate a one thread job. e.g. a ffmpeg transcoding job, or a autolabel job
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import time

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='dummy_external_dag',
    default_args=default_args,
    description='A dummy external DAG that simulates a job and signals success to the map-reduce DAG.',
    schedule_interval='@daily',
    catchup=False,
    tags=['external', 'dummy-job'],
) as dag:

    start = EmptyOperator(
        task_id='start'
    )

    def simulate_job():
        """
        Simulate some dummy work, like sleeping for a few seconds.
        Replace with real logic if needed.
        """
        print("Dummy job started...")
        time.sleep(10)
        print("Dummy job finished...")

    dummy_task = PythonOperator(
        task_id='dummy_job',
        python_callable=simulate_job
    )

    end = EmptyOperator(
        task_id='end'
    )

    # Flow: start -> dummy_job -> end
    start >> dummy_task >> end