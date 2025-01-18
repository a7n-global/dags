from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'email': ['user@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'cron_job_concurrency_test',
    default_args=default_args,
    description='A simple example DAG',
    schedule='*/5 * * * * *',
    start_date=datetime(2024, 9, 24),
    catchup=False,
    max_active_runs=1000,
    max_active_tasks=2000,
    dagrun_timeout=timedelta(seconds=20),
) as dag:

    print_date = BashOperator(
        task_id='print_date',
        bash_command='date',
        pool='default_pool',
        pool_slots=1,
    )

    sleep = BashOperator(
        task_id='sleep',
        bash_command='echo "Starting sleep task" && sleep 2 && echo "Sleep task completed"',
        pool='default_pool',
        pool_slots=1,
    )

    end = EmptyOperator(task_id='end')

    print_date >> sleep >> end
