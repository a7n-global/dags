from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'test',
    'depends_on_past': False,
}

with DAG(
    dag_id='log_fetch_test_dag',
    default_args=default_args,
    description='DAG to test log fetching with 3 tasks each printing 100 lines',
    schedule_interval='@once',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task 1: Print 100 lines
    t1 = BashOperator(
        task_id='print_100_lines_task1',
        bash_command='for i in $(seq 1 100); do echo "Task 1 line $i"; done'
    )

    # Task 2: Print 100 lines
    t2 = BashOperator(
        task_id='print_100_lines_task2',
        bash_command='for i in $(seq 1 100); do echo "Task 2 line $i"; done'
    )

    # Task 3: Print 100 lines
    t3 = BashOperator(
        task_id='print_100_lines_task3',
        bash_command='for i in $(seq 1 100); do echo "Task 3 line $i"; done'
    )

    # Set the order: t1 -> t2 -> t3
    t1 >> t2 >> t3
