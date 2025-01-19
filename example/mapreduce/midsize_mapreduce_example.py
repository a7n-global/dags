from datetime import datetime, timedelta
import time
import random

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


# ----------------------------------------------------------
# CONFIGURE HOW MANY PARALLEL MAPPER TASKS YOU WANT
# ----------------------------------------------------------
NUM_MAPPERS = 50  # Increase or decrease to stress test more or less

# Default arguments for tasks
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def dummy_mapper(mapper_id, **kwargs):
    """Simulate some CPU or time usage in parallel."""
    sleep_time = random.randint(5, 15)
    print(f"Mapper {mapper_id} starting. Sleeping for {sleep_time} seconds...")
    time.sleep(sleep_time)
    # Return some dummy data for aggregator
    rows_processed = random.randint(100, 1000)  # pretend to process 100-1000 rows
    print(f"Mapper {mapper_id} finished, processed {rows_processed} rows.")
    return {
        'mapper_id': mapper_id,
        'rows': rows_processed,
    }

def decide_branch(**kwargs):
    """Branch to 'big_data_path' if the total rows across all mappers > 20,000 else 'small_data_path'."""
    ti = kwargs['ti']
    total_rows = 0
    for i in range(NUM_MAPPERS):
        mapper_result = ti.xcom_pull(key='return_value', task_ids=f'map_extraction_group.mapper_{i}')
        if mapper_result:
            total_rows += mapper_result.get('rows', 0)

    print(f"Total rows from {NUM_MAPPERS} mappers = {total_rows}")
    return 'big_data_path' if total_rows > 20000 else 'small_data_path'

def dummy_path_task(path_name, **kwargs):
    """A dummy path task that also does random sleeping to simulate big/small path work."""
    if path_name == 'big_data':
        sleep_time = random.randint(10, 20)
    else:
        sleep_time = random.randint(3, 7)
    print(f"{path_name} path task sleeping for {sleep_time} seconds...")
    time.sleep(sleep_time)
    print(f"{path_name} path task complete.")

def reduce_aggregation(**kwargs):
    """Combine mapper results in a final 'reduce' step."""
    ti = kwargs['ti']
    total_rows = 0
    for i in range(NUM_MAPPERS):
        mapper_result = ti.xcom_pull(key='return_value', task_ids=f'map_extraction_group.mapper_{i}')
        if mapper_result:
            total_rows += mapper_result.get('rows', 0)

    print(f"Reducer sees total_rows={total_rows} from all mappers.")
    return {
        'total_rows_processed': total_rows
    }

with DAG(
    dag_id='midsize_mapreduce_example',
    default_args=default_args,
    description='Map-reduce DAG for stress testing. Generates the file before waiting on it with FileSensor.',
    schedule='@daily',
    catchup=False,
    max_active_tasks=128,
    tags=['stress-test', 'map-reduce', 'parallel'],
) as dag:

    # 1) START
    start = EmptyOperator(task_id='start')

    # 2) PRODUCE THE FILE
    generate_file = BashOperator(
        task_id='generate_input_file',
        bash_command='touch /opt/airflow/shared/big_input_dataset_ready.txt && echo "Data is ready" > /opt/airflow/shared/big_input_dataset_ready.txt'
    )

    # 3) SENSOR - wait for that newly created file to appear
    wait_for_file = FileSensor(
        task_id='wait_for_input_file',
        filepath='/opt/airflow/shared/big_input_dataset_ready.txt',  # Must match what's created above
        poke_interval=30,
        timeout=60 * 60,
        mode='poke',
        fs_conn_id='fs_local',
    )

    # 4) MAP EXTRACTION GROUP - multiple parallel tasks
    with TaskGroup(group_id='map_extraction_group') as map_extraction_group:
        for i in range(NUM_MAPPERS):
            PythonOperator(
                task_id=f'mapper_{i}',
                python_callable=dummy_mapper,
                op_kwargs={'mapper_id': i},
                # queue='default', # specify if you want a particular Celery queue
            )

    # 5) BRANCH
    branch_op = BranchPythonOperator(
        task_id='branch_decision',
        python_callable=decide_branch,
    )

    # 6) BIG PATH / SMALL PATH tasks
    big_data_path = PythonOperator(
        task_id='big_data_path',
        python_callable=dummy_path_task,
        op_kwargs={'path_name': 'big_data'},
    )

    small_data_path = PythonOperator(
        task_id='small_data_path',
        python_callable=dummy_path_task,
        op_kwargs={'path_name': 'small_data'},
    )

    # 7) REDUCE (AGGREGATOR)
    reduce_task = PythonOperator(
        task_id='reduce_aggregations',
        python_callable=reduce_aggregation,
        trigger_rule=TriggerRule.NONE_FAILED
    )

    # 8) END - with cleanup
    # garbage cleanup
    cleanup_file = BashOperator(
        task_id='cleanup_indicator_file',
        bash_command='rm -f /opt/airflow/shared/big_input_dataset_ready.txt',
        trigger_rule=TriggerRule.ALL_DONE
    )

    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_DONE
    )

    # DAG FLOW
    start >> generate_file >> wait_for_file >> map_extraction_group
    map_extraction_group >> branch_op >> [big_data_path, small_data_path] >> reduce_task >> cleanup_file >> end