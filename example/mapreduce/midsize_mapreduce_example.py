# Best Practice for MapReduce Job
# This is a mid-size MR job, 50 mappers. Does random sleep and rand number dummy task.
# Feel free to use as template

from datetime import datetime, timedelta
import time
import random

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


# ----------------------------------------------------------
# CONFIGURE HOW MANY PARALLEL MAPPER TASKS YOU WANT
# ----------------------------------------------------------
NUM_MAPPERS = 50  # Increase or decrease to adjust parallel stress

# Default arguments for all tasks
default_args = {
    'owner': 'Infra-Erik',
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
    return {
        'mapper_id': mapper_id,
        'rows': random.randint(100, 1000)  # pretend to process 100-1000 rows
    }


def decide_branch(**kwargs):
    """Example branch: if the total rows processed by mappers exceeds 20,000, go 'big_data_path' else 'small_data_path'."""
    ti = kwargs['ti']
    total = 0
    for i in range(NUM_MAPPERS):
        mapper_result = ti.xcom_pull(key='return_value', task_ids=f'map_extraction_group.mapper_{i}')
        if mapper_result:
            total += mapper_result.get('rows', 0)

    print(f"Total rows from {NUM_MAPPERS} mappers = {total}")
    if total > 20000:
        return 'big_data_path'
    else:
        return 'small_data_path'


def dummy_path_task(path_name, **kwargs):
    """
    A dummy task that also does some random sleeping to simulate big/small path processing.
    """
    sleep_time = random.randint(10, 20) if path_name == 'big_data' else random.randint(3, 7)
    print(f"{path_name} path task sleeping for {sleep_time} seconds...")
    time.sleep(sleep_time)
    print(f"{path_name} path task complete.")


def reduce_aggregation(**kwargs):
    """Combine results from the mappers and the path tasks for a final 'reduce' operation."""
    ti = kwargs['ti']
    total_rows = 0

    # Gather mapper results
    for i in range(NUM_MAPPERS):
        mapper_result = ti.xcom_pull(key='return_value', task_ids=f'map_extraction_group.mapper_{i}')
        if mapper_result:
            total_rows += mapper_result.get('rows', 0)

    # Potentially gather more info from big/small path tasks if needed, but here we just log total_rows
    print(f"Reducer sees total_rows={total_rows} from all mappers.")
    return {
        'total_rows_processed': total_rows
    }


with DAG(
    dag_id='midsize_mapreduce_example',
    default_args=default_args,
    description='A large map-reduce DAG that can generate massive parallel tasks to stress-test an Airflow cluster.',
    schedule_interval='@daily',
    catchup=False,
    concurrency=128,  # Up to 128 tasks at once (requires cluster capacity)
    tags=['stress-test', 'map-reduce', 'parallel'],
) as dag:

    # 1) START
    start = EmptyOperator(task_id='start')

    # 2) SENSOR - wait for a dummy file to exist (replace with your real path if desired)
    wait_for_file = FileSensor(
        task_id='wait_for_input_file',
        filepath='/tmp/big_input_dataset_ready.txt',  # or any path
        poke_interval=30,
        timeout=60 * 60,
        mode='poke'
    )

    # 3) MAP EXTRACTION GROUP - create 50 parallel tasks
    with TaskGroup(group_id='map_extraction_group') as map_extraction_group:
        for i in range(NUM_MAPPERS):
            PythonOperator(
                task_id=f'mapper_{i}',
                python_callable=dummy_mapper,
                op_kwargs={'mapper_id': i},
                # queue='default', # specify if you want a particular Celery queue
            )

    # 4) BRANCH - big vs small path
    branch_op = BranchPythonOperator(
        task_id='branch_decision',
        python_callable=decide_branch,
        provide_context=True
    )

    # 5) BIG PATH / SMALL PATH (dummy tasks)
    big_data_path = PythonOperator(
        task_id='big_data_path',
        python_callable=dummy_path_task,
        op_kwargs={'path_name': 'big_data'},
        provide_context=True
    )

    small_data_path = PythonOperator(
        task_id='small_data_path',
        python_callable=dummy_path_task,
        op_kwargs={'path_name': 'small_data'},
        provide_context=True
    )

    # 6) REDUCE TASK - aggregator
    reduce_task = PythonOperator(
        task_id='reduce_aggregations',
        python_callable=reduce_aggregation,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED  # only run if upstream succeeded
    )

    # 7) END
    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_DONE
    )

    # DAG flow
    start >> wait_for_file >> map_extraction_group
    map_extraction_group >> branch_op >> [big_data_path, small_data_path] >> reduce_task >> end