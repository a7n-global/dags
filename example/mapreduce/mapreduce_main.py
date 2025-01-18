from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='map_reduce_example_dag',
    default_args=default_args,
    description='A map-reduce style Airflow DAG that waits for dummy_external_dag to finish.',
    schedule_interval='@daily',
    start_date=datetime(2023,1,1),
    catchup=False,
    tags=['map-reduce', 'external-sensor'],
) as dag:

    start = EmptyOperator(task_id='start')

    # ----------------------------------------------------------------
    # EXTERNAL TASK SENSOR
    # Wait for 'dummy_external_dag' and specifically the 'end' task 
    # to succeed (for the same execution date).
    # ----------------------------------------------------------------
    wait_for_dummy_external = ExternalTaskSensor(
        task_id='wait_for_dummy_external',
        external_dag_id='dummy_external_dag',    # DAG ID in the external DAG
        external_task_id='end',                  # Wait for the 'end' task to be successful
        poke_interval=60,                        # Check every 60 seconds
        timeout=60 * 60,                         # 1 hour timeout
        mode='poke',                             # or 'reschedule'
    )

    # ----------------------------------------------------------------
    # MAP (PARALLEL EXTRACTION) TASK GROUP
    # (Same as your map logic; shown briefly for completeness)
    # ----------------------------------------------------------------
    with TaskGroup(group_id='map_extraction_group') as map_extraction_group:

        @task(task_id='extract_source_1')
        def extract_source_1():
            print("Extracting source 1 ...")
            return {'source': 'source1', 'row_count': 1000}

        @task(task_id='extract_source_2')
        def extract_source_2():
            print("Extracting source 2 ...")
            return {'source': 'source2', 'row_count': 5000}

        @task(task_id='extract_source_3')
        def extract_source_3():
            print("Extracting source 3 ...")
            return {'source': 'source3', 'row_count': 7000}

        t1 = extract_source_1()
        t2 = extract_source_2()
        t3 = extract_source_3()

    # ----------------------------------------------------------------
    # BRANCH LOGIC
    # ----------------------------------------------------------------
    def decide_branch(**kwargs):
        ti = kwargs['ti']
        d1 = ti.xcom_pull(key='return_value', task_ids='map_extraction_group.extract_source_1')
        d2 = ti.xcom_pull(key='return_value', task_ids='map_extraction_group.extract_source_2')
        d3 = ti.xcom_pull(key='return_value', task_ids='map_extraction_group.extract_source_3')

        total = (d1['row_count'] if d1 else 0) + (d2['row_count'] if d2 else 0) + (d3['row_count'] if d3 else 0)

        return 'large_data_path' if total > 10000 else 'small_data_path'

    branch_op = BranchPythonOperator(
        task_id='branch_decision',
        python_callable=decide_branch,
        provide_context=True
    )

    large_data_path = EmptyOperator(task_id='large_data_path')
    small_data_path = EmptyOperator(task_id='small_data_path')

    # ----------------------------------------------------------------
    # REDUCE TASK
    # ----------------------------------------------------------------
    @task(task_id='reduce_aggregations', trigger_rule=TriggerRule.NONE_FAILED)
    def reduce_aggregations(**kwargs):
        ti = kwargs['ti']
        d1 = ti.xcom_pull(task_ids='map_extraction_group.extract_source_1')
        d2 = ti.xcom_pull(task_ids='map_extraction_group.extract_source_2')
        d3 = ti.xcom_pull(task_ids='map_extraction_group.extract_source_3')

        combined = [d for d in [d1, d2, d3] if d]
        total_rows = sum(d['row_count'] for d in combined)

        result = {
            'source_count': len(combined),
            'total_rows': total_rows,
            'sources': combined,
        }
        print(f"Reduced data: {result}")
        return result

    final_reduce = reduce_aggregations()

    end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.ALL_DONE)

    # DAG flow:
    start >> wait_for_dummy_external >> map_extraction_group
    map_extraction_group >> branch_op >> [large_data_path, small_data_path] >> final_reduce >> end