from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule


# Default settings applied to all tasks
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Define the DAG
with DAG(
    dag_id='map_reduce_example_dag',
    default_args=default_args,
    description='A sample map-reduce style Airflow DAG with logging, branching, sensors, XCom, etc.',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['example', 'map-reduce', 'logging', 'sensor', 'branching'],
) as dag:

    # ----------------------------------------------------------------
    # 1) START - typically a dummy or empty operator
    # ----------------------------------------------------------------
    start = EmptyOperator(
        task_id='start'
    )

    # ----------------------------------------------------------------
    # 2) SENSOR EXAMPLE
    #    Wait for an external task/DAG to finish (common pattern)
    # ----------------------------------------------------------------
    wait_for_external_data = ExternalTaskSensor(
        task_id='wait_for_external_data',
        external_dag_id='some_other_dag',
        external_task_id='some_task_id',
        mode='poke',  # or 'reschedule'
        poke_interval=60 * 5,  # check every 5 minutes
        timeout=60 * 60 * 2,   # 2-hour timeout
    )

    # ----------------------------------------------------------------
    # 3) TASK GROUP: PARALLEL EXTRACTION (MAP)
    #    Example of extracting or processing data from multiple sources in parallel
    # ----------------------------------------------------------------
    with TaskGroup(group_id='map_extraction_group') as map_extraction_group:

        @task(task_id='extract_source_1', provide_context=True)
        def extract_source_1(**kwargs):
            """Extract data from Source 1. Possibly reading from a DB or an API."""
            # Log some info
            print("Starting extraction from Source 1...")

            # (Pseudo) complicated logic
            # ...
            extracted_data = {'source_name': 'source1', 'rows': 1234}

            # Print or log results
            print(f"Extraction from Source 1 done: {extracted_data}")

            # Return data so it can be stored in XCom
            return extracted_data

        @task(task_id='extract_source_2', provide_context=True)
        def extract_source_2(**kwargs):
            """Extract data from Source 2."""
            print("Starting extraction from Source 2...")
            # (Pseudo) complicated logic
            extracted_data = {'source_name': 'source2', 'rows': 5678}
            print(f"Extraction from Source 2 done: {extracted_data}")
            return extracted_data

        @task(task_id='extract_source_3', provide_context=True)
        def extract_source_3(**kwargs):
            """Extract data from Source 3."""
            print("Starting extraction from Source 3...")
            # (Pseudo) complicated logic
            extracted_data = {'source_name': 'source3', 'rows': 9012}
            print(f"Extraction from Source 3 done: {extracted_data}")
            return extracted_data

        # You can add more sources in parallel as needed
        extract_1 = extract_source_1()
        extract_2 = extract_source_2()
        extract_3 = extract_source_3()

    # ----------------------------------------------------------------
    # 4) BRANCHING EXAMPLE
    #    Decide which path to take based on dynamic logic
    # ----------------------------------------------------------------
    def decide_branch(**kwargs):
        """
        Simple logic that branches depending on combined row counts from prior tasks.
        """
        # Pull XCom data from tasks in the map_extraction_group
        ti = kwargs['ti']
        data_1 = ti.xcom_pull(key='return_value', task_ids='map_extraction_group.extract_source_1')
        data_2 = ti.xcom_pull(key='return_value', task_ids='map_extraction_group.extract_source_2')
        data_3 = ti.xcom_pull(key='return_value', task_ids='map_extraction_group.extract_source_3')

        total_rows = 0
        for d in [data_1, data_2, data_3]:
            if d is not None and 'rows' in d:
                total_rows += d['rows']

        # Example: If total > 10000, use the 'large_data_path', else 'small_data_path'
        if total_rows > 10000:
            print(f"Total rows is {total_rows}, branching to LARGE data path.")
            return 'large_data_path'
        else:
            print(f"Total rows is {total_rows}, branching to SMALL data path.")
            return 'small_data_path'

    branch_task = BranchPythonOperator(
        task_id='branch_decision',
        python_callable=decide_branch,
        provide_context=True
    )

    large_data_path = EmptyOperator(task_id='large_data_path')
    small_data_path = EmptyOperator(task_id='small_data_path')

    # ----------------------------------------------------------------
    # 5) REDUCE TASK (AGGREGATION)
    #    Combine or reduce the mapped outputs after branching
    # ----------------------------------------------------------------
    @task(task_id='reduce_aggregations', trigger_rule=TriggerRule.NONE_FAILED)
    def reduce_aggregations(**kwargs):
        """
        Example reduce step - merges data from previous tasks, e.g. deduplicate or join.
        """
        print("Starting reduce step with complicated logic...")

        ti = kwargs['ti']
        # For demonstration, let's read from both path possibilities:
        data_1 = ti.xcom_pull(key='return_value', task_ids='map_extraction_group.extract_source_1')
        data_2 = ti.xcom_pull(key='return_value', task_ids='map_extraction_group.extract_source_2')
        data_3 = ti.xcom_pull(key='return_value', task_ids='map_extraction_group.extract_source_3')

        # Possibly do logic that differs if we branched to large or small
        # (We can also do branching in code based on XCom variables)
        combined = []
        for d in [data_1, data_2, data_3]:
            if d:
                combined.append(d)

        # Example final reduced data
        final_output = {
            'combined_sources': len(combined),
            'sample_data': combined
        }

        print(f"Reduce step completed. Aggregated data: {final_output}")
        # Return final data
        return final_output

    reduce_task = reduce_aggregations()

    # ----------------------------------------------------------------
    # 6) END - final step or empty operator
    # ----------------------------------------------------------------
    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_DONE  # Ensures it runs even if some tasks skip
    )

    # ----------------------------------------------------------------
    # DAG SEQUENCE / DEPENDENCIES
    # ----------------------------------------------------------------

    # 1) Start -> wait for external -> map group -> branch -> [ large / small path ] -> reduce -> end
    start >> wait_for_external_data >> map_extraction_group
    map_extraction_group >> branch_task >> [large_data_path, small_data_path] >> reduce_task >> end