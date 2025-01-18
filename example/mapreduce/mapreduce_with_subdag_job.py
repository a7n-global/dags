from datetime import datetime, timedelta
import random

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule


#######################################
# SUBDAG CREATION FUNCTION
#######################################
def create_mapper_subdag(parent_dag_name, child_dag_name, args):
    """
    This function returns a DAG object that defines the mapper sub-DAG.

    :param parent_dag_name: Name of the parent DAG (main DAG).
    :param child_dag_name: Name of the sub-DAG (task_id for the SubDagOperator).
    :param args: Default arguments for the sub-DAG.
    :return: The sub-DAG (DAG object) with mapper tasks.
    """

    subdag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=args,
        start_date=args['start_date'],
        schedule_interval=args.get('schedule_interval'),
        catchup=False,
        concurrency=64,
    )

    with subdag:
        def dummy_mapper(mapper_id):
            """
            Simulate some "map" job. Possibly returns random data for demonstration.
            """
            print(f"Mapper {mapper_id} is running a dummy job.")
            # Return some random data to XCom to simulate an output
            return {
                'mapper_id': mapper_id,
                'random_value': random.randint(1, 100)
            }

        # Example: two mapper tasks in parallel
        mapper_1 = PythonOperator(
            task_id='mapper_1',
            python_callable=dummy_mapper,
            op_kwargs={'mapper_id': '1'},
            queue='default'  # Ensure your CeleryExecutor queue allows parallel tasks
        )

        mapper_2 = PythonOperator(
            task_id='mapper_2',
            python_callable=dummy_mapper,
            op_kwargs={'mapper_id': '2'},
            queue='default'
        )

        # Add as many mappers as you want; they can run parallel if concurrency allows

    return subdag


#######################################
# MAIN DAG
#######################################
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='mapreduce_subdag_example',
    default_args=default_args,
    description='MapReduce-style DAG with SubDagOperator (mappers), then reduce in main DAG.',
    schedule_interval='@daily',
    catchup=False,
    concurrency=10,  # Up to 10 tasks in parallel in the main DAG
    tags=['example', 'subdag', 'mapreduce'],
) as dag:

    start = EmptyOperator(task_id='start')

    # SubDagOperator references our sub-DAG creation function
    subdag_op = SubDagOperator(
        task_id='mapper_subdag',
        subdag=create_mapper_subdag(
            parent_dag_name='mapreduce_subdag_example',
            child_dag_name='mapper_subdag',
            args=default_args
        ),
        default_args=default_args,
    )

    def reducer_function(**kwargs):
        """
        Simulates the "reduce" step, aggregating data from the mapper tasks.
        We'll pull XCom data from the sub-DAG tasks for demonstration.
        """
        ti = kwargs['ti']

        # The sub-DAG's tasks will be named 'mapper_subdag.mapper_1', etc.
        mapper_1_data = ti.xcom_pull(task_ids='mapper_subdag.mapper_1')
        mapper_2_data = ti.xcom_pull(task_ids='mapper_subdag.mapper_2')

        print(f"Reducer sees mapper_1 data = {mapper_1_data}")
        print(f"Reducer sees mapper_2 data = {mapper_2_data}")

        # Combine or aggregate the results (dummy example)
        random_values = []
        if mapper_1_data:
            random_values.append(mapper_1_data.get('random_value', 0))
        if mapper_2_data:
            random_values.append(mapper_2_data.get('random_value', 0))

        result = {
            'sum_of_random_values': sum(random_values),
            'count_of_mappers': len(random_values),
        }

        print(f"Reducer final result: {result}")
        return result

    reducer_task = PythonOperator(
        task_id='reduce',
        python_callable=reducer_function,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED  # Only run if sub-DAG is successful
    )

    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_DONE
    )

    start >> subdag_op >> reducer_task >> end