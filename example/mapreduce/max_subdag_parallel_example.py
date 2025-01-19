# For stress test scheduler only. Not a best practice for MapReduce job. DO NOT COPY.
# 1. UI complexity -- It will visualize all mapper, making the graph very hard to read.
# 2. Scheduling overhead -- deadlock

from datetime import datetime, timedelta
import random, time

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule

########################################
# SUBDAG CREATION FUNCTION
########################################
def create_subdag(parent_dag_id, child_dag_id, default_args):
    """
    Creates a sub-DAG with 8 "mapper" tasks running in parallel.
    Each mapper simulates dummy work and returns a random value.
    """

    from airflow import DAG  # Import here to avoid circular references

    subdag = DAG(
        dag_id=f"{parent_dag_id}.{child_dag_id}",
        default_args=default_args,
        start_date=default_args['start_date'],
        schedule_interval=default_args.get('schedule_interval'),
        catchup=False,
        concurrency=8,  # Let this sub-DAG run up to 8 tasks in parallel
    )

    with subdag:

        def dummy_mapper(mapper_id):
            """
            Dummy 'map' job: sleep a bit, return random data.
            """
            sleep_time = random.randint(1, 5)
            print(f"SubDAG={child_dag_id}, mapper={mapper_id}, sleeping {sleep_time}s")
            time.sleep(sleep_time)
            value = random.randint(1, 100)
            print(f"SubDAG={child_dag_id}, mapper={mapper_id}, returning {value}")
            return {
                'subdag': child_dag_id,
                'mapper_id': mapper_id,
                'value': value
            }

        # Create 8 mapper tasks
        for i in range(1, 9):
            PythonOperator(
                task_id=f"mapper_{i}",
                python_callable=dummy_mapper,
                op_kwargs={'mapper_id': i},
                queue='default',  # Ensure the queue can handle many parallel tasks
                dag=subdag
            )
    
    return subdag


########################################
# MAIN DAG
########################################
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# We'll create 16 subdags, each with 8 parallel tasks => potential 128 tasks in parallel
NUMBER_OF_SUBDAGS = 16

with DAG(
    dag_id='max_subdag_parallel_example',
    description='Generate many sub-DAGs and tasks to use all cluster resources.',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    concurrency=128,  # Up to 128 tasks in parallel at the DAG level
    tags=['stress-test', 'subdag', 'parallel'],
) as dag:

    start = EmptyOperator(task_id='start')

    # Dynamically create SubDagOperators
    subdag_operators = []
    for i in range(1, NUMBER_OF_SUBDAGS + 1):
        subdag_task_id = f"subdag_{i}"

        subdag_op = SubDagOperator(
            task_id=subdag_task_id,
            subdag=create_subdag(
                parent_dag_id='max_subdag_parallel_example',
                child_dag_id=subdag_task_id,
                default_args=default_args
            ),
            default_args=default_args,
            # concurrency doesn't exist as a parameter on SubDagOperator,
            # we set concurrency in the subdag DAG object itself.
        )
        subdag_operators.append(subdag_op)

    def aggregator_function(**kwargs):
        """
        Aggregates all mapper results from every sub-DAG.
        We'll just sum up the 'value' returned by each mapper.
        """
        ti = kwargs['ti']
        total_sum = 0
        total_count = 0

        # For each subdag i in [1..16], each has 8 mappers
        for i in range(1, NUMBER_OF_SUBDAGS + 1):
            subdag_id = f"subdag_{i}"
            for mapper_idx in range(1, 9):
                # The sub-dag task ID is subdag_i.mapper_j
                result = ti.xcom_pull(task_ids=f"{subdag_id}.mapper_{mapper_idx}")
                if result:
                    total_sum += result.get('value', 0)
                    total_count += 1

        print(f"Aggregator sees total_count={total_count}, total_sum={total_sum}")
        return {
            'total_count': total_count,
            'total_sum': total_sum,
        }

    aggregator = PythonOperator(
        task_id='final_aggregator',
        python_callable=aggregator_function,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED  # only run if all sub-DAG tasks succeed
    )

    end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.ALL_DONE)

    # DAG FLOW
    start >> subdag_operators >> aggregator >> end