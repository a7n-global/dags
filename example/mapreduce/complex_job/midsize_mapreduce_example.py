from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s


# ----------------------------------------------------------
# CONFIGURE HOW MANY PARALLEL MAPPER TASKS YOU WANT
# ----------------------------------------------------------
NUM_MAPPERS = 10  # Increase or decrease to stress test more or less

# Default arguments for tasks
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Add Kubernetes configurations
NAMESPACE = 'airflow'
BASE_IMAGE = 'python:3.9-slim'

# Resource Configuration
K8S_RESOURCES = k8s.V1ResourceRequirements(
    requests={
        "cpu": "100m",
        "memory": "256Mi"
    },
    limits={
        "cpu": "200m",
        "memory": "512Mi"
    }
)

# Volume Configuration
volume = k8s.V1Volume(
    name='shared-volume',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name='airflow-shared'
    )
)
volume_mount = k8s.V1VolumeMount(
    name='shared-volume',
    mount_path='/opt/airflow/shared',
    read_only=False
)

# Add DAGs volume configuration
dags_volume = k8s.V1Volume(
    name='dags',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name='airflow-dags'
    )
)
dags_volume_mount = k8s.V1VolumeMount(
    name='dags',
    mount_path='/opt/airflow/dags',
    read_only=True
)


def build_mapper_operator(i: int) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        task_id=f"mapper_{i}",
        name=f"mapper-{i}",
        namespace=NAMESPACE,
        image=BASE_IMAGE,
        cmds=["python3"],
        arguments=[
            "/opt/airflow/dags/repo/example/mapreduce/complex_job/example_lib/mapper_script.py", str(i)],
        volumes=[volume, dags_volume],
        volume_mounts=[volume_mount, dags_volume_mount],
        container_resources=K8S_RESOURCES,
        get_logs=True,
        do_xcom_push=False,
        on_finish_action='delete_pod',
        in_cluster=True
    )


with DAG(
    dag_id='midsize_mapreduce_example',
    default_args=default_args,
    description='Map-reduce DAG for stress testing. Generates the file before processing.',
    schedule='@daily',
    catchup=False,
    max_active_tasks=128,
    tags=['stress-test', 'map-reduce', 'parallel'],
) as dag:

    # 1) START
    start = EmptyOperator(task_id='start')

    # 2) PRODUCE THE FILE
    generate_file = KubernetesPodOperator(
        task_id='generate_input_file',
        name='generate-input-file',
        namespace=NAMESPACE,
        image=BASE_IMAGE,
        cmds=['bash', '-c'],
        arguments=['touch /opt/airflow/shared/big_input_dataset_ready.txt && echo "Data is ready" > /opt/airflow/shared/big_input_dataset_ready.txt'],
        volumes=[volume],
        volume_mounts=[volume_mount],
        container_resources=K8S_RESOURCES,
    )

    # 4) MAP EXTRACTION GROUP - multiple parallel tasks
    with TaskGroup(group_id='map_extraction_group') as map_extraction_group:
        for i in range(NUM_MAPPERS):
            build_mapper_operator(i)

    # 5) BRANCH
    branch_op = KubernetesPodOperator(
        task_id='branch_decision',
        name='branch-decision',
        namespace=NAMESPACE,
        image=BASE_IMAGE,
        cmds=["python3"],
        arguments=[
            "/opt/airflow/dags/repo/example/mapreduce/complex_job/example_lib/branch_script.py"],
        volumes=[volume, dags_volume],
        volume_mounts=[volume_mount, dags_volume_mount],
        container_resources=K8S_RESOURCES,
        get_logs=True,
        do_xcom_push=False,
        on_finish_action='delete_pod',
        in_cluster=True
    )

    # 6) BIG PATH / SMALL PATH tasks
    big_data_path = KubernetesPodOperator(
        task_id='big_data_path',
        name='big-data-path',
        namespace=NAMESPACE,
        image=BASE_IMAGE,
        cmds=["python3"],
        arguments=[
            "/opt/airflow/dags/repo/example/mapreduce/complex_job/example_lib/path_task.py", "big_data"],
        volumes=[volume, dags_volume],
        volume_mounts=[volume_mount, dags_volume_mount],
        container_resources=K8S_RESOURCES,
        get_logs=True,
        do_xcom_push=False,
        on_finish_action='delete_pod',
        in_cluster=True
    )

    small_data_path = KubernetesPodOperator(
        task_id='small_data_path',
        name='small-data-path',
        namespace=NAMESPACE,
        image=BASE_IMAGE,
        cmds=["python3"],
        arguments=[
            "/opt/airflow/dags/repo/example/mapreduce/complex_job/example_lib/path_task.py", "small_data"],
        volumes=[volume, dags_volume],
        volume_mounts=[volume_mount, dags_volume_mount],
        container_resources=K8S_RESOURCES,
        get_logs=True,
        do_xcom_push=False,
        on_finish_action='delete_pod',
        in_cluster=True
    )

    # 7) REDUCE (AGGREGATOR)
    reduce_task = KubernetesPodOperator(
        task_id='reduce_aggregations',
        name='reduce-aggregations',
        namespace=NAMESPACE,
        image=BASE_IMAGE,
        cmds=["python3"],
        arguments=[
            "/opt/airflow/dags/repo/example/mapreduce/complex_job/example_lib/reduce_task.py"],
        volumes=[volume, dags_volume],
        volume_mounts=[volume_mount, dags_volume_mount],
        container_resources=K8S_RESOURCES,
        get_logs=True,
        do_xcom_push=False,
        on_finish_action='delete_pod',
        in_cluster=True,
        trigger_rule=TriggerRule.NONE_FAILED
    )

    # 8) END - with cleanup
    # garbage cleanup
    cleanup_file = KubernetesPodOperator(
        task_id='cleanup_indicator_file',
        name='cleanup-indicator-file',
        namespace=NAMESPACE,
        image=BASE_IMAGE,
        cmds=['bash', '-c'],
        arguments=['rm -f /opt/airflow/shared/big_input_dataset_ready.txt'],
        volumes=[volume, dags_volume],
        volume_mounts=[volume_mount, dags_volume_mount],
        container_resources=K8S_RESOURCES,
        get_logs=True,
        do_xcom_push=False,
        on_finish_action='delete_pod',
        in_cluster=True,
        trigger_rule=TriggerRule.ALL_DONE
    )

    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_DONE
    )

    # DAG FLOW
    start >> generate_file >> map_extraction_group
    map_extraction_group >> branch_op >> [
        big_data_path, small_data_path] >> reduce_task >> cleanup_file >> end
