from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# Config & Constants
NUM_FILES = 10
SHARED_DIR = '/opt/airflow/shared'
INPUT_FILE_PATTERN = f'{SHARED_DIR}/generated_video_{{i}}.mp4'
OUTPUT_FILE_PATTERN = f'{SHARED_DIR}/result_{{i}}.txt'

# Kubernetes Config
NAMESPACE = 'airflow'
PYTORCH_GPU_IMAGE = 'huggingface/transformers-pytorch-gpu:latest'

# Resource Configuration
K8S_RESOURCES = k8s.V1ResourceRequirements(
    requests={
        "cpu": "1000m",
        "memory": "4Gi",
        "nvidia.com/gpu": "1"
    },
    limits={
        "cpu": "2000m",
        "memory": "8Gi",
        "nvidia.com/gpu": "1"
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
    mount_path=SHARED_DIR,
    read_only=False
)

# DAGs volume configuration
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

def build_human_detection_operator(i: int) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        task_id=f"detect_humans_{i}",
        name=f"detect-humans-{i}",
        namespace=NAMESPACE,
        image=PYTORCH_GPU_IMAGE,
        cmds=["bash", "-c"],
        arguments=[f"pip install opencv-python-headless && python /opt/airflow/dags/repo/example/mapreduce/mapreduce_video_utils.py detect {INPUT_FILE_PATTERN.format(i=i)} {OUTPUT_FILE_PATTERN.format(i=i)}"],
        volumes=[volume, dags_volume],
        volume_mounts=[volume_mount, dags_volume_mount],
        container_resources=K8S_RESOURCES,
        get_logs=True,
        do_xcom_push=False,
        on_finish_action='delete_pod',
        in_cluster=True,
        startup_timeout_seconds=2000,
        image_pull_policy='IfNotPresent',
        node_selector={"nvidia.com/gpu.present": "true"},
        tolerations=[
            k8s.V1Toleration(
                key="nvidia.com/gpu",
                operator="Exists",
                effect="NoSchedule"
            )
        ]
    )

with DAG(
    dag_id='mapreduce_diffusers_and_detector_gpu',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'retries': 2,
        'retry_delay': timedelta(minutes=2),
    },
    description='MapReduce DAG using GPU for video generation and human detection',
    schedule='@daily',
    catchup=False,
    max_active_tasks=16,
    tags=['map-reduce', 'gpu', 'video', 'pytorch'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Generate videos using transformer model
    generate_videos = KubernetesPodOperator(
        task_id='generate_videos',
        name='generate-videos',
        namespace=NAMESPACE,
        image=PYTORCH_GPU_IMAGE,
        cmds=["bash", "-c"],
        arguments=[f"pip install opencv-python-headless && python /opt/airflow/dags/repo/example/mapreduce/mapreduce_video_utils.py generate {NUM_FILES} {INPUT_FILE_PATTERN}"],
        volumes=[volume, dags_volume],
        volume_mounts=[volume_mount, dags_volume_mount],
        container_resources=K8S_RESOURCES,
        get_logs=True,
        do_xcom_push=False,
        on_finish_action='delete_pod',
        in_cluster=True,
        startup_timeout_seconds=2000,
        image_pull_policy='IfNotPresent',
        node_selector={"nvidia.com/gpu.present": "true"},
        tolerations=[
            k8s.V1Toleration(
                key="nvidia.com/gpu",
                operator="Exists",
                effect="NoSchedule"
            )
        ]
    )
    
    # Verify generated files
    verify_files = KubernetesPodOperator(
        task_id='verify_files',
        name='verify-files',
        namespace=NAMESPACE,
        image=PYTORCH_GPU_IMAGE,
        cmds=['bash', '-cx'],
        arguments=[f"""
            echo "Verifying video files exist and are accessible..." && \
            for i in $(seq 0 $(({NUM_FILES}-1))); do
                echo "Checking generated_video_$i.mp4..." && \
                ls -l {SHARED_DIR}/generated_video_$i.mp4
            done
        """],
        volumes=[volume],
        volume_mounts=[volume_mount],
        container_resources=k8s.V1ResourceRequirements(  # No GPU needed for verification
            requests={"cpu": "100m", "memory": "256Mi"},
            limits={"cpu": "200m", "memory": "512Mi"}
        ),
        get_logs=True,
        do_xcom_push=False,
        on_finish_action='delete_pod',
        in_cluster=True
    )
    
    # Human detection tasks
    with TaskGroup(group_id='detect_humans_group') as detect_group:
        for i in range(NUM_FILES):
            build_human_detection_operator(i)
    
    # Reduce task
    reduce_task = KubernetesPodOperator(
        task_id='reduce_humans',
        name='reduce-humans',
        namespace=NAMESPACE,
        image='python:3.9-slim',
        cmds=["python"],
        arguments=["/opt/airflow/dags/repo/example/mapreduce/mapreduce_video_utils.py",
                  SHARED_DIR,
                  str(NUM_FILES)],
        volumes=[volume, dags_volume],
        volume_mounts=[volume_mount, dags_volume_mount],
        container_resources=k8s.V1ResourceRequirements(  # No GPU needed for reduction
            requests={"cpu": "100m", "memory": "256Mi"},
            limits={"cpu": "200m", "memory": "512Mi"}
        ),
        get_logs=True,
        do_xcom_push=False,
        on_finish_action='delete_pod',
        in_cluster=True,
        startup_timeout_seconds=300,
        image_pull_policy='IfNotPresent'
    )
    
    # Cleanup
    cleanup_files = KubernetesPodOperator(
        task_id='cleanup_files',
        name='cleanup-files',
        namespace=NAMESPACE,
        image='python:3.9-slim',
        cmds=['bash', '-cx'],
        arguments=[f"""
            rm -f {SHARED_DIR}/generated_video_*.mp4 {SHARED_DIR}/result_*.txt || true
            echo "Cleaned up video files and results"
        """],
        volumes=[volume],
        volume_mounts=[volume_mount],
        container_resources=k8s.V1ResourceRequirements(  # No GPU needed for cleanup
            requests={"cpu": "100m", "memory": "256Mi"},
            limits={"cpu": "200m", "memory": "512Mi"}
        ),
        get_logs=True,
        do_xcom_push=False,
        on_finish_action='delete_pod',
        in_cluster=True
    )
    
    end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.ALL_DONE)
    
    # DAG Flow
    start >> generate_videos >> verify_files >> detect_group >> reduce_task >> cleanup_files >> end 