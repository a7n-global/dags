from datetime import datetime, timedelta
import logging
from typing import Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# Config & Constants
NUM_FILES = 10  # Reduced from 100 for testing
SHARED_DIR = '/opt/airflow/shared'
INPUT_FILE_PATTERN = f'{SHARED_DIR}/dummy_720p_{{i}}.mp4'
OUTPUT_FILE_PATTERN = f'{SHARED_DIR}/dummy_480p_{{i}}.mp4'

# Kubernetes Config
NAMESPACE = 'airflow'
FFMPEG_IMAGE = 'jrottenberg/ffmpeg:latest'

# Common K8s configurations with reduced resource requests
K8S_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "100m", "memory": "256Mi"},  # Reduced CPU request
    limits={"cpu": "500m", "memory": "512Mi"}     # Reduced CPU limit
)

# Volume configuration
volume = k8s.V1Volume(
    name='shared-volume',
    host_path=k8s.V1HostPathVolumeSource(path='/opt/airflow/shared')
)
volume_mount = k8s.V1VolumeMount(
    name='shared-volume',
    mount_path=SHARED_DIR,
    read_only=False
)

def reduce_frames(**kwargs) -> Dict[str, int]:
    """Aggregate frame counts from transcoding tasks."""
    logger = logging.getLogger(__name__)
    ti = kwargs['ti']
    
    total_frames = 0
    failed_tasks = []
    
    for i in range(NUM_FILES):
        try:
            xcom_output = ti.xcom_pull(task_ids=f'transcode_group.transcode_{i}')
            if xcom_output and isinstance(xcom_output, str):
                lines = xcom_output.strip().splitlines()
                if lines:
                    frame_count = int(lines[-1])
                    total_frames += frame_count
                else:
                    failed_tasks.append(i)
            else:
                failed_tasks.append(i)
        except Exception as e:
            logger.error(f"Error processing task {i}: {str(e)}")
            failed_tasks.append(i)
    
    if failed_tasks:
        raise AirflowException(f"Failed tasks: {failed_tasks}")
    
    return {'total_frames': total_frames}

with DAG(
    dag_id='mapreduce_ffmpeg_cpu',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'retries': 2,
        'retry_delay': timedelta(minutes=2),
    },
    description='MapReduce DAG using KubernetesPodOperator for FFmpeg transcoding',
    schedule='@daily',
    catchup=False,
    max_active_tasks=16,  # Reduced from 128
    tags=['map-reduce', 'ffmpeg', 'k8s'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Generate files
    generate_720p_files = KubernetesPodOperator(
        task_id='generate_720p_files',
        name='generate-720p-files',
        namespace=NAMESPACE,
        image=FFMPEG_IMAGE,
        cmds=['bash', '-cx'],
        arguments=[f"""
            mkdir -p {SHARED_DIR} && \
            echo "Generating {NUM_FILES} dummy 720p MP4 files..." && \
            for i in $(seq 0 $(({NUM_FILES}-1))); do
                ffmpeg -y -f lavfi -i color=c=red:s=1280x720:d=2 \
                       -c:v libx264 -preset ultrafast \
                       {SHARED_DIR}/dummy_720p_$i.mp4;
            done && \
            echo "Done generating dummy files."
        """],
        volumes=[volume],
        volume_mounts=[volume_mount],
        container_resources=K8S_RESOURCES,
        on_finish_action='delete_pod',
        in_cluster=True,
        get_logs=True
    )
    
    # Transcode files
    def build_transcode_k8s_operator(i: int) -> KubernetesPodOperator:
        return KubernetesPodOperator(
            task_id=f"transcode_{i}",
            name=f"transcode-{i}",
            namespace=NAMESPACE,
            image=FFMPEG_IMAGE,
            cmds=["bash", "-cx"],
            arguments=[f"""
                ffmpeg -y -i {INPUT_FILE_PATTERN.format(i=i)} \
                       -c:v libx264 -preset ultrafast \
                       -vf scale=854:480 \
                       {OUTPUT_FILE_PATTERN.format(i=i)} && \
                ffprobe -v error -count_frames -select_streams v:0 \
                        -show_entries stream=nb_read_frames \
                        -of default=nokey=1:noprint_wrappers=1 \
                        {OUTPUT_FILE_PATTERN.format(i=i)}
            """],
            volumes=[volume],
            volume_mounts=[volume_mount],
            container_resources=K8S_RESOURCES,
            get_logs=True,
            do_xcom_push=True,
            on_finish_action='delete_pod',
            in_cluster=True
        )
    
    with TaskGroup(group_id='transcode_group') as transcode_group:
        for i in range(NUM_FILES):
            build_transcode_k8s_operator(i)
    
    # Reduce task
    reduce_task = PythonOperator(
        task_id='reduce_frames',
        python_callable=reduce_frames,
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    # Cleanup
    cleanup_files = KubernetesPodOperator(
        task_id='cleanup_files',
        name='cleanup-files',
        namespace=NAMESPACE,
        image=FFMPEG_IMAGE,
        cmds=['bash', '-cx'],
        arguments=[f"""
            rm -f {SHARED_DIR}/dummy_*.mp4 || true
            echo "Cleaned up temporary files"
        """],
        volumes=[volume],
        volume_mounts=[volume_mount],
        container_resources=K8S_RESOURCES,
        on_finish_action='delete_pod',
        in_cluster=True,
        get_logs=True
    )
    
    end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.ALL_DONE)
    
    # DAG Flow
    start >> generate_720p_files >> transcode_group >> reduce_task >> cleanup_files >> end