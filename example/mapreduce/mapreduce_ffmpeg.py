from datetime import datetime, timedelta
import random
import json
import logging
from typing import Dict

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException

# Import the KubernetesPodOperator and K8s client models
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s


# -------------------------------------------------------------
# CONFIG & CONSTANTS
# -------------------------------------------------------------
# File Processing
NUM_FILES = 100
NUM_FILES_MINUS_ONE = NUM_FILES - 1

# Paths and Storage
SHARED_DIR = '/opt/airflow/shared'
INPUT_FILE_PATTERN = f'{SHARED_DIR}/dummy_720p_{{i}}.mp4'
OUTPUT_FILE_PATTERN = f'{SHARED_DIR}/dummy_480p_{{i}}.mp4'

# Kubernetes Config
NAMESPACE = 'airflow'
FFMPEG_IMAGE = 'jrottenberg/ffmpeg:latest'

# Common K8s configurations
K8S_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "1"},
    limits={"cpu": "2"}
)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# -------------------------------------------------------------
# REDUCE FUNCTION
# -------------------------------------------------------------
def reduce_frames(**kwargs) -> Dict[str, int]:
    """
    Aggregate frame counts from transcoding tasks.
    Parse the final line of stdout from each K8sPodOperator.
    Raise AirflowException if any tasks didn't return valid output.
    """
    logger = logging.getLogger(__name__)
    ti = kwargs['ti']

    total_frames = 0
    failed_tasks = []

    logger.info(f"Starting frame reduction for {NUM_FILES} files")

    for i in range(NUM_FILES):
        xcom_output = ti.xcom_pull(task_ids=f'transcode_group.transcode_{i}')
        if not xcom_output:
            # If there's no log output for that task, mark it as failed
            failed_tasks.append(i)
            continue

        # xcom_output is the entire stdout of the container (if get_logs=True & do_xcom_push=True).
        lines = xcom_output.strip().splitlines()
        if not lines:
            failed_tasks.append(i)
            continue

        # The last line of ffprobe is presumably the frame count
        try:
            frame_count = int(lines[-1])
        except ValueError:
            frame_count = 0

        total_frames += frame_count

    if failed_tasks:
        raise AirflowException(f"Failed to process frames for tasks: {failed_tasks}")

    logger.info(f"Completed processing with total frames: {total_frames}")
    return {'total_frames': total_frames}


# -------------------------------------------------------------
# VOLUME CONFIGURATION
# -------------------------------------------------------------
# Define volume & mount before tasks
volume = k8s.V1Volume(
    name='shared-volume',
    host_path=k8s.V1HostPathVolumeSource(
        path='/opt/airflow/shared'
    )
)
volume_mount = k8s.V1VolumeMount(
    name='shared-volume',
    mount_path=SHARED_DIR,
    read_only=False
)


# -------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------
with DAG(
    dag_id='mapreduce_ffmpeg_k8s',
    default_args=default_args,
    description='MapReduce DAG using KubernetesPodOperator for FFmpeg transcoding.',
    schedule='@daily',
    catchup=False,
    max_active_tasks=128,  # or concurrency=128, depending on your Airflow version
    tags=['map-reduce', 'ffmpeg', 'k8s'],
) as dag:

    start = EmptyOperator(task_id='start')

    # 1) GENERATE FILES using KubernetesPodOperator
    generate_720p_files = KubernetesPodOperator(
        task_id='generate_720p_files',
        name='generate-720p-files',
        namespace='airflow',
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

    # 2) TRANSCODE TASK GROUP (PARALLEL) USING KUBERNETESPODOPERATOR
    #    We'll create a Pod for each file. Each Pod runs ffmpeg then ffprobe.

    def build_transcode_k8s_operator(i: int) -> KubernetesPodOperator:
        """
        Returns a KubernetesPodOperator that:
         - pulls an ffmpeg image
         - reads from dummy_720p_{i}.mp4, writes dummy_480p_{i}.mp4
         - runs ffprobe to get frames
         - pushes logs to XCom
        """
        input_file = INPUT_FILE_PATTERN.format(i=i)
        output_file = OUTPUT_FILE_PATTERN.format(i=i)

        ffmpeg_cmd = f"""
          ffmpeg -y -i {input_file} \
                 -c:v libx264 -preset ultrafast \
                 -vf scale=854:480 \
                 {output_file} && \
          ffprobe -v error -count_frames -select_streams v:0 \
                  -show_entries stream=nb_read_frames \
                  -of default=nokey=1:noprint_wrappers=1 \
                  {output_file}
        """

        return KubernetesPodOperator(
            task_id=f"transcode_{i}",
            name=f"transcode-{i}",
            namespace="airflow",
            image=FFMPEG_IMAGE,
            cmds=["bash", "-cx"],
            arguments=[ffmpeg_cmd],
            container_resources=K8S_RESOURCES,
            volumes=[volume],
            volume_mounts=[volume_mount],
            get_logs=True,
            do_xcom_push=True,
            on_finish_action='delete_pod',
            in_cluster=True
        )

    # (B) Construct the parallel tasks in a TaskGroup
    with TaskGroup(group_id='transcode_group') as transcode_group:
        for i in range(NUM_FILES):
            op = build_transcode_k8s_operator(i)

    # 3) REDUCE TASK
    reduce_task = PythonOperator(
        task_id='reduce_frames',
        python_callable=reduce_frames,
        trigger_rule=TriggerRule.NONE_FAILED
    )

    # 4) CLEANUP (OPTIONAL) - remove the dummy files afterwards
    cleanup_files = KubernetesPodOperator(
        task_id='cleanup_files',
        name='cleanup-files',
        namespace=NAMESPACE,
        image=FFMPEG_IMAGE,
        cmds=['bash', '-cx'],
        arguments=[f"""
            if [ -d "{SHARED_DIR}" ]; then
                rm -f {SHARED_DIR}/dummy_*.mp4
                echo "Cleaned up temporary files"
            else
                echo "Directory {SHARED_DIR} not found"
                exit 1
            fi
        """],
        volumes=[volume],
        volume_mounts=[volume_mount],
        container_resources=K8S_RESOURCES,
        on_finish_action='delete_pod',
        in_cluster=True,
        get_logs=True
    )

    end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.ALL_DONE)

    # DAG FLOW
    start >> generate_720p_files >> transcode_group >> reduce_task >> cleanup_files >> end