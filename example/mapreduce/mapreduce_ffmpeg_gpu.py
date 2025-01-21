from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# Config & Constants
NUM_FILES = 10  # Reduced from 100 for testing
SHARED_DIR = '/opt/airflow/shared'
INPUT_FILE_PATTERN = f'{SHARED_DIR}/dummy_720p_{{i}}.mp4'
OUTPUT_FILE_PATTERN = f'{SHARED_DIR}/dummy_480p_{{i}}.mp4'

# Kubernetes Config
NAMESPACE = 'airflow'
FFMPEG_GPU_IMAGE = 'jrottenberg/ffmpeg:7-nvidia'  # GPU-enabled FFmpeg image

# Common K8s configurations with GPU resources
K8S_RESOURCES = k8s.V1ResourceRequirements(
    requests={
        "cpu": "100m",
        "memory": "256Mi",
        "nvidia.com/gpu": "1"  # Request 1 GPU
    },
    limits={
        "cpu": "500m", 
        "memory": "512Mi",
        "nvidia.com/gpu": "1"  # Limit to 1 GPU
    }
)

# Volume configurations (same as CPU version)
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

def build_transcode_k8s_operator(i: int) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        task_id=f"transcode_{i}",
        name=f"transcode-{i}",
        namespace=NAMESPACE,
        image=FFMPEG_GPU_IMAGE,
        cmds=["bash", "-cx"],
        arguments=[f"""
            set -e
            echo "Starting transcoding for file {i} using GPU..."
            
            # Transcode using GPU acceleration
            ffmpeg -y -hwaccel cuda -hwaccel_output_format cuda -i {INPUT_FILE_PATTERN.format(i=i)} \
                   -c:v h264_nvenc -preset p4 \
                   -vf scale_cuda=854:480 \
                   {OUTPUT_FILE_PATTERN.format(i=i)}
            
            # Count frames and save to result file
            ffprobe -v error -count_frames -select_streams v:0 \
                    -show_entries stream=nb_read_frames \
                    -of default=nokey=1:noprint_wrappers=1 \
                    {OUTPUT_FILE_PATTERN.format(i=i)} > {SHARED_DIR}/result_{i}.txt
        """],
        volumes=[volume],
        volume_mounts=[volume_mount],
        container_resources=K8S_RESOURCES,
        get_logs=True,
        on_finish_action='delete_pod',
        in_cluster=True,
        # Update node selector to match cluster's GPU label
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
    dag_id='mapreduce_ffmpeg_gpu',  # Updated DAG ID
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'retries': 2,
        'retry_delay': timedelta(minutes=2),
    },
    description='MapReduce DAG using KubernetesPodOperator for FFmpeg GPU transcoding',
    schedule='@daily',
    catchup=False,
    max_active_tasks=16,
    tags=['map-reduce', 'ffmpeg', 'k8s', 'gpu'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Generate files (no GPU needed for this)
    generate_720p_files = KubernetesPodOperator(
        task_id='generate_720p_files',
        name='generate-720p-files',
        namespace=NAMESPACE,
        image=FFMPEG_GPU_IMAGE,  # Using GPU image but not using GPU features
        cmds=['bash', '-cx'],
        arguments=[f"""
            mkdir -p {SHARED_DIR} && \
            echo "Generating {NUM_FILES} dummy 720p MP4 files..." && \
            for i in $(seq 0 $(({NUM_FILES}-1))); do
                ffmpeg -y -f lavfi -i color=c=red:s=1280x720:d=2 \
                       -c:v h264_nvenc -preset p4 \
                       {SHARED_DIR}/dummy_720p_$i.mp4;
            done && \
            echo "Done generating dummy files."
        """],
        volumes=[volume],
        volume_mounts=[volume_mount],
        container_resources=K8S_RESOURCES,
        on_finish_action='delete_pod',
        in_cluster=True,
        get_logs=True,
        node_selector={"nvidia.com/gpu.present": "true"},
        tolerations=[
            k8s.V1Toleration(
                key="nvidia.com/gpu",
                operator="Exists",
                effect="NoSchedule"
            )
        ]
    )
    
    # Verify files (no GPU needed)
    verify_files = KubernetesPodOperator(
        task_id='verify_files',
        name='verify-files',
        namespace=NAMESPACE,
        image=FFMPEG_GPU_IMAGE,
        cmds=['bash', '-cx'],
        arguments=[f"""
            echo "Verifying files exist and are accessible..." && \
            for i in $(seq 0 $(({NUM_FILES}-1))); do
                echo "Checking dummy_720p_$i.mp4..." && \
                ls -l {SHARED_DIR}/dummy_720p_$i.mp4 && \
                ffprobe -v error -show_format -show_streams {SHARED_DIR}/dummy_720p_$i.mp4
            done
        """],
        volumes=[volume],
        volume_mounts=[volume_mount],
        container_resources=k8s.V1ResourceRequirements(  # No GPU needed for verification
            requests={"cpu": "100m", "memory": "256Mi"},
            limits={"cpu": "500m", "memory": "512Mi"}
        ),
        on_finish_action='delete_pod',
        in_cluster=True,
        get_logs=True
    )
    
    # Transcode files using GPU
    with TaskGroup(group_id='transcode_group') as transcode_group:
        for i in range(NUM_FILES):
            build_transcode_k8s_operator(i)
    
    # Reduce task (no GPU needed)
    reduce_task = KubernetesPodOperator(
        task_id='reduce_frames',
        name='reduce-frames',
        namespace=NAMESPACE,
        image='python:3.9-slim',
        cmds=["python"],
        arguments=["/opt/airflow/dags/repo/example/mapreduce/mapreduce_utils.py", 
                  SHARED_DIR, 
                  str(NUM_FILES)],
        volumes=[volume, dags_volume],
        volume_mounts=[volume_mount, dags_volume_mount],
        container_resources=k8s.V1ResourceRequirements(  # No GPU needed for reduction
            requests={"cpu": "100m", "memory": "256Mi"},
            limits={"cpu": "500m", "memory": "512Mi"}
        ),
        get_logs=True,
        do_xcom_push=False,
        on_finish_action='delete_pod',
        in_cluster=True,
        startup_timeout_seconds=300,
        image_pull_policy='IfNotPresent'
    )
    
    # Cleanup (no GPU needed)
    cleanup_files = KubernetesPodOperator(
        task_id='cleanup_files',
        name='cleanup-files',
        namespace=NAMESPACE,
        image=FFMPEG_GPU_IMAGE,
        cmds=['bash', '-cx'],
        arguments=[f"""
            rm -f {SHARED_DIR}/dummy_*.mp4 {SHARED_DIR}/result_*.txt || true
            echo "Cleaned up temporary files and results"
        """],
        volumes=[volume],
        volume_mounts=[volume_mount],
        container_resources=k8s.V1ResourceRequirements(  # No GPU needed for cleanup
            requests={"cpu": "100m", "memory": "256Mi"},
            limits={"cpu": "500m", "memory": "512Mi"}
        ),
        on_finish_action='delete_pod',
        in_cluster=True,
        get_logs=True
    )
    
    end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.ALL_DONE)
    
    # DAG Flow
    start >> generate_720p_files >> verify_files >> transcode_group >> reduce_task >> cleanup_files >> end 