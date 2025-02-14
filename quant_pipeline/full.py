from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.trigger_rule import TriggerRule
import uuid
from kubernetes.client import models as k8s

default_args = {
    "owner": "quant",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id="quant_pipeline",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={
        'model_input': Param(
            default='/mnt/share/ocean/ocean3.1-2.8',
            type='string',
            description='Path to input model'
        ),
        'model_output': Param(
            default='/mnt/share/ocean/output/quant/',
            type='string',
            description='Path to save quant model'
        ),
        'scheme': Param(
            default='FP8_DYNAMIC',
            enum=['FP8_DYNAMIC','INT8_STATIC'],
            description='Quant scheme'
        ),
        'serving_gpus': Param(
            default=0,
            enum=[0, 2, 4, 8],
            description='Number of GPUs for serving (0=no serving, 2/4/8=deploy serving)'
        ),
        'job_name': Param(
            default="default",
            description='Job Name'
        ),
        'model_size': Param(
            default="small",
            description='Model size'
        ),
    },
    tags=["test"],
) as dag:

    start = EmptyOperator(task_id="start")

    print("params:", dag.params)

    def get_dshm_volume(gpu_count):
        if gpu_count <= 0:
            gpu_count = 2
        base_size = 256
        factor = gpu_count / 2
        size_gi = int(base_size * factor)
        return k8s.V1Volume(
            name="dshm",
            empty_dir=k8s.V1EmptyDirVolumeSource(
                medium="Memory",
                size_limit=f"{size_gi}Gi",
            ),
        )

    basic_env_vars = [
        k8s.V1EnvVar(name="MLP_CLUSTER", value="va"),
        k8s.V1EnvVar(name="MLP_PROJECT", value="llm"),
        k8s.V1EnvVar(name="MLP_USER", value="xuguang.zhao"),
        k8s.V1EnvVar(name="NCCL_DEBUG", value="WARN"),
        k8s.V1EnvVar(name="NCCL_SOCKET_IFNAME", value="eth0"),
        k8s.V1EnvVar(name="NCCL_IB_QPS_PER_CONNECTION", value="4"),
        k8s.V1EnvVar(name="TORCH_CPP_LOG_LEVEL", value="INFO"),
        k8s.V1EnvVar(name="TORCH_DISTRIBUTED_DEBUG", value="INFO"),
        k8s.V1EnvVar(name="RAY_COLOR_PREFIX", value="0"),
        k8s.V1EnvVar(name="PYTHONUNBUFFERED", value="1"),
        k8s.V1EnvVar(name="MASTER_PORT", value="23456"),
        k8s.V1EnvVar(name="PET_MASTER_PORT", value="23456"),
        k8s.V1EnvVar(name="MASTER_ADDR", value="localhost"),
        k8s.V1EnvVar(name="PET_MASTER_ADDR", value="localhost"),
        k8s.V1EnvVar(name="RANK", value="0"),
        k8s.V1EnvVar(name="NODE_RANK", value="0"),
        k8s.V1EnvVar(name="PET_NODE_RANK", value="0"),
        k8s.V1EnvVar(name="PET_NNODES", value="1"),
        k8s.V1EnvVar(name="MODEL_SIZE", value="{{ params.model_size }}"),
    ]

    resources_cheap = k8s.V1ResourceRequirements(
        limits={
            "cpu": "23200m",
            "memory": "228073Mi",
        },
        requests={
            "cpu": "23200m",
            "memory": "228073Mi",
        }
    )

    # 4 GPUs
    resources_4gpu = k8s.V1ResourceRequirements(
        limits={
            "cpu": "106400m",         # 212800 / 2
            "memory": "996147Mi",     # 1992294 / 2
            "nvidia.com/gpu": "4",
            "nvidia.com/rdma0": "1",
            "nvidia.com/rdma1": "1",
            "nvidia.com/rdma2": "1",
            "nvidia.com/rdma3": "1",
        },
        requests={
            "cpu": "100800m",         # 201600 / 2
            "memory": "943718Mi",     # 1887436 / 2
            "nvidia.com/gpu": "4",
            "nvidia.com/rdma0": "1",
            "nvidia.com/rdma1": "1",
            "nvidia.com/rdma2": "1",
            "nvidia.com/rdma3": "1",
        }
    )

    resources_8gpu = k8s.V1ResourceRequirements(
        limits={
            "cpu": "212800m",
            "memory": "1992294Mi",
            "nvidia.com/gpu": "8",
            "nvidia.com/rdma0": "1",
            "nvidia.com/rdma1": "1",
            "nvidia.com/rdma2": "1",
            "nvidia.com/rdma3": "1",
            "nvidia.com/rdma4": "1",
            "nvidia.com/rdma5": "1",
            "nvidia.com/rdma6": "1",
            "nvidia.com/rdma7": "1",
        },
        requests={
            "cpu": "201600m",
            "memory": "1887436Mi",
            "nvidia.com/gpu": "8",
            "nvidia.com/rdma0": "1",
            "nvidia.com/rdma1": "1",
            "nvidia.com/rdma2": "1",
            "nvidia.com/rdma3": "1",
            "nvidia.com/rdma4": "1",
            "nvidia.com/rdma5": "1",
            "nvidia.com/rdma6": "1",
            "nvidia.com/rdma7": "1",
        }
    )

    basic_volumes = [
        k8s.V1Volume(
            name="host-path-share",
            host_path=k8s.V1HostPathVolumeSource(
                path="/mnt/ddnfs01/share",
                type="Directory",
            ),
        ),
        k8s.V1Volume(
            name="host-path-project",
            host_path=k8s.V1HostPathVolumeSource(
                path="/mnt/ddnfs01/project/project-llm",
                type="Directory",
            ),
        ),
        k8s.V1Volume(
            name="host-path-personal",
            host_path=k8s.V1HostPathVolumeSource(
                path="/mnt/ddnfs01/personal/xuguang.zhao",
                type="Directory",
            ),
        ),
        k8s.V1Volume(
            name="git-volume",
            empty_dir=k8s.V1EmptyDirVolumeSource(),
        ),
        k8s.V1Volume(
            name="kube-api-access-jbzsf",
            projected=k8s.V1ProjectedVolumeSource(
                sources=[
                    k8s.V1VolumeProjection(
                        service_account_token=k8s.V1ServiceAccountTokenProjection(
                            expiration_seconds=3607,
                            path="token",
                        )
                    ),
                ]
            )
        )
    ]

    volume_mounts = [
        k8s.V1VolumeMount(name="host-path-share", mount_path="/mnt/share"),
        k8s.V1VolumeMount(name="host-path-project", mount_path="/mnt/project"),
        k8s.V1VolumeMount(name="host-path-personal", mount_path="/mnt/personal"),
        k8s.V1VolumeMount(
            name="kube-api-access-jbzsf",
            mount_path="/var/run/secrets/kubernetes.io/serviceaccount",
            read_only=True,
        ),
        k8s.V1VolumeMount(name="git-volume", mount_path="/opt/scripts"),
    ]

    quant_main_cmds = ["python3"]
    quant_main_args = [
        "/mnt/project/llm/users/xug/code/Ocean/users/xuguang/quant/ptq/hf_model_quant.py",
        "--model_id", "{{ params.model_input }}",
        "--save_dir", "{{ params.model_output }}",
        "--scheme",   "{{ params.scheme }}"
    ]

    quant_env_vars = basic_env_vars.copy()
    quant_env_vars.extend([
        k8s.V1EnvVar(name="WORLD_SIZE", value="8"),
        k8s.V1EnvVar(name="PET_NPROC_PER_NODE", value="8"),
        k8s.V1EnvVar(name="CUDA_VISIBLE_DEVICES", value="0"),
    ])

    quant_volumes = basic_volumes.copy()
    quant_volumes.append(get_dshm_volume(8))

    quant_task = KubernetesPodOperator(
        task_id="quant_task",
        namespace="airflow",
        image="hub.anuttacon.com/infra/quant:20241231",
        cmds=quant_main_cmds,
        arguments=quant_main_args,
        container_resources=resources_8gpu,
        volumes=quant_volumes,
        volume_mounts=volume_mounts,
        env_vars=quant_env_vars,
        get_logs=True,
        startup_timeout_seconds=600,
        is_delete_operator_pod=True,
        in_cluster=True,
        do_xcom_push=False,
    )

    serving_create_args = [
        "/mnt/project/llm/users/xug/code/Ocean/users/xuguang/quant/airflow_pipeline/airflow_serving_startup.sh",
        "{{ params.model_output }}",
        "{{ params.job_name }}",
    ]

    serving_volumes = basic_volumes.copy()
    serving_volumes.append(get_dshm_volume(2))
    serving_env_vars = basic_env_vars.copy()

    serving_task = KubernetesPodOperator(
        task_id="serving_task",
        namespace="airflow",
        image="hub.anuttacon.com/infra/quant:20241231",
        cmds=["/bin/bash"],
        arguments=serving_create_args,
        container_resources=resources_cheap,
        volumes=serving_volumes,
        volume_mounts=volume_mounts,
        env_vars=serving_env_vars,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=1200,
        do_xcom_push=False,
    )

    # ---------------------
    # Evaluation 相关
    # ---------------------
    evaluation_last_turn_loss_create_args = [
        "/mnt/project/llm/users/xug/code/Ocean/users/xuguang/quant/evaluate_last_turn_loss.py",
        "--model_path", "{{ params.model_output }}",
        "--model_endpoint", "http://{{ params.job_name }}.serving.va-mlp.anuttacon.com"
    ]

    evaluation_volumes = basic_volumes.copy()
    evaluation_volumes.append(get_dshm_volume(4))
    evaluation_task_env_vars = basic_env_vars.copy()

    evaluate_last_turn_loss_task = KubernetesPodOperator(
        task_id="evaluate_last_turn_loss_task",
        namespace="airflow",
        image="hub.anuttacon.com/infra/quant:20241231",
        cmds=["python3"],
        arguments=evaluation_last_turn_loss_create_args,
        container_resources=resources_4gpu,
        volumes=evaluation_volumes,
        volume_mounts=volume_mounts,
        env_vars=evaluation_task_env_vars,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=1200,
        do_xcom_push=False,
    )

    evaluate_vllm_output_loss_create_args = [
        "/mnt/project/llm/users/xug/code/Ocean/users/xuguang/quant/evaluate_vllm_output_loss.py",
        "--model_path", "{{ params.model_output }}",
        "--model_endpoint", "http://{{ params.job_name }}.serving.va-mlp.anuttacon.com"
    ]

    evaluate_vllm_output_loss_task = KubernetesPodOperator(
        task_id="evaluate_vllm_output_loss_task",
        namespace="airflow",
        image="hub.anuttacon.com/infra/quant:20241231",
        cmds=["python3"],
        arguments=evaluate_vllm_output_loss_create_args,
        container_resources=resources_4gpu,
        volumes=evaluation_volumes,
        volume_mounts=volume_mounts,
        env_vars=evaluation_task_env_vars,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=1200,
        do_xcom_push=False,
    )

    rm_score_task_create_args = [
        "/mnt/project/llm/users/xug/code/Ocean/users/xuguang/quant/airflow_pipeline/airflow_rm_scores.sh",
        "{{ params.model_output }}",
        "{{ params.job_name }}",
    ]

    rm_score_task = KubernetesPodOperator(
        task_id="rm_score_task",
        namespace="airflow",
        image="hub.anuttacon.com/infra/quant:20241231",
        cmds=["/bin/bash"],
        arguments=rm_score_task_create_args,
        container_resources=resources_cheap,
        volumes=evaluation_volumes,
        volume_mounts=volume_mounts,
        env_vars=evaluation_task_env_vars,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=1200,
        do_xcom_push=False,
    )

    benchmark_task_create_args = [
        "/quant_benchmark/benchmarks/benchmark_serving.py",
        "--backend", "vllm",
        "--base-url", "http://{{ params.job_name }}.serving.va-mlp.anuttacon.com",
        "--dataset-name=sharegpt",
        "--dataset-path", "/quant_benchmark/benchmark_data.json",
        "--model", "{{ params.model_output }}",
        "--seed", "12345"
    ]

    benchmark_task = KubernetesPodOperator(
        task_id="benchmark_task",
        namespace="airflow",
        image="hub.anuttacon.com/infra/quant:20241231",
        cmds=["python3"],
        arguments=benchmark_task_create_args,
        container_resources=resources_4gpu,
        volumes=evaluation_volumes,
        volume_mounts=volume_mounts,
        env_vars=evaluation_task_env_vars,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=1200,
        do_xcom_push=False,
    )

    cleanup_task_create_args = [
        "/mnt/project/llm/users/xug/code/Ocean/users/xuguang/quant/serving/serving_delete.py",
        "--url",
        "https://va-mlp.anuttacon.com/api",
        "--serving_name",
        "{{ params.job_name }}",
    ]

    cleanup_task = KubernetesPodOperator(
        task_id="cleanup_task",
        namespace="airflow",
        image="hub.anuttacon.com/infra/quant:20241231",
        cmds=["python3"],
        arguments=cleanup_task_create_args,
        container_resources=resources_cheap,
        volumes=evaluation_volumes,
        volume_mounts=volume_mounts,
        env_vars=evaluation_task_env_vars,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=1200,
        do_xcom_push=False,
        trigger_rule=TriggerRule.ALL_DONE
    )

    start >> quant_task >> serving_task >> evaluate_last_turn_loss_task >> evaluate_vllm_output_loss_task >> rm_score_task >> benchmark_task 

    evaluate_last_turn_loss_task >> cleanup_task
    evaluate_vllm_output_loss_task >> cleanup_task
    rm_score_task >> cleanup_task
    benchmark_task >> cleanup_task
