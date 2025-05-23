from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from kubernetes.client import models as k8s

default_args = {
    "owner": "quant",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="deepseek_v2_lite_llm_eval_harness",
    default_args=default_args,
    schedule=None,  # 不自动调度，只能手动触发
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={
        'job_name': Param(
            default='eval-test',
            type='string',
            description='eval test'
        ),
        'project_name': Param(
            default='deepseek_v2_lite',
            type='string',
            description='eval test'
        ),
        'model_input': Param(
            default="",
            type='string',
            description='model input'
        ),
        'task_input': Param(
            default=['a', 'b'],
            type='array',
            description='List of eval tasks'
        )
    },
    tags=["test"],
) as dag:

    start = EmptyOperator(task_id="start")
    print("params", dag.params)
    def get_dshm_volume(gpu_count):
        base_size = 256  # 基础大小（2 GPUs 对应 256Gi）
        size_per_2gpu = base_size * (gpu_count // 2)  # 按照 GPU 数量比例计算
        return k8s.V1Volume(
            name="dshm",
            empty_dir=k8s.V1EmptyDirVolumeSource(
                medium="Memory",
                size_limit=f"{size_per_2gpu}Gi",
            ),
        )

    # 环境变量
    basic_env_vars = [
        k8s.V1EnvVar(name="MLP_CLUSTER", value="il2"),
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
    ]
    resources_request = k8s.V1ResourceRequirements(
        limits={
            "cpu": "53200m",          # 212800 / 4
            "memory": "498073Mi",     # 1992294 / 4
        },
        requests={
            "cpu": "50400m",          # 201600 / 4
            "memory": "71859Mi",     # 1887436 / 4
        }
    )

    # Volumes
    basic_volumes = [
        k8s.V1Volume(
            name="host-path-share",
            host_path=k8s.V1HostPathVolumeSource(
                path="/mnt/xstorage/share",
                type="Directory",
            ),
        ),
        k8s.V1Volume(
            name="host-path-project",
            host_path=k8s.V1HostPathVolumeSource(
                path="/mnt/xstorage/project/project-llm",
                type="Directory",
            ),
        ),
        k8s.V1Volume(
            name="host-path-personal",
            host_path=k8s.V1HostPathVolumeSource(
                path="/mnt/xstorage/personal/xuguang.zhao",
                type="Directory",
            ),
        ),
        k8s.V1Volume(
            name="git-volume",
            empty_dir=k8s.V1EmptyDirVolumeSource(),
        ),
        k8s.V1Volume(
            name="kube-api-access-mpdxx",
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
            name="kube-api-access-mpdxx",
            mount_path="/var/run/secrets/kubernetes.io/serviceaccount",
            read_only=True,
        ),
        k8s.V1VolumeMount(name="git-volume", mount_path="/opt/scripts"),
    ]

    convert_main_cmds = ["bash"]
    convert_main_args = [
        "/mnt/project/llm/users/xug/code/trial/Ocean/users/xuguang/evaluation/airflow_pipeline/airflow_deepseek_convert.sh",
        "{{ params.model_input }}",
        "{{ params.job_name }}",
    ]

    eval_env_vars = basic_env_vars.copy()
    eval_volumes = basic_volumes.copy()
    
    # Create convert task
    convert_task = KubernetesPodOperator(
        task_id="convert_deepseek_to_hf_task",  # 静态task_id
        namespace="airflow",
        image="hub.anuttacon.com/nvcr.io/nvidia/nemo:25.02.rc5",
        cmds=convert_main_cmds,
        arguments=convert_main_args,
        container_resources=resources_request,
        volumes=eval_volumes,
        volume_mounts=volume_mounts,
        env_vars=eval_env_vars,
        get_logs=True,
        startup_timeout_seconds=600,
        is_delete_operator_pod=True,
        in_cluster=True,
        do_xcom_push=False,
    )
    
    eval_main_cmds = ["bash"]
    eval_main_args = [
        "/mnt/project/llm/users/xug/code/trial/Ocean/users/xuguang/evaluation/airflow_pipeline/airflow_llm_evaluation_harness.sh",
    ]
    
    eval_tasks = []  # 存储所有eval任务
    for i, task_input in enumerate(dag.params['task_input']):
        task_id = f"llm_eval_harness_task_{i}_{task_input}"  # 使用索引而不是运行时参数
        eval_task = KubernetesPodOperator(
            task_id=task_id,
            namespace="airflow",
            image="hub.anuttacon.com/docker.io/vllm/vllm-openai:v0.6.4",
            cmds=eval_main_cmds,
            arguments=eval_main_args + ["{{ params.model_input }}/hf"] + ["{{ params.project_name }}"] + ["{{ params.job_name }}"] + [task_input],
            container_resources=resources_request,
            volumes=eval_volumes,
            volume_mounts=volume_mounts,
            env_vars=eval_env_vars,
            get_logs=True,
            startup_timeout_seconds=600,
            is_delete_operator_pod=True,
            in_cluster=True,
            do_xcom_push=False,
        )
        eval_tasks.append(eval_task)
    
    # Set task dependencies
    start >> convert_task
    for eval_task in eval_tasks:
        convert_task >> eval_task  # eval任务在convert任务完成后执行
