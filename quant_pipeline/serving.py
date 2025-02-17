from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
import uuid
from kubernetes.client import models as k8s

default_args = {
    "owner": "quant",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="quant_serving_test",
    default_args=default_args,
    schedule=None,  # 不自动调度，只能手动触发
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
    },
    tags=["test"],
) as dag:

    start = EmptyOperator(task_id="start")

    # 打印一下传参，便于观察
    print("params:", dag.params)

    def get_dshm_volume(gpu_count):
        """
        按照 GPU 数量，动态生成 dshm volume 大小。
        注意：此处只是示例，你可以根据实际需要调整公式。
        """
        if gpu_count <= 0:
            gpu_count = 2  # 避免出现 0 时计算问题，这里做个保护（或自行处理）
        base_size = 256  # 基础大小（2 GPUs 对应 256Gi）
        factor = gpu_count / 2
        size_gi = int(base_size * factor)
        return k8s.V1Volume(
            name="dshm",
            empty_dir=k8s.V1EmptyDirVolumeSource(
                medium="Memory",
                size_limit=f"{size_gi}Gi",
            ),
        )

    # ---------------------
    # 公共环境变量
    # ---------------------
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
    ]

    # ---------------------
    # 资源定义 (2, 4, 8 GPUs)
    # ---------------------
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

    # ---------------------
    # Volume & Mounts
    # ---------------------
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

    # ---------------------
    # 量化任务 (quant_task)
    # ---------------------
    # 主容器命令与参数
    quant_main_cmds = ["python3"]
    quant_main_args = [
        "/mnt/project/llm/users/xug/code/Ocean/users/xuguang/quant/ptq/hf_model_quant.py",
        "--model_id", "{{ params.model_input }}",
        "--save_dir", "{{ params.model_output }}",
        "--scheme",   "{{ params.scheme }}"
    ]

    # 环境变量
    quant_env_vars = basic_env_vars.copy()
    quant_env_vars.extend([
        k8s.V1EnvVar(name="WORLD_SIZE", value="8"),
        k8s.V1EnvVar(name="PET_NPROC_PER_NODE", value="8"),
        k8s.V1EnvVar(name="CUDA_VISIBLE_DEVICES", value="0"),
    ])

    quant_volumes = basic_volumes.copy()
    # 这里硬编码了 8个GPU 的场景，因为原先代码是这样写的
    quant_volumes.append(get_dshm_volume(8))

    # ---------------------
    # Serving 相关
    # ---------------------

    # 准备一些必要变量
    job_name = f"quant-pipeline-serving-{str(uuid.uuid4())[:8]}"

    # 生成 arguments
    # 这里注意 "--command" 不再用 Jinja 读 GPU，直接用 Python 拼接
    # 但对 model_output 依然保留 "{{ params.model_output }}" 给 Airflow 渲染
    serving_create_args = [
        "/mnt/project/llm/users/xug/code/Ocean/users/xuguang/quant/airflow_serving_startup.sh",
        "{{ params.model_output }}",
        job_name,
    ]

    # 准备资源、volume
    serving_volumes = basic_volumes.copy()
    serving_volumes.append(get_dshm_volume(2))
    serving_env_vars = basic_env_vars.copy()

    # 创建 Serving Pod 任务
    serving_task = KubernetesPodOperator(
        task_id="serving_task",
        namespace="airflow",
        image="hub.anuttacon.com/infra/quant:20241231",
        cmds=["/bin/bash"],  # 主容器的启动命令
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
    # 如果 serving_gpus != 0, 我们就把 start -> serving_task
    start >> serving_task
