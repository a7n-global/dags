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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="405b_quant_pipeline",
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

    # 环境变量
    env_vars = [
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
        k8s.V1EnvVar(name="WORLD_SIZE", value="8"),
        k8s.V1EnvVar(name="RANK", value="0"),
        k8s.V1EnvVar(name="NODE_RANK", value="0"),
        k8s.V1EnvVar(name="PET_NPROC_PER_NODE", value="8"),
        k8s.V1EnvVar(name="PET_NODE_RANK", value="0"),
        k8s.V1EnvVar(name="PET_NNODES", value="1"),
        k8s.V1EnvVar(name="CUDA_VISIBLE_DEVICES", value="0"),
    ]
    resources_2gpu = k8s.V1ResourceRequirements(
    limits={
        "cpu": "53200m",          # 212800 / 4
        "memory": "498073Mi",     # 1992294 / 4
        "nvidia.com/gpu": "2",
        "nvidia.com/rdma0": "1",
        "nvidia.com/rdma1": "1",
    },
    requests={
        "cpu": "50400m",          # 201600 / 4
        "memory": "471859Mi",     # 1887436 / 4
        "nvidia.com/gpu": "2",
        "nvidia.com/rdma0": "1",
        "nvidia.com/rdma1": "1",
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

    # 8 GPUs (原始配置)
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

    # 资源限制
    container_resources = k8s.V1ResourceRequirements(
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
        },
    )

    # Volumes
    volumes = [
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
            name="dshm",
            empty_dir=k8s.V1EmptyDirVolumeSource(
                medium="Memory",
                size_limit="1Ti",
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
        k8s.V1VolumeMount(name="dshm", mount_path="/dev/shm"),
        k8s.V1VolumeMount(
            name="kube-api-access-jbzsf",
            mount_path="/var/run/secrets/kubernetes.io/serviceaccount",
            read_only=True,
        ),
        k8s.V1VolumeMount(name="git-volume", mount_path="/opt/scripts"),
    ]

    # 主容器命令与参数 (一重列表)
    quant_main_cmds = ["python3"]
    quant_main_args = [
        "/mnt/project/llm/users/xug/code/Ocean/users/xuguang/quant/ptq/hf_model_quant.py",
        "--model_id", "{{ params.model_input }}",
        "--save_dir", "{{ params.model_output }}",
        "--scheme",   "{{ params.scheme }}"
    ]

    # KubernetesPodOperator
    quant_task = KubernetesPodOperator(
        task_id="quant_task",
        namespace="airflow",
        image="hub.anuttacon.com/infra/quant:20241231",
        cmds=quant_main_cmds,
        arguments=quant_main_args,
        container_resources=resources_8gpu,
        volumes=volumes,
        volume_mounts=volume_mounts,
        env_vars=env_vars,
        get_logs=True,
        startup_timeout_seconds=600,
        is_delete_operator_pod=True,  # 是否结束后删除 Pod
        in_cluster=True,
        do_xcom_push=False,
    )

    serving_main_cmds = ["python3"]
    serving_main_args = [
        "-m", "vllm.entrypoints.openai.api_server",
        "--model", "{{ params.model_output }}",
        "--port", "6002",
        "--tensor-parallel-size", "{{ params.serving_gpus }}",
        "--gpu_memory_utilization", "0.95",
        "--enable_prefix_caching",
    ]

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

    gpu_count = "{{ params.serving_gpus }}"
    serving_resource = None
    serving_volumes = volumes
    if gpu_count in ["2", "4", "8"]:
        # 动态选择资源配置
        serving_resource = {
            "2": resources_2gpu,
            "4": resources_4gpu,
            "8": resources_8gpu
        }[gpu_count]
        
        # 添加共享内存卷（256Gi per 2 GPUs）
        serving_volumes = volumes + [
            k8s.V1Volume(
                name="dshm",
                empty_dir=k8s.V1EmptyDirVolumeSource(
                    medium="Memory",
                    size_limit=f"{int(gpu_count) * 128}Gi",  # 2 GPUs = 256Gi, 4 GPUs = 512Gi, 8 GPUs = 1024Gi
                ),
            )
        ]

    if "{{ params.serving_gpus }}" != "0":
        serving_task = KubernetesPodOperator(
            task_id="serving_task",
            namespace="airflow",
            image="hub.anuttacon.com/docker.io/vllm/vllm-openai:v0.6.4",
            cmds=serving_main_cmds,
            arguments=serving_main_args,
            container_resources=serving_resource,
            volumes=serving_volumes,
            volume_mounts=volume_mounts,
            env_vars=env_vars,
            get_logs=True,
            is_delete_operator_pod=False,  # 是否结束后删除 Pod
            in_cluster=True,
            startup_timeout_seconds=600,
            do_xcom_push=False,
        )
        start >> quant_task >> serving_task
    else:
        start >> quant_task