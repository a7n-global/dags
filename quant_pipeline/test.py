from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

# 关键：导入 Kubernetes Python Client 中的各种模型类
from kubernetes.client import (
    models as k8s,  # 常见写法
)

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
    schedule=None,  # 仅示例，按需调整
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={
        'model_input': Param(default='/mnt/project/models/input', type='string', description='Path to input model'),
        'model_output': Param(default='/mnt/project/models/output', type='string', description='Path to save quant model'),
        'scheme': Param(default='FP8_DYNAMIC', enum=['FP8_DYNAMIC','INT8_STATIC'], description='Quant scheme')
    },
    tags=["test"],
) as dag:

    start = EmptyOperator(task_id="start")

    #
    # 1) initContainer - 使用 git-sync 拉取代码
    #
    #
    # 2) 定义主容器的环境变量 (env) - 对应 Pod YAML 里的 env:
    #
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

    #
    # 3) 定义资源限制 (requests/limits)
    #
    container_resources = k8s.V1ResourceRequirements(
        limits={
            "cpu": "212800m",
            "memory": "1992294Mi",
            "nvidia.com/gpu": "8",
            # 下面 RDMA 相关视具体集群而定
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

    #
    # 4) 定义各类 Volume (hostPath, emptyDir 等) + 对应的 VolumeMount
    #
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
        # 新增的空目录卷，用来存放 git-sync 拉下来的内容
        k8s.V1Volume(
            name="git-volume",
            empty_dir=k8s.V1EmptyDirVolumeSource(),
        ),
        # 下面这个 projected Volume (kube-api-access-jbzsf) 
        # 若需要也可定义；通常由 K8s 自动注入，手动声明也行。
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
                    k8s.V1VolumeProjection(
                        config_map=k8s.V1ConfigMapProjection(
                            name="kube-root-ca.crt",
                            items=[
                                k8s.V1KeyToPath(
                                    key="ca.crt",
                                    path="ca.crt"
                                )
                            ]
                        )
                    ),
                    k8s.V1VolumeProjection(
                        downward_api=k8s.V1DownwardAPIProjection(
                            items=[
                                k8s.V1DownwardAPIVolumeFile(
                                    path="namespace",
                                    field_ref=k8s.V1ObjectFieldSelector(
                                        api_version="v1",
                                        field_path="metadata.namespace",
                                    )
                                )
                            ]
                        )
                    )
                ]
            )
        )
    ]

    volume_mounts = [
        k8s.V1VolumeMount(
            name="host-path-share", mount_path="/mnt/share"
        ),
        k8s.V1VolumeMount(
            name="host-path-project", mount_path="/mnt/project"
        ),
        k8s.V1VolumeMount(
            name="host-path-personal", mount_path="/mnt/personal"
        ),
        k8s.V1VolumeMount(
            name="dshm", mount_path="/dev/shm"
        ),
        k8s.V1VolumeMount(
            name="kube-api-access-jbzsf",
            mount_path="/var/run/secrets/kubernetes.io/serviceaccount",
            read_only=True,
        ),
        # 挂载上面 emptyDir 卷，以便访问 /opt/scripts 下的 Git 同步内容
        k8s.V1VolumeMount(
            name="git-volume", mount_path="/opt/scripts"
        ),
    ]

    #
    # 5) 主容器启动命令 & 参数 (对应 YAML 中 command/args)
    #
    main_cmds = ["python3"]
    # 注意 Python 字符串的缩进与换行。
    main_args=[
            "/mnt/project/llm/users/xug/code/Ocean/users/xuguang/quant/ptq/hf_model_quant.py",
            "--model_id", "{{ params.model_input }}",
            "--save_dir", "{{ params.model_output }}",
            "--scheme", "{{ params.scheme }}"
        ],
    #
    # 6) 创建实际的 KubernetesPodOperator
    #
    quant_task = KubernetesPodOperator(
        task_id="quant_task",
        # 填写跟你 YAML 一致的 namespace
        namespace="project-llm",
        # 用到的镜像
        image="hub.anuttacon.com/infra/ocean:latest",
        # initContainer
        # init_containers=[git_sync_init_container],
        # working_dir="/root/code/Ocean",
        # 主容器启动命令与参数
        cmds=main_cmds,
        arguments=main_args,
        # 资源限制
        container_resources=container_resources,
        # 挂载的卷
        volumes=volumes,
        volume_mounts=volume_mounts,
        # 环境变量
        env_vars=env_vars,
        # 是否查看日志、是否删除 Pod 等
        get_logs=True,
        is_delete_operator_pod=False,  # 视需求设置 True/False
        in_cluster=True,
        do_xcom_push=False,
    )

    # 此处也可以添加任务依赖
    start >> quant_task
