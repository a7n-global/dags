# dags best practices


## mapreduce job example

- [mapreduce_video_gpu.py](mapreduce/mapreduce_video_gpu.py)
- [midsize_mapreduce_example.py](mapreduce/midsize_mapreduce_example.py)
- [complex_job/midsize_mapreduce_example.py](mapreduce/complex_job/midsize_mapreduce_example.py)

## Docker Build DAG

- [docker_build_dag.py](docker_build_dag.py) - Automated Docker image building and pushing DAG

### Docker Build DAG Usage

The Docker Build DAG uses the Image Builder Service to build and push Docker images from a Git repository.

#### Parameters

- `git_url`: Git repository URL containing the Dockerfile
- `dockerfile_path`: Path to the directory containing the Dockerfile within the repository
- `git_branch`: Git branch to use (default: "main")
- `build_cmd`: Custom build command (optional)
- `build_env`: Environment variables for the build (optional)
- `image_name`: Name for the built Docker image
- `tag_name`: Tag for the Docker image (default: "latest")
- `docker_hub_urls`: List of Docker Hub URLs to push the image to
- `priority`: Build priority, higher means more priority (default: 0)

Example trigger:
```bash
airflow dags trigger cicd/docker_build_pipeline \
  --conf '{"git_url": "https://github.com/a7n-global/dockers.git", "dockerfile_path": "helloworld", "image_name": "hub.anuttacon.com/infra/helloworld", "tag_name": "v1.0"}'
```

# Deepseek 评估工作流：从 Airflow 到 Argo Workflows 迁移指南

这个项目将原来的 Airflow DAG 转换为 Argo Workflows，用于运行 Deepseek 模型的评估任务。

## 文件说明

- `deepseek-eval-workflow.yaml` - 基础 Argo Workflow 定义（固定两个评估任务）
- `deepseek-eval-template.yaml` - 可重用的 WorkflowTemplate（支持动态评估任务数量）
- `run-deepseek-eval.yaml` - 使用 WorkflowTemplate 的示例
- `eval/deepseek_harness_il2.py` - 原始的 Airflow DAG

## 主要改变

### 1. 工作流定义方式
- **Airflow**: 使用 Python 代码定义 DAG
- **Argo**: 使用 YAML 定义工作流

### 2. 参数传递
- **Airflow**: 使用 `{{ params.xxx }}` 和 Jinja2 模板
- **Argo**: 使用 `{{workflow.parameters.xxx}}` 和 `{{inputs.parameters.xxx}}`

### 3. 任务依赖
- **Airflow**: 使用 `>>` 操作符定义依赖
- **Argo**: 使用 `dependencies` 字段或 `steps` 顺序定义依赖

### 4. 动态任务
- **Airflow**: 使用 Python 循环创建多个 KubernetesPodOperator
- **Argo**: 使用 `withParam` 创建动态并行任务

## 使用方法

### 1. 部署 WorkflowTemplate
```bash
kubectl apply -f deepseek-eval-template.yaml
```

### 2. 提交工作流
```bash
# 方式1：使用 kubectl 提交
kubectl apply -f run-deepseek-eval.yaml

# 方式2：使用 argo CLI 提交（推荐）
argo submit run-deepseek-eval.yaml

# 方式3：从 WorkflowTemplate 创建工作流
argo submit --from workflowtemplate/deepseek-eval-template \
  -p job_name="my-eval-job" \
  -p project_name="deepseek_v2_lite" \
  -p model_input="/path/to/your/model" \
  -p task_input='["mmlu", "hellaswag", "arc"]'
```

### 3. 查看工作流状态
```bash
# 列出所有工作流
argo list -n argo

# 查看特定工作流详情
argo get <workflow-name> -n argo

# 查看工作流日志
argo logs <workflow-name> -n argo

# 实时监控工作流
argo watch <workflow-name> -n argo
```

### 4. 通过 Web UI 管理
访问 https://localhost:2746/ （需要先运行端口转发）

## 参数说明

- `job_name`: 任务名称，用于标识这次评估运行
- `project_name`: 项目名称，通常是模型系列名
- `model_input`: 输入模型的路径（在转换步骤后会添加 `/hf` 后缀）
- `task_input`: 评估任务列表，JSON 数组格式，如 `["mmlu", "hellaswag", "truthfulqa"]`

## 工作流执行流程

1. **Convert Step**: 将 Deepseek 模型转换为 HuggingFace 格式
   - 容器镜像: `hub.anuttacon.com/nvcr.io/nvidia/nemo:25.02.rc5`
   - 脚本: `airflow_deepseek_convert.sh`

2. **Eval Steps**: 并行执行多个评估任务
   - 容器镜像: `hub.anuttacon.com/docker.io/vllm/vllm-openai:v0.6.4`
   - 脚本: `airflow_llm_evaluation_harness.sh`
   - 每个任务对应一个评估数据集

## 资源配置

每个任务容器的资源配置：
- CPU 限制: 53.2 核心 (53200m)
- 内存限制: ~486 GiB (498073Mi)
- CPU 请求: 50.4 核心 (50400m)
- 内存请求: ~70 GiB (71859Mi)

## 存储卷

工作流使用以下存储卷：
- `/mnt/share`: 共享存储 (hostPath: `/mnt/xstorage/share`)
- `/mnt/project`: 项目存储 (hostPath: `/mnt/xstorage/project/project-llm`)
- `/mnt/personal`: 个人存储 (hostPath: `/mnt/xstorage/personal/xuguang.zhao`)
- `/opt/scripts`: 临时脚本存储 (emptyDir)

## 与 Airflow 的主要差异

### 优势
1. **原生 Kubernetes**: 不需要额外的 Airflow 调度器
2. **更好的可视化**: Argo UI 提供更直观的工作流视图
3. **更轻量**: 没有 Airflow 的额外开销
4. **更好的资源管理**: 直接使用 Kubernetes 资源管理

### 注意事项
1. **参数格式**: task_input 必须是有效的 JSON 数组字符串
2. **命名空间**: 确保在正确的命名空间中运行
3. **权限**: 确保 service account 有足够的权限访问所需资源
4. **存储**: 确保 hostPath 卷在目标节点上存在

## 故障排除

### 常见问题
1. **权限错误**: 检查 service account 和 RBAC 配置
2. **镜像拉取失败**: 确保集群可以访问指定的镜像仓库
3. **存储卷错误**: 检查 hostPath 路径是否存在
4. **资源不足**: 检查集群是否有足够的 CPU 和内存资源

### 调试命令
```bash
# 查看 pod 详情
kubectl describe pod <pod-name> -n argo

# 查看 pod 日志
kubectl logs <pod-name> -n argo

# 查看工作流事件
kubectl get events -n argo --sort-by='.lastTimestamp'
```
