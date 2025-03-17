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
