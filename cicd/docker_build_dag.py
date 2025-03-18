from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
import requests
import json
import time
import logging

# Image Builder Service API endpoint
IMAGE_BUILDER_URL = "10.27.70.229:9000"

default_args = {
    "owner": "docker_builder",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Function to submit a build job
def submit_build(**kwargs):
    ti = kwargs['ti']
    params = kwargs['params']
    
    # Prepare the API request payload
    payload = {
        "git_url": params['git_url'],
        "dockerfile_path": params['dockerfile_path'],
        "git_branch": params['git_branch'],
        "image_name": params['image_name'],
        "tag_name": params['tag_name'],
        "docker_hub_urls": params['docker_hub_urls'],
        "priority": params.get('priority', 0)
    }
    
    # Add optional parameters if provided
    if 'build_cmd' in params:
        payload["build_cmd"] = params['build_cmd']
    
    if 'build_env' in params:
        payload["build_env"] = params['build_env']
    
    logging.info(f"Submitting build job with payload: {payload}")
    
    # Send API request
    response = requests.post(f"http://{IMAGE_BUILDER_URL}/build", 
                            headers={'Content-Type': 'application/json'},
                            data=json.dumps(payload))
    
    response_data = response.json()
    logging.info(f"Build job submission response: {response_data}")
    
    # Store the job ID for later use
    job_id = response_data.get('job_id')
    if not job_id:
        raise ValueError(f"Failed to get job_id from response: {response_data}")
    
    ti.xcom_push(key='job_id', value=job_id)
    return job_id

# Function to check build status
def check_build_status(**kwargs):
    ti = kwargs['ti']
    job_id = ti.xcom_pull(task_ids='submit_build_task', key='job_id')
    
    if not job_id:
        raise ValueError("No job_id found from previous task")
        
    logging.info(f"Starting to monitor build status for job ID: {job_id}")
    
    max_checks = 120  # 增加检查次数，支持更长的构建时间（20分钟）
    check_interval = 10  # 秒
    
    for i in range(max_checks):
        try:
            logging.info(f"Checking build status for job {job_id}, attempt {i+1}/{max_checks}")
            
            response = requests.get(f"http://{IMAGE_BUILDER_URL}/status/{job_id}", timeout=30)
            
            if response.status_code != 200:
                logging.error(f"Received non-200 status code: {response.status_code}, response: {response.text}")
                time.sleep(check_interval)
                continue
                
            status_data = response.json()
            current_status = status_data.get('status')
            
            if not current_status:
                logging.warning(f"No status found in response: {status_data}")
                time.sleep(check_interval)
                continue
                
            logging.info(f"Current build status: {current_status}")
            
            # 如果构建完成或失败，跳出循环
            if current_status in ['completed', 'failed']:
                break
                
        except Exception as e:
            logging.error(f"Error checking build status: {str(e)}")
            # 继续检查，而不是立即失败
        
        # 等待下次检查
        time.sleep(check_interval)
    
    # 最终状态检查
    try:
        logging.info(f"Performing final status check for job {job_id}")
        response = requests.get(f"http://{IMAGE_BUILDER_URL}/status/{job_id}", timeout=30)
        final_status = response.json()
        
        # 获取日志
        try:
            logs_response = requests.get(f"http://{IMAGE_BUILDER_URL}/logs/{job_id}", timeout=30)
            logs_data = logs_response.json()
            logs = logs_data.get('logs', '')
        except Exception as e:
            logging.error(f"Error fetching logs: {str(e)}")
            logs = f"无法获取日志: {str(e)}"
        
        # 存储结果
        ti.xcom_push(key='final_status', value=final_status)
        ti.xcom_push(key='build_logs', value=logs)
        
        # 记录重要的日志片段（最后 20 行）
        log_lines = logs.split('\n')
        if len(log_lines) > 20:
            logging.info(f"Last 20 lines of build logs:\n{'\n'.join(log_lines[-20:])}")
        else:
            logging.info(f"Complete build logs:\n{logs}")
        
        status = final_status.get('status')
        if status == 'failed':
            raise Exception(f"Docker 构建失败. 任务 ID: {job_id}. 请检查日志了解详情.")
        elif status != 'completed':
            raise Exception(f"构建未在预期时间内完成. 最终状态: {status}. 任务 ID: {job_id}")
            
        logging.info(f"Build completed successfully for job {job_id}")
        return final_status
        
    except Exception as e:
        logging.error(f"Final status check failed: {str(e)}")
        raise

with DAG(
    dag_id="docker_build_pipeline",
    default_args=default_args,
    description="DAG to build and push Docker images using Image Builder Service",
    schedule=None,  # Triggered manually
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        'git_url': Param(
            default="https://github.com/example/repo.git",
            type='string',
            description='Git repository URL containing the Dockerfile'
        ),
        'dockerfile_path': Param(
            default=".",
            type='string',
            description='Path to the directory containing the Dockerfile within the repository'
        ),
        'git_branch': Param(
            default="main",
            type='string',
            description='Git branch to use'
        ),
        'build_cmd': Param(
            default="",
            type='string',
            description='Custom build command (optional)'
        ),
        'build_env': Param(
            default={},
            type='object',
            description='Environment variables for the build (optional)'
        ),
        'image_name': Param(
            default="hub.anuttacon.com/infra/example",
            type='string',
            description='Name for the built Docker image'
        ),
        'tag_name': Param(
            default="latest",
            type='string',
            description='Tag for the Docker image'
        ),
        'docker_hub_urls': Param(
            default=["hub.anuttacon.com"],
            type='array',
            description='List of Docker Hub URLs to push the image to'
        ),
        'priority': Param(
            default=0,
            type='integer',
            description='Build priority (higher means more priority)'
        ),
    },
) as dag:
    
    # Task to submit build job
    submit_build_task = PythonOperator(
        task_id="submit_build_task",
        python_callable=submit_build,
    )
    
    # Task to check build status and wait for completion
    check_build_status_task = PythonOperator(
        task_id="check_build_status_task",
        python_callable=check_build_status,
    )
    
    # Set task dependencies
    submit_build_task >> check_build_status_task 