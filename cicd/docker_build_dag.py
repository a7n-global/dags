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
    max_timeout = 60  # 增加最大超时时间到60秒
    base_timeout = 30  # 初始超时设置
    timeout_backoff_factor = 1.5  # 超时指数回退因子
    current_timeout = base_timeout
    consecutive_timeouts = 0
    max_consecutive_timeouts = 5  # 连续超时最大次数
    last_log_length = 0  # 跟踪上次获取的日志长度，只显示新增内容
    
    for i in range(max_checks):
        try:
            logging.info(f"Checking build status for job {job_id}, attempt {i+1}/{max_checks}")
            
            # 1. 获取构建状态
            response = requests.get(f"http://{IMAGE_BUILDER_URL}/status/{job_id}", timeout=current_timeout)
            
            # 成功请求后重置超时设置
            current_timeout = base_timeout
            consecutive_timeouts = 0
            
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
            
            # 2. 获取队列状态
            try:
                queue_response = requests.get(f"http://{IMAGE_BUILDER_URL}/queue", timeout=current_timeout)
                if queue_response.status_code == 200:
                    queue_data = queue_response.json()
                    logging.info(f"IBS Queue Status: Active builds: {queue_data.get('active_builds')}/{queue_data.get('max_concurrent_builds')}, Queued: {queue_data.get('queued_builds')}")
            except Exception as queue_err:
                logging.warning(f"Unable to fetch queue status: {str(queue_err)}")
            
            # 3. 获取当前构建日志（只显示新增部分）
            try:
                logs_response = requests.get(f"http://{IMAGE_BUILDER_URL}/logs/{job_id}", timeout=current_timeout)
                if logs_response.status_code == 200:
                    logs_data = logs_response.json()
                    current_logs = logs_data.get('logs', '')
                    
                    # 只显示新增的日志内容
                    if len(current_logs) > last_log_length:
                        new_logs = current_logs[last_log_length:]
                        if new_logs.strip():  # 如果有新的非空日志
                            logging.info(f"New build logs:\n{new_logs}")
                        last_log_length = len(current_logs)
            except Exception as logs_err:
                logging.warning(f"Unable to fetch build logs: {str(logs_err)}")
            
            # 4. 如果有后台进程，检查进程状态
            if current_status == 'in_progress':
                try:
                    process_response = requests.get(f"http://{IMAGE_BUILDER_URL}/process/{job_id}", timeout=current_timeout)
                    if process_response.status_code == 200:
                        process_data = process_response.json()
                        logging.info(f"Build process status: {process_data.get('status')}, CPU: {process_data.get('cpu_percent')}%, Memory: {process_data.get('memory_percent')}%")
                except Exception as process_err:
                    logging.debug(f"Unable to fetch process info: {str(process_err)}")
                
            logging.info(f"Current build status: {current_status}")
            
            # 如果构建完成或失败，跳出循环
            if current_status in ['completed', 'failed']:
                break
                
        except requests.exceptions.Timeout as e:
            # 超时处理
            consecutive_timeouts += 1
            logging.error(f"Error checking build status: {str(e)}")
            
            # 增加超时设置，但不超过最大值
            current_timeout = min(current_timeout * timeout_backoff_factor, max_timeout)
            logging.info(f"Increasing timeout to {current_timeout} seconds")
            
            # 如果连续超时次数太多，尝试不同的策略
            if consecutive_timeouts >= max_consecutive_timeouts:
                logging.warning(f"Experienced {consecutive_timeouts} consecutive timeouts. Attempting to verify IBS service health...")
                try:
                    # 尝试请求根路径以检查服务是否响应
                    health_check = requests.get(f"http://{IMAGE_BUILDER_URL}/", timeout=10)
                    if health_check.status_code == 200:
                        logging.info("IBS service root endpoint is responsive, continuing status checks...")
                    else:
                        logging.error(f"IBS service health check failed with status {health_check.status_code}")
                except Exception as health_e:
                    logging.error(f"IBS service health check failed: {str(health_e)}")
                
                # 重置连续超时计数，继续尝试
                consecutive_timeouts = 0
        except Exception as e:
            logging.error(f"Error checking build status: {str(e)}")
            # 继续检查，而不是立即失败
        
        # 等待下次检查
        time.sleep(check_interval)
    
    # 最终状态检查，使用更长的超时时间
    try:
        logging.info(f"Performing final status check for job {job_id}")
        response = requests.get(f"http://{IMAGE_BUILDER_URL}/status/{job_id}", timeout=max_timeout)
        final_status = response.json()
        
        # 获取日志
        try:
            logs_response = requests.get(f"http://{IMAGE_BUILDER_URL}/logs/{job_id}", timeout=max_timeout)
            logs_data = logs_response.json()
            logs = logs_data.get('logs', '')
        except Exception as e:
            logging.error(f"Error fetching logs: {str(e)}")
            logs = f"无法获取日志: {str(e)}"
        
        # 存储结果
        ti.xcom_push(key='final_status', value=final_status)
        ti.xcom_push(key='build_logs', value=logs)
        
        # 获取最终队列状态
        try:
            queue_response = requests.get(f"http://{IMAGE_BUILDER_URL}/queue", timeout=max_timeout)
            if queue_response.status_code == 200:
                queue_data = queue_response.json()
                logging.info(f"Final IBS Queue Status: Active builds: {queue_data.get('active_builds')}/{queue_data.get('max_concurrent_builds')}, Queued: {queue_data.get('queued_builds')}")
                ti.xcom_push(key='queue_status', value=queue_data)
        except Exception as queue_err:
            logging.warning(f"Unable to fetch final queue status: {str(queue_err)}")
        
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