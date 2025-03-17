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
    
    max_checks = 60  # Maximum number of status checks (10 minutes with 10-second intervals)
    check_interval = 10  # seconds
    
    for i in range(max_checks):
        logging.info(f"Checking build status for job {job_id}, attempt {i+1}/{max_checks}")
        
        response = requests.get(f"http://{IMAGE_BUILDER_URL}/status/{job_id}")
        status_data = response.json()
        
        current_status = status_data.get('status')
        logging.info(f"Current build status: {current_status}")
        
        # If build is complete or failed, break the loop
        if current_status in ['completed', 'failed']:
            break
            
        # Wait before checking again
        time.sleep(check_interval)
    
    # Final status check
    response = requests.get(f"http://{IMAGE_BUILDER_URL}/status/{job_id}")
    final_status = response.json()
    
    # Get logs
    logs_response = requests.get(f"http://{IMAGE_BUILDER_URL}/logs/{job_id}")
    logs_data = logs_response.json()
    
    # Store the results
    ti.xcom_push(key='final_status', value=final_status)
    ti.xcom_push(key='build_logs', value=logs_data.get('logs', ''))
    
    # If build failed, raise an exception
    if final_status.get('status') == 'failed':
        raise Exception(f"Docker build failed. Job ID: {job_id}. Check logs for details.")
    
    return final_status

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