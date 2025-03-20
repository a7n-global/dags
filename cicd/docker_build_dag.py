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
    
    # Verify the response status code
    if response.status_code != 200:
        raise ValueError(f"Build request failed with status code {response.status_code}: {response.text}")
    
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
    
    max_checks = 3600  # Increased number of checks to support longer build times (10 secs for 1 try)
    check_interval = 10  # Seconds between checks
    max_timeout = 60  # Maximum timeout value in seconds
    base_timeout = 30  # Initial timeout setting
    timeout_backoff_factor = 1.5  # Exponential backoff factor for timeouts
    current_timeout = base_timeout
    consecutive_timeouts = 0
    max_consecutive_timeouts = 5  # Maximum consecutive timeouts before health check
    last_log_length = 0  # Track length of previously fetched logs to show only new content
    
    for i in range(max_checks):
        try:
            logging.info(f"Checking build status for job {job_id}, attempt {i+1}/{max_checks}")
            
            # 1. Get the build status
            response = requests.get(f"http://{IMAGE_BUILDER_URL}/status/{job_id}", timeout=current_timeout)
            
            # Reset timeout settings after successful request
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
            
            # 2. Get queue status
            try:
                queue_response = requests.get(f"http://{IMAGE_BUILDER_URL}/queue", timeout=current_timeout)
                if queue_response.status_code == 200:
                    queue_data = queue_response.json()
                    logging.info(f"IBS Queue Status: Active builds: {queue_data.get('active_builds')}/{queue_data.get('max_concurrent_builds')}, Queued: {queue_data.get('queued_builds')}")
            except Exception as queue_err:
                logging.warning(f"Unable to fetch queue status: {str(queue_err)}")
            
            # 3. Get current build logs (only display new content)
            try:
                logs_response = requests.get(f"http://{IMAGE_BUILDER_URL}/logs/{job_id}", timeout=current_timeout)
                if logs_response.status_code == 200:
                    logs_data = logs_response.json()
                    current_logs = logs_data.get('logs', '')
                    
                    # Only show new log content
                    if len(current_logs) > last_log_length:
                        new_logs = current_logs[last_log_length:]
                        if new_logs.strip():  # If there's new non-empty log content
                            logging.info(f"New build logs:\n{new_logs}")
                        last_log_length = len(current_logs)
            except Exception as logs_err:
                logging.warning(f"Unable to fetch build logs: {str(logs_err)}")
            
            # 4. Check process status if there's a background process
            if current_status == 'in_progress':
                try:
                    process_response = requests.get(f"http://{IMAGE_BUILDER_URL}/process/{job_id}", timeout=current_timeout)
                    if process_response.status_code == 200:
                        process_data = process_response.json()
                        logging.info(f"Build process status: {process_data.get('status')}, CPU: {process_data.get('cpu_percent')}%, Memory: {process_data.get('memory_percent')}%")
                except Exception as process_err:
                    logging.debug(f"Unable to fetch process info: {str(process_err)}")
                
            logging.info(f"Current build status: {current_status}")
            
            # Exit loop if build is completed or failed
            if current_status in ['completed', 'failed']:
                break
                
        except requests.exceptions.Timeout as e:
            # Handle timeouts with exponential backoff
            consecutive_timeouts += 1
            logging.error(f"Error checking build status: {str(e)}")
            
            # Increase timeout setting, but don't exceed maximum
            current_timeout = min(current_timeout * timeout_backoff_factor, max_timeout)
            logging.info(f"Increasing timeout to {current_timeout} seconds")
            
            # Try different strategies if too many consecutive timeouts
            if consecutive_timeouts >= max_consecutive_timeouts:
                logging.warning(f"Experienced {consecutive_timeouts} consecutive timeouts. Attempting to verify IBS service health...")
                try:
                    # Try to request root path to check if service is responsive
                    health_check = requests.get(f"http://{IMAGE_BUILDER_URL}/", timeout=10)
                    if health_check.status_code == 200:
                        logging.info("IBS service root endpoint is responsive, continuing status checks...")
                    else:
                        logging.error(f"IBS service health check failed with status {health_check.status_code}")
                except Exception as health_e:
                    logging.error(f"IBS service health check failed: {str(health_e)}")
                
                # Reset consecutive timeout counter and continue
                consecutive_timeouts = 0
        except Exception as e:
            logging.error(f"Error checking build status: {str(e)}")
            # Continue checking instead of failing immediately
        
        # Wait before next check
        time.sleep(check_interval)
    
    # Final status check with longer timeout
    try:
        logging.info(f"Performing final status check for job {job_id}")
        response = requests.get(f"http://{IMAGE_BUILDER_URL}/status/{job_id}", timeout=max_timeout)
        final_status = response.json()
        
        # Get logs
        try:
            logs_response = requests.get(f"http://{IMAGE_BUILDER_URL}/logs/{job_id}", timeout=max_timeout)
            logs_data = logs_response.json()
            logs = logs_data.get('logs', '')
        except Exception as e:
            logging.error(f"Error fetching logs: {str(e)}")
            logs = f"Unable to fetch logs: {str(e)}"
        
        # Store results
        ti.xcom_push(key='final_status', value=final_status)
        ti.xcom_push(key='build_logs', value=logs)
        
        # Get final queue status
        try:
            queue_response = requests.get(f"http://{IMAGE_BUILDER_URL}/queue", timeout=max_timeout)
            if queue_response.status_code == 200:
                queue_data = queue_response.json()
                logging.info(f"Final IBS Queue Status: Active builds: {queue_data.get('active_builds')}/{queue_data.get('max_concurrent_builds')}, Queued: {queue_data.get('queued_builds')}")
                ti.xcom_push(key='queue_status', value=queue_data)
        except Exception as queue_err:
            logging.warning(f"Unable to fetch final queue status: {str(queue_err)}")
        
        # Log important log snippets (last 20 lines)
        log_lines = logs.split('\n')
        if len(log_lines) > 20:
            logging.info(f"Last 20 lines of build logs:\n{'\n'.join(log_lines[-20:])}")
        else:
            logging.info(f"Complete build logs:\n{logs}")
        
        status = final_status.get('status')
        if status == 'failed':
            raise Exception(f"Docker build failed. Job ID: {job_id}. Please check logs for details.")
        elif status != 'completed':
            raise Exception(f"Build did not complete within expected time. Final status: {status}. Job ID: {job_id}")
            
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