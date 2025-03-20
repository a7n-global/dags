from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
import requests
import json
import time
import logging
import re

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
    
    # Get IBS service URL from params or use default
    ibs_service_url = params.get('ibs_service_url', IMAGE_BUILDER_URL)
    
    # Prepare the API request payload
    payload = {
        "git_url": params['git_url'],
        "dockerfile_path": params['dockerfile_path'],
        "git_branch": params['git_branch'],
        "image_name": params['image_name'],
        "tag_name": params['tag_name'],
        "docker_hub_urls": params['docker_hub_urls'],
        "priority": params.get('priority', 0),
        "clean_images": params.get('clean_images', True)
    }
    
    # Add optional parameters if provided
    if 'build_cmd' in params:
        payload["build_cmd"] = params['build_cmd']
    
    if 'build_env' in params:
        payload["build_env"] = params['build_env']
    
    logging.info(f"Submitting build job to {ibs_service_url} with payload: {payload}")
    
    # Send API request using the specified service URL
    response = requests.post(f"http://{ibs_service_url}/build", 
                            headers={'Content-Type': 'application/json'},
                            data=json.dumps(payload))
    
    # Verify the response status code
    if response.status_code != 200:
        raise ValueError(f"Build request failed with status code {response.status_code}: {response.text}")
    
    response_data = response.json()
    logging.info(f"Build job submission response: {response_data}")
    
    # Store the job ID and service URL for later use
    job_id = response_data.get('job_id')
    if not job_id:
        raise ValueError(f"Failed to get job_id from response: {response_data}")
    
    ti.xcom_push(key='job_id', value=job_id)
    ti.xcom_push(key='ibs_service_url', value=ibs_service_url)
    return job_id

# Function to parse and format Docker build logs
def format_docker_build_logs(logs):
    """
    Parse and format Docker build logs to highlight build steps and important information.
    Uses WebUI-friendly formatting with clear dividers and labels.
    Reorders steps by Dockerfile order instead of BuildKit internal IDs.
    
    Args:
        logs: Raw Docker build logs
        
    Returns:
        Formatted logs with build steps clearly marked and properly ordered
    """
    if not logs:
        return ""
    
    formatted_logs = []
    lines = logs.split('\n')
    current_step = None
    step_num = None
    
    # First pass: collect all steps with their BuildKit IDs and Dockerfile positions
    all_steps = {}
    for line in lines:
        step_match = re.match(r'#(\d+) \[(\d+)/(\d+)\] (.+)', line)
        if step_match:
            buildkit_id = int(step_match.group(1))
            dockerfile_pos = int(step_match.group(2))
            dockerfile_total = int(step_match.group(3))
            step_desc = step_match.group(4)
            all_steps[buildkit_id] = {
                'position': dockerfile_pos,
                'total': dockerfile_total,
                'description': step_desc,
                'buildkit_id': buildkit_id
            }
    
    # Sort steps by their Dockerfile position
    sorted_steps = sorted(all_steps.values(), key=lambda x: x['position'])
    
    # Map from BuildKit ID to sequential step number
    step_id_mapping = {step['buildkit_id']: i+1 for i, step in enumerate(sorted_steps)}
    
    # Second pass: format logs with the mapped step numbers
    for line in lines:
        step_match = re.match(r'#(\d+) \[(\d+)/(\d+)\] (.+)', line)
        if step_match:
            buildkit_id = int(step_match.group(1))
            step_pos = step_match.group(2)
            step_total = step_match.group(3)
            step_desc = step_match.group(4)
            
            # Use sequential step number instead of BuildKit ID
            sequential_num = step_id_mapping.get(buildkit_id, buildkit_id)
            
            # Create a visually distinct header for build steps
            formatted_logs.append("")
            formatted_logs.append("=" * 80)
            formatted_logs.append(f"STEP {sequential_num} [{step_pos}/{step_total}]: {step_desc}")
            formatted_logs.append("=" * 80)
            current_step = step_desc
            step_num = str(buildkit_id)  # Keep original BuildKit ID for matching output lines
            continue
        
        # Mark important lines
        if 'error' in line.lower() or 'failed' in line.lower():
            formatted_logs.append(f"[ERROR] {line}")
        elif 'warning' in line.lower():
            formatted_logs.append(f"[WARNING] {line}")
        elif 'pulling from' in line.lower() or line.startswith('FROM '):
            formatted_logs.append(f"[PULL] {line}")
        elif 'download complete' in line.lower() or 'downloading' in line.lower():
            formatted_logs.append(f"[DOWNLOAD] {line}")
        # Add indentation for lines within a step to improve readability
        elif step_num and current_step:
            if line.startswith('#' + step_num):
                formatted_logs.append(f"  > {line}")
            else:
                formatted_logs.append(f"  {line}")
        else:
            formatted_logs.append(line)
    
    return '\n'.join(formatted_logs)

# Function to summarize Docker build progress
def summarize_build_progress(logs):
    """
    Create a summary of the Docker build progress, showing completed and current steps.
    Reorders steps by Dockerfile order instead of BuildKit internal IDs.
    
    Args:
        logs: Complete build logs so far
        
    Returns:
        A summary string showing build progress in logical order
    """
    if not logs:
        return "Build not started yet"
    
    # Extract all steps using regex
    buildkit_steps = {}
    for line in logs.split('\n'):
        step_match = re.match(r'#(\d+) \[(\d+)/(\d+)\] (.+)', line)
        if step_match:
            buildkit_id = int(step_match.group(1))
            dockerfile_pos = int(step_match.group(2))
            dockerfile_total = int(step_match.group(3))
            step_desc = step_match.group(4)
            buildkit_steps[buildkit_id] = {
                'position': dockerfile_pos,
                'total': dockerfile_total,
                'description': step_desc,
                'buildkit_id': buildkit_id
            }
    
    if not buildkit_steps:
        return "Build initialized, waiting for steps to begin"
    
    # Sort steps by their position in the Dockerfile
    sorted_steps = sorted(buildkit_steps.values(), key=lambda x: x['position'])
    dockerfile_total = sorted_steps[0]['total'] if sorted_steps else 0
    
    # Create a summary
    summary = ["BUILD PROGRESS SUMMARY:"]
    
    # Find the latest step (the one with the highest position)
    latest_step = sorted_steps[-1] if sorted_steps else None
    
    # Create sequential step numbers
    steps_with_sequential_ids = []
    for i, step in enumerate(sorted_steps):
        step_num = i + 1  # Sequential ID
        step_text = f"Step {step_num} [{step['position']}/{step['total']}]: {step['description']}"
        steps_with_sequential_ids.append(step_text)
    
    summary.append(f"Total Dockerfile steps: {dockerfile_total}")
    if latest_step:
        progress_pct = (latest_step['position'] / dockerfile_total) * 100
        summary.append(f"Current progress: {latest_step['position']}/{dockerfile_total} steps ({progress_pct:.1f}%)")
        summary.append(f"Current/Last step: {steps_with_sequential_ids[-1]}")
    
    summary.append("")
    summary.append("Recent steps:")
    
    # Show the last 5 steps at most (in Dockerfile order)
    for step_text in steps_with_sequential_ids[-5:]:
        summary.append(f"  - {step_text}")
    
    return '\n'.join(summary)

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
    
    # Store complete logs to generate summaries
    complete_logs = ""
    last_summary_time = time.time()
    summary_interval = 60
    
    # Track potentially useful error information
    error_details = []
    process_status_history = []
    
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
                    
                    # Update complete logs for summary generation
                    complete_logs = current_logs
                    
                    # Only show new log content with formatting
                    if len(current_logs) > last_log_length:
                        new_logs = current_logs[last_log_length:]
                        if new_logs.strip():  # If there's new non-empty log content
                            # Check if this is a Docker build log
                            if '#' in new_logs and '[' in new_logs and ']' in new_logs:
                                formatted_logs = format_docker_build_logs(new_logs)
                                logging.info(f"New build logs:\n{formatted_logs}")
                            else:
                                logging.info(f"New build logs:\n{new_logs}")
                                
                            # Check for error indicators in new logs
                            error_lines = [line for line in new_logs.split('\n') 
                                         if ('error' in line.lower() or 'failed' in line.lower() or 
                                             'not found' in line.lower() or 'denied' in line.lower())]
                            if error_lines:
                                for error_line in error_lines:
                                    if error_line.strip() and error_line not in error_details:
                                        error_details.append(error_line.strip())
                                        logging.warning(f"Potential error detected: {error_line.strip()}")
                                    
                        last_log_length = len(current_logs)
                        
                    # Generate summary at regular intervals
                    current_time = time.time()
                    if current_time - last_summary_time >= summary_interval:
                        summary = summarize_build_progress(complete_logs)
                        logging.info(f"\n{summary}")
                        last_summary_time = current_time
                        
            except Exception as logs_err:
                logging.warning(f"Unable to fetch build logs: {str(logs_err)}")
            
            # 4. Check process status if there's a background process
            if current_status == 'in_progress':
                try:
                    process_response = requests.get(f"http://{IMAGE_BUILDER_URL}/process/{job_id}", timeout=current_timeout)
                    if process_response.status_code == 200:
                        process_data = process_response.json()
                        proc_status = process_data.get('status')
                        logging.info(f"Build process status: {proc_status}, CPU: {process_data.get('cpu_percent')}%, Memory: {process_data.get('memory_percent')}%")
                        
                        # Track process status changes
                        if process_status_history and process_status_history[-1] != proc_status:
                            logging.info(f"Process status changed: {process_status_history[-1]} -> {proc_status}")
                        process_status_history.append(proc_status)
                        
                        # Check for error status in process
                        if proc_status in ['failed', 'terminated', 'zombie']:
                            error_msg = process_data.get('error')
                            if error_msg and error_msg not in error_details:
                                error_details.append(error_msg)
                                logging.warning(f"Error in build process: {error_msg}")
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
        
        # Get final process status
        try:
            process_response = requests.get(f"http://{IMAGE_BUILDER_URL}/process/{job_id}", timeout=max_timeout)
            if process_response.status_code == 200:
                process_data = process_response.json()
                ti.xcom_push(key='process_status', value=process_data)
                
                # Add process error to error details if available
                if process_data.get('error') and process_data.get('error') not in error_details:
                    error_details.append(process_data.get('error'))
        except Exception as proc_err:
            logging.warning(f"Unable to fetch final process status: {str(proc_err)}")
        
        # Generate a final build summary
        final_summary = summarize_build_progress(logs)
        logging.info(f"\nFINAL BUILD SUMMARY:\n{final_summary}")
        
        # Log important log snippets (last 20 lines)
        log_lines = logs.split('\n')
        if len(log_lines) > 20:
            logging.info(f"Last 20 lines of build logs:\n{'\n'.join(log_lines[-20:])}")
        else:
            logging.info(f"Complete build logs:\n{logs}")
        
        # Final check for any error indicators in the complete logs
        if 'error' in logs.lower() or 'failed' in logs.lower():
            error_pattern = re.compile(r'(?i)(error|failed|not found).*')
            for line in logs.split('\n'):
                match = error_pattern.search(line)
                if match and line.strip() not in error_details:
                    error_details.append(line.strip())
        
        # Store collected error details
        if error_details:
            ti.xcom_push(key='error_details', value=error_details)
            logging.error(f"Build errors detected:\n" + "\n".join(error_details))
        
        status = final_status.get('status')
        if status == 'failed':
            error_msg = f"Docker build failed. Job ID: {job_id}."
            if error_details:
                error_msg += f" Errors:\n" + "\n".join(error_details)
            raise Exception(error_msg)
        elif status != 'completed':
            error_msg = f"Build did not complete within expected time. Final status: {status}. Job ID: {job_id}"
            if error_details:
                error_msg += f" Potential issues:\n" + "\n".join(error_details)
            raise Exception(error_msg)
            
        logging.info(f"Build completed successfully for job {job_id}")
        return final_status
        
    except Exception as e:
        logging.error(f"Final status check failed: {str(e)}")
        raise

# Add this new function after check_build_status

def handle_build_failure(**kwargs):
    """
    Function to handle build failures, analyze error details, and provide useful feedback.
    This task runs when a build fails and provides detailed diagnostic information.
    """
    ti = kwargs['ti']
    
    # Get collected data from previous task
    try:
        job_id = ti.xcom_pull(task_ids='submit_build_task', key='job_id')
        final_status = ti.xcom_pull(task_ids='check_build_status_task', key='final_status')
        logs = ti.xcom_pull(task_ids='check_build_status_task', key='build_logs')
        error_details = ti.xcom_pull(task_ids='check_build_status_task', key='error_details')
        process_status = ti.xcom_pull(task_ids='check_build_status_task', key='process_status')
    except Exception as e:
        logging.error(f"Failed to retrieve task data: {str(e)}")
        raise Exception("Build failed with incomplete error information. Check IBS logs directly.")
    
    # Check what type of failure occurred
    status = final_status.get('status') if final_status else 'unknown'
    
    # Build a comprehensive error message
    failure_reason = "Unknown failure"
    troubleshooting_steps = []
    
    # Analyze error details and logs to determine failure type
    if error_details:
        # Look for specific error patterns
        for error in error_details:
            if "permission denied" in error.lower() or "access denied" in error.lower():
                failure_reason = "Permission issue"
                troubleshooting_steps.append("- Check Docker registry authentication and permissions")
                troubleshooting_steps.append("- Verify IBS service has proper access to the Docker daemon")
                break
            elif "no such file or directory" in error.lower() and "dockerfile" in error.lower():
                failure_reason = "Dockerfile not found"
                troubleshooting_steps.append("- Verify the Dockerfile exists in the specified path")
                troubleshooting_steps.append("- Check that the dockerfile_path parameter is correct")
                break
            elif "git" in error.lower() and ("not found" in error.lower() or "does not exist" in error.lower()):
                failure_reason = "Git repository or branch issue"
                troubleshooting_steps.append("- Verify the git URL is correct and accessible")
                troubleshooting_steps.append("- Check that the specified branch exists")
                break
            elif "network" in error.lower() or "connection" in error.lower():
                failure_reason = "Network connectivity issue"
                troubleshooting_steps.append("- Check network connectivity between IBS and Docker registry")
                troubleshooting_steps.append("- Verify Docker registry is accessible")
                break
    
    # Check process status for additional clues
    if process_status:
        proc_status = process_status.get('status')
        if proc_status == 'zombie':
            failure_reason = f"{failure_reason} (process became zombie)"
            troubleshooting_steps.append("- Docker build process terminated unexpectedly")
            troubleshooting_steps.append("- Check IBS server resources (CPU/memory)")
        elif proc_status == 'terminated':
            failure_reason = f"{failure_reason} (process terminated)"
            troubleshooting_steps.append("- The build process was terminated before completion")
    
    # If no specific reason found, provide generic guidance
    if not troubleshooting_steps:
        troubleshooting_steps = [
            "- Check Docker daemon status on IBS server",
            "- Verify Dockerfile syntax",
            "- Ensure required build context files are present in the repository",
            "- Review the complete build logs for specific error messages"
        ]
    
    # Create a detailed error report
    error_report = f"""
DOCKER BUILD FAILURE REPORT
==========================
Job ID: {job_id}
Status: {status}
Failure Reason: {failure_reason}

ERROR DETAILS:
{chr(10).join(error_details) if error_details else "No specific error details available"}

TROUBLESHOOTING STEPS:
{chr(10).join(troubleshooting_steps)}

For more information, review the complete build logs.
"""
    
    # Log the error report
    logging.error(error_report)
    
    # Store the error report for downstream tasks
    ti.xcom_push(key='error_report', value=error_report)
    
    # Return the error report but still raise an exception to mark the task as failed
    raise Exception(f"Docker build failed: {failure_reason}. See error report for details.")

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
            default=["hub.anuttacon.com", "sea-hub.shiyak-office.com"],
            type='array',
            description='Docker Hub URLs for image pushing'
        ),
        'priority': Param(
            default=0,
            type='integer',
            description='Build priority (higher number = higher priority)'
        ),
        'clean_images': Param(
            default=True,
            type='boolean',
            description='Automatically remove images after successful build and push (default: True)'
        ),
        'ibs_service_url': Param(
            default=IMAGE_BUILDER_URL,
            type='string',
            description='Image Builder Service URL'
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
        trigger_rule="all_success"
    )
    
    # Task to handle build failures with detailed analysis
    handle_failure_task = PythonOperator(
        task_id="handle_build_failure",
        python_callable=handle_build_failure,
        trigger_rule="all_failed",  # This task only runs if check_build_status fails
    )
    
    # Set task dependencies
    submit_build_task >> check_build_status_task >> handle_failure_task 