import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Branch based on total rows in result files."""
    shared_dir = '/opt/airflow/shared'
    total_rows = 0

    # Read results from shared volume
    for filename in os.listdir(shared_dir):
        if filename.startswith('mapper_result_'):
            with open(os.path.join(shared_dir, filename), 'r') as f:
                total_rows += int(f.read().strip())

    logger.info(f"Total rows: {total_rows}")
    result = 'big_data_path' if total_rows > 20000 else 'small_data_path'
    
    # Write result to a file in shared volume
    result_file = os.path.join(shared_dir, 'branch_decision.txt')
    with open(result_file, 'w') as f:
        f.write(result)
    
    logger.info(f"Selected path: {result}")


if __name__ == '__main__':
    main()