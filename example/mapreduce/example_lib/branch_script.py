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
    print(result)  # For XCom


if __name__ == '__main__':
    main()
