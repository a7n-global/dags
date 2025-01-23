import os
import logging
from typing import Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def reduce_results() -> Dict[str, int]:
    """
    Reduce function to aggregate results from all mappers.
    
    Returns:
        Dict containing total processed rows
    """
    shared_dir = '/opt/airflow/shared'
    total_rows = 0
    failed_files = []

    # Process all mapper result files
    for filename in os.listdir(shared_dir):
        if filename.startswith('mapper_result_'):
            try:
                with open(os.path.join(shared_dir, filename), 'r') as f:
                    rows = int(f.read().strip())
                    total_rows += rows
                    logger.info(f"Processed {filename}: {rows} rows")
            except Exception as e:
                logger.error(f"Error processing {filename}: {e}")
                failed_files.append(filename)

    if failed_files:
        raise Exception(f"Failed to process files: {failed_files}")

    # Write final result to shared volume
    result_file = os.path.join(shared_dir, 'reduce_result.txt')
    with open(result_file, 'w') as f:
        f.write(str(total_rows))

    logger.info(f"Total rows processed: {total_rows}")
    return {'total_rows': total_rows}

if __name__ == '__main__':
    try:
        result = reduce_results()
        logger.info(f"Reduce task completed successfully")
    except Exception as e:
        logger.error(f"Reduce task failed: {e}")
        raise 