import random
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(mapper_id: int) -> int:
    """
    Simulate some CPU or time usage in parallel.
    
    Args:
        mapper_id: Unique identifier for this mapper
        
    Returns:
        Number of rows processed
    """
    sleep_time = random.randint(5, 15)
    logger.info(f"Mapper {mapper_id} starting. Sleeping for {sleep_time} seconds...")
    time.sleep(sleep_time)
    
    rows_processed = random.randint(100, 1000)
    
    # Write results to shared volume
    output_file = f'/opt/airflow/shared/mapper_result_{mapper_id}.txt'
    try:
        with open(output_file, 'w') as f:
            f.write(str(rows_processed))
        logger.info(f"Mapper {mapper_id} finished, processed {rows_processed} rows.")
    except Exception as e:
        logger.error(f"Failed to write results for mapper {mapper_id}: {e}")
        raise

    return rows_processed


if __name__ == '__main__':
    import sys
    mapper_id = int(sys.argv[1])
    main(mapper_id)
