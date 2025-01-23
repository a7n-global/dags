import random
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(mapper_id):
    """Simulate some CPU or time usage in parallel."""
    sleep_time = random.randint(5, 15)
    logger.info(
        f"Mapper {mapper_id} starting. Sleeping for {sleep_time} seconds...")
    time.sleep(sleep_time)
    rows_processed = random.randint(100, 1000)
    logger.info(
        f"Mapper {mapper_id} finished, processed {rows_processed} rows.")
    return rows_processed


if __name__ == '__main__':
    import sys
    mapper_id = int(sys.argv[1])
    result = main(mapper_id)
    print(result)  # For XCom
