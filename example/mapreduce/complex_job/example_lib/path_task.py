import sys
import time
import random
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(path_name):
    """Process data based on path type."""
    sleep_time = random.randint(
        10, 20) if path_name == 'big_data' else random.randint(3, 7)
    logger.info(f"{path_name} path task sleeping for {sleep_time} seconds...")
    time.sleep(sleep_time)
    logger.info(f"{path_name} path task complete.")


if __name__ == '__main__':
    path_name = sys.argv[1]
    main(path_name)
