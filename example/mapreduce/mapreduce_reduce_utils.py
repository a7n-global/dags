import logging
import os
from typing import Dict

def sum_human_counts(shared_dir: str, num_files: int) -> Dict[str, int]:
    """Sum up human counts from result files."""
    logger = logging.getLogger(__name__)
    total = 0
    failed = []

    for i in range(num_files):
        result_file = os.path.join(shared_dir, f'result_{i}.txt')
        try:
            with open(result_file, 'r') as f:
                humans = int(f.read().strip())
                total += humans
                logger.info(f'Video {i}: {humans} humans')
        except Exception as e:
            logger.error(f'Error reading video {i} result: {e}')
            failed.append(i)

    if failed:
        raise Exception(f'Failed videos: {failed}')

    logger.info(f'Total humans detected: {total}')
    return {'total_humans': total}

if __name__ == '__main__':
    import sys
    result = sum_human_counts(sys.argv[1], int(sys.argv[2]))
    print(f"TOTAL_HUMANS={result['total_humans']}") 