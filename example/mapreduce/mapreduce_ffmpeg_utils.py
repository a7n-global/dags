import logging
from typing import Dict
import os

def sum_frame_counts(shared_dir: str, num_files: int) -> Dict[str, int]:
    """
    Sum up frame counts from result files in the shared directory.
    
    Args:
        shared_dir: Path to shared directory containing result files
        num_files: Number of files to process
    
    Returns:
        Dict containing total frame count
    
    Raises:
        Exception: If any task results are missing or invalid
    """
    logger = logging.getLogger(__name__)
    total = 0
    failed = []

    for i in range(num_files):
        result_file = os.path.join(shared_dir, f'result_{i}.txt')
        try:
            with open(result_file, 'r') as f:
                frames = int(f.read().strip())
                total += frames
                logger.info(f'Task {i}: {frames} frames')
        except Exception as e:
            logger.error(f'Error reading task {i} result: {e}')
            failed.append(i)

    if failed:
        raise Exception(f'Failed tasks: {failed}')

    logger.info(f'Total frames processed: {total}')
    return {'total_frames': total}

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print("Usage: python mapreduce_utils.py <shared_dir> <num_files>")
        sys.exit(1)
    
    result = sum_frame_counts(sys.argv[1], int(sys.argv[2]))
    print(f"TOTAL_FRAMES={result['total_frames']}") 