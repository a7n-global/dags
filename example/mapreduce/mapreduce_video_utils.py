import logging
from typing import Dict
import os
import torch
from diffusers import DiffusionPipeline
import cv2
from torchvision.models import resnet50

def generate_video(output_path: str, duration_sec: int = 2) -> None:
    """Generate a short video using Stable Diffusion."""
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    
    # Initialize text-to-video model
    pipe = DiffusionPipeline.from_pretrained(
        "damo-vilab/text-to-video-ms-1.7b",
        torch_dtype=torch.float16,
        variant="fp16"
    ).to(device)
    
    # Generate video frames
    prompt = "A person walking in a city street"
    video_frames = pipe(
        prompt,
        num_inference_steps=20,
        num_frames=duration_sec * 30  # 30fps
    ).frames
    
    # Save as video
    height, width = video_frames[0].shape[:2]
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(output_path, fourcc, 30.0, (width, height))
    
    for frame in video_frames:
        out.write(cv2.cvtColor(frame, cv2.COLOR_RGB2BGR))
    out.release()

def count_humans_in_video(video_path: str) -> int:
    """Count number of humans in video using ResNet50."""
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    
    # Load pre-trained model
    model = resnet50(pretrained=True).to(device)
    model.eval()
    
    # Open video
    cap = cv2.VideoCapture(video_path)
    max_humans = 0
    
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break
            
        # Preprocess frame
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        frame = cv2.resize(frame, (224, 224))
        frame = torch.from_numpy(frame).float().to(device)
        frame = frame.permute(2, 0, 1).unsqueeze(0)
        frame = frame / 255.0
        
        # Get predictions
        with torch.no_grad():
            outputs = model(frame)
            # Person class index in ImageNet is 1
            humans_score = torch.sigmoid(outputs[0][1]).item()
            max_humans = max(max_humans, int(humans_score > 0.5))
    
    cap.release()
    return max_humans

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

def batch_generate_videos(num_files: int, output_pattern: str) -> None:
    """Generate multiple videos using the pattern."""
    for i in range(num_files):
        generate_video(output_pattern.format(i=i))

def process_single_video(input_path: str, output_path: str) -> None:
    """Process a single video and save human count."""
    num_humans = count_humans_in_video(input_path)
    with open(output_path, 'w') as f:
        f.write(str(num_humans))

if __name__ == '__main__':
    import sys
    if len(sys.argv) == 4 and sys.argv[1] == 'generate':
        batch_generate_videos(int(sys.argv[2]), sys.argv[3])
    elif len(sys.argv) == 4 and sys.argv[1] == 'detect':
        process_single_video(sys.argv[2], sys.argv[3])
    elif len(sys.argv) == 3:
        result = sum_human_counts(sys.argv[1], int(sys.argv[2]))
        print(f"TOTAL_HUMANS={result['total_humans']}") 