import json
import os
import boto3
import logging
import time
import random
from typing import Dict, Any, Tuple

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
SOURCE_BUCKET = os.environ.get('SOURCE_BUCKET', 'story-story-images')
DESTINATION_BUCKET = os.environ.get('DESTINATION_BUCKET', 'story-video-output')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
MODEL_ID = "amazon.nova-reel-v1:1"
SLEEP_SECONDS = 15
MAX_MONITORING_TIME = 900  # 15 minutes maximum monitoring time

def load_scenes_from_s3(bucket: str, story_id: str) -> dict:
    """Load scenes.json file from S3."""
    try:
        s3_client = boto3.client('s3')
        file_path = f"{story_id}/scenes.json"
        logger.info(f"Attempting to load scenes.json from {bucket}/{file_path}")
        
        response = s3_client.get_object(
            Bucket=bucket,
            Key=file_path
        )
        scenes = json.loads(response['Body'].read().decode('utf-8'))
        logger.info(f"Successfully loaded scenes: {json.dumps(scenes, indent=2)}")
        return scenes
    except Exception as e:
        logger.error(f"Error loading scenes.json from {file_path}: {str(e)}")
        raise

def clean_scene_text(text: str) -> str:
    """Clean and format scene text."""
    try:
        if not text:
            return ""
        
        logger.debug(f"Original text: {text[:100]}...")
        
        # Convert to string if needed
        text = str(text).strip()
        
        # Remove markdown formatting
        text = text.replace('**', '')
        
        # Remove numbering if present
        if text[0].isdigit():
            parts = text.split('.', 1)
            if len(parts) > 1:
                text = parts[1]
        
        # Remove any leading/trailing whitespace
        text = text.strip()
        
        logger.debug(f"Cleaned text: {text[:100]}...")
        return text
    except Exception as e:
        logger.warning(f"Error in clean_scene_text: {str(e)}")
        return text

def check_image_exists(bucket: str, story_id: str, scene_num: int) -> bool:
    """Check if scene image exists in S3."""
    try:
        s3_client = boto3.client('s3')
        image_path = f"{story_id}/scene_{scene_num}.png"
        s3_client.head_object(Bucket=bucket, Key=image_path)
        logger.info(f"Image found: {image_path}")
        return True
    except Exception as e:
        logger.info(f"Image not found: {image_path}")
        return False

def get_model_input(event: dict) -> dict:
    """Create model input configuration."""
    try:
        story_id = event.get('story_id')
        if not story_id:
            raise ValueError("story_id is required in the event")
        
        logger.info(f"Processing story_id: {story_id}")
        
        # Load scenes
        scenes = load_scenes_from_s3(SOURCE_BUCKET, story_id)
        
        # Create shots array
        shots = []
        shot_keys = sorted([k for k in scenes.keys() if k.startswith('shot') and k.endswith('_text')])
        
        logger.info(f"Found {len(shot_keys)} shots to process")
        
        for shot_key in shot_keys:
            if scenes[shot_key]:
                shot_num = int(shot_key.replace('shot', '').replace('_text', ''))
                
                # Create shot with cleaned text
                cleaned_text = clean_scene_text(scenes[shot_key])
                shot = {
                    "text": cleaned_text
                }
                
                # Add image if exists
                if check_image_exists(SOURCE_BUCKET, story_id, shot_num):
                    shot["image"] = {
                        "format": "png",
                        "source": {
                            "s3Location": {
                                "uri": f"s3://{SOURCE_BUCKET}/{story_id}/scene_{shot_num}.png"
                            }
                        }
                    }
                
                shots.append(shot)
                logger.info(f"Processed {shot_key} successfully")
        
        if not shots:
            raise ValueError("No valid shots found in scenes.json")
        
        model_input = {
            "taskType": "MULTI_SHOT_MANUAL",
            "multiShotManualParams": {
                "shots": shots
            },
            "videoGenerationConfig": {
                "seed": event.get('seed', random.randint(0, 2147483648)),
                "fps": 24,
                "dimension": "1280x720"
            }
        }
        
        logger.info(f"Created model input: {json.dumps(model_input, indent=2)}")
        return model_input
        
    except Exception as e:
        logger.error(f"Error in get_model_input: {str(e)}")
        raise

def monitor_video_generation(bedrock_client, invocation_arn: str) -> Tuple[str, str]:
    """Monitor the video generation process and return status and output location"""
    job_id = invocation_arn.split("/")[-1]
    s3_location = f"s3://{DESTINATION_BUCKET}/{job_id}"
    start_time = time.time()
    
    logger.info(f"Monitoring job folder: {s3_location}")
    
    while True:
        try:
            response = bedrock_client.get_async_invoke(invocationArn=invocation_arn)
            status = response["status"]
            logger.info(f"Status: {status}")
            
            # Check if the process is complete
            if status != "InProgress":
                break
                
            # Check if we've exceeded maximum monitoring time
            if time.time() - start_time > MAX_MONITORING_TIME:
                logger.warning("Maximum monitoring time exceeded")
                return "Timeout", s3_location
                
            time.sleep(SLEEP_SECONDS)
            
        except Exception as e:
            logger.error(f"Error monitoring video generation: {str(e)}")
            return "Error", s3_location
    
    output_location = f"{s3_location}/output.mp4" if status == "Completed" else None
    return status, output_location

def start_video_generation(bedrock_client, model_input: dict) -> Dict:
    """Start the video generation process"""
    try:
        invocation = bedrock_client.start_async_invoke(
            modelId=MODEL_ID,
            modelInput=model_input,
            outputDataConfig={
                "s3OutputDataConfig": {
                    "s3Uri": f"s3://{DESTINATION_BUCKET}"
                }
            }
        )
        return invocation
    except Exception as e:
        logger.error(f"Error starting video generation: {str(e)}")
        raise

def validate_environment() -> None:
    """Validate required environment variables"""
    if not SOURCE_BUCKET or not DESTINATION_BUCKET:
        raise ValueError("SOURCE_BUCKET and DESTINATION_BUCKET environment variables must be set")

def create_error_response(error: Exception, status_code: int = 500) -> Dict[str, Any]:
    """Create standardized error response"""
    return {
        'statusCode': status_code,
        'body': {
            'error': str(error),
            'status': 'Error',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
    }

def handler(event: dict, context: Any) -> Dict[str, Any]:
    """Lambda function handler."""
    try:
        logger.info(f"Received event: {json.dumps(event)}")
        
        # Validate environment
        validate_environment()

        # Initialize Bedrock client
        bedrock_client = boto3.client(
            service_name="bedrock-runtime",
            region_name=AWS_REGION
        )
        
        # Get model input configuration
        model_input = get_model_input(event)
        
        # Start video generation
        invocation = start_video_generation(bedrock_client, model_input)
        invocation_arn = invocation["invocationArn"]
        
        # Monitor the generation process
        status, output_location = monitor_video_generation(bedrock_client, invocation_arn)
        
        # Prepare response based on status
        response = {
            'statusCode': 200,
            'body': {
                'status': status,
                'source_bucket': SOURCE_BUCKET,
                'destination_bucket': DESTINATION_BUCKET,
                'invocation_details': invocation,
                'output_location': output_location,
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            }
        }
        
        # Add additional information for different statuses
        if status == "Completed":
            response['body']['message'] = 'Video generation completed successfully'
        elif status == "Failed":
            response['statusCode'] = 500
            response['body']['message'] = 'Video generation failed'
        elif status == "Timeout":
            response['statusCode'] = 408
            response['body']['message'] = 'Video generation monitoring timed out'
        else:
            response['body']['message'] = f'Video generation status: {status}'
        
        logger.info(f"Final response: {json.dumps(response, default=str)}")
        return response

    except Exception as err:
        logger.error(f"Error in lambda_handler: {str(err)}", exc_info=True)
        return create_error_response(err)

