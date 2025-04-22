import json
import os
import boto3
import logging
from typing import Dict, Any

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
SOURCE_BUCKET = os.environ.get('SOURCE_BUCKET', 'story-story-images')
DESTINATION_BUCKET = os.environ.get('DESTINATION_BUCKET', 'story-video-output')

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
        
        # Log original text
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
        
        # Log cleaned text
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
                "seed": event.get('seed', 1234),
                "fps": 24,
                "dimension": "1280x720"
            }
        }
        
        logger.info(f"Created model input: {json.dumps(model_input, indent=2)}")
        return model_input
        
    except Exception as e:
        logger.error(f"Error in get_model_input: {str(e)}")
        raise

def lambda_handler(event: dict, context: Any) -> Dict[str, Any]:
    """Lambda function handler."""
    try:
        logger.info(f"Received event: {json.dumps(event)}")
        
        if not SOURCE_BUCKET or not DESTINATION_BUCKET:
            raise ValueError("SOURCE_BUCKET and DESTINATION_BUCKET environment variables must be set")

        bedrock_client = boto3.client(
            service_name="bedrock-runtime",
            region_name=os.environ.get('AWS_REGION', 'us-east-1')
        )
        
        model_input = get_model_input(event)
        
        invocation = bedrock_client.start_async_invoke(
            modelId="amazon.nova-reel-v1:1",
            modelInput=model_input,
            outputDataConfig={
                "s3OutputDataConfig": {
                    "s3Uri": f"s3://{DESTINATION_BUCKET}"
                }
            }
        )
        
        response = {
            'statusCode': 200,
            'body': {
                'message': 'Video generation started successfully',
                'source_bucket': SOURCE_BUCKET,
                'destination_bucket': DESTINATION_BUCKET,
                'invocation_details': invocation
            }
        }
        
        logger.info(f"Success response: {json.dumps(response, default=str)}")
        return response

    except Exception as err:
        logger.error(f"Error in lambda_handler: {str(err)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': {
                'error': str(err)
            }
        }
