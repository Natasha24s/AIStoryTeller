import json
import boto3
import os
import time
import logging
from typing import Dict, Any, Tuple
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)
SLEEP_SECONDS = 15
MAX_MONITORING_TIME = 900
INPUT_BUCKET = os.environ['SOURCE_BUCKET']
OUTPUT_BUCKET = os.environ['DESTINATION_BUCKET']

def extract_job_id(response):
    """Extract job ID from Bedrock response"""
    try:
        # The requestToken in the response contains the job ID
        request_token = response.get('requestToken')
        if request_token:
            return request_token.split('-')[0]  # First part of the token is the job ID
        
        # Alternative: try getting from invocationId
        invocation_id = response.get('invocationId')
        if invocation_id:
            return invocation_id
        
        # If neither is available, use part of the invocationArn
        invocation_arn = response.get('invocationArn', '')
        return invocation_arn.split('/')[-1]
        
    except Exception as e:
        logger.error(f"Error extracting job ID: {str(e)}")
        return None

def handler(event, context):
    s3_client = boto3.client('s3')
    bedrock_client = boto3.client('bedrock-runtime')
    
    try:
        if isinstance(event, dict):
            if 'body' in event:
                body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
            else:
                body = event
        else:
            body = json.loads(event)

        story_id = body.get('story_id')
        if not story_id:
            raise ValueError("story_id is required")

        logger.info(f"Processing story_id: {story_id}")

        scene_json_response = s3_client.get_object(
            Bucket=INPUT_BUCKET,
            Key=f"{story_id}/scenes.json"
        )
        scene_data = json.loads(scene_json_response['Body'].read().decode('utf-8'))
        
        shots = []
        for i in range(1, 6):
            shot = {
                "text": scene_data[f"shot{i}_text"].strip(),
                "image": {
                    "format": "png",
                    "source": {
                        "s3Location": {
                            "uri": f"s3://{INPUT_BUCKET}/{story_id}/scene_{i}.png"
                        }
                    }
                }
            }
            shots.append(shot)
        
        request_body = {
            "taskType": "MULTI_SHOT_MANUAL",
            "multiShotManualParams": {
                "shots": shots
            },
            "videoGenerationConfig": {
                "fps": 24,
                "dimension": "1280x720",
                "seed": 42
            }
        }
        
        logger.info(f"Request body: {json.dumps(request_body, indent=2)}")
        
        # Start async video generation
        invoke_response = bedrock_client.start_async_invoke(
            modelId='amazon.nova-reel-v1:1',
            modelInput=request_body,
            outputDataConfig={
                "s3OutputDataConfig": {
                    "s3Uri": f"s3://{OUTPUT_BUCKET}/{story_id}/"
                }
            }
        )
        
        # Extract job ID and invocation ARN
        job_id = extract_job_id(invoke_response)
        invocation_arn = invoke_response["invocationArn"]
        
        logger.info(f"Started async job with ID: {job_id}")
        logger.info(f"Invocation ARN: {invocation_arn}")
        
        status, output_location = monitor_video_generation(
            bedrock_client, 
            invocation_arn, 
            story_id,
            job_id
        )

        response = {
            'status': status,
            'story_id': story_id,
            'source_bucket': INPUT_BUCKET,
            'destination_bucket': OUTPUT_BUCKET,
            'output_location': output_location or f"s3://{OUTPUT_BUCKET}/{story_id}/{job_id}/output.mp4",
            'job_id': job_id,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        if status == "Completed":
            response['message'] = 'Video generation completed successfully'
        elif status == "Failed":
            response['message'] = 'Video generation failed'
        elif status == "Timeout":
            response['message'] = 'Video generation monitoring timed out'
        else:
            response['message'] = f'Video generation status: {status}'
            
        logger.info(f"Final response: {json.dumps(response, default=str)}")
        return response

    except Exception as err:
        logger.error(f"Error in lambda_handler: {str(err)}", exc_info=True)
        return {
            'error': str(err),
            'status': 'Error',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }

def monitor_video_generation(bedrock_client, invocation_arn: str, story_id: str, job_id: str) -> Tuple[str, str]:
    start_time = time.time()
    
    logger.info(f"Monitoring job with ID: {job_id}")
    expected_path = f"{story_id}/{job_id}/output.mp4"
    logger.info(f"Expected output path: {expected_path}")
    
    while True:
        try:
            response = bedrock_client.get_async_invoke(invocationArn=invocation_arn)
            status = response["status"]
            logger.info(f"Status: {status}")
            
            if status != "InProgress":
                break
                    
            if time.time() - start_time > MAX_MONITORING_TIME:
                logger.warning("Maximum monitoring time exceeded")
                return "Timeout", None
                    
            time.sleep(SLEEP_SECONDS)
                
        except Exception as e:
            logger.error(f"Error monitoring video generation: {str(e)}")
            return "Error", None

    if status == "Completed":
        output_location = f"s3://{OUTPUT_BUCKET}/{expected_path}"
        logger.info(f"Job completed. Output at: {output_location}")
        return status, output_location
    
    return status, None
