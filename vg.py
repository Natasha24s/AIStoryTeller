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

        print(f"Processing story_id: {story_id}")

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
        
        print("Request body:", json.dumps(request_body, indent=2))
        
        response = bedrock_client.start_async_invoke(
            modelId='amazon.nova-reel-v1:1',
            modelInput=request_body,
            outputDataConfig={
                "s3OutputDataConfig": {
                    "s3Uri": f"s3://{OUTPUT_BUCKET}/{story_id}/"
                }
            }
        )
        invocation_arn = response["invocationArn"]
        status, output_location = monitor_video_generation(bedrock_client, invocation_arn)

        response = {
            'status': status,
            'story_id': story_id,
            'source_bucket': INPUT_BUCKET,
            'destination_bucket': OUTPUT_BUCKET,
            'output_location': output_location,
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

def monitor_video_generation(bedrock_client, invocation_arn: str) -> Tuple[str, str]:
    job_id = invocation_arn.split("/")[-1]
    s3_location = f"s3://{OUTPUT_BUCKET}/{job_id}"
    start_time = time.time()
    
    logger.info(f"Monitoring job folder: {s3_location}")
    
    while True:
        try:
            response = bedrock_client.get_async_invoke(invocationArn=invocation_arn)
            status = response["status"]
            logger.info(f"Status: {status}")
            
            if status != "InProgress":
                break
                    
            if time.time() - start_time > MAX_MONITORING_TIME:
                logger.warning("Maximum monitoring time exceeded")
                return "Timeout", s3_location
                    
            time.sleep(SLEEP_SECONDS)
                
        except Exception as e:
            logger.error(f"Error monitoring video generation: {str(e)}")
            return "Error", s3_location
            
    output_location = f"{s3_location}/output.mp4" if status == "Completed" else None
    return status, output_location
