import boto3
import json
import os
import time
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_mediaconvert_endpoint():
    """Get MediaConvert endpoint for the current region"""
    try:
        mediaconvert_client = boto3.client('mediaconvert')
        response = mediaconvert_client.describe_endpoints()
        return response['Endpoints'][0]['Url']
    except Exception as e:
        logger.error(f"Error getting MediaConvert endpoint: {str(e)}")
        raise

def verify_file_exists(s3_client, bucket, key):
    """
    Verify that a file exists in S3
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as e:
        logger.error(f"File not found - Bucket: {bucket}, Key: {key}, Error: {str(e)}")
        return False

def get_job_settings():
    """Return MediaConvert job settings with audio mixing"""
    sts_client = boto3.client('sts')
    account_id = sts_client.get_caller_identity()['Account']
    region = os.environ.get('AWS_REGION', 'us-east-1')
    
    return {
        "Queue": f"arn:aws:mediaconvert:{region}:{account_id}:queues/Default",
        "UserMetadata": {},
        "Role": os.environ['MEDIACONVERT_ROLE_ARN'],
        "Settings": {
            "TimecodeConfig": {
                "Source": "ZEROBASED"
            },
            "OutputGroups": [
                {
                    "CustomName": "output",
                    "Name": "File Group",
                    "Outputs": [
                        {
                            "ContainerSettings": {
                                "Container": "MP4",
                                "Mp4Settings": {}
                            },
                            "VideoDescription": {
                                "CodecSettings": {
                                    "Codec": "H_264",
                                    "H264Settings": {
                                        "MaxBitrate": 5000000,
                                        "RateControlMode": "QVBR",
                                        "SceneChangeDetect": "TRANSITION_DETECTION"
                                    }
                                }
                            },
                            "AudioDescriptions": [
                                {
                                    "AudioSourceName": "Audio Selector 2",
                                    "AudioNormalizationSettings": {
                                        "Algorithm": "ITU_BS_1770_3",
                                        "AlgorithmControl": "CORRECT_AUDIO",
                                        "TargetLkfs": -23
                                    },
                                    "CodecSettings": {
                                        "Codec": "AAC",
                                        "AacSettings": {
                                            "Bitrate": 96000,
                                            "CodingMode": "CODING_MODE_2_0",
                                            "SampleRate": 48000
                                        }
                                    }
                                }
                            ]
                        }
                    ],
                    "OutputGroupSettings": {
                        "Type": "FILE_GROUP_SETTINGS",
                        "FileGroupSettings": {
                            "Destination": "",
                            "DestinationSettings": {
                                "S3Settings": {
                                    "StorageClass": "STANDARD"
                                }
                            }
                        }
                    }
                }
            ],
            "Inputs": []
        },
        "AccelerationSettings": {
            "Mode": "DISABLED"
        },
        "StatusUpdateInterval": "SECONDS_60",
        "Priority": 0
    }

def get_polly_output_file(s3_client, bucket, prefix, task_id, max_attempts=60, delay=10):
    """Wait for and return the actual Polly output file path"""
    logger.info(f"Waiting for Polly file in bucket: {bucket}, prefix: {prefix}, task_id: {task_id}")
    
    for attempt in range(max_attempts):
        try:
            # Check Polly task status
            polly_client = boto3.client('polly')
            task_status = polly_client.get_speech_synthesis_task(TaskId=task_id)
            task_state = task_status['SynthesisTask']['TaskStatus']
            
            logger.info(f"Polly task status (Attempt {attempt + 1}/{max_attempts}): {task_state}")
            
            if task_state == 'completed':
                # Get the output URL directly from the task status
                output_uri = task_status['SynthesisTask']['OutputUri']
                output_key = output_uri.split(bucket + '/')[-1]
                logger.info(f"Found Polly output file: {output_key}")
                return output_key
                
            elif task_state == 'failed':
                error_message = task_status['SynthesisTask'].get('TaskStatusReason', 'Unknown error')
                logger.error(f"Polly task failed: {error_message}")
                raise Exception(f"Polly task failed: {error_message}")
            
            elif task_state == 'scheduled' or task_state == 'inProgress':
                logger.info(f"Polly task still processing. Waiting {delay} seconds...")
                time.sleep(delay)
                continue
            
        except Exception as e:
            logger.error(f"Error checking Polly file (Attempt {attempt + 1}): {str(e)}")
            if attempt == max_attempts - 1:
                raise
            time.sleep(delay)
            continue
    
    raise Exception(f"Timeout waiting for Polly file after {max_attempts} attempts")

def wait_for_mediaconvert_job(mediaconvert_client, job_id, max_attempts=30, delay=10):
    """Wait for MediaConvert job to complete"""
    logger.info(f"Waiting for MediaConvert job {job_id} to complete")
    
    for attempt in range(max_attempts):
        try:
            response = mediaconvert_client.get_job(Id=job_id)
            status = response['Job']['Status']
            
            logger.info(f"MediaConvert job status (Attempt {attempt + 1}/{max_attempts}): {status}")
            
            if status == 'COMPLETE':
                logger.info("MediaConvert job completed successfully")
                return True, None
            elif status in ['ERROR', 'CANCELED']:
                error_message = response['Job'].get('ErrorMessage', 'Unknown error')
                logger.error(f"MediaConvert job failed: {error_message}")
                return False, error_message
            
            logger.info(f"Waiting {delay} seconds before next check...")
            time.sleep(delay)
            
        except Exception as e:
            logger.error(f"Error checking MediaConvert job: {str(e)}")
            if attempt == max_attempts - 1:
                return False, str(e)
            time.sleep(delay)
    
    return False, "Timeout waiting for MediaConvert job"

def lambda_handler(event, context):
    try:
        # Initialize AWS clients
        s3_client = boto3.client('s3')
        polly_client = boto3.client('polly')
        
        # Get MediaConvert endpoint
        endpoint_url = os.environ.get('MEDIACONVERT_ENDPOINT')
        if not endpoint_url:
            endpoint_url = get_mediaconvert_endpoint()
        
        mediaconvert_client = boto3.client('mediaconvert', endpoint_url=endpoint_url)
        
        # Get input parameters
        story_id = event.get('story_id')
        polly_input = event.get('polly_input')
        video_path = event.get('video_path')
        
        if not story_id or not polly_input or not video_path:
            return {
                'statusCode': 400,
                'body': {
                    'message': 'Missing required parameters. story_id, polly_input, and video_path are required.',
                    'story_id': story_id
                }
            }
        
        # Parse video path and handle Nova Reel's output path structure
        video_path = video_path.replace('s3://', '')
        video_bucket = video_path.split('/')[0]
        
        # Check if the path contains story_id
        path_parts = video_path.split('/')
        if len(path_parts) > 3:  # If path includes story_id
            video_key = '/'.join(path_parts[1:])  # Include story_id in the path
        else:
            video_key = '/'.join(path_parts[1:])

        logger.info(f"Parsed video path - Bucket: {video_bucket}, Key: {video_key}")

        # Verify video file exists
        if not verify_file_exists(s3_client, video_bucket, video_key):
            # Try alternative path with story_id
            alternative_key = f"{story_id}/{video_key}"
            logger.info(f"Trying alternative path: {alternative_key}")
            
            if verify_file_exists(s3_client, video_bucket, alternative_key):
                video_key = alternative_key
                logger.info(f"Found video at alternative path")
            else:
                return {
                    'statusCode': 500,
                    'body': {
                        'message': f'Input video file not found at either path: \n1. s3://{video_bucket}/{video_key}\n2. s3://{video_bucket}/{alternative_key}',
                        'story_id': story_id
                    }
                }
        
        source_bucket = os.environ['SOURCE_BUCKET']
        destination_bucket = os.environ['DESTINATION_BUCKET']
        
        try:
            logger.info(f"Starting Polly synthesis for story_id: {story_id}")
            
            timestamp = int(time.time())
            audio_prefix = f"{story_id}/audio/speech_{timestamp}"
            
            polly_response = polly_client.start_speech_synthesis_task(
                Engine='neural',
                LanguageCode='en-US',
                OutputFormat='mp3',
                OutputS3BucketName=destination_bucket,
                OutputS3KeyPrefix=audio_prefix,
                Text=polly_input,
                VoiceId='Ruth',
                SampleRate='24000',
                TextType='text'
            )
            
            task_id = polly_response['SynthesisTask']['TaskId']
            logger.info(f"Polly task started with ID: {task_id}")
            
            actual_audio_key = get_polly_output_file(
                s3_client, 
                destination_bucket, 
                f"{story_id}/audio/",
                task_id,
                max_attempts=60,
                delay=10
            )
            
            if not actual_audio_key:
                return {
                    'statusCode': 500,
                    'body': {
                        'message': 'Failed to locate Polly output file',
                        'story_id': story_id,
                        'polly_task_id': task_id
                    }
                }
            
            logger.info(f"Found Polly output file: {actual_audio_key}")
            
            try:
                job_settings = get_job_settings()
                
                input_config = {
                    'FileInput': f"s3://{video_bucket}/{video_key}",
                    'AudioSelectors': {
                        'Audio Selector 2': {
                            'ExternalAudioFileInput': f"s3://{destination_bucket}/{actual_audio_key}"
                        }
                    }
                }
                logger.info(f"MediaConvert input configuration: {json.dumps(input_config)}")
                
                job_settings['Settings']['Inputs'] = [{
                    'AudioSelectors': {
                        'Audio Selector 1': {
                            'DefaultSelection': 'DEFAULT',
                            'SelectorType': 'TRACK',
                            'Tracks': [1],
                            'Offset': 0
                        },
                        'Audio Selector 2': {
                            'DefaultSelection': 'DEFAULT',
                            'ExternalAudioFileInput': f"s3://{destination_bucket}/{actual_audio_key}",
                            'SelectorType': 'TRACK',
                            'Tracks': [1],
                            'Offset': 0,
                            'ProgramSelection': 1
                        }
                    },
                    'AudioSelectorGroups': {
                        'Audio Selector Group 1': {
                            'AudioSelectorNames': ['Audio Selector 2']
                        }
                    },
                    'VideoSelector': {},
                    'TimecodeSource': 'ZEROBASED',
                    'FileInput': f"s3://{video_bucket}/{video_key}"
                }]
                
                # Set output location
                output_key = f"{story_id}/final/final_output.mp4"
                job_settings['Settings']['OutputGroups'][0]['OutputGroupSettings']['FileGroupSettings']['Destination'] = \
                    f"s3://{destination_bucket}/{output_key}"
                
                logger.info(f"Creating MediaConvert job for story_id: {story_id}")
                logger.info(f"Using video input: s3://{video_bucket}/{video_key}")
                
                mediaconvert_response = mediaconvert_client.create_job(**job_settings)
                job_id = mediaconvert_response['Job']['Id']

                # Wait for MediaConvert job to complete
                success, error = wait_for_mediaconvert_job(
                    mediaconvert_client,
                    job_id,
                    max_attempts=30,
                    delay=10
                )

                if not success:
                    return {
                        'statusCode': 500,
                        'body': {
                            'message': f"MediaConvert job failed: {error}",
                            'story_id': story_id,
                            'job_id': job_id
                        }
                    }

                # Verify the output file exists
                if not verify_file_exists(s3_client, destination_bucket, output_key):
                    return {
                        'statusCode': 500,
                        'body': {
                            'message': 'MediaConvert output file not found',
                            'story_id': story_id,
                            'job_id': job_id
                        }
                    }
                
                return {
                    'statusCode': 200,
                    'body': {
                        'message': 'Processing completed successfully',
                        'mediaconvert_job_id': job_id,
                        'polly_task_id': task_id,
                        'story_id': story_id,
                        'input_paths': {
                            'video': f"s3://{video_bucket}/{video_key}",
                            'audio': f"s3://{destination_bucket}/{actual_audio_key}"
                        },
                        'output_path': f"s3://{destination_bucket}/{output_key}",
                        'status': {
                            'polly': 'COMPLETED',
                            'mediaconvert': 'COMPLETED'
                        }
                    }
                }
                
            except ClientError as e:
                error_message = str(e)
                logger.error(f"MediaConvert error: {error_message}")
                return {
                    'statusCode': 500,
                    'body': {
                        'message': f"Error in MediaConvert job creation: {error_message}",
                        'story_id': story_id,
                        'polly_task_id': task_id
                    }
                }
                
        except ClientError as e:
            error_message = str(e)
            logger.error(f"Polly error: {error_message}")
            return {
                'statusCode': 500,
                'body': {
                    'message': f"Error in Polly synthesis: {error_message}",
                    'story_id': story_id
                }
            }
            
    except Exception as e:
        error_message = str(e)
        logger.error(f"General error: {error_message}")
        return {
            'statusCode': 500,
            'body': {
                'message': f"General error: {error_message}",
                'story_id': story_id if 'story_id' in locals() else None
            }
        }
