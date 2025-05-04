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
    """
    Wait for and return the actual Polly output file path with increased timeout and better monitoring
    """
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
                
                # Extract the key from the output URI
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
        
        # Parse video path
        video_path = video_path.replace('s3://', '')
        video_bucket = video_path.split('/')[0]
        video_key = '/'.join(video_path.split('/')[1:])
        
        source_bucket = os.environ['SOURCE_BUCKET']
        destination_bucket = os.environ['DESTINATION_BUCKET']
        
        try:
            logger.info(f"Starting Polly synthesis for story_id: {story_id}")
            
            timestamp = int(time.time())
            audio_prefix = f"{story_id}/audio/speech_{timestamp}"
            
            # Start Polly synthesis with enhanced settings
            polly_response = polly_client.start_speech_synthesis_task(
                Engine='neural',
                LanguageCode='en-US',
                OutputFormat='mp3',
                OutputS3BucketName=destination_bucket,
                OutputS3KeyPrefix=audio_prefix,
                Text=polly_input,
                VoiceId='Ruth',
                SampleRate='24000',  # Match with video frame rate
                TextType='text'
            )
            
            task_id = polly_response['SynthesisTask']['TaskId']
            logger.info(f"Polly task started with ID: {task_id}")
            
            # Wait for Polly file with increased timeout
            actual_audio_key = get_polly_output_file(
                s3_client, 
                destination_bucket, 
                f"{story_id}/audio/",
                task_id,
                max_attempts=60,  # Increased to 10 minutes total wait time
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
            
            # Verify the audio file exists
            try:
                s3_client.head_object(
                    Bucket=destination_bucket,
                    Key=actual_audio_key
                )
            except Exception as e:
                logger.error(f"Audio file not found in S3: {str(e)}")
                return {
                    'statusCode': 500,
                    'body': {
                        'message': 'Audio file not found in S3 after successful Polly task',
                        'story_id': story_id,
                        'polly_task_id': task_id
                    }
                }

            try:
                job_settings = get_job_settings()
                
                # Set input settings with modified audio selectors
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
                job_settings['Settings']['OutputGroups'][0]['OutputGroupSettings']['FileGroupSettings']['Destination'] = \
                    f"s3://{destination_bucket}/{story_id}/final/final_output.mp4"
                
                logger.info(f"Creating MediaConvert job for story_id: {story_id}")
                logger.info(f"Using video input: s3://{video_bucket}/{video_key}")
                
                mediaconvert_response = mediaconvert_client.create_job(**job_settings)
                
                return {
                    'statusCode': 200,
                    'body': {
                        'message': 'Processing jobs started successfully',
                        'mediaconvert_job_id': mediaconvert_response['Job']['Id'],
                        'polly_task_id': task_id,
                        'story_id': story_id,
                        'input_paths': {
                            'video': f"s3://{video_bucket}/{video_key}",
                            'audio': f"s3://{destination_bucket}/{actual_audio_key}"
                        },
                        'output_path': f"s3://{destination_bucket}/{story_id}/final/final_output.mp4",
                        'status': {
                            'polly': 'COMPLETED',
                            'mediaconvert': 'SUBMITTED'
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
