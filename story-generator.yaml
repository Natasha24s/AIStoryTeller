import json
import boto3
import base64
import time
from botocore.config import Config
from datetime import datetime
import uuid
import re
import os

# Get bucket name from environment variable
BUCKET_NAME = os.environ['BUCKET_NAME']

# Define the target resolution
TARGET_WIDTH = 1280
TARGET_HEIGHT = 720

# Create the clients
bedrock = boto3.client(
    service_name='bedrock-runtime',
    region_name="us-east-1",
    config=Config(read_timeout=300)
)
s3 = boto3.client('s3')

def sanitize_topic(topic):
    """
    Sanitizes the topic string for use in file names
    """
    sanitized = topic.lower().replace(' ', '_')
    sanitized = re.sub(r'[^a-z0-9_]', '', sanitized)
    return sanitized[:30]

def generate_story_id(topic):
    """
    Generates a unique story ID using date, topic, and UUID
    """
    date_str = datetime.now().strftime('%Y%m%d')
    topic_str = sanitize_topic(topic)
    unique_id = str(uuid.uuid4())[:6]
    return f"{date_str}_{topic_str}_{unique_id}"

def generate_story_steps(user_input):
    """
    Generates story scenes using Nova-Lite model
    """
    try:
        conversation = [
            {
                "role": "user",
                "content": [{"text": f"Create 5 visual scene descriptions for a story about: {user_input}"}],
            }
        ]

        response = bedrock.converse(
            modelId="amazon.nova-lite-v1:0",
            messages=conversation,
            inferenceConfig={
                "maxTokens": 1000,
                "temperature": 0.7,
                "topP": 0.9,
            }
        )

        # Get the generated text from Nova-Lite
        story_text = response["output"]["message"]["content"][0]["text"]
        
        # Clean up the text by removing scene numbers and titles
        scenes = []
        for line in story_text.split('\n'):
            line = line.strip()
            # Skip empty lines and lines that are scene titles
            if not line or line.startswith('###') or line.startswith('Scene'):
                continue
            scenes.append(line)
        
        # Ensure we have exactly 5 scenes
        scenes = scenes[:5]
        while len(scenes) < 5:
            scenes.append(f"Scene {len(scenes) + 1} about {user_input}")
            
        return {
            'scenes': scenes,
            'full_text': story_text
        }
        
    except Exception as e:
        print(f"Error in generate_story_steps: {str(e)}")
        default_scenes = [f"Scene {i} about {user_input}" for i in range(1, 6)]
        return {
            'scenes': default_scenes,
            'full_text': '\n'.join(default_scenes)
        }

def image_from_text(text):
    """
    Generates an image from text using Nova-Canvas model
    """
    body = json.dumps({
        "taskType": "TEXT_IMAGE",
        "textToImageParams": {
            "text": text
        },
        "imageGenerationConfig": {
            "numberOfImages": 1,
            "width": TARGET_WIDTH,
            "height": TARGET_HEIGHT,
            "cfgScale": 8.0,
            "seed": 0
        }
    })
    
    response = bedrock.invoke_model(
        body=body,
        modelId="amazon.nova-canvas-v1:0",
        accept="application/json",
        contentType="application/json"
    )
    
    response_body = json.loads(response.get("body").read())
    return response_body.get("images")[0]

def save_image_to_s3(image_base64, story_id, scene_number):
    """
    Saves a base64 encoded image to S3 and returns the URL
    """
    try:
        image_data = base64.b64decode(image_base64)
        key = f"{story_id}/scene_{scene_number}.png"
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=image_data,
            ContentType='image/png'
        )
        
        url = f"https://{BUCKET_NAME}.s3.amazonaws.com/{key}"
        return url
    except Exception as e:
        print(f"Error saving image to S3: {str(e)}")
        return None

def save_metadata_to_s3(story_id, metadata, scenes):
    """
    Saves metadata and scene information to S3
    """
    try:
        # Save the original metadata
        metadata['image_resolution'] = {
            'width': TARGET_WIDTH,
            'height': TARGET_HEIGHT
        }
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{story_id}/metadata.json",
            Body=json.dumps(metadata),
            ContentType='application/json'
        )

        # Create scenes data in the requested format
        scenes_data = {
            f"shot{i+1}_text": scene
            for i, scene in enumerate(scenes)
        }

        # Save the scenes JSON
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{story_id}/scenes.json",
            Body=json.dumps(scenes_data, indent=2),
            ContentType='application/json'
        )
        
        return True
    except Exception as e:
        print(f"Error saving metadata to S3: {str(e)}")
        return False

def handler(event, context):
    """
    Lambda handler function
    """
    try:
        # Parse the incoming event
        if isinstance(event, dict):
            if 'body' in event:
                body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
            else:
                body = event
        else:
            body = json.loads(event)

        # Validate input
        user_input = body.get('topic')
        if not user_input:
            raise ValueError("Topic is required")
        
        # Generate story ID and scenes
        story_id = generate_story_id(user_input)
        print(f"Generating story for topic: {user_input}")
        
        # Get scenes and full text from Nova-Lite
        story_data = generate_story_steps(user_input)
        scenes = story_data['scenes']
        full_text = story_data['full_text']
        
        print("Generating images for scenes")
        images = []
        image_urls = []
        
        metadata = {
            'story_id': story_id,
            'story_topic': user_input,
            'creation_date': datetime.now().isoformat(),
            'scene_count': len(scenes),
            'scenes': scenes,
            'full_text': full_text,  # Store the full text in metadata
            'image_format': 'png'
        }
        
        # Save metadata and scenes
        save_metadata_to_s3(story_id, metadata, scenes)
        
        # Generate and save images
        for idx, scene in enumerate(scenes):
            print(f"Generating image {idx + 1}/5")
            if idx > 0:
                time.sleep(2)  # Rate limiting
            
            image_base64 = image_from_text(scene)
            image_url = save_image_to_s3(
                image_base64,
                story_id,
                idx + 1
            )
            
            images.append(image_base64)
            image_urls.append(image_url)
        
        response_data = {
            'story_id': story_id,
            'topic': user_input,
            'scenes': scenes,
            'full_text': full_text,
            'images': images,
            'image_urls': image_urls,
            'metadata': metadata
        }

        return {
            'statusCode': 200,
            'body': json.dumps(response_data),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            }),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }
