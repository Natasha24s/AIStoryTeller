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
    Generates story scenes using Claude 3 Sonnet through Amazon Bedrock
    """
    try:
        enhanced_prompt = f"""Create 5 vivid, cinematic scene descriptions for a compelling story about: {user_input}

Please make each scene:
- Rich with sensory details but keep descriptions focused and clear
- Include consistent characters with these guidelines:
    * Introduce characters with specific, detailed descriptions
    * Maintain each character's exact appearance throughout all scenes
    * Use the same names and descriptions for recurring characters
    * Keep character relationships and dynamics consistent
- Set in clear, well-defined locations
- Ensure clear separation between characters and background elements
- Each scene should be 3-4 sentences maximum
- Avoid vague character descriptions like 'someone' or 'a person'
- Maximum 2-3 sentences per scene
- Focus on visual elements and actions
- Clear, specific descriptions
- Mention only key characters and their essential actions
- Avoid complex narrative details
- Each scene description must be under 10 words

Format:
Scene 1: [brief, visual description]
Scene 2: [brief, visual description]
Scene 3: [brief, visual description]
Scene 4: [brief, visual description]
Scene 5: [brief, visual description]"""

        conversation = [
            {
                "role": "user",
                "content": [{"text": enhanced_prompt}],
            }
        ]

        response = bedrock.converse(
            modelId="anthropic.claude-3-sonnet-20240229-v1:0",
            messages=conversation,
            inferenceConfig={
                "maxTokens": 300,
                "temperature": 0.7,
                "topP": 0.9,
                "stopSequences": ["Scene 6"]
            }
        )

        story_text = response["output"]["message"]["content"][0]["text"]
        scene_pattern = re.compile(r'(?:Scene\s*\d+|###\s*Scene\s*\d+|\d+\.)')
        raw_scenes = re.split(scene_pattern, story_text)
        scenes = [scene.strip() for scene in raw_scenes if scene.strip()]
        scenes = [re.sub(r'^.{1,30}:?\s*\n', '', scene).strip() for scene in scenes]
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
            "text": text,
            "negativeText": "blurry, distorted, melting, overlapping elements, inconsistent appearances, changing features, morphing characters"
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
        
        url = f"s3://{BUCKET_NAME}/{key}"
        return url
    except Exception as e:
        print(f"Error saving image to S3: {str(e)}")
        return None

def save_metadata_to_s3(story_id, metadata, scenes):
    """
    Saves metadata and scene information to S3
    """
    try:
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

        scenes_data = {
            f"shot{i+1}_text": scene
            for i, scene in enumerate(scenes)
        }

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

def extract_character_details(story_text):
    """
    Extracts and tracks character details from the story
    """
    characters = {}
    name_pattern = r'([A-Z][a-z]+(?:\s[A-Z][a-z]+)*)'
    
    scenes = story_text.split('Scene')
    for scene in scenes:
        matches = re.finditer(name_pattern, scene)
        for match in matches:
            name = match.group(1)
            if name not in characters:
                sentence = next((s for s in scene.split('.') if name in s), '')
                characters[name] = {
                    'first_appearance': sentence,
                    'scenes_present': [scenes.index(scene) + 1]
                }
            else:
                characters[name]['scenes_present'].append(scenes.index(scene) + 1)
    
    return characters

def enhance_scene_description(scene_text, characters):
    """
    Enhances scene description with consistent character details
    """
    enhanced_text = scene_text
    for name, details in characters.items():
        if any(name in scene_text for name in characters.keys()):
            character_desc = details['first_appearance']
            enhanced_text = f"{enhanced_text}\nEnsure {name} appears exactly as: {character_desc}"
    
    return enhanced_text

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
        
        # Get scenes and full text from Claude 3 Sonnet
        story_data = generate_story_steps(user_input)
        scenes = story_data['scenes']
        full_text = story_data['full_text']
        
        # Extract character details for consistency
        characters = extract_character_details(full_text)
        print(f"Identified characters: {list(characters.keys())}")
        
        print("Generating images for scenes")
        image_urls = []
        
        metadata = {
            'story_id': story_id,
            'topic': user_input,
            'creation_date': datetime.now().isoformat(),
            'scene_count': len(scenes)
        }
        
        # Save metadata and scenes first
        save_metadata_to_s3(story_id, metadata, scenes)
        
        # Generate and save images with character consistency
        for idx, scene in enumerate(scenes):
            print(f"Generating image {idx + 1}/5")
            
            enhanced_scene = enhance_scene_description(scene, characters)
            scene_number = idx + 1
            scene_context = f"""Scene {scene_number} of 5:
            {enhanced_scene} """
            
            if idx > 0:
                time.sleep(2)
            
            try:
                image_base64 = image_from_text(scene_context)
                image_url = save_image_to_s3(
                    image_base64,
                    story_id,
                    scene_number
                )
                
                if image_url:
                    image_urls.append(image_url)
                else:
                    raise Exception(f"Failed to save image {scene_number} to S3")
                
            except Exception as img_error:
                print(f"Error generating image {scene_number}: {str(img_error)}")
                continue

        # Prepare minimal response data
        response_data = {
            'story_id': story_id,
            'topic': user_input,
            'scenes': scenes,
            'image_urls': image_urls,
            'character_names': list(characters.keys())
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
        error_message = str(e)
        print(f"Error in handler: {error_message}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': error_message,
                'error_type': type(e).__name__
            }),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }
