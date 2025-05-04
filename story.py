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

def generate_story_description(full_text):
    """
    Generates a 30-second narrative from the full story text using Claude
    """
    try:
        prompt = f"""Create a concise, engaging 30-second narration from this story. 
        Focus on the main character's journey and key moments.
        The narration should flow naturally and be suitable for voice-over.
        Keep it under 100 words while maintaining story impact.

        Story text:
        {full_text}

        Requirements:
        - Start with an engaging introduction of the main character
        - Highlight 2-3 key moments
        - End with the resolution
        - Use natural, conversational language
        - Maintain emotional connection
        - Keep it concise for 30-second narration

        Format: Single paragraph narrative suitable for voice-over."""

        conversation = [
            {
                "role": "user",
                "content": [{"text": prompt}],
            }
        ]

        response = bedrock.converse(
            modelId="anthropic.claude-3-sonnet-20240229-v1:0",
            messages=conversation,
            inferenceConfig={
                "maxTokens": 200,
                "temperature": 0.7,
                "topP": 0.9
            }
        )

        narrative = response["output"]["message"]["content"][0]["text"].strip()
        return narrative

    except Exception as e:
        print(f"Error generating story description: {str(e)}")
        return "A story unfolds across five scenes."
def generate_story_steps(user_input):
    """
    Generates story scenes using Claude 3 Sonnet through Amazon Bedrock
    """
    try:
        enhanced_prompt = f"""Create 5 sequential scenes telling a story about: {user_input}

Story arc requirements:
1. Scene 1 (Introduction): Establish main character and setting, introduce the basic situation
2. Scene 2 (Rising Action): Show first challenge or development
3. Scene 3 (Rising Action): Increase tension or progress
4. Scene 4 (Climax): Show the peak moment or main achievement
5. Scene 5 (Resolution): Show the outcome or conclusion

Format each scene as:
Scene X: [Shot type] - [Character details] - [Action] - [Setting] - [Lighting]

Character consistency:
- Maintain exact same character description across all scenes
- Format: Name (age gender, physical details, clothing)
- Maximum 3 characters per scene

Technical requirements:
- Each scene under 20 words
- Include shot type (Close-up, Medium, Wide, Full)
- Clear lighting conditions
- Single focused action
- Simple setting"""

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
    Generates an image from text using Nova-Canvas model with improved parameters
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
        current_time = datetime.now().isoformat()  
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=image_data,
            ContentType='image/png',
            Metadata={
                'created-date': current_time,
                'last-modified-date': current_time
            }
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
        current_time = datetime.now().isoformat()  
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{story_id}/scenes.json",
            Body=json.dumps(scenes_data, indent=2),
            ContentType='application/json',
            Metadata={
                'created-date': current_time,
                'last-modified-date': current_time
            }
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
        
        # Get scenes and full text from Claude 3 Sonnet
        story_data = generate_story_steps(user_input)
        scenes = story_data['scenes']
        full_text = story_data['full_text']
        
        # Generate the narrative for Polly
        polly_input = generate_story_description(full_text)
        
        print("Generating images for scenes")
        images = []
        image_urls = []
        
        metadata = {
            'story_id': story_id,
            'topic': user_input,
            'creation_date': datetime.now().isoformat(),
            'scene_count': len(scenes),
            'image_urls': image_urls
        }
        
        # Save metadata and scenes first
        save_metadata_to_s3(story_id, metadata, scenes)
        
        # Generate and save images with character consistency
        for idx, scene in enumerate(scenes):
            print(f"Generating image {idx + 1}/5")
            
            # Add scene context and character positioning
            scene_number = idx + 1
            scene_context = f"""Scene {scene_number} of 5:
            {scene} """
            
            if idx > 0:
                time.sleep(2)  # Rate limiting between API calls
            
            try:
                image_base64 = image_from_text(scene_context)
                image_url = save_image_to_s3(
                    image_base64,
                    story_id,
                    scene_number
                )
                
                if image_url:
                    images.append(image_base64)
                    image_urls.append(image_url)
                else:
                    raise Exception(f"Failed to save image {scene_number} to S3")
                
            except Exception as img_error:
                print(f"Error generating image {scene_number}: {str(img_error)}")
                continue
        
        # Update metadata with image information
        metadata['generated_images'] = len(images)
        metadata['image_urls'] = image_urls
        
        # Save updated metadata
        save_metadata_to_s3(story_id, metadata, scenes)
        
        # Prepare response data
        response_data = {
            'story_id': story_id,
            'topic': user_input,
            'scenes': scenes,
            'full_text': full_text,
            'image_urls': image_urls,
            'metadata': metadata,
            'polly_input': polly_input  # Added polly_input to response
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
        
        error_response = {
            'error': error_message,
            'error_type': type(e).__name__,
            'timestamp': datetime.now().isoformat()
        }
        
        return {
            'statusCode': 500,
            'body': json.dumps(error_response),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }

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

def enhance_scene_description(scene_text):
    return scene_text
