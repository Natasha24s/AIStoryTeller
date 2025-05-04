You can use the API in two steps:

Start the process:

```
curl -X POST \
  https://your-api-endpoint.execute-api.region.amazonaws.com/prod/generate \
  -H "Content-Type: application/json" \
  -d '{"topic": "A day at the beach"}'
```

    
This will return:

```
{
  "executionArn": "arn:aws:states:region:account:execution:StateMachine:execution-id",
  "startDate": "2025-05-01T15:53:38.000Z",
  "message": "Story generation process started successfully",
  "status": "IN_PROGRESS"
}
```
   
Check the status and get the final result:

```
curl "https://your-api-endpoint.execute-api.region.amazonaws.com/prod/status?executionArn=arn:aws:states:region:account:execution:StateMachine:execution-id"
```    
When the process is complete, you'll get the video generation result:

```
{
  "status": "Completed",
  "story_id": "20250501_a_day_at_the_beach_abc123",
  "source_bucket": "your-source-bucket",
  "destination_bucket": "your-destination-bucket",
  "output_location": "s3://your-destination-bucket/job-id/output.mp4",
  "timestamp": "2025-05-01 15:53:38",
  "message": "Video generation completed successfully"
}
```    
If the process is still running, you'll get:

```
{
  "executionArn": "arn:aws:states:region:account:execution:StateMachine:execution-id",
  "status": "RUNNING",
  "startDate": "2025-05-01T15:53:38.000Z",
  "message": "Execution in progress"
}
```

To test the API Gateway GET endpoint in the AWS Console, follow these steps:

Open AWS Console and navigate to API Gateway
Select your API (StoryGeneratorAPI)
In the left navigation pane, click on "Resources"
Click on the GET method under /status
Click the "TEST" button
In the Query Strings section, add:

```
executionArn = arn:aws:states:us-east-1:your-account-id:execution:StoryProcessingStateMachine-xxx:execution-id
```
The response should look like:

```    
{
  "status": "RUNNING",
  "startDate": "2025-05-01T15:53:38.000Z",
  "executionArn": "arn:aws:states:us-east-1:your-account-id:execution:StoryProcessingStateMachine-xxx:execution-id",
  "message": "Execution in progress"
}
```

    
Or if completed:

```
{
  "status": "Completed",
  "story_id": "20250501_a_day_at_the_beach_abc123",
  "source_bucket": "your-source-bucket",
  "destination_bucket": "your-destination-bucket",
  "output_location": "s3://your-destination-bucket/job-id/output.mp4",
  "timestamp": "2025-05-01 15:53:38",
  "message": "Video generation completed successfully"
}
```
The process flow is:

```    
GenerateStory (First Lambda)
    ↓
    - Generates scenes
    - Creates polly_input narrative
    ↓
GenerateVideo (Second Lambda)
    ↓
    - Creates silent video using Nova Reel
    - Saves to video_output_location
    ↓
GenerateAudioVideo (Third Lambda)
    ↓
    - Generates narration audio using Polly
    - Uses MediaConvert to merge:
      * Input video from video_output_location
      * Narration audio from Polly
    - Saves final merged video to final_output_location

 ```   

    
Final output sample:

  ```  
{
  "story_id": "20250501_a_day_at_the_beach_abc123",
  "video_status": "Completed",
  "video_output_location": "s3://your-destination-bucket/job-id/output.mp4",  // Silent video
  "audio_video_status": "Completed",
  "final_output_location": "s3://your-destination-bucket/20250501_a_day_at_the_beach_abc123/final/final_output.mp4",  // Video with narration
  "timestamp": "2025-05-01 15:53:38",
  "message": "Processing completed successfully",
  "outputs": {
    "initial_video": {
      "status": "Completed",
      "location": "s3://your-destination-bucket/job-id/output.mp4",  // Silent video
      "timestamp": "2025-05-01 15:53:38"
    },
    "final_video": {
      "status": "Completed",
      "location": "s3://your-destination-bucket/20250501_a_day_at_the_beach_abc123/final/final_output.mp4",  // Video with narration
      "timestamp": "2025-05-01 16:00:00"
    }
  }
}
```
    

    

    
