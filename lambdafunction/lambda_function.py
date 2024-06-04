import boto3
import pymysql
import os
import json
import logging

# Initialize AWS clients
s3_client = boto3.client('s3')

# Constants (loaded from environment variables)
MEDIA_BUCKET_NAME = 'car-network-media-bucket'
DB_HOST = 'car-network-db.c5kgayasi5x2.us-east-1.rds.amazonaws.com'
DB_USER = 'admin'
DB_PASSWORD = 'FrostGaming1!'
DB_NAME = 'media_metadata'

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("Received event: %s", event)
    try:
        http_method = event['httpMethod']
        if http_method == "POST":
            return process_media(event)
        else:
            return {
                'statusCode': 405,
                'body': json.dumps({'error': 'Method not allowed'})
            }
    except Exception as e:
        logger.error("Error processing request: %s", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def process_media(event):
    try:
        request_body = json.loads(event['body'])
        media_key = request_body.get('media_key')
        post_id = request_body.get('post_id')
        user_id = request_body.get('user_id')

        if not media_key or not post_id or not user_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing required parameters'})
            }

        # Extract metadata and generate download URL
        logger.info("Getting Meta Data")
        metadata = get_media_metadata(media_key)

        logger.info("Getting Download URl")
        download_url = generate_download_url(media_key)

        if not download_url:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Failed to generate download URL'})
            }

        # Save metadata to the database
        save_media_metadata(post_id, user_id, media_key, metadata, download_url)

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Media processed successfully'})
        }
    except Exception as e:
        logger.error("Error processing media: %s", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def get_media_metadata(media_key):
    try:
        response = s3_client.head_object(Bucket=MEDIA_BUCKET_NAME, Key=media_key)
        logger.info("S3 head_object response: %s", response)
        metadata = {
            'title': media_key.split('/')[-1],
            'size': response['ContentLength'],
            'type': response['ContentType'],
        }
        return metadata
    except Exception as e:
        logger.error("Error getting media metadata: %s", str(e))
        raise e

def generate_download_url(media_key, expiration=3600):
    """Generate a download URL for the media file."""
    try:
        response = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': MEDIA_BUCKET_NAME, 'Key': media_key},
            ExpiresIn=expiration
        )
        logger.info("Generated presigned URL: %s", response)
        return response
    except Exception as e:
        logger.error("Error generating download URL: %s", str(e))
        return None

def save_media_metadata(post_id, user_id, media_key, metadata, download_url):
    try:
        connection = pymysql.connect(host=DB_HOST,
                                     user=DB_USER,
                                     password=DB_PASSWORD,
                                     database=DB_NAME)
        with connection.cursor() as cursor:
            sql = """INSERT INTO media_metadata (user_id, post_id, media_key, download_url, size, type)
                     VALUES (%s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql, (user_id, post_id, media_key, download_url, metadata['size'], metadata['type']))
        connection.commit()
    except Exception as e:
        connection.rollback()
        logger.error("Error saving media metadata to DB: %s", str(e))
        raise e
    finally:
        connection.close()
