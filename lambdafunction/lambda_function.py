import boto3
import pymysql
import os
import json

# Initialize AWS clients
s3_client = boto3.client('s3')

# Constants (loaded from environment variables)
MEDIA_BUCKET_NAME = os.environ['MEDIA_BUCKET_NAME']
DB_HOST = os.environ['DB_HOST']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_NAME = os.environ['DB_NAME']

def lambda_handler(event, context):
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
        metadata = get_media_metadata(media_key)
        download_url = generate_download_url(media_key)

        # Save metadata to the database
        save_media_metadata(post_id, user_id, media_key, metadata, download_url)

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Media processed successfully'})
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def get_media_metadata(media_key):
    """Extract metadata from the media file."""
    # Implement metadata extraction logic here
    metadata = {
        'size': 12345,  # Example size in bytes
        'type': 'image/jpeg'  # Example media type
    }
    return metadata

def generate_download_url(media_key, expiration=3600):
    """Generate a download URL for the media file."""
    try:
        response = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': MEDIA_BUCKET_NAME, 'Key': media_key},
            ExpiresIn=expiration
        )
        return response
    except Exception as e:
        print(f"Error generating download URL: {e}")
        return None

def save_media_metadata(post_id, user_id, media_key, metadata, download_url):
    """Save media metadata to the database."""
    connection = pymysql.connect(host=DB_HOST,
                                 user=DB_USER,
                                 password=DB_PASSWORD,
                                 database=DB_NAME)
    try:
        with connection.cursor() as cursor:
            sql = """INSERT INTO media_metadata (post_id, user_id, media_key, size, type, download_url)
                     VALUES (%s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql, (post_id, user_id, media_key, metadata['size'], metadata['type'], download_url))
        connection.commit()
    except Exception as e:
        connection.rollback()
        print(str(e))
        raise e
    finally:
        connection.close()
