import boto3
import pymysql
import os
import json

# Initialize AWS clients
s3_client = boto3.client('s3')

# Constants (loaded from environment variables)
MEDIA_BUCKET_NAME = 'car-network-media-bucket'
DB_HOST = 'car-network-db.c5kgayasi5x2.us-east-1.rds.amazonaws.com'
DB_USER = 'admin'
DB_PASSWORD = 'FrostGaming1!'
DB_NAME = 'media_metadata'

def lambda_handler(event, context):
    try: 
        http_method = event['httpMethod']
        
        if http_method == "POST":
            return process_media(event)

    except Exception as e:

        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
        
def process_media(event):
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

    print("Extracted Meta Data, got download url")

    # Save metadata to the database
    save_media_metadata(post_id, user_id, media_key, metadata, download_url)

    print("Saved to db")

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Media processed successfully'})
    }

def get_media_metadata(media_key):

    response = s3_client.head_object(Bucket=MEDIA_BUCKET_NAME, Key=media_key)
    print(response)

    metadata = {
        'title': media_key.split('/')[-1],
        'size': response['ContentLength'],
        'type': response['ContentType'],
        # Add other relevant metadata extraction here
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
            sql = """INSERT INTO media_metadata (user_id, post_id, media_key, download_url, size, type)
                     VALUES (%s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql, ( user_id, post_id, media_key, download_url, metadata['size'], metadata['type']))
        connection.commit()
    except Exception as e:
        connection.rollback()
        print("Error saving got db: ", str(e))
        raise e
    finally:
        connection.close()
