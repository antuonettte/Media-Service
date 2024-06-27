
import boto3
import pymysql
import os
import json
import logging
import time
import os

# Initialize AWS clients

s3_client = boto3.client('s3')

# Constants (loaded from environment variables)
MEDIA_BUCKET_NAME = os.environ['MEDIA_BUCKET_NAME']
DB_HOST = os.environ['DB_HOST']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_NAME = os.environ['DB_NAME']

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("Received event: %s", event)
    try:
        http_method = event['httpMethod']
        path = event['path']
        if path == "/media":
            logger.info('/media methods')
            if http_method == "POST":
                return process_media(event)
                
            elif http_method == "GET":
                return get_media_metadata_for_post(event)
            else:
                return {
                    'statusCode': 405,
                    'body': json.dumps({'error': 'Method not allowed'})
                }
                
        elif path == "/media/update-url":
            if http_method == "PATCH":
                return update_media_url_in_database(event)
                
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
                'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
                'body': json.dumps({'error': 'Missing required parameters'})
            }

        # Extract metadata and generate download URL
        logger.info("Getting Meta Data")
        metadata = get_media_metadata(media_key)

        logger.info("Getting Download URl")
        download_url_object = generate_download_url(media_key)
        
        download_url = download_url_object['url']
        expiresAt = download_url_object['expiresAt']
        

        if not download_url:
            return {
                'statusCode': 500,
                'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
                'body': json.dumps({'error': 'Failed to generate download URL'})
            }
        logger.info("Saving meta data to database")
        # Save metadata to the database
        save_media_metadata(post_id, user_id, media_key, metadata, download_url, expiresAt)

        return {
            'statusCode': 200,
            'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
            'body': json.dumps({'message': 'Media processed successfully', 'url': download_url, 'expiresAt': expiresAt})
        }
    except Exception as e:
        logger.error("Error processing media: %s", str(e))
        return {
            'statusCode': 500,
            'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
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
        
def update_media_url_in_database(event):
    logger.info("Getting data from event")
    request_body = json.loads(event['body'])
    media_key = request_body.get('media_key')
    post_id = request_body.get('post_id')
    
    logger.info("Generating new url")
    new_url_object = generate_download_url(media_key, 43200)
    new_url= new_url_object['url']
    expiresAt = new_url_object['expiresAt']
    logger.info(new_url_object)
    logger.info("Connecting to db")
    connection = pymysql.connect(host=DB_HOST,
                                 user=DB_USER,
                                 password=DB_PASSWORD,
                                 database=DB_NAME)
    try:
        logger.info("Updating DB Tables")
        with connection.cursor() as cursor:
            sql = "UPDATE media_metadata SET url = %s,  expiresAt = %s WHERE post_id = %s AND s3_key = %s"
            cursor.execute(sql, (new_url, expiresAt, post_id, media_key))
            logger.info(cursor.fetchall())
        connection.commit()
        return {
        "statusCode" : 200,
        'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
        "body": json.dumps({'message' : 'Download URL Successfully updated', "New URL": new_url, "ExpiresAt": expiresAt})
    }
    except Exception as e:
        connection.rollback()
        logger.error(f"Error updating media URL in database: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({ "error" : str(e) })
        }
    finally:
        connection.close()
        
    

def generate_download_url(media_key, expiration=43200):
    """Generate a download URL for the media file."""
    try:
        # Extract the file name from the media key
        file_name = media_key.split('/')[-1]

        # Generate the presigned URL with the Content-Disposition header
        response = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': MEDIA_BUCKET_NAME,
                'Key': media_key,
                'ResponseContentDisposition': f'attachment; filename={file_name}'
            },
            ExpiresIn=43200
        )
        
        expiration_timestamp = int(time.time()) + expiration

        logger.info("Generated presigned URL: %s", response)
        return {
            'url': response,
            'expiresAt': expiration_timestamp
        }
    except Exception as e:
        logger.error("Error generating download URL: %s", str(e))
        return None

def save_media_metadata(post_id, user_id, media_key, metadata, download_url, expiresAt):
    
    connection = pymysql.connect(host=DB_HOST,
                                 user=DB_USER,
                                 password=DB_PASSWORD,
                                 database=DB_NAME)
    try:
        with connection.cursor() as cursor:
            sql = """INSERT INTO media_metadata (user_id, post_id, s3_key, url, size, type, expiresAt)
                     VALUES (%s, %s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql, (user_id, post_id, media_key, download_url, metadata['size'], metadata['type'], expiresAt))
        connection.commit()
    except Exception as e:
        connection.rollback()
        logger.error("Error saving media metadata to DB: %s", str(e))
        raise e
    finally:
        connection.close()

def get_media_metadata_for_post(event):
    try:
        post_id = event['queryStringParameters'].get('post_id')
        
        logger.info(post_id)
        
        if not post_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing required parameter: post_id'})
            }
        
        connection = pymysql.connect(host=DB_HOST,
                                     user=DB_USER,
                                     password=DB_PASSWORD,
                                     database=DB_NAME)
        try:
            logger.info("Trying to fetch all results")
            with connection.cursor() as cursor:
                sql = "SELECT user_id, post_id, s3_key, url, size, type, expiresAt FROM media_metadata WHERE post_id = %s"
                
                cursor.execute(sql, (post_id))
                result = cursor.fetchall()
                metadata_list = []
                logger.info(result)
                
                for row in result:
                    metadata = {
                        'user_id': row[0],
                        'post_id': row[1],
                        's3_key': row[2],
                        'url': row[3],
                        'size': row[4],
                        'type': row[5],
                        'expiresAt': row[6]
                    }
                    metadata_list.append(metadata)
            return {
                'statusCode': 200,
                'body': json.dumps(metadata_list)
            }
        finally:
            connection.close()
    except Exception as e:
        logger.error("Error getting media metadata for post: %s", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }