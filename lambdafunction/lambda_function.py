import json

def lambda_handler(event, context):
    # Media Servicesad
    return {
        'statusCode': 200,
        'body': json.dumps('Media Service')
    }
