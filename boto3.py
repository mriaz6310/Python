import boto3
import os
from botocore.exceptions import ClientError


local_file_path = '/tmp/Dataset.csv'  
bucket_name = os.environ['BUCKET_NAME']
s3_file = os.environ['S3_FILE_KEY']

def lambda_handler(event, context):
    # Set up logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Start uploading the file
    logger.info("Uploading file: STARTED")

    # Create an S3 client
    s3 = boto3.client('s3')

    try:
        # Upload the file
        s3.upload_file(local_file_path, bucket_name, s3_file)
        logger.info("File upload: SUCCESSFULLY")
        return {
            'statusCode': 200,
            'body': 'File upload: SUCCESSFULLY'
        }
    except ClientError as e:
        # This will capture any error related to the boto3 S3 client operation
        logger.error(e)
        return {
            'statusCode': 500,
            'body': 'Error uploading file'
        }
    except FileNotFoundError:
        # This will capture the FileNotFoundError if the local file doesn't exist
        logger.error("File not found - ERROR")
        return {
            'statusCode': 500,
            'body': 'File not found - ERROR'
        }
