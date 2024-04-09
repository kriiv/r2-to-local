import boto3
import os
import requests
from concurrent.futures import ThreadPoolExecutor

access_key = ''
secret_key = ''
bucket_name = ''
local_download_path = ''
slack_webhook_url = ''
r2_endpoint_url = 'https://<r2-region>.r2.cloudflarestorage.com'

session = boto3.session.Session()
s3 = session.client(
    service_name='s3',
    endpoint_url=r2_endpoint_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

def send_slack_message(message):
    requests.post(slack_webhook_url, json={"text": message})

def download_file(file_name):
    try:
        local_file_path = os.path.join(local_download_path, file_name)
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        s3.download_file(bucket_name, file_name, local_file_path)
        print(f'Downloaded {file_name} to {local_file_path}')
    except Exception as e:
        error_message = f'Error downloading {file_name}: {e}'
        print(error_message)
        send_slack_message(error_message)
        raise

def download_files():
    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name):
            with ThreadPoolExecutor(max_workers=10) as executor:
                for obj in page['Contents']:
                    executor.submit(download_file, obj['Key'])
        success_message = 'All files downloaded successfully.'
        print(success_message)
        send_slack_message(success_message)
    except Exception as e:
        error_message = f'General error downloading files: {e}'
        print(error_message)
        send_slack_message(error_message)

download_files()
