import json
import subprocess
import sys

class client:

    def __init__(self, s3_url, token):
        try:
            import boto3
        except ImportError:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "boto3"])
        import boto3
        self.client = boto3.client(
            "s3",
            endpoint_url=s3_url,
            aws_access_key_id=token,
            aws_secret_access_key='s3',
        )

    def list_trace_files(self, s3_bucket, s3_prefix):
        response = self.client.list_objects(Bucket=s3_bucket)
        obj_key_list = []

        for con in response.get('Contents', []):
            key = con['Key']
            if s3_prefix in key:
                obj_key_list.append(key)
        return obj_key_list

    def get_obj_bulk(self, s3_bucket, obj_key_list):
        json_data_list = []
        for key in obj_key_list:
            # Get object data
            response = self.client.get_object(Bucket=s3_bucket, Key=key)
            body = response['Body'].read().decode('utf-8')

            # Parse line-by-line
            for line in body.strip().splitlines():
                obj = json.loads(line)
                json_data_list.append(obj)
        return json_data_list
