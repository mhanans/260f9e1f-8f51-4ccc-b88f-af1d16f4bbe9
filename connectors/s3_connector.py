import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import logging
import io

logger = logging.getLogger(__name__)

class S3Connector:
    def __init__(self):
        self.s3_client = None
        self.bucket_name = None

    def connect(self, endpoint_url, access_key, secret_key, bucket_name):
        """
        Establishes connection to S3/MinIO.
        """
        try:
            self.s3_client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key
            )
            self.bucket_name = bucket_name
            # Verify connection by listing buckets
            self.s3_client.list_buckets()
            logger.info("Successfully connected to S3/MinIO")
            return True, "Connected"
        except Exception as e:
            logger.error(f"S3 Connection Error: {e}")
            return False, str(e)

    def list_files(self):
        """
        Lists files in the configured bucket.
        Returns a list of dictionaries with 'Key', 'Size', 'LastModified'.
        """
        if not self.s3_client or not self.bucket_name:
            return []
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
            if 'Contents' in response:
                return response['Contents']
            return []
        except ClientError as e:
            logger.error(f"Error listing files: {e}")
            return []

    def upload_file(self, file_obj, object_name):
        """
        Uploads a file-like object to the bucket.
        """
        if not self.s3_client or not self.bucket_name:
            return False
        try:
            # Determine content type content logic if needed, else auto
            self.s3_client.upload_fileobj(file_obj, self.bucket_name, object_name)
            return True
        except ClientError as e:
            logger.error(f"Error uploading file: {e}")
            return False

    def get_file_content(self, object_name):
        """
        Retrieves file content as bytes.
        """
        if not self.s3_client or not self.bucket_name:
            return None
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=object_name)
            return response['Body'].read()
        except ClientError as e:
            logger.error(f"Error reading file: {e}")
            return None

    def delete_file(self, object_name):
        """
        Deletes a file from the bucket.
        """
        if not self.s3_client or not self.bucket_name:
            return False
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=object_name)
            return True
        except ClientError as e:
            logger.error(f"Error deleting file: {e}")
            return False

# Singleton instance
s3_connector = S3Connector()
