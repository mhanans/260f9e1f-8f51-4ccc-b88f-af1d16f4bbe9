
from app.connectors.base import BaseConnector
from typing import List, Dict, Any, Generator
import boto3
import json
import logging

class S3Connector(BaseConnector):

    def _get_client(self, connection_string: str):
        creds = json.loads(connection_string)
        return boto3.client('s3', **creds)

    def test_connection(self, connection_string: str) -> bool:
        try:
             s3 = self._get_client(connection_string)
             s3.list_buckets()
             return True, "Connected"
        except Exception as e:
             return False, str(e)

    def get_metadata(self, connection_string: str) -> List[Dict[str, Any]]:
        metadata = []
        try:
             s3 = self._get_client(connection_string)
             response = s3.list_buckets()
             for bucket in response.get('Buckets', []):
                 metadata.append({"container": bucket['Name'], "columns": ["key", "size"], "row_count": 0, "type": "bucket"})
        except Exception as e:
             logging.error(f"S3 Metadata Error: {e}")
        return metadata

    def scan_data_generator(self, connection_string: str, container_name: str, limit: int = 100) -> Generator[Dict[str, Any], None, None]:
        try:
             s3 = self._get_client(connection_string)
             # List objects
             response = s3.list_objects_v2(Bucket=container_name, MaxKeys=limit)
             
             if 'Contents' in response:
                 for obj in response['Contents']:
                     key = obj['Key']
                     if key.endswith(('.txt', '.csv', '.json', '.log')):
                         # Stream body
                         s3_obj = s3.get_object(Bucket=container_name, Key=key)
                         body = s3_obj['Body']
                         
                         # Read line by line or chunk
                         # Simplified: yield chunks of lines or fixed size
                         chunk_buffer = ""
                         
                         for line in body.iter_lines():
                             if line:
                                 decoded_line = line.decode('utf-8', errors='ignore')
                                 # Yield line-by-line or group them? 
                                 # Line-by-line is safer for memory.
                                 yield {
                                     "source_type": "s3",
                                     "container": container_name,
                                     "field": key,
                                     "value": decoded_line
                                 }
        except Exception as e:
             logging.error(f"S3 Scan Error: {e}")
