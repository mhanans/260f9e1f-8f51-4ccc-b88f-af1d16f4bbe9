
from app.connectors.base import BaseConnector
from typing import List, Dict, Any, Generator
import pymongo
import logging

class MongoConnector(BaseConnector):

    def test_connection(self, connection_string: str) -> bool:
        try:
             client = pymongo.MongoClient(connection_string, serverSelectionTimeoutMS=2000)
             client.server_info()
             return True, "Connected"
        except Exception as e:
             return False, str(e)

    def get_metadata(self, connection_string: str) -> List[Dict[str, Any]]:
        metadata = []
        try:
             client = pymongo.MongoClient(connection_string)
             db = client.get_database()
             for coll_name in db.list_collection_names():
                 # Sample document for keys
                 doc = db[coll_name].find_one()
                 keys = list(doc.keys()) if doc else []
                 count = db[coll_name].estimated_document_count()
                 metadata.append({"container": coll_name, "columns": keys, "row_count": count, "type": "collection"})
        except Exception as e:
             logging.error(f"Mongo Metadata Error: {e}")
        return metadata


    def _flatten(self, data: Any, parent_key: str = '', depth: int = 0, max_depth: int = 5) -> List[Dict[str, Any]]:
        """
        Recursively flattens JSON with depth protection.
        """
        items = []
        if depth > max_depth:
            # stop recursing, just stringify
            return [{"field": parent_key, "value": str(data)}]

        if isinstance(data, dict):
            for k, v in data.items():
                new_key = f"{parent_key}.{k}" if parent_key else k
                items.extend(self._flatten(v, new_key, depth + 1, max_depth))
        elif isinstance(data, list):
            for i, v in enumerate(data):
                new_key = f"{parent_key}[{i}]"
                items.extend(self._flatten(v, new_key, depth + 1, max_depth))
        else:
             items.append({"field": parent_key, "value": str(data)})
        return items

    def get_changes(self, connection_string: str, container_name: str, last_scan_time: datetime) -> Generator[Dict[str, Any], None, None]:
        try:
            client = pymongo.MongoClient(connection_string)
            db = client.get_database()
            coll = db[container_name]
            
            # MongoDB ObjectId includes timestamp. But relies on creation time.
            # Ideally use 'updatedAt' field if exists.
            # Fallback: Scan all (Changes not easily detectable without timestamp).
            # We try querying 'updatedAt'.
            
            query = {"updatedAt": {"$gt": last_scan_time}}
            cursor = coll.find(query)
            
            for doc in cursor:
                 flat_items = self._flatten(doc, max_depth=5)
                 for item in flat_items:
                     yield {
                         "source_type": "mongodb",
                         "container": container_name,
                         "field": item['field'],
                         "value": item['value'],
                         "row_id": str(doc.get('_id'))
                     }
        except Exception as e:
             logging.error(f"Mongo Get Changes Error: {e}") # Added logging for clarity
             pass # Original instruction had pass, keeping it.

    def scan_data_generator(self, connection_string: str, container_name: str, limit: int = 1000) -> Generator[Dict[str, Any], None, None]:
        try:
            client = pymongo.MongoClient(connection_string)
            db = client.get_database()
            coll = db[container_name]
            
            # Use cursor for streaming
            cursor = coll.find().limit(limit)
            
            for doc in cursor:
                 # Yield flattened items for each doc
                 flat_items = self._flatten(doc, max_depth=5)
                 for item in flat_items:
                     yield {
                         "source_type": "mongodb",
                         "container": container_name,
                         "field": item['field'],
                         "value": item['value']
                     }
        except Exception as e:
             logging.error(f"Mongo Scan Error: {e}")
