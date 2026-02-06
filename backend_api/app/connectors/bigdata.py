
from pyhive import hive
# import presto if needed
import logging

logger = logging.getLogger(__name__)

class BigDataConnector:
    def __init__(self):
        self.connection = None

    def connect_hive(self, host, port=10000, username="hadoop"):
        try:
            self.connection = hive.Connection(host=host, port=port, username=username)
            logger.info(f"Connected to Hive at {host}:{port}")
        except Exception as e:
            logger.error(f"Hive Connection Error: {e}")

    def scan_table(self, table_name, limit=100):
        if not self.connection:
            return []
        
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
        return cursor.fetchall()

bigdata_connector = BigDataConnector()
