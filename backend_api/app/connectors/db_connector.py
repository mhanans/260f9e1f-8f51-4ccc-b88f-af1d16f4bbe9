
from typing import List, Dict, Any, Generator
from datetime import datetime
import logging
from app.connectors.factory import connector_factory

logger = logging.getLogger(__name__)

class GenericDBConnector:
    """
    Proxy class that forwards requests to the appropriate Connector implementation
    via the ConnectorFactory. Features streaming support.
    """
    def __init__(self):
        pass

    def test_connection(self, type: str, connection_string: str):
        try:
            connector = connector_factory.get_connector(type)
            return connector.test_connection(connection_string)
        except Exception as e:
            return False, str(e)

    def get_schema_metadata(self, type: str, connection_string: str) -> List[Dict[str, Any]]:
        try:
            connector = connector_factory.get_connector(type)
            return connector.get_metadata(connection_string)
        except Exception as e:
            logger.error(f"Metadata Crawl Error ({type}): {e}")
            return []

    def scan_target(self, type: str, connection_string: str, container: str, limit: int = 50, last_scan_time: datetime = None) -> List[Dict[str, Any]]:
        """
        Legacy Buffered Scan: Consumes the generator and returns a list.
        Use scan_target_generator for large datasets.
        """
        results = []
        try:
            connector = connector_factory.get_connector(type)
            gen = connector.scan_data_generator(connection_string, container, limit)
            for item in gen:
                results.append(item)
        except Exception as e:
            logger.error(f"Target Scan Error: {e}")
        return results

    def scan_target_generator(self, type: str, connection_string: str, container: str, limit: int = 1000) -> Generator[Dict[str, Any], None, None]:
        """
        New Streaming Scan: Yields items one by one.
        """
        try:
            connector = connector_factory.get_connector(type)
            yield from connector.scan_data_generator(connection_string, container, limit)
        except Exception as e:
            logger.error(f"Generator Error: {e}")
            yield {} # or raise?

    def scan_source(self, type: str, connection_string: str, query_or_params: str = None, last_scan_time: datetime = None) -> List[Dict[str, Any]]:
        # Legacy/API method - fallback or implement API connector
        return []

db_connector = GenericDBConnector()
