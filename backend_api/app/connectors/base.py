
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Generator

class BaseConnector(ABC):
    
    @abstractmethod
    def test_connection(self, connection_string: str) -> bool:
        pass

    @abstractmethod
    def get_metadata(self, connection_string: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def scan_data_generator(self, connection_string: str, container_name: str, limit: int = 1000) -> Generator[Dict[str, Any], None, None]:
        pass

    @abstractmethod
    def get_changes(self, connection_string: str, container_name: str, last_scan_time: datetime) -> Generator[Dict[str, Any], None, None]:
        """
        Yields items that have changed since last_scan_time.
        For DBs: WHERE updated_at > last_scan_time
        For S3: LastModified > last_scan_time
        """
        pass
