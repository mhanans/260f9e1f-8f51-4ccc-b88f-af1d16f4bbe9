
from app.connectors.base import BaseConnector
from app.connectors.impl.postgres_connector import PostgresConnector
from app.connectors.impl.mongo_connector import MongoConnector
from app.connectors.impl.s3_connector import S3Connector

class ConnectorFactory:
    @staticmethod
    def get_connector(target_type: str) -> BaseConnector:
        # Standardize type names
        t_type = target_type.lower()
        
        if t_type in ['database', 'postgresql', 'postgres']:
            return PostgresConnector()
        elif t_type in ['mongodb', 'mongo']:
            return MongoConnector()
        elif t_type in ['s3', 'datalake', 'minio']:
            return S3Connector()
        else:
            raise ValueError(f"Unsupported target_type: {target_type}")

connector_factory = ConnectorFactory()
