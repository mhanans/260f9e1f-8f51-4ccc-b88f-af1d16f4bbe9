from typing import List, Dict
import logging
import requests
import psycopg2
# import pyodbc # for sql server
# import pymongo # for mongo

logger = logging.getLogger(__name__)

class GenericDBConnector:
    def __init__(self):
        self.connections = {} # Store multiple connections {id: conn_obj}

    def test_connection(self, type: str, connection_string: str):
        try:
            if type == 'postgresql':
                conn = psycopg2.connect(connection_string)
                conn.close()
                return True, "Connected"
            elif type == 'api_get':
                res = requests.get(connection_string, timeout=5)
                if res.status_code == 200:
                    return True, f"API Reachable ({res.status_code})"
                return False, f"API Error {res.status_code}"
            # Add other types (MSSQL, Mongo) specific logic here
            return False, "Unsupported Type"
        except Exception as e:
            return False, str(e)

    def scan_source(self, type: str, connection_string: str, query_or_params: str = None) -> List[str]:
        """
        Fetches data samples (e.g., first 50 rows or API json) to be sent to scanner.
        Returns list of strings (documents) to scan.
        """
        results = []
        try:
            if type == 'postgresql':
                conn = psycopg2.connect(connection_string)
                cursor = conn.cursor()
                # Default to pulling first 50 rows of 'users' or whatever table if specified
                # For this generic demo, we assume 'query' helps safely or we just list tables
                # To prevent injection risk in real app, use better query builder.
                if query_or_params:
                    cursor.execute(query_or_params)
                    rows = cursor.fetchall()
                    for r in rows:
                        results.append(str(r)) # Convert row to string for PII scanning
                conn.close()
            
            elif type == 'api_get':
                res = requests.get(connection_string, timeout=10)
                if res.status_code == 200:
                    try:
                        data = res.json()
                        # Flatten json to list of strings
                        if isinstance(data, list):
                            for item in data:
                                results.append(str(item))
                        else:
                            results.append(str(data))
                    except:
                        results.append(res.text)

        except Exception as e:
            logger.error(f"Scan Source Error ({type}): {e}")
        
        return results

db_connector = GenericDBConnector()
