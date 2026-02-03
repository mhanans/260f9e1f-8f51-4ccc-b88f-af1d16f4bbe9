from typing import List, Dict, Any
import logging
import requests
import psycopg2
import json

logger = logging.getLogger(__name__)

class GenericDBConnector:
    def __init__(self):
        self.connections = {} 

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
            return False, "Unsupported Type"
        except Exception as e:
            return False, str(e)

    def scan_source(self, type: str, connection_string: str, query_or_params: str = None) -> List[Dict[str, Any]]:
        """
        Fetches data samples in a structured format specifically for column-level tracking.
        Returns: List of {"table": str, "column": str, "value": str} 
        """
        results = []
        try:
            if type == 'postgresql':
                conn = psycopg2.connect(connection_string)
                cursor = conn.cursor()
                
                # Auto-Scan Mode: Fetch tables AND columns
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    LIMIT 20;
                """)
                tables = [row[0] for row in cursor.fetchall()]
                
                for table in tables:
                    try:
                        # Get Column Names for this table
                        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}' ORDER BY ordinal_position")
                        columns = [row[0] for row in cursor.fetchall()]
                        
                        # Sample rows (LIMIT 10)
                        cursor.execute(f"SELECT * FROM \"{table}\" LIMIT 10;")
                        rows = cursor.fetchall()
                        
                        for row in rows:
                            # Iterate each cell in the row to preserve Column Identity
                            for idx, cell_value in enumerate(row):
                                if cell_value: # Skip nulls
                                    results.append({
                                        "source_type": "db",
                                        "container": table,     # Table Name
                                        "field": columns[idx], # Column Name
                                        "value": str(cell_value)
                                    })

                    except Exception as inner_e:
                        logger.error(f"Error scanning table {table}: {inner_e}")

                conn.close()
            
            elif type == 'api_get':
                res = requests.get(connection_string, timeout=10)
                if res.status_code == 200:
                    try:
                        data = res.json()
                        # Flatten JSON and attempt to key by field
                        if isinstance(data, list):
                            for idx, item in enumerate(data):
                                if isinstance(item, dict):
                                    for key, val in item.items():
                                        results.append({
                                            "source_type": "api",
                                            "container": f"Record #{idx}",
                                            "field": key,
                                            "value": str(val)
                                        })
                        elif isinstance(data, dict):
                             for key, val in data.items():
                                 results.append({
                                     "source_type": "api",
                                     "container": "Root Object",
                                     "field": key,
                                     "value": str(val)
                                 })
                    except:
                        pass

        except Exception as e:
            logger.error(f"Scan Source Error ({type}): {e}")
        
        return results

db_connector = GenericDBConnector()
