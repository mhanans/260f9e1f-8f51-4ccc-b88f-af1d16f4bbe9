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
                # Try simple GET
                res = requests.get(connection_string, timeout=10)
                if res.status_code == 200:
                    return True, f"API Reachable ({res.status_code})"
                return False, f"API Error {res.status_code}"
            return False, "Unsupported Type"
        except Exception as e:
            return False, str(e)

    def _flatten_api_response(self, data: Any, parent_key: str = '') -> List[Dict[str, Any]]:
        """Recursively flattens JSON to extract all leaf values."""
        items = []
        if isinstance(data, dict):
            for k, v in data.items():
                new_key = f"{parent_key}.{k}" if parent_key else k
                items.extend(self._flatten_api_response(v, new_key))
        elif isinstance(data, list):
            for i, v in enumerate(data):
                new_key = f"{parent_key}[{i}]"
                items.extend(self._flatten_api_response(v, new_key))
        else:
            # Leaf node
            items.append({
                "source_type": "api",
                "container": "API Response",
                "field": parent_key,
                "value": str(data)
            })
        return items

    def scan_source(self, type: str, connection_string: str, query_or_params: str = None) -> List[Dict[str, Any]]:
        """
        Fetches data samples in a structured format specifically for column-level tracking.
        Returns: List of {"source_type", "container", "field", "value"} 
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
                try:
                    res = requests.get(connection_string, timeout=15)
                    if res.status_code == 200:
                        try:
                            # Try parsing as JSON first
                            json_data = res.json()
                            results = self._flatten_api_response(json_data)
                        except json.JSONDecodeError:
                            # Fallback: simple text scan if not JSON
                            results.append({
                                "source_type": "api",
                                "container": "Raw Text",
                                "field": "response.text",
                                "value": res.text
                            })
                    else:
                        logger.error(f"API returned status {res.status_code}")
                except Exception as req_err:
                     logger.error(f"Request failed: {req_err}")

        except Exception as e:
            logger.error(f"Scan Source Error ({type}): {e}")
        
        return results

db_connector = GenericDBConnector()
