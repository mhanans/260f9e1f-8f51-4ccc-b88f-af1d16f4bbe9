from typing import List, Dict, Any
from datetime import datetime
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
                res = requests.get(connection_string, timeout=10)
                if res.status_code == 200:
                    return True, f"API Reachable ({res.status_code})"
                return False, f"API Error {res.status_code}"
            return False, "Unsupported Type"
        except Exception as e:
            return False, str(e)

    def get_schema_metadata(self, type: str, connection_string: str) -> List[Dict[str, Any]]:
        metadata = []
        try:
            if type == 'postgresql':
                conn = psycopg2.connect(connection_string)
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)
                tables = [r[0] for r in cursor.fetchall()]
                for t in tables:
                    cursor.execute(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = '{t}'
                    """)
                    cols = [r[0] for r in cursor.fetchall()]
                    cursor.execute(f"SELECT count(*) FROM \"{t}\"")
                    row_count = cursor.fetchone()[0]
                    metadata.append({"table": t, "columns": cols, "row_count": row_count})
                conn.close()
        except Exception as e:
            logger.error(f"Metadata Crawl Error: {e}")
        return metadata

    def scan_target(self, type: str, connection_string: str, table: str, limit: int = 50, last_scan_time: datetime = None) -> List[Dict[str, Any]]:
        results = []
        try:
            if type == 'postgresql':
                conn = psycopg2.connect(connection_string)
                cursor = conn.cursor()
                cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}' ORDER BY ordinal_position")
                columns = [r[0] for r in cursor.fetchall()]

                # Delta Scan Logic (Naive implementation assuming 'updated_at' column exists if last_scan_time is provided)
                if last_scan_time:
                     # Check if updated_at exists
                    if 'updated_at' in columns:
                        cursor.execute(f"SELECT * FROM \"{table}\" WHERE updated_at > '{last_scan_time}' LIMIT {limit}")
                    else:
                        logger.warning(f"Delta scan requested for {table} but 'updated_at' column not found. Falling back to full scan.")
                        cursor.execute(f"SELECT * FROM \"{table}\" LIMIT {limit}")
                else:
                    cursor.execute(f"SELECT * FROM \"{table}\" LIMIT {limit}")

                rows = cursor.fetchall()
                for row in rows:
                    for idx, cell in enumerate(row):
                        if cell:
                            results.append({
                                "source_type": "db",
                                "container": table,
                                "field": columns[idx],
                                "value": str(cell)
                            })
                conn.close()
        except Exception as e:
            logger.error(f"Target Scan Error: {e}")
        return results

    def _flatten_api_response(self, data: Any, parent_key: str = '') -> List[Dict[str, Any]]:
        """Recursively flattens JSON."""
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

    def scan_source(self, type: str, connection_string: str, query_or_params: str = None, last_scan_time: datetime = None) -> List[Dict[str, Any]]:
        """
        Generic Scan source method.
        """
        results = []
        
        # 1. API Logic Refined
        if type == 'api_get':
            try:
                res = requests.get(connection_string, timeout=15, verify=False)
                if res.status_code == 200:
                    try:
                        json_data = res.json()
                        # Flatten the entire JSON structure
                        results = self._flatten_api_response(json_data)
                    except json.JSONDecodeError:
                        results.append({"source_type":"api", "container":"Raw Text", "field":"response.text", "value":res.text})
            except Exception as e:
                logger.error(f"API Scan Error: {e}")
        
        # 2. Postgres Logic
        elif type == 'postgresql':
             # Use simplified auto-scan logic if this method is called directly
             try:
                conn = psycopg2.connect(connection_string)
                cursor = conn.cursor()
                cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' LIMIT 5")
                tables = [r[0] for r in cursor.fetchall()]
                for t in tables:
                     cursor.execute(f"SELECT * FROM \"{t}\" LIMIT 5")
                     rows = cursor.fetchall()
                     for r in rows:
                         results.append({"source_type":"db", "container":t, "field":"unknown_col", "value":str(r)})
             except: pass

        return results

db_connector = GenericDBConnector()
