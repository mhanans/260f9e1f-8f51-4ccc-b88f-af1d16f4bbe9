from typing import List, Dict
import logging
import requests
import psycopg2

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

    def scan_source(self, type: str, connection_string: str, query_or_params: str = None) -> List[str]:
        """
        Fetches data samples. 
        If query is None for DB, it attempts to Auto-Discover public tables and sample rows.
        """
        results = []
        try:
            if type == 'postgresql':
                conn = psycopg2.connect(connection_string)
                cursor = conn.cursor()
                
                if query_or_params:
                    # User provided a specific query
                    cursor.execute(query_or_params)
                    rows = cursor.fetchall()
                    for r in rows: results.append(str(r))
                else:
                    # Auto-Scan Mode: Fetch valid tables from public schema
                    cursor.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        LIMIT 10;
                    """)
                    tables = [row[0] for row in cursor.fetchall()]
                    
                    for table in tables:
                        try:
                            # Sample 5 rows from each table
                            # Formatting table name safely (simple alphanumeric check suggested in real prod)
                            cursor.execute(f"SELECT * FROM \"{table}\" LIMIT 5;")
                            rows = cursor.fetchall()
                            for r in rows:
                                # Prepend table name for context
                                results.append(f"[Table: {table}] {str(r)}")
                        except Exception as inner_e:
                            logger.error(f"Error scanning table {table}: {inner_e}")

                conn.close()
            
            elif type == 'api_get':
                res = requests.get(connection_string, timeout=10)
                if res.status_code == 200:
                    try:
                        data = res.json()
                        if isinstance(data, list):
                            for item in data: results.append(str(item))
                        else:
                            results.append(str(data))
                    except:
                        results.append(res.text)

        except Exception as e:
            logger.error(f"Scan Source Error ({type}): {e}")
        
        return results

db_connector = GenericDBConnector()
