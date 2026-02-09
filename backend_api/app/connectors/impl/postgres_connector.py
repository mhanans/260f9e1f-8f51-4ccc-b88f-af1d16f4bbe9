
from datetime import datetime
from app.connectors.base import BaseConnector
from typing import List, Dict, Any, Generator
import psycopg2
import logging

class PostgresConnector(BaseConnector):
    
    def test_connection(self, connection_string: str) -> bool:
        try:
            conn = psycopg2.connect(connection_string)
            conn.close()
            return True, "Connected"
        except Exception as e:
            return False, str(e)
            
    def get_metadata(self, connection_string: str) -> List[Dict[str, Any]]:
        metadata = []
        try:
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
                metadata.append({"container": t, "columns": cols, "row_count": row_count, "type": "table"})
            conn.close()
        except Exception as e:
            logging.error(f"Postgres Metadata Error: {e}")
        return metadata

    def scan_data_generator(self, connection_string: str, container_name: str, limit: int = 1000) -> Generator[Dict[str, Any], None, None]:
        conn = None
        try:
            conn = psycopg2.connect(connection_string)
            # Use server-side cursor for streaming large tables
            cursor = conn.cursor(name='server_side_cursor_scan') 
            cursor.execute(f"SELECT * FROM \"{container_name}\" LIMIT {limit}")
            
            # Fetch columns for mapping
            col_names = [desc[0] for desc in cursor.description]
            
            while True:
                rows = cursor.fetchmany(100) # Fetch in small chunks
                if not rows:
                    break
                for row in rows:
                    for idx, cell in enumerate(row):
                         if cell:
                             yield {
                                 "source_type": "postgresql",
                                 "container": container_name,
                                 "field": col_names[idx],
                                 "value": str(cell)
                             }
        except Exception as e:
            logging.error(f"Postgres Scan Error: {e}")

    def get_changes(self, connection_string: str, container_name: str, last_scan_time: datetime) -> Generator[Dict[str, Any], None, None]:
        conn = None
        try:
            conn = psycopg2.connect(connection_string)
            cursor = conn.cursor(name='server_side_cursor_changes')
            
            # Assumption: Table has 'updated_at' or 'modified_at'. 
            # In a real enterprise system, we'd check metadata for the tracking column.
            # Here we try 'updated_at'.
            
            query = f"SELECT * FROM \"{container_name}\" WHERE updated_at > %s"
            cursor.execute(query, (last_scan_time,))
            
            while True:
                rows = cursor.fetchmany(100)
                if not rows:
                    break
                    
                col_names = [desc[0] for desc in cursor.description]
                for row in rows:
                    row_dict = dict(zip(col_names, row))
                    # Yield per column
                    for col, val in row_dict.items():
                        yield {
                            "source_type": "postgresql",
                            "container": container_name,
                            "field": col,
                            "value": str(val),
                            "row_id": str(row_dict.get('id', 'unknown')) # Try to capture PK
                        }
        except Exception as e:
            # If updated_at doesn't exist, log and maybe fallback to full scan?
            # For this task, we just log.
            # print(f"Change tracking failed (missing updated_at?): {e}")
            pass
        finally:
             if conn: conn.close()
