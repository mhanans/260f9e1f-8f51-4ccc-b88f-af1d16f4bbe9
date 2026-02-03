import sqlglot
from sqlglot import exp

def extract_lineage(sql: str):
    """
    Parse SQL and return source -> target tables.
    """
    lineage = []
    try:
        parsed = sqlglot.parse_one(sql)
        
        # Find all INSERT INTO or CREATE TABLE AS
        target_table = None
        
        if isinstance(parsed, exp.Insert):
            target_table = parsed.this.sql()
        elif isinstance(parsed, exp.Create):
             target_table = parsed.this.sql()
        
        # Find all source tables in SELECT
        source_tables = [
            table.sql() 
            for table in parsed.find_all(exp.Table)
            if table.sql() != target_table
        ]
        
        if target_table:
            for src in source_tables:
                lineage.append({"source": src, "target": target_table})
                
    except Exception as e:
        print(f"Error parsing SQL: {e}")
        
    return lineage
