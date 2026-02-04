import structlog
import uuid
import json
from typing import List, Dict, Optional, Any, Set

try:
    import sqlglot
    from sqlglot import exp
except ImportError:
    sqlglot = None
    exp = None

# Import engines if available
try:
    from engine.classification import classification_engine
except ImportError:
    classification_engine = None

logger = structlog.get_logger()

class LineageEngine:
    def __init__(self):
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.edges: List[Dict[str, Any]] = []
        self.manual_flows: List[Dict[str, Any]] = []

    def _get_node_id(self, name: str, type_label: str) -> str:
        """Generates a consistent ID for nodes."""
        return f"{type_label.lower()}::{name.lower()}"

    def _add_node(self, name: str, node_type: str, metadata: Dict[str, Any] = None):
        """Adds or updates a node in the graph."""
        node_id = self._get_node_id(name, node_type)
        if node_id not in self.nodes:
            self.nodes[node_id] = {
                "id": node_id,
                "label": name,
                "type": node_type,  # table, column, system
                "data": metadata or {},
                "pii_present": False,
                "tags": []
            }
        else:
            # Merge metadata if needed
            if metadata:
                self.nodes[node_id]["data"].update(metadata)
        return node_id

    def _add_edge(self, source_id: str, target_id: str, relation_type: str, logic: str = None):
        """Adds a directional edge."""
        edge = {
            "source": source_id,
            "target": target_id,
            "id": f"{source_id}->{target_id}",
            "label": relation_type,
            "transformation": logic
        }
        # Avoid duplicates
        if edge not in self.edges:
            self.edges.append(edge)

    def _scan_for_pii(self, column_name: str) -> Optional[str]:
        """Uses ClassificationEngine to detect PII type from column name context."""
        if not classification_engine:
            return None
            
        # Simplistic check: treat column name as 'data' to classify intent
        # In a real scenario, we might want a different method in classification_engine 
        # specifically for column name heuristics.
        # For now, we simulate by checking keywords if classification_engine doesn't have a specific column checker.
        
        lower_col = column_name.lower()
        if "email" in lower_col: return "EMAIL_ADDRESS"
        if "phone" in lower_col or "hp" in lower_col: return "PHONE_NUMBER"
        if "nik" in lower_col or "ktp" in lower_col: return "ID_NIK"
        if "salary" in lower_col or "gaji" in lower_col: return "FINANCIAL_DATA"
        
        return None

    def parse_sql(self, sql: str, system_name: str = "SQL_DB"):
        """
        Parses SQL to extract Table and Column level lineage.
        Enriches with PII detection.
        """
        if not sqlglot:
            logger.warning("sqlglot_not_installed", message="Cannot parse SQL lineage.")
            return

        try:
            parsed = sqlglot.parse_one(sql)
        except Exception as e:
            logger.error("sql_parse_error", error=str(e))
            return

        # 1. Identify Target Table
        target_table_name = None
        if isinstance(parsed, exp.Insert):
            target_table_name = parsed.this.sql()
        elif isinstance(parsed, exp.Create):
            target_table_name = parsed.this.sql()
        
        # If just a SELECT, we might treat it as a View or ephemeral lineage if needed.
        # For now, focus on movements writing to a target.
        if target_table_name:
            tgt_node_id = self._add_node(target_table_name, "table", {"system": system_name})
            
            # Metadata: Purpose & Subject (Placeholder/Rule-based)
            if "user" in target_table_name.lower() or "customer" in target_table_name.lower():
                self.nodes[tgt_node_id]["data"]["subject_type"] = "Customer"
                self.nodes[tgt_node_id]["tags"].append("Non-Public")
            
            # 2. Identify Source Tables & Direct Columns
            # Logic: Iterate through projections (SELECT clause) to find mapping
            # This is complex in static analysis; simple heuristic used here.
            
            # Find SOURCE TABLES
            for table in parsed.find_all(exp.Table):
                src_name = table.sql()
                if src_name != target_table_name:
                    src_node_id = self._add_node(src_name, "table", {"system": system_name})
                    self._add_edge(src_node_id, tgt_node_id, "flows_to", "SQL Transformation")

            # 3. Column Level Lineage (Best Effort)
            # Look for "SELECT col AS alias" or "SELECT col"
            if isinstance(parsed, (exp.Insert, exp.Create)):
                select_stmt = parsed.find(exp.Select)
                if select_stmt:
                    for expression in select_stmt.expressions:
                        # Case 1: Alias (SELECT email AS user_email)
                        if isinstance(expression, exp.Alias):
                            source_col = expression.this.sql()  # email
                            target_col = expression.alias       # user_email
                        # Case 2: Simple Column (SELECT email)
                        else:
                            source_col = expression.sql()
                            target_col = source_col
                        
                        # Add Column Nodes
                        # Note: We don't verify which source table it comes from in this simplified parser
                        # In full implementation, we need schema awareness or complex aliasing resolution.
                        
                        # Add Source Column
                        src_col_id = self._add_node(source_col, "column", {"parent_table": "source_inferred"})
                        pii_type = self._scan_for_pii(source_col)
                        if pii_type:
                            self.nodes[src_col_id]["pii_present"] = True
                            self.nodes[src_col_id]["data"]["pii_type"] = pii_type
                            self.nodes[src_col_id]["tags"].append("PII")

                        # Add Target Column
                        tgt_col_id = self._add_node(target_col, "column", {"parent_table": target_table_name})
                        
                        # Propagate PII
                        if pii_type:
                             self.nodes[tgt_col_id]["pii_present"] = True
                             self.nodes[tgt_col_id]["data"]["pii_type"] = pii_type
                             self.nodes[tgt_col_id]["tags"].append("PII")
                             self.nodes[tgt_col_id]["tags"].append("Propagated")

                        # Edge
                        self._add_edge(src_col_id, tgt_col_id, "maps_to", f"Projection: {source_col} -> {target_col}")

    def add_manual_lineage(self, source: str, target: str, description: str = "Manual Flow"):
        """
        Allows injection of non-SQL lineage (e.g. Python scripts, API calls).
        """
        src_id = self._add_node(source, "dataset", {"source_type": "manual"})
        tgt_id = self._add_node(target, "dataset", {"source_type": "manual"})
        self._add_edge(src_id, tgt_id, "manual_flow", description)

    def get_graph(self) -> Dict[str, Any]:
        """Returns the graph in JSON format standard for UI libraries."""
        return {
            "nodes": list(self.nodes.values()),
            "edges": self.edges
        }

# Singleton instance
lineage_engine = LineageEngine()

def extract_lineage(sql: str):
    """Legacy wrapper for backward compatibility or simple usage"""
    lineage_engine.parse_sql(sql)
    return lineage_engine.get_graph()
