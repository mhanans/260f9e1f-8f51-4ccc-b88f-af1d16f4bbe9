import structlog
import uuid
from typing import List, Dict, Optional, Any, Set
from collections import Counter

try:
    import sqlglot
    from sqlglot import exp
except ImportError:
    sqlglot = None
    exp = None

# Connectors & Engines
try:
    from connectors.db_connector import db_connector
except ImportError:
    db_connector = None

try:
    from engine.scanner import scanner_engine
except ImportError:
    scanner_engine = None

try:
    from engine.classification import classification_engine
except ImportError:
    classification_engine = None

logger = structlog.get_logger()

class LineageEngine:
    def __init__(self):
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.edges: List[Dict[str, Any]] = []

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
        # Check for duplicates
        existing = next((e for e in self.edges if e["id"] == edge["id"]), None)
        if not existing:
            self.edges.append(edge)

    def build_automated_lineage(self, connection_string: str, db_type: str = 'postgresql', sql_logs: List[str] = None):
        """
        1. Fetch Schema Metadata (Real Tables/Columns).
        2. Scan Sample Data for PII (Presidio).
        3. Parse SQL Logs to map flows.
        """
        self.nodes = {}
        self.edges = []
        
        if not db_connector:
            logger.error("DB Connector missing")
            return
            
        # Step 1: Get Metadata
        logger.info(f"Fetching metadata from {connection_string} ({db_type})")
        metadata = db_connector.get_schema_metadata(db_type, connection_string)
        
        table_map = {} # map normalized table name to node_id
        
        for table_info in metadata:
            t_name = table_info["table"]
            cols = table_info["columns"]
            row_count = table_info["row_count"]
            
            # Create Table Node
            t_id = self._add_node(t_name, "table", {"row_count": row_count, "system": db_type})
            table_map[t_name.lower()] = t_id
            
            # Step 2: Scan for PII (Bulk)
            sample_data = db_connector.scan_target(db_type, connection_string, t_name, limit=50)
            
            # Group sample values by column
            column_samples: Dict[str, List[str]] = {c: [] for c in cols}
            for item in sample_data:
                col = item.get("field")
                val = item.get("value")
                if col in column_samples and val:
                    column_samples[col].append(val)
            
            # Analyze each column
            for col_name, values in column_samples.items():
                if not values:
                    c_id = self._add_node(col_name, "column", {"parent_table": t_name})
                    self._add_edge(t_id, c_id, "contains")
                    continue
                
                # Check PII
                joined_text = " ".join(values[:10]) 
                
                if scanner_engine:
                    findings = scanner_engine.analyze_text(joined_text)
                    if findings:
                        types = [f["type"] for f in findings]
                        most_common = Counter(types).most_common(1)
                        pii_type = most_common[0][0] if most_common else None
                        
                        c_id = self._add_node(col_name, "column", {"parent_table": t_name})
                        self._add_edge(t_id, c_id, "contains")
                        
                        if pii_type:
                            self.nodes[c_id]["pii_present"] = True
                            self.nodes[c_id]["data"]["pii_type"] = pii_type
                            self.nodes[c_id]["tags"].append("PII")
                            
                            if classification_engine:
                                sens = classification_engine.classify_sensitivity(pii_type)
                                self.nodes[c_id]["data"]["sensitivity"] = sens
                    else:
                        c_id = self._add_node(col_name, "column", {"parent_table": t_name})
                        self._add_edge(t_id, c_id, "contains")
                else:
                     c_id = self._add_node(col_name, "column", {"parent_table": t_name})
                     self._add_edge(t_id, c_id, "contains")

        # Step 3: Parse SQL Logs for Lineage
        if sql_logs and sqlglot:
            for sql in sql_logs:
                self._process_sql(sql)

    def _process_sql(self, sql: str):
        try:
            parsed = sqlglot.parse_one(sql)
            
            target_table = None
            if isinstance(parsed, (exp.Insert, exp.Create)):
                target_table = parsed.this.sql()
            
            if not target_table:
                return

            tgt_id = self._get_node_id(target_table, "table")
            if tgt_id not in self.nodes:
                # Mark as External or New/Derived
                tgt_id = self._add_node(target_table, "table", {"external": True})
                self.nodes[tgt_id]["tags"].append("Derived")

            # Extract Sources and Columns
            for table in parsed.find_all(exp.Table):
                src_name = table.sql()
                if src_name != target_table:
                    src_id = self._get_node_id(src_name, "table")
                    if src_id not in self.nodes:
                         src_id = self._add_node(src_name, "table", {"external": True})
                    
                    self._add_edge(src_id, tgt_id, "flows_to", "SQL Transform")
            
            # Column Mappings & Propagation
            select_stmt = parsed.find(exp.Select)
            if select_stmt:
                for expression in select_stmt.expressions:
                    source_col = None
                    target_col_name = None
                    
                    if isinstance(expression, exp.Alias):
                        source_col = expression.this.sql()
                        target_col_name = expression.alias
                    elif isinstance(expression, exp.Column):
                         source_col = expression.sql()
                         target_col_name = source_col
                    
                    if source_col and target_col_name:
                        src_c_id = self._find_column_node(source_col)
                        tgt_c_id = self._get_node_id(target_col_name, "column") 
                        
                        if tgt_c_id not in self.nodes:
                             tgt_c_id = self._add_node(target_col_name, "column", {"parent_table": target_table})
                        
                        if src_c_id:
                            self._add_edge(src_c_id, tgt_c_id, "maps_to", "SQL Projection")
                            
                            # PII Propagation
                            if self.nodes[src_c_id].get("pii_present"):
                                self.nodes[tgt_c_id]["pii_present"] = True
                                pii_t = self.nodes[src_c_id]["data"].get("pii_type")
                                self.nodes[tgt_c_id]["data"]["pii_type"] = pii_t
                                self.nodes[tgt_c_id]["tags"].append("Propagated")
                                
                                # Cross-border check
                                if self.nodes[tgt_id].get("data", {}).get("external"):
                                     self.nodes[tgt_id]["tags"].append("Third-party Transfer")
                                     self.nodes[tgt_id]["data"]["compliance_tags"] = ["Data Cross-border"]

        except Exception as e:
            logger.error(f"SQL Parse Error: {e}")

    def _find_column_node(self, col_name: str) -> Optional[str]:
        """Simple lookup for column by name."""
        for nid, n in self.nodes.items():
            if n["type"] == "column" and n["label"].lower() == col_name.lower():
                return nid
        return None

    def add_manual_lineage(self, source: str, target: str, description: str):
        self._add_node(source, "dataset", {"source_type": "manual"})
        self._add_node(target, "dataset", {"source_type": "manual"})
        self._add_edge(self._get_node_id(source, "dataset"), self._get_node_id(target, "dataset"), "manual_flow", description)

    def get_graph(self) -> Dict[str, Any]:
        return {
            "nodes": list(self.nodes.values()),
            "edges": self.edges
        }
    
    def parse_sql(self, sql: str, system_name="SQL_DB"):
        self._process_sql(sql)

lineage_engine = LineageEngine()
