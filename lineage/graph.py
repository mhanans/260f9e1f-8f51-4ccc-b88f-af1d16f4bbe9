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

for lib in ["connectors.db_connector", "engine.scanner", "engine.classification"]:
    try:
        exec(f"from {lib} import {lib.split('.')[-1].replace('custom_', '')}") # approximate logic, cleaner to just try-import
    except: pass
    
# Proper imports
try: from connectors.db_connector import db_connector
except: db_connector = None
try: from engine.scanner import scanner_engine
except: scanner_engine = None
try: from engine.classification import classification_engine
except: classification_engine = None

logger = structlog.get_logger()

class LineageEngine:
    def __init__(self):
        # Global Storage: Not reset every scan
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.edges: List[Dict[str, Any]] = []

    def _get_node_id(self, name: str, type_label: str, system: str, parent: str = None) -> str:
        """
        Global ID Format: system::type::parent::name
        Example: prod_db::column::users::email
        """
        sys_clean = system.lower().replace(" ", "_").replace(":", "")
        if type_label == "column" and parent:
             return f"{sys_clean}::column::{parent.lower()}::{name.lower()}"
        return f"{sys_clean}::{type_label.lower()}::{name.lower()}"

    def _add_node(self, name: str, node_type: str, metadata: Dict[str, Any] = None):
        """Adds or updates a node in the graph."""
        system = metadata.get("system", "unknown_system")
        parent = metadata.get("parent_table")
        
        node_id = self._get_node_id(name, node_type, system, parent)
        
        if node_id not in self.nodes:
            self.nodes[node_id] = {
                "id": node_id,
                "label": name,
                "type": node_type,  
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

    def build_global_catalog(self, all_connections: List[Dict]):
        """Main function for OneTrust-style Global Metadata Discovery"""
        logger.info("Starting Global Metadata Discovery")
        
        # Reset graph for full rebuild (or optionally keep)
        self.nodes = {}
        self.edges = [] 
        
        for conn in all_connections:
            conn_name = conn["name"]
            conn_type = conn["type"]
            conn_details = conn.get("details", "")
            
            try:
                # 1. Database Metadata Crawling
                if "Database" in conn_type and db_connector:
                    # Determine type, default 'postgresql'
                    db_sys_type = 'postgresql'
                    metadata = db_connector.get_schema_metadata(db_sys_type, conn_details)
                    
                    for table_info in metadata:
                        t_name = table_info["table"]
                        cols = table_info["columns"]
                        # Add Table Node
                        t_id = self._add_node(t_name, "table", {
                            "system": conn_name, 
                            "row_count": table_info["row_count"],
                            "conn_type": conn_type
                        })
                        
                        # Add Column Nodes
                        for col in cols:
                            c_id = self._add_node(col, "column", {
                                "system": conn_name, 
                                "parent_table": t_name
                            })
                            self._add_edge(t_id, c_id, "contains")
                            
                            # Integrate PII Check?
                            # For full catalog, we can check basic regex or name heuristic if deep scan is too slow
                            # Or do a lightweight scan. Let's do Name Heuristic + Optional Lightweight Sample if desired
                            # For now: Name Heuristic to emulate "Smart" categorization at scale
                            pii_type = self._name_heuristic(col)
                            if pii_type:
                                self.nodes[c_id]["pii_present"] = True
                                self.nodes[c_id]["data"]["pii_type"] = pii_type

                # 2. S3/Storage Metadata
                elif "S3" in conn_type:
                    s3_id = self._add_node(conn_name, "bucket", {"system": conn_name, "conn_type": conn_type})
                    # Add dummy files if list available
                    pass

            except Exception as e:
                logger.error(f"Error scanning connection {conn_name}: {e}")

        # 2. Smart Reconciler
        self._reconcile_cross_system_flows()
        
    def _name_heuristic(self, col_name):
        lower = col_name.lower()
        if "email" in lower: return "EMAIL_ADDRESS"
        if "nik" in lower: return "ID_NIK"
        if "phone" in lower or "hp" in lower: return "PHONE_NUMBER"
        return None

    def _reconcile_cross_system_flows(self):
        """
        Smart Linker: heuristic matching of similar columns across unrelated systems.
        """
        # Find all column nodes
        columns = [n for n in self.nodes.values() if n["type"] == "column"]
        
        # Group by (label, pii_type) tuple as key
        grouped = {}
        for col in columns:
            label = col["label"].lower()
            p_type = col["data"].get("pii_type")
            key = (label, p_type)
            if key not in grouped: grouped[key] = []
            grouped[key].append(col)
            
        for key, group in grouped.items():
            if len(group) > 1:
                # Potential match
                for i in range(len(group)):
                    for j in range(i+1, len(group)):
                        node_a = group[i]
                        node_b = group[j]
                        sys_a = node_a["data"].get("system")
                        sys_b = node_b["data"].get("system")
                        
                        # Link if different systems
                        if sys_a and sys_b and sys_a != sys_b:
                            self._add_edge(node_a["id"], node_b["id"], "probable_flow", "Smart Match (Heuristic)")

    # UI Helpers
    def get_impact_path(self, start_node_id: str) -> Set[str]:
        visited = set()
        queue = [start_node_id]
        while queue:
            current = queue.pop(0)
            if current in visited: continue
            visited.add(current)
            for edge in self.edges:
                if edge["source"] == current:
                    queue.append(edge["target"])
        return visited

    def get_graph(self) -> Dict[str, Any]:
        return {"nodes": list(self.nodes.values()), "edges": self.edges}
    
    # Legacy / Simulation Support
    def parse_sql(self, sql: str):
        self._process_sql(sql, system="OneTrust_Simulation")
             
    def add_manual_lineage(self, source: str, target: str, description: str):
         # Create distinct manual system nodes
         sys = "Manual_Flows"
         src_id = self._add_node(source, "dataset", {"system": sys})
         tgt_id = self._add_node(target, "dataset", {"system": sys})
         self._add_edge(src_id, tgt_id, "manual_flow", description)
         
    def _process_sql(self, sql: str, system="SQL_DB"):
        # Simplified parser that tries to hook into existing global nodes or creates ephemeral
        try:
            parsed = sqlglot.parse_one(sql)
            tgt = parsed.this.sql() if isinstance(parsed, (exp.Insert, exp.Create)) else None
            if not tgt: return
            
            tgt_id = self._add_node(tgt, "table", {"system": system})
            
            for t in parsed.find_all(exp.Table):
                src = t.sql()
                if src != tgt:
                    src_id = self._add_node(src, "table", {"system": system})
                    self._add_edge(src_id, tgt_id, "flows_to", "SQL")
        except: pass

lineage_engine = LineageEngine()
