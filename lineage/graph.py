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
        
        self.nodes = {}
        self.edges = [] 
        
        for conn in all_connections:
            conn_name = conn["name"]
            conn_type = conn["type"]
            conn_details = conn.get("details", "")
            
            try:
                # 1. Database Metadata Crawling
                if "Database" in conn_type and db_connector:
                    db_sys_type = 'postgresql' # Default for now
                    metadata = db_connector.get_schema_metadata(db_sys_type, conn_details)
                    
                    for table_info in metadata:
                        t_name = table_info["table"]
                        cols = table_info["columns"]
                        
                        t_id = self._add_node(t_name, "table", {
                            "system": conn_name, 
                            "row_count": table_info["row_count"],
                            "conn_type": conn_type,
                            "data_subject_type": self._guess_subject_type(t_name)
                        })
                        
                        # Fetch Sample Data for PII Validation (Real Data-Driven)
                        # We try to get a small sample for the whole table to minimize queries
                        table_sample = []
                        if db_connector:
                             table_sample = db_connector.scan_target(db_sys_type, conn_details, t_name, limit=10)
                        
                        # Process Columns
                        for col in cols:
                            c_id = self._add_node(col, "column", {
                                "system": conn_name, 
                                "parent_table": t_name
                            })
                            self._add_edge(t_id, c_id, "contains")
                            
                            # Real PII Detection
                            self._detect_pii_real(c_id, col, table_sample)

                # 2. S3/Storage Metadata
                elif "S3" in conn_type:
                    # Create Bucket Node
                    s3_id = self._add_node(conn_name, "bucket", {"system": conn_name, "conn_type": conn_type})
                    # Note: Files within S3 will be populated via inject_scan_results if scanned

            except Exception as e:
                logger.error(f"Error scanning connection {conn_name}: {e}")

        # 3. Smart Reconciler
        self._reconcile_cross_system_flows()
        
    def _guess_subject_type(self, table_name: str) -> str:
        t = table_name.lower()
        if "user" in t or "cust" in t: return "Customer"
        if "emp" in t: return "Employee"
        return "General"

    def _detect_pii_real(self, node_id: str, col_name: str, sample_data: List[Dict]):
        """
        Uses Presidio Scanner on actual sample data to determine PII.
        """
        # Filter sample for this column
        values = [row.get("value") for row in sample_data if row.get("field") == col_name and row.get("value")]
        if not values: return

        joined_text = " ".join(values[:5]) # Context window
        
        pii_type = None
        if scanner_engine:
            findings = scanner_engine.analyze_text(joined_text)
            if findings:
                # Get most frequent type
                types = [f["type"] for f in findings]
                if types:
                    pii_type = Counter(types).most_common(1)[0][0]
        
        # Fallback to name heuristic for obvious fields if scanner misses (optional, generic safety)
        if not pii_type:
             pii_type = self._name_heuristic(col_name)

        if pii_type:
            self._enrich_pii_node(node_id, pii_type)

    def _enrich_pii_node(self, node_id: str, pii_type: str):
        self.nodes[node_id]["pii_present"] = True
        self.nodes[node_id]["data"]["pii_type"] = pii_type
        
        # Risk & Compliance
        risk = "Low"
        if pii_type in ["EMAIL_ADDRESS", "PHONE_NUMBER", "IP_ADDRESS"]: risk = "Medium"
        if pii_type in ["ID_NIK", "CREDIT_CARD", "IBAN_CODE"]: risk = "High"
        
        self.nodes[node_id]["data"]["risk_level"] = risk
        self.nodes[node_id]["tags"].append("PII")
        if risk == "High": self.nodes[node_id]["tags"].append("Sensitive")

    def _name_heuristic(self, col_name):
        lower = col_name.lower()
        if "email" in lower: return "EMAIL_ADDRESS"
        if "nik" in lower: return "ID_NIK"
        if "phone" in lower or "hp" in lower: return "PHONE_NUMBER"
        return None

    def _reconcile_cross_system_flows(self):
        """
        Smart Linker with Blacklist to prevent Spaghetti Graph.
        """
        blacklist = ["id", "uuid", "created_at", "updated_at", "status", "is_active", "type", "category"]
        
        columns = [n for n in self.nodes.values() if n["type"] == "column"]
        grouped = {}
        
        for col in columns:
            label = col["label"].lower()
            if label in blacklist: continue # Skip generic IDs
            
            p_type = col["data"].get("pii_type")
            # Only exact match names, but ideally we match (Name + PII Type) for stronger confidence
            key = (label, p_type)
            if key not in grouped: grouped[key] = []
            grouped[key].append(col)
            
        for key, group in grouped.items():
            if len(group) > 1:
                label, p_type = key
                # Strict: Only link if PII is present OR name is very specific (len > 3)
                if not p_type and len(label) < 4: continue
                
                for i in range(len(group)):
                    for j in range(i+1, len(group)):
                        node_a = group[i]
                        node_b = group[j]
                        sys_a = node_a["data"].get("system")
                        sys_b = node_b["data"].get("system")
                        
                        if sys_a and sys_b and sys_a != sys_b:
                            self._add_edge(node_a["id"], node_b["id"], "probable_flow", "Smart Match")

    def inject_scan_results(self, csv_path: str = "data.csv"):
        """
        Ingests historical File Scan results (e.g. from local/S3 exports) into the graph.
        """
        import pandas as pd
        import os
        
        if not os.path.exists(csv_path):
            logger.warning(f"Scan history {csv_path} not found.")
            return

        try:
            df = pd.read_csv(csv_path)
            # Expected cols: file, pii_type, confidence, etc.
            # We assume 'file' column contains the file name/path
            
            for _, row in df.iterrows():
                file_name = row.get("file", row.get("filename", "unknown_file"))
                pii_type = row.get("pii_type", row.get("type", None))
                
                # File Node (Represented as a dataset/table in generic system 'FileStorage')
                # Try to map to existing S3 bucket if path matches, else generic 'Local/Export'
                system = "FileStorage"
                if "minio" in file_name or "s3" in str(file_name): system = "S3_Storage"
                
                f_id = self._add_node(file_name, "file", {"system": system})
                
                if pii_type:
                    # In files, the File itself is the container of PII. 
                    # Optionally create a 'column' node if we had specific location info, 
                    # but usually files are treated as atomic datasets.
                    self._enrich_pii_node(f_id, pii_type)
                    
        except Exception as e:
            logger.error(f"Error injecting CSV scans: {e}")

    def propagate_pii_labels(self):
        """
        Propagates PII attributes downstream.
        Iterative approach to handle chains.
        """
        changed = True
        while changed:
            changed = False
            for edge in self.edges:
                src = self.nodes.get(edge["source"])
                tgt = self.nodes.get(edge["target"])
                
                if src and tgt and src.get("pii_present") and not tgt.get("pii_present"):
                    # Propagate
                    pii_t = src["data"].get("pii_type")
                    self._enrich_pii_node(tgt["id"], pii_t)
                    tgt["tags"].append("Propagated")
                    changed = True

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
