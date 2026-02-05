from typing import List, Dict
import re
import json
from pathlib import Path

CONFIG_PATH = Path("config/scanner_rules.json")

class ClassificationEngine:
    def __init__(self):
        # 2. Context Rules (Automated Labeling) - kept hardcoded for now or can move to config too
        self.context_rules = [
            {"category": "Financial", "keywords": ["gaji", "salary", "rekening", "bank", "transfer", "rupiah", "rp", "keuangan", "pajak"]},
            {"category": "Health", "keywords": ["sakit", "diagnosa", "dokter", "rs", "rawat", "darah", "medis", "pasien"]},
            {"category": "HR", "keywords": ["karyawan", "pegawai", "cuti", "absensi", "kontrak", "rekrutmen", "hrd"]},
            {"category": "Legal", "keywords": ["perjanjian", "hukum", "pidana", "perdata", "pasal", "uu", "regulasi"]}
        ]
        
        self.load_config()

    def load_config(self):
        """Reloads mapping and denied headers from Database specific ScanRules."""
        self.sensitivity_map = {}
        self.header_blacklist = []
        # context_rules defaults are hardcoded but we will try to append/override from DB
        
        try:
            from sqlmodel import Session, select
            from api.db import engine
            from api.models import ScanRule
            
            with Session(engine) as session:
                rules = session.exec(select(ScanRule).where(ScanRule.is_active == True)).all()
                
                # 1. Deny List (Header Blacklist)
                self.header_blacklist = [r.pattern.lower() for r in rules if r.rule_type == "deny_list"] # Use deny_list type
                
                # 1.5 Exclude Entities (Scanner Logic usually, but good to have here if needed)
                # (Scanner engine handles exclude separately)

                # 2. Context Rules (Document Classification)
                db_context_rules = {}
                for r in rules:
                    if r.rule_type == "classification":
                        cat = r.name
                        if cat not in db_context_rules:
                            db_context_rules[cat] = set()
                        keywords = [k.strip() for k in r.pattern.split(',') if k.strip()]
                        for k in keywords:
                            db_context_rules[cat].add(k)
                    
                    # 3. NEW: Sensitivity Map
                    elif r.rule_type == "sensitivity":
                        # Pattern = Classification Label, Entity_Type = PII Type
                        if r.entity_type:
                            self.sensitivity_map[r.entity_type] = r.pattern

                # Merge Context Rules
                for rule in self.context_rules:
                    cat = rule["category"]
                    if cat in db_context_rules:
                        rule["keywords"].extend(list(db_context_rules[cat]))
                        del db_context_rules[cat]
                
                for cat, keywords in db_context_rules.items():
                    self.context_rules.append({"category": cat, "keywords": list(keywords)})

        except Exception as e:
            print(f"Error loading classification rules from DB: {e}")
            pass

    def classify_sensitivity(self, pii_type: str) -> str:
        """Returns Sensitivity Category based on UU PDP."""
        # Reload on call to allow dynamic updates? 
        # For performance, usually load once. But for local tool, we can reload or have a refresh method.
        # Let's rely on explicit reload call or init.
        return self.sensitivity_map.get(pii_type, "Umum/Lainnya")

    def is_false_positive(self, text: str, entity_type: str) -> bool:
        """Mencegah Label Dokumen terdeteksi sebagai Data."""
        text_clean = text.lower().strip()
        
        # 1. Header Blacklist Check
        if text_clean in self.header_blacklist:
            return True
            
        # 2. NIK validation
        if entity_type == "ID_NIK" and not re.search(r'\d{16}', text_clean):
            return True
            
        return False

    def classify_document_category(self, text: str) -> List[str]:
        """Scans text for keywords to determine document category (e.g. Financial)."""
        tags = set()
        text_lower = text.lower()
        
        for rule in self.context_rules:
            for keyword in rule["keywords"]:
                if re.search(r'\b' + re.escape(keyword) + r'\b', text_lower):
                    tags.add(rule["category"])
                    break 
        
        return list(tags)

classification_engine = ClassificationEngine()
