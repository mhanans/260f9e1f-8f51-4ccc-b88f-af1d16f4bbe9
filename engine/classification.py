from typing import List, Dict
import re
import json
from pathlib import Path

CONFIG_PATH = Path("config/scanner_rules.json")

class ClassificationEngine:
    def __init__(self):
        self.load_config()

        # 2. Context Rules (Automated Labeling) - kept hardcoded for now or can move to config too
        self.context_rules = [
            {"category": "Financial", "keywords": ["gaji", "salary", "rekening", "bank", "transfer", "rupiah", "rp", "keuangan", "pajak"]},
            {"category": "Health", "keywords": ["sakit", "diagnosa", "dokter", "rs", "rawat", "darah", "medis", "pasien"]},
            {"category": "HR", "keywords": ["karyawan", "pegawai", "cuti", "absensi", "kontrak", "rekrutmen", "hrd"]},
            {"category": "Legal", "keywords": ["perjanjian", "hukum", "pidana", "perdata", "pasal", "uu", "regulasi"]}
        ]

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
                self.header_blacklist = [r.pattern.lower() for r in rules if r.rule_type == "deny_list"]
                
                # 2. Context Rules (Document Classification)
                # We expect rules with type "classification"
                # Name = Category (e.g. Financial), Pattern = Keyword (e.g. gaji) or Pattern = list of keywords stringified
                
                db_context_rules = {}
                for r in rules:
                    if r.rule_type == "classification":
                        cat = r.name
                        if cat not in db_context_rules:
                            db_context_rules[cat] = set()
                        # If pattern contain commas, split
                        keywords = [k.strip() for k in r.pattern.split(',') if k.strip()]
                        for k in keywords:
                            db_context_rules[cat].add(k)
                
                # Merge with default or replace? Let's append for now to ensure base functionality.
                for rule in self.context_rules:
                    cat = rule["category"]
                    if cat in db_context_rules:
                        rule["keywords"].extend(list(db_context_rules[cat]))
                        del db_context_rules[cat]
                
                # Add remaining new categories
                for cat, keywords in db_context_rules.items():
                    self.context_rules.append({"category": cat, "keywords": list(keywords)})

        except Exception as e:
            print(f"Error loading classification rules from DB: {e}")
            # Ensure at least defaults are there (set in init)
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
