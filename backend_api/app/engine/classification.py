
from typing import List, Dict
import re
import json
from pathlib import Path

class ClassificationEngine:
    def __init__(self):
        self.context_rules = [] 
        self.sensitivity_map = {}
        self.header_blacklist = []
        
        self.load_config()

    def load_config(self):
        """Reloads mapping, denied headers, and document classification rules from Database."""
        self.sensitivity_map = {}
        self.header_blacklist = []
        self.context_rules = []
        
        # Temp storage for classification merging
        classification_map = {} # {category: set(keywords)}
        
        try:
            from sqlmodel import Session, select
            from app.core.db import engine
            from app.models.all_models import ScanRule
            
            with Session(engine) as session:
                rules = session.exec(select(ScanRule).where(ScanRule.is_active == True)).all()
                
                for r in rules:
                    # 1. Deny List (Header Blacklist)
                    if r.rule_type == "deny_list":
                        self.header_blacklist.append(r.pattern.lower())
                    
                    # 2. Document Classification (Context Tags)
                    elif r.rule_type == "classification":
                        cat_name = r.name.replace("class_", "") if r.name.startswith("class_") else r.name
                        
                        if cat_name not in classification_map:
                            classification_map[cat_name] = set()
                        
                        keywords = [k.strip().lower() for k in r.pattern.split(',') if k.strip()]
                        classification_map[cat_name].update(keywords)
                    
                    # 3. Sensitivity Map
                    elif r.rule_type == "sensitivity":
                        if r.entity_type:
                            self.sensitivity_map[r.entity_type] = r.pattern

                # Finalize Classification Rules List
                for cat, keywords in classification_map.items():
                    self.context_rules.append({
                        "category": cat,
                        "keywords": list(keywords)
                    })

        except Exception as e:
            print(f"Error loading classification rules from DB: {e}")
            pass

    def classify_sensitivity(self, pii_type: str) -> str:
        """Returns Sensitivity Category based on UU PDP."""
        return self.sensitivity_map.get(pii_type, "Umum/Lainnya")

    def is_false_positive(self, text: str, entity_type: str) -> bool:
        """Mencegah Label Dokumen terdeteksi sebagai Data."""
        text_clean = text.lower().strip()
        
        if text_clean in self.header_blacklist:
            return True
            
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
