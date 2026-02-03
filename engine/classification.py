from typing import List, Dict
import re

class ClassificationEngine:
    def __init__(self):
        # 1. Sensitivity Mapping (Data Catalog Logic)
        self.sensitivity_map = {
            "ID_NIK": "Specific/Sensitive",
            "ID_NPWP": "Specific/Sensitive",
            "CREDIT_CARD": "Specific/Sensitive",
            "PHONE_NUMBER": "General",
            "PERSON": "General",
            "EMAIL_ADDRESS": "General",
            "ID_PHONE_ID": "General"
        }

        # 2. Context Rules (Automated Labeling)
        self.context_rules = [
            {"category": "Financial", "keywords": ["gaji", "salary", "rekening", "bank", "transfer", "rupiah", "rp"]},
            {"category": "Health", "keywords": ["sakit", "diagnosa", "dokter", "rs", "rawat", "darah"]},
            {"category": "HR", "keywords": ["karyawan", "pegawai", "cuti", "absensi", "kontrak"]},
            {"category": "Legal", "keywords": ["perjanjian", "hukum", "pidana", "perdata", "pasal"]}
        ]

    def classify_sensitivity(self, pii_type: str) -> str:
        """Returns 'Specific/Sensitive' or 'General' based on PII type."""
        return self.sensitivity_map.get(pii_type, "General")

    def classify_document_category(self, text: str) -> List[str]:
        """Scans text for keywords to determine document category (e.g. Financial)."""
        tags = set()
        text_lower = text.lower()
        
        for rule in self.context_rules:
            for keyword in rule["keywords"]:
                # Simple exact match (can be improved with word boundaries)
                if keyword in text_lower:
                    tags.add(rule["category"])
                    break # One keyword enough for category
        
        return list(tags)

classification_engine = ClassificationEngine()
