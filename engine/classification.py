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
        """Reloads mapping and denied headers from Config JSON."""
        # Defaults
        self.sensitivity_map = {}
        self.header_blacklist = []
        
        if CONFIG_PATH.exists():
            try:
                with open(CONFIG_PATH, "r") as f:
                    data = json.load(f)
                    self.sensitivity_map = data.get("sensitivity_map", {})
                    # Add deny_list lowercased for checking
                    self.header_blacklist = [x.lower() for x in data.get("deny_list", [])]
            except Exception as e:
                print(f"Error loading classification config: {e}")

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
