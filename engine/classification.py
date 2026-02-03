from typing import List, Dict
import re

class ClassificationEngine:
    def __init__(self):
        # 1. Sensitivity Mapping based on UU PDP (UU No. 27 Tahun 2022)
        self.sensitivity_map = {
            "ID_NIK": "Spesifik (Data Biometrik/Identitas)",
            "ID_NPWP": "Spesifik (Data Keuangan)",
            "ID_PHONE_ID": "Spesifik (Data Identitas)",
            "CREDIT_CARD": "Spesifik (Data Keuangan)",
            "HEALTH_DATA": "Spesifik (Data Kesehatan)",
            "RELIGION": "Spesifik (Data Keyakinan)",
            "PHONE_NUMBER": "Spesifik (Data Identitas)",
            "EMAIL_ADDRESS": "Spesifik (Data Identitas)",
            "PERSON": "Umum",
            "GENDER": "Umum",
            "DATE_TIME": "Umum"
        }

        # 2. Context Rules (Automated Labeling)
        self.context_rules = [
            {"category": "Financial", "keywords": ["gaji", "salary", "rekening", "bank", "transfer", "rupiah", "rp", "keuangan", "pajak"]},
            {"category": "Health", "keywords": ["sakit", "diagnosa", "dokter", "rs", "rawat", "darah", "medis", "pasien"]},
            {"category": "HR", "keywords": ["karyawan", "pegawai", "cuti", "absensi", "kontrak", "rekrutmen", "hrd"]},
            {"category": "Legal", "keywords": ["perjanjian", "hukum", "pidana", "perdata", "pasal", "uu", "regulasi"]}
        ]

    def classify_sensitivity(self, pii_type: str) -> str:
        """Returns Sensitivity Category based on UU PDP."""
        return self.sensitivity_map.get(pii_type, "Umum")

    def classify_document_category(self, text: str) -> List[str]:
        """Scans text for keywords to determine document category (e.g. Financial)."""
        tags = set()
        text_lower = text.lower()
        
        for rule in self.context_rules:
            for keyword in rule["keywords"]:
                # Use regex word boundary for more accurate matching
                if re.search(r'\b' + re.escape(keyword) + r'\b', text_lower):
                    tags.add(rule["category"])
                    break 
        
        return list(tags)

classification_engine = ClassificationEngine()
