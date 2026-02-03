from typing import List, Dict
import re

class ClassificationEngine:
    def __init__(self):
        # 1. Sensitivity Mapping based on UU PDP (UU No. 27 Tahun 2022) NO REVISION NEEDED AS PER USER REQUEST, JUST ALIGNMENT
        self.sensitivity_map = {
            # DATA SPESIFIK (Pasal 4 ayat 2)
            "ID_NIK": "Spesifik (Identitas)",
            "ID_NPWP": "Spesifik (Keuangan)",
            "CREDIT_CARD": "Spesifik (Keuangan)",
            "HEALTH_DATA": "Spesifik (Kesehatan)",
            "BIOMETRIC": "Spesifik (Biometrik)",
            "CRIMINAL_RECORD": "Spesifik (Catatan Kejahatan)",
            "CHILD_DATA": "Spesifik (Anak)",
            "RELIGION": "Spesifik (Data Keyakinan)", # Often Specific in practice
            "ID_PHONE_ID": "Spesifik (Identitas)", # Mapping internal ID to specific
            
            # DATA UMUM (Pasal 4 ayat 3)
            "PERSON": "Umum (Nama)",
            "GENDER": "Umum (Jenis Kelamin)",
            "PHONE_NUMBER": "Umum (Kontak)",
            "EMAIL_ADDRESS": "Umum (Kontak)",
            "LOCATION": "Umum (Alamat)",
            "DATE_TIME": "Umum (Waktu)"
        }

        # 2. Context Rules (Automated Labeling)
        self.context_rules = [
            {"category": "Financial", "keywords": ["gaji", "salary", "rekening", "bank", "transfer", "rupiah", "rp", "keuangan", "pajak"]},
            {"category": "Health", "keywords": ["sakit", "diagnosa", "dokter", "rs", "rawat", "darah", "medis", "pasien"]},
            {"category": "HR", "keywords": ["karyawan", "pegawai", "cuti", "absensi", "kontrak", "rekrutmen", "hrd"]},
            {"category": "Legal", "keywords": ["perjanjian", "hukum", "pidana", "perdata", "pasal", "uu", "regulasi"]}
        ]
        
        # Blacklist to avoid False Positives (Header vs Data)
        self.header_blacklist = ["nik", "nomor", "nama", "tempat", "tanggal", "alamat", "telepon", "handphone", "npwp", "halaman", "page"]

    def classify_sensitivity(self, pii_type: str) -> str:
        """Returns Sensitivity Category based on UU PDP."""
        return self.sensitivity_map.get(pii_type, "Umum/Lainnya")

    def is_false_positive(self, text: str, entity_type: str) -> bool:
        """Mencegah Label Dokumen terdeteksi sebagai Data."""
        text_clean = text.lower().strip()
        
        # Jika teks yang dideteksi ada di blacklist (berarti itu cuma header)
        if text_clean in self.header_blacklist:
            return True
            
        # NIK harus 16 digit, kalau cuma kata 'NIK' maka itu False Positive
        if entity_type == "ID_NIK" and not re.search(r'\d{16}', text_clean):
            return True
            
        return False

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
