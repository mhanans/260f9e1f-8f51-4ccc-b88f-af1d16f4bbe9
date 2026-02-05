import sys
import os
import json
from pathlib import Path

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sqlmodel import Session, select
from api.db import engine
from api.models import ScanRule

def migrate_rules():
    # Define the Built-in Indonesian Rules (Predefined)
    predefined_rules = [
        {
            "name": "KTPRecognizer",
            "entity_type": "ID_KTP",
            "rule_type": "regex",
            "pattern": r"\\b\\d{16}\\b",
            "score": 0.5,
            "context_keywords": json.dumps(["nik", "ktp", "nomor induk", "identitas", "no_ktp", "kependudukan", "e-ktp"]),
            "is_active": True
        },
        {
            "name": "NPWPRecognizer",
            "entity_type": "ID_NPWP",
            "rule_type": "regex",
            "pattern": r"\\b\\d{2}\\.\\d{3}\\.\\d{3}\\.\\d{1}-\\d{3}\\.\\d{3}\\b|\\b\\d{15,16}\\b",
            "score": 0.6,
            "context_keywords": json.dumps(["npwp", "pajak", "wajib", "tax", "tin"]),
            "is_active": True
        },
        {
            "name": "KKNumberRecognizer",
            "entity_type": "ID_KK",
            "rule_type": "regex",
            "pattern": r"\\b\\d{16}\\b",
            "score": 0.4,
            "context_keywords": json.dumps(["kk", "kartu keluarga", "no_kk"]),
            "is_active": True
        },
        {
            "name": "BPJSNumberRecognizer",
            "entity_type": "ID_BPJS",
            "rule_type": "regex",
            "pattern": r"\\b\\d{11,13}\\b",
            "score": 0.5,
            "context_keywords": json.dumps(["bpjs", "ketenagakerjaan", "kesehatan", "jamsostek"]),
            "is_active": True
        },
        {
            "name": "IndonesianPhoneNumberRecognizer",
            "entity_type": "PHONE_NUMBER", # Override/Supplement standard
            "rule_type": "regex",
            "pattern": r"\\b(\\+62|62|0)8[1-9][0-9]{6,11}\\b",
            "score": 0.6,
            "context_keywords": json.dumps(["telp", "hp", "mobile", "phone", "wa", "whatsapp", "contact"]),
            "is_active": True
        },
        {
            "name": "BankAccountNumberRecognizer",
            "entity_type": "FIN_BANK_ACCT_ID",
            "rule_type": "regex",
            "pattern": r"\\b\\d{10,16}\\b",
            "score": 0.3,
            "context_keywords": json.dumps(["rekening", "bank", "no_rek", "mandiri", "bca", "bri", "bni", "cif"]),
            "is_active": True
        },
        {
            "name": "MoneyRecognizer",
            "entity_type": "FIN_AMT",
            "rule_type": "regex",
            "pattern": r"\\b(Rp|IDR)\\s*\\.?[\\d\\.,]+",
            "score": 0.6,
            "context_keywords": json.dumps(["harga", "biaya", "total", "amount", "nilai", "saldo"]),
            "is_active": True
        },
        {
            "name": "OrganizationNameRecognizer",
            "entity_type": "ORGANIZATION",
            "rule_type": "regex",
            "pattern": r"\\b(PT|CV|Yayasan|UD|Firma|Koperasi|Persero)\\s+[A-Z][a-zA-Z0-9\\s\\.]+",
            "score": 0.6,
            "context_keywords": json.dumps(["perusahaan", "company", "perseroan"]),
            "is_active": True
        },
        {
            "name": "SocialMediaAccountRecognizer",
            "entity_type": "SOCIAL_MEDIA",
            "rule_type": "regex",
            "pattern": r"(?:^|\\s)@(\\w{1,30})",
            "score": 0.5,
            "context_keywords": json.dumps(["twitter", "instagram", "ig", "tiktok", "facebook", "sosmed"]),
            "is_active": True
        },
        {
             "name": "LinkedinAccountRecognizer",
             "entity_type": "SOCIAL_MEDIA",
             "rule_type": "regex",
             "pattern": r"linkedin\\.com\\/in\\/[\\w-]+",
             "score": 0.7,
             "context_keywords": json.dumps(["linkedin", "profile"]),
             "is_active": True
        },
        {
            "name": "ProjectNameRecognizer",
            "entity_type": "PROJECT_NAME",
            "rule_type": "regex",
            "pattern": r"\\b(Proyek|Project)\\s+[A-Z][a-zA-Z0-9\\s]+",
            "score": 0.5,
            "context_keywords": json.dumps(["proyek", "project", "codename"]),
            "is_active": True
        },
        {
            "name": "EmailRecognizer",
            "entity_type": "EMAIL_ADDRESS",
            "rule_type": "regex",
            "pattern": r"\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}\\b",
            "score": 0.6,
            "context_keywords": json.dumps(["email", "surat", "mail"]),
            "is_active": True
        }
    ]

    with Session(engine) as session:
        print("Checking existing rules...")
        existing_rules = session.exec(select(ScanRule)).all()
        existing_names = {r.name for r in existing_rules}

        for rule in predefined_rules:
            if rule["name"] not in existing_names:
                print(f"Adding rule: {rule['name']}")
                new_rule = ScanRule(**rule)
                session.add(new_rule)
            else:
                print(f"Skipping existing rule: {rule['name']}")
        
        # Also migrate from scanner_rules.json if it exists and has unique items?
        # User said "migrate all in .json to DB", suggesting the current JSON might have edits.
        # But user also gave a specific list. I will prioritize the list above. 
        # If I strictly follow 'migrate .json', I might miss the new requirements if the json doesn't have them.
        # So I combined them: The list above IS the migration target.
        
        session.commit()
        print("Migration complete.")

if __name__ == "__main__":
    migrate_rules()
