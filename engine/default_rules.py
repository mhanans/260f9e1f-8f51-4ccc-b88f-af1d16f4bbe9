import json

# Standard Indonesian PII Ruleset (Built-in Defaults)
# This module serves as the initial seed data.
# It is NOT used at runtime by the scanner; the scanner reads from the DB.

DEFAULT_INDO_RULES = [
    {
        "name": "KTPRecognizer",
        "entity_type": "ID_KTP",
        "rule_type": "regex",
        "pattern": r"\b\d{16}\b",
        "score": 0.5,
        "context_keywords": json.dumps(["nik", "ktp", "nomor induk", "identitas", "no_ktp", "kependudukan", "e-ktp"]),
        "is_active": True
    },
    {
        "name": "NPWPRecognizer",
        "entity_type": "ID_NPWP",
        "rule_type": "regex",
        "pattern": r"\b\d{2}\.\d{3}\.\d{3}\.\d{1}-\d{3}\.\d{3}\b|\b\d{15,16}\b",
        "score": 0.6,
        "context_keywords": json.dumps(["npwp", "pajak", "wajib", "tax", "tin"]),
        "is_active": True
    },
    {
        "name": "KKNumberRecognizer",
        "entity_type": "ID_KK",
        "rule_type": "regex",
        "pattern": r"\b\d{16}\b",
        "score": 0.5,
        "context_keywords": json.dumps(["kk", "keluarga", "family", "card"]),
        "is_active": True
    },
    {
        "name": "BPJSNumberRecognizer",
        "entity_type": "ID_BPJS",
        "rule_type": "regex",
        "pattern": r"\b\d{11,13}\b",
        "score": 0.5,
        "context_keywords": json.dumps(["bpjs", "ketenagakerjaan", "kesehatan", "jamsostek"]),
        "is_active": True
    },
    {
        "name": "IndoPhoneNumber",
        "entity_type": "PHONE_NUMBER",
        "rule_type": "regex",
        "pattern": r"(\+62|62|0)8[1-9][0-9]{6,10}",
        "score": 0.6,
        "context_keywords": json.dumps(["telp", "wa", "hp", "phone", "mobile"]),
        "is_active": True
    },
    {
        "name": "BankAccountNumberRecognizer",
        "entity_type": "FIN_BANK_ACCT_ID",
        "rule_type": "regex",
        "pattern": r"\b\d{10,16}\b",
        "score": 0.3,
        "context_keywords": json.dumps(["rekening", "bank", "no_rek", "mandiri", "bca", "bri", "bni", "cif"]),
        "is_active": True
    },
    {
        "name": "MoneyRecognizer",
        "entity_type": "FIN_AMT",
        "rule_type": "regex",
        "pattern": r"\b(Rp|IDR)\s*\.?[0-9\.,]+",
        "score": 0.6,
        "context_keywords": json.dumps(["harga", "biaya", "total", "amount", "nilai", "saldo"]),
        "is_active": True
    },
    {
        "name": "OrganizationNameRecognizer",
        "entity_type": "ORGANIZATION",
        "rule_type": "regex",
        "pattern": r"\b(PT|CV|Yayasan|UD|Firma|Koperasi|Persero)\s+[A-Z][a-zA-Z0-9\s\.]+",
        "score": 0.6,
        "context_keywords": json.dumps(["perusahaan", "company", "perseroan"]),
        "is_active": True
    },
    {
        "name": "SocialMediaAccountRecognizer",
        "entity_type": "SOCIAL_MEDIA",
        "rule_type": "regex",
        "pattern": r"(?:^|\s)@(\w{1,30})",
        "score": 0.5,
        "context_keywords": json.dumps(["twitter", "instagram", "ig", "tiktok", "facebook", "sosmed"]),
        "is_active": True
    },
    {
            "name": "LinkedinAccountRecognizer",
            "entity_type": "SOCIAL_MEDIA",
            "rule_type": "regex",
            "pattern": r"linkedin\.com\/in\/[\w-]+",
            "score": 0.7,
            "context_keywords": json.dumps(["linkedin", "profile"]),
            "is_active": True
    },
    {
        "name": "ProjectNameRecognizer",
        "entity_type": "PROJECT_NAME",
        "rule_type": "regex",
        "pattern": r"\b(Proyek|Project)\s+[A-Z][a-zA-Z0-9\s]+",
        "score": 0.5,
        "context_keywords": json.dumps(["proyek", "project", "codename"]),
        "is_active": True
    },
    {
        "name": "EmailRecognizer",
        "entity_type": "EMAIL_ADDRESS",
        "rule_type": "regex",
        "pattern": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
        "score": 0.6,
        "context_keywords": json.dumps(["email", "surat", "mail"]),
        "is_active": True
    }
]

# --- DYNAMIC SEEDING FOR PERSON FILTERS ---
# To avoid manually listing hundreds of rules, we define them here and extend the list.

_person_false_positives = [
    # Admin
    "jalan", "jl", "jl.", "gang", "gg", "rt", "rw", "no", "nomor", 
    "kecamatan", "kelurahan", "kabupaten", "kota", "provinsi", 
    "blok", "lantai", "gedung", "menara", "kode", "pos", "komplek",
    # Biz
    "pt", "cv", "persero", "tbk", "ud", "bank", "kcp", "kc", "unit", 
    "kantor", "cabang", "pusat", "divisi", "bagian", "departemen",
    "direktur", "manager", "staf", "admin", "hrd", "pic", "cs",
    "pembayaran", "transaksi", "saldo", "total", "rupiah", "transfer",
    "rekening", "biaya", "tagihan", "faktur", "invoice", "kwitansi",
    "po", "pr", "order", "qty", "amount", "harga", "diskon",
    # Time
    "tanggal", "bulan", "tahun", "jam", "pukul", "waktu", "hari",
    "senin", "selasa", "rabu", "kamis", "jumat", "sabtu", "minggu",
    "januari", "februari", "maret", "april", "mei", "juni", 
    "juli", "agustus", "september", "oktober", "november", "desember",
    # Formal
    "hormat", "kami", "kita", "saya", "anda", "beliau", "mereka",
    "ketua", "sekretaris", "bendahara", "anggota", "pimpinan",
    "kepada", "yth", "dari", "hal", "lampiran", "perihal", "tembusan",
    "catatan", "keterangan", "status", "aktif", "nonaktif", "valid",
    "bapak", "ibu", "sdr", "sdri", "saudara", "pemohon", "penerima",
    # Generic
    "jenis", "kelamin", "laki-laki", "perempuan", "pria", "wanita",
    "tempat", "lahir", "nrp", "nik", "nis", "nip", "ktp", "sim", "npwp",
    "kartu", "keluarga", "kk"
]

_person_negative_contexts = [
    "jalan", "jl", "jl.", "loc", "lokasi", "alamat", "address",
    "bank", "rekening", "atm", "bca", "bri", "mandiri", "bni",
    "pt", "cv", "perusahaan", "company",
    "kabupaten", "kota", "provinsi", "kec.", "kel.",
    "status", "keterangan", "note", "desc", "perihal", "hal",
    "tanggal", "date", "hari",
    "jenis", "kelamin", "tempat", "lahir",
    "nrp", "nis", "nip", "nik",
    "nomor", "ktp", "kk", "npwp", "kartu", "keluarga"
]

_person_invalid_particles = [
    "dan", "yang", "atau", "untuk", "adalah", "ini", "itu", "dengan",
    "di", "ke", "dari", "pada", "tersebut", "bisa", "akan", "telah",
    "sebagai", "jika", "maka", "tidak", "belum", "sudah", "lagi",
    "oleh", "tentang", "seperti", "yaitu", "yakni", "dalam"
]

for word in set(_person_false_positives):
    DEFAULT_INDO_RULES.append({
        "name": f"fp_person_{word.replace('.', '')}",
        "entity_type": "PERSON_FILTER",
        "rule_type": "false_positive_person",
        "pattern": word,
        "score": 1.0,
        "is_active": True
    })

for word in set(_person_negative_contexts):
    DEFAULT_INDO_RULES.append({
        "name": f"neg_ctx_{word.replace('.', '')}",
        "entity_type": "PERSON_FILTER",
        "rule_type": "negative_context_person",
        "pattern": word,
        "score": 1.0,
        "is_active": True
    })

for word in set(_person_invalid_particles):
    DEFAULT_INDO_RULES.append({
        "name": f"inv_part_{word}",
        "entity_type": "PERSON_FILTER",
        "rule_type": "invalid_particle_person",
        "pattern": word,
        "score": 1.0,
        "is_active": True
    })
