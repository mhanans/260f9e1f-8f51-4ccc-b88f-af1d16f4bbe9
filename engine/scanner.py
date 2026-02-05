from presidio_analyzer import AnalyzerEngine, PatternRecognizer, RecognizerResult, Pattern
from presidio_analyzer.nlp_engine import NlpEngineProvider
import re
import json
from pathlib import Path

class CustomPIIScanner:
    def __init__(self):
        # Initialize Presidio Analyzer
        self.analyzer = AnalyzerEngine()
        
        # 1. Disable Noise Recognizers (Hardcoded static noise removal)
        useless_recognizers = [
            "USPassportRecognizer", "UsBankRecognizer", "UsItinRecognizer", "UsLicenseRecognizer"
        ]
        self.analyzer.registry.recognizers = [
            r for r in self.analyzer.registry.recognizers 
            if r.name not in useless_recognizers
        ]

        self.deny_words = []
        self.exclude_entities = []
        
        # Load Dynamic Rules (DB Only as Source of Truth)
        # Seeding is handled by main.py on startup
        self.reload_rules()

    def reload_rules(self):
        """Loads Custom Recognizers and Deny List from Database."""
        try:
            from sqlmodel import Session, select
            from api.db import engine
            from api.models import ScanRule
            
            with Session(engine) as session:
                rules = session.exec(select(ScanRule).where(ScanRule.is_active == True)).all()
                if not rules:
                    return 

                # Separation
                deny_rules = [r.pattern for r in rules if r.rule_type == "deny_list"]
                regex_rules = [r for r in rules if r.rule_type == "regex"]
                exclude_rules = [r.pattern for r in rules if r.rule_type == "exclude_entity"]
                
                # A. Update Deny List
                self.deny_words = deny_rules
                self._remove_recognizer("indonesian_header_deny")
                if self.deny_words:
                    deny_recognizer = PatternRecognizer(
                        supported_entity="DENY_LIST",
                        name="indonesian_header_deny",
                        deny_list=list(set(self.deny_words))
                    )
                    self.analyzer.registry.add_recognizer(deny_recognizer)
                
                # B. Update Custom Regex
                for c in regex_rules:
                    self._remove_recognizer(c.name)
                    
                    try:
                        context_list = json.loads(c.context_keywords) if c.context_keywords else []
                    except:
                        context_list = []

                    pat = Pattern(name=f"{c.name}_pattern", regex=c.pattern, score=c.score)
                    rec = PatternRecognizer(
                        supported_entity=c.entity_type, 
                        name=c.name,
                        patterns=[pat],
                        context=context_list,
                        supported_language=None # Apply to ALL languages (EN, ID, etc.)
                    )
                    self.analyzer.registry.add_recognizer(rec)
                    
                # C. Exclude entities
                self.exclude_entities = exclude_rules
                
        except Exception as e:
            print(f"Error loading scanner rules from DB: {e}")
            pass

    def _remove_recognizer(self, name):
        self.analyzer.registry.recognizers = [
            r for r in self.analyzer.registry.recognizers if r.name != name
        ]

    def analyze_text(self, text: str, context: list[str] = None) -> list[dict]:
        """
        Analyzes text for PII. 
        Args:
            text: Text to analyze.
            context: List of context words (e.g. ['phone', 'mobile', 'filename.pdf']) to boost detection.
        """
        # Common Indonesian words often detected as PERSON by Spacy (False Positives)
        COMMON_ID_FALSE_POSITIVES = {
            # Administrative / Address
            "jalan", "jl", "jl.", "gang", "gg", "rt", "rw", "no", "nomor", 
            "kecamatan", "kelurahan", "kabupaten", "kota", "provinsi", 
            "blok", "lantai", "gedung", "menara", "kode", "pos", "komplek",
            
            # Business / Corporate
            "pt", "cv", "persero", "tbk", "ud", "bank", "kcp", "kc", "unit", 
            "kantor", "cabang", "pusat", "divisi", "bagian", "departemen",
            "direktur", "manager", "staf", "admin", "hrd", "pic", "cs",
            "pembayaran", "transaksi", "saldo", "total", "rupiah", "transfer",
            "rekening", "biaya", "tagihan", "faktur", "invoice", "kwitansi",
            "po", "pr", "order", "qty", "amount", "harga", "diskon",
            
            # Time / Calendar
            "tanggal", "bulan", "tahun", "jam", "pukul", "waktu", "hari",
            "senin", "selasa", "rabu", "kamis", "jumat", "sabtu", "minggu",
            "januari", "februari", "maret", "april", "mei", "juni", 
            "juli", "agustus", "september", "oktober", "november", "desember",
            
            # Correspondence / Formal
            "hormat", "kami", "kita", "saya", "anda", "beliau", "mereka",
            "ketua", "sekretaris", "bendahara", "anggota", "pimpinan",
            "kepada", "yth", "dari", "hal", "lampiran", "perihal", "tembusan",
            "catatan", "keterangan", "status", "aktif", "nonaktif", "valid",
            "bapak", "ibu", "sdr", "sdri", "saudara", "pemohon", "penerima",
            
            # Generic Terms / Labels often mistaken for Names
            "jenis", "kelamin", "laki-laki", "perempuan", "pria", "wanita", # Gender
            "tempat", "lahir", "tanggal", "waktu", # Place/Time
            "nrp", "nik", "nis", "nip", "ktp", "sim", "npwp", # ID Names
            "nomor", "no", "kartu", "keluarga", "kk" # Doc Names
        }
        
        # Context that INVALIDATES a Person match (e.g. "Jalan Sudirman" -> Sudirman is not a person here)
        PERSON_NEGATIVE_CONTEXTS = {
            "jalan", "jl", "jl.", "loc", "lokasi", "alamat", "address", # Address indicators
            "bank", "rekening", "atm", "bca", "bri", "mandiri", "bni", # Finance
            "pt", "cv", "perusahaan", "company", # Org
            "kabupaten", "kota", "provinsi", "kec.", "kel.", # Administrative
            "status", "keterangan", "note", "desc", "perihal", "hal", # Random headers
            "tanggal", "date", "hari", # Time headers
            
            # --- NEW ADDITIONS BASED ON FEEDBACK ---
            "jenis", "kelamin", # "Jenis Kelamin" header often followed by "Laki-laki" which might be detected as name
            "tempat", "lahir", # "Tempat Lahir" header
            "nrp", "nis", "nip", "nik", # ID Headers
            "nomor", "ktp", "kk", "npwp", "kartu", "keluarga" # Document Headers
        }

        # Context that VALIDATES a Person match (Strong indicators)
        PERSON_POSITIVE_CONTEXTS = {
            "nama", "name", "customer", "karyawan", "pegawai", "staff",
            "pic", "penanggung", "jawab", "oleh", "by",
            "bapak", "ibu", "bpk", "sdr", "sdri", "mr", "mrs", "ms", "miss"
        }

        # 5. Threshold Filter (Score > 0.4)
        # Note: We use 'en' as the pipeline language to leverage the English NLP model for context/NER.
        # Custom Indonesian recognizers are registered with supported_language=None, so they run alongside English rules.
        results = self.analyzer.analyze(
            text=text, 
            language='en',
            score_threshold=0.4,
            context=context
        )
        
        output = []
        for res in results:
            if res.entity_type == "DENY_LIST": continue
            
            # Helper to check excludes
            if res.entity_type in self.exclude_entities: continue

            extracted_text = text[res.start:res.end]
            extracted_lower = extracted_text.lower().replace(".", "") # simple clean
            
            # Double check deny list for strictness
            if extracted_text.lower() in [x.lower() for x in self.deny_words]: 
                 continue

            # --- SMART FILTERING ---
            
            # --- SMART FILTERING ---
            
            # 1. Filter PERSON False Positives
            if res.entity_type == "PERSON":
                # Rule A: Capitalization Check
                # Names in formal docs almost ALWAYS start with Uppercase. 
                # If it starts with lower eventhough Spacy found it, it's likely a common word in the middle of a sentence.
                # (Exception: if the whole extracted text is UPPERCASE, we keep it)
                if extracted_text and extracted_text[0].islower():
                     continue

                # Rule B: Invalid Particle Check
                # If these words appear INSIDE the detected name, it is definitely NOT a name.
                # e.g. "dan tidak", "yang perlu", "ke dalam"
                INVALID_NAME_PARTICLES = {
                    "dan", "yang", "atau", "untuk", "adalah", "ini", "itu", "dengan",
                    "di", "ke", "dari", "pada", "tersebut", "bisa", "akan", "telah",
                    "sebagai", "jika", "maka", "tidak", "belum", "sudah", "lagi",
                    "oleh", "tentang", "seperti", "yaitu", "yakni", "dalam"
                }
                extracted_words = set(extracted_lower.split())
                if not extracted_words.isdisjoint(INVALID_NAME_PARTICLES):
                    continue

                # Rule C: Common Header/False Positive Check (Startswith)
                # If the detected text STARTS with a blacklist word, reject it.
                # e.g. "Nomor Kartu Keluarga" (Starts with 'nomor') -> Reject
                # e.g. "Tempat Lahir" (Starts with 'tempat') -> Reject
                if any(extracted_lower.startswith(x) for x in COMMON_ID_FALSE_POSITIVES):
                    continue

                # Rule D: Negative Context Lookbehind (e.g. "Jalan Name")
                # Look back ~35 chars
                preceding_text = text[max(0, res.start - 35) : res.start].lower()
                preceding_words = set(re.findall(r'\w+', preceding_text))
                
                if not preceding_words.isdisjoint(PERSON_NEGATIVE_CONTEXTS):
                    # Found a negative context word, so text is likely NOT a person
                    continue

                # Rule E: Positive Context Boost 
                # If positive context exists, we trust it more, BUT rules A/B/C still apply first (sanity checks).
                if not preceding_words.isdisjoint(PERSON_POSITIVE_CONTEXTS):
                    pass 
                else:
                    # Rule F: Standard Sanity Checks
                    if any(char.isdigit() for char in extracted_text): continue
                    if len(extracted_text) < 3: continue
                    if len(extracted_words) > 5: continue # Names rarely > 5 words
                
            # 2. Skip common False Positives for DATE_TIME that look like coordinates or just numbers
            if res.entity_type == "DATE_TIME":
                if re.match(r"^-?\d{1,3}\.\d+$", extracted_text): continue
                if extracted_text.isdigit(): continue # Just a year or number is rarely PII alone

            output.append({
                "type": res.entity_type,
                "start": res.start,
                "end": res.end,
                "score": res.score,
                "text": extracted_text 
            })
        return output

    def analyze_dataframe(self, df):
        """
        Analyzes a pandas DataFrame using Presidio Structured BatchAnalyzerEngine.
        Returns iterator of Dict results.
        """
        try:
            from presidio_structured import BatchAnalyzerEngine
            
            # Initialize Batch Analyzer with our configured analyzer
            batch_analyzer = BatchAnalyzerEngine(analyzer_engine=self.analyzer)
            
            # Analyze - returns generator of BatchAnalysisResult
            # We assume df columns are headers
            result_iter = batch_analyzer.analyze_iterator(df)
            return result_iter
            
        except ImportError:
            # Fallback if library missing despite being in requirements (e.g. dev env mismatch)
            print("presidio-structured not found, falling back to manual loop.")
            results = []
            # Manual fallback logic if needed, but for now let's error or return empty to signal issue
            # Ideally this shouldn't happen if requirements installed.
            return []
        except Exception as e:
            print(f"Batch Analysis Error: {e}")
            return []

# Singleton instance
scanner_engine = CustomPIIScanner()
