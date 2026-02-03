from presidio_analyzer import AnalyzerEngine, PatternRecognizer, RecognizerResult, Pattern
from presidio_analyzer.nlp_engine import NlpEngineProvider
import re

class CustomPIIScanner:
    def __init__(self):
        # Initialize Presidio Analyzer
        # Using 'en' model but adding significant filtering for Indonesian contexts
        self.analyzer = AnalyzerEngine()
        
        # 1. Disable Noise Recognizers (US-centric low value ones for this context)
        # We identify them by name usually, or removing the standard ones.
        # Since removing by instance is hard, we just ignore them in results or override.
        # A cleaner way is to remove them from the registry.
        useless_recognizers = [
            "USPassportRecognizer",
            "UsBankRecognizer",
            "UsItinRecognizer",
            "UsLicenseRecognizer",
        ]
        
        # Filter out these recognizers
        self.analyzer.registry.recognizers = [
            r for r in self.analyzer.registry.recognizers 
            if r.name not in useless_recognizers
        ]

        self._add_custom_recognizers()
        self._add_deny_list()

    def _add_deny_list(self):
        # 2. Deny List (Common headers that trigger FP for Person/NRP)
        deny_list = [
            "NIK", "Nomor Induk Kependudukan", "Nama", "Tempat", "Tanggal", "Lahir",
            "Alamat", "Nomor", "NPWP", "Rekening", "Jenis Kelamin", "Ibu Kandung",
            "Status", "Agama", "Pekerjaan", "Kewarganegaraan", "Berlaku Hingga",
            "Golongan Darah", "Kel/Desa", "Kecamatan", "RTRW", "Kelurahan", "Desa",
            "Konfigurasi", "Menggunakan", "Halaman", "Page", "Total"
        ]
        
        deny_recognizer = PatternRecognizer(
            supported_entity="DENY_LIST",
            name="indonesian_header_deny",
            deny_list=deny_list
        )
        self.analyzer.registry.add_recognizer(deny_recognizer)

    def _add_custom_recognizers(self):
        # 3. Enhanced NIK Recognizer with Context
        nik_pattern = Pattern(
            name="nik_pattern",
            regex=r"\b\d{16}\b",
            score=0.6 # Base score
        )
        nik_recognizer = PatternRecognizer(
            supported_entity="ID_NIK",
            name="id_nik_recognizer",
            patterns=[nik_pattern],
            context=["nik", "nomor", "induk", "kependudukan", "ktp"]
        )
        self.analyzer.registry.add_recognizer(nik_recognizer)

        # 4. Enhanced NPWP with Context
        npwp_pattern = Pattern(
            name="npwp_pattern",
            regex=r"\b\d{2}\.\d{3}\.\d{3}\.\d{1}-\d{3}\.\d{3}\b",
            score=0.6
        )
        npwp_recognizer = PatternRecognizer(
            supported_entity="ID_NPWP",
            name="id_npwp_recognizer",
            patterns=[npwp_pattern],
            context=["npwp", "pajak", "wajib"]
        )
        self.analyzer.registry.add_recognizer(npwp_recognizer)
        
        # Indonesian Phone Number (+62 or 08)
        phone_pattern = Pattern(
             name="phone_pattern",
             regex=r"\b(\+62|62|0)8[1-9][0-9]{6,11}\b",
             score=0.5
        )
        phone_recognizer = PatternRecognizer(
             supported_entity="ID_PHONE_ID",
             name="id_phone_recognizer",
             patterns=[phone_pattern],
             context=["telp", "telepon", "hp", "handphone", "wa", "whatsapp"]
        )
        self.analyzer.registry.add_recognizer(phone_recognizer)

    def analyze_text(self, text: str) -> list[dict]:
        # 5. Threshold Filter (Score > 0.4)
        results = self.analyzer.analyze(
            text=text, 
            language='en',
            score_threshold=0.4
        )
        
        output = []
        for res in results:
            # Filter out blacklisted entities explicitly if they slipped through
            # (Though deny_list recognizer usually handles scoring logic, 
            # sometimes we want to be sure to ignore our Custom DENY entity)
            if res.entity_type == "DENY_LIST":
                continue

            # Skip common False Positives for DATE_TIME that look like coordinates
            # -106.8456 (Coordinate) vs Date
            extracted_text = text[res.start:res.end]
            
            # Simple heuristic: If it's DATE_TIME but looks like a float coordinate, skip
            if res.entity_type == "DATE_TIME":
                if re.match(r"^-?\d{1,3}\.\d+$", extracted_text):
                    continue

            output.append({
                "type": res.entity_type,
                "start": res.start,
                "end": res.end,
                "score": res.score,
                "text": extracted_text 
            })
        return output

# Singleton instance
scanner_engine = CustomPIIScanner()
