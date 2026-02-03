from presidio_analyzer import AnalyzerEngine, PatternRecognizer, RecognizerResult
from presidio_analyzer.nlp_engine import NlpEngineProvider
import re

class CustomPIIScanner:
    def __init__(self):
        # Initialize Presidio Analyzer
        # We can configure NLP engine here if we need better Indonesian support
        self.analyzer = AnalyzerEngine()
        self._add_custom_recognizers()

    def _add_custom_recognizers(self):
        # Indonesian NIK (Nomor Induk Kependudukan)
        # 16 Digits. Simple regex for now.
        nik_recognizer = PatternRecognizer(
            supported_entity="ID_NIK",
            name="id_nik_recognizer",
            regex=r"\b\d{16}\b",
            score=0.5,
        )
        self.analyzer.registry.add_recognizer(nik_recognizer)

        # Indonesian NPWP (xx.xxx.xxx.x-xxx.xxx)
        npwp_recognizer = PatternRecognizer(
            supported_entity="ID_NPWP",
            name="id_npwp_recognizer",
            regex=r"\b\d{2}\.\d{3}\.\d{3}\.\d{1}-\d{3}\.\d{3}\b",
            score=0.6,
        )
        self.analyzer.registry.add_recognizer(npwp_recognizer)
        
        # Indonesian Phone Number (+62 or 08)
        phone_recognizer = PatternRecognizer(
             supported_entity="ID_PHONE_ID",
             name="id_phone_recognizer",
             regex=r"(\+62|62|0)8[1-9][0-9]{6,10}",
             score=0.5
        )
        self.analyzer.registry.add_recognizer(phone_recognizer)

    def analyze_text(self, text: str) -> list[dict]:
        results = self.analyzer.analyze(text=text, language='en') # using 'en' model as base for regex
        output = []
        for res in results:
            output.append({
                "type": res.entity_type,
                "start": res.start,
                "end": res.end,
                "score": res.score
            })
        return output

# Singleton instance
scanner_engine = CustomPIIScanner()
