from presidio_analyzer import AnalyzerEngine, PatternRecognizer, RecognizerResult, Pattern
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
        nik_pattern = Pattern(
            name="nik_pattern",
            regex=r"\b\d{16}\b",
            score=0.5
        )
        nik_recognizer = PatternRecognizer(
            supported_entity="ID_NIK",
            name="id_nik_recognizer",
            patterns=[nik_pattern]
        )
        self.analyzer.registry.add_recognizer(nik_recognizer)

        # Indonesian NPWP (xx.xxx.xxx.x-xxx.xxx)
        npwp_pattern = Pattern(
            name="npwp_pattern",
            regex=r"\b\d{2}\.\d{3}\.\d{3}\.\d{1}-\d{3}\.\d{3}\b",
            score=0.6
        )
        npwp_recognizer = PatternRecognizer(
            supported_entity="ID_NPWP",
            name="id_npwp_recognizer",
            patterns=[npwp_pattern]
        )
        self.analyzer.registry.add_recognizer(npwp_recognizer)
        
        # Indonesian Phone Number (+62 or 08)
        phone_pattern = Pattern(
             name="phone_pattern",
             regex=r"(\+62|62|0)8[1-9][0-9]{6,10}",
             score=0.5
        )
        phone_recognizer = PatternRecognizer(
             supported_entity="ID_PHONE_ID",
             name="id_phone_recognizer",
             patterns=[phone_pattern]
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
                "score": res.score,
                "text": text[res.start:res.end] # Extract actual text
            })
        return output

# Singleton instance
scanner_engine = CustomPIIScanner()
