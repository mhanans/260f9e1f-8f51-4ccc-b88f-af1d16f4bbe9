from presidio_analyzer import AnalyzerEngine, PatternRecognizer, RecognizerResult, Pattern
from presidio_analyzer.nlp_engine import NlpEngineProvider
import re
import json
from pathlib import Path

CONFIG_PATH = Path("config/scanner_rules.json")

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
        
        # Load Dynamic Rules
        self.reload_rules()

    def reload_rules(self):
        """Loads Custom Recognizers and Deny List from JSON."""
        if not CONFIG_PATH.exists(): return

        try:
            with open(CONFIG_PATH, "r") as f:
                data = json.load(f)
                
                # A. Load Deny List
                self.deny_words = data.get("deny_list", [])
                if self.deny_words:
                    # Remove old deny recognizer if exists
                    self._remove_recognizer("indonesian_header_deny")
                    
                    deny_recognizer = PatternRecognizer(
                        supported_entity="DENY_LIST",
                        name="indonesian_header_deny",
                        deny_list=self.deny_words
                    )
                    self.analyzer.registry.add_recognizer(deny_recognizer)
                
                # B. Load Custom Regex Recognizers
                customs = data.get("custom_recognizers", [])
                for c in customs:
                    if not c.get("active", True):
                        self._remove_recognizer(c["name"])
                        continue

                    # Add or Update
                    self._remove_recognizer(c["name"]) # Remove first to avoid dupe
                    
                    pat = Pattern(name=f"{c['name']}_pattern", regex=c["regex"], score=c["score"])
                    rec = PatternRecognizer(
                        supported_entity=c["entity"],
                        name=c["name"],
                        patterns=[pat],
                        context=c.get("context", [])
                    )
                    self.analyzer.registry.add_recognizer(rec)
                
                # C. Exclude Entities list
                self.exclude_entities = data.get("exclude_entities", [])

        except Exception as e:
            print(f"Error loading scanner rules: {e}")

    def _remove_recognizer(self, name):
        self.analyzer.registry.recognizers = [
            r for r in self.analyzer.registry.recognizers if r.name != name
        ]

    def analyze_text(self, text: str) -> list[dict]:
        # 5. Threshold Filter (Score > 0.4)
        results = self.analyzer.analyze(
            text=text, 
            language='en',
            score_threshold=0.4
        )
        
        output = []
        for res in results:
            if res.entity_type == "DENY_LIST": continue
            
            # Helper to check excludes
            if res.entity_type in self.exclude_entities: continue

            extracted_text = text[res.start:res.end]
            
            # Double check deny list for strictness
            if extracted_text.lower() in [x.lower() for x in self.deny_words]: 
                 continue

            # Skip common False Positives for DATE_TIME that look like coordinates
            if res.entity_type == "DATE_TIME":
                if re.match(r"^-?\d{1,3}\.\d+$", extracted_text): continue

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
