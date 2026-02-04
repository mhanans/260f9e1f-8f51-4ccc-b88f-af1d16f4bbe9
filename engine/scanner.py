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
        """Loads Custom Recognizers and Deny List from Database (ScanRule table)."""
        try:
            # Need to import here to avoid circular init issues if possible, 
            # or ensure api.models is ready.
            from sqlmodel import Session, select
            from api.db import engine
            from api.models import ScanRule
            
            with Session(engine) as session:
                rules = session.exec(select(ScanRule).where(ScanRule.is_active == True)).all()
                
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
                        deny_list=self.deny_words
                    )
                    self.analyzer.registry.add_recognizer(deny_recognizer)
                
                # B. Update Custom Regex
                for c in regex_rules:
                    self._remove_recognizer(c.name)
                    
                    pat = Pattern(name=f"{c.name}_pattern", regex=c.pattern, score=c.score)
                    rec = PatternRecognizer(
                        supported_entity=c.entity_type, 
                        name=c.name,
                        patterns=[pat],
                        context=json.loads(c.context_keywords) if c.context_keywords else []
                    )
                    self.analyzer.registry.add_recognizer(rec)
                    
                # C. Exclude entities
                self.exclude_entities = exclude_rules
                
        except Exception as e:
            # Fallback to JSON if DB fails (init time) or just log error
            print(f"Error loading scanner rules from DB: {e}")
            # Try loading from file as backup? 
            # Keeping original JSON load logic as backup could be wise but requested to move to managed.
            pass

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
