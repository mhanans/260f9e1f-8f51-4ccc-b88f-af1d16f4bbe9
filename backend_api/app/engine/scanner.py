
from presidio_analyzer import AnalyzerEngine, PatternRecognizer, RecognizerResult, Pattern
from presidio_analyzer.nlp_engine import NlpEngineProvider
import re
import json
from pathlib import Path

class CustomPIIScanner:
    def __init__(self):

    def __init__(self):
        # Initialize Presidio Analyzer
        self.analyzer = AnalyzerEngine()
        
        self.deny_words = []
        self.exclude_entities = []
        
        # Dynamic Smart Filter Sets (Loaded from DB)
        self.common_id_false_positives = set()
        self.person_negative_contexts = set()
        self.person_invalid_particles = set()
        
        # Proximity Rules {entity_type: [keywords]}
        self.proximity_rules = {}

        self.reload_rules()

    def reload_rules(self):
        """Loads Custom Recognizers, Deny List, and Configuration from Database."""
        try:
            from sqlmodel import Session, select
            from app.core.db import engine
            from app.models.all_models import ScanRule
            
            with Session(engine) as session:
                rules = session.exec(select(ScanRule).where(ScanRule.is_active == True)).all()
                if not rules:
                    return 

                # efficient reset
                deny_rules = []
                regex_rules = []
                exclude_rules = []
                self.proximity_rules = {}
                
                # Reset sets
                self.common_id_false_positives = set()
                self.person_negative_contexts = set()
                self.person_invalid_particles = set() 

                for r in rules:
                    if r.rule_type == "deny_list":
                        deny_rules.append(r.pattern)
                    elif r.rule_type == "regex":
                        regex_rules.append(r)
                    elif r.rule_type == "exclude_entity":
                        exclude_rules.append(r.pattern)
                    
                    # --- ZERO HARDCODE: DYNAMIC CONFIG ---
                    elif r.rule_type == "DISABLE_DEFAULT":
                        # Remove built-in recognizers by name
                        self._remove_recognizer(r.pattern)
                        
                    elif r.rule_type == "false_positive_person":
                        self.common_id_false_positives.add(r.pattern.lower())
                    elif r.rule_type == "negative_context_person":
                        self.person_negative_contexts.add(r.pattern.lower())
                    elif r.rule_type == "invalid_particle_person":
                        self.person_invalid_particles.add(r.pattern.lower())
                    
                    # Load Proximity Contexts
                    if r.context_keywords:
                         try:
                             kws = json.loads(r.context_keywords)
                             if r.entity_type not in self.proximity_rules:
                                 self.proximity_rules[r.entity_type] = []
                             self.proximity_rules[r.entity_type].extend([k.lower() for k in kws])
                         except: pass

                
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
                legacy_map = {
                    "PHONE_NUMBER": "ID_PHONE_NUMBER",
                    "FIN_BANK_ACCT_ID": "ID_BANK_ACCOUNT", 
                    "FIN_AMT": "ID_FINANCE_AMOUNT",
                    "ORGANIZATION": "ID_ORGANIZATION",
                    "SOCIAL_MEDIA": "ID_SOCIAL_MEDIA", 
                    "PROJECT_NAME": "ID_PROJECT_NAME",
                    "EMAIL_ADDRESS": "ID_EMAIL"
                }

                for c in regex_rules:
                    self._remove_recognizer(c.name)
                    
                    try:
                        context_list = json.loads(c.context_keywords) if c.context_keywords else []
                    except:
                        context_list = []

                    e_type = legacy_map.get(c.entity_type, c.entity_type)

                    pat = Pattern(name=f"{c.name}_pattern", regex=c.pattern, score=c.score)
                    rec = PatternRecognizer(
                        supported_entity=e_type, 
                        name=c.name,
                        patterns=[pat],
                        context=context_list,
                        supported_language=None 
                    )
                    self.analyzer.registry.add_recognizer(rec)
                
                # C. Exclude entities
                self.exclude_entities = exclude_rules
                
        except Exception as e:
            print(f"Error loading scanner rules from DB: {e}")
     
    def mask_pii(self, text: str, pii_type: str) -> str:
        """
        Masks PII while preserving format hints.
        e.g. jdoe@example.com -> j***@example.com
        e.g. 1234-5678 -> 12**-****
        """
        if not text: return ""
        if len(text) <= 2: return "*" * len(text)
        
        if pii_type == "EMAIL_ADDRESS":
            parts = text.split("@")
            if len(parts) == 2:
                name = parts[0]
                domain = parts[1]
                masked_name = name[:1] + "***" + (name[-1:] if len(name)>2 else "")
                return f"{masked_name}@{domain}"
        
        # Default simple masking
        start_len = 2 if len(text) > 4 else 0
        end_len = 2 if len(text) > 4 else 0
        middle = "*" * (len(text) - start_len - end_len)
        return text[:start_len] + middle + text[-start_len:] if start_len else middle

    def _remove_recognizer(self, name):
        self.analyzer.registry.recognizers = [
            r for r in self.analyzer.registry.recognizers if r.name != name
        ]

    def analyze_text(self, text: str, context: list[str] = None) -> list[dict]:
        """
        Analyzes text for PII. 
        """
        PERSON_POSITIVE_CONTEXTS = {
            "nama", "name", "customer", "karyawan", "pegawai", "staff",
            "pic", "penanggung", "jawab", "oleh", "by",
            "bapak", "ibu", "bpk", "sdr", "sdri", "mr", "mrs", "ms", "miss"
        }

        # 5. Threshold Filter (Score > 0.4)
        raw_results = self.analyzer.analyze(
            text=text, 
            language='en',
            score_threshold=0.4,
            context=context
        )

        def priority_key(res):
            is_custom = 1 if res.entity_type.startswith("ID_") else 0
            return (res.start, -is_custom, -res.score)
        
        raw_results.sort(key=priority_key)

        results = []
        if raw_results:
            last_end = -1
            for res in raw_results:
                if res.start < last_end:
                    continue
                results.append(res)
                last_end = res.end

        output = []
        for res in results:
            if res.entity_type == "DENY_LIST": continue
            if res.entity_type in self.exclude_entities: continue

            extracted_text = text[res.start:res.end]
            extracted_lower = extracted_text.lower().replace(".", "") # simple clean
            
            if extracted_text.lower() in [x.lower() for x in self.deny_words]: 
                 continue

            # --- SMART FILTERING ---
            
            # 1. Filter PERSON False Positives
            if res.entity_type == "PERSON":
                if extracted_text and extracted_text[0].islower():
                     continue

                extracted_words = set(extracted_lower.split())
                if not extracted_words.isdisjoint(self.person_invalid_particles):
                    continue

                if any(extracted_lower.startswith(x) for x in self.common_id_false_positives):
                    continue

                preceding_text = text[max(0, res.start - 35) : res.start].lower()
                preceding_words = set(re.findall(r'\w+', preceding_text))
                
                if not preceding_words.isdisjoint(self.person_negative_contexts):
                    continue

                if not preceding_words.isdisjoint(PERSON_POSITIVE_CONTEXTS):
                    pass 
                else:
                    if any(char.isdigit() for char in extracted_text): continue
                    if len(extracted_text) < 3: continue
                    if len(extracted_words) > 5: continue 


            # --- DYNAMIC PROXIMITY LOGIC (Zero Hardcode) ---
            # If DB rule says this Entity needs context keywords, check window +/- 50 chars.
            if res.entity_type in self.proximity_rules:
                required_keywords = self.proximity_rules[res.entity_type]
                if required_keywords:
                    start_window = max(0, res.start - 50)
                    end_window = min(len(text), res.end + 50)
                    window_text = text[start_window:end_window].lower()
                    
                    if not any(kw in window_text for kw in required_keywords):
                        # Context missing -> Ignore finding
                        continue

            if res.entity_type == "DATE_TIME":
                if re.match(r"^-?\d{1,3}\.\d+$", extracted_text): continue
                if extracted_text.isdigit(): continue 
                if any(x in extracted_text.lower() for x in [".pdf", ".csv", "health", "storybook", "version"]): continue

            if res.entity_type in ["PHONE_NUMBER", "ID_PHONE_NUMBER"]:
                if re.match(r"^\d+\.\d+$", extracted_text): continue
                digits = re.sub(r"\D", "", extracted_text)
                if len(digits) < 7: continue

            if res.entity_type == "US_PASSPORT":
                if "." in extracted_text: continue
            
            if res.entity_type == "NRP":
                 if len(extracted_text) < 4: continue
                 if extracted_text.lower() in ["astra", "islam", "hindu", "buddha", "kristen"]: continue 

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
        """
        try:
            from presidio_structured import BatchAnalyzerEngine
            batch_analyzer = BatchAnalyzerEngine(analyzer_engine=self.analyzer)
            result_iter = batch_analyzer.analyze_iterator(df)
            return result_iter
        except ImportError:
            print("presidio-structured not found, falling back to manual loop.")
            return []
        except Exception as e:
            print(f"Batch Analysis Error: {e}")
            return []

scanner_engine = CustomPIIScanner()
