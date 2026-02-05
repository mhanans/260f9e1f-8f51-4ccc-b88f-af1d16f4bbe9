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
        
        # Dynamic Smart Filter Sets (Empty by default)
        self.common_id_false_positives = set()
        self.person_negative_contexts = set()
        self.person_invalid_particles = set()
        
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

                # Initialize containers
                deny_rules = []
                regex_rules = []
                exclude_rules = []
                
                # Dynamic Smart Filter Sets
                self.common_id_false_positives = set()
                self.person_negative_contexts = set()
                self.person_invalid_particles = set() # Renamed to standard snake_case

                for r in rules:
                    if r.rule_type == "deny_list":
                        deny_rules.append(r.pattern)
                    elif r.rule_type == "regex":
                        regex_rules.append(r)
                    elif r.rule_type == "exclude_entity":
                        exclude_rules.append(r.pattern)
                    
                    # New Types
                    elif r.rule_type == "false_positive_person":
                        self.common_id_false_positives.add(r.pattern.lower())
                    elif r.rule_type == "negative_context_person":
                        self.person_negative_contexts.add(r.pattern.lower())
                    elif r.rule_type == "invalid_particle_person":
                        self.person_invalid_particles.add(r.pattern.lower())
                    
                
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
                print(f"DEBUG: Loading {len(regex_rules)} regex rules from DB...")
                
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

                    # Enforce ID_ prefix for known types (Legacy DB Fix)
                    e_type = legacy_map.get(c.entity_type, c.entity_type)

                    pat = Pattern(name=f"{c.name}_pattern", regex=c.pattern, score=c.score)
                    rec = PatternRecognizer(
                        supported_entity=e_type, 
                        name=c.name,
                        patterns=[pat],
                        context=context_list,
                        supported_language=None # Apply to ALL languages (EN, ID, etc.)
                    )
                    self.analyzer.registry.add_recognizer(rec)
                
                print(f"DEBUG: Successfully registered {len(regex_rules)} custom recognizers.")
                    
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
        # Note: We rely on self.common_id_false_positives, self.person_negative_contexts, etc loaded from DB.
        
        # Hardcoded Positive Contexts (Safe to keep in code as they represent core language logic, rarely changed)
        PERSON_POSITIVE_CONTEXTS = {
            "nama", "name", "customer", "karyawan", "pegawai", "staff",
            "pic", "penanggung", "jawab", "oleh", "by",
            "bapak", "ibu", "bpk", "sdr", "sdri", "mr", "mrs", "ms", "miss"
        }

        # 5. Threshold Filter (Score > 0.4)
        # Note: We use 'en' as the pipeline language to leverage the English NLP model for context/NER.
        # Custom Indonesian recognizers are registered with supported_language=None, so they run alongside English rules.
        raw_results = self.analyzer.analyze(
            text=text, 
            language='en',
            score_threshold=0.4,
            context=context
        )

        # --- DEDUPLICATION & PRIORITIZATION ---
        # 1. Sort by Start position, then Score (desc), then prefer ID_ prefix
        # This puts the "Best" candidate for a span first.
        def priority_key(res):
            is_custom = 1 if res.entity_type.startswith("ID_") else 0
            return (res.start, -is_custom, -res.score)
        
        raw_results.sort(key=priority_key)

        results = []
        if raw_results:
            # Greedy overlap removal
            # Since we sorted by Start -> Custom -> Score, the first one we see is the "best" for that start pos.
            # We just need to make sure we don't add something that heavily overlaps a previously added chosen entity.
            
            # Note: Presidio usually handles non-overlapping, but since we mix languages/recognizers, overlaps happen.
            
            last_end = -1
            for res in raw_results:
                # If this result starts before the previous one ended, it's an overlap.
                # However, strict strictly > might kill nested entities. 
                # For PII scanning, we usually want the longest/best match.
                # Let's check overlap ratio.
                if res.start < last_end:
                    # Overlap detected. 
                    # Because of our sort order, the previous one was "better" (or started earlier).
                    # We skip this one (the "duplicate" or "inferior" one).
                    continue
                
                results.append(res)
                last_end = res.end

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
                # Use DB-loaded set
                extracted_words = set(extracted_lower.split())
                if not extracted_words.isdisjoint(self.person_invalid_particles):
                    continue

                # Rule C: Common Header/False Positive Check (Startswith)
                # If the detected text STARTS with a blacklist word, reject it.
                # e.g. "Nomor Kartu Keluarga" (Starts with 'nomor') -> Reject
                # e.g. "Tempat Lahir" (Starts with 'tempat') -> Reject
                # Use DB-loaded set
                if any(extracted_lower.startswith(x) for x in self.common_id_false_positives):
                    continue

                # Rule D: Negative Context Lookbehind (e.g. "Jalan Name")
                # Look back ~35 chars
                preceding_text = text[max(0, res.start - 35) : res.start].lower()
                preceding_words = set(re.findall(r'\w+', preceding_text))
                
                # Use DB-loaded set
                if not preceding_words.isdisjoint(self.person_negative_contexts):
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

            # 2. Skip common False Positives for DATE_TIME containing file artifacts or weird text
            if res.entity_type == "DATE_TIME":
                if re.match(r"^-?\d{1,3}\.\d+$", extracted_text): continue
                if extracted_text.isdigit(): continue # Just a year or number is rarely PII alone
                if any(x in extracted_text.lower() for x in [".pdf", ".csv", "health", "storybook", "version"]): continue

            # 3. Filter PHONE_NUMBER Noise (e.g. coordinates like 0.8123 or short numeric strings)
            # We check both standard Presidio "PHONE_NUMBER" and our custom "ID_PHONE_NUMBER"
            if res.entity_type in ["PHONE_NUMBER", "ID_PHONE_NUMBER"]:
                # Reject floats (e.g. 0.8269353) which are likely confidence scores from raw text
                if re.match(r"^\d+\.\d+$", extracted_text): continue
                # Reject if too short (e.g. < 7 digits)
                digits = re.sub(r"\D", "", extracted_text)
                if len(digits) < 7: continue

            # 4. Filter US_PASSPORT false positives (random 9 digit numbers)
            if res.entity_type == "US_PASSPORT":
                # Passports are strictly 9 chars usually. 
                # If found in a float context (e.g. 0.222222222), it's noise.
                if "." in extracted_text: continue
            
            # 5. Filter NRP (Custom) false positives
            if res.entity_type == "NRP":
                 if len(extracted_text) < 4: continue
                 if extracted_text.lower() in ["astra", "islam", "hindu", "buddha", "kristen"]: continue # Religion/Company noise

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
