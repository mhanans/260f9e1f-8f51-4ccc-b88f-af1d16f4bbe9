import math
from difflib import SequenceMatcher

class AnalyticsEngine:
    def check_encryption(self, text: str) -> bool:
        """
        Determines if text might be encrypted using Shannon Entropy.
        High entropy (> 4.5 for short strings, typically > 3.5-4.0ish) usually indicates randomness/encryption.
        Real language usually has entropy between 2.5 and 3.5.
        """
        if not text:
            return False
            
        entropy = self._shannon_entropy(text)
        # Threshold implies high randomness (encrypted or compressed)
        # Plain hex/base64 of encrypted data usually looks very random
        return entropy > 4.5

    def check_security_posture(self, column_name: str, sample_data: str) -> str:
        """
        Example of Security Posture Check.
        Alerts if a sensitive-sounding column has low entropy (plain text).
        """
        sensitive_keywords = ["password", "token", "secret", "cvv", "credit_card", "pin"]
        name_lower = column_name.lower()
        
        is_sensitive_col = any(k in name_lower for k in sensitive_keywords)
        is_encrypted = self.check_encryption(sample_data)
        
        if is_sensitive_col and not is_encrypted:
            return "CRITICAL: Sensitive Column in Plain Text"
        elif is_sensitive_col and is_encrypted:
            return "SECURE: Sensitive Column Encrypted"
        return "SAFE"

    def calculate_similarity(self, text1: str, text2: str) -> float:
        return SequenceMatcher(None, text1, text2).ratio()

    def _shannon_entropy(self, data: str) -> float:
        """Calculates Shannon entropy of string."""
        if not data:
            return 0
        entropy = 0
        for x in range(256):
            p_x = float(data.count(chr(x))) / len(data)
            if p_x > 0:
                entropy += - p_x * math.log(p_x, 2)
        return entropy

analytics_engine = AnalyticsEngine()
