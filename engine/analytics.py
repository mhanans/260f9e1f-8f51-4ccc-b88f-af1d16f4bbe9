from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import logging

logger = logging.getLogger(__name__)

class ContentAnalytics:
    def __init__(self):
        self.vectorizer = TfidfVectorizer(stop_words='english')

    def calculate_similarity(self, text1: str, text2: str) -> float:
        """
        Calculate Cosine Similarity between two text blobs.
        Returns a float between 0.0 and 1.0.
        """
        if not text1 or not text2:
            return 0.0
        
        try:
            tfidf_matrix = self.vectorizer.fit_transform([text1, text2])
            similarity = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])
            return float(similarity[0][0])
        except Exception as e:
            logger.error(f"Error calculating similarity: {e}")
            return 0.0

    def check_encryption(self, text: str) -> bool:
        """
        Basic heuristic to check if text seems encrypted (high entropy/randomness).
        """
        # A simple placeholder. Real implementation would use Great Expectations or Entropy calculation.
        # Encrypted text usually has no spaces and high character variety.
        if len(text) > 50 and " " not in text[:50]:
            return True
        return False

analytics_engine = ContentAnalytics()
