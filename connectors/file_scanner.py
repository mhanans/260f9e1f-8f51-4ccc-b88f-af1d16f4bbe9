import io
import logging
import fitz  # PyMuPDF
from pathlib import Path
from docx import Document # python-docx

logger = logging.getLogger(__name__)

class FileScanner:
    def extract_text(self, content: bytes, filename: str) -> str:
        """
        Extracts text from various file formats.
        Currently supports: .txt, .pdf, .csv, .docx
        """
        file_ext = filename.lower().split('.')[-1]
        
        try:
            if file_ext == 'txt':
                return content.decode('utf-8', errors='ignore')
            
            elif file_ext == 'csv':
                return content.decode('utf-8', errors='ignore')
            
            elif file_ext == 'pdf':
                return self._extract_from_pdf(content)
            
            elif file_ext in ['docx']:
                return self._extract_from_docx(content)
                
            else:
                return f"Unsupported file type: {file_ext}"
                
        except Exception as e:
            logger.error(f"Error extracting text from {filename}: {e}")
            return ""

    def _extract_from_pdf(self, content: bytes) -> str:
        text = ""
        try:
            with fitz.open(stream=content, filetype="pdf") as doc:
                for page in doc:
                    text += page.get_text()
        except Exception as e:
            logger.error(f"PDF Extraction Error: {e}")
            return "[Error reading PDF content]"
        return text

    def _extract_from_docx(self, content: bytes) -> str:
        try:
            # python-docx requires a file-like object
            doc = Document(io.BytesIO(content))
            return "\n".join([para.text for para in doc.paragraphs])
        except Exception as e:
            logger.error(f"DOCX Extraction Error: {e}")
            return "[Error reading DOCX content]"

file_scanner = FileScanner()
