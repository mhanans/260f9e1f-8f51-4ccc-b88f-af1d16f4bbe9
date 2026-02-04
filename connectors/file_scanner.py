import io
import logging
from pathlib import Path
from docx import Document # python-docx

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

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
        if fitz is None:
            return "[Error: PyMuPDF not installed]"
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

    def extract_with_metadata(self, content: bytes, filename: str) -> list[dict]:
        """
        Extracts text with location metadata.
        Returns: [{"text": "...", "metadata": {"page": 1, ...}}]
        """
        file_ext = filename.lower().split('.')[-1]
        try:
            if file_ext == 'pdf':
                return self._extract_pdf_with_meta(content)
            elif file_ext in ['xlsx', 'xls']:
                # Note: openpyxl is needed for excel
                return self._extract_excel_with_meta(content)
            elif file_ext == 'docx':
                 # Docx pages are hard, but we can return paragraphs
                return [{"text": self._extract_from_docx(content), "metadata": {"type": "docx"}}] # fallback
            else:
                 # Fallback for others
                 text = self.extract_text(content, filename)
                 return [{"text": text, "metadata": {"type": "general"}}]
        except Exception as e:
            logger.error(f"Error extracting metadata from {filename}: {e}")
            return []

    def _extract_pdf_with_meta(self, content: bytes) -> list[dict]:
        chunks = []
        if fitz is None:
            logger.warning("PyMuPDF (fitz) not installed. Skipping PDF metadata extraction.")
            return [{"text": "[PDF Support Missing]", "metadata": {"error": "missing_dependency"}}]
            
        try:
            with fitz.open(stream=content, filetype="pdf") as doc:
                for page_num, page in enumerate(doc):
                    text = page.get_text()
                    if text.strip():
                        chunks.append({
                            "text": text,
                            "metadata": {"page": page_num + 1}
                        })
        except Exception as e:
            logger.error(f"PDF Meta Extraction Error: {e}")
        return chunks

    def _extract_excel_with_meta(self, content: bytes) -> list[dict]:
        chunks = []
        try:
            import openpyxl
            wb = openpyxl.load_workbook(filename=io.BytesIO(content), data_only=True)
            for sheet in wb.sheetnames:
                ws = wb[sheet]
                for row in ws.iter_rows(values_only=True):
                    # Rough row text
                    row_text = " ".join([str(c) for c in row if c is not None])
                    if row_text.strip():
                         # We treat row as a chunk for location precision
                         # If we want cell precision, we need to iterate cells. 
                         # For now, Row granularity is good.
                         # Need row index? iter_rows doesn't give it easily in values_only
                         pass
                
                # Re-iterate with index
                for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
                     text_parts = []
                     for j, cell in enumerate(row, start=1):
                         if cell:
                             text_parts.append(str(cell))
                     
                     if text_parts:
                         chunks.append({
                             "text": " ".join(text_parts),
                             "metadata": {"sheet": sheet, "row": i}
                         })
        except ImportError:
            logger.error("openpyxl not installed")
            return [{"text": "Excel support missing openpyxl", "metadata": {}}]
        except Exception as e:
            logger.error(f"Excel Meta Extraction Error: {e}")
        return chunks

file_scanner = FileScanner()
