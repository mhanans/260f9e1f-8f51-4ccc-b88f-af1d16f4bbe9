import fitz  # PyMuPDF
import docx
import openpyxl
import io
import logging
import pytesseract
from PIL import Image

logger = logging.getLogger(__name__)

class FileScanner:
    def extract_text(self, file_content: bytes, filename: str) -> str:
        ext = filename.split('.')[-1].lower()
        
        try:
            if ext == 'pdf':
                return self._read_pdf(file_content)
            elif ext in ['docx', 'doc']:
                return self._read_docx(file_content)
            elif ext in ['xlsx', 'xls']:
                return self._read_excel(file_content)
            elif ext in ['jpg', 'jpeg', 'png']:
                 return self._read_image(file_content)
            elif ext == 'txt':
                return file_content.decode('utf-8')
            else:
                logger.warning(f"Unsupported file extension: {ext}")
                return ""
        except Exception as e:
            logger.error(f"Error reading file {filename}: {str(e)}")
            return ""

    def _read_pdf(self, content: bytes) -> str:
        text = ""
        with fitz.open(stream=content, filetype="pdf") as doc:
            for page in doc:
                page_text = page.get_text()
                # If page is empty, try OCR (files containing scanned images)
                if not page_text.strip():
                    pix = page.get_pixmap()
                    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                    page_text = pytesseract.image_to_string(img)
                text += page_text
        return text

    def _read_docx(self, content: bytes) -> str:
        doc = docx.Document(io.BytesIO(content))
        return "\n".join([para.text for para in doc.paragraphs])

    def _read_excel(self, content: bytes) -> str:
        wb = openpyxl.load_workbook(io.BytesIO(content), data_only=True)
        text = []
        for sheet in wb.sheetnames:
            ws = wb[sheet]
            for row in ws.iter_rows(values_only=True):
                text.append(" ".join([str(c) for c in row if c is not None]))
        return "\n".join(text)

    def _read_image(self, content: bytes) -> str:
        try:
            image = Image.open(io.BytesIO(content))
            return pytesseract.image_to_string(image)
        except Exception as e:
            logger.error(f"OCR Failed: {e}")
            return ""

file_scanner = FileScanner()
