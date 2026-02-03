from PIL import Image
import pytesseract
from pdf2image import convert_from_bytes
import io
import docx

class OCREngine:
    def extract_text_from_image(self, image_bytes: bytes) -> str:
        try:
            image = Image.open(io.BytesIO(image_bytes))
            return pytesseract.image_to_string(image)
        except Exception:
            return ""

    def extract_text_from_pdf(self, pdf_bytes: bytes) -> str:
        try:
            images = convert_from_bytes(pdf_bytes)
            full_text = ""
            for img in images:
                full_text += pytesseract.image_to_string(img) + "\n"
            return full_text
        except Exception:
            return ""

    def extract_text_from_docx(self, docx_bytes: bytes) -> str:
        try:
            doc = docx.Document(io.BytesIO(docx_bytes))
            full_text = "\n".join([p.text for p in doc.paragraphs])
            
            # Extract images from DOCX
            for rel in doc.part.rels.values():
                if "image" in rel.target_ref:
                    img_bytes = rel.target_part.blob
                    full_text += "\n[Image Content]: " + self.extract_text_from_image(img_bytes)
            
            return full_text
        except Exception:
            return ""

ocr_engine = OCREngine()
