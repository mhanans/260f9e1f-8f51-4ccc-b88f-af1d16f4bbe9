
from PIL import Image, ImageOps, ImageEnhance
import pytesseract
from pdf2image import convert_from_bytes
import io
import docx
import structlog

logger = structlog.get_logger()

class OCREngine:
    def preprocess_image(self, image):
        """Meningkatkan kualitas gambar untuk OCR yang lebih baik"""
        # Ubah ke Grayscale
        image = ImageOps.grayscale(image)
        # Tingkatkan kontras
        enhancer = ImageEnhance.Contrast(image)
        image = enhancer.enhance(2.0)
        return image

    def extract_text_from_image(self, image_bytes: bytes, filename: str = "image") -> str:
        try:
            image = Image.open(io.BytesIO(image_bytes))
            image = self.preprocess_image(image)
            
            # Gunakan bahasa Indonesia (ind) dan Inggris (eng)
            # Pastikan tesseract-ocr-ind sudah terinstall di sistem
            text = pytesseract.image_to_string(image, lang='ind+eng')
            
            logger.info("OCR_SUCCESS", file=filename, length=len(text), preview=text[:50].replace('\n', ' '))
            return text
        except Exception as e:
            logger.error("OCR_ERROR_IMAGE", file=filename, error=str(e))
            return ""

    def extract_text_from_pdf(self, pdf_bytes: bytes, filename: str = "doc.pdf") -> str:
        try:
            # Mengonversi PDF ke gambar dengan DPI tinggi (300) untuk akurasi yang lebih baik
            images = convert_from_bytes(pdf_bytes, dpi=300)
            full_text = ""
            
            for i, img in enumerate(images):
                img = self.preprocess_image(img)
                page_text = pytesseract.image_to_string(img, lang='ind+eng')
                full_text += f"--- Page {i+1} ---\n{page_text}\n"
                logger.info("OCR_PAGE_SUCCESS", file=filename, page=i+1, length=len(page_text))
            
            return full_text
        except Exception as e:
            logger.error("OCR_ERROR_PDF", file=filename, error=str(e))
            return ""

    def extract_text_from_docx(self, docx_bytes: bytes, filename: str = "doc.docx") -> str:
        try:
            doc = docx.Document(io.BytesIO(docx_bytes))
            full_text = "\n".join([p.text for p in doc.paragraphs])
            
            image_count = 0
            # Extract images from DOCX
            for rel in doc.part.rels.values():
                if "image" in rel.target_ref:
                    image_count += 1
                    try:
                        img_bytes = rel.target_part.blob
                        # Reuse the optimized image extraction method
                        img_text = self.extract_text_from_image(img_bytes, f"{filename}_img_{image_count}")
                        full_text += f"\n[Embedded Image {image_count}]:\n{img_text}"
                    except Exception as inner:
                        logger.error("OCR_ERROR_DOCX_IMG", file=filename, img_id=image_count, error=str(inner))
            
            logger.info("OCR_DOCX_SUCCESS", file=filename, images_found=image_count, total_length=len(full_text))
            return full_text
        except Exception as e:
            logger.error("OCR_ERROR_DOCX", file=filename, error=str(e))
            return ""

ocr_engine = OCREngine()
