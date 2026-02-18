from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import connectors.file_scanner as fs_mod


def test_image_file_uses_ocr(monkeypatch):
    calls = {}

    def fake_ocr(image_bytes, filename="image"):
        calls["filename"] = filename
        return "NIK 3201010101010101"

    monkeypatch.setattr(fs_mod.ocr_engine, "extract_text_from_image", fake_ocr)

    chunks = fs_mod.file_scanner.extract_with_metadata(b"fakeimg", "ktp_photo.png")

    assert len(chunks) == 1
    assert "3201010101010101" in chunks[0]["text"]
    assert chunks[0]["metadata"]["type"] == "image"
    assert calls["filename"] == "ktp_photo.png"


def test_pdf_without_text_falls_back_to_full_page_ocr(monkeypatch):
    class FakePixmap:
        def tobytes(self, fmt):
            assert fmt == "png"
            return b"pagepng"

    class FakePage:
        def get_text(self):
            return "   "

        def get_images(self, full=True):
            return []

        def get_pixmap(self):
            return FakePixmap()

    class FakeDoc:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def __iter__(self):
            return iter([FakePage()])

    monkeypatch.setattr(fs_mod, "fitz", type("F", (), {"open": lambda **kwargs: FakeDoc()}))
    monkeypatch.setattr(
        fs_mod.ocr_engine,
        "extract_text_from_image",
        lambda image_bytes, filename="image": "Nomor HP 081234567890",
    )

    chunks = fs_mod.file_scanner.extract_with_metadata(b"fakepdf", "scan.pdf")

    assert len(chunks) == 1
    assert "081234567890" in chunks[0]["text"]
    assert chunks[0]["metadata"]["ocr"] is True
    assert chunks[0]["metadata"]["source"] == "full_page"
