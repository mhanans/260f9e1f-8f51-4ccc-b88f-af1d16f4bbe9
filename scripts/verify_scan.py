import sys
import os

# Add parent dir to path
sys.path.append(os.getcwd())

from connectors.file_scanner import file_scanner

def test_metadata_extraction():
    print("Testing extraction...")
    
    # Test Dummy PDF extraction (mocked content if real PDF not available, but let's try text)
    # file_scanner handles 'txt' via extract_text fallback to single chunk
    
    content = b"This is a test file for PII."
    chunks = file_scanner.extract_with_metadata(content, "test.txt")
    print("TXT Chunks:", chunks)
    assert len(chunks) == 1
    assert chunks[0]["metadata"]["type"] == "general"
    
    # Test Mock PDF (needs fitz, which might fail if not installed or empty bytes)
    # We just want to check if the method exists and runs without syntax error
    try:
        chunks_pdf = file_scanner.extract_with_metadata(b"%PDF-1.4...", "doc.pdf")
        print("PDF Chunks (Empty/Error expected):", chunks_pdf)
    except Exception as e:
        print(f"PDF Test Exception: {e}")

if __name__ == "__main__":
    test_metadata_extraction()
