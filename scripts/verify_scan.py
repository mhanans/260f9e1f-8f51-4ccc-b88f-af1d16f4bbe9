import sys
import os
import json
from pathlib import Path

# Add parent dir to path
sys.path.append(os.getcwd())

from engine.unified_scanner import UnifiedScanner

def test_unified_scan():
    print("Testing Unified Context-Aware Scanner...")
    
    scanner = UnifiedScanner()
    
    # Create valid dummy file
    filename = "daftar_cif_nasabah_penting.txt"  # Context: 'cif', 'nasabah'
    content = """
    List Data:
    1234567890
    08123456789
    """
    
    with open(filename, "w") as f:
        f.write(content)
        
    try:
        scanner.scan_file(filename)
        results = scanner.get_results()
        
        print("\nScan Results:")
        print(json.dumps(results, indent=2))
        
        # Assertions
        assert "ID_CIF" in results, "Failed to identify CIF from context!"
        assert "ID_PHONE_IDN" in results, "Failed to identify Phone from context!"
        
        cif_loc = results["ID_CIF"]["locations"][0]
        assert cif_loc["file_name"] == filename, "Location capture failed"
        assert cif_loc["source"] == "file"
        
        print("\nSUCCESS: Context aware scanning and object grouping works.")
        
    finally:
        if os.path.exists(filename):
            os.remove(filename)

if __name__ == "__main__":
    test_unified_scan()
