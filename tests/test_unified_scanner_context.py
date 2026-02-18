from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from engine.unified_scanner import UnifiedScanner
import engine.unified_scanner as us_mod


def test_scan_database_row_uses_table_and_column_context(monkeypatch):
    scanner = UnifiedScanner()
    captured = {}

    def fake_analyze_text(text, context=None):
        captured["context"] = context
        return []

    monkeypatch.setattr(us_mod.scanner_engine, "analyze_text", fake_analyze_text)

    scanner.scan_database_row(
        row_data={"customer_phone": "081234567890"},
        table_name="customer_master",
        db_name="corebanking",
    )

    assert "customer" in captured["context"]
    assert "master" in captured["context"]
    assert "phone" in captured["context"]
