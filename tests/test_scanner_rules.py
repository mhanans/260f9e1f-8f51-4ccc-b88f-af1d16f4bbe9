from pathlib import Path
from types import SimpleNamespace
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from engine.scanner import CustomPIIScanner


class _FakeRegistry:
    def __init__(self):
        self.recognizers = []

    def add_recognizer(self, rec):
        self.recognizers.append(rec)


class _FakeAnalyzer:
    def __init__(self):
        self.registry = _FakeRegistry()



def _build_scanner():
    s = CustomPIIScanner.__new__(CustomPIIScanner)
    s.analyzer = _FakeAnalyzer()
    s.deny_words = []
    s.exclude_entities = []
    s.dynamic_recognizer_names = set()
    s.common_id_false_positives = set()
    s.person_negative_contexts = set()
    s.person_invalid_particles = set()
    return s


def test_parse_context_keywords_accepts_json_and_csv():
    scanner = _build_scanner()

    assert scanner._parse_context_keywords('["nama", "nik"]') == ["nama", "nik"]
    assert scanner._parse_context_keywords("name,customer_id") == ["name", "customer_id"]


def test_reload_rules_clears_stale_dynamic_recognizers_and_uses_entity_exclude():
    scanner = _build_scanner()

    stale = SimpleNamespace(name="stale_rule")
    scanner.analyzer.registry.recognizers = [stale]
    scanner.dynamic_recognizer_names = {"stale_rule"}

    rules = [
        SimpleNamespace(
            name="nik_rule",
            rule_type="regex",
            pattern=r"\\b\\d{16}\\b",
            score=0.8,
            entity_type="ID_NIK",
            context_keywords='["nik", "ktp"]',
        ),
        SimpleNamespace(
            name="exclude_person",
            rule_type="exclude_entity",
            pattern="IGNORE",
            score=1.0,
            entity_type="PERSON",
            context_keywords=None,
        ),
        SimpleNamespace(
            name="deny_header",
            rule_type="deny_list",
            pattern="nama",
            score=1.0,
            entity_type="DENY",
            context_keywords=None,
        ),
    ]

    scanner._fetch_active_rules = lambda: rules

    scanner.reload_rules()

    names = [r.name for r in scanner.analyzer.registry.recognizers]
    assert "stale_rule" not in names
    assert "nik_rule" in names
    assert "indonesian_header_deny" in names
    assert scanner.exclude_entities == ["PERSON"]


def test_reload_rules_applies_runtime_scan_config_rules():
    scanner = _build_scanner()
    scanner.score_threshold = 0.4
    scanner.analysis_language = "en"

    rules = [
        SimpleNamespace(
            name="scan_score_threshold",
            rule_type="scan_config",
            pattern="0.75",
            score=1.0,
            entity_type="SCAN_CONFIG",
            context_keywords=None,
        ),
        SimpleNamespace(
            name="scan_language",
            rule_type="scan_config",
            pattern="id",
            score=1.0,
            entity_type="SCAN_CONFIG",
            context_keywords=None,
        ),
    ]

    scanner._fetch_active_rules = lambda: rules

    scanner.reload_rules()

    assert scanner.score_threshold == 0.75
    assert scanner.analysis_language == "id"
