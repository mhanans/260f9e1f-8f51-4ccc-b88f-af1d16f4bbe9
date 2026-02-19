from pathlib import Path
from types import SimpleNamespace
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from engine.scanner import CustomPIIScanner
from engine.default_rules import DEFAULT_INDO_RULES


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
    s.custom_regex_recognizer_names = set()
    s.custom_regex_entities = set()
    s.regex_context_map = {}
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
            entity_type="ID_KTP",
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
    assert scanner.custom_regex_recognizer_names == {"nik_rule"}
    assert scanner.custom_regex_entities == {"ID_KTP"}


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


def test_analyze_text_only_returns_custom_regex_recognizers_with_name():
    scanner = _build_scanner()
    scanner.score_threshold = 0.4
    scanner.analysis_language = "en"
    scanner.custom_regex_entities = {"ID_KTP"}
    scanner.custom_regex_recognizer_names = {"nik_rule"}

    custom = SimpleNamespace(
        entity_type="ID_KTP",
        start=4,
        end=20,
        score=0.95,
        recognition_metadata={"recognizer_name": "nik_rule"},
    )
    builtin = SimpleNamespace(
        entity_type="PERSON",
        start=0,
        end=3,
        score=0.98,
        recognition_metadata={"recognizer_name": "SpacyRecognizer"},
    )

    def _analyze(**kwargs):
        assert kwargs["entities"] == ["ID_KTP"]
        return [builtin, custom]

    scanner.analyzer.analyze = _analyze

    text = "Ana 1234567890123456"
    result = scanner.analyze_text(text)

    assert result == [
        {
            "name": "nik_rule",
            "type": "ID_KTP",
            "start": 4,
            "end": 20,
            "score": 0.95,
            "text": "1234567890123456",
        }
    ]


def test_reload_rules_ignores_regex_entities_outside_allowed_list():
    scanner = _build_scanner()

    rules = [
        SimpleNamespace(
            name="org_rule",
            rule_type="regex",
            pattern=r"PT\s+[A-Z]+",
            score=0.8,
            entity_type="ID_ORGANIZATION",
            context_keywords='["company"]',
        ),
    ]

    scanner._fetch_active_rules = lambda: rules
    scanner.reload_rules()

    names = [r.name for r in scanner.analyzer.registry.recognizers]
    assert "org_rule" not in names
    assert scanner.custom_regex_recognizer_names == set()
    assert scanner.custom_regex_entities == set()


def test_default_rules_include_required_custom_regex_entities():
    required_entities = {
        "ID_KTP",
        "ID_KK",
        "ID_NPWP",
        "ID_BPJS",
        "ID_CREDIT_CARD",
        "ID_BANK_ACCOUNT",
        "ID_PHONE_NUMBER",
        "ID_EMAIL",
        "ID_SOCIAL_MEDIA",
        "ID_NAME",
    }

    regex_entities = {
        r["entity_type"]
        for r in DEFAULT_INDO_RULES
        if r.get("rule_type") == "regex"
    }

    assert required_entities.issubset(regex_entities)


def test_analyze_text_returns_empty_when_no_custom_regex_entities():
    scanner = _build_scanner()
    scanner.score_threshold = 0.4
    scanner.analysis_language = "en"

    def _analyze(**kwargs):
        raise AssertionError("analyzer should not be called without custom regex entities")

    scanner.analyzer.analyze = _analyze

    assert scanner.analyze_text("John Doe") == []


def test_analyze_text_returns_empty_when_registry_has_no_matching_recognizers():
    scanner = _build_scanner()
    scanner.score_threshold = 0.4
    scanner.analysis_language = "id"
    scanner.custom_regex_entities = {"ID_KTP"}

    def _analyze(**kwargs):
        raise ValueError("No matching recognizers were found to serve the request.")

    scanner.analyzer.analyze = _analyze

    assert scanner.analyze_text("1234567890123456") == []


def test_analyze_text_skips_match_when_required_context_missing():
    scanner = _build_scanner()
    scanner.score_threshold = 0.4
    scanner.analysis_language = "en"
    scanner.custom_regex_entities = {"ID_KTP"}
    scanner.custom_regex_recognizer_names = {"nik_rule"}
    scanner.regex_context_map = {"nik_rule": ["nik"]}

    hit = SimpleNamespace(
        entity_type="ID_KTP",
        start=0,
        end=16,
        score=0.95,
        recognition_metadata={"recognizer_name": "nik_rule"},
    )

    scanner.analyzer.analyze = lambda **kwargs: [hit]

    assert scanner.analyze_text("1234567890123456") == []


def test_analyze_text_phone_id_requires_plus62_prefix():
    scanner = _build_scanner()
    scanner.score_threshold = 0.4
    scanner.analysis_language = "en"
    scanner.custom_regex_entities = {"ID_PHONE_NUMBER"}
    scanner.custom_regex_recognizer_names = {"phone_rule"}
    scanner.regex_context_map = {"phone_rule": []}

    local_phone = SimpleNamespace(
        entity_type="ID_PHONE_NUMBER",
        start=0,
        end=12,
        score=0.9,
        recognition_metadata={"recognizer_name": "phone_rule"},
    )
    intl_phone = SimpleNamespace(
        entity_type="ID_PHONE_NUMBER",
        start=13,
        end=26,
        score=0.9,
        recognition_metadata={"recognizer_name": "phone_rule"},
    )

    scanner.analyzer.analyze = lambda **kwargs: [local_phone, intl_phone]

    result = scanner.analyze_text("081234567890 +628123456789")

    assert result == [
        {
            "name": "phone_rule",
            "type": "ID_PHONE_NUMBER",
            "start": 13,
            "end": 26,
            "score": 0.9,
            "text": "+628123456789",
        }
    ]
