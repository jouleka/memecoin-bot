import json
import logging

from memebot.logging_setup import JsonFormatter, setup_logging


def test_formatter_emits_json_with_extras():
    record = logging.LogRecord("memebot.x", logging.INFO, "f.py", 1, "hello %s", ("w",), None)
    record.extra_fields = {"mint": "M"}
    out = json.loads(JsonFormatter().format(record))
    assert out["msg"] == "hello w"
    assert out["level"] == "INFO"
    assert out["logger"] == "memebot.x"
    assert out["mint"] == "M"
    assert "ts" in out


def test_formatter_survives_non_serializable_extras():
    import datetime

    record = logging.LogRecord("memebot.x", logging.INFO, "f.py", 1, "m", (), None)
    record.extra_fields = {"holders": {"a", "b"}, "when": datetime.datetime(2026, 7, 3)}
    out = json.loads(JsonFormatter().format(record))  # must not raise, must be valid JSON
    assert "holders" in out and "when" in out  # stringified, not dropped


def test_setup_is_idempotent():
    setup_logging("INFO")
    setup_logging("INFO")
    root = logging.getLogger()
    assert len([h for h in root.handlers if getattr(h, "_memebot", False)]) == 1


def test_formatter_redacts_secrets_in_any_field():
    # httpx logs full request URLs with the api-key at INFO — the formatter must scrub them.
    record = logging.LogRecord(
        "httpx", logging.INFO, "f.py", 1,
        "HTTP Request: POST https://mainnet.helius-rpc.com/?api-key=7ff0SECRET \"200 OK\"",
        (), None)
    line = JsonFormatter().format(record)
    assert "7ff0SECRET" not in line and "<redacted>" in line
    # a key hidden in an extra_field is scrubbed too (whole-line redaction)
    record2 = logging.LogRecord("memebot.x", logging.INFO, "f.py", 1, "m", (), None)
    record2.extra_fields = {"url": "https://x/?api-key=LEAKED"}
    assert "LEAKED" not in JsonFormatter().format(record2)


def test_setup_silences_httpx_request_logging():
    setup_logging("INFO")
    assert logging.getLogger("httpx").level == logging.WARNING
    assert logging.getLogger("httpcore").level == logging.WARNING
