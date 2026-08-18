"""Structured JSON logging to stderr (journald picks it up on the VPS)."""
from __future__ import annotations

import json
import logging
import sys
import time

from memebot.redact import redact_secrets


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        out.update(getattr(record, "extra_fields", {}))
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        # Defense-in-depth: scrub secrets from the whole line, whatever field carried them
        # (httpx/httpcore echo full request URLs with api-key=…; so can a stray error string).
        return redact_secrets(json.dumps(out, default=str))


def setup_logging(level: str = "INFO") -> None:
    # Third-party request loggers echo full URLs (with api-key / bot-token) at INFO — silence
    # them so secrets never even reach the formatter (redaction is the backstop, not the plan).
    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    root = logging.getLogger()
    if any(getattr(h, "_memebot", False) for h in root.handlers):
        root.setLevel(level)
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    handler._memebot = True
    root.addHandler(handler)
    root.setLevel(level)
