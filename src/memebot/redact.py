"""Strip secrets from strings before they reach logs or persisted details."""
from __future__ import annotations

import re

_PATTERNS = [
    (re.compile(r"/bot[^/\s]+/"), "/bot<redacted>/"),          # telegram bot token in URL
    (re.compile(r"api-key=[^&\s'\"]+"), "api-key=<redacted>"),  # helius rpc key in URL
]


def redact_secrets(text: str) -> str:
    for pat, repl in _PATTERNS:
        text = pat.sub(repl, text)
    return text
