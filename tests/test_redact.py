from memebot.redact import redact_secrets


def test_redacts_telegram_token():
    s = "HTTPStatusError ... for url 'https://api.telegram.org/bot123:SECRET/sendMessage'"
    out = redact_secrets(s)
    assert "SECRET" not in out and "<redacted>" in out


def test_redacts_helius_key():
    s = "error for url 'https://mainnet.helius-rpc.com/?api-key=7ff0secret'"
    out = redact_secrets(s)
    assert "7ff0secret" not in out and "<redacted>" in out
