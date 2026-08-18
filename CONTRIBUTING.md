# Contributing

Thanks for helping improve `memecoin-bot`.

## Before opening a change

- Keep examples and tests paper-only.
- Never add private keys, seed phrases, API tokens, production endpoints, private infrastructure,
  or captured operator data.
- Do not present simulated results as evidence of profitability.
- Preserve decision-time causality: future information must never affect an earlier score or fill.
- Keep changes focused and include regression tests for changed behavior.

## Development checks

```bash
uv sync --locked
uv run --locked ruff check .
uv run --locked pytest
uv run --locked pip-audit
uv run --locked bandit -q -ll -r src
```

## Security reports

Do not open a public issue for a suspected vulnerability. Follow [SECURITY.md](SECURITY.md).
