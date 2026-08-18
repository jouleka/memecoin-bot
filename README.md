# memecoin-bot

[![CI](https://github.com/jouleka/memecoin-bot/actions/workflows/ci.yml/badge.svg)](https://github.com/jouleka/memecoin-bot/actions/workflows/ci.yml)
[![CodeQL](https://github.com/jouleka/memecoin-bot/actions/workflows/codeql.yml/badge.svg)](https://github.com/jouleka/memecoin-bot/actions/workflows/codeql.yml)
[![Secret scan](https://github.com/jouleka/memecoin-bot/actions/workflows/secret-scan.yml/badge.svg)](https://github.com/jouleka/memecoin-bot/actions/workflows/secret-scan.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

An experimental, paper-only Solana memecoin scanner and strategy-research system.

> [!WARNING]
> This is unaudited research software, not financial advice. It has no wallet, private-key,
> signer, or live-order path. Paper fills can be unrealistic, and no result should be treated as
> evidence of profitability. Do not use it with real funds.

## Current scope

| Capability | Status |
| --- | --- |
| PumpPortal birth and migration ingestion | Implemented |
| Bonding-curve polling and lifecycle tracking | Implemented |
| Fail-closed safety checks and provider governors | Implemented |
| CLIMBING feature scoring and paper execution | Implemented, disabled by default |
| Smart-wallet evidence and early-buyer concentration checks | Implemented |
| Canonical-token selection and clone suppression | Implemented |
| Append-only decisions, fills, and forward-return evidence | Implemented |
| Telegram status and paper-trade alerts | Implemented, disabled by default |
| Post-graduation TRENDING strategy and market regime | Not included on `main` |
| Live signing or order submission | Not implemented |

The default configuration keeps both Telegram and paper entries disabled. The code is designed to
measure whether a strategy has an edge; it does not assume one exists.

## Architecture

```text
public market feeds
        |
        v
ingestion -> lifecycle tracker -> safety gate
                                 |
                                 v
                       deterministic features
                                 |
                                 v
                    canonical-token resolver
                                 |
                                 v
                     CLIMBING paper strategy
                                 |
                    +------------+------------+
                    |                         |
                    v                         v
             append-only ledger       optional Telegram alerts
                    |
                    v
          counterfactual forward returns
```

The implementation lives under `src/memebot/`. Provider behavior and known limitations are
summarized in [docs/PROVIDERS.md](docs/PROVIDERS.md).

## Requirements

- Python 3.13
- [`uv`](https://docs.astral.sh/uv/)
- A Helius RPC URL for provider-backed scanning
- Optional Telegram bot token and chat ID for alerts

## Install

```bash
git clone https://github.com/jouleka/memecoin-bot.git
cd memecoin-bot
uv sync --locked
cp config.toml config.local.toml
cp .env.example .env
```

Keep `.env` and `config.local.toml` local. The checked-in configuration contains no credentials.

## Run

For an idle local smoke run without provider credentials:

```bash
uv run --locked python -m memebot.main --config config.local.toml
```

For provider-backed paper research, set `MEMEBOT_HELIUS_RPC_URL` in the process environment or
load `.env` with your own process supervisor. If Telegram is wanted, set its two environment
variables and explicitly enable it in `config.local.toml`.

Paper entries remain disabled until `strategy.climbing.entries_enabled` is deliberately changed to
`true`. This still does not enable live trading: the repository contains no signer or order client.

The process writes its SQLite database and append-only journal beneath `storage.data_dir` (`data/`
by default). Treat those runtime artifacts as sensitive operational data and never commit them.

## Development

```bash
uv sync --locked
uv run --locked ruff check .
uv run --locked pytest
uv run --locked pip-audit
uv run --locked bandit -q -ll -r src
```

Tests use fixtures and local fakes; they must not require real credentials. Changes to scoring,
fills, or outcome tracking should preserve the no-look-ahead and never-better-than-quote
invariants.

## Security

Do not open a public issue containing credentials, private infrastructure, wallet data, or exploit
details. Follow [SECURITY.md](SECURITY.md) for private reporting.

## Contributing

Focused paper-mode fixes and reproducible research improvements are welcome. See
[CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE)
