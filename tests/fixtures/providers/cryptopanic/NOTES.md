# CryptoPanic — DECISION RECORD (not a capture; no fixtures in this directory)

**Decision: DROPPED from the memecoin-bot design.**

## Why

CryptoPanic discontinued its free Developer API plan. Removal date: **2026-04-01**.
Confirmed by the operator (memecoin-bot project owner) on **2026-07-04**, ahead of
this recon pass — no live capture was attempted against CryptoPanic's API, since
there is no free tier left to recon against. This directory intentionally holds
no `*.json`/`*.jsonl` fixtures; it exists solely to record why CryptoPanic is
absent from `scripts/recon/rest_targets.json` and from the M0/M2 provider list,
so a future pass doesn't waste time re-discovering this.

## What changes in the design

The original plan used CryptoPanic as a news-sentiment / "panic" signal input to
market-regime detection. With it gone, **market-regime inputs are now**:

1. **Jupiter SOL-price drawdown** — already free, already captured in this recon
   arc (`tests/fixtures/providers/jupiter/`), no new dependency needed. A rolling
   SOL price drawdown is a reasonable proxy for "risk-off" market conditions that
   correlate with memecoin curve activity drying up.
2. **RSS panic keywords** — see `tests/fixtures/providers/rss/NOTES.md` (this same
   recon pass, A5 reshaped). CoinDesk + The Block + Cointelegraph RSS feeds give
   `title`/`description` text free of charge, pollable on a simple cadence, with
   no auth and no rate-limit wall encountered. A keyword scan over recent items
   (e.g. "crash", "hack", "exploit", "delist", "liquidation", "selloff") is a
   coarser but zero-cost substitute for a dedicated news-sentiment API.

Net effect: one less paid/gated dependency in the critical path; regime detection
leans more heavily on price action (Jupiter) with RSS as a qualitative overlay
rather than a quantitative sentiment score.

## Revisit condition

Only reconsider a paid news aggregator (CryptoPanic's paid tier, or an
alternative) if the Jupiter-drawdown + RSS-keyword combination proves materially
insufficient in practice (e.g. misses regime shifts that a sentiment feed would
have caught) **and** the cost is justified against the bot's live P&L — not
before. No specific paid-tier pricing was evaluated in this pass since the
free-tier removal alone was sufficient grounds to drop it for now.
