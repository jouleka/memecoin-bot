# Helius recon (MB-2 / A4)

Captured 2026-07-04. API key sourced from `~/.memebot-recon.env` at runtime, never
written to any repo file or fixture. Scrub check run after capture (see bottom).

## Program id

`6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` — CONFIRMED, not just a candidate.
The 5-minute `logsSubscribe({"mentions": [...]})` capture returned continuous
high-volume traffic (98,383 frames in 5 minutes) and direct log lines like
`"Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P invoke [2]"` followed by
`"Program log: Instruction: Buy"` / `"Instruction: Sell"` / `"Instruction: Create"`.
If it were the wrong id we'd have seen zero frames; we saw the opposite problem
(overwhelming volume). No correction needed.

## RPC safety-gate probes (`rpc` mode)

Three calls, run twice (see `getTokenLargestAccounts` note below), all against BONK
(`DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263`) as a wired-open reference token:

- `get_account_info_mint.json` — HTTP 200. JSON paths:
  - `body.result.value.data.parsed.info.mintAuthority` → `null`
  - `body.result.value.data.parsed.info.freezeAuthority` → `null`
  Both null/revoked, as expected for BONK. This is the exact shape the safety gate
  needs for the mint/freeze-authority check (rugpull vector #1: a live mint
  authority can print unlimited supply post-launch).
- `get_token_supply.json` — HTTP 200. Path: `body.result.value.uiAmountString`.
  Clean.
- `get_token_largest_accounts.json` — HTTP 200 **transport-level**, but the JSON-RPC
  body itself is an error both times it was run:
  `{"error": {"code": -32603, "message": "account index service overloaded, please
  try again."}}`. This is Helius's own account-index service, not a rate-limit or
  auth problem (no backoff/retry-after signal, no per-key throttle indicated).
  IMPLICATION: `getTokenLargestAccounts` cannot be treated as reliably available in
  the safety gate — needs a retry-with-backoff wrapper and a design fallback (e.g.
  gate on mint/freeze authority + LP-lock heuristics alone if this call keeps
  failing) rather than blocking token admission on it. Path if it succeeds:
  `body.result.value[]` = array of `{address, amount, decimals, uiAmount}` sorted
  descending — top entry(ies) vs `getTokenSupply` gives holder-concentration ratio.

## logsSubscribe capture — volume & the trade-data assessment (the headline)

5 minutes, `mentions: [pump.fun program]`, commitment `confirmed`:

- **98,383 frames** in 300s ≈ **328 frames/sec**, raw capture **123 MB** on disk.
- Of those, only **10,190 frames (~10%)** contain a `Program data: <base64>` blob
  (the Anchor self-describing event log emitted on Buy/Sell/Create instructions).
  Instruction-name counts in the 5 min window: **Buy 5,997, Sell 4,523, Create 137**
  (sums are close to the 10,190 data-blob count, consistent with ~1 event log per
  successful curve instruction).
- **~90% of frames (88,216) have a non-null `err`.** This looked alarming at first
  (implying pump.fun itself fails 9 times out of 10) but a follow-up pass over the
  full capture shows that's misleading: of those 88,216 error frames, only **637
  (0.7%)** are cases where the pump.fun program's own invocation is the one that
  failed (`"...failed: custom program error: 0x..."` on a line right after a
  `6EF8rr...` invoke). The other ~87,500 error frames are transactions where the
  pump.fun program id is merely *mentioned* (present as an account key /
  incidentally logged) while an unrelated program actually reverted — heavily
  dominated by MEV/arb-sniper bots (repeating `"Program log: !a"` spam programs
  like `5kZmvKbaqNjSNEnQKEREEKu5r9JkaDRKrQMUvZagCMoz` failing on `Custom` errors).
  **Practical consequence:** `mentions`-filtered `logsSubscribe` is noisy — a
  consuming pipeline needs to filter on "pump.fun program actually invoked and
  succeeded" (check for its own `invoke`/`success` pair, not just presence in the
  mentions match) before treating a frame as a real curve event, otherwise ~90% of
  ingested frames are wasted parsing effort on irrelevant/failed transactions.

### What's derivable from logs alone vs. what needs `getTransaction`

Example real Buy frame (line 2 of the committed fixture, trimmed):

```
"logs":[
  ...,
  "Program GMgnVFR8Jb39LoXsEVzb3DvBy3ywCmdmJquHUy1Lrkqb invoke [1]",
  "Program log: Instruction: Buy",
  "Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P invoke [2]",
  "Program log: Instruction: Buy",
  ...
  "Program data: vdt/007mYe4pX99a1GatUhujBFf2n0CwkhNcILF+3wcLErCYgFLKRVc0bx0AAAAA...",
  "Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P consumed 2093 of 41468 compute units",
  "Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P success",
  ...
]
```

Honest breakdown:

- **Derivable directly from the log text, no follow-up call:** instruction type
  (Buy/Sell/Create), which program invoked it (here `GMgnVFR8...` = a GMGN router,
  not a direct pump.fun UI trade — so we also get "was this routed through an
  aggregator" for free), success/failure, the tx signature, and the slot. Enough
  to build a live "something happened" tick stream and a coarse buy/sell/create
  counter per mint (mint address would need extracting from `Program data`
  accounts or a companion `getTransaction`/pubkey-in-logs parse — the raw log
  lines here do NOT print the mint pubkey or token amounts as plain text).
- **NOT derivable from the log text as printed:** actual trade size (SOL in /
  tokens out), price, or the mint address in a directly-readable form. Those live
  inside the base64 `Program data:` blob, which is an Anchor-style borsh-encoded
  event struct (discriminator + fields) — decodable *offline*, with zero extra
  RPC/credit cost, IF we reverse-engineer or find the published pump.fun IDL/event
  layout (same reverse-engineering lift every third-party pump.fun indexer/GMGN
  clone has done; not attempted in this recon pass — flagged as next-step work,
  candidate ticket for M2).
  Fallback if the blob can't be decoded in time: call `getTransaction` on the
  signature, which returns full parsed instruction data (mint, amounts, pre/post
  balances) at a **known, additional credit cost per call** (standard RPC credit
  weighting, i.e. this is metered separately from the streaming 20cr/MB and would
  multiply cost roughly 1x per real trade if done for every Buy/Sell — expensive
  at 5,997+4,523 ≈ 10,500 real trades / 5 min, i.e. ~3M+ `getTransaction` calls/month
  if done for every trade at this volume, not credit-cheap alongside the streaming
  bill below).
- **Verdict: decoding the `Program data` blob offline is the only economically
  viable path to trade size/price at this volume — `getTransaction` fallback
  should be reserved for spot-checks / low-volume verification, not the primary
  path.**

## Free-tier fit verdict (stream vs. poll-degrade for M2)

Free plan (from https://www.helius.dev/pricing, confirmed via search of
https://www.helius.dev/docs): **1,000,000 credits/month, 10 RPC requests/sec cap,
standard LaserStream WSS access included** (no documented WS-specific rps/connection
cap found in the public docs at recon time). Per Helius's 2026-04-07 pricing update,
**streaming traffic (LaserStream gRPC + WebSockets, which is what `logsSubscribe`
rides on) is billed at 20 credits per 1 MB** of data transferred — not per-message,
not per-subscribe-call.

Extrapolating the observed capture (123 MB / 5 min):

- 24/7 continuous `logsSubscribe` on pump.fun ≈ **1.06 TB/month** ≈
  **~21.25M credits/month** — **~21x over the free 1M-credit budget.**
- The free tier's 1M credits only buys **~2,033 minutes (~33.9 hours, ~1.4 days)**
  of continuous streaming per month at this program's traffic level.

**Caveat on the 21x figure**: this extrapolates a single 5-minute sample taken
on a Friday/Saturday linearly to a full month. Real pump.fun traffic is bursty
(intraday and day-of-week variation, viral-launch spikes, etc.), so the true
monthly average bytes/sec could differ from this one window — but traffic
would need to be ~20x lower on average, essentially continuously, to bring
24/7 streaming back under the free 1M-credit budget. That scale of
systematic overstatement is not plausible for this program (pump.fun is
consistently one of the highest-traffic programs on Solana), so it does not
change the verdict below, only the precision of the "21x" number itself.

**VERDICT: `logsSubscribe` on the raw pump.fun program firehose does NOT fit the
free tier for always-on M2 operation.** Options, in order of preference for a
free-tier-first bot:
1. **Narrower server-side filter** — Helius `logsSubscribe` only supports
   `mentions` (no instruction-level filter), so volume reduction has to happen
   client-side (drop non-invoke/non-success frames immediately — cuts ~90% of
   bytes-to-parse but does NOT cut the metered WS byte volume, since Helius bills
   on bytes sent over the wire before your client filters them).
2. **Poll-degrade**: fall back to periodic `getSignaturesForAddress` +
   `getTransaction` polling for the pump.fun program at a controlled cadence,
   trading completeness for a bounded, predictable credit cost — likely the
   pragmatic free-tier default, with the WS stream reserved for short bursty
   windows (e.g. only during an active watch on a specific just-launched mint,
   not the whole-program firehose).
3. **Pay for it**: Developer plan ($49/mo) — needs its credit allotment checked
   against this same 21.25M/month math before assuming it's enough headroom either
   (not done in this pass; flag for A6/M2 costing).

## Key-scrub confirmation

Ran after every capture step:
- Full-value grep for the sourced env var across `tests/` and `scripts/` → no matches (CLEAN).
- Grep for the key's first four characters across `tests/fixtures/` → no matches (CLEAN).
- `grep -rn 'api-key=' tests/fixtures/providers/helius scripts/recon/capture_helius.py`
  → only matches inside `capture_helius.py`'s `RPC`/`WSS` f-string *templates*
  (`{KEY}` placeholder substituted from env at runtime); no literal key material in
  any committed file. Fixture JSON bodies do not echo the request URL, so no key
  leaked into captured responses either.

## Fixture truncation disclosure

The raw capture was **123 MB / 98,383 lines** — far over the ~2 MB guideline.
`logs_subscribe_pumpfun.jsonl` as committed is a **curated 147-line slice** (not a
naive first-300-lines truncation): the subscription-ack line, a 7-line contiguous
"typical burst" from the start of the session, plus up to 40 real Buy frames, 40
real Sell frames, 20 Create frames, and 40 error frames sampled across the full
session — chosen so the fixture stays representative of both the trade-relevant
frames and the noisy-error majority discussed above, rather than just whatever
happened to arrive in the first few seconds. Full 123 MB capture was **not**
committed and was moved out of the repo working tree (`/tmp/helius_full_capture.jsonl.bak`
on the WSL box, outside git) to avoid bloating the repo; regenerate by re-running
`capture_helius.py logs --minutes 5` if a fuller sample is ever needed.
