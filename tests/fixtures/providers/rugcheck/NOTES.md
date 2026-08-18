# RugCheck REST capture notes

Captured with `scripts/recon/capture_rest.py rugcheck`. Both targets from the
task's `rest_targets.json` worked exactly as given — **no URL correction was
needed**.

| fixture | endpoint | status | size |
|---|---|---|---|
| `report_bonk.json` | `GET /v1/tokens/{mint}/report` | 200 | live response ~635 KB; committed fixture hand-trimmed to ~23 KB (see below) |
| `summary_bonk.json` | `GET /v1/tokens/{mint}/report/summary` | 200 | <1 KB |

## Auth

None required/observed. No API key on either endpoint.

## Rate limits

Both responses returned explicit rate-limit headers:

```
report_bonk:  "x-rate-limit-limit": "15", "x-rate-limit-remaining": "14"
summary_bonk: "x-rate-limit-limit": "15", "x-rate-limit-remaining": "13"
```

i.e. a documented/enforced **15 requests per window** (window duration not
specified by the header itself — treat as the safe assumption of a short
window, e.g. per-minute, until proven otherwise) on the free/keyless tier,
and the counter is shared across both endpoints (remaining dropped 15->14->13
across the two sequential calls in this session, decrementing by 1 each
time — not scoped separately per-endpoint). **This is the tightest
documented limit of the 5 keyless providers captured in this task** — the M4
poller must throttle RugCheck calls accordingly (roughly 1 call per 4s
sustained, well under 15/window, with slack for jitter/retries).

No burst test was run against RugCheck (task's burst-test step targets
DexScreener only; would also be reckless given the 15-req ceiling above).

## Fixture size warning — and fixture hand-trimmed post-capture

The **live** `/report` response for BONK was **~635 KB** — far larger than
any other capture in this recon batch (next largest, `token_bonk.json` from
DexScreener, is ~40 KB). The bulk of it was the `markets[]` array (**207**
pool/market entries for BONK, each with full mint/liquidity-account
sub-objects, ~409 KB) plus a `knownAccounts` map of **578** known
Meteora/Raydium pool addresses (~53 KB). **Downstream code consuming live
RugCheck report responses should expect this endpoint's payload size to
scale with a token's pool count** — a long-tail memecoin with 1-2 pools will
be far smaller; an established token like BONK with many external pools will
be this large. Consider whether the M4 poller needs the full `report` at all
vs. just `report/summary` for routine polling, reserving the full report for
one-time/on-demand deep checks.

**Fixture hand-trimmed post-capture** (review follow-up): the committed
`report_bonk.json` was trimmed from the raw 635 KB capture to **~23 KB** to
keep the parser fixture lean:

- `body.markets[]`: **207 -> 3** representative entries, all genuinely
  observed data (arrays trimmed, nothing synthesized), chosen to preserve
  the shape variety a parser must handle: one fully-locked pool
  (`4UDXmQvau4tYoAGZr3j2Pd6DPQTE48dPKy2GnoPbyonE`, `marketType:
  "raydium_cpmm"`, `lp.lpLockedPct: 100`), one unlocked pool
  (`6oFWm7KPLfxnwMb3z5xwBoXNSPP3JJyirAPqPSiVcnsp`, `marketType:
  "meteoraDlmm"`, `lpLockedPct: 0`), and one of a third market type
  (`3UwfrdLTpAjxTRni1boc5HUWe6hzc4HgE5yLdvEp2Noc`, `marketType:
  "raydium_clmm"`, `lpLockedPct: 0`).
- `body.knownAccounts`: **578 -> 5** entries (shape-representative sample).
- Every other field (topHolders, risks, score, lockers, insiderNetworks,
  verification, totalMarketLiquidity, etc.) left byte-identical to the
  capture.

Consequence: aggregate figures in the fixture (e.g. `totalMarketLiquidity`,
`totalLPProviders: 80`) were computed by RugCheck over the FULL 207-market
set and will NOT reconcile against summing the 3 retained `markets[]`
entries — don't write a fixture test asserting that reconciliation.

## Safety-gate-relevant field mapping

This is one of the two safety-gate providers (with GoPlus). Exact JSON paths
in `report_bonk.json` (all under top-level `body`):

- **Mint authority**: `body.token.mintAuthority` (also duplicated at
  `body.mintAuthority` top-level) — `null` for BONK (mint authority
  renounced; a live/non-null value here is a rug-risk red flag: the deployer
  can mint more supply at will).
- **Freeze authority**: `body.token.freezeAuthority` (also duplicated at
  `body.freezeAuthority` top-level) — `null` for BONK (non-null means the
  deployer can freeze holder wallets, another red flag).
- **LP / liquidity-lock status**: **per-market**, not a single top-level
  field — `body.markets[].lp.{lpLocked, lpLockedPct, lpLockedUSD,
  lpUnlocked, lpMaxSupply, lpTotalSupply}`. `lpLockedPct` is the field to
  read per pool (ranges observed 0 to 100 across BONK's 207 market entries
  in the raw capture — e.g. one pool shows `lpLockedPct: 100` fully locked,
  most show `0` unlocked; the committed fixture retains 3 of the 207, see
  the trim note above).
  There is **no single aggregate "is LP locked" boolean** — a real adapter
  must decide how to roll up per-pool lock percentages (e.g.
  liquidity-weighted average, or "worst pool with >X% of total liquidity").
  Related: `body.lockers{}` (keyed by pool pubkey) gives `unlockDate`
  (`0` in every entry observed = no unlock date set / perpetually locked or
  unspecified) and `usdcLocked` per locker contract.
- **Top holders**: `body.topHolders[]`, each
  `{address, amount, decimals, pct, uiAmount, uiAmountString, owner,
  insider}`. **`pct`** is the per-holder concentration to threshold against
  (top holder for BONK: `7.95%`). **`insider`** (bool) flags holders
  RugCheck's own heuristics tag as insider-linked — all `false` in this
  capture despite `graphInsidersDetected: 5623` at the top level (see next
  bullet) — i.e. `topHolders[].insider` and the top-level insider-graph
  count are computed differently / not synced 1:1; don't assume the
  top-holders list surfaces the same insiders the graph flags.
- **Overall risk verdict fields**:
  - `body.score` (int, `101` for BONK) and `body.score_normalised` (int
    0-100 scale presumably, `7` for BONK — low/good) — **`score_normalised`
    is the one to threshold on**, not raw `score` (raw score is unbounded
    and additive across risks, seen accumulating past 100).
  - `body.risks[]` — array of `{name, value, description, score, level}`;
    `level` is the human-readable severity (`"warn"` observed here for
    "Mutable metadata" — the only risk flagged for BONK). Other `level`
    values (e.g. `"danger"`/`"critical"`) not observed in this single
    low-risk-token capture; the M3 safety gate schema should treat `level`
    as an open string enum, not a fixed 2-value set, until a genuinely risky
    token is captured for comparison.
  - `body.rugged` (bool) — `false` for BONK; presumably flips true for
    tokens RugCheck has confirmed as an executed rug (distinct from
    `risks[]`, which is predictive/heuristic).
  - `summary_bonk.json`'s **`/report/summary` endpoint duplicates a subset**:
    `{tokenProgram, tokenType, risks[], score, score_normalised,
    lpLockedPct}` — notably `summary` exposes a **single top-level
    `lpLockedPct: 3.14...`** (a pre-aggregated rollup RugCheck computes for
    you) whereas the full `/report` only gives it per-market under
    `markets[].lp.lpLockedPct`. **For the M4 poller, prefer `/report/summary`
    for the routine per-cycle LP-lock check** (small payload, pre-aggregated
    `lpLockedPct`), and only fetch the full `/report` when a deeper
    per-pool/holder/insider breakdown is needed.
- **Other risk-relevant fields worth flagging for M3**:
  `body.totalMarketLiquidity` / `body.totalStableLiquidity` (floats, USD,
  top-level aggregate — the sum RugCheck itself computes across all
  `markets[]`, sparing the adapter from summing per-pool `lp.quoteUSD` /
  `baseUSD` itself), `body.totalHolders` (int), `body.totalLPProviders`
  (int), `body.transferFee.{pct,maxAmount,authority}` (a nonzero `pct` here
  would flag a transfer-tax/honeypot-style token — `0` for BONK),
  `body.graphInsidersDetected` (int) + `body.insiderNetworks[]` (each
  `{id, size, type, tokenAmount, activeAccounts}` — cluster-level insider
  detection, distinct from the per-holder `insider` bool above; BONK shows
  2 detected clusters totaling 5623 accounts despite the low
  `score_normalised`, so a **high insider-cluster count does not by itself
  imply a high risk score** — these appear to be independent signals in
  RugCheck's model, not merged into one verdict), `body.verification.
  {jup_verified, jup_strict}` (bools — whether Jupiter's own token list has
  vetted this mint; both `true` for BONK), `body.launchpad` (`null` for
  BONK — presumably populated with e.g. `"pump.fun"` for a pump.fun-native
  launch, not exercised here since BONK predates pump.fun).

## Corrections made

None — both target URLs resolved first try with `200` and valid JSON.
