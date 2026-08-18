# pump.fun mechanics recon (MB-2 / A6)

No new capture in this task — this is a derivation pass over data already
collected in A2 (`tests/fixtures/providers/pumpportal/session1.jsonl`) and A4
(`tests/fixtures/providers/helius/logs_subscribe_pumpfun.jsonl`, plus the
122 MB uncommitted backup at `/tmp/helius_full_capture.jsonl.bak` on the WSL
box, sampled for extra confidence), plus the official pump.fun Anchor IDL
(public repo, see Part 2). Program id `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P`
(confirmed in A4) throughout.

## Part 1 — curve mechanics from our captured data

### Initial virtual reserves (the constant)

All 197 `pool:"pump"` `create` frames in `session1.jsonl`: 30 of them are
"fresh, zero-activity" mints (`initialBuy: 0`, `solAmount: 0`). Every one of
those 30 shows **exactly**:

- `vSolInBondingCurve = 30`
- `vTokensInBondingCurve = 1073000000`
- `marketCapSol = 27.958993476234856` (identical to full float precision across all 30)

This matches the pump.fun-wide known constants (30 SOL / 1.073B tokens
virtual reserves at launch) and is corroborated independently by the Helius
`CreateEvent` decode in Part 2 (`virtual_sol_reserves = 30_000_000_000`
lamports, `virtual_token_reserves = 1_073_000_000_000_000` raw units — same
30 / 1.073B ratio once the 9-decimal SOL / 6-decimal token scaling is
divided out).

### Constant-product invariant check

Using `k = vSol0 * vTokens0 = 30 * 1,073,000,000 = 32,190,000,000`:

Checked `vSolInBondingCurve * vTokensInBondingCurve` for every one of the 197
`pump`-pool create frames (not just the fresh ones) — **all 197 reproduce
`k = 3.219e10` to full float precision** (max relative deviation ~1.2e-16,
i.e. IEEE-754 double noise — exact for all practical purposes). Example rows
(`vSol`, `vTokens`, `product`, `ratio to k`):

```
vSol=30.0000  vTokens=1073000000.00  product=3.219000e+10  ratio=1.00000000
vSol=34.9383  vTokens=921339222.67   product=3.219000e+10  ratio=1.00000000
vSol=30.8840  vTokens=1042288935.12  product=3.219000e+10  ratio=1.00000000
vSol=31.5000  vTokens=1021904761.90  product=3.219000e+10  ratio=1.00000000
```

**This is a textbook constant-product AMM curve** (`x*y=k`, same shape as
Uniswap v2), operating on *virtual* reserves rather than the smaller *real*
reserves that actually get transferred at migration (see Part 2's IDL
`BondingCurve`/`CreateEvent` fields — `virtual_*` vs `real_*` are distinct
fields, confirmed by the Anchor decode).

### `marketCapSol` formula

Checked `marketCapSol` against `(vSolInBondingCurve / vTokensInBondingCurve)
* 1_000_000_000` (spot price times the 1B fixed total token supply) for all
197 create frames: **max absolute error = 0** — this is an exact formula,
not an approximation:

```
initial price = 30 / 1,073,000,000 = 2.7958993476234855e-08 SOL/token
marketCapSol  = price * 1,000,000,000 = 27.958993476234856  (exact match to the reported value)
```

### Graduation / migration — what each feed shows

- **PumpPortal** (`txType:"migrate"`, `pool:"pump-amm"`): minimal 4-field
  payload (`signature, mint, txType, pool`) — no curve-state or amounts (see
  A2 NOTES.md). All 7 migrate frames in session1.jsonl land on `pool:
  "pump-amm"` — pump.fun's own AMM (PumpSwap), not an external DEX
  (Raydium/Orca). This is independently corroborated in Part 2: the Helius
  capture shows direct invocations of program id
  `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA` (PumpSwap's AMM program, 157
  log-line mentions in the 5-minute/123 MB capture) — i.e. graduated curves
  migrate to a pump.fun-operated constant-product pool, not a third-party
  AMM. PumpPortal's migrate frame gives no way to recover the graduation
  price/liquidity on its own — that has to come from the last curve state
  before migration (Helius logs, see Part 2) or a separate on-chain lookup.
- **Helius logs**: found and decoded exactly one `CompleteEvent` in the
  122 MB / 5-minute capture (graduation is rare — this run saw 137 `Create`
  and only 1 completion, consistent with the well-known "very few pump.fun
  launches ever graduate" pattern). Decoded fields: `user, mint,
  bonding_curve, timestamp, quote_mint` — `quote_mint` was the native-SOL
  placeholder pubkey `11111111111111111111111111111111` for the one example
  found, confirming this curve trades against SOL (pump.fun also supports
  other quote mints per the IDL's `Global.whitelisted_quote_mints`, not
  exercised in our sample). **Caveat**: the one `CompleteEvent` mint found in
  our sample uses a much smaller/non-standard virtual-reserve scale (~0.09
  SOL total, not the ~30-85 SOL "classic" curve) — this looks like a
  mayhem-mode or micro-liquidity variant (its trades also show
  `fee_basis_points`/`creator_fee_basis_points` frequently `0`, unlike the
  standard 95/30 seen on ordinary curves — see Part 2). None of the 316
  "standard-scale" mints (virtual_sol_reserves ~30 SOL at first trade) in our
  5-minute sample reached graduation (max observed: ~38.3 SOL raised on the
  most-traded standard mint) — expected, since graduation needs ~85 SOL and
  5 minutes of real-time trading rarely gets any single mint that far.

## Part 2 — Anchor event decode (headline)

### DECODE WORKED — yes, cleanly, at scale

**IDL source**: the official pump.fun Anchor IDL, published at
`https://github.com/pump-fun/pump-public-docs/blob/main/idl/pump.json`
(fetched during this recon session; not vendored into the repo to avoid
drift — re-fetch if ever needed for re-validation). This is the primary,
first-party source (pump.fun's own docs repo), not a third-party
reverse-engineering guess.

**Discriminator convention confirmed two independent ways**: (1) the IDL's
own `events[].discriminator` arrays, and (2) recomputing
`sha256(f"event:{EventName}")[:8]` from scratch (the standard Anchor
convention) — both agree byte-for-byte for every event checked (`CreateEvent
= 1b72a94ddeeb6376`, `TradeEvent = bddb7fd34ee661ee`, `CompleteEvent =
5f72619cd42e9808`, etc). The 8-byte discriminator prefixes every `Program
data:` blob; the rest is borsh-encoded fields in IDL declaration order.

**Validation run 1 — committed fixture** (`logs_subscribe_pumpfun.jsonl`,
147 lines): **93/93 TradeEvents decoded** (100%), **19/19 CreateEvents
decoded** (100%), **0 undecodable blobs among recognized discriminators**,
**0 is_buy/log-line mismatches** (every decoded `TradeEvent.is_buy` agreed
with the "Instruction: Buy"/"Instruction: Sell" log line in the same frame).

**Validation run 2 — full 122 MB/98,383-line capture** (`/tmp/
helius_full_capture.jsonl.bak`, sampled for extra confidence, not committed):
**9,973/9,974 TradeEvents decoded** (99.99%; the 1 gap is a length-8 blob
from an unrelated program that happened to share byte-length, filtered by
the decoder's `undecodable` counter), **117/117 CreateEvents decoded**
(100%), and **22/9,973 (0.22%) is_buy/log mismatches** — inspected a sample
of these and they are transactions where a router/aggregator program
invokes *both* a Buy and a Sell in the same transaction (e.g. arb bots), so
the "first Instruction: X seen in the log" heuristic used for this
validation check picks the wrong one; this is a check-script artifact, not a
decoder bug (the decoded event's own `is_buy` field is authoritative — it
comes from the event payload itself, not from log-text pattern matching).
One `CompleteEvent` was also found and decoded (see Part 1).

**Example — decoded `TradeEvent` vs its log line** (from the committed
fixture, first Sell in the file):

Log line: `"Program log: Instruction: Sell"` immediately followed by
`"Program data: vdt/007mYe4pX99a1GatUhujBFf2n0CwkhNcILF+3wcLErCYgFLKRVc0bx0AAAAA..."`

Decoded:
```
mint: CJiJAKFMD7HruUZwDhK5vBPV379yGhnR7YuLNbFxpump
sol_amount: 1068544735       (1.068544735 SOL, 9 decimals)
token_amount: 25511755060526 (25,511,755.060526 tokens, 6 decimals)
is_buy: False                 <- matches "Instruction: Sell" exactly
user: 6Q9413Rt2YTH2rKCjVueFwnxM4Ri472EmrgRPDzMyxxf
virtual_sol_reserves: 36188261779    (36.188261779 SOL)
virtual_token_reserves: 889514954991884
real_sol_reserves: 6188261779
real_token_reserves: 609614954991884
fee_recipient: FWsW1xNtWscwNmKv6wVsU1iTzRN6wmmk3MjxRP5tT7hz
fee_basis_points: 95          (0.95% protocol fee)
fee: 10151175                 (matches sol_amount * 0.0095, within rounding)
creator: 96rrAeq23jwPC9NwVEtCVxgbWbtsc2XPf5zU1pF1frc9
creator_fee_basis_points: 30  (0.30% creator fee)
creator_fee: 3205635          (matches sol_amount * 0.003, within rounding)
```

Per-mint monotonicity: checked all mints with >1 decoded trade in the
committed fixture (16 mints) and a larger set in the full capture — virtual
reserves move consistently with trade direction every time (BUY: vSol up /
vToken down; SELL: vSol down / vToken up), and consecutive trades for the
same mint chain correctly (each trade's post-state reserves become the next
trade's pre-state reserves, modulo other trades interleaved for the same
mint from other traders in between).

**Decoder**: `scripts/recon/decode_pumpfun_events.py` (stdlib only — base64,
struct, hashlib, no anchorpy/borsh/base58 dependency; includes a minimal
inline base58 encoder for pubkeys). Decodes `TradeEvent` and `CreateEvent`
fully; recognizes (but does not fully field-decode) several other event
discriminators seen in the wild (`ClaimCashbackEvent`,
`CloseUserVolumeAccumulatorEvent`, `InitUserVolumeAccumulatorEvent`,
`CollectCreatorFeeEvent`, `MigrateBondingCurveCreatorEvent`,
`ExtendAccountEvent`, `DistributeCreatorFeesEvent`, `CompleteEvent` —
manually spot-decoded for this recon, not wired into the script's main
loop since they're not needed for the core price/size use case). A handful
of "unknown" discriminators appear at low counts (1-4 occurrences each in
the 122 MB capture) — these did not match any `events[]` discriminator in
the fetched IDL; most likely `Program data:` lines from *other* programs
that also happen to appear inside a pump.fun-mentioned transaction (our line
scan isn't scoped to invoke-depth), not gaps in the pump.fun event set
itself. Not chased further — irrelevant to the trade/price/graduation path.

### Field-layout table: `TradeEvent` (stable prefix, what the decoder reads)

| offset (bytes, from start of `Program data:` payload) | field | type | notes |
|---|---|---|---|
| 0 | discriminator | `[u8; 8]` | `bddb7fd34ee661ee` |
| 8 | mint | pubkey (32B) | base58 |
| 40 | sol_amount | u64 | lamports (9 decimals) |
| 48 | token_amount | u64 | raw token units (6 decimals) |
| 56 | is_buy | bool (1B) | |
| 57 | user | pubkey (32B) | trader wallet |
| 89 | timestamp | i64 | unix seconds |
| 97 | virtual_sol_reserves | u64 | lamports |
| 105 | virtual_token_reserves | u64 | raw units |
| 113 | real_sol_reserves | u64 | lamports (smaller — excludes the virtual cushion) |
| 121 | real_token_reserves | u64 | raw units |
| 129 | fee_recipient | pubkey (32B) | |
| 161 | fee_basis_points | u64 | protocol fee, bps |
| 169 | fee | u64 | lamports actually charged |
| 177 | creator | pubkey (32B) | token creator wallet |
| 185 | creator_fee_basis_points | u64 | bps |
| 193 | creator_fee | u64 | lamports actually charged |

**Version-skew caveat**: the *current* on-chain IDL's `TradeEvent` has many
more trailing fields after `creator_fee` (`track_volume,
total_unclaimed_tokens, total_claimed_tokens, current_sol_volume,
last_update_timestamp, ix_name (string), mayhem_mode, cashback_fee_bps,
cashback, buyback_fee_bps, buyback_fee, shareholders (vec), quote_mint,
quote_amount, virtual_quote_reserves, real_quote_reserves` — reflecting
newer features like mayhem mode, cashback, non-SOL quote mints, and creator
fee-sharing). Our decoder reads only the 16-field stable prefix through
`creator_fee`, which fully covers what M2 needs (price, size, direction,
reserves, fees) and decoded successfully on every trade in both samples
(the trailing bytes are read but not parsed — `trailing_raw_len` in the
dataclass just reports how many bytes are left over, for visibility). If a
future capture needs the newer fields (e.g. `mayhem_mode`,
`virtual_quote_reserves` for non-SOL-quoted tokens), extend the decoder
past `creator_fee` using the field order shown in the IDL's current
`TradeEvent` type — do not assume the prefix-only decode is forward-proof if
pump.fun changes the *early* field order (only appending new trailing
fields is safe for this decoder, which is exactly what has happened between
our sample and the current IDL).

### Field-layout table: `CreateEvent`

| field | type |
|---|---|
| discriminator | `[u8;8]` = `1b72a94ddeeb6376` |
| name | string (u32 length prefix + utf8) |
| symbol | string |
| uri | string |
| mint | pubkey |
| bonding_curve | pubkey |
| user | pubkey |
| creator | pubkey |
| timestamp | i64 |
| virtual_token_reserves | u64 |
| virtual_sol_reserves | u64 |
| real_token_reserves | u64 |
| token_total_supply | u64 |

(The current IDL's `CreateEvent` additionally has trailing `token_program,
is_mayhem_mode, is_cashback_enabled, quote_mint, virtual_quote_reserves` —
same append-only skew as `TradeEvent`, not needed for M2's core use case and
not decoded here.)

Cross-check: all 19 `CreateEvent`s decoded from the committed fixture show
**identical** `virtual_token_reserves = 1,073,000,000,000,000`,
`virtual_sol_reserves = 30,000,000,000`, `real_token_reserves =
793,100,000,000,000`, `token_total_supply = 1,000,000,000,000,000` — i.e.
(dividing out 6/9 decimals) 1.073B virtual tokens / 30 virtual SOL / 793.1M
real (sellable) tokens / 1B total supply, for every single mint. This
matches Part 1's PumpPortal-derived constants exactly and confirms these are
fixed protocol constants, not per-mint parameters.

## Pinned parameters (value + source; observed wins where they disagree)

| parameter | observed value | docs value | source | notes |
|---|---|---|---|---|
| initial virtual SOL reserves | 30.000000000 SOL | 30 SOL | observed (PumpPortal + Helius CreateEvent, exact across 197+19 samples) | agrees with docs |
| initial virtual token reserves | 1,073,000,000 tokens | 1.073B | observed, exact | agrees with docs |
| total token supply | 1,000,000,000 tokens | 1B | observed (`token_total_supply` in CreateEvent), exact | agrees with docs |
| real (curve-sellable) tokens | 793,100,000 tokens (79.31% of supply) | commonly cited ~793M-800M | observed exact via CreateEvent; docs/community figures vary (some cite "800M") | **observed wins**: 793.1M exactly, not a round 800M |
| protocol (curve) trading fee | 0.95% (95 bps), 90/93 fixture trades (3 trades on 2 mints showed 0/0 bps — see Part 1 mayhem/micro-scale caveat) | 0.950% | https://pump.fun/docs/fees (fetched this session) | observed and docs agree exactly on standard-curve trades |
| creator fee | 0.30% (30 bps) on curve trades, but 0 bps on ~half of trades for one non-standard/mayhem-scale mint | 0.300% (standard bonding-curve tier) | https://pump.fun/docs/fees | standard curve trades agree exactly (30 bps); the 0-bps cases are a distinct mayhem/micro-scale variant, not a docs mismatch — see Part 1 caveat |
| total curve trading fee | 1.25% (0.95 protocol + 0.30 creator) | 1.25% | https://pump.fun/docs/fees | agrees |
| migration/graduation fee | not observed (no graduation trade sequence captured start-to-finish in our window) | 0.015 SOL | https://pump.fun/docs/fees (fetched this session) | **docs only** — note some older third-party sources online cite a stale "~6 SOL" migration fee figure; pump.fun's own current docs page says 0.015 SOL, and that's the one used here (first-party, freshly fetched) |
| graduation threshold | not fully observed (max real_sol_reserves seen in our 5-min sample: ~38.3 SOL on the furthest-progressed standard-scale mint, out of ~85 SOL needed) | ~85 SOL raised / ~$69K market cap (fluctuates 84-86 SOL with SOL price) | community sources (multiple 2026 articles, cross-checked) — pump.fun's own /docs/fees page does not state the SOL threshold explicitly | docs-adjacent community consensus; not independently confirmed on-chain in this pass since no mint in our sample graduated on the standard curve |
| destination pool on graduation | `pump-amm` (PumpPortal), PumpSwap program `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA` invoked 157x in the Helius capture | PumpSwap (pump.fun's own AMM) | observed (both feeds) | agrees — graduation goes to pump.fun's own AMM, not Raydium/Orca |

## Corrections made

None to A2/A4's captured data (this task only derives from what's already
committed). The IDL fetch (`pump.json`, `pump_fees.json` from
`pump-fun/pump-public-docs`) needed no correction — the published IDL's
`events[].discriminator` values matched the independently-recomputed
`sha256("event:Name")[:8]` values on the first attempt, and every
`TradeEvent`/`CreateEvent` decoded from real captured data on the first
attempt at the field layout (no trial-and-error on field order/types was
needed once the IDL was in hand).
