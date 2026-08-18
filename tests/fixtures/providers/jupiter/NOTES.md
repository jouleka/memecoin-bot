# Jupiter REST capture notes

Captured with `scripts/recon/capture_rest.py jupiter`. **The task's original 4
targets required correction** — see below. All 4 corrected targets then
worked.

## Corrections made (required)

The task's template used `quote-api.jup.ag/v6/quote` and
`api.jup.ag/price/v2` / `lite-api.jup.ag/price/v2`. First run against the
as-given template:

| target (as given in task) | result |
|---|---|
| `https://quote-api.jup.ag/v6/quote?...` (x2: fwd + reverse) | `ConnectError: [Errno -5] No address associated with hostname` — **host no longer resolves at all** |
| `https://api.jup.ag/price/v2?...` | `404` |
| `https://lite-api.jup.ag/price/v2?...` | `404` |

This matches the task's own heads-up ("The Jupiter `quote-api.jup.ag` vs
`api.jup.ag`/`lite-api.jup.ag` split moved in 2025"). Per the recon ground
rule, corrected via web search (dev.jup.ag redirects to
developers.jup.ag, whose docs pages 404'd on direct fetch — a JS-rendered
SPA — so the current hosts/paths were confirmed by directly probing
candidate URLs with `curl` and reading the actual status codes, not by
guessing):

| corrected target | status |
|---|---|
| `https://lite-api.jup.ag/swap/v1/quote?...` | `200` |
| `https://api.jup.ag/swap/v1/quote?...` | `200` |
| `https://lite-api.jup.ag/price/v3?...` | `200` |
| `https://lite-api.jup.ag/price/v2?...` | `404` (confirmed dead — v2 price is gone, v3 replaced it) |

`scripts/recon/rest_targets.json` was updated to the 4 corrected targets:
`quote_sol_bonk` and `quote_bonk_sol_reverse` now hit
`lite-api.jup.ag/swap/v1/quote`, `price_v3` hits `lite-api.jup.ag/price/v3`,
and a 4th target `quote_v1_paid_host_candidate` was added hitting
`api.jup.ag/swap/v1/quote` (same path, different host) specifically to
capture the paid-host's rate-limit headers for comparison — see below.

| fixture | endpoint | status |
|---|---|---|
| `quote_sol_bonk.json` | `GET lite-api.jup.ag/swap/v1/quote` (SOL->BONK) | 200 |
| `quote_bonk_sol_reverse.json` | `GET lite-api.jup.ag/swap/v1/quote` (BONK->SOL) | 200 |
| `price_v3.json` | `GET lite-api.jup.ag/price/v3` (SOL,BONK) | 200 |
| `quote_v1_paid_host_candidate.json` | `GET api.jup.ag/swap/v1/quote` (SOL->BONK) | 200 |

## Auth

**None required for `lite-api.jup.ag`** (free/keyless tier — all 3
`lite-api.jup.ag` targets returned `200` with no key). `api.jup.ag` (the
"paid" host per Jupiter's current tiering) also answered `200` **without any
key supplied here** and returned rate-limit headers (see below) — so
`api.jup.ag` is reachable keyless too, just presumably rate-limited harder
without a key than with one. For M4/M1 purposes, default to `lite-api.jup.ag`
as the free/no-key host.

## Rate limits

- `lite-api.jup.ag` responses (`quote_sol_bonk.json`,
  `quote_bonk_sol_reverse.json`, `price_v3.json`): **no rate-limit headers
  returned at all** (`"headers": {}` in all 3 fixtures) — no visibility into
  the free-tier ceiling from headers alone; go by Jupiter's documented
  general guidance (community-reported ~60 requests/min per IP for
  `lite-api.jup.ag`, not independently confirmed by a header here) and build
  in backoff/retry regardless.
- `api.jup.ag` response (`quote_v1_paid_host_candidate.json`) **did** return
  rate-limit headers:
  ```
  "x-ratelimit-remaining": "4",
  "x-ratelimit-current": "1",
  "x-ratelimit-reset": "1783091197"
  ```
  i.e. a very tight keyless ceiling on this host (remaining dropped to 4
  after a single request in this session — consistent with `api.jup.ag`
  being the paid/metered host that expects an API key for real throughput).
  **Do not build the M4 poller against `api.jup.ag` without a key** — use
  `lite-api.jup.ag` for the free tier.

No burst test was run against Jupiter (task's burst-test step targets
DexScreener only); given the tight `api.jup.ag` keyless ceiling observed
above, this is the right call.

## JSON paths adapters will read

### `quote_sol_bonk.json` / `quote_bonk_sol_reverse.json` / `quote_v1_paid_host_candidate.json` — swap quote response (flat object, not wrapped in `data`)

- `inputMint`, `outputMint`, `inAmount`, `outAmount` (all strings — token
  base units, apply `decimals` from elsewhere, e.g. GeckoTerminal/RugCheck,
  to convert to human units)
- `otherAmountThreshold` (string) — worst-case output after slippage
- `swapMode` (`"ExactIn"` observed), `slippageBps` (integer)
- **`priceImpactPct`** (string, e.g. `"0"` or
  `"0.000999816107059216263738614"`) — **this is the key field the M3
  safety gate should read** for slippage/liquidity-thinness signal on a
  prospective trade size.
- `routePlan[]` — each hop: `swapInfo.{ammKey,label,inputMint,outputMint,inAmount,outAmount}`,
  `percent` (int, % of total routed through this hop), `bps` (seen `null`
  in all captures here — may be populated for split routes, not exercised
  in this single-route capture)
- `contextSlot` (integer, Solana slot at quote time), `timeTaken` (float,
  seconds), `swapUsdValue` (string, USD value of the swap)
- `mostReliableAmmsQuoteReport.info` — map of `ammKey -> quoted outAmount
  string` from alternate AMMs Jupiter cross-checked, useful as a
  manipulation/outlier sanity check
- `platformFee`, `longtailMarketQuoteReport`, `useIncurredSlippageForQuoting`,
  `useRewards`, `otherRoutePlans`, `instructionVersion` — all observed
  `null` in every capture here; presence/shape when non-null not
  characterized by this recon.
- `loadedLongtailToken` (bool) — `false` in all captures (BONK/SOL/USDC are
  not "longtail"); semantics for `true` not observed.

Route composition observed: the SOL->BONK forward quote routed through 3
hops (AlphaQ -> Hadron -> ZeroFi, via JitoSOL and USDC as intermediate
legs) with `priceImpactPct: "0"`; the BONK->SOL reverse quote (10x larger
notional) took a single direct hop (Meteora DLMM) with a small nonzero
impact (`"0.000999816107059216263738614"` ~= 0.1%). Route choice and hop
count are amount- and direction-dependent, not fixed per pair.

### `price_v3.json` — price response (object keyed by mint address, NOT an array)

`body["<mint>"] = {createdAt, liquidity, usdPrice, blockId, decimals, priceChange24h}`:

- **`usdPrice`** (float) — the price field for the M4 poller to read
- `liquidity` (float, USD) — token-level aggregate liquidity figure (same
  role as GeckoTerminal's `total_reserve_in_usd`)
- `priceChange24h` (float, %) — plain percent, unlike DexScreener's
  occasionally-nonsensical `priceChange.h24`
- `blockId` (integer, Solana slot) — same slot value shared across both
  mints queried in the same request (`430544445` for both SOL and BONK
  here), i.e. it's the slot the price snapshot was taken at, not
  per-token.
- `createdAt` (ISO-8601 string) — when Jupiter's price engine first indexed
  this mint, not the token's on-chain creation time (BONK shows
  `"2024-06-07T10:26:40.709Z"`, long after BONK's actual 2022/2023 launch).
- `decimals` (integer) — convenient to have alongside price without a
  second lookup.

This is **v3**, replacing the task-template's v2 shape (`price_lite_candidate`
target from the original template no longer exists as given — v2 itself
404s, not just the lite-api host for it).

## Pagination

None — quote and price are both single-shot request/response, no
pagination concept applies to either endpoint.
