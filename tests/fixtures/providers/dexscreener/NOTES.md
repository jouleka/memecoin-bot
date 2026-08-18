# DexScreener REST capture notes

Captured with `scripts/recon/capture_rest.py dexscreener` (also re-run as part of
`--all`). All 4 targets from the task's `rest_targets.json` worked exactly as
given — **no URL correction was needed** for DexScreener.

| fixture | endpoint | status |
|---|---|---|
| `token_bonk.json` | `GET /latest/dex/tokens/{mint}` | 200 |
| `search_sol.json` | `GET /latest/dex/search?q={query}` | 200 |
| `profiles_latest.json` | `GET /token-profiles/latest/v1` | 200 |
| `boosts_latest.json` | `GET /token-boosts/latest/v1` | 200 |

## Auth

None required/observed. No API key, no auth header, on any of the 4 endpoints.

## Rate limits

No `x-rate-*`/`retry-*`/`x-request-id` style headers were present on any
DexScreener response captured here (the harness's header filter matched
nothing — `"headers": {}` in every fixture file). The official docs
(https://docs.dexscreener.com/api/reference) state **60 requests/minute** for
the `token-profiles` and related "latest"/"trending" endpoints; the
pairs/tokens/search endpoints aren't given an explicit documented number on
that page, but community reports and the same docs page group them under a
similar general limit.

**Empirical burst test** (per task Step 3, one modest burst against
`token_bonk`'s endpoint, 30 sequential `curl` requests, no sleep):

```
200 200 200 200 200 200 200 200 200 200 200 200 200 200 200 200 200 200 200
200 200 200 200 200 200 200 200 200 200 200
```

All 30 requests returned `200` — no `429` observed at this volume. Consistent
with (or under) the ~60/min documented ceiling; do not conclude that the
service allows unlimited bursts beyond this — 30 requests is a floor, not a
proven ceiling.

## JSON paths adapters will read

### `token_bonk.json` / `search_sol.json` — `body.pairs[]`

Each pair object (this is the DEX-pair/pool granularity, **not** one row per
token — a token can appear in many pairs across many pools/DEXes):

- `chainId` (e.g. `"solana"`), `dexId` (e.g. `"raydium"`, `"orca"`,
  `"meteora"`, `"pumpfun"`, `"pumpswap"`), `pairAddress`, `url`
- `baseToken.{address,name,symbol}` / `quoteToken.{address,name,symbol}`
- **Price**: `priceNative` (string, quote-token units), `priceUsd` (string)
- **Liquidity**: `liquidity.{usd,base,quote}` — `usd` is what the safety
  gate/pollers should key on; note `liquidity` is *absent entirely* on some
  low-activity pairs (e.g. the BSC/Polygon SOL pairs in `search_sol.json`),
  so treat it as optional, not guaranteed present.
- **Volume**: `volume.{m5,h1,h6,h24}` (numbers, USD)
- **Txns**: `txns.{m5,h1,h6,h24}.{buys,sells}` (integers) — this is the
  buy/sell count field the M4 poller and safety heuristics should use.
  `priceChange.{m5,h1,h6,h24}` also present (numeric %, can be missing on
  buckets with zero volume).
  Caution: `priceChange` values can be enormous nonsense-looking numbers
  (e.g. `500822`) on illiquid/thin pairs — these are not "%" in a sane range;
  don't trust them blindly for gating without a sanity clamp.
- `fdv`, `marketCap` (numbers, USD)
- `pairCreatedAt` (epoch **milliseconds**) — absent on some pairs (a few BSC
  pairs in `search_sol.json` omit it).
- `info.{imageUrl,header,openGraph,websites[],socials[]}` — optional,
  present on established tokens, frequently a smaller subset (or entirely
  absent) on freshly-created/thin pairs.

### `profiles_latest.json` — top-level array (NOT wrapped in `pairs`/`data`)

Each element: `url`, `chainId`, `tokenAddress`, `icon`, `header`,
`openGraph`, `description` (optional — some entries omit it), `links[]`
(each `{type?, label?, url}` — `type` and `label` are mutually
optional/either-or, not both always present), `cto` (bool — "is this a
community-takeover project"), `updatedAt` (ISO-8601 string with
milliseconds, e.g. `"2026-07-03T15:04:36.207Z"`).

**No pagination** — this is a fixed-size "latest N" feed (30 items observed).
No `page`/`cursor`/`next` field anywhere in the response.

### `boosts_latest.json` — top-level array (also unwrapped)

Same `url`/`chainId`/`tokenAddress`/`icon`/`header`/`openGraph`/`description`/
`links[]` shape as profiles, plus **`totalAmount`** and **`amount`** (both
integers — boost-purchase amounts; `amount` can be less than `totalAmount`,
e.g. `{"totalAmount": 200, "amount": 50}`, meaning partial/staged boost
purchases). No `cto` field here. Also unpaginated ("latest N" feed, 30 items
observed here too); the same token can appear twice in one response (seen:
`9zZVV9wytrbCLK3iHyiszLht55fBKpAP6VQqxTzrpump` listed twice, identical
payload both times — treat as a possible at-least-once delivery quirk, not a
guaranteed-unique feed).

## Corrections made

None — all 4 target URLs in the task's `rest_targets.json` template resolved
first try with `200` and valid JSON.
