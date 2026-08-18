# GeckoTerminal REST capture notes

Captured with `scripts/recon/capture_rest.py geckoterminal`. Both targets from
the task's `rest_targets.json` worked exactly as given — **no URL correction
was needed**.

| fixture | endpoint | status |
|---|---|---|
| `trending_pools.json` | `GET /api/v2/networks/solana/trending_pools` | 200 |
| `token_bonk.json` | `GET /api/v2/networks/solana/tokens/{mint}` | 200 |

## Auth

None required/observed. No API key on either endpoint (this is GeckoTerminal's
free public API — distinct from the paid CoinGecko Pro API/keys).

## Rate limits

Both responses included an `x-request-id` header (e.g.
`"b53a141e-0cf6-4082-b986-b0a7d39f1211"`) but **no rate-limit-specific
headers** (`x-ratelimit-*`, `retry-after`, etc.) were present in either
capture. GeckoTerminal's publicly documented free-tier limit (per their docs
site, not independently re-verified against captured headers here since none
were returned) is commonly cited as **30 calls/minute**; this recon did not
run a burst test against GeckoTerminal (the task's burst-test step targets
DexScreener only), so treat the 30/min figure as a documented claim, not an
empirically confirmed one from this session.

## JSON paths adapters will read

Both responses use the **JSON:API-style envelope**: `body.data` (single
object or array of objects), each with `id`, `type`, `attributes{...}`, and
`relationships{...}`.

### `trending_pools.json` — `body.data[]` (array of `type: "pool"`)

- `id` (format `"solana_<pool_address>"`), `attributes.address` (bare pool
  address)
- **Price**: `attributes.base_token_price_usd` / `base_token_price_native_currency`
  (both **strings**, often absurdly long decimal expansions — parse as
  Decimal, not float, to avoid precision loss) and the mirrored
  `quote_token_price_usd` / `quote_token_price_native_currency`
- **Liquidity proxy**: `attributes.reserve_in_usd` (string) — this is the
  field to key on for a liquidity/safety gate, since there's no separate
  `liquidity.usd` object like DexScreener's.
- **Volume**: `attributes.volume_usd.{m5,m15,m30,h1,h6,h24}` (strings, USD)
- **Txns**: `attributes.transactions.{m5,m15,m30,h1,h6,h24}.{buys,sells,buyers,sellers}`
  (integers) — note GeckoTerminal additionally splits out **unique
  buyers/sellers**, not just buy/sell counts, which DexScreener doesn't
  provide.
- `attributes.price_change_percentage.{m5,m15,m30,h1,h6,h24}` (strings, %)
- `attributes.fdv_usd`, `attributes.market_cap_usd` (strings)
- `attributes.pool_created_at` (ISO-8601 string, e.g.
  `"2026-07-02T04:11:20Z"`) — unlike DexScreener's epoch-ms `pairCreatedAt`.
- `relationships.base_token.data.id` / `relationships.quote_token.data.id`
  (format `"solana_<mint_address>"` — must strip the `"solana_"` prefix to
  get the bare mint) and `relationships.dex.data.id` (e.g. `"pumpswap"`,
  bare DEX slug, no chain prefix).

### `token_bonk.json` — `body.data` (single `type: "token"` object)

- `attributes.address`, `attributes.name`, `attributes.symbol`,
  `attributes.decimals` (integer)
- `attributes.price_usd` (string), `attributes.fdv_usd` (string),
  `attributes.market_cap_usd` (string)
- `attributes.total_reserve_in_usd` (string) — token-level aggregate
  liquidity across all its pools (as opposed to `trending_pools`'
  per-pool `reserve_in_usd`)
- `attributes.volume_usd.h24` (string) — **only `h24` present** at the
  token-detail level, not the full `m5/m15/.../h24` breakdown that
  `trending_pools` gives per-pool.
- `attributes.total_supply` / `attributes.normalized_total_supply` (strings)
- `attributes.coingecko_coin_id` (string or absent — only populated for
  tokens CoinGecko has indexed; BONK has one, a fresh memecoin launch would
  not)
- `relationships.top_pools.data[]` — array of `{id: "solana_<pool_addr>", type: "pool"}`,
  a **direct link from token to its pool addresses** (3 pools for BONK in
  this capture) that can be used to fan out to per-pool detail without a
  search step.

## Pagination

None observed on either endpoint in this capture (no `links.next` /
`meta.page` envelope fields present in the response bodies) — both are
fixed-size "top N" lists/single-object lookups, consistent with
GeckoTerminal's general v2 API pagination convention living under
`page`/`links` keys that simply weren't present here because neither
endpoint variant used here is paginated.

## Corrections made

None — both target URLs resolved first try with `200` and valid JSON.
