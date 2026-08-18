# Provider reference

This reference documents the behavior observed while building the provider adapters. Every
fact below is distilled from the public-data captures under
`tests/fixtures/providers/<provider>/NOTES.md` — read those for full detail,
raw examples, and byte-level field tables. This file is the summary + the
decisions that follow from it; it does not introduce anything not already
established there.

Config knobs derived from this recon are pinned in `config.toml` under
`[providers.*]` and `[pumpfun]`.

---

## PumpPortal

- **Endpoint**: `wss://pumpportal.fun/api/data` (WebSocket, JSON Lines).
- **Auth**: none for `subscribeNewToken` / `subscribeMigration` (free tier).
- **Rate limits**: no documented rps cap observed; empirical frame rate in a
  9-minute capture was ~23.5 frames/min (bursty, no silent minute, largest
  gap 42.7s — well under any reasonable liveness timeout).
- **Free-tier verdict + fallback**: **`subscribeTokenTrade` (and
  `subscribeAccountTrade`) require a funded API key (>= 0.02 SOL)** — the
  free/no-key connection gets an explicit ack-rejection instead of trade
  data: `"'subscribeTokenTrade' and 'subscribeAccountTrade' methods are only
  available when connecting with an API key funded with at least 0.02
  SOL."` Free tier covers **creates + migrations only**. Fallback for
  trade-level data: derive it from the Helius `logsSubscribe` +
  Anchor-event-decode path (see Helius section and the pump.fun section
  below) rather than from PumpPortal.
- **No-sequence-field finding**: no payload (across 208 captured frames)
  carries any sequence/slot/block-height/monotonic-order field. The only
  ordering signals available are (a) capture-side wall-clock arrival order,
  or (b) resolving the `signature`'s Solana slot via a separate RPC call.
  **Gap detection cannot be done from frame content alone** — needs an
  external heartbeat/liveness check or reconciliation against a secondary
  source if required.
- **Schema notes**:
  - `create` / `pool:"pump"` (197/208 frames observed): keys —
    `signature, mint, traderPublicKey, txType, initialBuy, solAmount,
    bondingCurveKey, vTokensInBondingCurve, vSolInBondingCurve,
    marketCapSol, is_mayhem_mode, pool`, plus optional/best-effort `name,
    symbol, uri` (absent on ~5% of frames — off-chain metadata not yet
    resolved at broadcast time; do not treat as guaranteed).
  - `create` / `pool:"bonk"` (n=1, provisional schema): structurally
    different — no `bondingCurveKey`/`vSolInBondingCurve`/
    `vTokensInBondingCurve`; instead `solInPool`, `tokensInPool`,
    `newTokenBalance`. Treat as low-confidence until corroborated by a
    longer capture.
  - `migrate` / `pool:"pump-amm"` (7/208 frames): minimal 4-field payload —
    `signature, mint, txType, pool` only. No curve-state or amounts;
    graduation price/liquidity must come from the last `create` state for
    that mint or a separate lookup.
  - Three one-off ack/info frames of shape `{"message": "..."}"` (protocol
    acks, not token/trade events).
- **Fixture paths**: `tests/fixtures/providers/pumpportal/session1.jsonl`,
  `tests/fixtures/providers/pumpportal/NOTES.md`.
- **Docs**: https://pumpportal.fun (data WS docs).

---

## Helius

- **Endpoints**: RPC (HTTPS, JSON-RPC) for `getAccountInfo`,
  `getTokenSupply`, `getTokenLargestAccounts`, etc.; `logsSubscribe`
  (WebSocket / LaserStream) for streaming program logs.
- **Auth**: API key in the RPC/WSS URL (env-only — never committed; sourced
  at capture time from `~/.memebot-recon.env`, scrub-verified clean of key
  material in every committed fixture and script).
- **Program id**: `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` — CONFIRMED
  (a 5-minute `logsSubscribe` capture returned 98,383 frames with direct
  `"Program 6EF8rr... invoke"` log lines; a wrong id would have produced
  zero frames).
- **Observed rate limits / volume**: free plan = 1,000,000 credits/month,
  10 RPC req/sec cap. Streaming (`logsSubscribe`) is billed **20 credits per
  MB transferred**, not per-message. A 5-minute full-program capture was
  **123 MB** (~328 frames/sec).
- **The 21x-over-free-tier math**: extrapolating 123 MB / 5 min to 24/7
  continuous streaming ≈ **1.06 TB/month ≈ ~21.25M credits/month — ~21x
  over the free 1M-credit budget.** The free tier's 1M credits only buys
  ~33.9 hours (~1.4 days) of continuous full-program streaming per month.
  **Burstiness caveat**: this extrapolates a single Friday/Saturday
  5-minute sample linearly; true monthly average bytes/sec could differ,
  but pump.fun is consistently one of the highest-traffic Solana programs,
  so traffic would need to be ~20x lower on average — essentially
  continuously — to bring 24/7 streaming back under budget. Not plausible;
  the 21x verdict direction is trusted, only its precision is approximate.
- **Free-tier verdict + fallback**: raw `logsSubscribe` firehose on the
  pump.fun program does **not** fit the free tier for always-on M2
  operation. Recommended approach, in order of preference:
  1. Client-side filtering only cuts parsing cost, **not** metered bytes
     (Helius bills on wire bytes before client filtering) — `mentions` is
     the only server-side filter `logsSubscribe` supports; no
     instruction-level filter exists.
  2. **Targeted subscribe / poll-degrade (recommended for M2, MB-4
     design)**: subscribe/poll per tracked mint rather than the whole
     program firehose, or fall back to periodic
     `getSignaturesForAddress` + `getTransaction` polling at a controlled,
     bounded-cost cadence; reserve full-program WS streaming for short
     bursty windows only.
  3. Paid Developer plan ($49/mo) — its credit allotment has not been
     checked against this math yet; flag for M2 costing.
- **Noise caveat**: ~90% of `logsSubscribe` frames carry a non-null `err`,
  but only ~0.7% of those are the pump.fun program's *own* invocation
  failing — the rest are unrelated MEV/arb-bot transactions that merely
  mention the program. A consumer must check for pump.fun's own
  invoke/success pair, not just presence-in-mentions, before treating a
  frame as a real curve event.
- **`getTokenLargestAccounts` flakiness**: returned HTTP 200 transport-level
  but a JSON-RPC-level error both times tested: `{"error": {"code":
  -32603, "message": "account index service overloaded, please try
  again."}}`. This is Helius's own account-index service being overloaded,
  not rate-limiting or auth. **Needs a retry-with-backoff wrapper**, and a
  design fallback (gate on mint/freeze authority + LP-lock heuristics alone
  if this call keeps failing) rather than blocking token admission on it.
- **RPC JSON paths** (mint/freeze/holders):
  - `getAccountInfo` (parsed, mint account):
    `body.result.value.data.parsed.info.mintAuthority` and
    `...freezeAuthority` (both `null` = renounced/safe for a wired-open
    reference token).
  - `getTokenSupply`: `body.result.value.uiAmountString`.
  - `getTokenLargestAccounts` (when it succeeds):
    `body.result.value[]` = `{address, amount, decimals, uiAmount}`,
    sorted descending — combine with `getTokenSupply` for a
    holder-concentration ratio.
- **Trade-data derivation**: instruction type / success / signature / slot
  are derivable directly from `logsSubscribe` log text with no extra call.
  Actual trade size, price, and mint address are **not** printed as plain
  text — they live in the base64 `Program data:` Anchor event blob, which
  is decodable **offline at zero extra RPC/credit cost** once the IDL
  layout is known (see pump.fun section). `getTransaction` per-signature is
  a correct but expensive fallback (~1 extra metered call per trade — at
  observed volume, ~3M+ calls/month if used for every trade) and should be
  reserved for spot-checks, not the primary path.
- **Fixture paths**:
  `tests/fixtures/providers/helius/logs_subscribe_pumpfun.jsonl` (curated
  147-line slice of a 123 MB/98,383-line raw capture — see NOTES.md for the
  sampling method), `get_account_info_mint.json`, `get_token_supply.json`,
  `get_token_largest_accounts.json`, `tests/fixtures/providers/helius/NOTES.md`.
- **Docs**: https://www.helius.dev/docs, https://www.helius.dev/pricing.

---

## DexScreener

- **Endpoints**: `GET /latest/dex/tokens/{mint}`, `GET
  /latest/dex/search?q={query}`, `GET /token-profiles/latest/v1`, `GET
  /token-boosts/latest/v1`.
- **Auth**: none.
- **Rate limits**: no rate-limit headers returned in any capture. Docs
  (https://docs.dexscreener.com/api/reference) cite **60 requests/minute**
  for `token-profiles`/"latest"/"trending"-style endpoints (not explicitly
  stated for pairs/tokens/search, but grouped under a similar general
  limit). Empirically burst-tested: **30 sequential requests, all 200s, no
  429** — consistent with (or under) the 60/min ceiling; this is a floor,
  not a proven ceiling.
- **Free-tier verdict + fallback**: fits the free tier comfortably for
  polling cadence at the 60 rpm documented ceiling; no fallback needed.
- **Schema notes**:
  - `token_bonk.json` / `search_sol.json`: `body.pairs[]`, pair/pool
    granularity (a token can span many pairs). Key fields:
    `chainId, dexId, pairAddress, url`,
    `baseToken/quoteToken.{address,name,symbol}`, price
    `priceNative`/`priceUsd` (strings), `liquidity.{usd,base,quote}`
    (**absent entirely on some low-activity pairs** — optional, not
    guaranteed), `volume.{m5,h1,h6,h24}`, `txns.{m5,h1,h6,h24}.{buys,sells}`,
    `priceChange.{...}` (can show nonsense values like `500822` on illiquid
    pairs — clamp before use), `fdv`, `marketCap`, `pairCreatedAt` (epoch
    **ms**, sometimes absent), `info.{imageUrl,...,websites[],socials[]}`
    (optional).
  - `profiles_latest.json`: top-level array (not wrapped), 30-item
    unpaginated "latest N" feed. `description` optional; `links[]` entries
    have optional `type`/`label`.
  - `boosts_latest.json`: top-level array, adds `totalAmount`/`amount`
    (integers); the same token can appear twice with identical payload
    (possible at-least-once delivery quirk).
- **Pagination**: none on any of the 4 endpoints.
- **Fixture paths**: `tests/fixtures/providers/dexscreener/{token_bonk,
  search_sol,profiles_latest,boosts_latest}.json`,
  `tests/fixtures/providers/dexscreener/NOTES.md`.
- **Docs**: https://docs.dexscreener.com/api/reference.

---

## GeckoTerminal

- **Endpoints**: `GET /api/v2/networks/solana/trending_pools`, `GET
  /api/v2/networks/solana/tokens/{mint}`.
- **Auth**: none (free public API, distinct from paid CoinGecko Pro).
- **Rate limits**: no rate-limit headers returned (only a generic
  `x-request-id`). Docs commonly cite **30 calls/minute** for the free
  tier — a documented claim, not empirically confirmed by headers in this
  session (no burst test run against GeckoTerminal).
- **Free-tier verdict + fallback**: treat the documented 30 rpm as the
  operating budget; no fallback needed at expected M2 polling cadence.
- **Schema notes** (JSON:API envelope, `body.data` + `attributes` +
  `relationships`):
  - `trending_pools.json`: `body.data[]` (`type: "pool"`), `id` =
    `"solana_<pool_address>"`. Price: `attributes.base_token_price_usd` /
    `base_token_price_native_currency` (**strings** — parse as Decimal, not
    float, to avoid precision loss). Liquidity proxy:
    `attributes.reserve_in_usd` (string) — the field to key a liquidity
    gate on (no separate `liquidity.usd` object like DexScreener).
    Volume: `attributes.volume_usd.{m5,m15,m30,h1,h6,h24}`. Txns:
    `attributes.transactions.{...}.{buys,sells,buyers,sellers}` — uniquely
    also splits out buyer/seller counts, not just buy/sell counts.
    `attributes.pool_created_at` is ISO-8601 (unlike DexScreener's epoch-ms).
    `relationships.base_token.data.id` / `quote_token.data.id` are
    `"solana_<mint>"` (strip the prefix); `relationships.dex.data.id` is a
    bare DEX slug.
  - `token_bonk.json`: `body.data` (single `type: "token"` object).
    `attributes.{address,name,symbol,decimals}`, `price_usd`, `fdv_usd`,
    `market_cap_usd` (strings), `total_reserve_in_usd` (token-level
    aggregate liquidity), `volume_usd.h24` (**only h24** at token-detail
    level), `total_supply`/`normalized_total_supply`, `coingecko_coin_id`
    (absent for tokens CoinGecko hasn't indexed — a fresh memecoin launch
    would lack this), `relationships.top_pools.data[]` — direct
    token-to-pool-address links for fan-out without a search step.
- **Pagination**: none on either endpoint.
- **Fixture paths**:
  `tests/fixtures/providers/geckoterminal/{trending_pools,token_bonk}.json`,
  `tests/fixtures/providers/geckoterminal/NOTES.md`.
- **Docs**: https://www.geckoterminal.com/dex-api.

---

## Jupiter

- **Host migration story**: the task's original template
  (`quote-api.jup.ag/v6/quote`, `api.jup.ag/price/v2` /
  `lite-api.jup.ag/price/v2`) is **dead**: `quote-api.jup.ag` no longer
  resolves at all (`ConnectError: No address associated with hostname`);
  `price/v2` on either host 404s. Confirmed current hosts/paths by
  directly probing candidate URLs with curl (the docs site,
  developers.jup.ag, is a JS-rendered SPA that 404s on direct fetch):
  **`lite-api.jup.ag/swap/v1/quote`** and **`lite-api.jup.ag/price/v3`**
  both return 200. `api.jup.ag/swap/v1/quote` (same path, paid host) also
  answers 200 keyless but is tightly rate-limited (see below).
  `scripts/recon/rest_targets.json` was updated to the corrected hosts.
- **Endpoints**: `GET lite-api.jup.ag/swap/v1/quote`, `GET
  lite-api.jup.ag/price/v3`.
- **Auth**: none required for `lite-api.jup.ag` (free/keyless tier — use
  this as the default host for M2/M4). `api.jup.ag` also answers keyless
  but is the "paid" host per Jupiter's current tiering.
- **Rate limits**: `lite-api.jup.ag` returns **no rate-limit headers at
  all** in any capture — go by community-reported guidance (~60 req/min
  per IP, not independently confirmed) and build in backoff/retry
  regardless. `api.jup.ag` **did** return headers on a single request:
  `x-ratelimit-remaining: 4`, i.e. a very tight keyless ceiling — **do not
  build the M4 poller against `api.jup.ag` without a key.**
- **Free-tier verdict + fallback**: use `lite-api.jup.ag` for both quote
  and price; no fallback needed since it's the intended free host. Avoid
  `api.jup.ag` unless a paid key is later provisioned.
- **Schema notes**:
  - Quote response (flat object, not wrapped): `inputMint, outputMint,
    inAmount, outAmount` (strings, base units), `otherAmountThreshold`,
    `swapMode` (`"ExactIn"` observed), `slippageBps`, **`priceImpactPct`**
    (string — **the key field for the M3 safety gate's slippage/liquidity
    signal**), `routePlan[]` (`swapInfo.{ammKey,label,inputMint,
    outputMint,inAmount,outAmount}`, `percent`), `contextSlot`,
    `timeTaken`, `swapUsdValue`,
    `mostReliableAmmsQuoteReport.info` (cross-AMM sanity check map). Route
    hop count and composition are amount/direction-dependent, not fixed
    per pair.
  - Price response (`price_v3`): object keyed by mint address (not an
    array) — `body["<mint>"] = {createdAt, liquidity, usdPrice, blockId,
    decimals, priceChange24h}`. `usdPrice` is the field to poll;
    `liquidity` is USD aggregate liquidity; `blockId` is shared across all
    mints in one request (the snapshot slot, not per-token); `createdAt`
    is when Jupiter's price engine first indexed the mint, not the token's
    actual on-chain creation time.
- **Pagination**: none — both endpoints are single-shot.
- **Fixture paths**:
  `tests/fixtures/providers/jupiter/{quote_sol_bonk,
  quote_bonk_sol_reverse,price_v3,quote_v1_paid_host_candidate}.json`,
  `tests/fixtures/providers/jupiter/NOTES.md`.
- **Docs**: https://dev.jup.ag (redirects to developers.jup.ag).

---

## RugCheck

- **Endpoints**: `GET /v1/tokens/{mint}/report`, `GET
  /v1/tokens/{mint}/report/summary`.
- **Auth**: none.
- **Rate limits**: **15 requests per window (hard, header-enforced)** —
  `x-rate-limit-limit: 15`, decrementing shared across both endpoints
  (15→14→13 across two sequential calls in one session). Window duration
  not stated by the header; treat as a short window (e.g. per-minute)
  until proven otherwise. **This is the tightest documented limit of the
  keyless providers captured.**
- **Free-tier verdict + fallback**: throttle to roughly 1 call per 4s
  sustained, well under 15/window with slack for jitter/retries.
  **Summary-vs-full strategy**: prefer `/report/summary` for routine
  per-cycle polling (small payload, pre-aggregated `lpLockedPct`); reserve
  the full `/report` for on-demand deep checks only, since its payload
  size scales with a token's pool count (BONK's full report was ~635 KB
  live, with 207 markets — a long-tail memecoin with 1-2 pools will be far
  smaller).
- **Schema notes** (all under top-level `body`):
  - Mint/freeze authority: `body.token.mintAuthority` /
    `body.token.freezeAuthority` (also duplicated at top level); `null` =
    renounced/safe.
  - LP lock: **per-market**, `body.markets[].lp.{lpLocked,lpLockedPct,
    lpLockedUSD,lpUnlocked,lpMaxSupply,lpTotalSupply}` — no single
    top-level aggregate in the full report (an adapter must roll this up,
    e.g. liquidity-weighted average). `/report/summary` instead exposes a
    single pre-aggregated top-level `lpLockedPct`.
  - Top holders: `body.topHolders[]` (20 entries observed for BONK), each
    `{address, amount, decimals, pct, uiAmount, uiAmountString, owner,
    insider}`. `pct` is the concentration figure to threshold; `insider`
    (bool, per-holder) is not synced 1:1 with the top-level
    `graphInsidersDetected` cluster count — don't assume they agree.
  - Risk verdict: `body.score` (raw, unbounded/additive) vs
    `body.score_normalised` (0-100 — **use this one**),
    `body.risks[]` = `{name, value, description, score, level}` (`level`
    is an open string enum — only `"warn"` observed in this low-risk
    sample, don't assume a fixed 2-value set), `body.rugged` (bool,
    confirmed-rug flag, distinct from the predictive `risks[]`).
  - Other useful fields: `body.totalMarketLiquidity` /
    `totalStableLiquidity` (pre-summed, spares per-pool summing),
    `body.totalHolders`, `body.totalLPProviders`,
    `body.transferFee.{pct,maxAmount,authority}` (nonzero `pct` = tax/honeypot
    flag), `body.graphInsidersDetected` + `body.insiderNetworks[]`
    (cluster-level, independent signal from per-holder `insider` — a high
    cluster count does not by itself imply a high risk score),
    `body.verification.{jup_verified,jup_strict}`, `body.launchpad`
    (`null` for pre-pump.fun tokens like BONK; presumably populated
    `"pump.fun"` for native launches).
- **Fixture note**: committed `report_bonk.json` is hand-trimmed from 635 KB
  to ~23 KB (207→3 representative `markets[]` entries covering 3 distinct
  lock/market-type shapes, 578→5 `knownAccounts`); aggregate fields
  (`totalMarketLiquidity`, `totalLPProviders`) reflect the FULL 207-market
  set and will not reconcile against summing the 3 retained entries —
  don't write a test asserting that reconciliation.
- **Fixture paths**: `tests/fixtures/providers/rugcheck/{report_bonk,
  summary_bonk}.json`, `tests/fixtures/providers/rugcheck/NOTES.md`.
- **Docs**: https://rugcheck.xyz (public API, undocumented formal spec at
  recon time — behavior characterized empirically here).

---

## GoPlus

- **Endpoint**: `GET
  /api/v1/solana/token_security?contract_addresses={mint}`.
- **Auth**: none (free public tier).
- **Rate limits**: no rate-limit headers of any kind returned; no hard
  number found in GoPlus's docs (JS-rendered SPA, no numeric table
  fetched). Treat as **unknown/undocumented** — build conservative
  backoff/retry rather than assuming a specific ceiling.
- **Free-tier verdict + fallback**: usable at conservative, undocumented
  budget; combine with RugCheck rather than relying on GoPlus alone (see
  below).
- **No aggregate verdict field**: unlike RugCheck's `score`/
  `score_normalised`/`risks[]`, GoPlus's `token_security` endpoint returns
  **no single risk score or verdict** — only individual boolean-ish flags
  the M3 gate must combine itself: `mintable.{authority,status}` /
  `freezable.{authority,status}` (status is a `"0"`/`"1"` **string**, not
  bool), `closable`, `balance_mutable_authority`,
  `default_account_state`/`default_account_state_upgradable` (Token-2022
  extension state — `"1"` here is the extension's default init state, not
  "currently frozen" — don't misinterpret), `transfer_fee`/
  `transfer_fee_upgradable`, `transfer_hook`/`transfer_hook_upgradable`
  (nonempty `transfer_hook[]` = strong honeypot-risk flag),
  `non_transferable`, `trusted_token` (integer, inconsistent typing vs the
  string-status fields — don't treat `0` alone as a risk signal, appears
  under-populated), `creators[]` (empty for BONK despite RugCheck
  independently attributing a creator — coverage differs, don't rely on
  GoPlus alone for creator checks), `holder_count` (string; ~3% lower than
  RugCheck's `totalHolders` for the same mint at the same time — holder
  counts are **not directly comparable across providers** without
  normalizing methodology).
- **M3 combination note**: GoPlus and RugCheck **agree** on
  mint/freeze-authority-renounced state (both `null`/`"0"` for BONK) —
  useful as a cross-check. Each surfaces fields the other doesn't:
  GoPlus's Token-2022 extension flags vs RugCheck's aggregate
  score/insider-graph/launchpad attribution. **M3 combines both rather
  than treating either as a complete standalone verdict.**
- **Schema notes** (under `body.result["<mint>"]`, response envelope
  `{code, message, result}`, `result` keyed by contract address — supports
  comma-separated multi-address batching): LP/lock via `dex[]` per-pool
  array (`burn_percent` is the closest lock/burn-based safety signal,
  correlated with pool type — populated mainly on "Standard" Raydium
  pools, mostly absent on Concentrated/CLMM); top holders via `holders[]`
  (only 10 returned vs RugCheck's 20, but `percent` values agree on shared
  addresses — cross-provider agreement confirmed for holder
  concentration).
- **Fixture paths**:
  `tests/fixtures/providers/goplus/token_security_bonk.json`,
  `tests/fixtures/providers/goplus/NOTES.md`.
- **Docs**: https://docs.gopluslabs.io.

---

## RSS (regime signal)

- **Feeds** (3): CoinDesk `https://www.coindesk.com/arc/outboundfeeds/rss`,
  The Block `https://www.theblock.co/rss.xml`, Cointelegraph
  `https://cointelegraph.com/rss`.
- **Auth**: none — plain unauthenticated GET, no key/UA/cookie requirement
  on any of the three.
- **Cadence**: standard RSS 2.0. CoinDesk `<ttl>5</ttl>` / hourly
  `sy:updateFrequency`; The Block `<ttl>8</ttl>` / hourly, also exposes
  `<atom:link rel="next">` pagination; Cointelegraph no `<ttl>`, hourly
  `sy:updatePeriod`, densest of the three (items <30 min apart during
  active periods). None enforced a rate limit in this recon pass (fetched
  once each). `config.toml` pins a 15-minute poll cadence.
- **The Block -0400 gotcha**: CoinDesk and Cointelegraph publish `pubDate`
  in UTC (`+0000`); **The Block publishes in native US Eastern offset
  (`-0400`)** — a regime-detection consumer must parse the offset
  per-item, not assume UTC across all three feeds.
- **Schema notes**: `title` (CDATA-wrapped on CoinDesk/Cointelegraph, not
  always on The Block), `description` (Cointelegraph embeds a `<p>` +
  inline image before the real summary — needs HTML-stripping before
  keyword matching; The Block's `description`/`content:encoded` are
  duplicates), `pubDate` (RFC-822), `category` (present on all three, a
  coarse pre-filter), `guid` (stable dedup key on all three — prefer this
  over `pubDate` for "new since last poll", especially on Cointelegraph
  which also has `<atom:updated>` showing silent post-publish edits).
- **Correction needed**: CoinDesk's URL with a trailing slash
  (`.../rss/`) 308-redirects to the same path without it;
  `capture_rest.py` doesn't follow redirects by default, so
  `rest_targets.json` was fixed to the no-trailing-slash form.
- **Fixture paths**: `tests/fixtures/providers/rss/NOTES.md` (raw XML not
  committed as separate fixtures beyond what the REST-capture harness
  stored — see the harness output for this provider).
- **Docs**: each site's public RSS feed; no formal API docs (RSS is the
  interface).

---

## CryptoPanic — DISCONTINUED

**Dropped from the design.** CryptoPanic discontinued its free Developer
API plan (removal date 2026-04-01, confirmed by the project owner
2026-07-04). No live capture was attempted — there is no free tier left to
recon against.

**Decision record**: `tests/fixtures/providers/cryptopanic/NOTES.md` (this
directory intentionally holds no fixtures — it documents the decision so a
future pass doesn't re-discover this).

**Replacement regime inputs** (both already free and already captured
elsewhere in this recon arc):
1. **Jupiter SOL-price drawdown** — a rolling SOL price drawdown as a
   risk-off proxy (see Jupiter section above).
2. **RSS panic keywords** — keyword scan over CoinDesk/The Block/
   Cointelegraph titles/descriptions (see RSS section above).

Revisit only if this combination proves materially insufficient in
practice **and** the cost is justified against live P&L.

---

## pump.fun mechanics

Derived from data already captured in the PumpPortal and Helius passes
(`tests/fixtures/providers/pumpportal/session1.jsonl`,
`tests/fixtures/providers/helius/logs_subscribe_pumpfun.jsonl`, a 122 MB
uncommitted backup sampled for extra confidence), plus the official
first-party pump.fun Anchor IDL
(`https://github.com/pump-fun/pump-public-docs/blob/main/idl/pump.json`,
fetched during recon, not vendored — re-fetch if re-validation is ever
needed). Full derivation and field tables:
`tests/fixtures/providers/pumpfun/NOTES.md`.

- **Program id**: `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` (confirmed
  independently by both PumpPortal frame content and Helius log lines).
- **PumpSwap (graduation destination) program id**:
  `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA` — invoked 157x in the
  Helius 5-minute/123 MB capture; graduated curves migrate to this
  pump.fun-operated AMM, not Raydium/Orca.
- **Initial virtual reserves (fixed constant)**: 30 SOL / 1,073,000,000
  tokens — observed exact across all 197 PumpPortal `create` frames and
  independently corroborated by all 19 Helius `CreateEvent` decodes
  (`virtual_sol_reserves = 30_000_000_000` lamports,
  `virtual_token_reserves = 1_073_000_000_000_000` raw units, same 30/1.073B
  ratio once 9/6-decimal scaling is divided out).
- **x·y=k invariant**: `k = 30 * 1,073,000,000 = 32,190,000,000`. Checked
  across all 197 `pump`-pool create frames: **max relative deviation
  ~1.2e-16** (IEEE-754 double noise; exact for all practical purposes) — a
  textbook constant-product AMM on *virtual* reserves (distinct from the
  smaller *real* reserves actually transferred at migration).
- **Fees**: 95 bps protocol fee + 30 bps creator fee = 1.25% total,
  observed matching pump.fun's own current docs
  (https://pump.fun/docs/fees, fetched this session) exactly on
  standard-curve trades — **90/93 fixture TradeEvents** show this; the
  other 3 (on 2 mints) showed 0/0 bps, a non-standard mayhem/micro-scale
  curve variant (also showed a much smaller ~0.09 SOL total virtual-reserve
  scale rather than the ~30-85 SOL "classic" curve) — nonstandard mints
  exist and should not be treated as a docs mismatch.
- **Migration fee**: 0.015 SOL (docs only — https://pump.fun/docs/fees,
  fetched this session; not independently observed in a captured
  graduation sequence). Note: some stale third-party sources online cite
  an outdated "~6 SOL" figure — pump.fun's own current docs page states
  0.015 SOL and that's the value used here.
- **Graduation threshold**: **~85 SOL raised / ~$69K market cap
  (COMMUNITY-SOURCED, NOT independently confirmed on-chain in this pass)**
  — pump.fun's own `/docs/fees` page does not state the SOL threshold
  explicitly; multiple 2026 community articles cross-checked converge on
  ~84-86 SOL (fluctuates with SOL price). No mint in the 5-minute sample
  graduated on the standard curve (max observed: ~38.3 SOL raised on the
  furthest-progressed standard-scale mint), so this figure could not be
  confirmed directly against on-chain data this pass.
- **Real (curve-sellable) supply**: **793,100,000 tokens exactly**
  (observed via `CreateEvent.real_token_reserves`, identical across all 19
  decoded creates) — 79.31% of the 1B total supply. **Observed wins** over
  commonly-cited-but-imprecise community figures of "~800M".
- **Anchor event decode** (`scripts/recon/decode_pumpfun_events.py`):
  stdlib-only (base64/struct/hashlib, no anchorpy/borsh dependency,
  includes an inline base58 encoder). Discriminator = first 8 bytes of
  `sha256(f"event:{EventName}")` — confirmed two independent ways (IDL's
  own `discriminator` array vs recomputing from scratch; both agree
  byte-for-byte for every event checked, e.g. `TradeEvent =
  bddb7fd34ee661ee`, `CreateEvent = 1b72a94ddeeb6376`,
  `CompleteEvent = 5f72619cd42e9808`). The decoder reads the **16-field
  stable prefix** of `TradeEvent` (`mint` through `creator_fee`) and the
  full 12-field `CreateEvent`, which fully covers price/size/direction/
  reserves/fees for M2's core use case. Validation: 93/93 TradeEvents +
  19/19 CreateEvents decoded (100%) on the committed 147-line fixture;
  9,973/9,974 TradeEvents (99.99%) + 117/117 CreateEvents (100%) on the
  full 122 MB/98,383-line sample (the one gap being an unrelated program's
  same-length blob, not a decoder bug).
  - **Trailing-fields caveat**: the *current* on-chain `TradeEvent` IDL has
    many more fields after `creator_fee` (`track_volume`, cashback/buyback
    fields, `quote_mint`, `virtual_quote_reserves`, etc. — reflecting
    mayhem mode, cashback, non-SOL quote mints, creator fee-sharing added
    after our capture). The decoder reads only the stable prefix; trailing
    bytes are counted (`trailing_raw_len`) but not parsed. **Only
    appending new trailing fields is safe for this decoder** — if
    pump.fun ever changes the *early* field order, the prefix decode
    would break; extend past `creator_fee` using the current IDL's field
    order if newer fields (e.g. `virtual_quote_reserves` for non-SOL
    quotes) are ever needed.
