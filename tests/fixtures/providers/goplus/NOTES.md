# GoPlus REST capture notes

Captured with `scripts/recon/capture_rest.py goplus`. The single target from
the task's `rest_targets.json` worked exactly as given — **no URL correction
was needed**.

| fixture | endpoint | status |
|---|---|---|
| `token_security_bonk.json` | `GET /api/v1/solana/token_security?contract_addresses={mint}` | 200 |

## Auth

None required/observed. No API key — this is GoPlus's free public
token-security endpoint (GoPlus also offers a paid/keyed tier for higher
volume elsewhere in their API surface, not exercised here since this
endpoint answered fully without one).

## Rate limits

**No rate-limit headers of any kind were returned** (`"headers": {}` in the
fixture — the harness's header filter, which matches
`x-rate*`/`ratelimit*`/`retry*`/`x-request*`, found nothing). No burst test
was run against GoPlus (task's burst-test step targets DexScreener only).
GoPlus's public docs (docs.gopluslabs.io — a JS-rendered SPA; direct fetch
returned only high-level API-listing prose, no numeric rate-limit table) did
not yield a hard number during this recon. Treat GoPlus's keyless rate limit
as **unknown/undocumented from this session** — build conservative
backoff/retry into the M4 poller rather than assuming a specific ceiling.

## Response envelope

Top-level shape: `{code, message, result}` where `result` is an **object
keyed by lowercase-preserved contract address** (here,
`result["DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"]`, matching the
request's `contract_addresses` query param exactly) rather than an array —
important because the endpoint accepts **comma-separated multiple
addresses** in one call (not exercised in this single-mint capture, but the
per-address-keyed response shape implies multi-address batching is
supported and the adapter should key lookups by mint, not assume a single
fixed key or array index).

`code: 1` / `message: "ok"` observed for success; failure-code shape not
captured in this recon (would need an invalid/malformed address to trigger).

## Safety-gate-relevant field mapping

The other safety-gate provider (with RugCheck) — GoPlus's field names are
Solana-SPL-specific (`mintable`/`freezable`/etc. as GoPlus's own vocabulary,
distinct from RugCheck's raw `mintAuthority`/`freezeAuthority`). All paths
below are under `body.result["<mint>"]`:

- **Mint authority**: `mintable.{authority: [], status: "0"}` — GoPlus
  encodes this as a **status string `"0"`/`"1"`** (not a bool) plus an
  `authority` array (empty when status is `"0"`; presumably populated with
  the authority pubkey(s) when `status: "1"`). BONK shows `status: "0"`
  (mint authority renounced), consistent with RugCheck's `mintAuthority:
  null` finding for the same token — **the two providers agree here**,
  useful as a cross-check pattern for the M3 gate (flag disagreement between
  RugCheck and GoPlus as itself a signal worth surfacing).
- **Freeze authority**: `freezable.{authority: [], status: "0"}` — same
  status-string convention. BONK: `"0"` (not freezable), agreeing with
  RugCheck's `freezeAuthority: null`.
- **LP / liquidity-lock status**: GoPlus does **not** give a locked-%
  figure directly the way RugCheck does. Instead: `dex[]` — an array of
  per-pool entries `{dex_name, id (pool address), type ("Standard" |
  "Concentrated"), tvl (string, USD), price, fee_rate, burn_percent,
  lp_amount, open_time, day/week/month.{price_max,price_min,volume}}`.
  **`burn_percent`** (float or `null`) is the closest analogue to a
  lock/burn-based rug-safety signal here (e.g. one BONK Raydium pool shows
  `burn_percent: 61.85`, meaning ~62% of that pool's LP tokens were burned —
  a common rug-safety pattern is LP-burn instead of/in addition to
  time-locking). `lp_amount` is `null` on most pools in this capture (only
  populated, as a string, on the 3 "Standard"-type Raydium pools that also
  have nonzero `burn_percent`) — **`lp_amount`/`burn_percent` presence seems
  correlated with pool type** (`"Standard"` Raydium pools populate it,
  `"Concentrated"`/CLMM pools mostly don't in this capture) — do not assume
  every pool entry has a usable lock/burn figure. There is **no top-level
  aggregate LP-lock/burn percentage** — like RugCheck's full report, a real
  adapter must roll this up per-pool itself (or, as with RugCheck, prefer a
  provider that pre-aggregates — RugCheck's `/report/summary.lpLockedPct`
  is the simpler source for this specific figure).
- **Top holders**: `holders[]`, each `{account, balance, is_locked (0/1),
  locked_detail: [], percent (string, e.g. "0.0795" = 7.95%), tag, token_account}`.
  Only **10 holders returned** here (vs. RugCheck's `topHolders[]`, which
  actually returns **20** entries in `report_bonk.json`, not a matching
  top-10 — correction: an earlier pass of this file mistakenly said "also
  10"; verified by counting the array directly). Despite the different list
  length, the `percent` values for the addresses the two lists do share
  match RugCheck's `pct` values to within float precision, e.g. both show
  the top holder at ~7.95% — **cross-provider agreement confirmed for
  holder concentration on the addresses in common**, giving confidence in
  using either source, or both, for the M3 gate's holder-concentration
  check (just don't assume identical list lengths across providers). `is_locked`/`locked_detail[]` are GoPlus-specific (not present in
  RugCheck's holder shape) — all `is_locked: 0` for BONK's top 10 (i.e. none
  of the top holders have their tokens under a detected lock contract).
  Note this field is **separate from `lp_holders[]`** (LP-token holders,
  empty `[]` for BONK in this capture — not exercised/populated here).
- **Overall risk verdict fields**: **GoPlus's `token_security` endpoint does
  NOT return a single aggregate risk score or verdict field** (no
  `score`/`risk_level`/`is_rug` top-level key anywhere in this response) —
  unlike RugCheck's `score`/`score_normalised`/`risks[]`. GoPlus instead
  exposes **individual boolean-ish risk flags** the M3 gate must combine
  itself:
  - `mintable.status`, `freezable.status` (above)
  - `closable.{authority, status}` (`"0"` for BONK — can the mint account
    itself be closed)
  - `balance_mutable_authority.{authority, status}` (`"0"` — can someone
    else mutate holder balances directly, a more severe honeypot-style
    flag than transfer fees)
  - `default_account_state` (`"1"`) + `default_account_state_upgradable.
    {authority, status}` (`"0"`) — SPL Token-2022 default-frozen-account
    extension state; `"1"` here is *not* itself a red flag by GoPlus's own
    convention (it's the extension's default init state field, not
    "is frozen right now") — needs care in the M3 gate not to
    misinterpret this as "the token is frozen."
  - `transfer_fee: {}` (empty object = no transfer-fee/tax extension
    active) + `transfer_fee_upgradable.{authority, status}` (`"0"`)
  - `transfer_hook: []` (empty array = no Token-2022 transfer-hook
    extension attached) + `transfer_hook_upgradable.{authority, status}`
    (`"0"`) — a nonempty `transfer_hook` array would be a strong
    honeypot-risk flag (hooks can arbitrarily block/tax transfers), not
    observed here since BONK is a legacy SPL token, not Token-2022.
  - `non_transferable` (`"0"` string) — Token-2022 non-transferable
    extension flag.
  - `trusted_token` (`0`, integer not string here, unlike the other
    status fields — inconsistent typing to watch for in the adapter) —
    presumably GoPlus's own allowlist flag, `0` even for an established
    token like BONK, so **don't treat `trusted_token: 0` alone as a risk
    signal** — likely under-populated/not meaningfully set for most tokens.
  - `creators: []` (empty for BONK — presumably populated with
    deployer-wallet info for tokens where GoPlus can attribute a creator;
    not populated here despite RugCheck independently identifying a
    `creator` address for the same mint — **the two providers disagree /
    have different coverage on creator attribution**, so don't rely on
    GoPlus alone for creator-based checks; prefer RugCheck's `creator`
    field or combine both).
  - `holder_count` (string, `"999069"`) — note this is a **string**, and
    notably almost 3% lower than RugCheck's `totalHolders` (`1022234`,
    integer) for the *same mint at roughly the same time* — the two
    providers' holder-counting methodology differs meaningfully (likely
    RugCheck counts zero-balance/dust accounts GoPlus filters, or vice
    versa); **do not treat holder counts as directly comparable across
    providers** without normalizing methodology.

**Net for M3**: GoPlus and RugCheck should be combined (agree on
mint/freeze-authority-renounced state for a safe token; each surfaces some
fields the other doesn't — GoPlus's Token-2022 extension flags
(`transfer_hook`, `non_transferable`, `default_account_state`) vs.
RugCheck's aggregate score/insider-graph/launchpad-attribution) rather than
either treated as a complete standalone verdict.

## Corrections made

None — the target URL resolved first try with `200` and valid JSON.
