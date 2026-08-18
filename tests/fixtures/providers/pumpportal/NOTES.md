# PumpPortal data-WS capture notes (session1.jsonl)

Captured with `scripts/recon/capture_pumpportal.py --minutes 9 --out
tests/fixtures/providers/pumpportal/session1.jsonl`, run to completion for the
full 9 minutes (real elapsed span between first and last frame: 530.6s ≈ 8m51s;
the remaining ~9s is connection setup/subscribe overhead before the first
frame arrived).

- Endpoint `wss://pumpportal.fun/api/data` and the `subscribeNewToken` /
  `subscribeMigration` / `subscribeTokenTrade` method names all worked exactly
  as specified in the task — **no URI or method-name correction was needed.**
- Result: **208 frames**, all valid JSON Lines (`{"t_wall": <float>, "raw":
  "<json string>"}`), 205 with a `signature` field and all 205 signatures
  distinct (no duplicate deliveries observed in this session).
- File size: ~141 KB (well under the 5 MB "large fixture" threshold).

## Message kinds observed

Three real message kinds, plus 3 one-off server ack/info messages (each of
shape `{"message": "..."}`, no other fields — these are protocol
acknowledgements, not token/trade events):

| kind | count | notes |
|---|---:|---|
| `txType":"create"`, `"pool":"pump"` | 197 | pump.fun bonding-curve mint |
| `txType":"create"`, `"pool":"bonk"` | 1 | pump.fun's Bonk/AMM-style launchpad variant — different field set (see below) |
| `txType":"migrate"`, `"pool":"pump-amm"` | 7 | bonding curve graduated to the AMM |
| ack `"Successfully subscribed to token creation events."` | 1 | reply to `subscribeNewToken` |
| ack `"Subscribed to 'migration' events."` | 1 | reply to `subscribeMigration` |
| ack `"'subscribeTokenTrade' and 'subscribeAccountTrade' methods are only available when connecting with an API key funded with at least 0.02 SOL."` | 1 | reply to our `subscribeTokenTrade` call — see "Trade subscription" finding below |

**No trade-event frames were captured in this session** — see finding below.

### `create` on `pool:"pump"` (verbatim example, line 3)

```json
{"signature":"2C8bmmyiDGs3Xnv1fQotJVa4BbYPAqQnWgo1dukC5fCxymZxVFS9AWCTmWaL2ve785stys7APDmCUxG17r51Cda1","mint":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v","traderPublicKey":"AU8pNovgxTCvgGSEntRXe3L3Fk22fRTDFy7nZBkpJGix","txType":"create","initialBuy":0,"solAmount":0,"bondingCurveKey":"BXASjpmw24T7oPwWztENXJ9ojVp3iP1vbiCtVbquJRdZ","vTokensInBondingCurve":1073000000,"vSolInBondingCurve":30,"marketCapSol":27.958993476234856,"is_mayhem_mode":false,"pool":"pump"}
```

A more typical (non-zero-activity) example was also observed (in a prior
smoke-test run, NOT present in session1.jsonl) and includes `name`, `symbol`,
`uri` as well — the full field set for this kind (union across 187/197 frames
of session1.jsonl itself) is:

`bondingCurveKey, initialBuy, is_mayhem_mode, marketCapSol, mint, name, pool,
signature, solAmount, symbol, traderPublicKey, txType, uri,
vSolInBondingCurve, vTokensInBondingCurve`

10 of the 197 `pump`-create frames (e.g. line 3 above) are missing
`name`/`symbol`/`uri` and all share `initialBuy: 0`, `solAmount: 0`,
`vSolInBondingCurve: 30` (the curve's fresh/untouched initial state) and an
identical `marketCapSol: 27.958993476234856` — i.e. these look like
just-created mints whose off-chain metadata (name/symbol/uri, presumably
fetched by PumpPortal from the token's metadata URI after creation) hadn't
resolved yet at broadcast time, rather than a different event kind. Treat
`name`/`symbol`/`uri` as **optional/best-effort** fields in the M2 adapter
schema, not guaranteed.

### `create` on `pool:"bonk"` (verbatim example, the only one seen, line 102)

```json
{"signature":"2smh7KZFp58k1DyQq67Fraep1MsSyiry7Q66EHALpZV1Fg34c1MvYp5SuXLozfzBF8r8d1roo9XgZyS9zrgZc4Z","traderPublicKey":"BWwDyKhHSJ8h3YNVws8mr6Ek3kP4orSstuLTSLcbgV9t","txType":"create","mint":"7VzfCJYiiQTVCeLumVrxQ8z1tGuFcQ75z5GMXQR2surg","solInPool":0.89055,"tokensInPool":967574707.778913,"initialBuy":32425292.22108698,"solAmount":0.89055,"newTokenBalance":32425292.221087,"marketCapSol":29.72713613571716,"name":"Blume","symbol":"BLUME","uri":"https://ipfs.io/ipfs/QmVGxiWSGprdNoUBghjurgb3Hxm1UAU6ZJTHnhzidT6Rc7","pool":"bonk"}
```

This is a **structurally different schema** from the `pool:"pump"` create: no
`bondingCurveKey` / `vSolInBondingCurve` / `vTokensInBondingCurve`; instead it
has `solInPool`, `tokensInPool`, `newTokenBalance`. `is_mayhem_mode` is also
absent. Only one such frame was seen in this session (n=1, so treat this
schema as provisional/low-confidence until corroborated by a longer capture —
Task A7/PROVIDERS.md should flag it as such).

### `migrate` on `pool:"pump-amm"` (verbatim example, line 4)

```json
{"signature":"52MoTgakx7Jdup1kTpZncNy99YmHLxTNEoS4FeZE8fQ3SKtHgWhsS9iYhvW5pKPDhMKkGAjvBGkzEqdETWN15inJ","mint":"BzqYzBGydDrwjnYVtWfupr8GFoSxpCQdTkjRppBKpump","txType":"migrate","pool":"pump-amm"}
```

Minimal payload: only `signature`, `mint`, `txType`, `pool` — no curve-state
or SOL/token amounts at all. All 7 migrate frames observed have exactly this
4-field shape. This means migration events alone don't tell you the
graduation price/liquidity — that has to be inferred from the last `create`
frame(s) for that `mint`, or fetched separately.

## Field identification (what to key on)

- **mint**: `mint` (base58 SPL mint address) — present on every non-ack
  frame, consistent key across create/migrate/(would-be trade) events.
- **creator/trader**: `traderPublicKey` — present on both `pump` and `bonk`
  create frames, absent on `migrate` frames.
- **SOL amount**: `solAmount` (pump create), also `solInPool` on the bonk
  variant. `initialBuy` is the token-amount equivalent of the creator's first
  buy (see below), not a SOL figure.
- **Token amount**: `initialBuy` (raw token units of the creator's opening
  buy) on both pump/bonk creates; `newTokenBalance` on the bonk variant looks
  like the trader's resulting token balance post-buy (equal to `initialBuy`
  in the one bonk example captured, so its exact semantics vs `initialBuy`
  are not fully disambiguated from n=1).
- **Curve state**: `vSolInBondingCurve` and `vTokensInBondingCurve` (virtual
  SOL/token reserves) plus `bondingCurveKey` (the curve account address) on
  the `pump`-pool create schema. The `bonk`-pool variant uses `solInPool` /
  `tokensInPool` instead — names differ between the two pool kinds.
- **Market cap**: `marketCapSol` present on both create variants (denominated
  in SOL, not USD).
- **Transaction signature**: `signature` (Solana tx signature, base58) is the
  natural per-event unique ID — verified unique across all 205 data frames in
  this session.

## Sequence / ordering field

**No sequence, slot, block-height, or monotonic order field of any kind
exists in any payload observed** (full field-name union across all 208
frames: `bondingCurveKey, initialBuy, is_mayhem_mode, marketCapSol, message,
mint, name, newTokenBalance, pool, signature, solAmount, solInPool, symbol,
tokensInPool, traderPublicKey, txType, uri, vSolInBondingCurve,
vTokensInBondingCurve`). This matters for M2 gap detection: the only ordering
signal available is (a) our own capture-time arrival order / `t_wall`
wall-clock timestamp (recorder-side, not provider-side), and (b) the
`signature`'s implied Solana slot if independently resolved via an RPC call
(not present in the frame itself). **The M2 stream adapter cannot detect
missed/dropped frames from frame content alone** — no provider-side
sequence number to diff against. If gap detection is required, it will need
either an external heartbeat/liveness check or reconciliation against a
secondary source (e.g. periodic REST poll or Solana RPC).

## Frame rate

208 frames over 530.6s ≈ **23.5 frames/minute** (~0.39 frames/sec) during
this session, across `subscribeNewToken` + `subscribeMigration` only (trade
subscription never actually activated — see below). Per-60s-bucket
breakdown (bucket 0 = first 60s after the first frame):

```
minute 0: 25 frames
minute 1: 29 frames
minute 2: 15 frames
minute 3: 33 frames
minute 4: 24 frames
minute 5: 15 frames
minute 6: 25 frames
minute 7: 17 frames
minute 8: 25 frames
```

Bursty but no minute was silent; largest inter-frame gap observed was 42.7s
in an earlier 82-second aborted test run and ~20s in the full 9-minute run —
well under the script's 30s WARN threshold, so no WARN was ever printed.

## Keepalive / ping behavior

No application-level ping/keepalive JSON messages were observed in the
captured frames — every non-ack frame is a real create/migrate event. The
`websockets` library (v16.0, used here) handles WebSocket protocol-level
ping/pong frames transparently under the hood; those are not delivered to
`ws.recv()` as text and therefore would never appear in this JSONL capture
regardless of whether the server sends them. This capture cannot confirm or
rule out protocol-level pings either way — that would require enabling
`websockets` debug logging or a packet capture, which was out of scope here.
The connection did not drop or need reconnection during the full 9-minute
session.

## Trade subscription — could not be captured (free tier limitation)

The task plan called for subscribing to `subscribeTokenTrade` after seeing 5
new mints, to capture the curve-trade schema. That subscription *was* sent
(5 distinct mints were collected and the `subscribeTokenTrade` message was
sent per the script logic), but PumpPortal replied with an explicit ack
frame instead of trade data:

```json
{"message":"'subscribeTokenTrade' and 'subscribeAccountTrade' methods are only available when connecting with an API key funded with at least 0.02 SOL."}
```

**Finding: trade-event capture requires a funded PumpPortal API key (>= 0.02
SOL) — it is not available on the free/no-key data websocket**, contrary to
the task's framing that the free data API needs no key for this purpose. The
free tier covers `subscribeNewToken` and `subscribeMigration` only (both
worked with no key). This is a blocker for capturing the curve-trade schema
via this exact method; getting real trade frames will require either (a) a
funded PumpPortal API key, or (b) deriving trade/curve-update schema
indirectly from consecutive `create`/on-chain data, or (c) an alternative
provider. Flag this for Task A6 (bonding-curve mechanics) and Task A7
(`docs/PROVIDERS.md`) — the curve math will need to be derived from
`vSolInBondingCurve`/`vTokensInBondingCurve` deltas across `create` events
and/or a different data source, not from `subscribeTokenTrade` frames, unless
a funded key becomes available.

## Corrections made to the script

None. `URI = "wss://pumpportal.fun/api/data"`, `subscribeNewToken`,
`subscribeMigration`, and `subscribeTokenTrade` (method name and `keys`
payload shape) all matched the live server's behavior exactly as given in
the task template — the only surprise was the *server-side entitlement gate*
on `subscribeTokenTrade`, not the method name or URI.
