# RSS regime feeds recon (MB-2 / A5, reshaped)

Captured 2026-07-04 via `scripts/recon/capture_rest.py rss` (added `rss` provider
to `scripts/recon/rest_targets.json`, reusing the existing REST-capture harness —
non-JSON responses fall into `body` as raw text, which is fine for XML/RSS).

## Feeds chosen

| feed | url | status |
|---|---|---|
| CoinDesk | `https://www.coindesk.com/arc/outboundfeeds/rss` | 200 |
| The Block | `https://www.theblock.co/rss.xml` | 200 |
| Cointelegraph | `https://cointelegraph.com/rss` | 200 |

### Correction needed

CoinDesk's URL as originally specified (`.../rss/` with a trailing slash) returns
**HTTP 308** (permanent redirect) to the same path *without* the trailing slash.
`httpx` does not follow redirects by default in `capture_rest.py`'s plain
`client.get(url)` call, so the first capture attempt landed a 308 with body
`"Redirecting...\n"` and empty captured headers (no `Location` — the header
allowlist in `capture_rest.py` only keeps rate-limit-shaped headers, so a
redirect's `Location` wasn't visible in the fixture; found the target manually
with `follow_redirects=True`). Fixed by dropping the trailing slash in
`rest_targets.json`; re-ran and got a clean 200. The Block and Cointelegraph
worked on the first try with their URLs as specified.

## Fields available for panic-keyword scanning

All three are standard RSS 2.0. Usable fields per `<item>`:

- `title` — plain text (CoinDesk/Cointelegraph wrap it in `<![CDATA[...]]>`,
  The Block does not always).
- `description` — short summary, HTML-escaped or CDATA-wrapped; Cointelegraph's
  description embeds a full `<p>` with an inline image tag before the actual
  summary text (needs HTML-stripping before keyword matching).
  The Block's description/`content:encoded` are effectively identical (same
  CDATA blurb duplicated in both tags).
- `pubDate` — RFC-822 format, e.g. `Sat, 04 Jul 2026 11:25:34 +0000` (CoinDesk),
  `Fri, 03 Jul 2026 13:55:56 -0400` (The Block, native -0400 offset, not UTC),
  `Sat, 04 Jul 2026 11:25:34 +0000` (Cointelegraph). **Note the offset
  inconsistency** — The Block publishes in US Eastern offset while CoinDesk and
  Cointelegraph publish in UTC; any regime-detection consumer must parse the
  offset, not assume UTC.
- `category` — present on all three, useful as a coarse pre-filter (e.g.
  CoinDesk/Cointelegraph tag `Markets`/`Policy`/`Latest News`).
- Cointelegraph additionally has `<atom:updated>`, distinct from `<pubDate>` —
  shows articles get silently re-edited after publish (timestamps differ by
  minutes to hours in the captured snapshot); if dedup logic keys off `guid`
  only (present and stable on all three, `isPermaLink="false"` on CoinDesk/
  The Block, `="true"` on Cointelegraph) this is not a problem, but don't key
  a "new since last poll" check off `pubDate` alone on Cointelegraph.

## Update cadence observed (single-snapshot inference from item timestamps)

- **CoinDesk**: `<ttl>5</ttl>`, `sy:updateFrequency` hourly. Item-to-item gaps in
  the captured feed range from ~10 minutes (during an active news window) up to
  several hours overnight; ~20 items span roughly 36 hours.
- **The Block**: `<ttl>8</ttl>`, hourly `sy:updateFrequency`; also exposes
  pagination via `<atom:link rel="next">`. ~20 items span roughly 29 hours,
  similar bursty-then-quiet cadence.
- **Cointelegraph**: no `<ttl>`, hourly `sy:updatePeriod`. Densest feed of the
  three — ~24 items span roughly 32 hours, several under 30 minutes apart during
  active periods (matches its higher publishing volume generally).

None of the three enforced a `ttl`-based rate limit on this recon pass (fetched
once each, well under any reasonable poll cadence); no 429s seen.

## Auth / UA quirks

None. All three served a plain, unauthenticated `GET` over `httpx`'s default
user agent with no 403/406/429 — no API key, no custom User-Agent header, no
cookie/session requirement observed. The only quirk was CoinDesk's redirect
(see Correction above), not an auth issue.

## Key-scrub

No credentials involved in this capture (public RSS, no auth). N/A for the
scrub check.
