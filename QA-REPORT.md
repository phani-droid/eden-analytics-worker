# QA report

Validated: 2026-07-15 Asia/Kolkata. No production deployment was performed.

## Result

Status: **PASS for pull-request review and controlled deployment**.

## Provenance

- CEO ZIP baseline Worker SHA-256: `2250affdf76f996d8e32ee772c248a0831d4aa7d11dd671efe6e8399e5e38139`.
- Final compatibility Worker SHA-256: `b2edb0bde3687f7f9833ae2c5c308e2d13832221e3e21a387a22c51b35107cb5`.
- CEO and packaged coordinator SHA-256: `d9aa4ce5ad33eb1000e33eb627745b75c7d5a6ee047c77368e1125fffcc7d0c0`.
- Byte comparison confirms the packaged coordinator is unchanged.
- Worker delta against the CEO baseline: 199 insertions and 28 deletions, confined to compatibility, delivery acknowledgement, destination ID handling, and release observability.

## Checks passed

- JavaScript syntax for Worker and coordinator.
- Full v5.56 Worker regression suite.
- New explicit regressions for `/collect/t`, `/collect/p`, `/collect/i`, fresh app bootstrap, legacy Webflow anonymous-owner bootstrap, exact `OS_intake_started`, synchronous Segment `503`, and Mixpanel-safe message IDs.
- ConversionCoordinator: `13/13` atomicity, reservation, revocation, corruption, concurrency, and migration-compatibility tests.
- Wrangler `4.110.0` dry-run.
- Bundle: `528.07 KiB` uncompressed and `105.31 KiB` gzip.
- Dry-run bindings: one Durable Object, three KV namespaces, the ad-click Queue, all production variables, and synchronous browser Segment delivery.
- `npm audit`: zero known vulnerabilities.
- GitHub Actions YAML parse.
- Secret-pattern scan found no embedded credential material; the only private-key text is parsing code for the encrypted runtime service-account secret.

## Production gates still required

- GitHub PR review and green Actions run.
- Cloudflare account, route, binding, Queue, migration, and secret-name verification.
- Exact live health verification after merge.
- Controlled Webflow, app, Segment, Mixpanel, `/server-collect`, Queue and DLQ smoke tests.
- Monitoring through a full business cycle.
- A recorded v5.56 migration-compatible rollback version.

## Scope limitation

This Worker repair cannot recover query parameters already stripped by `tryeden.com` before the request reaches `.eden.health`. It also cannot create the missing immutable HealthOS charge-bound attribution envelope identified in the upstream RCA.
