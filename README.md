# Eden Analytics Worker v5.56 — production repository

Release revision: `v556-phani-all-events-delivery-20260715`.

This package adds synchronous acknowledgement for both browser and authenticated server Segment delivery. See `EVENT-DELIVERY-RCA.md` for the exact Worker changes and the separate application producer gaps that cannot be repaired at the edge.

This folder is a complete replacement for the old `phani-droid/eden-analytics-worker` repository contents. A pull request runs syntax checks, the full Worker regression suite, the Durable Object suite, and a Wrangler dry-run. Merging the PR into `main` deploys one atomic version of `eden-analytics` to Cloudflare and verifies the exact release through the live health endpoint.

## Source and compatibility

- Production baseline: CEO ZIP `eden-marketing-architecture-implementation-main (1).zip`, Worker SHA-256 `2250affdf76f996d8e32ee772c248a0831d4aa7d11dd671efe6e8399e5e38139`.
- `eden-conversion-coordinator.js` is copied unchanged from that ZIP.
- Pipeline remains `5.56`; enrichment remains `5.54`.
- Existing attribution, privacy, first-touch, ad-click pointer, Queue, KV, server-authority, conversion-idempotency, and `OS_*` rules are retained.

Compatibility additions:

1. Accept AnalyticsBrowser aliases `/collect/t`, `/collect/p`, `/collect/s`, `/collect/i`, `/collect/a`, `/collect/g`, and defensive `/collect/v1/*` aliases through the same security gates.
2. Allow one safe collector-first bootstrap for an allowlisted same-site browser with no owner state. The Worker, not the request body, creates the anonymous ID, session, and signed capability.
3. Preserve the Webflow pattern where `eden_anon_id` may exist before `eden_session_id`.
4. Use synchronous browser-to-Segment delivery in production. Segment failure returns a retryable `503` rather than a false-success `200`.
5. Send a deterministic `m-<32 hex>` top-level Segment `messageId`, suitable for Mixpanel `$insert_id`, while retaining the original producer/coordinator ID in `properties.segment_source_message_id`.
6. Synchronously acknowledge authenticated non-conversion server events and expose Segment failures as retryable `503` responses.
7. Deliver identity-less authenticated operational telemetry through an isolated event-scoped ID without making it an attribution or person key.

## Repository layout

| Path | Purpose |
| --- | --- |
| `cloudflare-workers/eden-analytics.js` | v5.56 Worker plus bounded compatibility fixes |
| `cloudflare-workers/eden-conversion-coordinator.js` | unchanged CEO Durable Object implementation |
| `wrangler.jsonc` | complete routes, KV, Durable Object migration, Queue, variables, and observability configuration |
| `scripts/test-eden-analytics-worker-local.mjs` | full Worker regression suite with Mixpanel-safe ID assertions |
| `scripts/test-eden-conversion-coordinator-v556-local.mjs` | strongly consistent coordinator suite |
| `.github/workflows/deploy.yml` | PR validation and `main` auto-deployment |
| `DEPLOYMENT-GUIDE.md` | beginner-safe setup, PR, deployment, smoke-test, and rollback steps |

## Local validation

```bash
npm ci
npm test
npm run dry-run
```

## Required GitHub Actions secrets

- `CLOUDFLARE_API_TOKEN`
- `CLOUDFLARE_ACCOUNT_ID`

## Required Cloudflare Worker secrets

These belong on the existing `eden-analytics` Worker and must never be committed:

- `SEGMENT_WRITE_KEY`
- `SERVER_API_SECRET`
- `BROWSER_CAP_HMAC_SECRET`
- `PRIVACY_LEDGER_HMAC_SECRET`
- `AD_CLICK_GOOGLE_SERVICE_ACCOUNT_KEY`

## Browser/application contract

No Webflow or app change is required for the supplied scripts if all of the following remain true:

- Webflow sends browser events to `https://collect.eden.health/collect` with `credentials: "include"`.
- The app AnalyticsBrowser `apiHost` remains `https://app.eden.health/collect`; its method suffixes are handled by this Worker.
- Authoritative purchase/payment/order outcomes continue through authenticated `/server-collect`, not browser authority.
- App server requests retain `X-Eden-Server-Secret` and stable transaction/order identifiers.

This release fixes collection and destination compatibility. It does not fix query parameters stripped by `tryeden.com` redirects or missing charge-bound attribution envelopes inside HealthOS.

## Security migration warning

The existing `phani-droid/eden-analytics-worker` repository tracks `.dev.vars`, `.DS_Store`, and `.github/.DS_Store`; its ignore file is named `gitignore` instead of `.gitignore`. This package corrects the ignore filename and never includes secret values. Remove the tracked files during migration and treat credentials historically stored in `.dev.vars` as potentially exposed. Coordinate rotation with each owner; in particular, rotating `SERVER_API_SECRET` without updating the app server at the same time will break `/server-collect`.
