# Event Delivery RCA and Compatibility Findings

## Worker-side failure mode

The v5.56 Worker delivered conversion events such as `OS_purchase` synchronously, but ordinary authenticated server events used `ctx.waitUntil()`. The caller received HTTP 200 before Segment acknowledged the event. A Segment failure was therefore visible only in Worker logs and could not be retried by the producer.

Authenticated operational events without a user, anonymous ID, session, order, or conversion transaction were intentionally skipped because Segment requires a `userId` or `anonymousId`. This protected identity integrity, but it also dropped legitimate event-only telemetry.

## Worker changes

- Added `EDEN_SERVER_SEGMENT_DELIVERY_MODE=sync`.
- Ordinary authenticated `/server-collect` events now receive HTTP 200 only after Segment accepts them.
- Segment/network failure returns retryable HTTP 503 with `segment_forwarded=false`.
- Authenticated operational events without an identity receive an opaque event-scoped delivery ID.
- The event-scoped ID is never used for attribution lookup, user linking, KV, Durable Object ownership, conversion dedupe, or ad-click memory.
- Ownerless events containing advertising evidence remain diagnostic-only and are not assigned a synthetic identity.
- Browser and Webflow collection remains synchronous and preserves producer event names.
- Existing v5.56 conversion, privacy, attribution, Queue, KV, and coordinator behavior remains intact.

## Application findings that a Worker cannot repair

The supplied application has three independent producer implementations:

1. `packages/jobs/src/lib/segment.ts` uses authenticated `/server-collect`.
2. `apps/patient/src/lib/segment/server.ts` sends directly to Segment.
3. `apps/dashboard/src/lib/segment/server.ts` sends directly to Segment.

The patient and dashboard server helpers bypass this Worker completely. Their events cannot be fixed, observed, retried, or enriched by changing the Worker.

The jobs helper also calls `fetch()` without checking `response.ok`. HTTP 401, 422, or 503 responses are treated as success. The application should throw on non-2xx responses and allow the job/task retry policy to retry the event.

The app browser helper returns a no-op analytics client when `NEXT_PUBLIC_SEGMENT_WRITE_KEY` is missing. The jobs helper returns a no-op client when `EDEN_SERVER_API_SECRET` is missing. Those environment variables must be present in every runtime that emits events.

## Required end-to-end application follow-up

- Route patient and dashboard server events through authenticated `/server-collect`, or explicitly accept that they bypass the Worker.
- Check `response.ok` in every server collector helper and throw a retryable error for non-2xx responses.
- Confirm `EDEN_SERVER_API_SECRET` in each jobs runtime exactly matches Cloudflare `SERVER_API_SECRET`.
- Confirm `NEXT_PUBLIC_SEGMENT_WRITE_KEY` exists at patient-app build time for browser tracking.
- Do not send browser stable identity through `/identify`; authenticated `/server-collect` remains the stable-identity authority.

## Expected health response

After deployment, `/eden-health-check` must report:

- `ok: true`
- `ready: true`
- `version: 5.56`
- `release_revision: v556-phani-browser-sdk-hotfix-20260715`
- `browser_segment_delivery_mode: sync`
- `server_segment_delivery_mode: sync`
