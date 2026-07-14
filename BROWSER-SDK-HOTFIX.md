# Browser SDK event-delivery hotfix

## Incident and root cause

The app's Segment `AnalyticsBrowser` client uses `apiHost: https://app.eden.health/collect`. The SDK appends method suffixes such as `/t` (track), `/i` (identify), and `/m` (SDK metrics). Its browser beacon transport sends a JSON body with `Content-Type: text/plain;charset=UTF-8`.

The deployed Worker accepted only `application/json`, so `/collect/t` and `/collect/i` returned `415 Unsupported Media Type`. `/collect/m` had no route and returned `404`. Events such as `OS_intake_started`, `OS_bmi_screen`, `OS_question_answered`, and Vouched-related browser events were therefore stopped before Segment and Mixpanel.

## Applicable Worker changes

1. Browser collectors accept bounded JSON from `application/json` and `text/plain`.
2. Existing origin, cross-site, signed capability, owner/session, privacy, attribution, enrichment, and Segment-delivery logic remains in the request path.
3. `/server-collect` remains strict and authenticated; it does not accept `text/plain`.
4. `/collect/m` and `/collect/v1/m` return `204` after origin and cross-site checks and do not become customer events.
5. CEO v5.56 conversion coordination, KV, Queue, privacy, ad-click, multi-session attribution, and `OS_*` naming logic remains present.

## Files to deploy

Deploy the whole repository so code and bindings stay atomic. The runtime files are:

- `cloudflare-workers/eden-analytics.js` — changed Worker entry point.
- `cloudflare-workers/eden-conversion-coordinator.js` — required imported Durable Object module; unchanged.
- `wrangler.jsonc` — required routes, Durable Object, KV and Queue bindings, and production variables.
- `package.json` and `package-lock.json` — pinned Wrangler toolchain and validation scripts.

Do not paste only the Worker body into an old single-file configuration: it imports `./eden-conversion-coordinator.js` and expects the bindings declared in `wrangler.jsonc`.

## Post-deployment smoke test

1. Open `app.eden.health` in an Incognito window with DevTools Network open.
2. Complete several intake steps.
3. Confirm `/collect/t` returns `200`, `/collect/i` returns `200`, and `/collect/m` returns `204`.
4. Confirm exact events such as `OS_intake_started`, `OS_bmi_screen`, and `OS_question_answered` appear in Segment Debugger with the same anonymous/user identity and attribution fields.
5. Confirm those events appear in Mixpanel and do not show `$insert_id` rejection.
6. Run one authenticated server event and one test purchase to verify `/server-collect`, conversion coordination, KV and Queue behavior.

If browser requests still return `415`, verify the `app.eden.health/*` route points to the newly deployed version. If the Worker returns `200` but Segment is empty, inspect Worker logs for `segment_collect_delivery_failed` or `segmentPost network error`.
