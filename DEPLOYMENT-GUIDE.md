# Deployment guide

The safest workflow is: back up the old repository, replace its files on a branch, open a PR, wait for QA, merge once, and let GitHub Actions deploy the merged `main` commit atomically.

## 1. Confirm access before changing anything

You need:

- write access to `phani-droid/eden-analytics-worker`;
- access to the Cloudflare account containing Worker `eden-analytics`;
- GitHub Actions secrets `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID`;
- permission to edit the `eden.health` Worker routes;
- the five Worker secrets already present in Cloudflare.

The Cloudflare API token should be narrowly scoped to the Eden account/zone with Workers Scripts edit, Workers Routes edit, Queues edit, and the binding permissions needed by the existing Worker.

## 2. Back up the current repository state

```bash
git clone https://github.com/phani-droid/eden-analytics-worker.git
cd eden-analytics-worker
git switch main
git pull --ff-only origin main
git tag backup-before-v556-$(date -u +%Y%m%dT%H%M%SZ)
git push origin --tags
```

Record the currently active Cloudflare version from **Cloudflare → Workers & Pages → eden-analytics → Deployments**. This is the rollback version.

## 3. Create a feature branch

```bash
git switch -c phani-analytics-v556-compat
```

## 4. Replace the repository contents

Copy every file and folder from this package into the cloned repository root. Keep the `.git` directory from the clone. Remove these obsolete tracked files if they still exist:

```bash
git rm -f eden-analytics-worker.js wrangler.toml gitignore
git rm -f .dev.vars .DS_Store .github/.DS_Store
```

The new repository root must contain:

```text
.github/workflows/deploy.yml
.gitignore
cloudflare-workers/eden-analytics.js
cloudflare-workers/eden-conversion-coordinator.js
scripts/test-eden-analytics-worker-local.mjs
scripts/test-eden-conversion-coordinator-v556-local.mjs
scripts/verify-live-health.mjs
package.json
package-lock.json
wrangler.jsonc
README.md
DEPLOYMENT-GUIDE.md
```

Do not copy `.dev.vars`, `.env`, API keys, Segment keys, service-account JSON, cookie values, or raw event payloads.

The old repository already tracks `.dev.vars`. Do not print it or copy it into the new branch. Assume any credential historically stored there may have been disclosed and create a coordinated rotation plan:

- rotate the Cloudflare API token and update the GitHub Actions secret;
- rotate the Segment write key with the Segment owner and update the Cloudflare Worker secret;
- rotate `SERVER_API_SECRET` only in a coordinated app-server and Worker release;
- rotate browser/privacy HMAC and Google service-account credentials with their owners and verify backward-compatibility requirements first;
- use GitHub secret scanning/history cleanup if the repository was ever accessible outside the intended team.

Deleting `.dev.vars` in the new commit does not remove it from old Git history. Credential rotation is the real containment step.

## 5. Install and test locally

Install Node.js 22 or newer, then run:

```bash
npm ci
npm test
npm run dry-run
git diff --check
```

Stop if any command fails. `npm test` must end with both Worker and coordinator `PASS` messages.

## 6. Verify GitHub Actions secrets

In GitHub:

1. Open **Settings**.
2. Open **Secrets and variables → Actions**.
3. Confirm `CLOUDFLARE_API_TOKEN` exists.
4. Confirm `CLOUDFLARE_ACCOUNT_ID` exists and points to account `b08c39f6974b2943b22501e63592191b`.

Do not add the five Worker runtime secrets to the GitHub repository. They remain encrypted on the existing Cloudflare Worker.

## 7. Verify Cloudflare bindings and secrets

From the local repository, authenticate Wrangler and inspect names only:

```bash
npx wrangler whoami
npx wrangler secret list --config wrangler.jsonc
npx wrangler kv namespace list
npx wrangler queues list
```

Confirm the following existing bindings match `wrangler.jsonc`:

- KV `GCLID_KV`: `12c53db492b7419d8717d6d23831d186`.
- KV `PRIVACY_LEDGER_KV`: `aeb98a81cfa24c7ab55e0d7419627543`.
- KV `AD_CLICK_KV`: `82908a4725ad41a18dc307cbfc52fb72`.
- Durable Object binding `CONVERSION_COORDINATOR` → class `ConversionCoordinator`.
- Queue producer/consumer `eden-health-ad-click-memory`.
- Dead-letter queue `eden-health-ad-click-memory-dlq`.
- Routes for `eden.health`, `www.eden.health`, `app.eden.health`, and `collect.eden.health`.

Confirm secret names:

```text
SEGMENT_WRITE_KEY
SERVER_API_SECRET
BROWSER_CAP_HMAC_SECRET
PRIVACY_LEDGER_HMAC_SECRET
AD_CLICK_GOOGLE_SERVICE_ACCOUNT_KEY
```

If a secret name is absent, stop and have the authorized owner set it. Do not invent or rotate production values during this code deployment.

## 8. Commit and push the branch

```bash
git status --short
git add .
git diff --cached --check
git commit -m "fix: deploy v5.56 app Webflow and Mixpanel compatibility"
git push -u origin phani-analytics-v556-compat
```

## 9. Open the pull request

Use this title:

```text
Phani Analytics: deploy v5.56 collection compatibility
```

PR summary:

```markdown
- ports the CEO production v5.56 Worker and unchanged ConversionCoordinator
- restores app AnalyticsBrowser route aliases
- restores safe Webflow/app first-session collection
- makes production browser Segment delivery synchronous and retryable
- emits Mixpanel-safe destination message IDs while retaining original idempotency evidence
- adds full local QA, Wrangler dry-run, and post-deploy exact-release verification
```

Wait until the `validate` job passes. Review the complete diff. The coordinator should be the CEO file, and there must be no secrets.

## 10. Merge and deploy

Merge the PR into `main` once approved. The `push` to `main` triggers `.github/workflows/deploy.yml`.

The workflow:

1. installs pinned Wrangler dependencies;
2. reruns all tests;
3. builds a dry-run bundle;
4. deploys one atomic Worker version;
5. verifies version `5.56`, release revision `v556-phani-analytics-compat-20260715`, readiness, Segment secret presence, and synchronous browser delivery.

Do not create a 1%/10% version split. v5.56 uses shared Durable Object, Queue, and KV state; the repository deploy intentionally promotes one version atomically.

## 11. Production smoke tests

### Health

Open:

```text
https://app.eden.health/eden-health-check
```

Confirm:

- `ok: true`;
- `ready: true`;
- `version: "5.56"`;
- `release_revision: "v556-phani-analytics-compat-20260715"`;
- `browser_segment_delivery_mode: "sync"`;
- `segment_write_key_configured: true`.

### Webflow

1. Open an incognito `www.eden.health` page with QA UTMs.
2. Open Developer Tools → Network and filter `collect`.
3. Confirm `/collect` returns `200`.
4. Confirm the response sets anonymous, session, and browser-capability cookies.
5. Confirm the controlled event appears in Segment with its original name.

### App

1. Open `app.eden.health` in incognito mode.
2. Confirm `/collect/t` or `/collect/p` returns `200`, not `404`, `401`, or `409`.
3. Start a non-financial intake action.
4. Confirm `OS_intake_started` reaches Segment unchanged.

### Segment and Mixpanel

1. Confirm Segment receives the controlled event.
2. Inspect destination delivery to Mixpanel.
3. Confirm the outgoing `messageId`/Mixpanel `$insert_id` matches `m-[a-f0-9]{32}`.
4. Confirm the original source ID exists as `properties.segment_source_message_id`.
5. Confirm there are no new `$insert_id is invalid` destination errors.

### Server event

Using only an approved existing test transaction, confirm `/server-collect` receives one authoritative `OS_purchase` and Segment/Mixpanel receive it once. Do not create a real customer purchase solely for testing.

## 12. Monitor after deployment

For at least the first hour and again after a full business cycle, monitor:

- Cloudflare status counts for `/collect`, `/collect/*`, `/browser-capability`, and `/server-collect`;
- `401`, `403`, `409`, `422`, and `503` changes;
- `browser_capability_*`, `browser_owner_cookie_required`, and `browser_session_cookie_invalid` logs;
- `segment_collect_delivery_failed` and `segmentPost network error`;
- Segment source volume and destination failures;
- Mixpanel raw-event arrival and invalid insert-ID errors;
- Queue and DLQ health;
- exact `OS_*` event-name continuity.

Cloudflare log sampling is not complete evidence. Always corroborate it with Segment delivery and controlled Mixpanel receipt.

## 13. Rollback

If there is a confirmed regression:

1. Open **Cloudflare → Workers & Pages → eden-analytics → Deployments**.
2. Select the recorded pre-release v5.56-compatible version.
3. Roll it back at 100%.
4. Do not select a pre-Durable-Object-migration version without an engineering review.
5. Repeat health, Webflow, app, server, Segment, Mixpanel, Queue and DLQ checks.

Reverting the GitHub PR afterward restores repository truth and will trigger another deploy, so coordinate the Cloudflare rollback and Git revert deliberately rather than performing both blindly.
