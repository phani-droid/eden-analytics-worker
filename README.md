# eden-analytics-worker

Cloudflare Edge Worker — intercepts `eden.health` + `app.eden.health`, enriches ALL events (client and server-side) with gclid/UTM attribution via Cloudflare KV, strips PHI, and routes to Segment → Google Ads, Mixpanel, BigQuery and other destinations.

---

## What this worker does

```
User clicks Google Ad → gclid in URL
  → Worker intercepts page request
  → Extracts gclid + UTMs
  → Stores in Cloudflare KV against anonymousId (90 days)
  → Sets ITP-resistant HttpOnly cookie

Any future event — client OR server-side:
  → Goes through worker /collect or /server-collect
  → Worker looks up gclid from KV automatically
  → Attaches gclid to event properties
  → Strips PHI (email, name, phone, card data, health data)
  → Hashes raw email → email_sha256
  → Forwards to Segment

Segment routes to all destinations:
  → Google Ads → conversion attributed to paid click ✓
  → Mixpanel → full patient journey with attribution ✓
  → BigQuery → data warehouse ✓
  → Customer.io → lifecycle triggers ✓
```

---

## Endpoints

| Endpoint | Method | Who calls it | What it does |
|---|---|---|---|
| `/*` | GET | Browser | Intercepts page loads, sets cookie, fires page_viewed |
| `/collect` | POST | analytics.js (Danny) | Client-side events — enriched with KV gclid |
| `/server-collect` | POST | Node.js API (Ryon) | Server-side events — enriched with KV gclid |
| `/eden-health-check` | GET | Anyone | Health check |

---

## One-time setup

### 1. Create KV namespace
```bash
wrangler kv:namespace create "GCLID_KV"
```
Copy the `id` from the output → paste into `wrangler.toml` `[[kv_namespaces]]` section.

### 2. Set secrets
```bash
wrangler secret put SEGMENT_WRITE_KEY
# Paste your Segment write key when prompted

wrangler secret put SERVER_API_SECRET
# Paste any secret string — used to authenticate /server-collect
```

### 3. Deploy
```bash
wrangler deploy
```

---

## Adding new events

**No worker changes needed.**

Client-side:
```javascript
analytics.track('your_new_event', { prop1: 'value' });
// Worker forwards to Segment automatically with gclid attached
```

Server-side (Ryon):
```javascript
await fetch('https://app.eden.health/server-collect', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'X-Eden-Server-Secret': process.env.EDEN_SERVER_API_SECRET,
  },
  body: JSON.stringify({
    event:       'OS_qualified_first_order',
    userId:      patient.userId,
    anonymousId: patient.anonId,  // ← worker uses this to look up gclid from KV
    properties:  {
      order_id: order.id,
      value:    order.amountCents / 100,
      currency: 'USD',
      // NO gclid needed — worker adds automatically
    }
  })
});
```

## Adding new destinations

**No worker changes needed.**

Configure destination in Segment UI only.
Worker forwards everything to Segment as-is.
Segment routes to all configured destinations.

---

## PHI stripping

These fields are automatically stripped from ALL events before Segment:

**PII:** `customerEmail`, `email`, `firstName`, `lastName`, `phoneNumber`, `phone`, `full_name`, `address`, `dob`, `date_of_birth`

**Health data:** `weight_lbs`, `height_ft`, `bmi_value`, `goal_weight_lbs`, `highest_weight_lbs`, `selected_conditions`, `selected_medications`, `selected_allergies`, `lbs_lost`, `old_dose_mg`, `new_dose_mg`, `medication`

**PCI:** `card_number`, `card_exp_date`, `card_cvc`, `OS_card_number`, `OS_card_exp_date`, `OS_card_cvc`

Raw `email` is automatically hashed → `email_sha256` (SHA-256, lowercase, trimmed).

To add a new PHI field — add it to `PHI_PROPS` in `eden-analytics-worker.js` and deploy.

---

## Architecture

```
eden.health / app.eden.health
        ↓
  Cloudflare Worker (this)
  ├── KV: stores gclid per anonymousId (90 days)
  ├── PHI stripping
  ├── email → email_sha256
  └── forwards enriched events
        ↓
     Segment
  ├── Google Ads  → EdenOS - Purchase, EdenOS - QFO
  ├── Mixpanel    → product analytics
  ├── BigQuery    → data warehouse
  └── Customer.io → lifecycle
```

---

## Files

| File | Purpose |
|---|---|
| `eden-analytics-worker.js` | Worker code — single source of truth |
| `wrangler.toml` | Cloudflare config — routes, KV binding |
| `.github/workflows/deploy.yml` | Auto-deploy on push to main |
| `.gitignore` | Keeps secrets out of git |

---

## Environment variables

| Variable | Where set | Required | Description |
|---|---|---|---|
| `SEGMENT_WRITE_KEY` | `wrangler secret put` | ✅ Yes | Segment write key |
| `SERVER_API_SECRET` | `wrangler secret put` | Optional | Authenticates /server-collect |
| `GCLID_KV` | `wrangler.toml` | ✅ Yes | KV namespace binding |
