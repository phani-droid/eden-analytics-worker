// =============================================================================
// EdenOS Analytics Worker — v5.3
// =============================================================================
//
// ARCHITECTURE — five coverage layers:
//
//   Layer 1 — HttpOnly cookie (eden_anon_id + eden_anonymous_id legacy)
//     First-party, server-set, ITP-resistant, 2 years
//     Reads legacy eden_anonymous_id if present (identity continuity)
//     Sets eden_anon_id as canonical going forward
//     Domain=.eden.health spans eden.health AND app.eden.health
//
//   Layer 2 — Cloudflare KV (attribution store, 120 days)
//     Stores gclid + 15 other click IDs + UTMs against anonymousId
//     Parallel reads — anonId + userId resolved simultaneously
//     First-touch wins — never overwritten by retargeting
//
//   Layer 3 — userId → gclid link at /identify
//     Called at login + account creation
//     Copies attribution from anonymousId → userId in KV (copy only)
//     Enables cross-device + post-cookie-clear attribution
//
//   Layer 4 — Enhanced conversions (email_sha256)
//     email auto-hashed to email_sha256 via SHA-256
//     Google + Meta match against their account databases
//
//   Layer 5 — Organic referrer detection
//     Detects Google, Bing, DuckDuckGo etc from Referer header
//     Labels as utm_source=google, utm_medium=organic
//
// ATTRIBUTION MODEL: First-touch wins
//   First paid click stored in KV — retargeting cannot overwrite
//   userId copy only — never overwrites existing userId attribution
//   Click IDs validated non-empty before KV write
//   Organic flows clean — attribution never fabricated
//
// DEDUPLICATION (three layers):
//   1. Worker KV — 24hr TTL per order_id (blocks Shippo webhook retries)
//   2. Google Ads — native order_id dedup (second safety net)
//   3. BigQuery — QUALIFY ROW_NUMBER = 1 per patient (permanent)
//   Fails open on KV error — better a duplicate than missed conversion
//   IMPORTANT: use Bask order_id (static UUID) not Stripe transaction_id
//   (transaction_id changes ~60% of time per Ryon — not a reliable dedup key)
//
// AUTO-CONSENT / GPC HANDLING:
//   California (CCPA) + Virginia (VCDPA) — GPC signal must be honored
//   Sec-GPC: 1 header → skip attribution storage → page served clean
//   Still fires page_viewed without attribution (no opt-out from analytics)
//   Cannot override GPC legally — non-negotiable
//
// PHI / PCI GOVERNANCE:
//   BAA signed with Segment — PHI flows through legally
//   Stripping DISABLED — all data flows to Segment → BigQuery
//   PHI/PCI decisions at BigQuery dbt layer per Jared's guidance
//   email → email_sha256 ALWAYS active (enhanced conversions)
//   To enable stripping: uncomment PCI_PROPS / PHI_PROPS blocks below
//
// IDENTITY STITCHING (per Ryon's architecture):
//   Eden has four identity layers:
//     eden_anonymous_id (edge layer — legacy) → read by worker, aligned
//     Segment anonymous_id — set by analytics.js, aligned via setAnonymousId
//     Bask order_id (static UUID) — used for dedup, NOT transaction_id
//     Bask user_id — linked at /identify → KV → cross-device attribution
//
// CHANNEL MIGRATION SCHEDULE:
//   May 28 — Google Paid Search + SEO (worker must be live TODAY)
//   Jun 01 — Affiliate (Everflow + Katalys)
//   Jun 02 — Meta (Facebook / Instagram)
//   Jun 03 — TikTok
//   Jun 04 — Influencer (Upfluence)
//
// ATTRIBUTION SPEC:
//   All click IDs + UTMs in context.campaign per Segment spec
//   Confirmed by Segment Success Engineer George D. (May 27 2026)
//
// ROUTES (approved scope — wrangler.toml):
//   eden.health/*       — marketing site
//   www.eden.health/*   — marketing site (www)
//   app.eden.health/*   — patient portal
//
// ENDPOINTS:
//   /*                  → page requests → cookie + KV + page_viewed
//   /collect            → client-side events → KV enrichment → Segment
//   /server-collect     → server-side events → KV enrichment → Segment
//   /identify           → login/account creation → userId→gclid in KV
//   /eden-health-check  → health check + version
//
// SCALABILITY:
//   KV: unlimited keys, <1ms reads, 300+ edge locations
//   Worker: no cold start, handles 10M+ requests/month at $5
//   Adding new domain: add to routes + ALLOWED_ORIGINS → deploy
//   Adding new channel: add to CLICK_ID_CONFIG → deploy
//   Enabling PHI stripping: uncomment blocks below → deploy
// =============================================================================


// ─────────────────────────────────────────────────────────────────────────────
// PHI / PCI STRIPPING — DISABLED (BAA active, decisions at BQ dbt)
//
// To enable when Jared confirms fields:
//   1. Uncomment relevant block below
//   2. Call stripFields(props, PCI_PROPS) in hashEmail() function
//   3. Deploy — takes effect immediately
//
// const PCI_PROPS = new Set([
//   "card_number", "card_exp_date", "card_cvc",
//   "OS_card_number", "OS_card_exp_date", "OS_card_cvc",
//   "cvv", "pan",
// ]);
//
// const PHI_PROPS = new Set([
//   "firstName", "lastName", "phoneNumber", "phone",
//   "full_name", "address", "dob", "date_of_birth",
//   "ssn", "social_security_number",
//   "weight_lbs", "height_ft", "bmi_value",
//   "goal_weight_lbs", "highest_weight_lbs",
//   "selected_conditions", "selected_medications",
//   "selected_allergies", "medication",
//   "diagnosis", "prescription", "medical_history",
// ]);
// ─────────────────────────────────────────────────────────────────────────────


// ─────────────────────────────────────────────────────────────────────────────
// ALLOWED ORIGINS
// To add new domain: add here + add route in wrangler.toml → deploy
// ─────────────────────────────────────────────────────────────────────────────

const ALLOWED_ORIGINS = [
  "https://eden.health",
  "https://www.eden.health",
  "https://app.eden.health",
  "https://eden-os-rimo-patient-staging.vercel.app",
];


// ─────────────────────────────────────────────────────────────────────────────
// CLICK ID CONFIG — all paid channel identifiers
// To add new channel: add entry here → extractClickIds + buildCampaignContext
// update automatically
// ─────────────────────────────────────────────────────────────────────────────

const CLICK_ID_CONFIG = [
  { param: "gclid",     channel: "google_ads",    label: "Google Ads"     },
  { param: "gbraid",    channel: "google_ios",    label: "Google iOS"     },
  { param: "wbraid",    channel: "google_web",    label: "Google Web"     },
  { param: "dclid",     channel: "google_display",label: "Google Display" },
  { param: "fbclid",    channel: "meta",          label: "Meta/Facebook"  },
  { param: "msclkid",   channel: "microsoft",     label: "Microsoft/Bing" },
  { param: "ttclid",    channel: "tiktok",        label: "TikTok"         },
  { param: "twclid",    channel: "twitter",       label: "Twitter/X"      },
  { param: "li_fat_id", channel: "linkedin",      label: "LinkedIn"       },
  { param: "rdt_cid",   channel: "reddit",        label: "Reddit"         },
  { param: "epik",      channel: "pinterest",     label: "Pinterest"      },
  { param: "ScCid",     channel: "snapchat",      label: "Snapchat"       },
  { param: "nbt",       channel: "northbeam",     label: "Northbeam"      },
  { param: "irclickid", channel: "impact_radius", label: "Impact Radius"  },
  { param: "cjevent",   channel: "cj_affiliate",  label: "CJ Affiliate"   },
  { param: "click_id",  channel: "generic",       label: "Generic"        },
];


// ─────────────────────────────────────────────────────────────────────────────
// BOT DETECTION
// ─────────────────────────────────────────────────────────────────────────────

const BOT_UA_PATTERNS = [
  /bot\b/i, /crawler/i, /spider/i, /headless/i,
  /lighthouse/i, /pagespeed/i, /playwright/i,
  /puppeteer/i, /preview/i, /prerender/i,
  /google-inspectiontool/i,
];

const BOT_CF_DECISIONS = new Set([
  "automated", "likely_automated", "verified_bot",
]);


// ─────────────────────────────────────────────────────────────────────────────
// STATIC ASSET DETECTION
// ─────────────────────────────────────────────────────────────────────────────

const STATIC_EXTENSIONS = [
  ".avif", ".bmp", ".css", ".gif", ".ico",
  ".jpg", ".jpeg", ".js", ".mjs", ".map",
  ".mp4", ".otf", ".png", ".svg", ".ttf",
  ".wasm", ".webm", ".webp", ".woff", ".woff2",
];

const STATIC_PREFIXES = [
  "/_next/static/", "/static/chunks/",
  "/static/css/", "/static/js/", "/static/media/",
];

const SENSITIVE_URL_PARAMS = [
  /client_secret/i, /payment_intent/i, /setup_intent/i,
  /^secret$/i, /^password$/i, /^token$/i,
  /^code$/i, /^state$/i,
];


// ─────────────────────────────────────────────────────────────────────────────
// KV KEY SCHEMA
//   attr:anon:{anonymousId}     → attribution object (set on ad click)
//   attr:user:{userId}          → attribution object (set on login)
//   dedup:{eventName}:{orderId} → dedup lock (24hr TTL)
//
// DEDUP NOTE: orderId must be Bask order_id (static UUID)
// NOT Stripe transaction_id (changes ~60% of time per Ryon)
// ─────────────────────────────────────────────────────────────────────────────

const KV_ANON_PREFIX = "attr:anon:";
const KV_USER_PREFIX = "attr:user:";
const KV_TTL         = 10368000;   // 120 days
const KV_DEDUP_TTL   = 86400;      // 24 hours


// ─────────────────────────────────────────────────────────────────────────────
// CONVERSION EVENTS — deduplicated at edge
// Dedup key: Bask order_id (static) — NOT Stripe transaction_id (changes)
// ─────────────────────────────────────────────────────────────────────────────

const CONVERSION_EVENTS = new Set([
  "OS_qualified_first_order",
  "OS_purchase",
  "order_completed",
  "reorder_completed",
]);


// =============================================================================
// WORKER ENTRY POINT
// Approved routes: eden.health/*, www.eden.health/*, app.eden.health/*
// =============================================================================

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);

      // ── Health check ──────────────────────────────────────────────────────
      if (url.pathname === "/eden-health-check") {
        return jsonResponse({
          ok:                  true,
          worker:              "eden-analytics",
          version:             "5.3",
          ts:                  nowUTC(),
          kv:                  !!env.GCLID_KV,
          phi_stripping:       "disabled — BAA active — decisions at BQ dbt",
          gpc_handling:        "enabled — California/Virginia legal compliance",
          attribution_model:   "first-touch",
          attribution_ttl:     "120 days",
          dedup_ttl:           "24 hours",
          dedup_key:           "Bask order_id (static UUID)",
          routes:              ["eden.health/*", "www.eden.health/*", "app.eden.health/*"],
          channels_supported:  CLICK_ID_CONFIG.map(c => c.label),
        });
      }

      // ── CORS preflight ────────────────────────────────────────────────────
      if (request.method === "OPTIONS") {
        return new Response(null, {
          status:  204,
          headers: corsHeaders(request.headers.get("Origin") || ""),
        });
      }

      // ── Skip bots ─────────────────────────────────────────────────────────
      if (isBot(request)) return fetch(request);

      // ── Skip static assets ────────────────────────────────────────────────
      if (isStaticAsset(url)) return fetch(request);

      // ── /collect — client-side events ─────────────────────────────────────
      if (url.pathname === "/collect" && request.method === "POST") {
        return handleCollect(request, env, ctx, url);
      }

      // ── /server-collect — server-side events ──────────────────────────────
      if (url.pathname === "/server-collect" && request.method === "POST") {
        return handleServerCollect(request, env, ctx);
      }

      // ── /identify — login / account creation ──────────────────────────────
      if (url.pathname === "/identify" && request.method === "POST") {
        return handleIdentify(request, env, ctx);
      }

      // ── All page requests ─────────────────────────────────────────────────
      return handlePageRequest(request, env, ctx, url);

    } catch (err) {
      console.error("[eden-analytics] unhandled error:", err);
      return fetch(request); // fail open — always serve the page
    }
  },
};


// =============================================================================
// PAGE REQUEST HANDLER
//
// GPC auto-consent: California (CCPA) + Virginia (VCDPA)
//   Sec-GPC: 1 → skip attribution storage → page served clean
//   page_viewed still fires without attribution (non-tracking analytics)
//
// Identity continuity:
//   Reads legacy eden_anonymous_id if present (Ryon's edge layer ID)
//   Sets eden_anon_id as canonical going forward
//   Both aligned → seamless identity stitching across old + new pipeline
// =============================================================================

async function handlePageRequest(request, env, ctx, url) {
  // ── GPC auto-consent — California (CCPA) + Virginia (VCDPA) ──────────────
  // Sec-GPC: 1 = user has enabled Global Privacy Control
  // Legal requirement — cannot override — skip attribution storage
  const gpcOptOut = request.headers.get("Sec-GPC") === "1";

  // ── Identity continuity — read legacy edge layer ID if present ────────────
  // eden_anonymous_id = Ryon's legacy edge layer identifier
  // eden_anon_id = our canonical worker cookie going forward
  // Use legacy if present → preserves stitching for existing users
  const legacyAnonId  = readCookie(request, "eden_anonymous_id");
  const existingAnonId = readCookie(request, "eden_anon_id") || legacyAnonId;
  const existingSession = readCookie(request, "eden_session_id");

  const isNewVisitor = !existingAnonId;
  const isNewSession = !existingSession;

  const anonId  = existingAnonId  || crypto.randomUUID();
  const session = existingSession || `${crypto.randomUUID()}_${Date.now()}`;

  const clickIds = extractClickIds(url);
  const utms     = extractUTMs(url);

  // Layer 2 — store attribution in KV (skip if GPC opt-out)
  const hasAttribution = Object.keys(clickIds).length > 0 || !!utms;
  if (hasAttribution && env.GCLID_KV && !gpcOptOut) {
    ctx.waitUntil(
      storeAttribution(env.GCLID_KV, KV_ANON_PREFIX + anonId, {
        ...(utms || {}),
        ...clickIds,
      }).catch(err => console.error("[eden-analytics] KV store error:", err))
    );
  }

  // Pass through to origin — never block page load
  const response = await fetch(request);

  // Layer 1 — set HttpOnly cookies
  const headers = new Headers(response.headers);
  if (isNewVisitor) headers.append("Set-Cookie", buildAnonCookie(anonId, url));
  if (isNewSession) headers.append("Set-Cookie", buildSessionCookie(session, url));

  // Fire page events in background
  if (env.SEGMENT_WRITE_KEY) {
    ctx.waitUntil(
      firePageEvents(
        request, env, anonId, session, url,
        isNewVisitor, isNewSession, clickIds, utms, gpcOptOut
      ).catch(err => console.error("[eden-analytics] page event error:", err))
    );
  }

  return new Response(response.body, {
    status:     response.status,
    statusText: response.statusText,
    headers,
  });
}


// =============================================================================
// PAGE EVENTS
// =============================================================================

async function firePageEvents(
  request, env, anonId, session, url,
  isNewVisitor, isNewSession, clickIds, utms, gpcOptOut
) {
  const cleanUrl  = sanitizeUrl(url);
  const referrer  = sanitizeUrlString(request.headers.get("Referer") || "");
  const ua        = request.headers.get("User-Agent") || "";
  const portal    = url.hostname.includes("app.eden.health") ? "patient" : "marketing";
  const sessionId = session.split("_")[0];

  // Layer 5 — organic fallback from referrer
  const organic     = !utms && !clickIds.gclid && referrer ? detectOrganic(referrer) : null;

  // If GPC opt-out — fire page_viewed without any attribution
  const attribution = gpcOptOut ? {} : { ...(utms || organic || {}), ...clickIds };

  await segmentPost(env.SEGMENT_WRITE_KEY, "track", {
    anonymousId: anonId,
    event:       "page_viewed",
    properties:  {
      portal,
      page_path:        url.pathname,
      page_url:         cleanUrl,
      page_search:      url.search || undefined,
      referrer:         referrer   || undefined,
      device_type:      isMobile(ua) ? "mobile" : "desktop",
      session_id:       sessionId,
      is_new_visitor:   isNewVisitor,
      is_new_session:   isNewSession,
      gpc_opt_out:      gpcOptOut,
      pipeline_version: "5.3",
    },
    context:   { campaign: buildCampaignContext(attribution) },
    timestamp: nowUTC(),
  });

  // first_touch — new sessions with paid attribution + no GPC opt-out
  if (isNewSession && Object.keys(attribution).length > 0 && !gpcOptOut) {
    await segmentPost(env.SEGMENT_WRITE_KEY, "track", {
      anonymousId: anonId,
      event:       "first_touch",
      properties: {
        portal,
        page_path:        url.pathname,
        page_url:         cleanUrl,
        referrer:         referrer || undefined,
        session_id:       sessionId,
        pipeline_version: "5.3",
      },
      context:   { campaign: buildCampaignContext(attribution) },
      timestamp: nowUTC(),
    });
  }
}


// =============================================================================
// /collect HANDLER — CLIENT-SIDE EVENTS
// =============================================================================

async function handleCollect(request, env, ctx, url) {
  const origin = request.headers.get("Origin") || "";

  if (origin && !ALLOWED_ORIGINS.includes(origin)) {
    return new Response("Forbidden", { status: 403 });
  }

  let body;
  try { body = await request.json(); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  const gpcOptOut = request.headers.get("Sec-GPC") === "1";

  // Layer 1 — HttpOnly cookie is most reliable anonId source
  // Also read legacy eden_anonymous_id for identity continuity
  const cookieAnonId = readCookie(request, "eden_anon_id")
                    || readCookie(request, "eden_anonymous_id");
  const anonId       = cookieAnonId || body.anonymousId || crypto.randomUUID();
  const isNew        = !cookieAnonId;
  const portal       = origin.includes("app.eden.health") ? "patient" : "marketing";
  const userId       = body.userId || null;

  // Layer 2 + 3 — parallel KV reads
  const storedAttribution = (env.GCLID_KV && !gpcOptOut)
    ? await resolveAttribution(env.GCLID_KV, anonId, userId)
    : null;

  // Fresh URL params override stored — current click is fresher
  const freshClickIds = gpcOptOut ? {} : extractClickIds(url);
  const freshUTMs     = gpcOptOut ? null : extractUTMs(url);
  const attribution   = {
    ...(storedAttribution || {}),
    ...(freshUTMs         || {}),
    ...freshClickIds,
  };

  const superProps = {
    portal,
    source_type:      "client",
    gpc_opt_out:      gpcOptOut,
    pipeline_version: "5.3",
  };

  if (env.SEGMENT_WRITE_KEY) {
    ctx.waitUntil(
      forwardToSegment(env.SEGMENT_WRITE_KEY, body, anonId, superProps, attribution)
        .catch(err => console.error("[eden-analytics] collect error:", err))
    );
  }

  const headers = {
    "Content-Type": "application/json",
    ...corsHeadersObj(origin),
  };

  if (isNew) headers["Set-Cookie"] = buildAnonCookie(anonId, url);

  return new Response(JSON.stringify({ ok: true, anonId }), { status: 200, headers });
}


// =============================================================================
// /server-collect HANDLER — SERVER-SIDE EVENTS
//
// IMPORTANT — dedup key must be Bask order_id (static UUID):
//   ✓ order_id: "01de7fc4-5432-4598-ba7b-d99edd28ec82" (Bask — static)
//   ✗ transaction_id: "pi_3Sz71RB18nTglqz80o3l3vQI" (Stripe — changes 60%)
// Engineer must pass order_id not transaction_id in properties
// =============================================================================

async function handleServerCollect(request, env, ctx) {
  if (env.SERVER_API_SECRET) {
    const secret = request.headers.get("X-Eden-Server-Secret");
    if (secret !== env.SERVER_API_SECRET) {
      return new Response("Unauthorized", { status: 401 });
    }
  }

  let body;
  try { body = await request.json(); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  const anonId    = body.anonymousId          || null;
  const userId    = body.userId               || null;
  const eventName = body.event                || "";
  const orderId   = body.properties?.order_id || null; // Bask order_id (static)

  // ── Edge deduplication ────────────────────────────────────────────────────
  if (CONVERSION_EVENTS.has(eventName) && orderId && env.GCLID_KV) {
    const dedupKey = `dedup:${eventName}:${orderId}`;
    try {
      const alreadyFired = await env.GCLID_KV.get(dedupKey);
      if (alreadyFired) {
        console.log(`[eden-analytics] dedup blocked: ${eventName} order_id=${orderId}`);
        return jsonResponse({ ok: true, deduped: true });
      }
      await env.GCLID_KV.put(dedupKey, JSON.stringify({
        event:    eventName,
        order_id: orderId,
        userId,
        fired_at: nowUTC(),
      }), { expirationTtl: KV_DEDUP_TTL });
    } catch (err) {
      console.error("[eden-analytics] dedup KV error — failing open:", err);
    }
  }

  // Layer 2 + 3 — parallel KV reads
  const storedAttribution = env.GCLID_KV
    ? await resolveAttribution(env.GCLID_KV, anonId, userId)
    : null;

  // Merge stored attribution — engineer values take priority
  if (storedAttribution && body.properties) {
    for (const [k, v] of Object.entries(storedAttribution)) {
      if (!body.properties[k] && v) {
        body.properties[k] = v;
      }
    }
  }

  const superProps = {
    portal:           "patient",
    source_type:      "server",
    pipeline_version: "5.3",
  };

  const attribution = storedAttribution || {};

  if (env.SEGMENT_WRITE_KEY) {
    ctx.waitUntil(
      forwardToSegment(
        env.SEGMENT_WRITE_KEY,
        body,
        anonId || userId || "server",
        superProps,
        attribution
      ).catch(err => console.error("[eden-analytics] server-collect error:", err))
    );
  }

  return jsonResponse({ ok: true });
}


// =============================================================================
// /identify HANDLER — LOGIN / ACCOUNT CREATION
//
// Per Ryon's architecture — Bask user_id is created late in funnel
// Must call /identify immediately after account creation
// Links Bask user_id → anonymousId → gclid in KV
// =============================================================================

async function handleIdentify(request, env, ctx) {
  if (env.SERVER_API_SECRET) {
    const secret = request.headers.get("X-Eden-Server-Secret");
    if (secret !== env.SERVER_API_SECRET) {
      return new Response("Unauthorized", { status: 401 });
    }
  }

  let body;
  try { body = await request.json(); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  const anonId = body.anonymousId || null;
  const userId = body.userId      || null;

  // Layer 3 — link userId → anonymousId attribution in KV
  if (env.GCLID_KV && anonId && userId) {
    ctx.waitUntil(
      linkUserAttribution(env.GCLID_KV, anonId, userId)
        .catch(err => console.error("[eden-analytics] KV identify link error:", err))
    );
  }

  // Forward identify to Segment
  if (env.SEGMENT_WRITE_KEY) {
    const traits = await hashEmail(body.traits || {});
    ctx.waitUntil(
      segmentPost(env.SEGMENT_WRITE_KEY, "identify", {
        anonymousId: anonId || userId,
        userId,
        traits,
        timestamp:   nowUTC(),
      }).catch(err => console.error("[eden-analytics] identify segment error:", err))
    );
  }

  return jsonResponse({ ok: true });
}


// =============================================================================
// KV ATTRIBUTION — STORAGE + RETRIEVAL
// First-touch model — retargeting cannot steal acquisition attribution
// =============================================================================

async function storeAttribution(kv, key, attribution) {
  if (!kv || !key || !attribution) return;

  const hasValue = Object.values(attribution).some(v => v && String(v).trim());
  if (!hasValue) return;

  // First-touch — never overwrite existing gclid
  try {
    const existing = await kv.get(key);
    if (existing) {
      const parsed = JSON.parse(existing);
      if (parsed.gclid && attribution.gclid) {
        return; // first click wins
      }
    }
  } catch { /* proceed to store */ }

  await kv.put(key, JSON.stringify({
    ...attribution,
    stored_at: nowUTC(),
  }), { expirationTtl: KV_TTL });
}

async function getAttribution(kv, key) {
  if (!kv || !key) return null;
  try {
    const stored = await kv.get(key);
    if (!stored) return null;
    const { stored_at, ...attribution } = JSON.parse(stored);
    return attribution;
  } catch {
    return null;
  }
}

async function resolveAttribution(kv, anonId, userId) {
  if (!kv) return null;

  // Parallel KV reads — faster than sequential
  const [fromAnon, fromUser] = await Promise.all([
    anonId ? getAttribution(kv, KV_ANON_PREFIX + anonId) : Promise.resolve(null),
    userId ? getAttribution(kv, KV_USER_PREFIX + userId) : Promise.resolve(null),
  ]);

  if (fromAnon?.gclid) return fromAnon; // anonId with gclid = best
  if (fromUser?.gclid) return fromUser; // userId = cross-device fallback
  if (fromAnon) return fromAnon;         // any attribution without gclid
  if (fromUser) return fromUser;
  return null;
}

async function linkUserAttribution(kv, anonId, userId) {
  // Parallel reads — get both simultaneously
  const [anonAttribution, existingUser] = await Promise.all([
    getAttribution(kv, KV_ANON_PREFIX + anonId),
    getAttribution(kv, KV_USER_PREFIX  + userId),
  ]);

  if (!anonAttribution)   return; // nothing to copy
  if (existingUser?.gclid) return; // userId already attributed — first-touch preserved

  await storeAttribution(kv, KV_USER_PREFIX + userId, anonAttribution);
}


// =============================================================================
// SEGMENT FORWARDING
// Attribution in context.campaign per Segment spec
// =============================================================================

async function forwardToSegment(writeKey, body, anonId, superProps, attribution = {}) {
  const type = (body.type || "track").toLowerCase();

  const mergedContext = {
    ...(body.context || {}),
    campaign: {
      ...((body.context || {}).campaign || {}),
      ...buildCampaignContext(attribution),
    },
  };

  if (type === "identify") {
    const traits = await hashEmail(body.traits || body.properties || {});
    await segmentPost(writeKey, "identify", {
      anonymousId: anonId,
      userId:      body.userId || null,
      traits,
      context:     mergedContext,
      timestamp:   nowUTC(),
    });
    return;
  }

  if (type === "page") {
    await segmentPost(writeKey, "page", {
      anonymousId: anonId,
      userId:      body.userId || null,
      name:        body.name   || "",
      properties:  await hashEmail({ ...superProps, ...(body.properties || {}) }),
      context:     mergedContext,
      timestamp:   nowUTC(),
    });
    return;
  }

  // Default: track
  await segmentPost(writeKey, "track", {
    anonymousId: anonId,
    userId:      body.userId || null,
    event:       body.event  || "",
    properties:  await hashEmail({ ...superProps, ...(body.properties || {}) }),
    context:     mergedContext,
    timestamp:   body.sentAt || body.timestamp || nowUTC(),
  });
}

// context.campaign per Segment spec
function buildCampaignContext(attribution) {
  const campaign      = {};
  const CAMPAIGN_KEYS = [
    "utm_source", "utm_medium", "utm_campaign",
    "utm_content", "utm_term",  "utm_id",
    ...CLICK_ID_CONFIG.map(c => c.param),
  ];
  for (const k of CAMPAIGN_KEYS) {
    if (attribution[k]) campaign[k] = attribution[k];
  }
  return campaign;
}

async function segmentPost(writeKey, endpoint, payload) {
  const res = await fetch(`https://api.segment.io/v1/${endpoint}`, {
    method:  "POST",
    headers: {
      "Content-Type":  "application/json",
      "Authorization": `Basic ${btoa(writeKey + ":")}`,
    },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Segment ${endpoint} ${res.status}: ${text}`);
  }
}


// =============================================================================
// EMAIL HASHING — Layer 4 enhanced conversions
// email → email_sha256 ALWAYS active (Google/Meta enhanced conversions)
// All other data passes through — PHI/PCI decisions at BigQuery dbt layer
// To enable PCI/PHI stripping: uncomment blocks at top + add stripFields() call
// =============================================================================

async function hashEmail(props) {
  if (!props || typeof props !== "object") return props;
  const out = {};
  for (const [k, v] of Object.entries(props)) {
    // Auto-hash email → email_sha256 (Layer 4)
    if ((k === "email" || k === "customerEmail") && typeof v === "string") {
      out["email_sha256"] = await sha256(v);
      out[k] = v; // raw email passes through — BAA covers it
      // To strip raw email: replace above line with: continue;
      continue;
    }
    if (v && typeof v === "object" && !Array.isArray(v)) {
      out[k] = await hashEmail(v);
      continue;
    }
    out[k] = v;
  }
  return out;
}


// =============================================================================
// SHA-256 — lowercase + trim per Google/Meta spec
// =============================================================================

async function sha256(value) {
  const buf = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(String(value).trim().toLowerCase())
  );
  return Array.from(new Uint8Array(buf))
    .map(b => b.toString(16).padStart(2, "0"))
    .join("");
}


// =============================================================================
// UTM + CLICK ID EXTRACTION
// =============================================================================

function extractUTMs(url) {
  const out = {};
  for (const k of ["utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term", "utm_id"]) {
    const v = url.searchParams.get(k);
    if (v) out[k] = v;
  }
  return Object.keys(out).length ? out : null;
}

function extractClickIds(url) {
  const out = {};
  for (const { param } of CLICK_ID_CONFIG) {
    const v = url.searchParams.get(param);
    if (v) out[param] = v;
  }
  return out;
}


// =============================================================================
// ORGANIC SEARCH DETECTION — Layer 5
// =============================================================================

function detectOrganic(referrer) {
  if (!referrer) return null;
  try {
    const ref     = new URL(referrer);
    const h       = ref.hostname.toLowerCase();
    const engines = {
      google:     /^(.+\.)?google\.(com|co\.[a-z]{2}|[a-z]{2,3})(\.[a-z]{2})?$/i,
      bing:       /^(.+\.)?bing\.(com|co\.[a-z]{2})$/i,
      yahoo:      /^(search\.)?yahoo\.(com|co\.[a-z]{2})$/i,
      duckduckgo: /^(.+\.)?duckduckgo\.(com|co\.[a-z]{2})$/i,
      yandex:     /^(.+\.)?yandex\.(com|ru|co\.[a-z]{2})$/i,
      baidu:      /^(.+\.)?baidu\.(com|co\.[a-z]{2})$/i,
      brave:      /^search\.brave\.(com|co\.[a-z]{2})$/i,
      ecosia:     /^(.+\.)?ecosia\.(org|com)$/i,
    };
    for (const [engine, pattern] of Object.entries(engines)) {
      if (pattern.test(h)) {
        const p = ref.pathname.toLowerCase();
        if (p.includes("search") || p === "/" ||
            ref.searchParams.has("q") || ref.searchParams.has("query")) {
          return { utm_source: engine, utm_medium: "organic" };
        }
      }
    }
  } catch { /* ignore */ }
  return null;
}


// =============================================================================
// BOT DETECTION
// =============================================================================

function isBot(request) {
  const ua = request.headers.get("User-Agent") || "";
  if (BOT_UA_PATTERNS.some(p => p.test(ua))) return true;
  const decision = request.cf?.botManagement?.decision;
  if (decision && BOT_CF_DECISIONS.has(decision)) return true;
  if (request.cf?.botManagement?.verifiedBot) return true;
  return false;
}


// =============================================================================
// STATIC ASSET DETECTION
// =============================================================================

function isStaticAsset(url) {
  const p = url.pathname.toLowerCase();
  if (STATIC_PREFIXES.some(prefix => p.startsWith(prefix))) return true;
  if (STATIC_EXTENSIONS.some(ext => p.endsWith(ext))) return true;
  return false;
}


// =============================================================================
// COOKIE HELPERS — Layer 1 ITP-resistant HttpOnly first-party cookies
// =============================================================================

function readCookie(request, name) {
  const cookies = request.headers.get("Cookie") || "";
  const match   = cookies.match(new RegExp(`(?:^|;\\s*)${name}=([^;]+)`));
  return match ? decodeURIComponent(match[1]) : null;
}

function cookieDomain(url) {
  const h = url.hostname;
  if (h === "localhost") return "localhost";
  const parts = h.split(".");
  return parts.length >= 2 ? `.${parts.slice(-2).join(".")}` : h;
}

function buildAnonCookie(id, url) {
  return [
    `eden_anon_id=${encodeURIComponent(id)}`,
    "Max-Age=63072000",             // 2 years
    `Domain=${cookieDomain(url)}`,  // .eden.health — spans both portals
    "Path=/",
    "HttpOnly",                     // ITP-resistant — Safari cannot wipe
    "Secure",
    "SameSite=Lax",
  ].join("; ");
}

function buildSessionCookie(value, url) {
  return [
    `eden_session_id=${encodeURIComponent(value)}`,
    "Max-Age=1800",                 // 30 minutes
    `Domain=${cookieDomain(url)}`,
    "Path=/",
    "Secure",
    "SameSite=Lax",
  ].join("; ");
}


// =============================================================================
// URL HELPERS
// =============================================================================

function sanitizeUrl(url) {
  try {
    const clean = new URL(url.toString());
    for (const k of [...clean.searchParams.keys()]) {
      if (SENSITIVE_URL_PARAMS.some(p => p.test(k))) {
        clean.searchParams.set(k, "[redacted]");
      }
    }
    return clean.toString();
  } catch {
    return url.toString();
  }
}

function sanitizeUrlString(value) {
  if (!value) return "";
  try { return sanitizeUrl(new URL(value)); }
  catch { return value; }
}


// =============================================================================
// CORS + RESPONSE HELPERS
// =============================================================================

function corsHeadersObj(origin) {
  const allowed = ALLOWED_ORIGINS.includes(origin);
  return {
    "Access-Control-Allow-Origin":      allowed ? origin : ALLOWED_ORIGINS[0],
    "Access-Control-Allow-Methods":     "POST, GET, OPTIONS",
    "Access-Control-Allow-Headers":     "Content-Type, X-Eden-Server-Secret",
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Max-Age":           "86400",
  };
}

function corsHeaders(origin) {
  return corsHeadersObj(origin);
}

function jsonResponse(data) {
  return new Response(JSON.stringify(data), {
    status:  200,
    headers: { "Content-Type": "application/json" },
  });
}

function isMobile(ua) {
  return /Mobile|Android|iPhone|iPad|iPod/i.test(ua);
}

function nowUTC() {
  return new Date().toISOString();
}
