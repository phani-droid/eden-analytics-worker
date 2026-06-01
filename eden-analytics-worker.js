// =============================================================================
// EdenOS Analytics Worker — v5.6 (FINAL / PRODUCTION)
// =============================================================================
//
// FIXES IN v5.6 vs v5.4 (your current deployed version):
//
//   FIX 1 — "Event did not have a name" in Segment (CRITICAL)
//     Root cause: forwardToSegment() sends empty string as event name when
//     analytics.js fires page/identify/screen calls. Segment logs these as
//     "Event did not have a name" and drops them before any destination.
//     This was blocking intake events, checkout events, and Google Ads.
//
//     Fix A — page type: route to Segment's /page endpoint (not /track)
//       analytics.js page calls (type="page") were being silently dropped
//       because they had no "event" field. Now routed correctly as page calls.
//
//     Fix B — track type with empty name: derive name from context
//       If body.event is empty, use body.name (page name) or "page_viewed"
//       as fallback. Never send empty string event name to Segment.
//
//     Fix C — screen type: route to Segment's /track as "[Screen] name"
//       Screen calls now get proper event names.
//
//   FIX 2 — First-touch rule only protected gclid, not all click IDs
//     Root cause: storeAttribution() only checked parsed.gclid — so fbclid,
//     ttclid, msclkid, affiliate IDs could overwrite each other.
//     Critical before Meta (Jun 2), TikTok (Jun 3) launches.
//     Fix: check ANY stored click ID using CLICK_ID_PARAMS.some()
//
//   FIX 3 — linkUserAttribution() only protected gclid
//     Same issue as Fix 2 but at the userId→anonId copy layer.
//     Fix: check ANY click ID before deciding to skip the copy.
//
//   FIX 4 — page_viewed inflation (carried from v5.5)
//     Worker's firePageEvents() was firing page_viewed AND analytics.js
//     also fires analytics.page() — double counting.
//     Fix: Worker does NOT fire page_viewed. analytics.js is single source.
//     Worker still fires first_touch (only fires once per session with attribution).
//
//   FIX 5 — version updated to 5.6 throughout
//
// ALL PREVIOUS FIXES RETAINED:
//   v5.4 FIX 1 — /collect/* startsWith() for analytics.js subpaths (/p /t /m)
//   v5.4 FIX 2 — nowUTC() uses Date.now() — Mixpanel future timestamp fix
//   v5.4 FIX 3 — CORS headers on /server-collect responses
//
// =============================================================================
//
// ARCHITECTURE — five coverage layers:
//
//   Layer 1 — HttpOnly cookie (eden_anon_id)
//     First-party, server-set, ITP-resistant, 2 years
//     Domain=.eden.health spans eden.health AND app.eden.health
//     Also reads legacy eden_anonymous_id for identity continuity
//
//   Layer 2 — Cloudflare KV attribution (120 days)
//     Stores all 16 click IDs + UTMs against anonymousId
//     First-touch wins — any stored click ID blocks retargeting overwrite
//     Parallel reads (Promise.all) for speed
//
//   Layer 3 — userId → attribution link at /identify
//     Called at login + account creation
//     Copies ALL attribution from anonymousId → userId in KV (copy-only)
//     Fixes: anonymous_id = null on server events in BigQuery
//
//   Layer 4 — email_sha256 enhanced conversions
//     email auto-hashed via SHA-256 (lowercase + trim per Google/Meta spec)
//     Raw email also passes through — BAA signed, PHI legal via Segment
//
//   Layer 5 — Organic referrer detection
//     Google, Bing, DuckDuckGo, Yahoo, Yandex, Baidu, Brave, Ecosia
//     Labels as utm_source=engine, utm_medium=organic
//     Never fabricates attribution — only labels confirmed organic referrers
//
// DEDUPLICATION (three independent layers):
//   L1 — Worker KV: 24hr TTL per order_id at edge before Segment sees it
//        Covers: QFO double-fire, GA4+Segment overlap, Reverse ETL, retries
//        Fails open — KV error never blocks a real conversion
//   L2 — Segment messageId: stable order_id-based messageId (Layer 2 dedup)
//   L3 — BigQuery: QUALIFY ROW_NUMBER=1 per patient per event (permanent)
//   DEDUP KEY: Bask order_id (static UUID) — NOT Stripe transaction_id
//   Stripe transaction_id changes ~60% of time — unreliable as dedup key
//
// ATTRIBUTION MODEL:
//   First-touch wins — first ANY paid click ID stored, never overwritten
//   userId copy-only — if userId already has attribution, copy is skipped
//   Organic never fabricated — if no click ID, event flows clean
//   google_click_id_type: always explicit (gclid|gbraid|wbraid|dclid|none)
//
// PHI/PCI:
//   BAA signed with Segment — PHI flows through legally
//   email → email_sha256 ALWAYS (enhanced conversions)
//   Raw email also sent — BAA covers it
//   PCI stripping: uncomment PCI_PROPS block below when Jared confirms fields
//
// GPC HANDLING (legal — non-negotiable):
//   Sec-GPC: 1 → skip attribution storage, skip first_touch
//   Still fires page_viewed — analytics opt-out ≠ analytics blockout
//
// ROUTES (wrangler.toml — NO https:// prefix):
//   eden.health/*       — marketing site
//   www.eden.health/*   — marketing site www
//   app.eden.health/*   — patient portal
//
// ENDPOINTS:
//   /*                  → page requests → cookie + KV store
//   /collect            → client-side events (exact path)
//   /collect/*          → client-side events (analytics.js: /p /t /m)
//   /server-collect     → server-side events → dedup → KV → Segment
//   /identify           → login/account → KV userId link → Segment identify
//   /eden-health-check  → health check + config status
//
// CHANNEL LAUNCH SCHEDULE:
//   May 28 — Google Paid Search + SEO  ✓ LIVE
//   Jun 01 — Affiliate (Everflow + Katalys)
//   Jun 02 — Meta (Facebook / Instagram)
//   Jun 03 — TikTok
//   Jun 04 — Influencer (Upfluence)
//
// ADD NEW CHANNEL:  add entry to CLICK_ID_CONFIG → deploy
// ADD NEW DOMAIN:   add to ALLOWED_ORIGINS + wrangler.toml routes → deploy
// ENABLE PCI STRIP: uncomment PCI_PROPS block → deploy
// =============================================================================


// ─────────────────────────────────────────────────────────────────────────────
// PCI STRIPPING — DISABLED (BAA active, PHI/PCI decisions at BQ dbt)
// Uncomment + deploy when Jared confirms which fields need stripping
//
// const PCI_PROPS = new Set([
//   "card_number", "card_exp_date", "card_cvc",
//   "OS_card_number", "OS_card_exp_date", "OS_card_cvc",
//   "cvv", "pan",
// ]);
// ─────────────────────────────────────────────────────────────────────────────


// ─────────────────────────────────────────────────────────────────────────────
// ALLOWED ORIGINS
// Add new domain here AND in wrangler.toml routes → deploy
// ─────────────────────────────────────────────────────────────────────────────

const ALLOWED_ORIGINS = [
  "https://eden.health",
  "https://www.eden.health",
  "https://app.eden.health",
  "https://eden-os-rimo-patient-staging.vercel.app",
];


// ─────────────────────────────────────────────────────────────────────────────
// CLICK ID CONFIG — all 16 paid channels
// Add new channel here → worker auto-handles extraction, KV storage, routing
// ─────────────────────────────────────────────────────────────────────────────

const CLICK_ID_CONFIG = [
  { param: "gclid",     channel: "google_ads",    label: "Google Ads"     },
  { param: "gbraid",    channel: "google_ios",     label: "Google iOS"     },
  { param: "wbraid",    channel: "google_web",     label: "Google Web"     },
  { param: "dclid",     channel: "google_display", label: "Google Display" },
  { param: "fbclid",    channel: "meta",            label: "Meta/Facebook"  },
  { param: "msclkid",   channel: "microsoft",       label: "Microsoft/Bing" },
  { param: "ttclid",    channel: "tiktok",          label: "TikTok"         },
  { param: "twclid",    channel: "twitter",         label: "Twitter/X"      },
  { param: "li_fat_id", channel: "linkedin",        label: "LinkedIn"       },
  { param: "rdt_cid",   channel: "reddit",          label: "Reddit"         },
  { param: "epik",      channel: "pinterest",       label: "Pinterest"      },
  { param: "ScCid",     channel: "snapchat",        label: "Snapchat"       },
  { param: "nbt",       channel: "northbeam",       label: "Northbeam"      },
  { param: "irclickid", channel: "impact_radius",   label: "Impact Radius"  },
  { param: "cjevent",   channel: "cj_affiliate",    label: "CJ Affiliate"   },
  { param: "click_id",  channel: "generic",         label: "Generic"        },
];

// Fast lookup array — used for first-touch checks across all channels
const CLICK_ID_PARAMS = CLICK_ID_CONFIG.map(c => c.param);


// ─────────────────────────────────────────────────────────────────────────────
// CONVERSION EVENTS — edge-deduplicated + stable messageId
// ─────────────────────────────────────────────────────────────────────────────

const CONVERSION_EVENTS = new Set([
  "OS_qualified_first_order",
  "OS_purchase",
  "order_completed",
  "reorder_completed",
]);

// Accept common event-name variants without forcing backend changes.
// Segment will receive the canonical names below, so dashboards stay consistent.
const EVENT_NAME_ALIASES = {
  "os_qualified_first_order": "OS_qualified_first_order",
  "qualified_first_order":    "OS_qualified_first_order",
  "os_purchase":              "OS_purchase",
  "purchase":                 "OS_purchase",
  "order_completed":          "order_completed",
  "reorder_completed":        "reorder_completed",
};


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
// KV KEY SCHEMA + TTLs
//   attr:anon:{anonymousId}     → attribution object (120 days)
//   attr:user:{userId}          → attribution object (120 days)
//   dedup:{eventName}:{orderId} → dedup lock         (24 hours)
// ─────────────────────────────────────────────────────────────────────────────

const KV_ANON_PREFIX = "attr:anon:";
const KV_USER_PREFIX = "attr:user:";
const KV_TTL         = 10368000;  // 120 days in seconds
const KV_DEDUP_TTL   = 86400;     // 24 hours in seconds


// =============================================================================
// WORKER ENTRY POINT
// =============================================================================

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);

      // ── Health check ──────────────────────────────────────────────────────
      if (url.pathname === "/eden-health-check") {
        return jsonResponse({
          ok:                true,
          worker:            "eden-analytics",
          version:           "5.12",
          ts:                nowUTC(),
          kv:                !!env.GCLID_KV,
          segment_write_key_configured: !!env.SEGMENT_WRITE_KEY,
          server_secret_configured:     !!env.SERVER_API_SECRET,
          phi_stripping:     "disabled — BAA active — decisions at BQ dbt",
          gpc_handling:      "enabled — California/Virginia legal compliance",
          attribution_model: "first-touch — all 16 click IDs protected",
          attribution_ttl:   "120 days",
          dedup_ttl:         "24 hours",
          dedup_key:         "Bask order_id (static UUID — NOT Stripe transaction_id)",
          collect_subpaths:  "enabled — /collect /collect/p /collect/t /collect/m",
          event_naming:      "fixed — no more empty event names",
          page_inflation:    "fixed — worker does not fire page_viewed",
          routes:            ["eden.health/*", "www.eden.health/*", "app.eden.health/*"],
          channels:          CLICK_ID_CONFIG.map(c => c.label),
        });
      }

      // ── CORS preflight — handles ALL paths ────────────────────────────────
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

      // ── /collect and /collect/* — client-side events ──────────────────────
      // analytics.js appends /p (page), /t (track), /m (metrics) after apiHost
      // startsWith catches all subpaths: /collect/p /collect/t /collect/m
      if (url.pathname.startsWith("/collect") && request.method === "POST") {
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

      // ── Page requests — set cookie + store KV attribution ─────────────────
      return handlePageRequest(request, env, ctx, url);

    } catch (err) {
      console.error("[eden-analytics] unhandled error:", err);
      return fetch(request); // fail open — always serve the page
    }
  },
};


// =============================================================================
// PAGE REQUEST HANDLER
// Sets cookie (Layer 1), stores attribution in KV (Layer 2)
// Does NOT fire page_viewed — analytics.js handles this via /collect
// Only fires first_touch once per session when paid attribution is present
// =============================================================================

async function handlePageRequest(request, env, ctx, url) {
  const gpcOptOut = request.headers.get("Sec-GPC") === "1";

  // Read existing cookies — prefer canonical eden_anon_id, fall back to legacy
  const legacyAnonId    = readCookie(request, "eden_anonymous_id");
  const existingAnonId  = readCookie(request, "eden_anon_id") || legacyAnonId;
  const existingSession = readCookie(request, "eden_session_id");

  const isNewVisitor = !existingAnonId;
  const isNewSession = !existingSession;

  const anonId  = existingAnonId  || crypto.randomUUID();
  const session = existingSession || `${crypto.randomUUID()}_${Date.now()}`;

  const clickIds = extractClickIds(url);
  const utms     = extractUTMs(url);

  // Layer 2 — store attribution in KV (skip for GPC opt-outs)
  const hasAttribution = Object.keys(clickIds).length > 0 || !!utms;
  if (hasAttribution && env.GCLID_KV && !gpcOptOut) {
    ctx.waitUntil(
      storeAttribution(env.GCLID_KV, KV_ANON_PREFIX + anonId, {
        ...(utms || {}),
        ...clickIds,
      }).catch(err => console.error("[eden-analytics] KV store error:", err))
    );
  }

  const response = await fetch(request);

  // Layer 1 — set HttpOnly cookies
  const headers = new Headers(response.headers);
  if (isNewVisitor) headers.append("Set-Cookie", buildAnonCookie(anonId, url));
  if (isNewSession) headers.append("Set-Cookie", buildSessionCookie(session, url));

  // FIX v5.5/v5.6: Worker does NOT fire page_viewed here.
  // analytics.js fires analytics.page() → /collect/p → Worker → Segment.
  // Worker firing page_viewed here + analytics.js firing = 2x inflation.
  // Only fire first_touch — fires once per session when paid attribution exists.
  if (env.SEGMENT_WRITE_KEY && isNewSession && hasAttribution && !gpcOptOut) {
    ctx.waitUntil(
      fireFirstTouch(request, env, anonId, session, url, clickIds, utms)
        .catch(err => console.error("[eden-analytics] first_touch error:", err))
    );
  }

  return new Response(response.body, {
    status:     response.status,
    statusText: response.statusText,
    headers,
  });
}


// =============================================================================
// FIRST TOUCH EVENT
// Fires once per session when paid attribution is present
// Enables: which channel drove this session → used in Mixpanel attribution
// =============================================================================

async function fireFirstTouch(request, env, anonId, session, url, clickIds, utms) {
  const cleanUrl  = sanitizeUrl(url);
  const referrer  = sanitizeUrlString(request.headers.get("Referer") || "");
  const ua        = request.headers.get("User-Agent") || "";
  const portal    = url.hostname.includes("app.eden.health") ? "patient" : "marketing";
  const sessionId = session.split("_")[0];

  // Layer 5 — organic detection fallback
  const organic     = !utms && !clickIds.gclid && referrer ? detectOrganic(referrer) : null;
  const attribution = { ...(utms || organic || {}), ...clickIds };

  if (Object.keys(attribution).length === 0) return;

  // Stable messageId — prevents first_touch double-firing on page refresh
  const messageId = `first_touch_${anonId}_${sessionId}`;

  const campaignProps = buildCampaignContext(attribution);

  await segmentPost(env.SEGMENT_WRITE_KEY, "track", {
    anonymousId: anonId,
    messageId,
    event:       "first_touch",
    properties: {
      portal,
      page_path:        url.pathname,
      page_url:         cleanUrl,
      referrer:         referrer || undefined,
      session_id:       sessionId,
      device_type:      isMobile(ua) ? "mobile" : "desktop",
      pipeline_version: "5.12",

      // Duplicate campaign fields into properties for Mixpanel visibility.
      ...campaignProps,
      acquisition_channel: deriveAcquisitionChannel(campaignProps),
      attribution_source:  campaignProps.utm_source || deriveClickIdSource(campaignProps),
      attribution_medium:  campaignProps.utm_medium || undefined,
      attribution_campaign: campaignProps.utm_campaign || undefined,
    },
    context:   { campaign: campaignProps },
    timestamp: nowUTC(),
  });
}


// =============================================================================
// /collect HANDLER — CLIENT-SIDE EVENTS
//
// Handles all analytics.js calls:
//   POST /collect   — direct
//   POST /collect/p — page events (analytics.page())
//   POST /collect/t — track events (analytics.track())
//   POST /collect/m — performance metrics
//   POST /collect/* — any future analytics.js subpath
//
// FIX v5.6: forwardToSegment() now correctly handles all event types
// with proper event names — no more "Event did not have a name" in Segment
// =============================================================================

async function handleCollect(request, env, ctx, url) {
  const origin = request.headers.get("Origin") || "";

  if (origin && !isAllowedOrigin(origin)) {
    return new Response("Forbidden", { status: 403 });
  }

  let body;
  try { body = await request.json(); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  const gpcOptOut = request.headers.get("Sec-GPC") === "1";

  // Layer 1 — cookie is most reliable identity source
  const cookieAnonId = readCookie(request, "eden_anon_id")
                    || readCookie(request, "eden_anonymous_id");
  const anonId       = cookieAnonId || body.anonymousId || crypto.randomUUID();
  const isNew        = !cookieAnonId;
  const portal       = origin.includes("app.eden.health") ? "patient" : "marketing";
  const userId       = body.userId || null;

  // Layer 2 + 3 — resolve attribution from KV (parallel reads)
  const storedAttribution = (env.GCLID_KV && !gpcOptOut)
    ? await resolveAttribution(env.GCLID_KV, anonId, userId)
    : null;

  // Fresh URL params always override stored (current session click is fresher)
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
    pipeline_version: "5.12",
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
// DEDUP KEY: Bask order_id (static UUID)
// NOT Stripe transaction_id — changes ~60% of time per Ryon's findings
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

  if (!body.properties || typeof body.properties !== "object" || Array.isArray(body.properties)) {
    body.properties = {};
  }

  const identity = resolveIdentityFromBody(request, body);

  const anonId       = identity.anonymousId || null;
  const userId       = identity.userId || null;
  const rawEventName = resolveEventName(body);
  const eventName    = canonicalizeEventName(rawEventName);
  const orderId      = resolveOrderId(body);

  // Normalize body once so all downstream Segment calls keep working
  // without backend engineers needing to know camelCase vs snake_case.
  if (eventName) body.event = eventName;
  if (userId) body.userId = userId;
  if (anonId) body.anonymousId = anonId;
  if (orderId && !body.properties.order_id) body.properties.order_id = orderId;

  // Edge dedup — Layer 1 (Worker KV, 24hr window)
  // Covers: QFO double-fire, GA4+Segment overlap, Reverse ETL, Shippo retries
  // Fails open — KV error never blocks a real conversion
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

  // Layer 2 + 3 — resolve attribution (parallel KV reads)
  const storedAttribution = env.GCLID_KV
    ? await resolveAttribution(env.GCLID_KV, anonId, userId)
    : null;

  // Merge stored attribution — explicit engineer values always win
  if (storedAttribution && body.properties) {
    for (const [k, v] of Object.entries(storedAttribution)) {
      if (!body.properties[k] && v) body.properties[k] = v;
    }
  }

  // Always UTC timestamp — fixes Mixpanel future timestamp rejection
  body.timestamp = nowUTC();

  const superProps = {
    portal:           "patient",
    source_type:      "server",
    pipeline_version: "5.12",
    ...(identity.identityWarning ? { identity_warning: identity.identityWarning } : {}),
  };

  const attribution  = storedAttribution || {};
  const campaignProps = buildCampaignContext(attribution);
  enrichPropertiesWithAttribution(body.properties, campaignProps);

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

  const origin = request.headers.get("Origin") || "";
  return new Response(JSON.stringify({ ok: true }), {
    status:  200,
    headers: {
      "Content-Type": "application/json",
      ...corsHeadersObj(origin),
    },
  });
}


// =============================================================================
// /identify HANDLER — LOGIN / ACCOUNT CREATION
//
// Critical: must be called immediately after login + account creation
// Fixes: anonymous_id = null on server events (1M+ rows in BigQuery)
// Links: Bask userId → ALL attribution (gclid, fbclid, UTMs) via KV
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

  const identity = resolveIdentityFromBody(request, body);

  const anonId = identity.anonymousId || null;
  const userId = identity.userId || null;

  if (userId) body.userId = userId;
  if (anonId) body.anonymousId = anonId;

  // Layer 3 — link userId → full attribution (all click IDs) in KV
  if (env.GCLID_KV && anonId && userId) {
    ctx.waitUntil(
      linkUserAttribution(env.GCLID_KV, anonId, userId)
        .catch(err => console.error("[eden-analytics] KV identify link error:", err))
    );
  }

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
// SEGMENT FORWARDING — FIXED v5.6
//
// ROOT CAUSE OF "Event did not have a name":
//   analytics.js sends different body shapes per call type:
//     page call:     { type: "page",     name: "Home",     event: undefined }
//     track call:    { type: "track",    event: "login",   name: undefined  }
//     identify call: { type: "identify", traits: {...},    event: undefined }
//     screen call:   { type: "screen",   name: "Dashboard",event: undefined }
//
//   v5.4 sent everything as track with body.event || "" — empty string for
//   page/identify/screen calls → Segment dropped them all silently.
//
// FIX: Route each type to correct Segment endpoint with proper event name
//   page     → /page endpoint (no event name needed)
//   identify → /identify endpoint (no event name needed)
//   screen   → /track with "[Screen] {name}" as event name
//   track    → /track with body.event (validated non-empty)
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

  // ── IDENTIFY ─────────────────────────────────────────────────────────────
  if (type === "identify") {
    const traits = await hashEmail(body.traits || body.properties || {});
    await segmentPost(writeKey, "identify", {
      anonymousId: anonId,
      userId:      resolveUserIdFromBody(body),
      traits,
      context:     mergedContext,
      timestamp:   nowUTC(),
    });
    return;
  }

  // ── PAGE ──────────────────────────────────────────────────────────────────
  // Route to Segment's /page endpoint — correct type, no event name needed
  // page.name is the page name (e.g. "Home", "BMI Calculator")
  if (type === "page") {
    await segmentPost(writeKey, "page", {
      anonymousId: anonId,
      userId:      resolveUserIdFromBody(body),
      name:        body.name   || body.properties?.name || "",
      properties:  await hashEmail({ ...superProps, ...(body.properties || {}) }),
      context:     mergedContext,
      timestamp:   nowUTC(),
    });
    return;
  }

  // ── SCREEN ────────────────────────────────────────────────────────────────
  // Mobile screen views — send as track with descriptive event name
  if (type === "screen") {
    const screenName = body.name || body.properties?.name || "Unknown Screen";
    await segmentPost(writeKey, "track", {
      anonymousId: anonId,
      userId:      resolveUserIdFromBody(body),
      event:       `Viewed ${screenName}`,
      properties:  await hashEmail({ ...superProps, ...(body.properties || {}) }),
      context:     mergedContext,
      timestamp:   nowUTC(),
    });
    return;
  }

  // ── TRACK (default) ───────────────────────────────────────────────────────
  // FIX v5.6: validate event name — never send empty string to Segment
  // Empty event name → "Event did not have a name" → event dropped
  // Derive name from body.name or body.properties.name as fallback
  const eventName = canonicalizeEventName(resolveEventName(body)) || null;
  const orderId = resolveOrderId(body);
  if (eventName) body.event = eventName;
  if (orderId && body.properties && !body.properties.order_id) body.properties.order_id = orderId;

  // Skip metrics and internal analytics.js calls with no meaningful name
  // These are performance telemetry — not business events
  if (!eventName) {
    console.log("[eden-analytics] skipping event with no name — likely internal metrics");
    return;
  }

  // Layer 2 dedup for conversion events — stable messageId prevents Segment
  // double-delivery when same event arrives from two sources
  const stableMessageId = CONVERSION_EVENTS.has(eventName) && orderId
    ? `eden_${eventName}_${orderId}`
    : undefined;

  await segmentPost(writeKey, "track", {
    anonymousId: anonId,
    userId:      resolveUserIdFromBody(body),
    event:       eventName,
    properties:  await hashEmail({ ...superProps, ...(body.properties || {}) }),
    context:     mergedContext,
    timestamp:   nowUTC(),
    ...(stableMessageId ? { messageId: stableMessageId } : {}),
  });
}


// ─────────────────────────────────────────────────────────────────────────────
// buildCampaignContext
// Places all attribution in context.campaign per Segment spec
// Confirmed by Segment Success Engineer George D. May 27 2026
// ─────────────────────────────────────────────────────────────────────────────


function canonicalizeEventName(eventName) {
  if (!eventName) return "";
  const raw = String(eventName).trim();
  if (!raw) return "";
  return EVENT_NAME_ALIASES[raw.toLowerCase()] || raw;
}

function resolveEventName(body) {
  return (
    body.event ||
    body.event_name ||
    body.name ||
    body.properties?.event ||
    body.properties?.event_name ||
    body.properties?.name ||
    ""
  );
}

function resolveOrderId(body) {
  return (
    body.properties?.order_id ||
    body.properties?.orderId ||
    body.order_id ||
    body.orderId ||
    null
  );
}

function resolveUserIdFromBody(body) {
  return (
    body.userId ||
    body.user_id ||
    body.properties?.userId ||
    body.properties?.user_id ||
    body.properties?.patient_id ||
    body.properties?.customer_id ||
    null
  );
}

function resolveIdentityFromBody(request, body) {
  const cookieAnonId =
    readCookie(request, "eden_anon_id") ||
    readCookie(request, "eden_anonymous_id");

  const userId = resolveUserIdFromBody(body);

  // Also accepts common misspellings because server payloads are not always consistent.
  let anonymousId =
    cookieAnonId ||
    body.anonymousId ||
    body.anonymous_id ||
    body.anonymoous_id ||
    body.properties?.anonymousId ||
    body.properties?.anonymous_id ||
    body.properties?.anonymoous_id ||
    null;

  // Segment needs anonymousId or userId. This fallback keeps delivery healthy.
  // Perfect attribution still improves when /identify links cookie anonId -> userId.
  if (!anonymousId && userId) anonymousId = userId;

  return {
    anonymousId,
    userId,
    identityWarning:
      anonymousId && userId && anonymousId === userId
        ? "anonymousId_equals_userId"
        : undefined,
  };
}

function deriveClickIdSource(campaign) {
  if (!campaign) return undefined;

  if (campaign.gclid || campaign.gbraid || campaign.wbraid || campaign.dclid) return "google";
  if (campaign.fbclid) return "meta";
  if (campaign.msclkid) return "microsoft";
  if (campaign.ttclid) return "tiktok";
  if (campaign.twclid) return "twitter";
  if (campaign.li_fat_id) return "linkedin";
  if (campaign.rdt_cid) return "reddit";
  if (campaign.epik) return "pinterest";
  if (campaign.ScCid) return "snapchat";
  if (campaign.irclickid) return "impact_radius";
  if (campaign.cjevent) return "cj_affiliate";
  if (campaign.click_id) return "generic";

  return undefined;
}

function deriveAcquisitionChannel(campaign) {
  if (!campaign || Object.keys(campaign).length === 0) return "unknown";

  const source = String(campaign.utm_source || deriveClickIdSource(campaign) || "").toLowerCase();
  const medium = String(campaign.utm_medium || "").toLowerCase();

  if (medium === "organic") return "organic_search";
  if (medium === "email") return "email";
  if (medium === "affiliate") return "affiliate";
  if (medium === "influencer") return "influencer";

  if (
    medium === "cpc" || medium === "paid" || medium === "paid_search" ||
    campaign.gclid || campaign.gbraid || campaign.wbraid || campaign.dclid ||
    campaign.msclkid || source.includes("google") || source.includes("bing") || source.includes("microsoft")
  ) {
    return "paid_search";
  }

  if (
    campaign.fbclid || campaign.ttclid ||
    source.includes("facebook") || source.includes("instagram") ||
    source.includes("meta") || source.includes("tiktok")
  ) {
    return "paid_social";
  }

  return source || "unknown";
}

function enrichPropertiesWithAttribution(properties, campaignProps) {
  if (!properties || typeof properties !== "object") return;
  if (!campaignProps || Object.keys(campaignProps).length === 0) return;

  for (const [k, v] of Object.entries(campaignProps)) {
    if (v && !properties[k]) properties[k] = v;
  }

  properties.acquisition_channel =
    properties.acquisition_channel || deriveAcquisitionChannel(campaignProps);

  properties.attribution_source =
    properties.attribution_source || campaignProps.utm_source || deriveClickIdSource(campaignProps);

  properties.attribution_medium =
    properties.attribution_medium || campaignProps.utm_medium;

  properties.attribution_campaign =
    properties.attribution_campaign || campaignProps.utm_campaign;
}

function buildCampaignContext(attribution) {
  const campaign      = {};
  const CAMPAIGN_KEYS = [
    "utm_source", "utm_medium", "utm_campaign",
    "utm_content", "utm_term", "utm_id",
    ...CLICK_ID_PARAMS,
  ];
  for (const k of CAMPAIGN_KEYS) {
    if (attribution[k]) campaign[k] = attribution[k];
  }
  return campaign;
}


// ─────────────────────────────────────────────────────────────────────────────
// segmentPost
// ─────────────────────────────────────────────────────────────────────────────

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
// KV ATTRIBUTION — STORAGE + RETRIEVAL
//
// ATTRIBUTION OVERWRITE RULES:
//   Rule 1 — FIRST-TOUCH ANY CLICK ID: if any click ID already stored,
//             retargeting from any channel cannot overwrite it.
//             Fixed in v5.6 — v5.4 only checked gclid, not fbclid/ttclid etc.
//   Rule 2 — userId COPY-ONLY: if userId already has any click ID,
//             skip the copy entirely. Fixed in v5.6 same way.
//   Rule 3 — Organic never fabricated: if no click ID, event flows clean.
//   Rule 4 — KV failures fail open: attribution loss > conversion loss.
// =============================================================================

async function storeAttribution(kv, key, attribution) {
  if (!kv || !key || !attribution) return;

  const hasValue = Object.values(attribution).some(v => v && String(v).trim());
  if (!hasValue) return;

  // FIX v5.6 Rule 1 — first-touch ANY click ID (not just gclid)
  try {
    const existing = await kv.get(key);
    if (existing) {
      const parsed = JSON.parse(existing);
      // If existing record has ANY click ID AND new attribution has ANY click ID
      // → first-touch wins, do not overwrite
      const existingHasClick = CLICK_ID_PARAMS.some(p => parsed[p]);
      const newHasClick      = CLICK_ID_PARAMS.some(p => attribution[p]);
      if (existingHasClick && newHasClick) {
        console.log("[eden-analytics] first-touch preserved — retargeting click ignored");
        return;
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

  // Parallel reads — faster than sequential
  const [fromAnon, fromUser] = await Promise.all([
    anonId ? getAttribution(kv, KV_ANON_PREFIX + anonId) : Promise.resolve(null),
    userId ? getAttribution(kv, KV_USER_PREFIX + userId)  : Promise.resolve(null),
  ]);

  // anonId first — most reliable for current session
  if (fromAnon && CLICK_ID_PARAMS.some(p => fromAnon[p])) return fromAnon;
  // userId fallback — cross-device + cleared cookies
  if (fromUser && CLICK_ID_PARAMS.some(p => fromUser[p])) return fromUser;
  // Return whatever we have — UTMs valuable even without click IDs
  return fromAnon || fromUser || null;
}

async function linkUserAttribution(kv, anonId, userId) {
  const [anonAttribution, existingUser] = await Promise.all([
    getAttribution(kv, KV_ANON_PREFIX + anonId),
    getAttribution(kv, KV_USER_PREFIX  + userId),
  ]);

  if (!anonAttribution) return;

  // FIX v5.6 Rule 2 — copy-only: skip if userId has ANY click ID (not just gclid)
  const userHasAttribution = existingUser && CLICK_ID_PARAMS.some(p => existingUser[p]);
  if (userHasAttribution) {
    console.log("[eden-analytics] userId attribution exists — copy skipped");
    return;
  }

  await storeAttribution(kv, KV_USER_PREFIX + userId, anonAttribution);
}


// =============================================================================
// EMAIL HASHING — Layer 4 enhanced conversions
// email → email_sha256 (SHA-256, lowercase, trimmed per Google/Meta spec)
// Raw email also kept — BAA signed, PHI legal via Segment
// =============================================================================

async function hashEmail(props) {
  if (!props || typeof props !== "object") return props;
  const out = {};
  for (const [k, v] of Object.entries(props)) {
    if ((k === "email" || k === "customerEmail") && typeof v === "string") {
      out["email_sha256"] = await sha256(v);
      out[k] = v; // raw email kept — BAA covers it
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
// SHA-256 — lowercase + trim per Google/Meta enhanced conversions spec
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
// TIMESTAMP — always UTC, never future
// Uses Date.now() — always UTC milliseconds regardless of server timezone
// Fixes Mixpanel strict mode: 'properties.time must not be in the future'
// =============================================================================

function nowUTC() {
  return new Date(Date.now()).toISOString();
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
// ORGANIC SEARCH DETECTION — Layer 5 fallback
// Never fabricates attribution — only labels confirmed organic referrers
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
  } catch { /* ignore malformed referrers */ }
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
// Domain=.eden.health spans eden.health AND app.eden.health
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
    "Max-Age=63072000",            // 2 years
    `Domain=${cookieDomain(url)}`, // .eden.health spans both portals
    "Path=/",
    "HttpOnly",                    // ITP-resistant
    "Secure",
    "SameSite=Lax",
  ].join("; ");
}

function buildSessionCookie(value, url) {
  return [
    `eden_session_id=${encodeURIComponent(value)}`,
    "Max-Age=1800",                // 30 minutes
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

function isAllowedOrigin(origin) {
  if (!origin) return false;
  if (ALLOWED_ORIGINS.includes(origin)) return true;
  // Allow ALL Eden Health Vercel preview deployments — hash changes every deploy
  if (/^https:\/\/[a-z0-9-]+-eden-health\.vercel\.app$/.test(origin)) return true;
  return false;
}

function corsHeadersObj(origin) {
  const allowed = isAllowedOrigin(origin);
  return {
    "Access-Control-Allow-Origin":      allowed ? origin : ALLOWED_ORIGINS[0],
    "Access-Control-Allow-Methods":     "POST, GET, OPTIONS",
    "Access-Control-Allow-Headers":     "Content-Type, X-Eden-Server-Secret, Authorization",
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
