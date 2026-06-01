// =============================================================================
// EdenOS Analytics Worker — v5.20 (PRODUCTION — FINAL)
// =============================================================================
//
// FIXES IN v5.20 vs v5.6 (all fixes cumulative):
//
//   FIX 8 — Checkly synthetic monitor pollution (v5.20)
//     Checkly injects fake click IDs (checkly-vwo-gclid) → UNPARSEABLE_GCLID
//     errors in Google Ads + pollutes KV with fake attribution.
//     Fix: isSyntheticMonitor() blocks Checkly at worker entry point.
//     Detects via: eden_checkly_marker param, utm_medium=synthetic,
//     utm_source contains "checkly", or User-Agent contains "checklyhq".
//
//   FIX 7 — Bridge logic missing from /collect (v5.8)
//     OS_purchase fires CLIENT-SIDE via analytics.js → /collect, not
//     /server-collect. Bridge was only in handleServerCollect → never
//     triggered → attr:user + attr:order never written → OS_order_delivered
//     still had no gclid → still 0 conversions.
//     Fix: Bridge added to handleCollect identically.
//
//   FIX 6 — OS_order_delivered zero conversions in Google Ads (v5.7)
//     /identify never called → attr:user:{userId} never written → server-side
//     events couldn't resolve gclid → Segment filter dropped 99.98% of events.
//     Fix A: resolveAttribution() accepts orderId as 3rd parallel KV lookup.
//     Fix B: OS_purchase writes attr:user:{userId} + attr:order:{orderId}.
//     Fix C: OS_order_delivered writes attr:user:{userId} opportunistically.
//
//   FIX 5 — version strings updated throughout (v5.20)
//
// ALL PREVIOUS FIXES RETAINED:
//   v5.6 FIX 1 — "Event did not have a name" — page/identify/screen routing
//   v5.6 FIX 2 — First-touch rule covers ALL click IDs (not just gclid)
//   v5.6 FIX 3 — linkUserAttribution() covers ALL click IDs
//   v5.6 FIX 4 — page_viewed inflation fix
//   v5.4 FIX 1 — /collect/* startsWith() for analytics.js subpaths
//   v5.4 FIX 2 — nowUTC() uses Date.now()
//   v5.4 FIX 3 — CORS headers on /server-collect responses
//
// =============================================================================
//
// ARCHITECTURE — five coverage layers:
//   Layer 1 — HttpOnly cookie (eden_anon_id) — 2 years, ITP-resistant
//   Layer 2 — Cloudflare KV attribution (120 days, first-touch, all 16 channels)
//   Layer 3 — userId → attribution link at /identify
//   Layer 4 — email_sha256 enhanced conversions
//   Layer 5 — Organic referrer detection
//
// KV KEY SCHEMA:
//   attr:anon:{anonymousId}     → attribution (120 days)
//   attr:user:{userId}          → attribution (120 days)
//   attr:order:{orderId}        → attribution (120 days) [v5.7]
//   dedup:{eventName}:{orderId} → dedup lock  (24 hours)
//
// ROUTES (wrangler.toml):
//   eden.health/*  |  www.eden.health/*  |  app.eden.health/*
//
// ENDPOINTS:
//   /*               → page requests → cookie + KV store
//   /collect         → client-side events (exact)
//   /collect/*       → client-side events (analytics.js subpaths)
//   /server-collect  → server-side events → dedup → KV → Segment
//   /identify        → login/signup → KV userId link → Segment identify
//   /eden-health-check → health status
// =============================================================================


// ─────────────────────────────────────────────────────────────────────────────
// PCI STRIPPING — DISABLED (BAA active, PHI/PCI decisions at BQ dbt)
// Uncomment + deploy when Jared confirms which fields need stripping
// ─────────────────────────────────────────────────────────────────────────────
// const PCI_PROPS = new Set([
//   "card_number", "card_exp_date", "card_cvc",
//   "OS_card_number", "OS_card_exp_date", "OS_card_cvc",
//   "cvv", "pan",
// ]);


// ─────────────────────────────────────────────────────────────────────────────
// ALLOWED ORIGINS
// ─────────────────────────────────────────────────────────────────────────────

const ALLOWED_ORIGINS = [
  "https://eden.health",
  "https://www.eden.health",
  "https://app.eden.health",
];


// ─────────────────────────────────────────────────────────────────────────────
// CLICK ID CONFIG — all 16 paid channels
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

const CLICK_ID_PARAMS = CLICK_ID_CONFIG.map(c => c.param);


// ─────────────────────────────────────────────────────────────────────────────
// CONVERSION EVENTS
// ─────────────────────────────────────────────────────────────────────────────

const CONVERSION_EVENTS = new Set([
  "OS_qualified_first_order",
  "OS_purchase",
  "order_completed",
  "reorder_completed",
]);

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
  /checklyhq/i,  // v5.20 — Checkly synthetic monitor UA
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
// ─────────────────────────────────────────────────────────────────────────────

const KV_ANON_PREFIX  = "attr:anon:";
const KV_USER_PREFIX  = "attr:user:";
const KV_ORDER_PREFIX = "attr:order:";  // v5.7
const KV_TTL          = 10368000;       // 120 days
const KV_DEDUP_TTL    = 86400;          // 24 hours


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
          ok:                          true,
          worker:                      "eden-analytics",
          version:                     "5.20",
          hardening_version:           "5.20-final-prod-only",
          ts:                          nowUTC(),
          kv:                          !!env.GCLID_KV,
          segment_write_key_configured: !!env.SEGMENT_WRITE_KEY,
          server_secret_configured:    !!env.SERVER_API_SECRET,
          phi_stripping:               "disabled — BAA active — decisions at BQ dbt",
          gpc_handling:                "enabled — California/Virginia legal compliance",
          attribution_model:           "first-touch — all 16 click IDs protected",
          attribution_ttl:             "120 days",
          dedup_ttl:                   "24 hours",
          dedup_key:                   "Bask order_id (static UUID — NOT Stripe transaction_id)",
          collect_subpaths:            "enabled — /collect /collect/p /collect/t /collect/m",
          event_naming:                "fixed — no more empty event names",
          page_inflation:              "fixed — worker does not fire page_viewed",
          order_bridge:                "enabled v5.8 — OS_purchase writes attr:order + attr:user in /collect + /server-collect",
          synthetic_monitor_block:     "enabled v5.20 — Checkly fake gclid pollution prevented",
          routes:                      ["eden.health/*", "www.eden.health/*", "app.eden.health/*"],
          channels:                    CLICK_ID_CONFIG.map(c => c.label),
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

      // ── Skip synthetic monitors (Checkly etc.) ────────────────────────────
      // Prevents fake click IDs from polluting KV + causing Google Ads errors
      if (isSyntheticMonitor(request, url)) {
        console.log("[eden-analytics] synthetic monitor blocked:", url.searchParams.get("utm_source") || "unknown");
        return fetch(request);
      }

      // ── Skip static assets ────────────────────────────────────────────────
      if (isStaticAsset(url)) return fetch(request);

      // ── /collect and /collect/* — client-side events ──────────────────────
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
      return fetch(request);
    }
  },
};


// =============================================================================
// PAGE REQUEST HANDLER
// Sets cookie (Layer 1), stores attribution in KV (Layer 2)
// Fires first_touch once per session when paid attribution present
// Does NOT fire page_viewed — analytics.js handles via /collect
// =============================================================================

async function handlePageRequest(request, env, ctx, url) {
  const gpcOptOut = request.headers.get("Sec-GPC") === "1";

  const legacyAnonId    = readCookie(request, "eden_anonymous_id");
  const existingAnonId  = readCookie(request, "eden_anon_id") || legacyAnonId;
  const existingSession = readCookie(request, "eden_session_id");

  const isNewVisitor = !existingAnonId;
  const isNewSession = !existingSession;

  const anonId  = existingAnonId  || crypto.randomUUID();
  const session = existingSession || `${crypto.randomUUID()}_${Date.now()}`;

  const clickIds = extractClickIds(url);
  const utms     = extractUTMs(url);

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

  const headers = new Headers(response.headers);
  if (isNewVisitor) headers.append("Set-Cookie", buildAnonCookie(anonId, url));
  if (isNewSession) headers.append("Set-Cookie", buildSessionCookie(session, url));

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
// Fires once per session when paid attribution present
// =============================================================================

async function fireFirstTouch(request, env, anonId, session, url, clickIds, utms) {
  const cleanUrl   = sanitizeUrl(url);
  const referrer   = sanitizeUrlString(request.headers.get("Referer") || "");
  const ua         = request.headers.get("User-Agent") || "";
  const portal     = url.hostname.includes("app.eden.health") ? "patient" : "marketing";
  const sessionId  = session.split("_")[0];

  const organic     = !utms && !clickIds.gclid && referrer ? detectOrganic(referrer) : null;
  const attribution = { ...(utms || organic || {}), ...clickIds };

  if (Object.keys(attribution).length === 0) return;

  const messageId    = `first_touch_${anonId}_${sessionId}`;
  const campaignProps = buildCampaignContext(attribution);

  await segmentPost(env.SEGMENT_WRITE_KEY, "track", {
    anonymousId: anonId,
    messageId,
    event:       "first_touch",
    properties: {
      portal,
      page_path:            url.pathname,
      page_url:             cleanUrl,
      referrer:             referrer || undefined,
      session_id:           sessionId,
      device_type:          isMobile(ua) ? "mobile" : "desktop",
      pipeline_version:     "5.20",
      ...campaignProps,
      acquisition_channel:  deriveAcquisitionChannel(campaignProps),
      attribution_source:   campaignProps.utm_source || deriveClickIdSource(campaignProps),
      attribution_medium:   campaignProps.utm_medium || undefined,
      attribution_campaign: campaignProps.utm_campaign || undefined,
    },
    context:   { campaign: campaignProps },
    timestamp: nowUTC(),
  });
}


// =============================================================================
// /collect HANDLER — CLIENT-SIDE EVENTS
//
// v5.8: Added OS_purchase bridge — writes attr:user + attr:order to KV
//   OS_purchase is client-side (analytics.js → /collect), so it has
//   the eden_anon_id cookie → can resolve gclid from attr:anon → bridges
//   to attr:user + attr:order so server-side events resolve gclid later.
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

  const cookieAnonId = readCookie(request, "eden_anon_id")
                    || readCookie(request, "eden_anonymous_id");
  const anonId       = cookieAnonId || body.anonymousId || body.anonymous_id || crypto.randomUUID();
  const isNew        = !cookieAnonId;
  const portal       = origin.includes("app.eden.health") ? "patient" : "marketing";
  const userId       = resolveUserIdFromBody(body);

  const storedAttribution = (env.GCLID_KV && !gpcOptOut)
    ? await resolveAttribution(env.GCLID_KV, anonId, userId)
    : null;

  const freshClickIds   = gpcOptOut ? {} : extractClickIds(url);
  const freshUTMs       = gpcOptOut ? null : extractUTMs(url);
  const contextCampaign = gpcOptOut ? {} : ((body.context || {}).campaign || {});
  const attribution     = {
    ...(storedAttribution || {}),
    ...contextCampaign,
    ...(freshUTMs         || {}),
    ...freshClickIds,
  };

  if (!body.properties || typeof body.properties !== "object" || Array.isArray(body.properties)) {
    body.properties = {};
  }

  const campaignProps = buildCampaignContext(attribution);
  enrichPropertiesWithAttribution(body.properties, campaignProps);

  const superProps = {
    portal,
    source_type:      "client",
    gpc_opt_out:      gpcOptOut,
    pipeline_version: "5.20",
  };

  // ── v5.8 OS_purchase bridge ───────────────────────────────────────────────
  // OS_purchase fires client-side with cookie → resolves attr:anon → has gclid
  // Write attr:user + attr:order so server-side OS_order_delivered resolves it
  const collectEventName = canonicalizeEventName(resolveEventName(body));
  const collectOrderId   = resolveOrderId(body);
  const collectUserId    = resolveUserIdFromBody(body);

  if (
    env.GCLID_KV &&
    !gpcOptOut &&
    collectEventName === "OS_purchase" &&
    attribution &&
    CLICK_ID_PARAMS.some(p => attribution[p])
  ) {
    ctx.waitUntil(
      Promise.all([
        collectUserId ? storeAttribution(
          env.GCLID_KV,
          KV_USER_PREFIX + collectUserId,
          attribution
        ).catch(err => console.error("[eden-analytics] collect purchase user-link error:", err))
        : Promise.resolve(),

        collectOrderId ? storeAttribution(
          env.GCLID_KV,
          KV_ORDER_PREFIX + collectOrderId,
          attribution
        ).catch(err => console.error("[eden-analytics] collect purchase order-link error:", err))
        : Promise.resolve(),
      ])
    );
  }

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
// v5.7: resolveAttribution now uses orderId as 3rd fallback lookup
// v5.7: OS_purchase bridge writes attr:user + attr:order (server-fired path)
// v5.7: OS_order_delivered opportunistically writes attr:user
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

  const identity     = resolveIdentityFromBody(request, body);
  const anonId       = identity.anonymousId || null;
  const userId       = identity.userId || null;
  const rawEventName = resolveEventName(body);
  const eventName    = canonicalizeEventName(rawEventName);
  const orderId      = resolveOrderId(body);

  if (eventName) body.event = eventName;
  if (userId)    body.userId = userId;
  if (anonId)    body.anonymousId = anonId;
  if (orderId && !body.properties.order_id) body.properties.order_id = orderId;

  // ── Edge dedup — 24hr TTL per order_id ───────────────────────────────────
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

  // ── v5.7: resolve attribution with orderId as 3rd fallback ───────────────
  const storedAttribution = env.GCLID_KV
    ? await resolveAttribution(env.GCLID_KV, anonId, userId, orderId)
    : null;

  // Merge stored attribution — explicit engineer values always win
  if (storedAttribution && body.properties) {
    for (const [k, v] of Object.entries(storedAttribution)) {
      if (!body.properties[k] && v) body.properties[k] = v;
    }
  }

  // ── v5.7: attribution bridge ──────────────────────────────────────────────
  // OS_purchase (server path): write attr:user + attr:order
  // OS_order_delivered: opportunistically write attr:user
  if (env.GCLID_KV && storedAttribution && CLICK_ID_PARAMS.some(p => storedAttribution[p])) {

    if (eventName === "OS_purchase") {
      ctx.waitUntil(
        Promise.all([
          userId ? storeAttribution(
            env.GCLID_KV,
            KV_USER_PREFIX + userId,
            storedAttribution
          ).catch(err => console.error("[eden-analytics] purchase user-link error:", err))
          : Promise.resolve(),

          orderId ? storeAttribution(
            env.GCLID_KV,
            KV_ORDER_PREFIX + orderId,
            storedAttribution
          ).catch(err => console.error("[eden-analytics] purchase order-link error:", err))
          : Promise.resolve(),
        ])
      );
    }

    if (eventName === "OS_order_delivered" && userId) {
      ctx.waitUntil(
        storeAttribution(
          env.GCLID_KV,
          KV_USER_PREFIX + userId,
          storedAttribution
        ).catch(err => console.error("[eden-analytics] delivery user-link error:", err))
      );
    }
  }

  body.timestamp = nowUTC();

  const superProps = {
    portal:           "patient",
    source_type:      "server",
    pipeline_version: "5.20",
    ...(identity.identityWarning ? { identity_warning: identity.identityWarning } : {}),
  };

  const attribution   = {
    ...(storedAttribution || {}),
    ...((body.context || {}).campaign || {}),
  };
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
    headers: { "Content-Type": "application/json", ...corsHeadersObj(origin) },
  });
}


// =============================================================================
// /identify HANDLER — LOGIN / ACCOUNT CREATION
// Links anonymousId → userId attribution in KV (copy-only, first-touch preserved)
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
  const anonId   = identity.anonymousId || null;
  const userId   = identity.userId || null;

  if (userId) body.userId = userId;
  if (anonId) body.anonymousId = anonId;

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
// SEGMENT FORWARDING
// Routes page/identify/screen/track to correct Segment endpoints
// Ensures no empty event names reach Segment
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

  // ── identify ──────────────────────────────────────────────────────────────
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

  // ── page ──────────────────────────────────────────────────────────────────
  if (type === "page") {
    await segmentPost(writeKey, "page", {
      anonymousId: anonId,
      userId:      resolveUserIdFromBody(body),
      name:        body.name || body.properties?.name || "",
      properties:  await hashEmail({ ...superProps, ...(body.properties || {}) }),
      context:     mergedContext,
      timestamp:   nowUTC(),
    });
    return;
  }

  // ── screen ────────────────────────────────────────────────────────────────
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

  // ── track (default) ───────────────────────────────────────────────────────
  const eventName = canonicalizeEventName(resolveEventName(body)) || null;
  const orderId   = resolveOrderId(body);
  if (eventName) body.event = eventName;
  if (orderId && body.properties && !body.properties.order_id) body.properties.order_id = orderId;

  if (!eventName) {
    console.log("[eden-analytics] skipping event with no name — likely internal metrics");
    return;
  }

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


// =============================================================================
// KV ATTRIBUTION — STORAGE + RETRIEVAL
// first-touch rule: any stored click ID blocks retargeting overwrite
// copy-only rule: userId copy skipped if userId already has attribution
// fail-open: KV errors never block a real conversion
// =============================================================================

async function storeAttribution(kv, key, attribution) {
  if (!kv || !key || !attribution) return;

  const hasValue = Object.values(attribution).some(v => v && String(v).trim());
  if (!hasValue) return;

  try {
    const existing = await kv.get(key);
    if (existing) {
      const parsed = JSON.parse(existing);
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

// v5.7: orderId as optional 3rd parallel lookup
// Priority: anon (current session) → user (cross-device) → order (v5.7 bridge)
async function resolveAttribution(kv, anonId, userId, orderId = null) {
  if (!kv) return null;

  const [fromAnon, fromUser, fromOrder] = await Promise.all([
    anonId  ? getAttribution(kv, KV_ANON_PREFIX  + anonId)  : Promise.resolve(null),
    userId  ? getAttribution(kv, KV_USER_PREFIX   + userId)  : Promise.resolve(null),
    orderId ? getAttribution(kv, KV_ORDER_PREFIX  + orderId) : Promise.resolve(null),
  ]);

  if (fromAnon  && CLICK_ID_PARAMS.some(p => fromAnon[p]))  return fromAnon;
  if (fromUser  && CLICK_ID_PARAMS.some(p => fromUser[p]))  return fromUser;
  if (fromOrder && CLICK_ID_PARAMS.some(p => fromOrder[p])) return fromOrder;

  return fromAnon || fromUser || fromOrder || null;
}

async function linkUserAttribution(kv, anonId, userId) {
  const [anonAttribution, existingUser] = await Promise.all([
    getAttribution(kv, KV_ANON_PREFIX + anonId),
    getAttribution(kv, KV_USER_PREFIX + userId),
  ]);

  if (!anonAttribution) return;

  const userHasAttribution = existingUser && CLICK_ID_PARAMS.some(p => existingUser[p]);
  if (userHasAttribution) {
    console.log("[eden-analytics] userId attribution exists — copy skipped");
    return;
  }

  await storeAttribution(kv, KV_USER_PREFIX + userId, anonAttribution);
}


// =============================================================================
// EMAIL HASHING — SHA-256, lowercase + trim per Google/Meta enhanced conversions
// Raw email also kept — BAA signed, PHI legal via Segment
// =============================================================================

async function hashEmail(props) {
  if (!props || typeof props !== "object") return props;
  const out = {};
  for (const [k, v] of Object.entries(props)) {
    if ((k === "email" || k === "customerEmail") && typeof v === "string") {
      out["email_sha256"] = await sha256(v);
      out[k] = v;
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
// SYNTHETIC MONITOR DETECTION — v5.20
// Blocks Checkly and other synthetic monitors from polluting KV with fake
// click IDs that cause UNPARSEABLE_GCLID errors in Google Ads
// =============================================================================

function isSyntheticMonitor(request, url) {
  if (url.searchParams.has("eden_checkly_marker"))                    return true;
  if (url.searchParams.get("utm_medium") === "synthetic")             return true;
  if ((url.searchParams.get("utm_source") || "").includes("checkly")) return true;
  const ua = request.headers.get("User-Agent") || "";
  if (/checklyhq/i.test(ua))                                          return true;
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
// ATTRIBUTION HELPERS
// =============================================================================

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
    body.properties?.orderId  ||
    body.order_id             ||
    body.orderId              ||
    null
  );
}

function resolveUserIdFromBody(body) {
  return (
    body.userId               ||
    body.user_id              ||
    body.properties?.userId   ||
    body.properties?.user_id  ||
    body.properties?.patient_id  ||
    body.properties?.customer_id ||
    null
  );
}

function resolveIdentityFromBody(request, body) {
  const cookieAnonId =
    readCookie(request, "eden_anon_id") ||
    readCookie(request, "eden_anonymous_id");

  const userId = resolveUserIdFromBody(body);

  let anonymousId =
    cookieAnonId               ||
    body.anonymousId           ||
    body.anonymous_id          ||
    body.anonymoous_id         ||
    body.properties?.anonymousId  ||
    body.properties?.anonymous_id ||
    body.properties?.anonymoous_id ||
    null;

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
  if (campaign.fbclid)    return "meta";
  if (campaign.msclkid)   return "microsoft";
  if (campaign.ttclid)    return "tiktok";
  if (campaign.twclid)    return "twitter";
  if (campaign.li_fat_id) return "linkedin";
  if (campaign.rdt_cid)   return "reddit";
  if (campaign.epik)      return "pinterest";
  if (campaign.ScCid)     return "snapchat";
  if (campaign.irclickid) return "impact_radius";
  if (campaign.cjevent)   return "cj_affiliate";
  if (campaign.click_id)  return "generic";
  return undefined;
}

function deriveAcquisitionChannel(campaign) {
  if (!campaign || Object.keys(campaign).length === 0) return "unknown";

  const source = String(campaign.utm_source || deriveClickIdSource(campaign) || "").toLowerCase();
  const medium = String(campaign.utm_medium || "").toLowerCase();

  if (medium === "organic")    return "organic_search";
  if (medium === "email")      return "email";
  if (medium === "affiliate")  return "affiliate";
  if (medium === "influencer") return "influencer";

  if (
    medium === "cpc" || medium === "paid" || medium === "paid_search" ||
    campaign.gclid || campaign.gbraid || campaign.wbraid || campaign.dclid ||
    campaign.msclkid ||
    source.includes("google") || source.includes("bing") || source.includes("microsoft")
  ) return "paid_search";

  if (
    campaign.fbclid || campaign.ttclid ||
    source.includes("facebook") || source.includes("instagram") ||
    source.includes("meta")     || source.includes("tiktok")
  ) return "paid_social";

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


// =============================================================================
// SEGMENT POST
// =============================================================================

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
    `Domain=${cookieDomain(url)}`,
    "Path=/",
    "HttpOnly",
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
  return ALLOWED_ORIGINS.includes(origin);
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
