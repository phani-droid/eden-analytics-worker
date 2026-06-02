// =============================================================================
// EdenOS Analytics Worker — v5.22 (PRODUCTION FINAL)
// =============================================================================
//
// COMPLETE FIX LOG (all versions cumulative):
//
//   v5.22 FIX 12 — HttpOnly removed from eden_anon_id cookie
//     analytics.js (Gowtham + Danny) calls setAnonymousId(getCookie('eden_anon_id'))
//     HttpOnly made the cookie invisible to JS → setAnonymousId never fired
//     → anonId mismatch → ~40% of purchases lost attribution
//     Fix: remove HttpOnly. eden_anon_id is a UUID, not a session token.
//
//   v5.22 FIX 11 — UTM extraction reads page URL, not /collect endpoint URL
//     handleCollect was calling extractUTMs(url) where url = /collect endpoint
//     /collect never has UTMs → freshUTMs always null → UTMs only from KV
//     Fix: extract UTMs from body.context.page.url (actual page URL)
//
//   v5.22 FIX 10 — _gl linker parsed from body.context.page.url in /collect
//     If KV miss AND page URL had _gl param, attribution was unrecoverable
//     analytics.js sends page URL in body.context.page.url
//     Fix: parse _gl from page URL as attribution fallback in handleCollect
//
//   v5.22 FIX 9 — master_id as dedup fallback when no order_id
//     Pending Consult patients (15 of 27 today) had no order_id
//     dedup key = dedup:event:null → dedup always skipped → triple-fires
//     Fix: resolveOrderId() falls back to master_id
//
//   v5.22 FIX 8 — srsltid (Google Shopping) added to CLICK_ID_CONFIG
//     Users from Google Shopping arrived with srsltid= in URL
//     Not in CLICK_ID_CONFIG → treated as unknown channel
//     Fix: add srsltid as google_shopping channel
//
//   v5.22 FIX 7 — UTM enrichment allowed on existing KV entries
//     storeAttribution blocked ALL updates when click ID existed
//     utm_campaign / utm_content added after click never enriched the record
//     Fix: allow non-click UTM fields to enrich existing entries
//
//   v5.22 FIX 6 — eden_pre_auth cookie cleared on any page load (not just post-redirect)
//     Pre-auth cookie was only cleared when referrer matched SSO/BNPL domains
//     Referrer-Policy: no-referrer meant cookie lingered for 10 minutes
//     Fix: clear pre-auth on any page load after it is read
//
//   v5.21 FIX 5 — Pre-auth cookie for SSO + BNPL redirect attribution preservation
//   v5.21 FIX 4 — Google cross-domain _gl linker parameter parsed
//   v5.20 FIX 3 — Checkly synthetic monitor pollution blocked
//   v5.8  FIX 2 — OS_purchase bridge in /collect (attr:user + attr:order)
//   v5.7  FIX 1 — orderId as 3rd parallel KV lookup in resolveAttribution
//   v5.6        — page/identify/screen routing, first-touch all click IDs,
//                 page_viewed inflation fix
//   v5.4        — /collect/* startsWith, nowUTC(), CORS on /server-collect
//
// ARCHITECTURE:
//   Layer 1 — eden_anon_id cookie (2yr, JS-readable, ITP-resistant via edge)
//   Layer 2 — Cloudflare KV attribution (120 days, first-touch, 17 channels)
//   Layer 3 — userId → attribution link at /identify
//   Layer 4 — email_sha256 enhanced conversions
//   Layer 5 — Organic referrer detection
//   Layer 6 — _gl cross-domain linker parsing
//   Layer 7 — Pre-auth cookie for SSO/BNPL redirect survival
//
// KV KEY SCHEMA:
//   attr:anon:{anonymousId}     → attribution (120 days)
//   attr:user:{userId}          → attribution (120 days)
//   attr:order:{orderId}        → attribution (120 days)
//   dedup:{eventName}:{key}     → dedup lock  (24 hours)
//
// ROUTES:
//   eden.health/* | www.eden.health/* | app.eden.health/*
// =============================================================================


// ─────────────────────────────────────────────────────────────────────────────
// ALLOWED ORIGINS
// ─────────────────────────────────────────────────────────────────────────────

const ALLOWED_ORIGINS = [
  "https://eden.health",
  "https://www.eden.health",
  "https://app.eden.health",
];


// ─────────────────────────────────────────────────────────────────────────────
// CLICK ID CONFIG — 17 paid channels (v5.22: added _gcl_au + srsltid)
// ─────────────────────────────────────────────────────────────────────────────

const CLICK_ID_CONFIG = [
  { param: "gclid",     channel: "google_ads",      label: "Google Ads"          },
  { param: "gbraid",    channel: "google_ios",       label: "Google iOS"          },
  { param: "wbraid",    channel: "google_web",       label: "Google Web"          },
  { param: "dclid",     channel: "google_display",   label: "Google Display"      },
  { param: "_gcl_au",   channel: "google_ads",       label: "Google Cross-Domain" }, // v5.21
  { param: "srsltid",   channel: "google_shopping",  label: "Google Shopping"     }, // v5.22
  { param: "fbclid",    channel: "meta",              label: "Meta/Facebook"       },
  { param: "msclkid",   channel: "microsoft",         label: "Microsoft/Bing"      },
  { param: "ttclid",    channel: "tiktok",            label: "TikTok"              },
  { param: "twclid",    channel: "twitter",           label: "Twitter/X"           },
  { param: "li_fat_id", channel: "linkedin",          label: "LinkedIn"            },
  { param: "rdt_cid",   channel: "reddit",            label: "Reddit"              },
  { param: "epik",      channel: "pinterest",         label: "Pinterest"           },
  { param: "ScCid",     channel: "snapchat",          label: "Snapchat"            },
  { param: "nbt",       channel: "northbeam",         label: "Northbeam"           },
  { param: "irclickid", channel: "impact_radius",     label: "Impact Radius"       },
  { param: "cjevent",   channel: "cj_affiliate",      label: "CJ Affiliate"        },
  { param: "click_id",  channel: "generic",           label: "Generic"             },
];

const CLICK_ID_PARAMS = CLICK_ID_CONFIG.map(c => c.param);


// ─────────────────────────────────────────────────────────────────────────────
// CONVERSION EVENTS + ALIASES
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
// BOT + SYNTHETIC DETECTION
// ─────────────────────────────────────────────────────────────────────────────

const BOT_UA_PATTERNS = [
  /bot\b/i, /crawler/i, /spider/i, /headless/i,
  /lighthouse/i, /pagespeed/i, /playwright/i,
  /puppeteer/i, /preview/i, /prerender/i,
  /google-inspectiontool/i,
  /checklyhq/i,
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
// KV SCHEMA + TTLs
// ─────────────────────────────────────────────────────────────────────────────

const KV_ANON_PREFIX  = "attr:anon:";
const KV_USER_PREFIX  = "attr:user:";
const KV_ORDER_PREFIX = "attr:order:";
const KV_TTL          = 10368000;  // 120 days
const KV_DEDUP_TTL    = 86400;     // 24 hours

// UTM fields allowed to enrich existing KV entries even when click ID exists
// (first-touch click ID is preserved; campaign context can still be added)
const UTM_ENRICHABLE = [
  "utm_campaign", "utm_content", "utm_term", "utm_id",
  "attribution_campaign",
];


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
          ok:                           true,
          worker:                       "eden-analytics",
          version:                      "5.22",
          ts:                           nowUTC(),
          kv:                           !!env.GCLID_KV,
          segment_write_key_configured: !!env.SEGMENT_WRITE_KEY,
          server_secret_configured:     !!env.SERVER_API_SECRET,
          attribution_model:            "first-touch — 17 channels — UTM enrichment allowed",
          attribution_ttl:              "120 days",
          dedup_ttl:                    "24 hours",
          dedup_key:                    "order_id with master_id fallback",
          cookie_js_readable:           "true — HttpOnly removed from eden_anon_id v5.22",
          gl_linker_parsing:            "enabled v5.21 — _gcl_au extracted from _gl",
          gl_linker_in_collect:         "enabled v5.22 — parsed from body.context.page.url",
          utm_extraction:               "fixed v5.22 — reads page URL not /collect URL",
          pre_auth_cookie:              "enabled v5.21 — SSO + BNPL redirect survival",
          synthetic_monitor_block:      "enabled v5.20 — Checkly fake gclid prevention",
          srsltid:                      "enabled v5.22 — Google Shopping clicks captured",
          channels:                     CLICK_ID_CONFIG.map(c => c.label),
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

      // ── Skip synthetic monitors ───────────────────────────────────────────
      if (isSyntheticMonitor(request, url)) {
        console.log("[eden-analytics] synthetic monitor blocked");
        return fetch(request);
      }

      // ── Skip static assets ────────────────────────────────────────────────
      if (isStaticAsset(url)) return fetch(request);

      // ── /preserve-attribution — pre-SSO/BNPL attribution save ────────────
      if (url.pathname === "/preserve-attribution" && request.method === "POST") {
        return handlePreserveAttribution(request, env, ctx);
      }

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

      // ── Page requests — cookie + KV attribution ───────────────────────────
      return handlePageRequest(request, env, ctx, url);

    } catch (err) {
      console.error("[eden-analytics] unhandled error:", err);
      return fetch(request);
    }
  },
};


// =============================================================================
// PAGE REQUEST HANDLER
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

  // Extract click IDs — includes _gl linker + srsltid (v5.22)
  const clickIds = extractClickIds(url);
  const utms     = extractUTMs(url);

  // Pre-auth cookie: recover attribution after SSO/BNPL redirect
  const preAuth = !gpcOptOut ? extractPreAuthAttribution(request) : null;

  // Merge: fresh click IDs win over pre-auth
  const mergedClickIds = { ...(preAuth || {}), ...clickIds };
  const hasAttribution = Object.keys(mergedClickIds).length > 0 || !!utms;

  if (hasAttribution && env.GCLID_KV && !gpcOptOut) {
    ctx.waitUntil(
      storeAttribution(env.GCLID_KV, KV_ANON_PREFIX + anonId, {
        ...(utms || {}),
        ...mergedClickIds,
      }).catch(err => console.error("[eden-analytics] KV store error:", err))
    );
  }

  const response = await fetch(request);
  const contentType = response.headers.get("content-type") || "";

  const headers = new Headers(response.headers);

  // v5.22: eden_anon_id is NOT HttpOnly — JS must read it for setAnonymousId()
  if (isNewVisitor) headers.append("Set-Cookie", buildAnonCookie(anonId, url));
  if (isNewSession) headers.append("Set-Cookie", buildSessionCookie(session, url));

  // Clear pre-auth cookie on any page load after reading (v5.22: not just post-redirect)
  if (preAuth) {
    headers.append("Set-Cookie", [
      "eden_pre_auth=",
      "Max-Age=0",
      `Domain=${cookieDomain(url)}`,
      "Path=/",
      "Secure",
      "SameSite=Lax",
    ].join("; "));
  }

  if (env.SEGMENT_WRITE_KEY && isNewSession && hasAttribution && !gpcOptOut) {
    ctx.waitUntil(
      fireFirstTouch(request, env, anonId, session, url, mergedClickIds, utms)
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
// =============================================================================

async function fireFirstTouch(request, env, anonId, session, url, clickIds, utms) {
  const cleanUrl   = sanitizeUrl(url);
  const referrer   = sanitizeUrlString(request.headers.get("Referer") || "");
  const ua         = request.headers.get("User-Agent") || "";
  const portal     = url.hostname.includes("app.eden.health") ? "patient" : "marketing";
  const sessionId  = session.split("_")[0];

  const organic     = !utms && !clickIds.gclid && !clickIds._gcl_au && referrer
    ? detectOrganic(referrer)
    : null;
  const attribution = { ...(utms || organic || {}), ...clickIds };

  if (Object.keys(attribution).length === 0) return;

  const messageId     = `first_touch_${anonId}_${sessionId}`;
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
      pipeline_version:     "5.22",
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
// v5.22: UTMs extracted from body.context.page.url (not /collect endpoint URL)
// v5.22: _gl linker parsed from body.context.page.url as KV-miss fallback
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

  // KV lookup: anon → user → order
  const storedAttribution = (env.GCLID_KV && !gpcOptOut)
    ? await resolveAttribution(env.GCLID_KV, anonId, userId, resolveOrderId(body))
    : null;

  // v5.22 FIX: extract UTMs + _gl from actual page URL (body.context.page.url)
  // NOT from url (which is the /collect endpoint — never has UTMs)
  let freshClickIds = {};
  let freshUTMs     = null;

  if (!gpcOptOut) {
    const pageUrlStr = body?.context?.page?.url;
    if (pageUrlStr) {
      try {
        const pageUrl = new URL(pageUrlStr);
        freshClickIds = extractClickIds(pageUrl);
        freshUTMs     = extractUTMs(pageUrl);
      } catch { /* malformed URL — ignore */ }
    }
  }

  // context.campaign from analytics.js (carries UTMs analytics.js saw on load)
  const contextCampaign = gpcOptOut ? {} : ((body.context || {}).campaign || {});

  // Merge priority (highest wins):
  // stored KV > context.campaign > fresh from page URL > nothing
  const attribution = {
    ...(freshUTMs         || {}),
    ...freshClickIds,
    ...contextCampaign,
    ...(storedAttribution || {}),
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
    pipeline_version: "5.22",
  };

  // OS_purchase bridge: write attr:user + attr:order so server-side events resolve later
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
          env.GCLID_KV, KV_USER_PREFIX + collectUserId, attribution
        ).catch(err => console.error("[eden-analytics] collect purchase user-link error:", err))
        : Promise.resolve(),

        collectOrderId ? storeAttribution(
          env.GCLID_KV, KV_ORDER_PREFIX + collectOrderId, attribution
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

  // v5.22: not HttpOnly — JS-readable for setAnonymousId()
  if (isNew) headers["Set-Cookie"] = buildAnonCookie(anonId, new URL(request.url));

  return new Response(JSON.stringify({ ok: true, anonId }), { status: 200, headers });
}


// =============================================================================
// /server-collect HANDLER — SERVER-SIDE EVENTS
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

  // Edge dedup — 24hr TTL per dedup key (order_id with master_id fallback)
  if (CONVERSION_EVENTS.has(eventName) && orderId && env.GCLID_KV) {
    const dedupKey = `dedup:${eventName}:${orderId}`;
    try {
      const alreadyFired = await env.GCLID_KV.get(dedupKey);
      if (alreadyFired) {
        console.log(`[eden-analytics] dedup blocked: ${eventName} key=${orderId}`);
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

  // Resolve attribution: anon → user → order (3-way parallel lookup)
  const storedAttribution = env.GCLID_KV
    ? await resolveAttribution(env.GCLID_KV, anonId, userId, orderId)
    : null;

  if (storedAttribution && body.properties) {
    for (const [k, v] of Object.entries(storedAttribution)) {
      if (!body.properties[k] && v) body.properties[k] = v;
    }
  }

  // Attribution bridge: write attr:user + attr:order for downstream events
  if (env.GCLID_KV && storedAttribution && CLICK_ID_PARAMS.some(p => storedAttribution[p])) {
    if (eventName === "OS_purchase") {
      ctx.waitUntil(Promise.all([
        userId ? storeAttribution(env.GCLID_KV, KV_USER_PREFIX + userId, storedAttribution)
          .catch(err => console.error("[eden-analytics] purchase user-link error:", err))
        : Promise.resolve(),
        orderId ? storeAttribution(env.GCLID_KV, KV_ORDER_PREFIX + orderId, storedAttribution)
          .catch(err => console.error("[eden-analytics] purchase order-link error:", err))
        : Promise.resolve(),
      ]));
    }
    if (eventName === "OS_order_delivered" && userId) {
      ctx.waitUntil(
        storeAttribution(env.GCLID_KV, KV_USER_PREFIX + userId, storedAttribution)
          .catch(err => console.error("[eden-analytics] delivery user-link error:", err))
      );
    }
  }

  body.timestamp = nowUTC();

  const superProps = {
    portal:           "patient",
    source_type:      "server",
    pipeline_version: "5.22",
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
        env.SEGMENT_WRITE_KEY, body,
        anonId || userId || "server",
        superProps, attribution
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
// /identify HANDLER
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
// /preserve-attribution — pre-SSO/BNPL attribution save
// Called by app frontend BEFORE redirecting to Google OAuth or Klarna
// =============================================================================

async function handlePreserveAttribution(request, env, ctx) {
  const origin = request.headers.get("Origin") || "";
  if (origin && !isAllowedOrigin(origin)) {
    return new Response("Forbidden", { status: 403 });
  }

  let body;
  try { body = await request.json(); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  const cookieAnonId = readCookie(request, "eden_anon_id")
                    || readCookie(request, "eden_anonymous_id");
  const anonId  = cookieAnonId || body.anonymousId;
  const userId  = body.userId;
  const orderId = body.orderId;

  if (!env.GCLID_KV) return jsonResponse({ ok: true, skipped: "no_kv" });

  const attribution = await resolveAttribution(env.GCLID_KV, anonId, userId, orderId);

  if (!attribution || !CLICK_ID_PARAMS.some(p => attribution[p])) {
    return jsonResponse({ ok: true, skipped: "no_attribution" });
  }

  // Write attr:order BEFORE redirect so Klarna return can resolve it
  if (orderId) {
    ctx.waitUntil(
      storeAttribution(env.GCLID_KV, KV_ORDER_PREFIX + orderId, attribution)
        .catch(err => console.error("[eden-analytics] pre-auth order store error:", err))
    );
  }

  const preAuthValue = encodeURIComponent(JSON.stringify({
    ...(attribution._gcl_au      ? { _gcl_au:       attribution._gcl_au      } : {}),
    ...(attribution.gclid        ? { gclid:          attribution.gclid        } : {}),
    ...(attribution.utm_source   ? { utm_source:     attribution.utm_source   } : {}),
    ...(attribution.utm_medium   ? { utm_medium:     attribution.utm_medium   } : {}),
    ...(attribution.utm_campaign ? { utm_campaign:   attribution.utm_campaign } : {}),
    ...(attribution.utm_content  ? { utm_content:    attribution.utm_content  } : {}),
    ...(attribution.utm_term     ? { utm_term:       attribution.utm_term     } : {}),
  }));

  // HttpOnly: worker reads this, JS does not need to
  const preAuthCookie = [
    `eden_pre_auth=${preAuthValue}`,
    "Max-Age=600",
    `Domain=${cookieDomain(new URL(request.url))}`,
    "Path=/",
    "HttpOnly",
    "Secure",
    "SameSite=Lax",
  ].join("; ");

  return new Response(JSON.stringify({ ok: true }), {
    status: 200,
    headers: {
      "Content-Type": "application/json",
      "Set-Cookie":   preAuthCookie,
      ...corsHeadersObj(origin),
    },
  });
}

function extractPreAuthAttribution(request) {
  const raw = readCookie(request, "eden_pre_auth");
  if (!raw) return null;
  try { return JSON.parse(decodeURIComponent(raw)); }
  catch { return null; }
}


// =============================================================================
// SEGMENT FORWARDING
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
      userId:      resolveUserIdFromBody(body),
      traits,
      context:     mergedContext,
      timestamp:   nowUTC(),
    });
    return;
  }

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

  // track (default)
  const eventName = canonicalizeEventName(resolveEventName(body)) || null;
  const orderId   = resolveOrderId(body);
  if (eventName) body.event = eventName;
  if (orderId && body.properties && !body.properties.order_id) {
    body.properties.order_id = orderId;
  }

  if (!eventName) {
    console.log("[eden-analytics] skipping event with no name");
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
// v5.22: UTM enrichment allowed even when click ID exists (first-touch preserved)
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
        // First-touch preserved — block retargeting click overwrite
        // BUT allow UTM campaign context to enrich the existing entry (v5.22)
        let enriched = false;
        const updated = { ...parsed };
        for (const k of UTM_ENRICHABLE) {
          if (attribution[k] && !parsed[k]) {
            updated[k] = attribution[k];
            enriched = true;
          }
        }
        if (enriched) {
          await kv.put(key, JSON.stringify(updated), { expirationTtl: KV_TTL });
          console.log("[eden-analytics] first-touch preserved — UTM context enriched");
        } else {
          console.log("[eden-analytics] first-touch preserved — no enrichment needed");
        }
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

// Priority: anon (current session) → user (cross-device) → order (bridge)
// Within each: prefers entries with a click ID over UTM-only entries
async function resolveAttribution(kv, anonId, userId, orderId = null) {
  if (!kv) return null;

  const [fromAnon, fromUser, fromOrder] = await Promise.all([
    anonId  ? getAttribution(kv, KV_ANON_PREFIX  + anonId)  : Promise.resolve(null),
    userId  ? getAttribution(kv, KV_USER_PREFIX   + userId)  : Promise.resolve(null),
    orderId ? getAttribution(kv, KV_ORDER_PREFIX  + orderId) : Promise.resolve(null),
  ]);

  // Prefer entries that have a click ID
  if (fromAnon  && CLICK_ID_PARAMS.some(p => fromAnon[p]))  return fromAnon;
  if (fromUser  && CLICK_ID_PARAMS.some(p => fromUser[p]))  return fromUser;
  if (fromOrder && CLICK_ID_PARAMS.some(p => fromOrder[p])) return fromOrder;

  // Fall through to UTM-only entries
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
// CLICK ID + UTM EXTRACTION
// v5.22: extractClickIds parses _gl linker + direct params + srsltid
// =============================================================================

function extractClickIds(url) {
  const out = {};

  // 1. Direct click ID params
  for (const { param } of CLICK_ID_CONFIG) {
    const v = url.searchParams.get(param);
    if (v) out[param] = v;
  }

  // 2. Google cross-domain linker _gl (v5.21)
  // Only parse if no direct gclid/_gcl_au already found
  if (!out.gclid && !out._gcl_au) {
    const gl = url.searchParams.get("_gl");
    if (gl) {
      const glAttribution = extractGlLinker(gl);
      Object.assign(out, glAttribution);
    }
  }

  return out;
}

/**
 * Parse Google cross-domain _gl linker parameter.
 * Format: VERSION*SESSION*key*value*key*value*...
 * Extracts _gcl_au and infers utm_source=google, utm_medium=cpc
 */
function extractGlLinker(gl) {
  const out = {};
  if (!gl) return out;
  try {
    const parts = gl.split("*");
    // parts[0]=version, parts[1]=session, parts[2..n]=key*value pairs
    for (let i = 2; i < parts.length - 1; i += 2) {
      const key   = parts[i];
      const value = parts[i + 1];
      if (key === "_gcl_au" && value) {
        out._gcl_au = value;
        try {
          const b64      = value.replace(/\./g, "=").replace(/-/g, "+").replace(/_/g, "/");
          const decoded  = atob(b64);
          const segments = decoded.split(".");
          if (segments.length >= 3) out._gcl_hash = segments[2];
        } catch { /* decode failed — _gcl_au still stored */ }
        // _gcl_au only exists for paid clicks — infer channel
        if (!out.utm_source) out.utm_source = "google";
        if (!out.utm_medium) out.utm_medium = "cpc";
      }
    }
  } catch { /* fail open */ }
  return out;
}

function extractUTMs(url) {
  const out = {};
  for (const k of ["utm_source","utm_medium","utm_campaign","utm_content","utm_term","utm_id"]) {
    const v = url.searchParams.get(k);
    if (v) out[k] = v;
  }
  return Object.keys(out).length ? out : null;
}


// =============================================================================
// ATTRIBUTION HELPERS
// =============================================================================

function buildCampaignContext(attribution) {
  const campaign = {};
  const CAMPAIGN_KEYS = [
    "utm_source","utm_medium","utm_campaign","utm_content","utm_term","utm_id",
    ...CLICK_ID_PARAMS,
  ];
  for (const k of CAMPAIGN_KEYS) {
    if (attribution[k]) campaign[k] = attribution[k];
  }
  return campaign;
}

function enrichPropertiesWithAttribution(properties, campaignProps) {
  if (!properties || typeof properties !== "object") return;
  if (!campaignProps || Object.keys(campaignProps).length === 0) return;

  for (const [k, v] of Object.entries(campaignProps)) {
    if (v && !properties[k]) properties[k] = v;
  }

  // _gcl_au as top-level property for Google Enhanced Conversions
  if (campaignProps._gcl_au && !properties.gcl_au) {
    properties.gcl_au = campaignProps._gcl_au;
  }

  properties.acquisition_channel  = properties.acquisition_channel
    || deriveAcquisitionChannel(campaignProps);
  properties.attribution_source   = properties.attribution_source
    || campaignProps.utm_source || deriveClickIdSource(campaignProps);
  properties.attribution_medium   = properties.attribution_medium
    || campaignProps.utm_medium;
  properties.attribution_campaign = properties.attribution_campaign
    || campaignProps.utm_campaign;
}

function deriveClickIdSource(campaign) {
  if (!campaign) return undefined;
  if (campaign.gclid  || campaign.gbraid || campaign.wbraid ||
      campaign.dclid  || campaign._gcl_au || campaign.srsltid) return "google";
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

  if (medium === "organic")                return "organic_search";
  if (medium === "email")                  return "email";
  if (medium === "affiliate")              return "affiliate";
  if (medium === "influencer")             return "influencer";
  if (medium === "synthetic")              return "synthetic";

  if (
    medium === "cpc" || medium === "paid" ||
    medium === "paid_search" || medium === "search_cpc" ||
    campaign.gclid   || campaign.gbraid  || campaign.wbraid ||
    campaign.dclid   || campaign._gcl_au || campaign.srsltid ||
    campaign.msclkid ||
    source.includes("google") || source.includes("bing") || source.includes("microsoft")
  ) return "paid_search";

  if (
    campaign.fbclid  || campaign.ttclid  ||
    source.includes("facebook") || source.includes("instagram") ||
    source.includes("meta")     || source.includes("tiktok")
  ) return "paid_social";

  if (
    campaign.li_fat_id || source.includes("linkedin")
  ) return "paid_social_linkedin";

  return source || "unknown";
}

function canonicalizeEventName(eventName) {
  if (!eventName) return "";
  const raw = String(eventName).trim();
  if (!raw) return "";
  return EVENT_NAME_ALIASES[raw.toLowerCase()] || raw;
}

function resolveEventName(body) {
  return (
    body.event              ||
    body.event_name         ||
    body.name               ||
    body.properties?.event  ||
    body.properties?.event_name ||
    body.properties?.name   ||
    ""
  );
}

// v5.22: master_id as dedup fallback when no order_id
function resolveOrderId(body) {
  return (
    body.properties?.order_id  ||
    body.properties?.orderId   ||
    body.order_id              ||
    body.orderId               ||
    body.properties?.master_id ||  // v5.22: stops triple-fires on Pending Consult
    body.properties?.masterId  ||
    null
  );
}

function resolveUserIdFromBody(body) {
  return (
    body.userId                  ||
    body.user_id                 ||
    body.properties?.userId      ||
    body.properties?.user_id     ||
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
    cookieAnonId                    ||
    body.anonymousId                ||
    body.anonymous_id               ||
    body.anonymoous_id              ||  // typo in wild — preserved
    body.properties?.anonymousId   ||
    body.properties?.anonymous_id  ||
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


// =============================================================================
// ORGANIC SEARCH DETECTION
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
// BOT + SYNTHETIC DETECTION
// =============================================================================

function isBot(request) {
  const ua = request.headers.get("User-Agent") || "";
  if (BOT_UA_PATTERNS.some(p => p.test(ua))) return true;
  const decision = request.cf?.botManagement?.decision;
  if (decision && BOT_CF_DECISIONS.has(decision)) return true;
  if (request.cf?.botManagement?.verifiedBot) return true;
  return false;
}

function isSyntheticMonitor(request, url) {
  if (url.searchParams.has("eden_checkly_marker"))                    return true;
  if (url.searchParams.get("utm_medium") === "synthetic")             return true;
  if ((url.searchParams.get("utm_source") || "").includes("checkly")) return true;
  if (/checklyhq/i.test(request.headers.get("User-Agent") || ""))    return true;
  return false;
}

function isStaticAsset(url) {
  const p = url.pathname.toLowerCase();
  if (STATIC_PREFIXES.some(prefix => p.startsWith(prefix))) return true;
  if (STATIC_EXTENSIONS.some(ext => p.endsWith(ext))) return true;
  return false;
}


// =============================================================================
// EMAIL HASHING — SHA-256, lowercase + trim
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
// TIMESTAMP
// =============================================================================

function nowUTC() {
  return new Date(Date.now()).toISOString();
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
// COOKIE HELPERS
// v5.22: eden_anon_id is NOT HttpOnly — JS must read it for setAnonymousId()
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

// v5.22: HttpOnly REMOVED — required for Gowtham's setAnonymousId() and Danny's snippet
function buildAnonCookie(id, url) {
  return [
    `eden_anon_id=${encodeURIComponent(id)}`,
    "Max-Age=63072000",             // 2 years
    `Domain=${cookieDomain(url)}`,
    "Path=/",
    // HttpOnly intentionally absent — JS must read this cookie
    // eden_anon_id is a UUID tracking identifier, not a session token
    // Removing HttpOnly enables analytics.setAnonymousId() in both:
    //   - Danny's snippet on eden.health (Webflow)
    //   - Gowtham's getSegmentClient() on app.eden.health (Next.js)
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
