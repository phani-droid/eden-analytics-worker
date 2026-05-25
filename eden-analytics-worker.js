// =============================================================================
// EdenOS Analytics Worker — Production Ready
// Version: 2.1
// =============================================================================
//
// ARCHITECTURE:
//   Browser / Node.js API
//     → Cloudflare Worker (this file)
//       → enriches: gclid, UTMs, ITP cookie, PHI strip, integrations map
//         → Segment HTTP API (Eden OS source)
//           → Google Ads, Mixpanel, BigQuery, Customer.io, Facebook etc
//
// ENDPOINTS:
//   /*               → intercepts page requests, fires page_viewed automatically
//   /collect         → client-side events (analytics.js posts here)
//   /server-collect  → server-side events (Node.js posts here with secret header)
//   /eden-health-check → health check
//
// DEPLOY:
//   wrangler secret put SEGMENT_WRITE_KEY
//   wrangler deploy
//
//   SERVER_API_SECRET — add only when Ryon routes server-side calls here
//   wrangler secret put SERVER_API_SECRET
//
// SCALE:
//   New event      → add to DESTINATION_RULES below + analytics.track() in app
//   New destination → add to DESTINATION_RULES below + configure in Segment UI
//   Worker redeploy → only needed when DESTINATION_RULES or PHI_PROPS changes
// =============================================================================


// ─────────────────────────────────────────────────────────────────────────────
// DESTINATION ROUTING
//
// Controls which events go to which Segment destinations.
// { All: false } = nothing by default, then selectively enable.
//
// "*"  = all events go to this destination
// [...] = only listed events go to this destination
//
// Current event names (OS_ prefix) will be updated to snake_case
// after Ryon renames them — add both for safe transition.
// ─────────────────────────────────────────────────────────────────────────────

const DESTINATION_RULES = {

  // ── Google Ads ────────────────────────────────────────────────────────────
  // Only conversion + micro-conversion events
  // PHI events (intake_disqualified, OS_sign_in_failed) intentionally excluded
  "Google Ads": [
    // Marketing — eden.health
    "page_viewed",
    "get_started_clicked",
    "CTA to Intake Clicked",            // rename pending → get_started_clicked
    "eligibility_cta_clicked",
    "email_signup_submitted",
    "product_page_viewed",

    // Intake — app.eden.health
    "intake_started",
    "OS_intake_started",                // rename pending → intake_started
    "intake_form_completed",
    "OS_medical_questionnaire_completed", // rename pending → intake_form_completed

    // Checkout
    "checkout_started",
    "OS-Begin-checkout",                // rename pending → checkout_started
    "payment_info_added",
    "OS_bnpl_clicked",                  // rename pending → payment_info_added

    // Purchase — PRIMARY conversions
    "order_completed",
    "OS_purchase",                      // current server-side purchase event
    "subscription_activated",

    // Post purchase
    "account_created",
    "login_completed",
    "OS_login",                         // rename pending → login_completed
    "browse_treatments_clicked",
  ],

  // ── Mixpanel ──────────────────────────────────────────────────────────────
  // All events — full behavioural analytics
  "Mixpanel": "*",

  // ── BigQuery ──────────────────────────────────────────────────────────────
  // All events — data warehouse, PHI allowed here only
  "BigQuery": "*",

  // ── Customer.io ───────────────────────────────────────────────────────────
  // Lifecycle + retention events only
  "Customer.io": [
    "account_created",
    "login_completed",
    "OS_login",
    "order_completed",
    "OS_purchase",
    "order_failed",
    "intake_started",
    "OS_intake_started",
    "intake_form_completed",
    "OS_medical_questionnaire_completed",
    "intake_step_abandoned",
    "intake_disqualified",
    "subscription_activated",
    "support_chat_opened",
    "refill_requested",
    "cancellation_initiated",
  ],

  // ── Facebook Conversions API ──────────────────────────────────────────────
  "Facebook Conversions API": [
    "page_viewed",
    "get_started_clicked",
    "CTA to Intake Clicked",
    "checkout_started",
    "OS-Begin-checkout",
    "payment_info_added",
    "OS_bnpl_clicked",
    "order_completed",
    "OS_purchase",
    "account_created",
    "subscription_activated",
  ],

  // ── Future destinations (uncomment when ready in Segment UI) ─────────────
  // "TikTok CAPI":   ["page_viewed", "checkout_started", "OS-Begin-checkout", "order_completed", "OS_purchase"],
  // "Google Ads Remarketing": ["page_viewed", "product_page_viewed"],
  // "Northbeam":     ["order_completed", "OS_purchase", "page_viewed"],
  // "Everflow":      ["order_completed", "OS_purchase", "subscription_activated"],
  // "HubSpot":       ["account_created", "order_completed", "OS_purchase"],
  // "Zendesk":       ["account_created", "support_chat_opened"],
  // "GA4":           "*",
};


// ─────────────────────────────────────────────────────────────────────────────
// PHI + PII BLOCKLIST
//
// Stripped from ALL events before reaching Segment.
// Based on audit of OS_purchase schema — raw email, name, phone found.
// Add new PHI properties here — auto-stripped on next deploy.
// ─────────────────────────────────────────────────────────────────────────────

const PHI_PROPS = new Set([
  // Found in OS_purchase — strip immediately
  "customerEmail",          // top-level raw email on OS_purchase
  "email",                  // raw email anywhere

  // Found in ecommerce.customer object
  "firstName",
  "lastName",
  "phoneNumber",
  "phone",

  // Health data
  "weight_lbs",
  "height_ft",
  "bmi_value",
  "goal_weight_lbs",
  "highest_weight_lbs",
  "selected_conditions",
  "selected_medications",
  "selected_allergies",
  "lbs_lost",
  "old_dose_mg",
  "new_dose_mg",
  "medication",
  "dob",
  "date_of_birth",

  // Payment card data — PCI violation
  "card_number",
  "card_exp_date",
  "card_cvc",
  "OS_card_number",
  "OS_card_exp_date",
  "OS_card_cvc",

  // Other PII
  "full_name",
  "address",
  "ssn",
]);


// ─────────────────────────────────────────────────────────────────────────────
// ALLOWED ORIGINS
// ─────────────────────────────────────────────────────────────────────────────

const ALLOWED_ORIGINS = [
  "https://eden.health",
  "https://www.eden.health",
  "https://app.eden.health",
  "https://eden-os-rimo-patient-staging.vercel.app", // staging
];


// ─────────────────────────────────────────────────────────────────────────────
// BOT DETECTION — same as first-campaign-preserver.js
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
// STATIC ASSET DETECTION — same as first-campaign-preserver.js
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

const SENSITIVE_PARAMS = [
  /client_secret/i, /payment_intent/i, /setup_intent/i,
  /^secret$/i, /^password$/i, /^token$/i,
  /^code$/i, /^state$/i,
];


// =============================================================================
// WORKER ENTRY POINT
// =============================================================================

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);

      // Health check
      if (url.pathname === "/eden-health-check") {
        return jsonOk({
          ok: true,
          worker: "eden-analytics",
          version: "2.1",
          ts: Date.now(),
        });
      }

      // CORS preflight
      if (request.method === "OPTIONS") {
        return new Response(null, {
          status: 204,
          headers: corsHeaders(request.headers.get("Origin") || ""),
        });
      }

      // Skip static assets — no analytics needed
      if (isStaticAsset(url)) {
        return fetch(request);
      }

      // Skip bots — same as first-campaign-preserver
      if (isBot(request)) {
        return fetch(request);
      }

      // /collect — client-side events from analytics.js
      if (url.pathname === "/collect" && request.method === "POST") {
        return handleCollect(request, env, ctx, url);
      }

      // /server-collect — server-side events from Node.js API
      // Currently: Ryon posts directly to Segment
      // Future: Ryon changes URL to https://app.eden.health/server-collect
      //         Worker then adds gclid automatically to server events
      if (url.pathname === "/server-collect" && request.method === "POST") {
        return handleServerCollect(request, env, ctx);
      }

      // All other requests — intercept page load, fire page_viewed, pass through
      return handlePageRequest(request, env, ctx, url);

    } catch (err) {
      console.error("[eden-analytics] Worker error:", err);
      return fetch(request); // fail open — always serve the page
    }
  },
};


// =============================================================================
// PAGE REQUEST HANDLER
// Intercepts every page load.
// Sets ITP-resistant cookies.
// Fires page_viewed + first_touch to Segment in background.
// Passes through to origin — user never notices.
// =============================================================================

async function handlePageRequest(request, env, ctx, url) {
  const existingAnonId  = readCookie(request, "eden_anon_id");
  const existingSession = readCookie(request, "eden_session_id");
  const isNewVisitor    = !existingAnonId;
  const isNewSession    = !existingSession;

  const anonId  = existingAnonId  || crypto.randomUUID();
  const session = existingSession || (crypto.randomUUID() + "_" + Date.now());

  // Pass through to origin — get the real page
  const response = await fetch(request);

  // Append cookies to response
  const headers = new Headers(response.headers);
  if (isNewVisitor) headers.append("Set-Cookie", anonCookie(anonId, url));
  if (isNewSession) headers.append("Set-Cookie", sessionCookie(session, url));

  const modifiedResponse = new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });

  // Fire analytics in background — never blocks page load
  if (env.SEGMENT_WRITE_KEY) {
    ctx.waitUntil(
      firePageAnalytics(request, env, anonId, session, url, isNewVisitor, isNewSession)
        .catch(err => console.error("[eden-analytics] Page analytics error:", err))
    );
  }

  return modifiedResponse;
}


// =============================================================================
// PAGE ANALYTICS
// Auto-fires page_viewed on every page load.
// Auto-fires first_touch on new sessions with UTM/gclid attribution.
// =============================================================================

async function firePageAnalytics(request, env, anonId, session, url, isNewVisitor, isNewSession) {
  const sanitizedUrl = sanitizeUrl(url);
  const referrer     = sanitizeUrlStr(request.headers.get("Referer") || "");
  const ua           = request.headers.get("User-Agent") || "";
  const portal       = url.hostname.includes("app.eden.health") ? "patient" : "marketing";
  const sessionId    = session.split("_")[0];

  const utms        = extractUTMs(url);
  const clickIds    = extractClickIds(url);
  const organic     = (!utms && referrer) ? detectOrganic(referrer) : null;
  const attribution = { ...(utms || organic || {}), ...clickIds };

  // page_viewed — fires on every page load
  await segmentTrack(env.SEGMENT_WRITE_KEY, {
    anonymousId:  anonId,
    userId:       null,
    event:        "page_viewed",
    properties:   {
      portal,
      page_path:      url.pathname,
      page_url:       sanitizedUrl,
      page_search:    url.search || undefined,
      referrer:       referrer   || undefined,
      device_type:    isMobile(ua) ? "mobile" : "desktop",
      session_id:     sessionId,
      is_new_visitor: isNewVisitor,
      is_new_session: isNewSession,
      ...attribution,
    },
  });

  // first_touch — fires once per session when attribution is present
  if (isNewSession && Object.keys(attribution).length > 0) {
    await segmentTrack(env.SEGMENT_WRITE_KEY, {
      anonymousId: anonId,
      userId:      null,
      event:       "first_touch",
      properties:  {
        portal,
        page_path:  url.pathname,
        page_url:   sanitizedUrl,
        referrer:   referrer || undefined,
        session_id: sessionId,
        ...attribution,
      },
    });
  }
}


// =============================================================================
// /collect HANDLER
// Receives client-side events from analytics.js.
// Adds gclid + UTMs from URL, portal, source_type.
// Strips PHI.
// =============================================================================

async function handleCollect(request, env, ctx, url) {
  const origin = request.headers.get("Origin") || "";

  if (origin && !ALLOWED_ORIGINS.includes(origin)) {
    return new Response("Forbidden", { status: 403 });
  }

  let body;
  try { body = await request.json(); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  const cookieAnonId = readCookie(request, "eden_anon_id");
  const anonId       = cookieAnonId || body.anonymousId || crypto.randomUUID();
  const isNew        = !cookieAnonId;
  const portal       = origin.includes("app.eden.health") ? "patient" : "marketing";

  const superProps = {
    portal,
    source_type: "client",
    ...extractUTMs(url),
    ...extractClickIds(url),
  };

  if (env.SEGMENT_WRITE_KEY) {
    ctx.waitUntil(
      routeToSegment(env.SEGMENT_WRITE_KEY, body, anonId, superProps)
        .catch(err => console.error("[eden-analytics] Collect error:", err))
    );
  }

  const headers = {
    "Content-Type": "application/json",
    ...corsHeadersObj(origin),
  };

  if (isNew) headers["Set-Cookie"] = anonCookie(anonId, url);

  return new Response(JSON.stringify({ ok: true, anonId }), {
    status: 200, headers,
  });
}


// =============================================================================
// /server-collect HANDLER
// Receives server-side events from Node.js API.
// Requires X-Eden-Server-Secret header.
//
// RYON — to route OS_purchase through worker (gets gclid automatically):
//   Change your Segment HTTP API call from:
//     https://api.segment.io/v1/track
//   To:
//     https://app.eden.health/server-collect
//   Add header: X-Eden-Server-Secret: <value from Phanideep>
//
// This makes the worker add gclid to OS_purchase automatically.
// =============================================================================

async function handleServerCollect(request, env, ctx) {
  const secret = request.headers.get("X-Eden-Server-Secret");

  // If SERVER_API_SECRET not configured yet — accept all server calls
  // Remove this fallback once SERVER_API_SECRET is set
  if (env.SERVER_API_SECRET && secret !== env.SERVER_API_SECRET) {
    return new Response("Unauthorized", { status: 401 });
  }

  let body;
  try { body = await request.json(); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  const anonId     = body.anonymousId || "server-side";
  const superProps = { portal: "patient", source_type: "server" };

  if (env.SEGMENT_WRITE_KEY) {
    ctx.waitUntil(
      routeToSegment(env.SEGMENT_WRITE_KEY, body, anonId, superProps)
        .catch(err => console.error("[eden-analytics] Server collect error:", err))
    );
  }

  return jsonOk({ ok: true });
}


// =============================================================================
// SEGMENT ROUTING
// Determines track / identify / page call type and fires accordingly.
// =============================================================================

async function routeToSegment(writeKey, body, anonId, superProps) {
  const type = (body.type || "track").toLowerCase();

  if (type === "identify") {
    await segmentIdentify(writeKey, body, anonId);
    return;
  }

  if (type === "page") {
    await segmentPage(writeKey, body, anonId, superProps);
    return;
  }

  // Default: track
  await segmentTrack(writeKey, {
    anonymousId:  anonId,
    userId:       body.userId || null,
    event:        body.event  || "",
    properties:   await sanitizeProps({ ...superProps, ...(body.properties || {}) }),
    integrations: buildIntegrationsMap(body.event || ""),
    context:      body.context || {},
    timestamp:    body.sentAt || body.timestamp || new Date().toISOString(),
  });
}

async function segmentTrack(writeKey, payload) {
  if (payload.properties) {
    payload.properties = await sanitizeProps(payload.properties);
  }
  if (!payload.integrations) {
    payload.integrations = buildIntegrationsMap(payload.event || "");
  }
  await segmentPost(writeKey, "track", payload);
}

async function segmentIdentify(writeKey, body, anonId) {
  const traits = await sanitizeProps(body.traits || body.properties || {});
  await segmentPost(writeKey, "identify", {
    anonymousId: anonId,
    userId:      body.userId || null,
    traits,
    context:     body.context || {},
    timestamp:   new Date().toISOString(),
  });
}

async function segmentPage(writeKey, body, anonId, superProps) {
  const props = await sanitizeProps({ ...superProps, ...(body.properties || {}) });
  await segmentPost(writeKey, "page", {
    anonymousId: anonId,
    userId:      body.userId || null,
    name:        body.name || props.page_title || "",
    properties:  props,
    context:     body.context || {},
    timestamp:   new Date().toISOString(),
  });
}

async function segmentPost(writeKey, endpoint, payload) {
  const res = await fetch(`https://api.segment.io/v1/${endpoint}`, {
    method: "POST",
    headers: {
      "Content-Type":  "application/json",
      "Authorization": `Basic ${btoa(writeKey + ":")}`,
    },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Segment /${endpoint} ${res.status}: ${text}`);
  }
}


// =============================================================================
// INTEGRATIONS MAP
// Builds the { All: false, "Google Ads": true/false, ... } object
// that tells Segment which destinations receive each event.
// =============================================================================

function buildIntegrationsMap(eventName) {
  const map = { All: false };
  for (const [dest, allowed] of Object.entries(DESTINATION_RULES)) {
    map[dest] = allowed === "*" || (Array.isArray(allowed) && allowed.includes(eventName));
  }
  return map;
}


// =============================================================================
// PROPS SANITIZATION
// 1. Strips PHI top-level properties
// 2. Strips PHI nested inside ecommerce.customer (OS_purchase specific)
// 3. Hashes raw email → email_sha256 anywhere it appears
// =============================================================================

async function sanitizeProps(props) {
  if (!props || typeof props !== "object") return props;

  const out = {};

  for (const [k, v] of Object.entries(props)) {
    // Strip top-level PHI
    if (PHI_PROPS.has(k)) continue;

    // Hash raw email fields
    if ((k === "customerEmail" || k === "email") && typeof v === "string") {
      out["email_sha256"] = await sha256(v);
      continue;
    }

    // Recursively sanitize nested objects (e.g. ecommerce.customer)
    if (v && typeof v === "object" && !Array.isArray(v)) {
      out[k] = await sanitizeNestedObject(v);
      continue;
    }

    out[k] = v;
  }

  return out;
}

async function sanitizeNestedObject(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    // Strip PHI from nested objects
    if (PHI_PROPS.has(k)) continue;

    // Hash email in nested objects
    if (k === "email" && typeof v === "string") {
      out["email_sha256"] = await sha256(v);
      continue;
    }

    // Recursively handle deeper nesting
    if (v && typeof v === "object" && !Array.isArray(v)) {
      out[k] = await sanitizeNestedObject(v);
      continue;
    }

    out[k] = v;
  }
  return out;
}


// =============================================================================
// SHA-256 HASHING
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
// Same click ID list as first-campaign-preserver.js
// =============================================================================

function extractUTMs(url) {
  const out = {};
  for (const k of ["utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term"]) {
    const v = url.searchParams.get(k);
    if (v) out[k] = v;
  }
  return Object.keys(out).length ? out : null;
}

function extractClickIds(url) {
  const out = {};
  const keys = [
    "gclid",     // Google Ads ← most important
    "dclid",     // Google Display
    "gbraid",    // Google iOS
    "wbraid",    // Google web
    "fbclid",    // Facebook
    "msclkid",   // Microsoft/Bing
    "ttclid",    // TikTok
    "twclid",    // Twitter
    "li_fat_id", // LinkedIn
    "rdt_cid",   // Reddit
    "epik",      // Pinterest
    "ScCid",     // Snapchat
    "vmcid",     // Verizon
    "yclid",     // Yahoo
    "click_id",  // Generic
    "nbt",       // Northbeam
  ];
  for (const k of keys) {
    const v = url.searchParams.get(k);
    if (v) out[k] = v;
  }
  return out;
}


// =============================================================================
// ORGANIC SEARCH DETECTION
// Same search engines as first-campaign-preserver.js
// =============================================================================

function detectOrganic(referrer) {
  if (!referrer) return null;
  try {
    const ref = new URL(referrer);
    const h   = ref.hostname.toLowerCase();
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
// BOT DETECTION — same as first-campaign-preserver.js
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
// STATIC ASSET DETECTION — same as first-campaign-preserver.js
// =============================================================================

function isStaticAsset(url) {
  const p = url.pathname.toLowerCase();
  if (STATIC_PREFIXES.some(prefix => p.startsWith(prefix))) return true;
  if (STATIC_EXTENSIONS.some(ext => p.endsWith(ext))) return true;
  return false;
}


// =============================================================================
// COOKIE HELPERS
// HttpOnly = ITP-resistant (Safari cannot wipe)
// Domain=.eden.health = spans eden.health AND app.eden.health
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

function anonCookie(id, url) {
  return [
    `eden_anon_id=${encodeURIComponent(id)}`,
    "Max-Age=63072000",              // 2 years — ITP resistant
    `Domain=${cookieDomain(url)}`,   // .eden.health — both portals
    "Path=/",
    "HttpOnly",                      // Safari ITP cannot wipe HttpOnly
    "Secure",
    "SameSite=Lax",
  ].join("; ");
}

function sessionCookie(value, url) {
  return [
    `eden_session_id=${encodeURIComponent(value)}`,
    "Max-Age=1800",                  // 30 minutes
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
      if (SENSITIVE_PARAMS.some(p => p.test(k))) {
        clean.searchParams.set(k, "[redacted]");
      }
    }
    return clean.toString();
  } catch {
    return url.toString();
  }
}

function sanitizeUrlStr(value) {
  if (!value) return "";
  try { return sanitizeUrl(new URL(value)); }
  catch { return value; }
}


// =============================================================================
// CORS HELPERS
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


// =============================================================================
// UTILITIES
// =============================================================================

function jsonOk(data) {
  return new Response(JSON.stringify(data), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}

function isMobile(ua) {
  return /Mobile|Android|iPhone|iPad|iPod/i.test(ua);
}
