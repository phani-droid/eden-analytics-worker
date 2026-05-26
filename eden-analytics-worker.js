// =============================================================================
// EdenOS Analytics Worker — v3.0 Final
// =============================================================================
//
// JOB: Intercept all traffic on eden.health + app.eden.health
//      → enrich with gclid, UTMs, ITP cookie
//      → strip PHI
//      → forward to Segment as-is
//      → Segment handles all downstream routing
//
// ENDPOINTS:
//   /*                 → page requests → auto fires page_viewed to Segment
//   /collect           → client-side events from analytics.js
//   /server-collect    → server-side events from Node.js API
//   /eden-health-check → health check
//
// DEPLOY:
//   wrangler secret put SEGMENT_WRITE_KEY
//   wrangler deploy
//
// ADD NEW EVENT:
//   Just fire analytics.track('event_name') in app code
//   Worker forwards everything to Segment automatically
//   No worker changes needed
//
// ADD NEW DESTINATION:
//   Configure in Segment UI only
//   No worker changes needed
// =============================================================================


// ─────────────────────────────────────────────────────────────────────────────
// PHI BLOCKLIST
// Stripped from every event before reaching Segment.
// Add new PHI fields here — auto-stripped on next deploy.
// ─────────────────────────────────────────────────────────────────────────────

const PHI_PROPS = new Set([
  // Found in OS_purchase schema audit
  "customerEmail",
  "email",
  "firstName",
  "lastName",
  "phoneNumber",
  "phone",
  "full_name",
  "address",
  "dob",
  "date_of_birth",

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

  // PCI — card data must never reach analytics
  "card_number",
  "card_exp_date",
  "card_cvc",
  "OS_card_number",
  "OS_card_exp_date",
  "OS_card_cvc",
]);


// ─────────────────────────────────────────────────────────────────────────────
// ALLOWED ORIGINS
// ─────────────────────────────────────────────────────────────────────────────

const ALLOWED_ORIGINS = [
  "https://eden.health",
  "https://www.eden.health",
  "https://app.eden.health",
  "https://eden-os-rimo-patient-staging.vercel.app",
];


// ─────────────────────────────────────────────────────────────────────────────
// BOT + STATIC ASSET DETECTION
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


// =============================================================================
// WORKER ENTRY POINT
// =============================================================================

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);

      // Health check
      if (url.pathname === "/eden-health-check") {
        return jsonResponse({
          ok: true,
          worker: "eden-analytics",
          version: "3.0",
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

      // Skip bots
      if (isBot(request)) return fetch(request);

      // Skip static assets
      if (isStaticAsset(url)) return fetch(request);

      // /collect — client-side events from analytics.js
      if (url.pathname === "/collect" && request.method === "POST") {
        return handleCollect(request, env, ctx, url);
      }

      // /server-collect — server-side events from Node.js
      if (url.pathname === "/server-collect" && request.method === "POST") {
        return handleServerCollect(request, env, ctx);
      }

      // All page requests — intercept, set cookie, fire page_viewed, pass through
      return handlePageRequest(request, env, ctx, url);

    } catch (err) {
      console.error("[eden-analytics] error:", err);
      return fetch(request); // always serve the page — fail open
    }
  },
};


// =============================================================================
// PAGE REQUEST
// Intercepts every page load.
// Sets ITP-resistant cookie on first visit.
// Fires page_viewed + first_touch to Segment in background.
// Passes through to origin — user sees the page normally.
// =============================================================================

async function handlePageRequest(request, env, ctx, url) {
  const existingAnonId  = readCookie(request, "eden_anon_id");
  const existingSession = readCookie(request, "eden_session_id");
  const isNewVisitor    = !existingAnonId;
  const isNewSession    = !existingSession;

  const anonId  = existingAnonId  || crypto.randomUUID();
  const session = existingSession || `${crypto.randomUUID()}_${Date.now()}`;

  // Pass through to origin — get the real page
  const response = await fetch(request);

  // Append cookies to response
  const headers = new Headers(response.headers);
  if (isNewVisitor) headers.append("Set-Cookie", buildAnonCookie(anonId, url));
  if (isNewSession) headers.append("Set-Cookie", buildSessionCookie(session, url));

  // Fire analytics in background — never blocks page load
  if (env.SEGMENT_WRITE_KEY) {
    ctx.waitUntil(
      firePageEvents(request, env, anonId, session, url, isNewVisitor, isNewSession)
        .catch(err => console.error("[eden-analytics] page event error:", err))
    );
  }

  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}


// =============================================================================
// PAGE EVENTS
// Fires page_viewed on every page load.
// Fires first_touch on new sessions with attribution.
// =============================================================================

async function firePageEvents(request, env, anonId, session, url, isNewVisitor, isNewSession) {
  const cleanUrl   = sanitizeUrl(url);
  const referrer   = sanitizeUrlString(request.headers.get("Referer") || "");
  const ua         = request.headers.get("User-Agent") || "";
  const portal     = url.hostname.includes("app.eden.health") ? "patient" : "marketing";
  const sessionId  = session.split("_")[0];
  const utms       = extractUTMs(url);
  const clickIds   = extractClickIds(url);
  const organic    = !utms && referrer ? detectOrganic(referrer) : null;
  const attribution = { ...(utms || organic || {}), ...clickIds };

  // page_viewed — fires on every page load
  await segmentTrack(env.SEGMENT_WRITE_KEY, {
    anonymousId: anonId,
    event:       "page_viewed",
    properties:  stripPHI({
      portal,
      page_path:      url.pathname,
      page_url:       cleanUrl,
      page_search:    url.search || undefined,
      referrer:       referrer   || undefined,
      device_type:    isMobile(ua) ? "mobile" : "desktop",
      session_id:     sessionId,
      is_new_visitor: isNewVisitor,
      is_new_session: isNewSession,
      ...attribution,
    }),
  });

  // first_touch — fires once per session when attribution present
  if (isNewSession && Object.keys(attribution).length > 0) {
    await segmentTrack(env.SEGMENT_WRITE_KEY, {
      anonymousId: anonId,
      event:       "first_touch",
      properties:  stripPHI({
        portal,
        page_path:  url.pathname,
        page_url:   cleanUrl,
        referrer:   referrer || undefined,
        session_id: sessionId,
        ...attribution,
      }),
    });
  }
}


// =============================================================================
// /collect — CLIENT SIDE EVENTS
// Receives events from analytics.js.
// Enriches with gclid + UTMs from URL.
// Strips PHI.
// Forwards to Segment.
// =============================================================================

async function handleCollect(request, env, ctx, url) {
  const origin = request.headers.get("Origin") || "";

  if (origin && !ALLOWED_ORIGINS.includes(origin)) {
    return new Response("Forbidden", { status: 403 });
  }

  let body;
  try { body = await request.json(); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  // Read anonId from HttpOnly cookie (ITP-resistant) or fall back to body
  const cookieAnonId = readCookie(request, "eden_anon_id");
  const anonId       = cookieAnonId || body.anonymousId || crypto.randomUUID();
  const isNew        = !cookieAnonId;
  const portal       = origin.includes("app.eden.health") ? "patient" : "marketing";

  // Super props added to every event
  const superProps = {
    portal,
    source_type: "client",
    ...extractUTMs(url),
    ...extractClickIds(url),
  };

  if (env.SEGMENT_WRITE_KEY) {
    ctx.waitUntil(
      forwardToSegment(env.SEGMENT_WRITE_KEY, body, anonId, superProps)
        .catch(err => console.error("[eden-analytics] collect error:", err))
    );
  }

  const headers = {
    "Content-Type": "application/json",
    ...corsHeadersObj(origin),
  };

  // Set ITP cookie for new visitors
  if (isNew) headers["Set-Cookie"] = buildAnonCookie(anonId, url);

  return new Response(JSON.stringify({ ok: true, anonId }), {
    status: 200,
    headers,
  });
}


// =============================================================================
// /server-collect — SERVER SIDE EVENTS
// Receives events from Node.js API.
// Authenticates via X-Eden-Server-Secret header.
// Strips PHI.
// Forwards to Segment.
//
// USAGE (Node.js):
//   await fetch('https://app.eden.health/server-collect', {
//     method: 'POST',
//     headers: {
//       'Content-Type': 'application/json',
//       'X-Eden-Server-Secret': process.env.EDEN_SERVER_API_SECRET,
//     },
//     body: JSON.stringify({
//       event: 'OS_purchase',
//       userId: user.id,
//       anonymousId: req.cookies['eden_anon_id'],
//       properties: { ... }
//     })
//   });
// =============================================================================

async function handleServerCollect(request, env, ctx) {
  // Authenticate — if secret configured, validate it
  if (env.SERVER_API_SECRET) {
    const secret = request.headers.get("X-Eden-Server-Secret");
    if (secret !== env.SERVER_API_SECRET) {
      return new Response("Unauthorized", { status: 401 });
    }
  }

  let body;
  try { body = await request.json(); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  const anonId     = body.anonymousId || "server-side";
  const superProps = { portal: "patient", source_type: "server" };

  if (env.SEGMENT_WRITE_KEY) {
    ctx.waitUntil(
      forwardToSegment(env.SEGMENT_WRITE_KEY, body, anonId, superProps)
        .catch(err => console.error("[eden-analytics] server-collect error:", err))
    );
  }

  return jsonResponse({ ok: true });
}


// =============================================================================
// SEGMENT FORWARDING
// Routes to correct Segment call: track / identify / page
// Merges super props.
// Strips PHI from all properties.
// Hashes raw email automatically.
// =============================================================================

async function forwardToSegment(writeKey, body, anonId, superProps) {
  const type = (body.type || "track").toLowerCase();

  if (type === "identify") {
    const traits = await sanitizeProperties(body.traits || body.properties || {});
    await segmentPost(writeKey, "identify", {
      anonymousId: anonId,
      userId:      body.userId || null,
      traits,
      context:     body.context || {},
      timestamp:   new Date().toISOString(),
    });
    return;
  }

  if (type === "page") {
    const props = await sanitizeProperties({ ...superProps, ...(body.properties || {}) });
    await segmentPost(writeKey, "page", {
      anonymousId: anonId,
      userId:      body.userId || null,
      name:        body.name || "",
      properties:  props,
      context:     body.context || {},
      timestamp:   new Date().toISOString(),
    });
    return;
  }

  // Default: track
  await segmentTrack(writeKey, {
    anonymousId: anonId,
    userId:      body.userId || null,
    event:       body.event || "",
    properties:  await sanitizeProperties({ ...superProps, ...(body.properties || {}) }),
    context:     body.context || {},
    timestamp:   body.sentAt || body.timestamp || new Date().toISOString(),
  });
}

async function segmentTrack(writeKey, payload) {
  if (payload.properties) {
    payload.properties = await sanitizeProperties(payload.properties);
  }
  await segmentPost(writeKey, "track", payload);
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
// PHI STRIPPING + EMAIL HASHING
// Strips PHI_PROPS from top-level and nested objects.
// Auto-hashes raw email → email_sha256.
// =============================================================================

async function sanitizeProperties(props) {
  if (!props || typeof props !== "object") return props;
  const out = {};
  for (const [k, v] of Object.entries(props)) {
    // Strip PHI
    if (PHI_PROPS.has(k)) continue;

    // Hash raw email
    if ((k === "email" || k === "customerEmail") && typeof v === "string") {
      out["email_sha256"] = await sha256(v);
      continue;
    }

    // Recursively sanitize nested objects
    if (v && typeof v === "object" && !Array.isArray(v)) {
      out[k] = await sanitizeNested(v);
      continue;
    }

    out[k] = v;
  }
  return out;
}

async function sanitizeNested(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    if (PHI_PROPS.has(k)) continue;
    if (k === "email" && typeof v === "string") {
      out["email_sha256"] = await sha256(v);
      continue;
    }
    if (v && typeof v === "object" && !Array.isArray(v)) {
      out[k] = await sanitizeNested(v);
      continue;
    }
    out[k] = v;
  }
  return out;
}

function stripPHI(props) {
  if (!props || typeof props !== "object") return props;
  const out = {};
  for (const [k, v] of Object.entries(props)) {
    if (!PHI_PROPS.has(k)) out[k] = v;
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
    "gbraid",    // Google iOS
    "wbraid",    // Google web
    "dclid",     // Google Display
    "fbclid",    // Facebook
    "msclkid",   // Microsoft/Bing
    "ttclid",    // TikTok
    "twclid",    // Twitter
    "li_fat_id", // LinkedIn
    "rdt_cid",   // Reddit
    "epik",      // Pinterest
    "ScCid",     // Snapchat
    "ttclid",    // TikTok
    "nbt",       // Northbeam
    "click_id",  // Generic
  ];
  for (const k of keys) {
    const v = url.searchParams.get(k);
    if (v) out[k] = v;
  }
  return out;
}


// =============================================================================
// ORGANIC SEARCH DETECTION
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
// COOKIE HELPERS
// HttpOnly = ITP-resistant (Safari cannot wipe)
// Domain=.eden.health = spans both eden.health and app.eden.health
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
    "Max-Age=63072000",              // 2 years
    `Domain=${cookieDomain(url)}`,   // .eden.health — spans both portals
    "Path=/",
    "HttpOnly",                      // ITP-resistant
    "Secure",
    "SameSite=Lax",
  ].join("; ");
}

function buildSessionCookie(value, url) {
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
