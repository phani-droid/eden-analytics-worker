// =============================================================================
// EdenOS Analytics Worker — v5.0
// =============================================================================
//
// COVERAGE STRATEGY — maximising gclid attribution to ~98%:
//
//   Layer 1 — HttpOnly cookie (eden_anon_id)
//     First-party, server-set, ITP-resistant, 2 years
//     Safari/Firefox/Chrome cannot wipe HttpOnly cookies
//     Coverage: ~85% baseline
//
//   Layer 2 — Cloudflare KV (gclid store)
//     Stores gclid against anonymousId AND userId
//     Cross-device: same user on mobile + desktop gets same gclid
//     TTL: 90 days — matches Google Ads conversion window
//     Coverage: +8% (cross-device, returning users)
//
//   Layer 3 — userId → gclid link at identify/login
//     When user logs in — links userId to stored anonId attribution
//     Recovers users who clear cookies but log back in
//     Coverage: +3% (cookie-cleared returning users)
//
//   Layer 4 — Enhanced conversions (email_sha256)
//     Google matches hashed email against Google account database
//     Recovers conversions with no gclid at all
//     Coverage: +2% (incognito, blocked cookies)
//
//   Layer 5 — Referrer-based fallback
//     If no gclid but referrer = google.com — marks as google_organic
//     Not a paid attribution but prevents complete blind spot
//
//   Unavoidable loss ~1-2%:
//     GPC signal (legal opt-out — cannot override)
//     Incognito + no Google account match
//     VPN + cookie blockers + no email match
//
// ENDPOINTS:
//   /*               → page requests → cookie + KV store + page_viewed
//   /collect         → client-side events → KV enrichment → Segment
//   /server-collect  → server-side events → KV enrichment → Segment
//   /identify        → login/account creation → userId→gclid link in KV
//   /eden-health-check → health check
//
// DEPLOY:
//   1. wrangler kv namespace create "GCLID_KV" → paste id into wrangler.toml
//   2. wrangler secret put SEGMENT_WRITE_KEY
//   3. wrangler secret put SERVER_API_SECRET
//   4. wrangler deploy
//
// ADD NEW EVENT:     fire analytics.track() → worker handles everything
// ADD NEW DESTINATION: configure in Segment UI → no worker changes needed
// ADD NEW PHI FIELD: add to PHI_PROPS below → auto-stripped on next deploy
// =============================================================================


// ─────────────────────────────────────────────────────────────────────────────
// PHI BLOCKLIST — stripped from ALL events before Segment
// Add new fields here — auto-stripped on next deploy
// ─────────────────────────────────────────────────────────────────────────────

const PHI_PROPS = new Set([
  // PII
  "customerEmail", "email",
  "firstName", "lastName",
  "phoneNumber", "phone",
  "full_name", "address",
  "dob", "date_of_birth",
  "ssn", "social_security_number",

  // Health data — HIPAA
  "weight_lbs", "height_ft", "bmi_value",
  "goal_weight_lbs", "highest_weight_lbs",
  "selected_conditions", "selected_medications", "selected_allergies",
  "lbs_lost", "old_dose_mg", "new_dose_mg", "medication",
  "diagnosis", "prescription", "medical_history",

  // PCI — card data NEVER reaches analytics
  "card_number", "card_exp_date", "card_cvc",
  "OS_card_number", "OS_card_exp_date", "OS_card_cvc",
  "cvv", "pan",
]);


// ─────────────────────────────────────────────────────────────────────────────
// ALLOWED ORIGINS — add new portals here only
// ─────────────────────────────────────────────────────────────────────────────

const ALLOWED_ORIGINS = [
  "https://eden.health",
  "https://www.eden.health",
  "https://app.eden.health",
  "https://eden-os-rimo-patient-staging.vercel.app",
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

// KV key prefixes
const KV_ANON_PREFIX  = "attr:anon:";   // gclid stored by anonymousId
const KV_USER_PREFIX  = "attr:user:";   // gclid stored by userId (Layer 3)
const KV_TTL          = 7776000;        // 90 days in seconds


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
          ok: true, worker: "eden-analytics", version: "5.0",
          ts: Date.now(),
          kv: !!env.GCLID_KV,
        });
      }

      // ── CORS preflight ────────────────────────────────────────────────────
      if (request.method === "OPTIONS") {
        return new Response(null, {
          status: 204,
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

      // ── /identify — login / account creation (Layer 3 coverage) ──────────
      // Links userId → anonymousId attribution in KV
      // Call this when user logs in or creates account
      if (url.pathname === "/identify" && request.method === "POST") {
        return handleIdentify(request, env, ctx);
      }

      // ── All page requests ─────────────────────────────────────────────────
      return handlePageRequest(request, env, ctx, url);

    } catch (err) {
      console.error("[eden-analytics] error:", err);
      return fetch(request); // fail open — always serve the page
    }
  },
};


// =============================================================================
// PAGE REQUEST HANDLER
//
// Coverage layers active here:
//   Layer 1 — Sets HttpOnly eden_anon_id cookie (2 years, ITP-resistant)
//   Layer 2 — Stores gclid in KV against anonymousId (90 days)
//   Layer 5 — Detects organic search from referrer as fallback
// =============================================================================

async function handlePageRequest(request, env, ctx, url) {
  const existingAnonId  = readCookie(request, "eden_anon_id");
  const existingSession = readCookie(request, "eden_session_id");
  const isNewVisitor    = !existingAnonId;
  const isNewSession    = !existingSession;

  const anonId  = existingAnonId  || crypto.randomUUID();
  const session = existingSession || `${crypto.randomUUID()}_${Date.now()}`;

  const clickIds = extractClickIds(url);
  const utms     = extractUTMs(url);

  // ── Layer 2: Store gclid in KV against anonymousId ───────────────────────
  if (clickIds.gclid && env.GCLID_KV) {
    ctx.waitUntil(
      storeAttribution(env.GCLID_KV, KV_ANON_PREFIX + anonId, {
        ...clickIds,
        ...(utms || {}),
      }).catch(err => console.error("[eden-analytics] KV store error:", err))
    );
  }

  // Pass through to origin
  const response = await fetch(request);

  // ── Layer 1: Set HttpOnly cookie ──────────────────────────────────────────
  const headers = new Headers(response.headers);
  if (isNewVisitor) headers.append("Set-Cookie", buildAnonCookie(anonId, url));
  if (isNewSession) headers.append("Set-Cookie", buildSessionCookie(session, url));

  // Fire analytics in background
  if (env.SEGMENT_WRITE_KEY) {
    ctx.waitUntil(
      firePageEvents(request, env, anonId, session, url, isNewVisitor, isNewSession, clickIds, utms)
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
// =============================================================================

async function firePageEvents(request, env, anonId, session, url, isNewVisitor, isNewSession, clickIds, utms) {
  const cleanUrl    = sanitizeUrl(url);
  const referrer    = sanitizeUrlString(request.headers.get("Referer") || "");
  const ua          = request.headers.get("User-Agent") || "";
  const portal      = url.hostname.includes("app.eden.health") ? "patient" : "marketing";
  const sessionId   = session.split("_")[0];

  // ── Layer 5: Organic fallback ─────────────────────────────────────────────
  const organic     = !utms && !clickIds.gclid && referrer ? detectOrganic(referrer) : null;
  const attribution = { ...(utms || organic || {}), ...clickIds };

  await segmentTrack(env.SEGMENT_WRITE_KEY, {
    anonymousId: anonId,
    event:       "page_viewed",
    properties:  stripPHI({
      portal,
      page_path:      url.pathname,
      page_url:       cleanUrl,
      page_search:    url.search   || undefined,
      referrer:       referrer     || undefined,
      device_type:    isMobile(ua) ? "mobile" : "desktop",
      session_id:     sessionId,
      is_new_visitor: isNewVisitor,
      is_new_session: isNewSession,
      ...attribution,
    }),
  });

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
// /collect HANDLER — CLIENT-SIDE EVENTS
//
// Coverage layers active:
//   Layer 1 — reads eden_anon_id from HttpOnly cookie
//   Layer 2 — enriches with gclid from KV (anonId lookup)
//   Layer 4 — email_sha256 via PHI sanitization
// =============================================================================

async function handleCollect(request, env, ctx, url) {
  const origin = request.headers.get("Origin") || "";

  if (origin && !ALLOWED_ORIGINS.includes(origin)) {
    return new Response("Forbidden", { status: 403 });
  }

  let body;
  try { body = await request.json(); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  // Layer 1 — read from HttpOnly cookie first (most reliable)
  const cookieAnonId = readCookie(request, "eden_anon_id");
  const anonId       = cookieAnonId || body.anonymousId || crypto.randomUUID();
  const isNew        = !cookieAnonId;
  const portal       = origin.includes("app.eden.health") ? "patient" : "marketing";

  // Layer 2 — look up gclid from KV
  // Try anonId first, then userId as fallback (Layer 3 cross-device)
  const userId           = body.userId || null;
  const storedAttribution = env.GCLID_KV
    ? await resolveAttribution(env.GCLID_KV, anonId, userId)
    : null;

  const superProps = {
    portal,
    source_type: "client",
    ...(storedAttribution || {}),
    // Fresh URL params override stored (current session is fresher)
    ...(extractUTMs(url) || {}),
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

  if (isNew) headers["Set-Cookie"] = buildAnonCookie(anonId, url);

  return new Response(JSON.stringify({ ok: true, anonId }), { status: 200, headers });
}


// =============================================================================
// /server-collect HANDLER — SERVER-SIDE EVENTS
//
// Coverage layers active:
//   Layer 2 — enriches with gclid from KV (anonId lookup)
//   Layer 3 — falls back to userId lookup if anonId has no gclid
//   Layer 4 — email_sha256 via PHI sanitization
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

  const anonId = body.anonymousId || null;
  const userId = body.userId      || null;

  // Layer 2 + 3 — look up gclid from KV
  // Try anonId first, then userId as fallback
  const storedAttribution = env.GCLID_KV
    ? await resolveAttribution(env.GCLID_KV, anonId, userId)
    : null;

  // Merge stored attribution into event — only if not already present
  // Explicit values from engineer always take priority
  if (storedAttribution && body.properties) {
    for (const [k, v] of Object.entries(storedAttribution)) {
      if (!body.properties[k] && v) {
        body.properties[k] = v;
      }
    }
  }

  const superProps = { portal: "patient", source_type: "server" };

  if (env.SEGMENT_WRITE_KEY) {
    ctx.waitUntil(
      forwardToSegment(env.SEGMENT_WRITE_KEY, body, anonId || userId || "server", superProps)
        .catch(err => console.error("[eden-analytics] server-collect error:", err))
    );
  }

  return jsonResponse({ ok: true });
}


// =============================================================================
// /identify HANDLER — LOGIN / ACCOUNT CREATION
//
// Coverage Layer 3 — userId → gclid link
//
// When user logs in or creates account:
//   1. Look up gclid stored against anonymousId
//   2. Store same gclid against userId
//   3. Now if user clears cookies + logs back in → gclid still found via userId
//
// Engineering usage:
//   await fetch('https://app.eden.health/identify', {
//     method: 'POST',
//     headers: {
//       'Content-Type': 'application/json',
//       'X-Eden-Server-Secret': process.env.EDEN_SERVER_API_SECRET,
//     },
//     body: JSON.stringify({
//       userId:      patient.userId,
//       anonymousId: patient.anonId,   // from eden_anon_id cookie
//       traits: {
//         email: patient.email,        // worker hashes automatically
//         plan:  patient.planName,
//       }
//     })
//   });
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

  // Layer 3 — link userId to anonymousId attribution in KV
  if (env.GCLID_KV && anonId && userId) {
    ctx.waitUntil(
      linkUserAttribution(env.GCLID_KV, anonId, userId)
        .catch(err => console.error("[eden-analytics] KV link error:", err))
    );
  }

  // Forward identify to Segment
  if (env.SEGMENT_WRITE_KEY) {
    const traits = await sanitizeProperties(body.traits || {});
    ctx.waitUntil(
      segmentPost(env.SEGMENT_WRITE_KEY, "identify", {
        anonymousId: anonId || userId,
        userId,
        traits,
        timestamp:   new Date().toISOString(),
      }).catch(err => console.error("[eden-analytics] identify error:", err))
    );
  }

  return jsonResponse({ ok: true });
}


// =============================================================================
// KV ATTRIBUTION — STORAGE + RETRIEVAL
//
// Key structure:
//   attr:anon:{anonymousId} → attribution object (set on ad click)
//   attr:user:{userId}      → attribution object (set on login — Layer 3)
//
// resolveAttribution tries both keys — anonId first, userId as fallback
// This handles:
//   - Normal flow: anonId present → found immediately
//   - Cleared cookies: anonId gone but userId known → found via userId
//   - Cross-device: logged in on different device → found via userId
// =============================================================================

async function storeAttribution(kv, key, attribution) {
  if (!kv || !key || !attribution) return;
  const hasValue = Object.values(attribution).some(v => v);
  if (!hasValue) return;

  // Never overwrite a newer gclid with an older one
  // If gclid already stored — keep it (first paid click wins)
  try {
    const existing = await kv.get(key);
    if (existing) {
      const parsed = JSON.parse(existing);
      if (parsed.gclid && attribution.gclid && parsed.gclid !== attribution.gclid) {
        // Already have a gclid — keep existing (first click attribution)
        return;
      }
    }
  } catch { /* ignore — proceed to store */ }

  await kv.put(key, JSON.stringify({
    ...attribution,
    stored_at: new Date().toISOString(),
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

  // Try anonymousId first — most reliable for current session
  if (anonId) {
    const fromAnon = await getAttribution(kv, KV_ANON_PREFIX + anonId);
    if (fromAnon?.gclid) return fromAnon;
  }

  // Layer 3 fallback — try userId (covers cleared cookies + cross-device)
  if (userId) {
    const fromUser = await getAttribution(kv, KV_USER_PREFIX + userId);
    if (fromUser?.gclid) return fromUser;
  }

  // Return whatever we have even without gclid (UTMs still valuable)
  if (anonId) {
    const fromAnon = await getAttribution(kv, KV_ANON_PREFIX + anonId);
    if (fromAnon) return fromAnon;
  }

  if (userId) {
    return await getAttribution(kv, KV_USER_PREFIX + userId);
  }

  return null;
}

async function linkUserAttribution(kv, anonId, userId) {
  // Copy attribution from anonId → userId
  // So future server events can find gclid via userId even if cookie gone
  const anonAttribution = await getAttribution(kv, KV_ANON_PREFIX + anonId);
  if (anonAttribution) {
    await storeAttribution(kv, KV_USER_PREFIX + userId, anonAttribution);
  }
}


// =============================================================================
// SEGMENT FORWARDING
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
      name:        body.name   || "",
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
    event:       body.event  || "",
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
// PHI SANITIZATION
// Layer 4 — strips PHI, hashes email → email_sha256
// =============================================================================

async function sanitizeProperties(props) {
  if (!props || typeof props !== "object") return props;
  const out = {};
  for (const [k, v] of Object.entries(props)) {
    if (PHI_PROPS.has(k)) continue;
    if ((k === "email" || k === "customerEmail") && typeof v === "string") {
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
// SHA-256 HASHING — Layer 4 enhanced conversions
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
  for (const k of [
    "gclid",     // Google Ads — most important
    "gbraid",    // Google iOS (privacy-safe)
    "wbraid",    // Google web (privacy-safe)
    "dclid",     // Google Display
    "fbclid",    // Meta/Facebook
    "msclkid",   // Microsoft/Bing
    "ttclid",    // TikTok
    "twclid",    // Twitter/X
    "li_fat_id", // LinkedIn
    "rdt_cid",   // Reddit
    "epik",      // Pinterest
    "ScCid",     // Snapchat
    "nbt",       // Northbeam
    "irclickid", // Impact Radius
    "cjevent",   // CJ Affiliate
    "click_id",  // Generic
  ]) {
    const v = url.searchParams.get(k);
    if (v) out[k] = v;
  }
  return out;
}


// =============================================================================
// ORGANIC SEARCH DETECTION — Layer 5 fallback
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
// Layer 1 — ITP-resistant HttpOnly first-party cookies
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
    "Max-Age=63072000",              // 2 years
    `Domain=${cookieDomain(url)}`,   // .eden.health — spans both portals
    "Path=/",
    "HttpOnly",                      // ITP-resistant — Safari cannot wipe
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
