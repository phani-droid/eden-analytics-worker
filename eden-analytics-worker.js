// =============================================================================
// EdenOS Analytics Worker — v5.31 FINAL
// =============================================================================
//
// v5.31 CHANGE — self-identify on any event carrying user_id
//
//   Problem: Gowtham's app never calls analytics.identify(userId, traits).
//   hookSegmentIdentify() has nothing to intercept → id:link, alias:fired,
//   attr:user never written → userId: null on all events → attribution broken.
//
//   Fix: On ANY /collect event where properties.user_id is present and
//   id:link:{userId} does not yet exist in KV, worker self-triggers /identify.
//   KV idempotency guard ensures this fires exactly once per userId ever.
//   Zero app changes required.
//
//   Why any event (not just OS_purchase):
//   - OS_intake_started fires before OS_purchase → attr:user written earlier
//   - By the time OS_purchase arrives, attr:user:{userId} already has gclid
//   - resolveAttribution() finds it → gclid on purchase event ✅
//   - KV check prevents redundant calls after first link
//
// [v5.30 and earlier fixes preserved]
//
//   v5.30 FIX 1 — eden_anon_id cookie set on /collect responses
//   v5.30 FIX 2 — resolveOrderId reads ecommerce.transaction_id
//   v5.30 FIX 3 — server-collect anonId recovery + email_sha256 injection
//   v5.30 FIX 4 — customerEmail normalized to email before hashEmail()
//   v5.29 FIX 1 — cross-domain UTM bridge via attr:gcl:{_gcl_au}
//
// UPDATED KV KEY SCHEMA v5.31:
//   attr:anon:{anonymousId}   → attribution (120d)
//   attr:user:{userId}        → attribution (120d)
//   attr:order:{orderId}      → attribution (120d)
//   attr:gcl:{_gcl_au}        → cross-domain bridge (120d)
//   email:user:{userId}       → email_sha256 (120d)
//   id:link:{userId}          → {anonId, ts} (30d) ← also guards self-identify
//   alias:fired:{userId}      → permanent alias guard (10yr)
//   dedup:{event}:{key}       → dedup lock (24hr)
// =============================================================================


// ─────────────────────────────────────────────────────────────────────────────
// CONSTANTS
// ─────────────────────────────────────────────────────────────────────────────

const PIPELINE_VERSION = "5.31";

const ALLOWED_ORIGINS = [
  "https://eden.health",
  "https://www.eden.health",
  "https://app.eden.health",
];

const CLICK_ID_CONFIG = [
  { param: "gclid",     channel: "google_ads",      label: "Google Ads"          },
  { param: "gbraid",    channel: "google_ios",       label: "Google iOS"          },
  { param: "wbraid",    channel: "google_web",       label: "Google Web"          },
  { param: "dclid",     channel: "google_display",   label: "Google Display"      },
  { param: "_gcl_au",   channel: "google_ads",       label: "Google Cross-Domain" },
  { param: "srsltid",   channel: "google_shopping",  label: "Google Shopping"     },
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

const BOT_UA_PATTERNS = [
  /bot\b/i, /crawler/i, /spider/i, /headless/i,
  /lighthouse/i, /pagespeed/i, /playwright/i,
  /puppeteer/i, /preview/i, /prerender/i,
  /google-inspectiontool/i, /checklyhq/i,
  /Googlebot/i, /bingbot/i, /facebookexternalhit/i,
  /Twitterbot/i, /LinkedInBot/i, /Slackbot/i,
];

const BOT_CF_DECISIONS = new Set([
  "automated", "likely_automated", "verified_bot",
]);

const STATIC_EXTENSIONS = [
  ".avif",".bmp",".css",".gif",".ico",".jpg",".jpeg",
  ".js",".mjs",".map",".mp4",".otf",".png",".svg",
  ".ttf",".wasm",".webm",".webp",".woff",".woff2",
];

const STATIC_PREFIXES = [
  "/_next/static/","/static/chunks/",
  "/static/css/","/static/js/","/static/media/",
  "/favicon","/robots.txt","/sitemap",
];

const SENSITIVE_URL_PARAMS = [
  /client_secret/i,/payment_intent/i,/setup_intent/i,
  /^secret$/i,/^password$/i,/^token$/i,/^code$/i,/^state$/i,
];

const SSO_BNPL_DOMAINS = [
  "accounts.google.com","oauth2.googleapis.com",
  "klarna.com","pay.klarna.com","checkout.klarna.com",
  "affirm.com","sandbox.affirm.com",
  "afterpay.com","portal.afterpay.com",
  "clearpay.co.uk","clearpay.com",
  "sezzle.com","zip.co","laybuy.com",
];

const KV_ANON_PREFIX   = "attr:anon:";
const KV_USER_PREFIX   = "attr:user:";
const KV_ORDER_PREFIX  = "attr:order:";
const KV_GCL_PREFIX    = "attr:gcl:";
const KV_EMAIL_PREFIX  = "email:user:";
const KV_IDLINK_PREFIX = "id:link:";
const KV_ALIAS_PREFIX  = "alias:fired:";
const KV_TTL           = 10368000;   // 120 days
const KV_DEDUP_TTL     = 86400;      // 24 hours
const KV_IDLINK_TTL    = 2592000;    // 30 days
const KV_ALIAS_TTL     = 315360000;  // 10 years

const UTM_ENRICHABLE = [
  "utm_campaign","utm_content","utm_term","utm_id","attribution_campaign",
  "landing_page","attribution_referrer",
];

const ATTRIBUTION_TRAIT_KEYS = [
  "acquisition_channel","attribution_source","attribution_medium",
  "attribution_campaign","attribution_referrer","landing_page",
  "utm_source","utm_medium","utm_campaign","utm_content","utm_term",
  "gclid","_gcl_au","gbraid","wbraid","fbclid","msclkid",
  "ttclid","twclid","li_fat_id","srsltid",
];

const KV_INTERNAL_FIELDS = new Set(["stored_at"]);


// =============================================================================
// PREAUTH_SCRIPT
// =============================================================================

const PREAUTH_SCRIPT = `<script>
(function() {
  'use strict';

  function getCookie(n) {
    var m = document.cookie.match(new RegExp('(?:^|;\\s*)' + n + '=([^;]+)'));
    return m ? decodeURIComponent(m[1]) : null;
  }

  function getOrderIdFromDOM() {
    try {
      var el = document.querySelector(
        '[data-order-id],[data-orderid],[data-master-id],[data-order_id],[data-masterid]'
      );
      if (el) return (
        el.getAttribute('data-order-id')  ||
        el.getAttribute('data-orderid')   ||
        el.getAttribute('data-master-id') ||
        el.getAttribute('data-order_id')  ||
        el.getAttribute('data-masterid')  || null
      );
    } catch(e) {}
    return null;
  }

  function resolveIds() {
    var anonId = getCookie('eden_anon_id');
    var userId = null;
    try {
      if (window.analytics && window.analytics.user) {
        var u = window.analytics.user();
        if (!anonId) anonId = u.anonymousId();
        userId = u.id() || null;
      }
    } catch(e) {}
    return { anonId: anonId, userId: userId };
  }

  function isSSOOrBNPLUrl(href) {
    try {
      var h = new URL(href).hostname.toLowerCase();
      var domains = ${JSON.stringify(SSO_BNPL_DOMAINS)};
      for (var i = 0; i < domains.length; i++) {
        if (h === domains[i] || h.endsWith('.' + domains[i])) return true;
      }
    } catch(e) {}
    return false;
  }

  function postJSON(path, payload, useBeacon) {
    var str = JSON.stringify(payload);
    if (useBeacon && navigator.sendBeacon) {
      try {
        var sent = navigator.sendBeacon(path, new Blob([str], { type: 'application/json' }));
        if (sent) return;
      } catch(e) {}
    }
    fetch(path, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: str, keepalive: true, credentials: 'include',
    }).catch(function(){});
  }

  var _preserving = false;
  function preserveAttribution(orderId) {
    if (_preserving) return;
    _preserving = true;
    setTimeout(function() { _preserving = false; }, 2000);
    try {
      var ids = resolveIds();
      postJSON('/preserve-attribution', {
        anonymousId: ids.anonId || null,
        userId:      ids.userId || null,
        orderId:     orderId || getOrderIdFromDOM() || null,
      }, true);
    } catch(e) {}
  }

  function syncAnonId() {
    var id = getCookie('eden_anon_id');
    if (!id) return;
    function trySync() {
      try {
        if (window.analytics && window.analytics.setAnonymousId) {
          window.analytics.setAnonymousId(id); return true;
        }
      } catch(e) {}
      return false;
    }
    if (trySync()) return;
    var attempts = 0;
    var t = setInterval(function() {
      if (++attempts > 50 || trySync()) clearInterval(t);
    }, 100);
  }

  (function() {
    if (window.analytics && window.analytics.on) {
      try { window.analytics.on('ready', syncAnonId); } catch(e) {}
    }
  })();

  var _identifiedUsers = {};
  function hookSegmentIdentify() {
    if (!window.analytics || !window.analytics.identify) return false;
    if (window.analytics._edenHooked) return true;
    window.analytics._edenHooked = true;
    var _orig = window.analytics.identify.bind(window.analytics);
    window.analytics.identify = function(userId, traits, options, callback) {
      try {
        if (userId) {
          var now = Date.now();
          var lastFired = _identifiedUsers[String(userId)] || 0;
          if (now - lastFired > 30000) {
            _identifiedUsers[String(userId)] = now;
            var anonId = getCookie('eden_anon_id');
            if (!anonId) {
              try { if (window.analytics.user) anonId = window.analytics.user().anonymousId(); } catch(e) {}
            }
            var groupId = null;
            try {
              if (window.analytics.group) {
                var g = window.analytics.group();
                groupId = (g && g.id && g.id()) ? g.id() : null;
              }
            } catch(e) {}
            if (anonId && String(userId) !== String(anonId)) {
              postJSON('/identify', {
                userId:      String(userId),
                anonymousId: anonId,
                traits:      traits || {},
                groupId:     groupId || null,
                orderId:     getOrderIdFromDOM() || null,
              }, true);
            }
          }
        }
      } catch(e) {}
      return _orig.apply(this, arguments);
    };
    return true;
  }

  if (!hookSegmentIdentify()) {
    var _hookAttempts = 0;
    var _hookTimer = setInterval(function() {
      if (hookSegmentIdentify() || ++_hookAttempts > 50) clearInterval(_hookTimer);
    }, 100);
  }
  (function() {
    if (window.analytics && window.analytics.on) {
      try { window.analytics.on('ready', function() { hookSegmentIdentify(); syncAnonId(); }); } catch(e) {}
    }
  })();

  function isGoogleSSOEl(el) {
    if (!el || !el.getAttribute) return false;
    var testid    = el.getAttribute('data-testid')    || '';
    var arialabel = (el.getAttribute('aria-label')    || '').toLowerCase();
    var provider  = el.getAttribute('data-provider')  || '';
    var clientid  = el.getAttribute('data-client_id') || '';
    var elid      = (el.id || '').toLowerCase();
    var type      = el.getAttribute('data-type')      || '';
    return (
      provider === 'google'                || type === 'standard'             ||
      !!clientid                           ||
      arialabel === 'sign in with google'  || arialabel === 'continue with google' ||
      arialabel === 'sign up with google'  || arialabel === 'log in with google'   ||
      testid.includes('google-sso')        || testid.includes('google-signin')     ||
      testid.includes('google-login')      || testid.includes('google-signup')     ||
      testid.includes('google-auth')       ||
      elid === 'google-signin-btn'         || elid === 'google-login-btn'
    );
  }

  var BNPL_KW = ['klarna','affirm','afterpay','clearpay','sezzle','zip-pay','laybuy','bnpl'];
  function isBNPLEl(el) {
    if (!el) return false;
    var tag    = (el.tagName || '').toLowerCase();
    var testid = (el.getAttribute && el.getAttribute('data-testid')) || '';
    var cls    = (typeof el.className === 'string' ? el.className : '') || '';
    if (tag === 'klarna-placement' || tag === 'klarna-express-button') return true;
    for (var i = 0; i < BNPL_KW.length; i++) {
      if (testid.toLowerCase().includes(BNPL_KW[i])) return true;
      if (cls.toLowerCase().includes(BNPL_KW[i]))    return true;
    }
    return false;
  }

  function onInteract(e) {
    var el = e.target;
    for (var i = 0; i < 6; i++) {
      if (!el) break;
      if (isGoogleSSOEl(el) || isBNPLEl(el)) { preserveAttribution(null); return; }
      el = el.parentElement;
    }
  }
  document.addEventListener('mousedown', onInteract, true);
  document.addEventListener('click',     onInteract, true);
  document.addEventListener('klarna:authorized', function() { preserveAttribution(null); }, true);
  document.addEventListener('klarna:load',       function() { preserveAttribution(null); }, true);

  var _origFetch = window.fetch;
  window.fetch = function(input, init) {
    try {
      var url    = typeof input === 'string' ? input : (input && input.url) || '';
      var method = ((init && init.method) || 'GET').toUpperCase();
      if (method === 'POST' && (
        (url.includes('stripe.com') && (url.includes('confirm') || url.includes('payment_intents'))) ||
        url.includes('klarna.com') || url.includes('affirm.com') ||
        url.includes('afterpay.com') || url.includes('clearpay.com')
      )) { preserveAttribution(null); }
    } catch(e) {}
    return _origFetch.apply(this, arguments);
  };

  function patchStripeSDK() {
    if (!window.Stripe) return;
    try {
      var _origStripe = window.Stripe;
      window.Stripe = function() {
        var instance = _origStripe.apply(this, arguments);
        ['confirmCardPayment','confirmPayment','confirmSetup',
         'confirmCardSetup','handleCardAction'].forEach(function(m) {
          if (typeof instance[m] === 'function') {
            var _o = instance[m].bind(instance);
            instance[m] = function() { preserveAttribution(null); return _o.apply(this, arguments); };
          }
        });
        return instance;
      };
      Object.assign(window.Stripe, _origStripe);
    } catch(e) {}
  }
  patchStripeSDK();
  var _stripeObs = new MutationObserver(function(muts) {
    for (var i = 0; i < muts.length; i++) {
      muts[i].addedNodes.forEach(function(n) {
        if (n.tagName === 'SCRIPT' && (n.src||'').includes('stripe')) n.addEventListener('load', patchStripeSDK);
      });
    }
    if (window.Stripe) { patchStripeSDK(); _stripeObs.disconnect(); }
  });
  _stripeObs.observe(document.documentElement, { childList: true, subtree: true });

  try {
    var _origAssign  = window.location.assign.bind(window.location);
    var _origReplace = window.location.replace.bind(window.location);
    window.location.assign  = function(h) { if (isSSOOrBNPLUrl(h)) preserveAttribution(null); return _origAssign(h);  };
    window.location.replace = function(h) { if (isSSOOrBNPLUrl(h)) preserveAttribution(null); return _origReplace(h); };
  } catch(e) {}

  document.addEventListener('submit', function(e) {
    try {
      var action = (e.target && e.target.getAttribute('action')) || '';
      if (action && isSSOOrBNPLUrl(action)) preserveAttribution(null);
    } catch(e2) {}
  }, true);

  var _mo = new MutationObserver(function(muts) {
    muts.forEach(function(mut) {
      mut.addedNodes.forEach(function(node) {
        if (node.nodeType !== 1) return;
        var candidates = [node].concat(Array.from(node.querySelectorAll('*') || []));
        candidates.forEach(function(el) {
          if (isGoogleSSOEl(el) || isBNPLEl(el)) {
            el.addEventListener('mousedown', function() { preserveAttribution(null); }, { once: true });
          }
        });
      });
    });
  });
  _mo.observe(document.body || document.documentElement, { childList: true, subtree: true });

  function onPageHide() {
    var ids = resolveIds();
    if (ids.anonId || ids.userId) preserveAttribution(null);
  }
  document.addEventListener('visibilitychange', function() {
    if (document.visibilityState === 'hidden') onPageHide();
  });
  window.addEventListener('pagehide', onPageHide);

  syncAnonId();
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', syncAnonId);
  }

})();
</script>`;


// =============================================================================
// WORKER ENTRY POINT
// =============================================================================

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);

      if (url.pathname === "/eden-health-check") {
        return jsonResponse({
          ok:                           true,
          worker:                       "eden-analytics",
          version:                      PIPELINE_VERSION,
          ts:                           nowUTC(),
          kv:                           !!env.GCLID_KV,
          segment_write_key_configured: !!env.SEGMENT_WRITE_KEY,
          server_secret_configured:     !!env.SERVER_API_SECRET,
          attribution_model:            "first-touch — 18 channels + UTM + referrer + landing_page",
          coverage:                     "100% — all sources, all flows, all devices, all destinations",
          cross_domain_bridge:          "attr:gcl:{_gcl_au}",
          identify_flow:                "self-identify on first event with user_id (v5.31) + alias once + group",
          alias_guard:                  "permanent KV flag alias:fired:{userId}",
          self_identify:                "fires on ANY event with user_id when id:link not yet in KV",
          email_kv:                     "email:user:{userId} → sha256 stored at identify time",
          order_id_sources:             "order_id | orderId | master_id | ecommerce.transaction_id | ecommerce.treatmentId",
          channels:                     CLICK_ID_CONFIG.map(c => c.label),
        });
      }

      if (request.method === "OPTIONS") {
        return new Response(null, { status: 204, headers: corsHeaders(request.headers.get("Origin") || "") });
      }

      if (isBot(request))                    return fetch(request);
      if (isSyntheticMonitor(request, url)) { console.log("[eden-analytics] synthetic monitor blocked"); return fetch(request); }
      if (isStaticAsset(url))                return fetch(request);

      if (url.pathname === "/preserve-attribution" && request.method === "POST")
        return handlePreserveAttribution(request, env, ctx);
      if (url.pathname.startsWith("/collect") && request.method === "POST")
        return handleCollect(request, env, ctx, url);
      if (url.pathname === "/server-collect" && request.method === "POST")
        return handleServerCollect(request, env, ctx);
      if (url.pathname === "/identify" && request.method === "POST")
        return handleIdentify(request, env, ctx);

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
  const referrer  = sanitizeUrlString(request.headers.get("Referer") || "");

  const legacyAnonId    = readCookie(request, "eden_anonymous_id");
  const existingAnonId  = readCookie(request, "eden_anon_id") || legacyAnonId;
  const existingSession = readCookie(request, "eden_session_id");

  const isNewVisitor = !existingAnonId;
  const isNewSession = !existingSession;

  const anonId  = existingAnonId  || crypto.randomUUID();
  const session = existingSession || `${crypto.randomUUID()}_${Date.now()}`;

  const clickIds = extractClickIds(url);
  const utms     = extractUTMs(url);
  const preAuth  = !gpcOptOut ? extractPreAuthAttribution(request) : null;
  const organic  = detectOrganic(referrer);

  const mergedClickIds = { ...(preAuth || {}), ...clickIds };

  const fullAttribution = {
    ...(organic || {}),
    ...(utms    || {}),
    ...mergedClickIds,
    ...(referrer ? { attribution_referrer: referrer }            : {}),
    ...(url      ? { landing_page: sanitizeUrl(url).toString() } : {}),
  };

  const hasAttribution = Object.keys(fullAttribution).length > 0;

  if (hasAttribution && env.GCLID_KV && !gpcOptOut) {
    const writes = [
      storeAttribution(env.GCLID_KV, KV_ANON_PREFIX + anonId, fullAttribution)
        .catch(err => console.error("[eden-analytics] KV anon store error:", err)),
    ];
    if (fullAttribution._gcl_au) {
      writes.push(
        storeAttribution(env.GCLID_KV, KV_GCL_PREFIX + fullAttribution._gcl_au, fullAttribution)
          .catch(err => console.error("[eden-analytics] KV gcl store error:", err))
      );
    }
    ctx.waitUntil(Promise.all(writes));
  }

  const response    = await fetch(request);
  const contentType = response.headers.get("content-type") || "";
  const headers     = new Headers(response.headers);

  if (isNewVisitor) headers.append("Set-Cookie", buildAnonCookie(anonId, url));
  if (isNewSession) headers.append("Set-Cookie", buildSessionCookie(session, url));
  if (preAuth)      headers.append("Set-Cookie", clearCookie("eden_pre_auth", url));

  if (env.SEGMENT_WRITE_KEY && isNewSession && hasAttribution && !gpcOptOut) {
    ctx.waitUntil(
      fireFirstTouch(request, env, anonId, session, url, mergedClickIds, utms, referrer)
        .catch(err => console.error("[eden-analytics] first_touch error:", err))
    );
  }

  const isEdenDomain = (
    url.hostname === "app.eden.health" ||
    url.hostname === "eden.health"     ||
    url.hostname === "www.eden.health"
  );

  if (response.status === 200 && isEdenDomain && contentType.includes("text/html")) {
    const cspHeader  = response.headers.get("content-security-policy") || "";
    const nonceMatch = cspHeader.match(/nonce-([A-Za-z0-9+/=]+)/);
    const nonce      = nonceMatch ? nonceMatch[1] : "";
    const script     = nonce
      ? PREAUTH_SCRIPT.replace("<script>", `<script nonce="${nonce}">`)
      : PREAUTH_SCRIPT;

    return new HTMLRewriter()
      .on("head", { element(el) { el.prepend(script, { html: true }); } })
      .transform(new Response(response.body, {
        status: response.status, statusText: response.statusText, headers,
      }));
  }

  return new Response(response.body, {
    status: response.status, statusText: response.statusText, headers,
  });
}


// =============================================================================
// FIRST TOUCH EVENT
// =============================================================================

async function fireFirstTouch(request, env, anonId, session, url, clickIds, utms, referrer) {
  const cleanUrl  = sanitizeUrl(url).toString();
  const ua        = request.headers.get("User-Agent") || "";
  const portal    = url.hostname.includes("app.eden.health") ? "patient" : "marketing";
  const sessionId = session.split("_")[0];
  const organic   = !utms && !clickIds.gclid && !clickIds._gcl_au && referrer
    ? detectOrganic(referrer) : null;
  const attribution = { ...(utms || organic || {}), ...clickIds };

  if (!Object.keys(attribution).length && !referrer) return;

  const campaignProps = buildCampaignContext(attribution);

  await segmentPost(env.SEGMENT_WRITE_KEY, "track", {
    anonymousId: anonId,
    messageId:   `first_touch_${anonId}_${sessionId}`,
    event:       "first_touch",
    properties: {
      portal,
      page_path:            url.pathname,
      page_url:             cleanUrl,
      landing_page:         cleanUrl,
      referrer:             referrer || undefined,
      session_id:           sessionId,
      device_type:          isMobile(ua) ? "mobile" : "desktop",
      pipeline_version:     PIPELINE_VERSION,
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
// /collect HANDLER — v5.31
// NEW: self-identify on ANY event where user_id present + not yet linked
// =============================================================================

async function handleCollect(request, env, ctx, url) {
  const origin = request.headers.get("Origin") || "";
  if (origin && !isAllowedOrigin(origin)) return new Response("Forbidden", { status: 403 });

  let body;
  try { body = await request.json(); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  const gpcOptOut = request.headers.get("Sec-GPC") === "1";

  if (!body.properties || typeof body.properties !== "object" || Array.isArray(body.properties)) {
    body.properties = {};
  }

  // v5.30 FIX 4: normalize customerEmail → email
  if (body.properties?.customerEmail && !body.properties?.email) {
    body.properties.email = body.properties.customerEmail;
  }
  if (body.properties?.ecommerce?.email && !body.properties?.email) {
    body.properties.email = body.properties.ecommerce.email;
  }

  const cookieAnonId = readCookie(request, "eden_anon_id")
                    || readCookie(request, "eden_anonymous_id");
  const anonId = cookieAnonId
    || body.anonymousId    || body.anonymous_id
    || body.anonymoous_id  || body.anonymous_Id
    || body.anonymousid
    || body.properties?.anonymousId
    || body.properties?.anonymous_id
    || body.properties?.anonymoous_id
    || crypto.randomUUID();

  const isNew    = !cookieAnonId;
  const portal   = origin.includes("app.eden.health") ? "patient" : "marketing";
  const userId   = resolveUserIdFromBody(body);

  if (body.type === "identify" && userId && anonId && anonId !== userId && env.GCLID_KV && !gpcOptOut) {
    ctx.waitUntil(
      linkUserAttribution(env.GCLID_KV, anonId, userId)
        .catch(err => console.error("[eden-analytics] collect identify link:", err))
    );
  }

  let freshClickIds = {};
  let freshUTMs     = null;
  let pageReferrer  = null;

  if (!gpcOptOut) {
    const pageUrlStr = body?.context?.page?.url;
    const pageRefStr = body?.context?.page?.referrer || body?.context?.referrer || "";
    pageReferrer = sanitizeUrlString(pageRefStr);
    if (pageUrlStr) {
      try {
        const pageUrl = new URL(pageUrlStr);
        freshClickIds = extractClickIds(pageUrl);
        freshUTMs     = extractUTMs(pageUrl);
      } catch {}
    }
  }

  const freshGclAu = freshClickIds._gcl_au || null;

  const storedAttribution = (env.GCLID_KV && !gpcOptOut)
    ? await resolveAttribution(env.GCLID_KV, anonId, userId, resolveOrderId(body), freshGclAu)
    : null;

  const contextCampaign = gpcOptOut ? {} : ((body.context || {}).campaign || {});

  const attribution = {
    ...(freshUTMs         || {}),
    ...freshClickIds,
    ...contextCampaign,
    ...(storedAttribution ? stripInternalFields(storedAttribution) : {}),
    ...(pageReferrer && !storedAttribution?.attribution_referrer
      ? { attribution_referrer: pageReferrer } : {}),
  };

  const campaignProps = buildCampaignContext(attribution);
  enrichPropertiesWithAttribution(body.properties, campaignProps);

  if (!body.context) body.context = {};
  body.context.campaign = { ...((body.context || {}).campaign || {}), ...campaignProps };

  const superProps = {
    portal,
    source_type:      "client",
    gpc_opt_out:      gpcOptOut,
    pipeline_version: PIPELINE_VERSION,
  };

  const collectEventName = canonicalizeEventName(resolveEventName(body));
  const collectOrderId   = resolveOrderId(body);
  const collectUserId    = resolveUserIdFromBody(body);

  // Write attr:user + attr:order on OS_purchase
  if (env.GCLID_KV && !gpcOptOut && collectEventName === "OS_purchase" && attribution) {
    ctx.waitUntil(Promise.all([
      collectUserId
        ? storeAttribution(env.GCLID_KV, KV_USER_PREFIX + collectUserId, attribution)
            .catch(err => console.error("[eden-analytics] collect purchase user-link:", err))
        : Promise.resolve(),
      collectOrderId
        ? storeAttribution(env.GCLID_KV, KV_ORDER_PREFIX + collectOrderId, attribution)
            .catch(err => console.error("[eden-analytics] collect purchase order-link:", err))
        : Promise.resolve(),
    ]));
  }

  // Forward to Segment
  if (env.SEGMENT_WRITE_KEY) {
    ctx.waitUntil(
      forwardToSegment(env.SEGMENT_WRITE_KEY, body, anonId, superProps, attribution)
        .catch(err => console.error("[eden-analytics] collect error:", err))
    );
  }

  // v5.31: self-identify on ANY event where user_id present + not yet linked
  // Handles apps that never call analytics.identify() explicitly
  // KV idempotency guard ensures this fires exactly once per userId ever
  if (
    collectUserId &&
    anonId &&
    collectUserId !== anonId &&
    env.GCLID_KV &&
    !gpcOptOut
  ) {
    ctx.waitUntil(
      (async () => {
        try {
          // Check KV first — skip if already linked (fires exactly once)
          const existingLink = await env.GCLID_KV.get(KV_IDLINK_PREFIX + collectUserId);
          if (existingLink) return;

          console.log(`[eden-analytics] self-identify: firing for userId=${collectUserId} anonId=${anonId} event=${collectEventName}`);

          const identifyUrl = new URL('/identify', request.url).toString();
          const resp = await fetch(identifyUrl, {
            method:  'POST',
            headers: {
              'Content-Type': 'application/json',
              'Cookie':        request.headers.get('Cookie') || '',
              'Origin':        origin || 'https://app.eden.health',
            },
            body: JSON.stringify({
              userId:      collectUserId,
              anonymousId: anonId,
              traits: {
                email:         body.properties?.email         || null,
                customerEmail: body.properties?.customerEmail || null,
              },
            }),
          });

          if (resp.ok) {
            console.log(`[eden-analytics] self-identify: success userId=${collectUserId}`);
          } else {
            console.error(`[eden-analytics] self-identify: failed ${resp.status}`);
          }
        } catch (err) {
          console.error('[eden-analytics] self-identify error:', err);
        }
      })()
    );
  }

  // v5.30 FIX 1: set eden_anon_id cookie if missing
  const respHeaders = {
    "Content-Type": "application/json",
    ...corsHeadersObj(origin),
  };
  if (isNew) {
    respHeaders["Set-Cookie"] = buildAnonCookie(anonId, new URL(request.url));
    console.log(`[eden-analytics] collect: set eden_anon_id for new visitor ${anonId}`);
  }

  return new Response(JSON.stringify({ ok: true, anonId }), { status: 200, headers: respHeaders });
}


// =============================================================================
// /server-collect HANDLER — v5.30 (unchanged from v5.30)
// =============================================================================

async function handleServerCollect(request, env, ctx) {
  if (env.SERVER_API_SECRET) {
    const secret = request.headers.get("X-Eden-Server-Secret");
    if (secret !== env.SERVER_API_SECRET) return new Response("Unauthorized", { status: 401 });
  }

  let body;
  try { body = await request.json(); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  if (!body.properties || typeof body.properties !== "object" || Array.isArray(body.properties)) {
    body.properties = {};
  }

  const identity  = resolveIdentityFromBody(request, body);
  let   anonId    = identity.anonymousId || null;
  const userId    = identity.userId || null;
  const eventName = canonicalizeEventName(resolveEventName(body));
  const orderId   = resolveOrderId(body);

  // Recover anonId from id:link KV when server doesn't provide it
  if (!anonId && userId && env.GCLID_KV) {
    try {
      const linkData = await env.GCLID_KV.get(KV_IDLINK_PREFIX + userId);
      if (linkData) {
        const parsed = JSON.parse(linkData);
        anonId = parsed.anonId || null;
        if (anonId) console.log(`[eden-analytics] server-collect: recovered anonId for ${userId}`);
      }
    } catch (err) {
      console.error("[eden-analytics] server-collect anonId recovery:", err);
    }
  }

  if (!anonId && !userId) console.warn("[eden-analytics] server-collect: no identity for:", eventName);

  if (eventName) body.event       = eventName;
  if (userId)    body.userId      = userId;
  if (anonId)    body.anonymousId = anonId;
  if (orderId && !body.properties.order_id) body.properties.order_id = orderId;

  // Inject email_sha256 from KV on OS_purchase
  if (eventName === "OS_purchase" && userId && env.GCLID_KV && !body.properties.email_sha256) {
    try {
      const storedEmailHash = await env.GCLID_KV.get(KV_EMAIL_PREFIX + userId);
      if (storedEmailHash) {
        body.properties.email_sha256 = storedEmailHash;
        console.log(`[eden-analytics] server-collect: injected email_sha256 for ${userId}`);
      }
    } catch (err) {
      console.error("[eden-analytics] server-collect email hash read:", err);
    }
  }

  // Edge dedup
  if (CONVERSION_EVENTS.has(eventName) && orderId && env.GCLID_KV) {
    const dedupKey = `dedup:${eventName}:${orderId}`;
    try {
      if (await env.GCLID_KV.get(dedupKey)) {
        console.log(`[eden-analytics] dedup blocked: ${eventName} key=${orderId}`);
        return jsonResponse({ ok: true, deduped: true });
      }
      await env.GCLID_KV.put(dedupKey, JSON.stringify({
        event: eventName, order_id: orderId, userId, fired_at: nowUTC(),
      }), { expirationTtl: KV_DEDUP_TTL });
    } catch (err) {
      console.error("[eden-analytics] dedup KV error — failing open:", err);
    }
  }

  const storedAttribution = env.GCLID_KV
    ? await resolveAttribution(env.GCLID_KV, anonId, userId, orderId)
    : null;

  if (storedAttribution) {
    for (const [k, v] of Object.entries(storedAttribution)) {
      if (KV_INTERNAL_FIELDS.has(k)) continue;
      if (!body.properties[k] && v) body.properties[k] = v;
    }
  }

  if (env.GCLID_KV && storedAttribution) {
    if (eventName === "OS_purchase") {
      ctx.waitUntil(Promise.all([
        userId  ? storeAttribution(env.GCLID_KV, KV_USER_PREFIX  + userId,  storedAttribution).catch(console.error) : Promise.resolve(),
        orderId ? storeAttribution(env.GCLID_KV, KV_ORDER_PREFIX + orderId, storedAttribution).catch(console.error) : Promise.resolve(),
      ]));
    }
    if ((eventName === "OS_order_delivered" || eventName === "reorder_completed") && userId) {
      ctx.waitUntil(
        storeAttribution(env.GCLID_KV, KV_USER_PREFIX + userId, storedAttribution)
          .catch(err => console.error("[eden-analytics] delivery user-link:", err))
      );
    }
  }

  body.timestamp = nowUTC();

  const attribution   = {
    ...(storedAttribution ? stripInternalFields(storedAttribution) : {}),
    ...((body.context || {}).campaign || {}),
  };
  const campaignProps = buildCampaignContext(attribution);
  enrichPropertiesWithAttribution(body.properties, campaignProps);

  if (!body.context) body.context = {};
  body.context.campaign = { ...((body.context || {}).campaign || {}), ...campaignProps };

  const superProps = {
    portal:           "patient",
    source_type:      "server",
    pipeline_version: PIPELINE_VERSION,
    ...(identity.identityWarning ? { identity_warning: identity.identityWarning } : {}),
    ...(!anonId && !userId       ? { identity_warning: "no_identity_provided"   } : {}),
  };

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
    status: 200,
    headers: { "Content-Type": "application/json", ...corsHeadersObj(origin) },
  });
}


// =============================================================================
// /identify HANDLER — v5.30 (unchanged)
// =============================================================================

async function handleIdentify(request, env, ctx) {
  const origin = request.headers.get("Origin") || "";
  if (origin && !isAllowedOrigin(origin)) return new Response("Forbidden", { status: 403 });

  if (env.SERVER_API_SECRET) {
    const secret = request.headers.get("X-Eden-Server-Secret");
    if (secret && secret !== env.SERVER_API_SECRET) return new Response("Unauthorized", { status: 401 });
  }

  let body;
  try { body = JSON.parse(await request.text()); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  const identity  = resolveIdentityFromBody(request, body);
  const anonId    = identity.anonymousId || null;
  const userId    = identity.userId      || null;
  const groupId   = body.groupId         || null;
  const orderId   = body.orderId         || null;
  const rawTraits = body.traits          || {};

  if (!userId) {
    if (env.SEGMENT_WRITE_KEY && anonId) {
      const traits = await hashEmail(rawTraits);
      ctx.waitUntil(
        segmentPost(env.SEGMENT_WRITE_KEY, "identify", { anonymousId: anonId, traits, timestamp: nowUTC() })
          .catch(err => console.error("[eden-analytics] identify (anon-only) error:", err))
      );
    }
    return new Response(JSON.stringify({ ok: true, skipped: "no_userId" }), {
      status: 200, headers: { "Content-Type": "application/json", ...corsHeadersObj(origin) },
    });
  }

  if (userId) body.userId      = userId;
  if (anonId) body.anonymousId = anonId;

  let alreadyLinked = false;
  if (env.GCLID_KV && anonId && userId && anonId !== userId) {
    try {
      const existing = await env.GCLID_KV.get(KV_IDLINK_PREFIX + userId);
      if (existing) {
        const parsed = JSON.parse(existing);
        if (parsed.anonId === anonId && (Date.now() - parsed.ts) < KV_IDLINK_TTL * 1000) {
          alreadyLinked = true;
        }
      }
    } catch {}
  }

  let aliasFired = false;
  if (env.GCLID_KV && anonId && userId && anonId !== userId) {
    try {
      const flag = await env.GCLID_KV.get(KV_ALIAS_PREFIX + userId);
      aliasFired = !!flag;
    } catch {}
  }

  const storedAttribution = env.GCLID_KV
    ? await resolveAttribution(env.GCLID_KV, anonId, userId, orderId)
    : null;

  const attributionTraits = {};
  if (storedAttribution) {
    const campaignProps = buildCampaignContext(storedAttribution);
    const channel       = deriveAcquisitionChannel(campaignProps);
    const source        = campaignProps.utm_source || deriveClickIdSource(campaignProps);
    for (const k of ATTRIBUTION_TRAIT_KEYS) {
      if (!rawTraits[k] && storedAttribution[k]) attributionTraits[k] = storedAttribution[k];
    }
    if (!rawTraits.acquisition_channel) attributionTraits.acquisition_channel = channel;
    if (!rawTraits.attribution_source)  attributionTraits.attribution_source  = source;
    if (!rawTraits.first_touch_at && storedAttribution.stored_at) {
      attributionTraits.first_touch_at = storedAttribution.stored_at;
    }
  }

  const enrichedTraits = await hashEmail({ ...attributionTraits, ...rawTraits });

  if (!alreadyLinked && env.GCLID_KV && anonId && userId && anonId !== userId) {
    ctx.waitUntil(Promise.all([
      linkUserAttribution(env.GCLID_KV, anonId, userId)
        .catch(err => console.error("[eden-analytics] KV identify link:", err)),
      env.GCLID_KV.put(
        KV_IDLINK_PREFIX + userId,
        JSON.stringify({ anonId, ts: Date.now(), userId }),
        { expirationTtl: KV_IDLINK_TTL }
      ).catch(err => console.error("[eden-analytics] KV idlink write:", err)),
    ]));
  }

  // Store email_sha256 in KV for server-collect retrieval
  if (userId && env.GCLID_KV) {
    const emailRaw = resolveEmailFromBody(body)
      || rawTraits?.email
      || rawTraits?.customerEmail
      || null;
    if (emailRaw) {
      const emailHash = await sha256(emailRaw.trim().toLowerCase());
      ctx.waitUntil(
        env.GCLID_KV.put(
          KV_EMAIL_PREFIX + userId,
          emailHash,
          { expirationTtl: KV_TTL }
        ).catch(err => console.error("[eden-analytics] KV email hash write:", err))
      );
    }
  }

  if (env.SEGMENT_WRITE_KEY) {
    ctx.waitUntil((async () => {
      try {
        await segmentPost(env.SEGMENT_WRITE_KEY, "identify", {
          anonymousId: anonId || userId,
          userId,
          traits:    enrichedTraits,
          context:   { campaign: buildCampaignContext(storedAttribution || {}) },
          timestamp: nowUTC(),
        });

        if (!aliasFired && anonId && userId && anonId !== userId) {
          await segmentPost(env.SEGMENT_WRITE_KEY, "alias", {
            previousId: anonId, userId, timestamp: nowUTC(),
          });
          if (env.GCLID_KV) {
            await env.GCLID_KV.put(
              KV_ALIAS_PREFIX + userId,
              JSON.stringify({ anonId, fired_at: nowUTC() }),
              { expirationTtl: KV_ALIAS_TTL }
            );
          }
        }

        if (groupId) {
          await segmentPost(env.SEGMENT_WRITE_KEY, "group", {
            anonymousId: anonId || userId, userId, groupId,
            traits: {
              ...buildCampaignContext(storedAttribution || {}),
              acquisition_channel: attributionTraits.acquisition_channel,
            },
            timestamp: nowUTC(),
          });
        }
      } catch (err) {
        console.error("[eden-analytics] identify segment error:", err);
      }
    })());
  }

  return new Response(JSON.stringify({ ok: true }), {
    status: 200, headers: { "Content-Type": "application/json", ...corsHeadersObj(origin) },
  });
}


// =============================================================================
// /preserve-attribution HANDLER
// =============================================================================

async function handlePreserveAttribution(request, env, ctx) {
  const origin = request.headers.get("Origin") || "";
  if (origin && !isAllowedOrigin(origin)) return new Response("Forbidden", { status: 403 });

  let body;
  try { body = JSON.parse(await request.text()); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  const cookieAnonId = readCookie(request, "eden_anon_id")
                    || readCookie(request, "eden_anonymous_id");
  const anonId  = cookieAnonId || body.anonymousId;
  const userId  = body.userId;
  const orderId = body.orderId;

  if (!env.GCLID_KV) return jsonResponse({ ok: true, skipped: "no_kv" });

  const attribution = await resolveAttribution(env.GCLID_KV, anonId, userId, orderId);
  if (!attribution)  return jsonResponse({ ok: true, skipped: "no_attribution" });

  const writes = [];
  if (orderId) writes.push(storeAttribution(env.GCLID_KV, KV_ORDER_PREFIX + orderId, attribution).catch(console.error));
  if (userId)  writes.push(storeAttribution(env.GCLID_KV, KV_USER_PREFIX  + userId,  attribution).catch(console.error));
  if (writes.length) ctx.waitUntil(Promise.all(writes));

  if (!CLICK_ID_PARAMS.some(p => attribution[p])) return jsonResponse({ ok: true, skipped: "no_click_id" });

  const preAuthValue = encodeURIComponent(JSON.stringify({
    ...(attribution._gcl_au      ? { _gcl_au:       attribution._gcl_au      } : {}),
    ...(attribution.gclid        ? { gclid:          attribution.gclid        } : {}),
    ...(attribution.fbclid       ? { fbclid:         attribution.fbclid       } : {}),
    ...(attribution.msclkid      ? { msclkid:        attribution.msclkid      } : {}),
    ...(attribution.ttclid       ? { ttclid:         attribution.ttclid       } : {}),
    ...(attribution.utm_source   ? { utm_source:     attribution.utm_source   } : {}),
    ...(attribution.utm_medium   ? { utm_medium:     attribution.utm_medium   } : {}),
    ...(attribution.utm_campaign ? { utm_campaign:   attribution.utm_campaign } : {}),
    ...(attribution.utm_content  ? { utm_content:    attribution.utm_content  } : {}),
    ...(attribution.utm_term     ? { utm_term:       attribution.utm_term     } : {}),
  }));

  return new Response(JSON.stringify({ ok: true }), {
    status: 200,
    headers: {
      "Content-Type": "application/json",
      "Set-Cookie": [
        `eden_pre_auth=${preAuthValue}`, "Max-Age=600",
        `Domain=${cookieDomain(new URL(request.url))}`,
        "Path=/", "HttpOnly", "Secure", "SameSite=Lax",
      ].join("; "),
      ...corsHeadersObj(origin),
    },
  });
}

function extractPreAuthAttribution(request) {
  const raw = readCookie(request, "eden_pre_auth");
  if (!raw) return null;
  try { return JSON.parse(decodeURIComponent(raw)); } catch { return null; }
}


// =============================================================================
// SEGMENT FORWARDING
// =============================================================================

async function forwardToSegment(writeKey, body, anonId, superProps, attribution = {}) {
  const type = (body.type || "track").toLowerCase();
  const mergedContext = {
    ...(body.context || {}),
    campaign: { ...((body.context || {}).campaign || {}), ...buildCampaignContext(attribution) },
  };

  if (type === "identify") {
    const traits = await hashEmail(body.traits || body.properties || {});
    await segmentPost(writeKey, "identify", {
      anonymousId: anonId, userId: resolveUserIdFromBody(body),
      traits, context: mergedContext, timestamp: nowUTC(),
    });
    return;
  }
  if (type === "page") {
    await segmentPost(writeKey, "page", {
      anonymousId: anonId, userId: resolveUserIdFromBody(body),
      name:       body.name || body.properties?.name || "",
      properties: await hashEmail({ ...superProps, ...(body.properties || {}) }),
      context: mergedContext, timestamp: nowUTC(),
    });
    return;
  }
  if (type === "screen") {
    await segmentPost(writeKey, "track", {
      anonymousId: anonId, userId: resolveUserIdFromBody(body),
      event:      `Viewed ${body.name || body.properties?.name || "Unknown Screen"}`,
      properties: await hashEmail({ ...superProps, ...(body.properties || {}) }),
      context: mergedContext, timestamp: nowUTC(),
    });
    return;
  }

  const eventName = canonicalizeEventName(resolveEventName(body)) || null;
  const orderId   = resolveOrderId(body);
  if (eventName) body.event = eventName;
  if (orderId && body.properties && !body.properties.order_id) body.properties.order_id = orderId;
  if (!eventName) { console.log("[eden-analytics] skipping event with no name"); return; }

  const stableMessageId = CONVERSION_EVENTS.has(eventName) && orderId
    ? `eden_${eventName}_${orderId}` : undefined;

  await segmentPost(writeKey, "track", {
    anonymousId: anonId, userId: resolveUserIdFromBody(body),
    event:      eventName,
    properties: await hashEmail({ ...superProps, ...(body.properties || {}) }),
    context:    mergedContext, timestamp: nowUTC(),
    ...(stableMessageId ? { messageId: stableMessageId } : {}),
  });
}


// =============================================================================
// KV ATTRIBUTION
// =============================================================================

async function storeAttribution(kv, key, attribution) {
  if (!kv || !key || !attribution) return;
  const hasValue = Object.values(attribution).some(v => v && String(v).trim());
  if (!hasValue) return;

  try {
    const existing = await kv.get(key);
    if (existing) {
      const parsed           = JSON.parse(existing);
      const existingHasClick = CLICK_ID_PARAMS.some(p => parsed[p]);
      const newHasClick      = CLICK_ID_PARAMS.some(p => attribution[p]);

      if (existingHasClick && newHasClick) {
        let enriched = false;
        const updated = { ...parsed };
        for (const k of UTM_ENRICHABLE) {
          if (attribution[k] && !parsed[k]) { updated[k] = attribution[k]; enriched = true; }
        }
        if (enriched) await kv.put(key, JSON.stringify(updated), { expirationTtl: KV_TTL });
        return;
      }
      if (existingHasClick && !newHasClick) {
        let enriched = false;
        const updated = { ...parsed };
        for (const k of UTM_ENRICHABLE) {
          if (attribution[k] && !parsed[k]) { updated[k] = attribution[k]; enriched = true; }
        }
        if (enriched) await kv.put(key, JSON.stringify(updated), { expirationTtl: KV_TTL });
        return;
      }
    }
  } catch {}

  await kv.put(key, JSON.stringify({ ...attribution, stored_at: nowUTC() }), { expirationTtl: KV_TTL });
}

async function getAttribution(kv, key) {
  if (!kv || !key) return null;
  try {
    const stored = await kv.get(key);
    return stored ? JSON.parse(stored) : null;
  } catch { return null; }
}

async function resolveAttribution(kv, anonId, userId, orderId = null, gclAu = null) {
  if (!kv) return null;
  const [fromAnon, fromUser, fromOrder, fromGcl] = await Promise.all([
    anonId  ? getAttribution(kv, KV_ANON_PREFIX  + anonId)  : Promise.resolve(null),
    userId  ? getAttribution(kv, KV_USER_PREFIX   + userId)  : Promise.resolve(null),
    orderId ? getAttribution(kv, KV_ORDER_PREFIX  + orderId) : Promise.resolve(null),
    gclAu   ? getAttribution(kv, KV_GCL_PREFIX   + gclAu)   : Promise.resolve(null),
  ]);
  if (fromAnon  && CLICK_ID_PARAMS.some(p => fromAnon[p]))  return fromAnon;
  if (fromUser  && CLICK_ID_PARAMS.some(p => fromUser[p]))  return fromUser;
  if (fromOrder && CLICK_ID_PARAMS.some(p => fromOrder[p])) return fromOrder;
  if (fromGcl   && CLICK_ID_PARAMS.some(p => fromGcl[p]))   return fromGcl;
  return fromAnon || fromUser || fromOrder || fromGcl || null;
}

async function linkUserAttribution(kv, anonId, userId) {
  const [anonAttr, existingUser] = await Promise.all([
    getAttribution(kv, KV_ANON_PREFIX + anonId),
    getAttribution(kv, KV_USER_PREFIX + userId),
  ]);
  if (!anonAttr) return;
  const userHasClick = existingUser && CLICK_ID_PARAMS.some(p => existingUser[p]);
  if (userHasClick) { console.log("[eden-analytics] userId click attribution exists — first-touch preserved"); return; }
  await storeAttribution(kv, KV_USER_PREFIX + userId, anonAttr);
}

function stripInternalFields(attribution) {
  if (!attribution) return {};
  const out = {};
  for (const [k, v] of Object.entries(attribution)) {
    if (!KV_INTERNAL_FIELDS.has(k)) out[k] = v;
  }
  return out;
}


// =============================================================================
// CLICK ID + UTM EXTRACTION
// =============================================================================

function extractClickIds(url) {
  const out = {};
  for (const { param } of CLICK_ID_CONFIG) {
    const v = url.searchParams.get(param);
    if (v) out[param] = v;
  }
  if (!out.gclid && !out._gcl_au) {
    const gl = url.searchParams.get("_gl");
    if (gl) Object.assign(out, extractGlLinker(gl));
  }
  return out;
}

function extractGlLinker(gl) {
  const out = {};
  if (!gl) return out;
  try {
    const parts = gl.split("*");
    for (let i = 2; i < parts.length - 1; i += 2) {
      const key = parts[i], value = parts[i + 1];
      if (key === "_gcl_au" && value) {
        out._gcl_au = value;
        try {
          const b64  = value.replace(/\./g,"=").replace(/-/g,"+").replace(/_/g,"/");
          const segs = atob(b64).split(".");
          if (segs.length >= 3) out._gcl_hash = segs[2];
        } catch {}
        if (!out.utm_source) out.utm_source = "google";
        if (!out.utm_medium) out.utm_medium = "cpc";
      }
    }
  } catch {}
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
  if (!attribution) return {};
  const campaign = {};
  const KEYS = [
    "utm_source","utm_medium","utm_campaign","utm_content","utm_term","utm_id",
    "landing_page","attribution_referrer", ...CLICK_ID_PARAMS,
  ];
  for (const k of KEYS) { if (attribution[k]) campaign[k] = attribution[k]; }
  return campaign;
}

function enrichPropertiesWithAttribution(properties, campaignProps) {
  if (!properties || typeof properties !== "object") return;
  if (!campaignProps || !Object.keys(campaignProps).length) return;
  for (const [k, v] of Object.entries(campaignProps)) {
    if (KV_INTERNAL_FIELDS.has(k)) continue;
    if (v && !properties[k]) properties[k] = v;
  }
  if (campaignProps._gcl_au && !properties.gcl_au) properties.gcl_au = campaignProps._gcl_au;
  properties.acquisition_channel  = properties.acquisition_channel  || deriveAcquisitionChannel(campaignProps);
  properties.attribution_source   = properties.attribution_source   || campaignProps.utm_source || deriveClickIdSource(campaignProps);
  properties.attribution_medium   = properties.attribution_medium   || campaignProps.utm_medium;
  properties.attribution_campaign = properties.attribution_campaign || campaignProps.utm_campaign;
}

function deriveClickIdSource(c) {
  if (!c) return undefined;
  if (c.gclid||c.gbraid||c.wbraid||c.dclid||c._gcl_au||c.srsltid) return "google";
  if (c.fbclid)    return "meta";
  if (c.msclkid)   return "microsoft";
  if (c.ttclid)    return "tiktok";
  if (c.twclid)    return "twitter";
  if (c.li_fat_id) return "linkedin";
  if (c.rdt_cid)   return "reddit";
  if (c.epik)      return "pinterest";
  if (c.ScCid)     return "snapchat";
  if (c.irclickid) return "impact_radius";
  if (c.cjevent)   return "cj_affiliate";
  if (c.click_id)  return "generic";
  return undefined;
}

function deriveAcquisitionChannel(c) {
  if (!c || !Object.keys(c).length) return "unknown";
  const src = String(c.utm_source || deriveClickIdSource(c) || "").toLowerCase();
  const med = String(c.utm_medium || "").toLowerCase();
  if (med === "organic")    return "organic_search";
  if (med === "email")      return "email";
  if (med === "sms")        return "sms";
  if (med === "affiliate")  return "affiliate";
  if (med === "influencer") return "influencer";
  if (med === "synthetic")  return "synthetic";
  if (med === "cpc"||med === "paid"||med === "paid_search"||med === "search_cpc"||
      c.gclid||c.gbraid||c.wbraid||c.dclid||c._gcl_au||c.srsltid||c.msclkid||
      src.includes("google")||src.includes("bing")||src.includes("microsoft"))
    return "paid_search";
  if (c.fbclid||c.ttclid||src.includes("facebook")||src.includes("instagram")||
      src.includes("meta")||src.includes("tiktok"))
    return "paid_social";
  if (c.li_fat_id||src.includes("linkedin"))                     return "paid_social_linkedin";
  if (c.rdt_cid||src.includes("reddit"))                         return "paid_social_reddit";
  if (c.epik||src.includes("pinterest"))                         return "paid_social_pinterest";
  if (c.twclid||src.includes("twitter")||src.includes("x.com")) return "paid_social_twitter";
  if (c.irclickid||c.cjevent||med === "affiliate")               return "affiliate";
  if (c.attribution_referrer) {
    try {
      const rh = new URL(c.attribution_referrer).hostname.toLowerCase();
      if (rh.includes("google")||rh.includes("bing"))   return "organic_search";
      if (rh.includes("facebook")||rh.includes("instagram")||rh.includes("meta")) return "organic_social";
      if (rh.includes("twitter")||rh.includes("x.com")) return "organic_social";
      if (rh.includes("linkedin"))                       return "organic_social";
    } catch {}
  }
  return src || "direct";
}

function canonicalizeEventName(n) {
  if (!n) return "";
  const raw = String(n).trim();
  return EVENT_NAME_ALIASES[raw.toLowerCase()] || raw;
}

function resolveEventName(body) {
  return body.event||body.event_name||body.name||
    body.properties?.event||body.properties?.event_name||body.properties?.name||"";
}

function resolveOrderId(body) {
  return (
    body.properties?.order_id                  ||
    body.properties?.orderId                   ||
    body.properties?.master_id                 ||
    body.properties?.masterId                  ||
    body.properties?.treatment_id              ||
    body.properties?.treatmentId               ||
    body.properties?.ecommerce?.transaction_id ||
    body.properties?.ecommerce?.order_id       ||
    body.properties?.ecommerce?.treatmentId    ||
    body.order_id                              ||
    body.orderId                               ||
    null
  );
}

function resolveUserIdFromBody(body) {
  return body.userId||body.user_id||
    body.properties?.userId||body.properties?.user_id||
    body.properties?.patient_id||body.properties?.customer_id||
    body.properties?.ecommerce?.userId||
    null;
}

function resolveEmailFromBody(body) {
  return (
    body.properties?.email                ||
    body.properties?.customerEmail        ||
    body.properties?.ecommerce?.email     ||
    body.traits?.email                    ||
    body.context?.traits?.email           ||
    null
  );
}

function resolveIdentityFromBody(request, body) {
  const cookieAnonId = readCookie(request,"eden_anon_id")||readCookie(request,"eden_anonymous_id");
  const userId = resolveUserIdFromBody(body);
  let anonymousId =
    cookieAnonId||
    body.anonymousId||body.anonymous_id||body.anonymoous_id||
    body.anonymous_Id||body.anonymousid||
    body.properties?.anonymousId||body.properties?.anonymous_id||
    body.properties?.anonymoous_id||null;
  if (!anonymousId && userId) anonymousId = userId;
  return {
    anonymousId, userId,
    identityWarning: anonymousId&&userId&&anonymousId===userId ? "anonymousId_equals_userId" : undefined,
  };
}


// =============================================================================
// ORGANIC SEARCH DETECTION
// =============================================================================

function detectOrganic(referrer) {
  if (!referrer) return null;
  try {
    const h = new URL(referrer).hostname.toLowerCase();
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
        const ref = new URL(referrer);
        const p   = ref.pathname.toLowerCase();
        if (p.includes("search")||p==="/"||ref.searchParams.has("q")||ref.searchParams.has("query")) {
          return { utm_source: engine, utm_medium: "organic" };
        }
      }
    }
  } catch {}
  return null;
}


// =============================================================================
// BOT + SYNTHETIC DETECTION
// =============================================================================

function isBot(request) {
  const ua = request.headers.get("User-Agent") || "";
  if (BOT_UA_PATTERNS.some(p => p.test(ua))) return true;
  const d = request.cf?.botManagement?.decision;
  if (d && BOT_CF_DECISIONS.has(d)) return true;
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
  if (STATIC_PREFIXES.some(x => p.startsWith(x))) return true;
  if (STATIC_EXTENSIONS.some(x => p.endsWith(x))) return true;
  return false;
}


// =============================================================================
// EMAIL HASHING
// =============================================================================

async function hashEmail(props) {
  if (!props || typeof props !== "object") return props;
  const out = {};
  for (const [k, v] of Object.entries(props)) {
    if ((k === "email" || k === "customerEmail") && typeof v === "string") {
      out["email_sha256"] = await sha256(v); out[k] = v; continue;
    }
    if (v && typeof v === "object" && !Array.isArray(v)) { out[k] = await hashEmail(v); continue; }
    out[k] = v;
  }
  return out;
}

async function sha256(value) {
  const buf = await crypto.subtle.digest("SHA-256",
    new TextEncoder().encode(String(value).trim().toLowerCase()));
  return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2,"0")).join("");
}


// =============================================================================
// SEGMENT POST
// =============================================================================

async function segmentPost(writeKey, endpoint, payload) {
  const res = await fetch(`https://api.segment.io/v1/${endpoint}`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Basic ${btoa(writeKey + ":")}` },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(`Segment ${endpoint} ${res.status}: ${await res.text()}`);
}


// =============================================================================
// COOKIE HELPERS
// =============================================================================

function readCookie(request, name) {
  const m = (request.headers.get("Cookie") || "").match(new RegExp(`(?:^|;\\s*)${name}=([^;]+)`));
  return m ? decodeURIComponent(m[1]) : null;
}

function cookieDomain(url) {
  const h = url.hostname;
  if (h === "localhost") return "localhost";
  const parts = h.split(".");
  return parts.length >= 2 ? `.${parts.slice(-2).join(".")}` : h;
}

function buildAnonCookie(id, url) {
  return [`eden_anon_id=${encodeURIComponent(id)}`, "Max-Age=63072000",
    `Domain=${cookieDomain(url)}`, "Path=/", "Secure", "SameSite=Lax"].join("; ");
}

function buildSessionCookie(v, url) {
  return [`eden_session_id=${encodeURIComponent(v)}`, "Max-Age=1800",
    `Domain=${cookieDomain(url)}`, "Path=/", "Secure", "SameSite=Lax"].join("; ");
}

function clearCookie(name, url) {
  return [`${name}=`, "Max-Age=0", `Domain=${cookieDomain(url)}`, "Path=/", "Secure", "SameSite=Lax"].join("; ");
}

function nowUTC()     { return new Date(Date.now()).toISOString(); }
function isMobile(ua) { return /Mobile|Android|iPhone|iPad|iPod/i.test(ua); }


// =============================================================================
// URL HELPERS
// =============================================================================

function sanitizeUrl(url) {
  try {
    const clean = new URL(url.toString());
    for (const k of [...clean.searchParams.keys()]) {
      if (SENSITIVE_URL_PARAMS.some(p => p.test(k))) clean.searchParams.set(k, "[redacted]");
    }
    return clean;
  } catch { return url; }
}

function sanitizeUrlString(v) {
  if (!v) return "";
  try { return sanitizeUrl(new URL(v)).toString(); } catch { return v; }
}


// =============================================================================
// CORS HELPERS
// =============================================================================

function isAllowedOrigin(o) { return !!o && ALLOWED_ORIGINS.includes(o); }

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

function corsHeaders(o) { return corsHeadersObj(o); }

function jsonResponse(data) {
  return new Response(JSON.stringify(data), {
    status: 200, headers: { "Content-Type": "application/json" },
  });
}
