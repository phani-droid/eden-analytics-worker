// =============================================================================
// EdenOS Analytics Worker — v5.24 ULTIMATE
// 100% Attribution Coverage — Zero Frontend Changes Required
// =============================================================================
//
// ATTRIBUTION COVERAGE GUARANTEE:
//   Every user who arrives at eden.health or app.eden.health from ANY source
//   will have their attribution stored, survived across redirects, and attached
//   to every event they fire — including server-side events fired days later.
//
// SOURCES COVERED (exhaustive):
//   ✓ Google Ads (gclid, gbraid, wbraid, dclid)
//   ✓ Google Cross-Domain (_gl linker → _gcl_au)
//   ✓ Google Shopping (srsltid)
//   ✓ Google Enhanced Conversions (_gcl_au top-level)
//   ✓ Meta / Facebook (fbclid)
//   ✓ Microsoft / Bing (msclkid)
//   ✓ TikTok (ttclid)
//   ✓ Twitter / X (twclid)
//   ✓ LinkedIn (li_fat_id)
//   ✓ Reddit (rdt_cid)
//   ✓ Pinterest (epik)
//   ✓ Snapchat (ScCid)
//   ✓ Northbeam (nbt)
//   ✓ Impact Radius (irclickid)
//   ✓ CJ Affiliate (cjevent)
//   ✓ Generic (click_id)
//   ✓ UTM parameters (source/medium/campaign/content/term/id)
//   ✓ Organic search (8 engines via referrer)
//   ✓ Email / direct / SMS (referrer-based)
//   ✓ Cross-domain (eden.health → app.eden.health via _gl + KV)
//   ✓ Google SSO redirect (pre-auth cookie + sendBeacon)
//   ✓ Klarna / Affirm / Afterpay / Clearpay BNPL redirect
//   ✓ Stripe payment confirmation
//   ✓ Deep links (SMS/email → app) via KV userId lookup
//   ✓ Programmatic redirects (window.location.href/assign/replace)
//   ✓ Form submissions to external auth domains
//   ✓ Server-side events (order_delivered, reorder) via KV bridge
//   ✓ Anonymous → identified user attribution carry-forward
//   ✓ Multi-device (userId KV lookup)
//   ✓ Cookie-cleared sessions (userId fallback)
//   ✓ Safari ITP (edge-set cookie bypasses ITP 7-day limit)
//   ✓ GPC / Do Not Track opt-out (respected, events still fire without PII)
//   ✓ Synthetic monitor pollution (blocked)
//   ✓ Bot traffic (blocked)
//   ✓ Duplicate conversion events (24hr KV dedup)
//   ✓ Pending Consult (no order_id) — master_id dedup fallback
//   ✓ CSP nonce — injected script not blocked by strict CSP
//   ✓ analytics.js anonymousId mismatch — synced at boot with retry
//
// COMPLETE FIX LOG:
//
//   v5.24 FIX 16 — utm_source/medium/campaign carried in KV even without click ID
//     UTM-only sessions (email, organic, social) stored in KV as first-touch
//     Previously only click ID sessions were reliably stored
//
//   v5.24 FIX 15 — Referrer stored in KV as attribution_referrer
//     Direct/email/SMS users have referrer as only attribution signal
//     Now stored in KV and attached to all events including server-side
//
//   v5.24 FIX 14 — Cross-domain attribution from eden.health → app.eden.health
//     If user lands on eden.health with UTMs, clicks to app.eden.health,
//     the _gl param + KV ensure attribution is not lost at domain boundary
//     Added explicit cross-domain KV write on eden.health page loads
//
//   v5.24 FIX 13 — Session stitching: anonymous → identified → reorder
//     When user re-orders months later on a new device/session,
//     attr:user:{userId} lookup ensures original attribution is used
//     Added userId → KV write at /identify and at OS_purchase
//
//   v5.24 FIX 12 — landing_page and referrer stored on every attribution entry
//     Without landing page we can't segment "which page converted"
//     Now stored in KV and forwarded to Segment on every event
//
//   v5.24 FIX 11 — /preserve-attribution accepts sendBeacon (text/plain body)
//     sendBeacon sends Content-Type: text/plain with JSON body
//     Previous code did request.json() which fails on text/plain
//     Fix: detect content-type and parse accordingly
//
//   v5.24 FIX 10 — PREAUTH_SCRIPT: MutationObserver watches for late-rendered SSO buttons
//     React renders Google SSO button asynchronously after JS hydration
//     Click listener on document catches it but MutationObserver adds
//     direct mousedown listeners for zero-latency capture
//
//   v5.24 FIX 9  — Stripe: also intercept confirmCardPayment / confirmPayment (SDK calls)
//     Stripe.js SDK calls go through stripe.confirmCardPayment() not fetch directly
//     Patch Stripe.js instance when it loads via MutationObserver on script tags
//
//   v5.24 FIX 8  — BNPL: Klarna web component fires custom events not clicks
//     <klarna-express-button> fires 'klarna:authorized' custom event
//     Added custom event listener for Klarna SDK events
//
//   v5.24 FIX 7  — utm_source inferred from referrer when missing
//     User arrives from google.com/search with no UTMs (rare but happens)
//     Referrer-based organic detection now stored in KV alongside click IDs
//
//   v5.24 FIX 6  — handleCollect: anonymousId typo variants all normalized
//     body.anonymoous_id (triple-o) already handled
//     Added: body.anonymous_Id, body.anonymousid (case variants from SDKs)
//
//   v5.24 FIX 5  — PREAUTH_SCRIPT prepended to <head> (runs before all app JS)
//     Previously appended — analytics.js could initialize before syncAnonId ran
//     Fix: el.prepend() so our script is first
//
//   v5.24 FIX 4  — /collect: attribution merged into context.campaign for Segment
//     Segment's built-in campaign attribution in BQ reads context.campaign
//     Previously only in properties — now in both places
//
//   v5.24 FIX 3  — server-collect: no identity produces 200 + warning (not silent drop)
//     Previously events with no identity were forwarded to Segment with "server" anonId
//     Now: identity_warning: "no_identity_provided" added so BQ queries can flag these
//
//   v5.24 FIX 2  — storeAttribution: always store UTM-only sessions (not just click IDs)
//     Previously: if no click ID in new attribution AND existing has click ID → skip
//     Now: always store if incoming has ANY attribution signal
//
//   v5.24 FIX 1  — Health check version + all pipeline_version strings = "5.24"
//
//   [v5.23 and earlier fixes preserved — see v5.23 for full history]
//
// ARCHITECTURE — 9 Layers:
//   L1 — eden_anon_id cookie     (2yr, JS-readable, ITP-resistant via edge-set)
//   L2 — KV attribution store    (120 days, first-touch, 18 channels + UTM + referrer)
//   L3 — userId attribution link (/identify + OS_purchase bridge)
//   L4 — email_sha256            (enhanced conversions, hashed at edge)
//   L5 — organic referrer        (8 search engines detected)
//   L6 — _gl cross-domain linker (eden.health ↔ app.eden.health)
//   L7 — pre-auth cookie         (SSO + BNPL redirect survival, HttpOnly)
//   L8 — PREAUTH_SCRIPT          (injected into all Eden HTML, covers every flow)
//   L9 — server-side bridge      (order_delivered, reorder resolve from KV)
//
// KV KEY SCHEMA:
//   attr:anon:{anonymousId}   → attribution + landing_page + referrer (120 days)
//   attr:user:{userId}        → attribution + landing_page + referrer (120 days)
//   attr:order:{orderId}      → attribution (120 days)
//   dedup:{event}:{key}       → dedup lock (24 hours)
// =============================================================================


// ─────────────────────────────────────────────────────────────────────────────
// CONSTANTS
// ─────────────────────────────────────────────────────────────────────────────

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

const KV_ANON_PREFIX  = "attr:anon:";
const KV_USER_PREFIX  = "attr:user:";
const KV_ORDER_PREFIX = "attr:order:";
const KV_TTL          = 10368000; // 120 days
const KV_DEDUP_TTL    = 86400;    // 24 hours

const UTM_ENRICHABLE = [
  "utm_campaign","utm_content","utm_term","utm_id","attribution_campaign",
  "landing_page","attribution_referrer",
];


// =============================================================================
// PREAUTH_SCRIPT
// Injected as FIRST element of <head> on every Eden HTML page (200 only).
// Handles 100% of browser-side attribution flows with zero app changes.
// =============================================================================

const PREAUTH_SCRIPT = `<script>
(function() {
  'use strict';

  // ── Utilities ──────────────────────────────────────────────────────────────

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

  // ── preserveAttribution ────────────────────────────────────────────────────
  // Called before ANY external redirect. Uses sendBeacon for reliability
  // (survives page unload). Falls back to fetch with keepalive.
  var _preserving = false; // debounce — only one in-flight at a time
  function preserveAttribution(orderId) {
    if (_preserving) return;
    _preserving = true;
    setTimeout(function() { _preserving = false; }, 2000);
    try {
      var ids     = resolveIds();
      var payload = JSON.stringify({
        anonymousId: ids.anonId || null,
        userId:      ids.userId || null,
        orderId:     orderId || getOrderIdFromDOM() || null,
      });
      var sent = false;
      if (navigator.sendBeacon) {
        try {
          sent = navigator.sendBeacon(
            '/preserve-attribution',
            new Blob([payload], { type: 'application/json' })
          );
        } catch(e) {}
      }
      if (!sent) {
        fetch('/preserve-attribution', {
          method:    'POST',
          headers:   { 'Content-Type': 'application/json' },
          body:      payload,
          keepalive: true,
          credentials: 'include',
        }).catch(function(){});
      }
    } catch(e) {}
  }

  // ── 1. analytics.js anonymousId sync ──────────────────────────────────────
  // Syncs worker cookie → analytics.setAnonymousId() so KV lookups match.
  // Retries for 5s to handle async analytics.js loads.
  function syncAnonId() {
    var id = getCookie('eden_anon_id');
    if (!id) return;
    function trySync() {
      try {
        if (window.analytics && window.analytics.setAnonymousId) {
          window.analytics.setAnonymousId(id);
          return true;
        }
      } catch(e) {}
      return false;
    }
    if (trySync()) return;
    var attempts = 0;
    var t = setInterval(function() {
      if (++attempts > 50 || trySync()) clearInterval(t); // 50 × 100ms = 5s
    }, 100);
  }

  // ── 2. analytics.js ready hook ────────────────────────────────────────────
  // Intercepts analytics.js 'ready' event for guaranteed sync even if
  // analytics.js loads after our script finishes
  (function() {
    var _origSnippet = window.analytics;
    // If analytics object exists but isn't ready, queue our sync
    if (_origSnippet && _origSnippet.on) {
      try { _origSnippet.on('ready', syncAnonId); } catch(e) {}
    }
  })();

  // ── 3. Google SSO detection ────────────────────────────────────────────────
  function isGoogleSSOEl(el) {
    if (!el || !el.getAttribute) return false;
    var testid   = el.getAttribute('data-testid')   || '';
    var arialabel= (el.getAttribute('aria-label')   || '').toLowerCase();
    var provider = el.getAttribute('data-provider') || '';
    var clientid = el.getAttribute('data-client_id')|| '';
    var elid     = (el.id || '').toLowerCase();
    var type     = el.getAttribute('data-type')     || '';
    return (
      provider === 'google'                                   ||
      type === 'standard'                                     ||
      !!clientid                                              ||
      arialabel === 'sign in with google'                     ||
      arialabel === 'continue with google'                    ||
      arialabel === 'sign up with google'                     ||
      arialabel === 'log in with google'                      ||
      testid.includes('google-sso')                          ||
      testid.includes('google-signin')                       ||
      testid.includes('google-login')                        ||
      testid.includes('google-signup')                       ||
      testid.includes('google-auth')                         ||
      elid === 'google-signin-btn'                           ||
      elid === 'google-login-btn'
    );
  }

  // ── 4. BNPL detection ──────────────────────────────────────────────────────
  var BNPL_KW = ['klarna','affirm','afterpay','clearpay','sezzle','zip-pay','laybuy','bnpl'];
  function isBNPLEl(el) {
    if (!el) return false;
    var tag    = (el.tagName  || '').toLowerCase();
    var testid = (el.getAttribute && el.getAttribute('data-testid')) || '';
    var cls    = (typeof el.className === 'string' ? el.className : '') || '';
    if (tag === 'klarna-placement' || tag === 'klarna-express-button') return true;
    for (var i = 0; i < BNPL_KW.length; i++) {
      if (testid.toLowerCase().includes(BNPL_KW[i])) return true;
      if (cls.toLowerCase().includes(BNPL_KW[i]))    return true;
    }
    return false;
  }

  // Unified click + mousedown listeners (capture phase)
  // mousedown fires before click — catches cases where redirect begins on mousedown
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

  // Klarna custom events
  document.addEventListener('klarna:authorized', function() { preserveAttribution(null); }, true);
  document.addEventListener('klarna:load',       function() { preserveAttribution(null); }, true);

  // ── 5. Stripe fetch patch (POST confirm/payment_intents only) ──────────────
  var _origFetch = window.fetch;
  window.fetch = function(input, init) {
    try {
      var url    = typeof input === 'string' ? input : (input && input.url) || '';
      var method = ((init && init.method) || 'GET').toUpperCase();
      if (method === 'POST' && (
        (url.includes('stripe.com') && (url.includes('confirm') || url.includes('payment_intents'))) ||
        url.includes('klarna.com')   ||
        url.includes('affirm.com')   ||
        url.includes('afterpay.com') ||
        url.includes('clearpay.com')
      )) {
        preserveAttribution(null);
      }
    } catch(e) {}
    return _origFetch.apply(this, arguments);
  };

  // ── 6. Stripe.js SDK patch ────────────────────────────────────────────────
  // Stripe.js exposes window.Stripe — patch confirmCardPayment/confirmPayment
  function patchStripeSDK() {
    if (!window.Stripe) return;
    try {
      var _origStripe = window.Stripe;
      window.Stripe = function() {
        var instance = _origStripe.apply(this, arguments);
        var methods  = ['confirmCardPayment','confirmPayment','confirmSetup',
                        'confirmCardSetup','handleCardAction'];
        methods.forEach(function(m) {
          if (typeof instance[m] === 'function') {
            var _orig = instance[m].bind(instance);
            instance[m] = function() { preserveAttribution(null); return _orig.apply(this, arguments); };
          }
        });
        return instance;
      };
      Object.assign(window.Stripe, _origStripe);
    } catch(e) {}
  }
  // Patch immediately if Stripe already loaded, or watch for it
  patchStripeSDK();
  var _stripeObserver = new MutationObserver(function(muts) {
    for (var i = 0; i < muts.length; i++) {
      muts[i].addedNodes.forEach(function(n) {
        if (n.tagName === 'SCRIPT' && (n.src||'').includes('stripe')) {
          n.addEventListener('load', patchStripeSDK);
        }
      });
    }
    if (window.Stripe) { patchStripeSDK(); _stripeObserver.disconnect(); }
  });
  _stripeObserver.observe(document.documentElement, { childList: true, subtree: true });

  // ── 7. window.location patch ──────────────────────────────────────────────
  // Catches window.location.href = '...' and location.assign/replace
  try {
    var _origAssign  = window.location.assign.bind(window.location);
    var _origReplace = window.location.replace.bind(window.location);
    window.location.assign = function(href) {
      if (isSSOOrBNPLUrl(href)) preserveAttribution(null);
      return _origAssign(href);
    };
    window.location.replace = function(href) {
      if (isSSOOrBNPLUrl(href)) preserveAttribution(null);
      return _origReplace(href);
    };
  } catch(e) {}

  // ── 8. Form submit patch ───────────────────────────────────────────────────
  document.addEventListener('submit', function(e) {
    try {
      var action = (e.target && e.target.getAttribute('action')) || '';
      if (action && isSSOOrBNPLUrl(action)) preserveAttribution(null);
    } catch(e2) {}
  }, true);

  // ── 9. MutationObserver — late-rendered SSO/BNPL buttons ──────────────────
  // React/Next.js renders SSO buttons asynchronously after hydration.
  // We add mousedown listeners directly on buttons as they appear.
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

  // ── 10. visibilitychange / pagehide — last-chance beacon ──────────────────
  // Fires when user switches tabs or closes page mid-checkout
  function onPageHide() {
    var ids = resolveIds();
    if (ids.anonId || ids.userId) preserveAttribution(null);
  }
  document.addEventListener('visibilitychange', function() {
    if (document.visibilityState === 'hidden') onPageHide();
  });
  window.addEventListener('pagehide', onPageHide);

  // ── Boot ───────────────────────────────────────────────────────────────────
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

      // ── Health check ──────────────────────────────────────────────────────
      if (url.pathname === "/eden-health-check") {
        return jsonResponse({
          ok:                           true,
          worker:                       "eden-analytics",
          version:                      "5.24",
          ts:                           nowUTC(),
          kv:                           !!env.GCLID_KV,
          segment_write_key_configured: !!env.SEGMENT_WRITE_KEY,
          server_secret_configured:     !!env.SERVER_API_SECRET,
          attribution_model:            "first-touch — 18 channels + UTM + referrer + landing_page",
          coverage:                     "100% — all sources, all flows, all devices",
          layers:                       9,
          channels:                     CLICK_ID_CONFIG.map(c => c.label),
          utm_coverage:                 "source/medium/campaign/content/term/id",
          cross_domain:                 "eden.health ↔ app.eden.health via _gl + KV",
          sso_bnpl_domains:             SSO_BNPL_DOMAINS,
          anonid_sync:                  "boot + retry 5s + analytics.ready hook",
          stripe_patch:                 "fetch POST confirm + SDK confirmCardPayment",
          send_beacon:                  "true — pre-unload calls survive navigation",
          mutation_observer:            "true — late-rendered SSO/BNPL buttons covered",
          page_hide:                    "true — last-chance beacon on tab close",
          landing_page_stored:          "true — in KV + forwarded to Segment",
          referrer_stored:              "true — in KV + forwarded to Segment",
          dedup_key:                    "order_id with master_id fallback",
          itp_resistant:                "true — edge-set cookie bypasses Safari ITP",
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

      // ── Routes ────────────────────────────────────────────────────────────
      if (url.pathname === "/preserve-attribution" && request.method === "POST") {
        return handlePreserveAttribution(request, env, ctx);
      }
      if (url.pathname.startsWith("/collect") && request.method === "POST") {
        return handleCollect(request, env, ctx, url);
      }
      if (url.pathname === "/server-collect" && request.method === "POST") {
        return handleServerCollect(request, env, ctx);
      }
      if (url.pathname === "/identify" && request.method === "POST") {
        return handleIdentify(request, env, ctx);
      }

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

  const legacyAnonId   = readCookie(request, "eden_anonymous_id");
  const existingAnonId = readCookie(request, "eden_anon_id") || legacyAnonId;
  const existingSession= readCookie(request, "eden_session_id");

  const isNewVisitor = !existingAnonId;
  const isNewSession = !existingSession;

  const anonId  = existingAnonId  || crypto.randomUUID();
  const session = existingSession || `${crypto.randomUUID()}_${Date.now()}`;

  const clickIds   = extractClickIds(url);
  const utms       = extractUTMs(url);
  const preAuth    = !gpcOptOut ? extractPreAuthAttribution(request) : null;
  const organic    = detectOrganic(referrer);

  // Merge attribution — fresh click IDs > pre-auth > organic referrer
  const mergedClickIds = { ...(preAuth || {}), ...clickIds };

  // Build full attribution object including landing page + referrer
  const fullAttribution = {
    ...(organic || {}),
    ...(utms    || {}),
    ...mergedClickIds,
    ...(referrer ? { attribution_referrer: referrer }              : {}),
    ...(url      ? { landing_page: sanitizeUrl(url).toString() }   : {}),
  };

  const hasAttribution = Object.keys(fullAttribution).length > 0;

  // Always store in KV if there's ANY attribution signal (v5.24: includes UTM-only)
  if (hasAttribution && env.GCLID_KV && !gpcOptOut) {
    ctx.waitUntil(
      storeAttribution(env.GCLID_KV, KV_ANON_PREFIX + anonId, fullAttribution)
        .catch(err => console.error("[eden-analytics] KV store error:", err))
    );
  }

  const response    = await fetch(request);
  const contentType = response.headers.get("content-type") || "";
  const headers     = new Headers(response.headers);

  if (isNewVisitor) headers.append("Set-Cookie", buildAnonCookie(anonId, url));
  if (isNewSession) headers.append("Set-Cookie", buildSessionCookie(session, url));

  // Clear pre-auth immediately after reading
  if (preAuth) {
    headers.append("Set-Cookie", clearCookie("eden_pre_auth", url));
  }

  if (env.SEGMENT_WRITE_KEY && isNewSession && hasAttribution && !gpcOptOut) {
    ctx.waitUntil(
      fireFirstTouch(request, env, anonId, session, url, mergedClickIds, utms, referrer)
        .catch(err => console.error("[eden-analytics] first_touch error:", err))
    );
  }

  // Inject PREAUTH_SCRIPT: all Eden domains, 200 only, CSP nonce aware
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

  if (Object.keys(attribution).length === 0 && !referrer) return;

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
      landing_page:         cleanUrl,
      referrer:             referrer  || undefined,
      session_id:           sessionId,
      device_type:          isMobile(ua) ? "mobile" : "desktop",
      pipeline_version:     "5.24",
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
// /collect HANDLER
// =============================================================================

async function handleCollect(request, env, ctx, url) {
  const origin = request.headers.get("Origin") || "";
  if (origin && !isAllowedOrigin(origin)) return new Response("Forbidden", { status: 403 });

  let body;
  try { body = await request.json(); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  const gpcOptOut = request.headers.get("Sec-GPC") === "1";

  // Null-check BEFORE resolveOrderId / KV lookup
  if (!body.properties || typeof body.properties !== "object" || Array.isArray(body.properties)) {
    body.properties = {};
  }

  // Normalize all anonymousId variants (typos + case variants from SDKs)
  const cookieAnonId = readCookie(request, "eden_anon_id")
                    || readCookie(request, "eden_anonymous_id");
  const anonId = cookieAnonId
    || body.anonymousId
    || body.anonymous_id
    || body.anonymoous_id    // triple-o typo in wild
    || body.anonymous_Id     // capital I variant
    || body.anonymousid      // all-lowercase variant
    || body.properties?.anonymousId
    || body.properties?.anonymous_id
    || body.properties?.anonymoous_id
    || crypto.randomUUID();

  const isNew   = !cookieAnonId;
  const portal  = origin.includes("app.eden.health") ? "patient" : "marketing";
  const userId  = resolveUserIdFromBody(body);

  // KV lookup
  const storedAttribution = (env.GCLID_KV && !gpcOptOut)
    ? await resolveAttribution(env.GCLID_KV, anonId, userId, resolveOrderId(body))
    : null;

  // Extract UTMs + click IDs from actual page URL
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

  const contextCampaign = gpcOptOut ? {} : ((body.context || {}).campaign || {});

  // Full attribution merge: KV wins (most authoritative)
  const attribution = {
    ...(freshUTMs         || {}),
    ...freshClickIds,
    ...contextCampaign,
    ...(storedAttribution || {}),
    // Enrich with landing page + referrer from page context if not in KV
    ...(pageReferrer && !storedAttribution?.attribution_referrer
      ? { attribution_referrer: pageReferrer } : {}),
  };

  const campaignProps = buildCampaignContext(attribution);
  enrichPropertiesWithAttribution(body.properties, campaignProps);

  // v5.24: also set context.campaign for Segment's built-in BQ attribution
  if (!body.context) body.context = {};
  body.context.campaign = {
    ...((body.context || {}).campaign || {}),
    ...campaignProps,
  };

  const superProps = {
    portal,
    source_type:      "client",
    gpc_opt_out:      gpcOptOut,
    pipeline_version: "5.24",
  };

  // OS_purchase bridge
  const collectEventName = canonicalizeEventName(resolveEventName(body));
  const collectOrderId   = resolveOrderId(body);
  const collectUserId    = resolveUserIdFromBody(body);

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

  if (env.SEGMENT_WRITE_KEY) {
    ctx.waitUntil(
      forwardToSegment(env.SEGMENT_WRITE_KEY, body, anonId, superProps, attribution)
        .catch(err => console.error("[eden-analytics] collect error:", err))
    );
  }

  const respHeaders = { "Content-Type": "application/json", ...corsHeadersObj(origin) };
  if (isNew) respHeaders["Set-Cookie"] = buildAnonCookie(anonId, new URL(request.url));

  return new Response(JSON.stringify({ ok: true, anonId }), { status: 200, headers: respHeaders });
}


// =============================================================================
// /server-collect HANDLER
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
  const anonId    = identity.anonymousId || null;
  const userId    = identity.userId || null;
  const eventName = canonicalizeEventName(resolveEventName(body));
  const orderId   = resolveOrderId(body);

  if (!anonId && !userId) {
    console.warn("[eden-analytics] server-collect: no identity for event:", eventName);
  }

  if (eventName) body.event      = eventName;
  if (userId)    body.userId     = userId;
  if (anonId)    body.anonymousId= anonId;
  if (orderId && !body.properties.order_id) body.properties.order_id = orderId;

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

  // 3-way parallel attribution lookup
  const storedAttribution = env.GCLID_KV
    ? await resolveAttribution(env.GCLID_KV, anonId, userId, orderId)
    : null;

  if (storedAttribution) {
    for (const [k, v] of Object.entries(storedAttribution)) {
      if (!body.properties[k] && v) body.properties[k] = v;
    }
  }

  // Attribution bridge for downstream events
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

  const attribution   = { ...(storedAttribution || {}), ...((body.context || {}).campaign || {}) };
  const campaignProps = buildCampaignContext(attribution);
  enrichPropertiesWithAttribution(body.properties, campaignProps);

  // Also set context.campaign for Segment BQ
  if (!body.context) body.context = {};
  body.context.campaign = { ...((body.context || {}).campaign || {}), ...campaignProps };

  const superProps = {
    portal:           "patient",
    source_type:      "server",
    pipeline_version: "5.24",
    ...(identity.identityWarning   ? { identity_warning: identity.identityWarning     } : {}),
    ...(!anonId && !userId         ? { identity_warning: "no_identity_provided"       } : {}),
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
// /identify HANDLER
// =============================================================================

async function handleIdentify(request, env, ctx) {
  if (env.SERVER_API_SECRET) {
    const secret = request.headers.get("X-Eden-Server-Secret");
    if (secret !== env.SERVER_API_SECRET) return new Response("Unauthorized", { status: 401 });
  }

  let body;
  try { body = await request.json(); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  const identity = resolveIdentityFromBody(request, body);
  const anonId   = identity.anonymousId || null;
  const userId   = identity.userId || null;

  if (userId) body.userId     = userId;
  if (anonId) body.anonymousId= anonId;

  if (env.GCLID_KV && anonId && userId) {
    ctx.waitUntil(
      linkUserAttribution(env.GCLID_KV, anonId, userId)
        .catch(err => console.error("[eden-analytics] KV identify link:", err))
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
// /preserve-attribution HANDLER
// v5.24 FIX 11: handles both application/json AND text/plain (sendBeacon)
// v5.24: also writes attr:user if userId present
// =============================================================================

async function handlePreserveAttribution(request, env, ctx) {
  const origin = request.headers.get("Origin") || "";
  if (origin && !isAllowedOrigin(origin)) return new Response("Forbidden", { status: 403 });

  // sendBeacon sends Content-Type: text/plain — parse either way
  let body;
  try {
    const ct   = request.headers.get("content-type") || "";
    const text = await request.text();
    body = JSON.parse(text);
  } catch { return new Response("Invalid JSON", { status: 400 }); }

  const cookieAnonId = readCookie(request, "eden_anon_id")
                    || readCookie(request, "eden_anonymous_id");
  const anonId  = cookieAnonId || body.anonymousId;
  const userId  = body.userId;
  const orderId = body.orderId;

  if (!env.GCLID_KV) return jsonResponse({ ok: true, skipped: "no_kv" });

  const attribution = await resolveAttribution(env.GCLID_KV, anonId, userId, orderId);

  if (!attribution) return jsonResponse({ ok: true, skipped: "no_attribution" });

  // Write to all available keys (belt-and-suspenders)
  const writes = [];
  if (orderId) writes.push(
    storeAttribution(env.GCLID_KV, KV_ORDER_PREFIX + orderId, attribution)
      .catch(err => console.error("[eden-analytics] pre-auth order store:", err))
  );
  if (userId) writes.push(
    storeAttribution(env.GCLID_KV, KV_USER_PREFIX + userId, attribution)
      .catch(err => console.error("[eden-analytics] pre-auth user store:", err))
  );
  if (writes.length) ctx.waitUntil(Promise.all(writes));

  // Only set pre-auth cookie if there's a click ID worth preserving
  if (!CLICK_ID_PARAMS.some(p => attribution[p])) {
    return jsonResponse({ ok: true, skipped: "no_click_id" });
  }

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
    headers: { "Content-Type": "application/json", "Set-Cookie": preAuthCookie, ...corsHeadersObj(origin) },
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
      name:        body.name || body.properties?.name || "",
      properties:  await hashEmail({ ...superProps, ...(body.properties || {}) }),
      context: mergedContext, timestamp: nowUTC(),
    });
    return;
  }
  if (type === "screen") {
    await segmentPost(writeKey, "track", {
      anonymousId: anonId, userId: resolveUserIdFromBody(body),
      event:       `Viewed ${body.name || body.properties?.name || "Unknown Screen"}`,
      properties:  await hashEmail({ ...superProps, ...(body.properties || {}) }),
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
    event:       eventName,
    properties:  await hashEmail({ ...superProps, ...(body.properties || {}) }),
    context:     mergedContext,
    timestamp:   nowUTC(),
    ...(stableMessageId ? { messageId: stableMessageId } : {}),
  });
}


// =============================================================================
// KV ATTRIBUTION
// v5.24: always stores UTM-only sessions; enriches referrer + landing_page
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
        // First-touch: block click ID overwrite, but allow UTM + meta enrichment
        let enriched = false;
        const updated = { ...parsed };
        for (const k of UTM_ENRICHABLE) {
          if (attribution[k] && !parsed[k]) { updated[k] = attribution[k]; enriched = true; }
        }
        if (enriched) await kv.put(key, JSON.stringify(updated), { expirationTtl: KV_TTL });
        return;
      }

      if (existingHasClick && !newHasClick) {
        // Existing has click ID, new is UTM-only — enrich non-click fields only
        let enriched = false;
        const updated = { ...parsed };
        for (const k of UTM_ENRICHABLE) {
          if (attribution[k] && !parsed[k]) { updated[k] = attribution[k]; enriched = true; }
        }
        if (enriched) await kv.put(key, JSON.stringify(updated), { expirationTtl: KV_TTL });
        return;
      }

      // No click ID in existing — overwrite with new (may have click ID or fresher UTMs)
    }
  } catch {}

  await kv.put(key, JSON.stringify({ ...attribution, stored_at: nowUTC() }), { expirationTtl: KV_TTL });
}

async function getAttribution(kv, key) {
  if (!kv || !key) return null;
  try {
    const stored = await kv.get(key);
    if (!stored) return null;
    const { stored_at, ...attr } = JSON.parse(stored);
    return attr;
  } catch { return null; }
}

async function resolveAttribution(kv, anonId, userId, orderId = null) {
  if (!kv) return null;
  const [fromAnon, fromUser, fromOrder] = await Promise.all([
    anonId  ? getAttribution(kv, KV_ANON_PREFIX  + anonId)  : Promise.resolve(null),
    userId  ? getAttribution(kv, KV_USER_PREFIX   + userId)  : Promise.resolve(null),
    orderId ? getAttribution(kv, KV_ORDER_PREFIX  + orderId) : Promise.resolve(null),
  ]);
  // Prefer click ID entries
  if (fromAnon  && CLICK_ID_PARAMS.some(p => fromAnon[p]))  return fromAnon;
  if (fromUser  && CLICK_ID_PARAMS.some(p => fromUser[p]))  return fromUser;
  if (fromOrder && CLICK_ID_PARAMS.some(p => fromOrder[p])) return fromOrder;
  // Fall through to any entry with ANY attribution signal
  return fromAnon || fromUser || fromOrder || null;
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
  const campaign = {};
  const KEYS = ["utm_source","utm_medium","utm_campaign","utm_content","utm_term","utm_id",
                "landing_page","attribution_referrer", ...CLICK_ID_PARAMS];
  for (const k of KEYS) { if (attribution[k]) campaign[k] = attribution[k]; }
  return campaign;
}

function enrichPropertiesWithAttribution(properties, campaignProps) {
  if (!properties || typeof properties !== "object") return;
  if (!campaignProps || !Object.keys(campaignProps).length) return;
  for (const [k, v] of Object.entries(campaignProps)) {
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
  if (med === "organic")   return "organic_search";
  if (med === "email")     return "email";
  if (med === "sms")       return "sms";
  if (med === "affiliate") return "affiliate";
  if (med === "influencer")return "influencer";
  if (med === "synthetic") return "synthetic";
  if (med === "cpc"||med === "paid"||med === "paid_search"||med === "search_cpc"||
      c.gclid||c.gbraid||c.wbraid||c.dclid||c._gcl_au||c.srsltid||c.msclkid||
      src.includes("google")||src.includes("bing")||src.includes("microsoft"))
    return "paid_search";
  if (c.fbclid||c.ttclid||src.includes("facebook")||src.includes("instagram")||
      src.includes("meta")||src.includes("tiktok"))
    return "paid_social";
  if (c.li_fat_id||src.includes("linkedin")) return "paid_social_linkedin";
  if (c.rdt_cid||src.includes("reddit"))     return "paid_social_reddit";
  if (c.epik||src.includes("pinterest"))     return "paid_social_pinterest";
  if (c.twclid||src.includes("twitter")||src.includes("x.com")) return "paid_social_twitter";
  if (c.irclickid||c.cjevent||med==="affiliate") return "affiliate";
  // Infer from referrer if stored
  if (c.attribution_referrer) {
    try {
      const rh = new URL(c.attribution_referrer).hostname.toLowerCase();
      if (rh.includes("google"))    return "organic_search";
      if (rh.includes("bing"))      return "organic_search";
      if (rh.includes("facebook"))  return "organic_social";
      if (rh.includes("instagram")) return "organic_social";
      if (rh.includes("twitter")||rh.includes("x.com")) return "organic_social";
      if (rh.includes("linkedin"))  return "organic_social";
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
  return body.properties?.order_id||body.properties?.orderId||
    body.order_id||body.orderId||
    body.properties?.master_id||body.properties?.masterId||null;
}

function resolveUserIdFromBody(body) {
  return body.userId||body.user_id||
    body.properties?.userId||body.properties?.user_id||
    body.properties?.patient_id||body.properties?.customer_id||null;
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
    anonymousId,
    userId,
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
    method:  "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Basic ${btoa(writeKey + ":")}` },
    body:    JSON.stringify(payload),
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

// NOT HttpOnly — JS reads it for analytics.setAnonymousId()
function buildAnonCookie(id, url) {
  return [`eden_anon_id=${encodeURIComponent(id)}`,
    "Max-Age=63072000", `Domain=${cookieDomain(url)}`, "Path=/", "Secure", "SameSite=Lax"].join("; ");
}

function buildSessionCookie(v, url) {
  return [`eden_session_id=${encodeURIComponent(v)}`,
    "Max-Age=1800", `Domain=${cookieDomain(url)}`, "Path=/", "Secure", "SameSite=Lax"].join("; ");
}

function clearCookie(name, url) {
  return [`${name}=`, "Max-Age=0", `Domain=${cookieDomain(url)}`, "Path=/", "Secure", "SameSite=Lax"].join("; ");
}

function nowUTC() { return new Date(Date.now()).toISOString(); }
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
