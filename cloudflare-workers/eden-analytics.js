import { ConversionCoordinator } from "./eden-conversion-coordinator.js";

var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// eden-analytics-worker.js
var PIPELINE_VERSION = "5.56";
var ENRICHMENT_VERSION = "5.54";
var RELEASE_REVISION = "v556-phani-all-events-delivery-20260715";
var ALLOWED_ORIGINS = [
  "https://eden.health",
  "https://www.eden.health",
  "https://app.eden.health",
  "https://health-os-patient-staging-65y4qvz0n-eden-health.vercel.app"
];
var CLICK_ID_CONFIG = [
  { param: "gclid", channel: "google_ads", label: "Google Ads" },
  { param: "gbraid", channel: "google_ios", label: "Google iOS" },
  { param: "wbraid", channel: "google_web", label: "Google Web" },
  { param: "dclid", channel: "google_display", label: "Google Display" },
  { param: "_gcl_au", channel: "google_ads", label: "Google Cross-Domain" },
  { param: "gcl_au", channel: "google_ads", label: "Google Cross-Domain Alias" },
  { param: "_gcl_aw", channel: "google_ads", label: "Google Ads Cookie" },
  { param: "gcl_aw", channel: "google_ads", label: "Google Ads Cookie Alias" },
  { param: "_gcl_dc", channel: "google_display", label: "Google Display Cookie" },
  { param: "gcl_dc", channel: "google_display", label: "Google Display Cookie Alias" },
  { param: "_gcl_gb", channel: "google_ads", label: "Google GB Cookie" },
  { param: "gcl_gb", channel: "google_ads", label: "Google GB Cookie Alias" },
  { param: "_gcl_gs", channel: "google_ads", label: "Google GS Cookie" },
  { param: "gcl_gs", channel: "google_ads", label: "Google GS Cookie Alias" },
  { param: "srsltid", channel: "google_shopping", label: "Google Shopping" },
  { param: "fbclid", channel: "meta", label: "Meta/Facebook" },
  { param: "msclkid", channel: "microsoft", label: "Microsoft/Bing" },
  { param: "ttclid", channel: "tiktok", label: "TikTok" },
  { param: "twclid", channel: "twitter", label: "Twitter/X" },
  { param: "li_fat_id", channel: "linkedin", label: "LinkedIn" },
  { param: "rdt_cid", channel: "reddit", label: "Reddit" },
  { param: "epik", channel: "pinterest", label: "Pinterest" },
  { param: "ScCid", channel: "snapchat", label: "Snapchat" },
  { param: "nbt", channel: "northbeam", label: "Northbeam" },
  { param: "irclickid", channel: "impact_radius", label: "Impact Radius" },
  { param: "cjevent", channel: "cj_affiliate", label: "CJ Affiliate" },
  { param: "click_id", channel: "generic", label: "Generic" }
];
var CLICK_ID_PARAMS = CLICK_ID_CONFIG.map((c) => c.param);
var CANARY_ONLY_GOOGLE_PARAMS = /* @__PURE__ */ new Set([
  "gcl_au",
  "_gcl_aw",
  "gcl_aw",
  "_gcl_dc",
  "gcl_dc",
  "_gcl_gb",
  "gcl_gb",
  "_gcl_gs",
  "gcl_gs",
  "gclsrc",
  "_ga",
  "ga",
  "_gid",
  "gid",
  "ga_client_id",
  "ga_session_id",
  "gac",
  "gac_cookie_names",
  "gac_values"
]);
var PAID_SEARCH_CLICK_ID_PARAMS = ["gclid", "gbraid", "wbraid", "dclid", "_gcl_au", "gcl_au", "_gcl_aw", "gcl_aw", "_gcl_gb", "gcl_gb", "msclkid"];
var VALID_ACQUISITION_CHANNELS = /* @__PURE__ */ new Set([
  "paid_search",
  "organic_search",
  "direct",
  "email",
  "sms",
  "affiliate",
  "influencer",
  "paid_social",
  "organic_social",
  "paid_social_linkedin",
  "paid_social_reddit",
  "paid_social_pinterest",
  "paid_social_twitter",
  "synthetic",
  "unknown"
]);
var INVALID_ACQUISITION_CHANNELS = /* @__PURE__ */ new Set([
  "channel_main",
  "main",
  "default",
  "codex_test",
  "test",
  "qa",
  "chatgpt.com",
  "null",
  "undefined"
]);
var GOOGLE_AD_PARAM_FIELDS = [
  "gclsrc",
  "gad_source",
  "gad_campaignid",
  "gidrep",
  "creative",
  "matchtype",
  "network",
  "device",
  "targetid",
  "feeditemid",
  "placement",
  "nb_adtype",
  "nb_kwd",
  "nb_ti",
  "nb_mi",
  "nb_pc",
  "nb_pi",
  "nb_ppi",
  "_ga",
  "ga",
  "_gid",
  "gid",
  "ga_client_id",
  "ga_session_id",
  "gac",
  "gac_cookie_names",
  "gac_values",
  "nb_placement",
  "nb_li_ms",
  "nb_lp_ms",
  "nb_fii",
  "nb_ap",
  "nb_mt"
];
var PARTNER_PARAM_FIELDS = [
  "upfluence_id",
  "influencer_id",
  "creator_id",
  "partner_id",
  "affiliate_id",
  "referral_code",
  "referral_id",
  "ref",
  "source",
  "sub_id",
  "subid",
  "sub1",
  "sub2",
  "sub3",
  "sub4",
  "sub5",
  "campaign_id",
  "adgroup_id",
  "ad_group_id",
  "keyword",
  "search_term"
];
var QUERY_PARAM_NESTED_CONTAINER_KEYS = [
  "url",
  "u",
  "href",
  "target",
  "destination",
  "dest",
  "redirect",
  "redirect_url",
  "landing_page",
  "page_url",
  "next",
  "continue"
];

function debugLog(env, message) {
  if (String(env?.EDEN_ANALYTICS_DEBUG_LOGS || "").toLowerCase() !== "true") return;
  console.log(`[eden-analytics] ${message}`);
}
__name(debugLog, "debugLog");

var QUERY_PARAM_CANONICAL_NAME_BY_LOWER = null;
function getQueryParamCanonicalNameByLower() {
  if (QUERY_PARAM_CANONICAL_NAME_BY_LOWER) return QUERY_PARAM_CANONICAL_NAME_BY_LOWER;
  QUERY_PARAM_CANONICAL_NAME_BY_LOWER = /* @__PURE__ */ new Map();
  for (const key of [
    ...CLICK_ID_PARAMS,
    ...GOOGLE_AD_PARAM_FIELDS,
    ...PARTNER_PARAM_FIELDS,
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_content",
    "utm_term",
    "utm_id",
    "_gl",
    "eden_consent_handoff",
    "eden_consent_ads",
    "eden_attr_handoff"
  ]) {
    QUERY_PARAM_CANONICAL_NAME_BY_LOWER.set(String(key).toLowerCase(), key);
  }
  return QUERY_PARAM_CANONICAL_NAME_BY_LOWER;
}
__name(getQueryParamCanonicalNameByLower, "getQueryParamCanonicalNameByLower");
function safeDecodeQueryKey(rawKey) {
  let current = String(rawKey || "").trim();
  for (let i = 0; i < 3; i += 1) {
    try {
      const decoded = decodeURIComponent(current);
      if (decoded === current) break;
      current = decoded;
    } catch {
      break;
    }
  }
  return current.replace(/^(?:amp;)+/i, "").replace(/^[&?]+/, "").trim();
}
__name(safeDecodeQueryKey, "safeDecodeQueryKey");
function canonicalQueryParamName(rawKey) {
  const cleaned = safeDecodeQueryKey(rawKey);
  return getQueryParamCanonicalNameByLower().get(cleaned.toLowerCase()) || cleaned;
}
__name(canonicalQueryParamName, "canonicalQueryParamName");
function nestedAttributionSearchParams(value) {
  const raw = String(value || "");
  if (!raw || !/[?&=]|%3[fF]|%26|%3[dD]/.test(raw)) return [];
  const out = [];
  for (const candidate of [raw, safeDecodeQueryKey(raw)]) {
    if (!candidate) continue;
    try {
      out.push(new URL(candidate).searchParams);
      continue;
    } catch {
    }
    try {
      const normalized = candidate.startsWith("?") ? candidate.slice(1) : candidate;
      out.push(new URLSearchParams(normalized.includes("?") ? normalized.split("?").pop() : normalized));
    } catch {
    }
  }
  return out;
}
__name(nestedAttributionSearchParams, "nestedAttributionSearchParams");
function hashAttributionSearchParams(url) {
  if (!url?.hash) return [];
  const fragment = url.hash.slice(1);
  if (!fragment || !fragment.includes("=")) return [];
  const queryPart = fragment.includes("?") ? fragment.split("?").pop() : fragment.replace(/^\?/, "");
  try {
    return [new URLSearchParams(queryPart)];
  } catch {
    return [];
  }
}
__name(hashAttributionSearchParams, "hashAttributionSearchParams");
function semicolonDelimitedAttributionTail(value) {
  const raw = String(value || "");
  const pattern = /;(?=[A-Za-z0-9_%.-]+=)/g;
  let match;
  let start = -1;
  while ((match = pattern.exec(raw))) {
    if (raw.slice(Math.max(0, match.index - 4), match.index).toLowerCase() === "&amp") continue;
    start = match.index;
    break;
  }
  if (start < 0) return null;
  const tail = raw.slice(start + 1);
  return tail.replace(pattern, (token, offset, whole) =>
    whole.slice(Math.max(0, offset - 4), offset).toLowerCase() === "&amp" ? token : "&"
  );
}
__name(semicolonDelimitedAttributionTail, "semicolonDelimitedAttributionTail");
function extractCanonicalUrlParamEvidence(url, keys, { includeNested = true, includeHash = true, maxDepth = 6 } = {}) {
  const valuesByKey = /* @__PURE__ */ new Map();
  const wanted = new Set(keys.map((key) => canonicalQueryParamName(key)));
  const nestedSeen = /* @__PURE__ */ new Set();
  const add = (rawKey, value) => {
    const key = canonicalQueryParamName(rawKey);
    if (!wanted.has(key) || value === void 0 || value === null || String(value) === "") return;
    if (!valuesByKey.has(key)) valuesByKey.set(key, /* @__PURE__ */ new Set());
    valuesByKey.get(key).add(String(value));
  };
  const scan = /* @__PURE__ */ __name((params, depth = 0) => {
    if (!params) return;
    for (const [rawKey, value] of params.entries()) {
      if (!value) continue;
      const key = canonicalQueryParamName(rawKey);
      add(key, value);
      if (includeNested && depth < maxDepth && QUERY_PARAM_NESTED_CONTAINER_KEYS.includes(key.toLowerCase())) {
        const nestedKey = `${depth}:${value}`;
        if (!nestedSeen.has(nestedKey)) {
          nestedSeen.add(nestedKey);
          for (const nested of nestedAttributionSearchParams(value)) scan(nested, depth + 1);
        }
      }
      // Some redirect systems append recognized parameters with semicolons
      // inside one query value. Parse the tail as a bounded nested parameter
      // source while leaving the base value available for field-level
      // plausibility validation.
      const semicolonTail = includeNested && depth < maxDepth ? semicolonDelimitedAttributionTail(value) : null;
      if (semicolonTail) {
        scan(new URLSearchParams(semicolonTail), depth + 1);
      }
    }
  }, "scanCanonicalUrlParams");
  scan(url?.searchParams);
  if (includeHash) {
    for (const params of hashAttributionSearchParams(url)) scan(params);
  }
  const resolvedValues = {};
  const conflicts = {};
  for (const [key, candidateValues] of valuesByKey.entries()) {
    if (candidateValues.size === 1) {
      resolvedValues[key] = [...candidateValues][0];
    } else if (candidateValues.size > 1) {
      // Never return or log the conflicting raw identifiers. Callers only need
      // to know that the transport is ambiguous and therefore cannot be used
      // as owner-bound attribution evidence.
      conflicts[key] = candidateValues.size;
    }
  }
  return { values: resolvedValues, conflicts };
}
__name(extractCanonicalUrlParamEvidence, "extractCanonicalUrlParamEvidence");
function extractCanonicalUrlParams(url, keys, options = {}) {
  return extractCanonicalUrlParamEvidence(url, keys, options).values;
}
__name(extractCanonicalUrlParams, "extractCanonicalUrlParams");
function getCanonicalUrlParam(url, key, options = {}) {
  return extractCanonicalUrlParams(url, [key], options)[canonicalQueryParamName(key)] || null;
}
__name(getCanonicalUrlParam, "getCanonicalUrlParam");
function isBlockedObservationQueryKey(key) {
  return OBSERVATION_BLOCKED_QUERY_NAME_PATTERNS.some((pattern) => pattern.test(String(key || "")));
}
__name(isBlockedObservationQueryKey, "isBlockedObservationQueryKey");
function observeAttributionQueryKeys(url) {
  const known = /* @__PURE__ */ new Set();
  const unknown = /* @__PURE__ */ new Set();
  const blocked = /* @__PURE__ */ new Set();
  const normalizedAliases = /* @__PURE__ */ new Set();
  const nestedSources = /* @__PURE__ */ new Set();
  const knownKeys = /* @__PURE__ */ new Set([
    ...CLICK_ID_PARAMS.map((key) => canonicalQueryParamName(key)),
    ...GOOGLE_AD_PARAM_FIELDS.map((key) => canonicalQueryParamName(key)),
    ...PARTNER_PARAM_FIELDS.map((key) => canonicalQueryParamName(key)),
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_content",
    "utm_term",
    "utm_id",
    "_gl"
  ]);
  const addKey = /* @__PURE__ */ __name((rawKey) => {
    const canonical = canonicalQueryParamName(rawKey);
    if (!canonical) return;
    if (canonical !== String(rawKey || "").trim()) normalizedAliases.add(`${String(rawKey || "").trim()}=>${canonical}`);
    if (isBlockedObservationQueryKey(rawKey) || isBlockedObservationQueryKey(canonical)) {
      blocked.add(canonical.toLowerCase());
      return;
    }
    if (knownKeys.has(canonical)) known.add(canonical);
    else unknown.add(canonical.toLowerCase());
  }, "addObservedQueryKey");
  const scan = /* @__PURE__ */ __name((params, depth = 0, sourceKey = null) => {
    if (!params) return;
    for (const [rawKey, value] of params.entries()) {
      addKey(rawKey);
      const canonical = canonicalQueryParamName(rawKey);
      if (depth < 1 && value && QUERY_PARAM_NESTED_CONTAINER_KEYS.includes(canonical.toLowerCase())) {
        for (const nested of nestedAttributionSearchParams(value)) {
          nestedSources.add(canonical.toLowerCase());
          scan(nested, depth + 1, canonical);
        }
      }
    }
  }, "scanObservedQueryKeys");
  scan(url?.searchParams);
  for (const params of hashAttributionSearchParams(url)) scan(params);
  const limit = /* @__PURE__ */ __name((values) => [...values].sort().slice(0, 50), "limitObservedKeys");
  return compactDefined({
    known_query_keys: limit(known),
    unknown_query_keys: limit(unknown),
    blocked_query_keys: limit(blocked),
    normalized_query_key_aliases: limit(normalizedAliases),
    nested_query_key_sources: limit(nestedSources),
    unknown_query_key_count: unknown.size,
    blocked_query_key_count: blocked.size
  });
}
__name(observeAttributionQueryKeys, "observeAttributionQueryKeys");
// These are distinct server-authoritative business milestones, not aliases of
// one another. Server processing normalizes `purchase` to the current
// `OS_purchase` event, while browser forwarding preserves the producer's exact
// bounded event name and marks any matching outcome as a provisional client
// observation. Qualification, order completion, and reorder completion retain
// separate event names and downstream conversion actions.
var CONVERSION_EVENTS = /* @__PURE__ */ new Set([
  "OS_qualified_first_order",
  "OS_purchase",
  "order_completed",
  "reorder_completed"
]);
var CONVERSION_BUSINESS_STAGES = /* @__PURE__ */ new Map([
  ["OS_purchase", "commercial_payment_authorized"],
  ["OS_qualified_first_order", "qualified_first_order"],
  ["order_completed", "order_completed"],
  ["reorder_completed", "reorder_completed"]
]);
function isBrowserOutcomeObservationName(rawName) {
  const canonical = canonicalizeEventName(rawName);
  if (CONVERSION_EVENTS.has(canonical)) return true;
  const normalized = String(rawName || "")
    .trim()
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  // This is classification-only. It never blocks or renames an event. It
  // ensures a newly introduced browser outcome name remains visibly
  // provisional until Segment/dbt explicitly register server authority.
  return /(?:^|_)(?:purchase|qualified_first_order|reorder_completed|first_order|customer_acquired|conversion_completed|checkout_success|payment_(?:succeeded|authorized|captured|completed|processed)|order_(?:completed|delivered|approved|placed|paid))(?:_|$)/.test(normalized);
}
__name(isBrowserOutcomeObservationName, "isBrowserOutcomeObservationName");
// Browser collection is an attribution and continuity transport, not a second
// tracking-plan gate. Segment owns event registration/schema governance and
// dbt owns authoritative purchase/customer reconciliation. The Worker accepts
// every bounded track/page/screen name, preserves the producer name, enriches
// it with edge-owned session/attribution context, and blocks only technically
// unsafe payload content. A signed browser capability authenticates transport;
// it never turns a browser-supplied user/order claim into stable identity.
var EVENT_NAME_ALIASES = {
  "os_qualified_first_order": "OS_qualified_first_order",
  "qualified_first_order": "OS_qualified_first_order",
  "os_purchase": "OS_purchase",
  "purchase": "OS_purchase",
  "order_completed": "order_completed",
  "reorder_completed": "reorder_completed"
};
var BOT_UA_PATTERNS = [
  /bot\b/i,
  /crawler/i,
  /spider/i,
  /headless/i,
  /lighthouse/i,
  /pagespeed/i,
  /playwright/i,
  /puppeteer/i,
  /preview/i,
  /prerender/i,
  /google-inspectiontool/i,
  /checklyhq/i,
  /Googlebot/i,
  /bingbot/i,
  /facebookexternalhit/i,
  /Twitterbot/i,
  /LinkedInBot/i,
  /Slackbot/i
];
var BOT_CF_DECISIONS = /* @__PURE__ */ new Set([
  "automated",
  "likely_automated",
  "verified_bot"
]);
var STATIC_EXTENSIONS = [
  ".avif",
  ".bmp",
  ".css",
  ".gif",
  ".ico",
  ".jpg",
  ".jpeg",
  ".js",
  ".mjs",
  ".map",
  ".mp4",
  ".otf",
  ".png",
  ".svg",
  ".ttf",
  ".wasm",
  ".webm",
  ".webp",
  ".woff",
  ".woff2"
];
var STATIC_PREFIXES = [
  "/_next/static/",
  "/static/chunks/",
  "/static/css/",
  "/static/js/",
  "/static/media/",
  "/favicon",
  "/robots.txt",
  "/sitemap"
];
var SENSITIVE_URL_PARAMS = [
  /client_secret/i,
  /payment_intent/i,
  /setup_intent/i,
  /^secret$/i,
  /^password$/i,
  /^token$/i,
  /^code$/i,
  /^state$/i,
  /auth_code/i,
  /email/i,
  /phone/i,
  /name/i,
  /member_id/i,
  /order_id/i,
  /order_value/i,
  /bmi/i,
  /height/i,
  /weight/i,
  /medication/i,
  /answers/i,
  /diagnosis/i,
  /allergy/i,
  /contraindication/i,
  /clinical/i,
  /address/i
];
var OBSERVATION_BLOCKED_QUERY_NAME_PATTERNS = [
  ...SENSITIVE_URL_PARAMS,
  /^eden_attr_handoff$/i,
  /auth_code/i,
  /email/i,
  /phone/i,
  /name/i,
  /member_id/i,
  /order_id/i,
  /order_value/i,
  /bmi/i,
  /height/i,
  /weight/i,
  /medication/i,
  /answers/i,
  /diagnosis/i,
  /allergy/i,
  /contraindication/i,
  /clinical/i,
  /address/i
];
var SSO_BNPL_DOMAINS = [
  "accounts.google.com",
  "oauth2.googleapis.com",
  "klarna.com",
  "pay.klarna.com",
  "checkout.klarna.com",
  "affirm.com",
  "sandbox.affirm.com",
  "afterpay.com",
  "portal.afterpay.com",
  "clearpay.co.uk",
  "clearpay.com",
  "sezzle.com",
  "zip.co",
  "laybuy.com"
];
var KV_ANON_PREFIX = "attr:anon:";
// Stable user/order continuity is written and read only by the authenticated
// server route. A new namespace prevents previously browser-poisoned v1 rows
// from being promoted by a later legitimate purchase event.
var KV_TRUSTED_SERVER_USER_PREFIX = "attr:server:v1:user:";
var KV_TRUSTED_SERVER_ORDER_PREFIX = "attr:server:v1:order:";
var KV_ALIAS_PREFIX = "alias:fired:";
var ATTRIBUTION_DENIAL_PREFIX = "privacy:ads_denied:v1:";
var ATTRIBUTION_DENIAL_SCHEMA_VERSION = "eden_ads_denial_v1";
var ATTRIBUTION_DENIAL_TTL = 31536e3;
var ATTRIBUTION_DENIAL_COOKIE_NAME = "__Secure-eden_ads_denied";
var PRIVACY_LEDGER_HMAC_SECRET_ENV = "PRIVACY_LEDGER_HMAC_SECRET";
var PRIVACY_LEDGER_HMAC_PREVIOUS_SECRET_ENV = "PRIVACY_LEDGER_HMAC_SECRET_PREVIOUS";
var KV_TTL = 10368e3;
// Keep order-level conversion idempotency beyond normal reporting and retry
// windows. A one-day record allowed late producer replays to recreate the same
// purchase after Segment's own short dedupe window.
var KV_DEDUP_TTL = 31536e3;
var CONVERSION_COORDINATOR_BINDING = "CONVERSION_COORDINATOR";
var CONVERSION_COORDINATOR_LEASE_TTL_MS = 12e4;
var CONVERSION_SEGMENT_TIMEOUT_MS = 3e4;
// Durable Object values are limited to 128 KiB. Pending conversion records
// temporarily retain the exact Segment payload needed for byte-identical
// unknown-commit replay, so allow a bounded envelope while leaving headroom for
// storage metadata and future schema fields.
var CONVERSION_PENDING_SEGMENT_PAYLOAD_MAX_BYTES = 96e3;
var KV_IDLINK_TTL = 2592e3;
var KV_ALIAS_TTL = 31536e4;
var ATTR_COOKIE_NAME = "eden_attr";
var ENRICHMENT_CANARY_PARAM = "eden_tracking_enrichment_canary";
var ATTR_COOKIE_TTL = 2592e3;
var AD_CLICK_MEMORY_MODE_ENV = "EDEN_AD_CLICK_MEMORY_MODE";
var AD_CLICK_MEMORY_SCHEMA_VERSION = "eden_ad_click_v1";
var AD_CLICK_IDENTITY_LINK_SCHEMA_VERSION = "eden_ad_identity_link_v2";
var AD_CLICK_POINTER_COOKIE_NAME = "__Secure-eden_ad_click_id";
var AD_CLICK_POINTER_COOKIE_TTL = 7776e3;
var AD_CLICK_POINTER_RECORD_SCHEMA_VERSION = "eden_ad_click_pointer_v2";
var MAX_JSON_BODY_BYTES = 65536;
var BROWSER_CAPABILITY_COOKIE_NAME = "__Secure-eden_browser_cap";
var BROWSER_CAPABILITY_AUDIENCE = "eden-analytics-browser";
var BROWSER_CAPABILITY_VERSION = 2;
var BROWSER_CAPABILITY_COLLECTOR_HOST = "collect.eden.health";
var BROWSER_CAPABILITY_TTL_SECONDS = 7200;
var BROWSER_CAPABILITY_CLOCK_SKEW_SECONDS = 60;
var BROWSER_CAPABILITY_MAX_BYTES = 1024;
var BROWSER_CAPABILITY_SECRET_ENV = "BROWSER_CAP_HMAC_SECRET";
var BROWSER_CAPABILITY_PREVIOUS_SECRET_ENV = "BROWSER_CAP_HMAC_SECRET_PREVIOUS";
var BROWSER_CAPABILITY_ENFORCEMENT_ENV = "EDEN_BROWSER_CAP_ENFORCEMENT_MODE";
var INTERNAL_HANDOFF_QUERY_PARAM = "eden_attr_handoff";
var INTERNAL_HANDOFF_COOKIE_NAME = "__Secure-eden_internal_handoff";
var INTERNAL_HANDOFF_AUDIENCE = "eden-analytics-internal-handoff";
var INTERNAL_HANDOFF_ASSERTION_VERSION = 2;
var INTERNAL_HANDOFF_TTL_SECONDS = 7200;
var INTERNAL_HANDOFF_MAX_BYTES = 1536;
var AD_CLICK_KV_PREFIX = "adclick:";
var AD_CLICK_KV_INDEX_MODE_ENV = "EDEN_AD_CLICK_KV_INDEX_MODE";
var AD_CLICK_KV_RESOLVER_MODE_ENV = "EDEN_AD_CLICK_KV_RESOLVER_MODE";
var AD_CLICK_REVERSE_KV_RETENTION_MODE_ENV = "EDEN_AD_CLICK_REVERSE_KV_RETENTION_MODE";
var AD_CLICK_REVERSE_KV_TTL_SECONDS_ENV = "EDEN_AD_CLICK_REVERSE_KV_TTL_SECONDS";
var AD_CLICK_FULL_REVERSE_KV_RESOLVER_IMPLEMENTED = true;
// v3: every Google click ad_click_id is first-party scoped, reverse KV keys are
// first-party-only under adclick:v2:, and click-value/_gcl_au reverse keys are neither
// written nor read. v1 reverse indexes (adclick:anon|session|user|order|<param>|gcl_au:*)
// are quarantined: never trusted for recovery again.
var AD_CLICK_RESOLUTION_POLICY_VERSION = "ad_click_all_first_party_scope_policy_v3";
var AD_CLICK_KV_REVERSE_PREFIX = "adclick:v2:";
var AD_CLICK_QUEUE_ENVELOPE_SCHEMA_VERSION = "eden_ad_click_memory_envelope_v1";
var AD_CLICK_QUEUE_CONSUMER_ENABLED_ENV = "AD_CLICK_MEMORY_QUEUE_CONSUMER_ENABLED";
var AD_CLICK_BIGQUERY_EXTERNAL_IO_TIMEOUT_MS = 1e4;
var AD_CLICK_QUEUE_BATCH_CONCURRENCY = 3;
var AD_CLICK_SNAPSHOT_BIGQUERY_TABLE_DEFAULT = "ad_click_snapshots_v1";
var AD_CLICK_IDENTITY_LINK_BIGQUERY_TABLE_DEFAULT = "ad_click_identity_links_v1";
var AD_CLICK_INGEST_ERROR_BIGQUERY_TABLE_DEFAULT = "ad_click_memory_ingest_errors_v1";
var AD_CLICK_CLASS_A_GOOGLE_PARAMS = ["gclid", "gbraid", "wbraid"];
var AD_CLICK_DESTINATION_SPECIFIC_GOOGLE_PARAMS = ["dclid"];
var AD_CLICK_CLASS_B_GOOGLE_PARAMS = ["_gcl_au", "gcl_au", "_gcl_aw", "gcl_aw", "_gcl_dc", "gcl_dc", "_gcl_gb", "gcl_gb", "_gcl_gs", "gcl_gs", "gclsrc", "gad_source", "gad_campaignid", "srsltid", "_ga", "ga", "_gid", "gid", "ga_client_id", "ga_session_id", "gac", "gac_cookie_names", "gac_values"];
var RAW_AD_ID_BRIDGE_RETIRED_PARAMS = [...new Set([...CLICK_ID_PARAMS, ...AD_CLICK_CLASS_B_GOOGLE_PARAMS])];
var AD_CLICK_CLASS_C_CAMPAIGN_PARAMS = ["utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term", "utm_id", "landing_page", "attribution_referrer"];
// Fields that describe one current paid touch. When a genuinely different
// upload-grade click arrives, none of the prior touch's campaign or platform
// metadata may remain paired with the new click. Immutable first-touch history
// remains in its owner-scoped KV/snapshot rows; this set controls only the
// replaceable active-touch cookie/event view.
var ACTIVE_PAID_TOUCH_FIELDS = /* @__PURE__ */ new Set([
  ...CLICK_ID_PARAMS,
  ...AD_CLICK_CLASS_B_GOOGLE_PARAMS,
  ...AD_CLICK_CLASS_C_CAMPAIGN_PARAMS,
  ...GOOGLE_AD_PARAM_FIELDS,
  ...PARTNER_PARAM_FIELDS
].map((key) => canonicalQueryParamName(key)));
var INTERNAL_HANDOFF_TRANSPORT_QUERY_PARAMS = [...new Set([
  ...CLICK_ID_PARAMS,
  ...GOOGLE_AD_PARAM_FIELDS,
  "utm_source",
  "utm_medium",
  "utm_campaign",
  "utm_content",
  "utm_term",
  "utm_id",
  "_gl"
].map((key) => String(key).toLowerCase()))];
var ATTR_COOKIE_CLICK_ID_TTL = ATTR_COOKIE_TTL * 1e3;
var ATTR_COOKIE_MAX_ENCODED_BYTES = 3500;
var ATTR_COOKIE_CORE_FIELDS = [
  "gclid",
  "gbraid",
  "wbraid",
  "dclid",
  "gidrep",
  "gclsrc",
  "gad_source",
  "gad_campaignid",
  "_gcl_au",
  "gcl_au",
  "_gcl_aw",
  "gcl_aw",
  "_gcl_dc",
  "gcl_dc",
  "_gcl_gb",
  "gcl_gb",
  "_gcl_gs",
  "gcl_gs",
  "srsltid",
  "utm_source",
  "utm_medium",
  "utm_campaign",
  "utm_content",
  "utm_term",
  "utm_id",
  "landing_page",
  "attribution_referrer"
];
var ATTR_COOKIE_CORE_FIELD_SET = /* @__PURE__ */ new Set(ATTR_COOKIE_CORE_FIELDS);
var ATTR_COOKIE_DIAGNOSTIC_DROP_FIELDS = [
  "gac_values",
  "gac_cookie_names",
  "gac",
  "_ga",
  "ga",
  "_gid",
  "gid",
  "ga_client_id",
  "ga_session_id",
  "nb_li_ms",
  "nb_lp_ms",
  "nb_fii",
  "nb_ap",
  "nb_mt",
  "nb_adtype",
  "nb_kwd",
  "nb_ti",
  "nb_mi",
  "nb_pc",
  "nb_pi",
  "nb_ppi",
  "nb_placement"
];
var ATTR_COOKIE_SECONDARY_DROP_FIELDS = PARTNER_PARAM_FIELDS.filter((field) => !ATTR_COOKIE_CORE_FIELD_SET.has(field));
var ATTR_COOKIE_FIELDS = [
  "gclid",
  "_gcl_au",
  "gcl_au",
  "_gcl_aw",
  "gcl_aw",
  "_gcl_dc",
  "gcl_dc",
  "_gcl_gb",
  "gcl_gb",
  "_gcl_gs",
  "gcl_gs",
  "gbraid",
  "wbraid",
  "dclid",
  "srsltid",
  "fbclid",
  "msclkid",
  "ttclid",
  "twclid",
  "li_fat_id",
  "rdt_cid",
  "epik",
  "ScCid",
  "nbt",
  "irclickid",
  "cjevent",
  "click_id",
  "utm_source",
  "utm_medium",
  "utm_campaign",
  "utm_content",
  "utm_term",
  "utm_id",
  ...GOOGLE_AD_PARAM_FIELDS,
  ...PARTNER_PARAM_FIELDS
];
var UTM_ENRICHABLE = [
  "utm_campaign",
  "utm_content",
  "utm_term",
  "utm_id",
  "attribution_campaign",
  "landing_page",
  "attribution_referrer",
  ...GOOGLE_AD_PARAM_FIELDS
];
var ATTRIBUTION_TRAIT_KEYS = [
  "acquisition_channel",
  "attribution_source",
  "attribution_medium",
  "attribution_campaign",
  "attribution_referrer",
  "landing_page",
  "utm_source",
  "utm_medium",
  "utm_campaign",
  "utm_content",
  "utm_term",
  "gclid",
  "_gcl_au",
  "gcl_au",
  "_gcl_aw",
  "gcl_aw",
  "_gcl_dc",
  "gcl_dc",
  "_gcl_gb",
  "gcl_gb",
  "_gcl_gs",
  "gcl_gs",
  "gbraid",
  "wbraid",
  "fbclid",
  "msclkid",
  "ttclid",
  "twclid",
  "li_fat_id",
  "srsltid"
];
var KV_INTERNAL_FIELDS = /* @__PURE__ */ new Set(["stored_at", "_ts", "_click_first_observed_at", "_last_seen_at"]);
var PREAUTH_SCRIPT = `<script>
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
    var anonId = getCookie('eden_anonymous_id') || getCookie('eden_anon_id');
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

  function refreshBrowserCapability() {
    return fetch('/browser-capability', {
      method: 'GET', credentials: 'include', cache: 'no-store',
      headers: { 'Accept': 'application/json' },
    }).then(function(resp) {
      if (!resp.ok) throw new Error('browser_capability_refresh_failed_' + resp.status);
      return true;
    });
  }

  function postJSON(path, payload) {
    var str = JSON.stringify(payload);
    function send(attempt) {
      return fetch(path, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: str, keepalive: true, credentials: 'include',
      }).then(function(resp) {
        if ((resp.status === 401 || resp.status === 409) && attempt === 0) {
          return refreshBrowserCapability().then(function() { return send(1); });
        }
        return resp;
      });
    }
    return send(0).catch(function(){ return null; });
  }

  function readPreserveResponse(resp) {
    if (!resp || !resp.ok || resp.status !== 200 || typeof resp.clone !== 'function') {
      return Promise.resolve({ durable: false, handoffAssertion: null });
    }
    return resp.clone().json().then(function(body) {
      var observationDurable = !!(
        body
        && body.ok === true
        && body.ad_click_observation_persisted === true
        && body.queue_enqueued === true
        && body.pointer_kv_persisted === true
        && body.owner_attribution_kv_persisted === true
      );
      var handoffDurable = !!(
        body
        && body.ok === true
        && body.internal_handoff_durable === true
        && body.pointer_kv_persisted === true
        && body.owner_attribution_kv_persisted === true
      );
      var durable = observationDurable || handoffDurable;
      var handoffAssertion = handoffDurable && typeof body.internal_handoff_assertion === 'string'
        && /^h1\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(body.internal_handoff_assertion)
        ? body.internal_handoff_assertion
        : null;
      return { durable: durable, handoffAssertion: handoffAssertion };
    }).catch(function() { return { durable: false, handoffAssertion: null }; });
  }

  var _activePreservePromise = null;
  var _activePreserveFingerprint = null;
  var _lastPreservedFingerprint = null;
  var _lastPreservedAt = 0;
  var _lastPreserveOutcome = null;
  function beginPreserveRequest(resolvedOrderId, pageUrl, fingerprint, handoffDestination) {
    var ids = resolveIds();
    return postJSON('/preserve-attribution', {
      anonymousId: ids.anonId || null,
      userId:      ids.userId || null,
      orderId:     resolvedOrderId,
      // Cloudflare never receives a browser URL fragment. Supplying the
      // current URL to the authenticated first-party endpoint lets the
      // Worker extract governed fragment evidence before sanitizing it.
      pageUrl:     pageUrl,
      handoffDestination: handoffDestination || null,
    }).then(function(resp) {
      return readPreserveResponse(resp).then(function(outcome) {
        if (outcome.durable) {
          _lastPreservedFingerprint = fingerprint;
          _lastPreservedAt = Date.now();
          _lastPreserveOutcome = outcome;
        }
        return outcome;
      });
    }, function() {
      return { durable: false, handoffAssertion: null };
    });
  }

  function activatePreservePromise(trackedPromise, fingerprint) {
    _activePreservePromise = trackedPromise;
    _activePreserveFingerprint = fingerprint;
    var releaseTimer = setTimeout(function() {
      if (_activePreservePromise === trackedPromise) {
        _activePreservePromise = null;
        _activePreserveFingerprint = null;
      }
    }, 10000);
    function releaseTrackedPromise() {
      clearTimeout(releaseTimer);
      if (_activePreservePromise === trackedPromise) {
        _activePreservePromise = null;
        _activePreserveFingerprint = null;
      }
    }
    trackedPromise.then(releaseTrackedPromise, releaseTrackedPromise);
    return trackedPromise;
  }

  function preserveAttribution(orderId, pageUrlOverride, handoffDestinationOverride) {
    var pageUrl = String(pageUrlOverride || window.location.href || '');
    var resolvedOrderId = orderId || getOrderIdFromDOM() || null;
    var handoffDestination = String(handoffDestinationOverride || '');
    var fingerprint = pageUrl + '|' + String(resolvedOrderId || '') + '|' + handoffDestination;
    if (_activePreservePromise) {
      if (_activePreserveFingerprint === fingerprint) return _activePreservePromise;
      // A new fragment/History state appeared while an earlier preserve was in
      // flight. Make the serialized tail the active barrier so the earlier
      // Promise can never release a HealthOS handoff before the newer evidence
      // is durably preserved.
      var previousPromise = _activePreservePromise;
      var serializedPromise = previousPromise.then(function() {
        return beginPreserveRequest(resolvedOrderId, pageUrl, fingerprint, handoffDestination);
      }, function() {
        return beginPreserveRequest(resolvedOrderId, pageUrl, fingerprint, handoffDestination);
      });
      return activatePreservePromise(serializedPromise, fingerprint);
    }
    if (_lastPreservedFingerprint === fingerprint && Date.now() - _lastPreservedAt < 30000) {
      return Promise.resolve(_lastPreserveOutcome || { durable: true, handoffAssertion: null });
    }
    try {
      return activatePreservePromise(
        beginPreserveRequest(resolvedOrderId, pageUrl, fingerprint, handoffDestination),
        fingerprint
      );
    } catch(e) {
      _activePreservePromise = null;
      return Promise.resolve({ durable: false, handoffAssertion: null });
    }
  }

  function attributionTextHasGovernedParam(raw) {
    if (!raw || raw.indexOf('=') === -1) return false;
    for (var i = 0; i < 2; i++) {
      try {
        var decoded = decodeURIComponent(raw);
        if (decoded === raw) break;
        raw = decoded;
      } catch(e) { break; }
    }
    var governed = ${JSON.stringify([...new Set([...CLICK_ID_PARAMS, ...AD_CLICK_CLASS_B_GOOGLE_PARAMS, "gclsrc", "gad_source", "gad_campaignid"])])};
    for (var j = 0; j < governed.length; j++) {
      if (new RegExp('(?:^|[?&#;])(?:amp;)?' + governed[j] + '=', 'i').test(raw)) return true;
    }
    return false;
  }

  function browserLocationHasAttribution(includeSearch) {
    var raw = (includeSearch ? String(window.location.search || '') : '') + String(window.location.hash || '');
    return attributionTextHasGovernedParam(raw);
  }

  function pageUrlHasAttribution(pageUrl) {
    try {
      var parsed = new URL(String(pageUrl || ''), window.location.href);
      return attributionTextHasGovernedParam(String(parsed.search || '') + String(parsed.hash || ''));
    } catch(e) {
      return false;
    }
  }

  function preserveBrowserOnlyAttribution(includeSearch) {
    if (browserLocationHasAttribution(!!includeSearch)) preserveAttribution(null);
  }

  // Two durable writes can be serialized here: a fragment/History observation
  // already in flight, followed by the destination-bound handoff assertion.
  // A 1.5s total budget caused healthy second writes to lose the race and
  // navigate fail-open. Keep a bounded UX escape hatch, but allow the normal
  // two-write path to complete under realistic edge/network variance.
  var HEALTHOS_INTAKE_HANDOFF_TIMEOUT_MS = 5000;
  var _healthOsHandoffPending = false;
  function closestAnchorForClick(target) {
    var node = target;
    while (node && node !== document) {
      if (String(node.tagName || '').toLowerCase() === 'a' && node.getAttribute && node.getAttribute('href')) return node;
      node = node.parentElement;
    }
    return null;
  }

  function onHealthOsIntakeClick(event) {
    if (!event || event.defaultPrevented) return;
    if (typeof event.button === 'number' && event.button !== 0) return;
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    if (window.location.hostname !== 'www.eden.health') return;
    var anchor = closestAnchorForClick(event.target);
    if (!anchor || (anchor.hasAttribute && anchor.hasAttribute('download'))) return;
    var target = String(anchor.getAttribute('target') || '').trim().toLowerCase();
    if (target && target !== '_self') return;
    var destination;
    try { destination = new URL(anchor.href || anchor.getAttribute('href'), window.location.href); } catch(e) { return; }
    if (destination.origin !== 'https://app.eden.health') return;
    if (destination.pathname !== '/intake' && !destination.pathname.startsWith('/intake/')) return;
    var pendingPreserve = _activePreservePromise;
    // A prior paid touch may already have been cleaned from the address bar.
    // The browser-readable first-touch bridge is only a signal to ask the
    // authenticated Worker for a destination-bound assertion; the Worker still
    // requires the HttpOnly owner/session/pointer state before it will mint one.
    var destinationHasAttribution = pageUrlHasAttribution(destination.toString());
    if (!browserLocationHasAttribution(true) && !destinationHasAttribution && !pendingPreserve && !getCookie('eden_attr')) return;
    event.preventDefault();
    if (_healthOsHandoffPending) return;
    _healthOsHandoffPending = true;
    // The destination-bound assertion is minted only after the current browser
    // observation is durable. A pre-existing fragment preserve is serialized
    // ahead of this destination-bound request rather than reused without proof.
    // A paid identifier may exist only on the destination href (for example a
    // Webflow link assembled after the landing URL was cleaned). Preserve that
    // exact URL so the click is captured before navigation rather than relying
    // on the current page to still expose the evidence.
    // Current-page evidence is the newest browser observation and must select
    // the pointer even when a Webflow CTA still carries an older paid query.
    // Use destination evidence only when the browser location has none. The
    // destination URL is still separately bound into the signed assertion, so
    // native HealthOS trackers retain its transported query without allowing
    // that older transport to replace the current first-party pointer.
    var preservePageUrl = browserLocationHasAttribution(true)
      ? String(window.location.href || '')
      : destinationHasAttribution ? destination.toString() : null;
    var preservePromise = preserveAttribution(null, preservePageUrl, destination.toString());
    var durablePromise = Promise.resolve(preservePromise).then(function(outcome) {
      return outcome && outcome.durable === true && outcome.handoffAssertion
        ? outcome
        : { durable: false, handoffAssertion: null, definitive: true };
    }, function() { return { durable: false, handoffAssertion: null, definitive: true }; });
    var timeoutId = null;
    var timeoutPromise = new Promise(function(resolve) {
      timeoutId = setTimeout(function() { resolve('fail_open_timeout'); }, HEALTHOS_INTAKE_HANDOFF_TIMEOUT_MS);
    });
    function continueToHealthOs(outcome) {
      var durable = !!(outcome && outcome.durable === true && outcome.handoffAssertion);
      if (timeoutId !== null) clearTimeout(timeoutId);
      if (durable) destination.searchParams.set('${INTERNAL_HANDOFF_QUERY_PARAM}', outcome.handoffAssertion);
      var outcomeName = durable
        ? 'durable'
        : outcome === 'fail_open_timeout' ? 'fail_open_timeout' : 'fail_open_no_durable_evidence';
      try {
        window.__edenHealthOsHandoffLastOutcome = {
          outcome: outcomeName,
          handoffBound: durable,
          recordedAt: Date.now(),
        };
      } catch(e) {}
      _healthOsHandoffPending = false;
      // The preserve response can rotate the HttpOnly owner-scoped pointer.
      // Yield one short browser task so Set-Cookie commits before the outgoing
      // app.eden.health document request snapshots its Cookie header.
      setTimeout(function() {
        window.location.assign(destination.toString());
      }, 50);
    }
    Promise.race([durablePromise, timeoutPromise]).then(continueToHealthOs, function() {
      continueToHealthOs('fail_open_timeout');
    });
  }
  window.addEventListener('click', onHealthOsIntakeClick, true);

  // FIX 12: robust syncAnonId \u2014 handles ready-race, late analytics.js load,
  // and the case where analytics.js never loads (polling cap = 5s).
  var _syncPolling = false;
  function syncAnonId() {
    var id = getCookie('eden_anonymous_id') || getCookie('eden_anon_id');
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
    if (trySync()) { _syncPolling = false; return; }
    if (_syncPolling) return;
    _syncPolling = true;
    if (window.analytics && window.analytics.on) {
      try { window.analytics.on('ready', function() { trySync(); }); } catch(e) {}
    }
    var _attempts = 0;
    var _t = setInterval(function() {
      if (trySync() || ++_attempts >= 50) {
        _syncPolling = false;
        clearInterval(_t);
      }
    }, 100);
  }

  // Stable user/order identity is server-authoritative. Do not intercept
  // analytics.identify or send browser-supplied stable identifiers to the edge.

  function ensureSegmentContinuity() {
    syncAnonId();
  }

  var _healthOsHandoffArrivalSent = false;
  function emitHealthOsHandoffArrival() {
    if (_healthOsHandoffArrivalSent || window.location.hostname !== 'app.eden.health') return;
    if (!pageUrlHasAttribution(String(window.location.href || '')) && !getCookie('__Secure-eden_internal_handoff')) return;
    _healthOsHandoffArrivalSent = true;
    var ids = resolveIds();
    postJSON('https://collect.eden.health/collect', {
      type: 'track',
      event: 'HealthOS Handoff Arrived',
      anonymousId: ids.anonId || null,
      properties: {
        page_url: String(window.location.href || ''),
        landing_page: String(window.location.href || ''),
        edge_event_source: 'healthos_handoff_browser',
      },
      context: { page: { url: String(window.location.href || ''), path: String(window.location.pathname || '') } },
    });
  }

  // Consent managers can delay analytics.js until well after the initial five-
  // second bootstrap window. Re-run the id/hook contract around the user and
  // lifecycle signals that commonly follow consent without polling forever.
  document.addEventListener('click', function() {
    setTimeout(ensureSegmentContinuity, 0);
    setTimeout(ensureSegmentContinuity, 1500);
  }, true);
  window.addEventListener('focus', ensureSegmentContinuity);

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
  window.addEventListener('hashchange', function() { preserveBrowserOnlyAttribution(false); });
  window.addEventListener('popstate', function() { preserveBrowserOnlyAttribution(true); });

  try {
    ['pushState', 'replaceState'].forEach(function(method) {
      var original = window.history && window.history[method];
      if (typeof original !== 'function') return;
      window.history[method] = function() {
        // Webflow/VWO may synchronously clean the paid query. Start preserving
        // the outgoing attributed URL before the mutation so an immediate CTA
        // sees an active durability barrier even after location.search is gone.
        var outgoingPageUrl = String(window.location.href || '');
        if (pageUrlHasAttribution(outgoingPageUrl)) {
          preserveAttribution(null, outgoingPageUrl);
        }
        var pageUrlAtMutation = null;
        try {
          if (arguments.length > 2 && arguments[2] !== null && arguments[2] !== undefined) {
            pageUrlAtMutation = new URL(String(arguments[2]), window.location.href).toString();
          }
        } catch(e) {}
        var result = original.apply(this, arguments);
        if (pageUrlAtMutation && pageUrlHasAttribution(pageUrlAtMutation)) {
          preserveAttribution(null, pageUrlAtMutation);
        } else {
          setTimeout(function() { preserveBrowserOnlyAttribution(true); }, 0);
        }
        return result;
      };
    });
  } catch(e) {}

  // Keep long-lived app tabs admitted and recover cleanly after sleep/throttle.
  // Mutation calls also retry once after a 401 via this same safe bootstrap.
  setInterval(function() { refreshBrowserCapability().catch(function(){}); }, 60 * 60 * 1000);
  window.addEventListener('pageshow', function() {
    refreshBrowserCapability().catch(function(){});
    ensureSegmentContinuity();
  });
  document.addEventListener('visibilitychange', function() {
    if (document.visibilityState === 'visible') {
      refreshBrowserCapability().catch(function(){});
      ensureSegmentContinuity();
    }
  });

  syncAnonId();
  emitHealthOsHandoffArrival();
  preserveBrowserOnlyAttribution(false);
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', syncAnonId);
  }

})();
<\/script>`;
var INTERNAL_HANDOFF_CLEANUP_SCRIPT = `<script>
(function() {
  'use strict';
  try {
    var current = new URL(window.location.href);
    var nestedTransportKeys = ${JSON.stringify(QUERY_PARAM_NESTED_CONTAINER_KEYS)};
    function normalizedTransportKey(rawKey) {
      var currentKey = String(rawKey || '').trim();
      for (var index = 0; index < 3; index += 1) {
        try {
          var decoded = decodeURIComponent(currentKey);
          if (decoded === currentKey) break;
          currentKey = decoded;
        } catch(e) { break; }
      }
      return currentKey.replace(/^(?:amp;)+/i, '').replace(/^[&?]+/, '').toLowerCase();
    }
    function isTransportKey(rawKey) {
      var normalized = normalizedTransportKey(rawKey);
      // Remove only the opaque assertion. Keep the governed Google/UTM values
      // through the first HealthOS page and native tracker observation; the
      // Worker separately marks them as transported and prevents pointer/KV
      // promotion.
      return normalized === '${INTERNAL_HANDOFF_QUERY_PARAM}';
    }
    function serializeNestedUrl(parsed, raw) {
      if (/^[a-z][a-z0-9+.-]*:/i.test(raw)) return parsed.toString();
      if (raw.indexOf('//') === 0) return '//' + parsed.host + parsed.pathname + parsed.search + parsed.hash;
      if (raw.indexOf('?') === 0) return parsed.search + parsed.hash;
      if (raw.indexOf('/') === 0) return parsed.pathname + parsed.search + parsed.hash;
      var relativePath = parsed.pathname.indexOf('/') === 0 ? parsed.pathname.slice(1) : parsed.pathname;
      if (raw.indexOf('./') === 0) return './' + relativePath + parsed.search + parsed.hash;
      return relativePath + parsed.search + parsed.hash;
    }
    function cleanHash(parsed, depth) {
      var rawHash = String(parsed.hash || '');
      if (!rawHash || rawHash.indexOf('=') === -1) return false;
      var fragment = rawHash.slice(1);
      var queryOffset = fragment.indexOf('?');
      var prefix = queryOffset >= 0 ? fragment.slice(0, queryOffset) : '';
      var rawQuery = queryOffset >= 0 ? fragment.slice(queryOffset + 1) : fragment;
      var params = new URLSearchParams(rawQuery);
      var changed = cleanSearchParams(params, depth + 1);
      if (!changed) return false;
      var query = params.toString();
      parsed.hash = prefix ? '#' + prefix + (query ? '?' + query : '') : (query ? '#' + query : '');
      return true;
    }
    function cleanNestedValue(value, depth) {
      var raw = String(value || '').trim();
      if (!raw) return { changed: false, value: raw };
      if (depth > 6) return { changed: true, value: '' };
      try {
        var parsed = raw.indexOf('?') === 0
          ? new URL('https://app.eden.health/' + raw)
          : new URL(raw, 'https://app.eden.health');
        var searchChanged = cleanSearchParams(parsed.searchParams, depth + 1);
        var hashChanged = cleanHash(parsed, depth + 1);
        if (!searchChanged && !hashChanged) return { changed: false, value: raw };
        return { changed: true, value: serializeNestedUrl(parsed, raw) };
      } catch(e) {
        return { changed: false, value: raw };
      }
    }
    function cleanSearchParams(params, depth) {
      if (!params) return false;
      var changed = false;
      Array.from(params.keys()).forEach(function(rawKey) {
        if (isTransportKey(rawKey)) {
          params.delete(rawKey);
          changed = true;
          return;
        }
        var normalized = normalizedTransportKey(rawKey);
        if (nestedTransportKeys.indexOf(normalized) === -1) return;
        var nested = cleanNestedValue(params.get(rawKey), depth + 1);
        if (!nested.changed) return;
        if (nested.value) params.set(rawKey, nested.value);
        else params.delete(rawKey);
        changed = true;
      });
      return changed;
    }
    var queryKeys = Array.from(current.searchParams.keys());
    if (!queryKeys.some(function(rawKey) { return normalizedTransportKey(rawKey) === '${INTERNAL_HANDOFF_QUERY_PARAM}'; })) return;
    cleanSearchParams(current.searchParams, 0);
    cleanHash(current, 0);
    window.history.replaceState(window.history.state, document.title, current.pathname + current.search + current.hash);
  } catch(e) {}
})();
<\/script>`;
var eden_analytics_worker_default = {
  async queue(batch, env, ctx) {
    await processAdClickMemoryQueueBatch(batch, env);
  },
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);
      if (url.pathname === "/live") {
        return jsonResponse({
          ok: true,
          live: true,
          ready: false,
          worker: "eden-analytics",
          version: PIPELINE_VERSION,
          release_revision: RELEASE_REVISION,
          ts: nowUTC()
        });
      }
      if (["/ready", "/eden-health-check"].includes(url.pathname)) {
        const conversionCoordinatorHealth = await probeConversionCoordinatorHealth(env);
        const readiness = evaluateWorkerReadiness(env, conversionCoordinatorHealth);
        return jsonResponse({
          ok: readiness.ready,
          live: true,
          ready: readiness.ready,
          collection_mode: "on",
          readiness_missing: readiness.missing,
          worker: "eden-analytics",
          version: PIPELINE_VERSION,
          release_revision: RELEASE_REVISION,
          enrichment_version: ENRICHMENT_VERSION,
          enrichment_mode: String(env.EDEN_HEALTH_TRACKING_ENRICHMENT_MODE || "off"),
          enrichment_canary_param: ENRICHMENT_CANARY_PARAM,
          ts: nowUTC(),
          kv: !!env.GCLID_KV,
          segment_write_key_configured: !!env.SEGMENT_WRITE_KEY,
          server_secret_configured: !!env.SERVER_API_SECRET,
          server_previous_secret_configured: !!env.SERVER_API_SECRET_PREVIOUS,
          privacy_ledger_hmac_secret_configured: !!env[PRIVACY_LEDGER_HMAC_SECRET_ENV],
          privacy_ledger_previous_hmac_secret_configured: !!env[PRIVACY_LEDGER_HMAC_PREVIOUS_SECRET_ENV],
          privacy_ledger_kv_configured: !!env.PRIVACY_LEDGER_KV,
          browser_capability_hmac_secret_configured: !!env[BROWSER_CAPABILITY_SECRET_ENV],
          browser_capability_previous_secret_configured: !!env[BROWSER_CAPABILITY_PREVIOUS_SECRET_ENV],
          browser_capability_enforcement_mode: normalizeBrowserCapabilityEnforcementMode(env),
          browser_segment_delivery_mode: String(env.EDEN_BROWSER_SEGMENT_DELIVERY_MODE || "async").trim().toLowerCase() === "sync" ? "sync" : "async",
          server_segment_delivery_mode: String(env.EDEN_SERVER_SEGMENT_DELIVERY_MODE || "async").trim().toLowerCase() === "sync" ? "sync" : "async",
          browser_capability_cookie: BROWSER_CAPABILITY_COOKIE_NAME,
          browser_capability_ttl_seconds: BROWSER_CAPABILITY_TTL_SECONDS,
          browser_capability_bootstrap_path: "/browser-capability",
          browser_capability_refresh_contract: "every successful browser mutation refreshes a v2 capability bound to the current Eden anonymous ID, session ID, browser host, and collect.eden.health; same-site same-origin-referer bootstrap may mint missing Worker-owned anonymous/session state, never accepts body-supplied ownership or stable identity, and the injected client retries one 401 or 409; shadow is temporary mint-before-enforce only",
          browser_track_event_allowlist_mode: "default_allow_original_names_segment_governed",
          browser_track_unknown_event_policy: "preserve every bounded track/page/screen name, useful non-secret properties, nested product arrays, and context; namespace bounded producer message IDs to the Eden anonymous owner before Segment dedupe and retain only a raw-free producer-ID hash for reconciliation; Segment owns schema governance; source_type=client plus browser_event_authority=provisional_observation and edge-owned anonymous/session/click provenance distinguish browser outcome observations from authenticated server and warehouse truth",
          consent_denial_authority: "affirmative first-party action, opt-out flag, user choice, or denied_by_user basis only; producer false defaults, raw GPC, inferred policy, and unsigned URL transport are diagnostic",
          consent_candidate_precedence: "evaluate every current cookie/body candidate; any affirmative denial wins, otherwise any first-party action with an explicit positive advertising signal resumes current tracking",
          attribution_dependency_failure_policy: "explicit current denial and the first-party denial marker suppress attribution; privacy-ledger read or clear infrastructure failures are logged but default or affirmative-allow tracking continues",
          server_identity_claim_conflict_policy: "exactly one eden_identity_id anywhere in the bounded authenticated payload wins; multiple distinct Eden IDs quarantine all stable user claims and emit a value-free warning; namespace-typed source IDs are fallback only when Eden identity is absent",
          source_identity_namespace_contract: "source:user_id, source:patient_id, source:customer_id, and source:member_id are distinct namespaces even when their raw values match; only eden_identity_id is a global person key",
          stable_identifier_contract: "OS_purchase requires one charge transaction ID, directly or through canonical OS_purchase:<transaction_id> messageId; the internal conversion key remains transaction-authoritative while the outgoing Segment messageId is a deterministic Mixpanel-safe 34-byte hash; qualification/completion milestones require one real order_id; master and treatment IDs remain typed relationship evidence, never order or idempotency fallbacks; all stable server identifiers must be bounded scalar strings or safe integer numbers and malformed/conflicting claims fail or quarantine before delivery",
          server_event_time_contract: "validated authenticated producer event time is preserved for Segment and immutable snapshots; edge receipt time is separate as edge_received_at",
          conversion_dedup_contract: "eden_conversion_dedup_v4 uses the strongly consistent stable-conversion-key Durable Object record as canonical delivery state; OS_purchase is permanently keyed by its payment transaction and qualification/completion milestones by their real order ID, so later relationship IDs cannot change scope; synchronous Segment acknowledgement and an exact raw-free Queue/KV persistence intent precede final acknowledgement; an ambiguous base replays its stored exact Segment payload bytes, current truth uses a separate idempotent enrichment, and changed ambiguous non-conversion enrichment truth supersedes under a new idempotent enrichment ID",
          conversion_privacy_owner_contract: "conversion coordinator ownership is resolved before privacy-ledger mutation; a conflicting user, order, or anonymous retry may control only its immediate request and cannot add, propagate, or clear durable state for either identity set",
          conversion_coordinator_configured: conversionCoordinatorHealth.configured,
          conversion_coordinator_health_ok: conversionCoordinatorHealth.ok,
          conversion_coordinator_schema_version: conversionCoordinatorHealth.schemaVersion,
          conversion_coordinator_storage_readable: conversionCoordinatorHealth.storageReadable,
          conversion_event_semantics: "OS_purchase, OS_qualified_first_order, order_completed, and reorder_completed are distinct business milestones; syntactic aliases normalize before dedupe, and each canonical milestone has one base delivery per stable conversion key",
          conversion_serialization_contract: "a Durable Object lease keyed by the SHA-256 stable conversion key serializes every canonical conversion milestone for that key; each distinct milestone keeps its own durable record, while syntactic aliases normalize to one milestone before the lease; staged delivery distinguishes unacknowledged Segment delivery, Segment-acknowledged pending persistence, and final acknowledgement",
          conversion_ledger_authority: "ConversionCoordinator Durable Object per stable conversion key is canonical; raw-free dedup:v4:{event}:{scope_hash} in GCLID_KV is the canonical one-year compatibility/observability mirror; temporary real-order raw rows are dual-written only for v5.55 migration, and still-live v5.55 overloaded one-day rows are read once into the corrected scope but are never delivery authority",
          conversion_unknown_commit_retry_contract: "network, timeout, or HTTP 5xx ambiguity keeps the exact bounded Segment payload, raw-free attempted signal hashes, payload fingerprint, timestamp, and delivery kind; the stored base or unchanged enrichment is replayed byte-identically, current changed truth uses a separate stable enrichment, and missing required prior state returns 409 conversion_retry_state_incomplete_or_regressed",
          conversion_coordinator_lease_ttl_ms: CONVERSION_COORDINATOR_LEASE_TTL_MS,
          conversion_segment_timeout_ms: CONVERSION_SEGMENT_TIMEOUT_MS,
          conversion_dedup_ttl_seconds: KV_DEDUP_TTL,
          google_click_id_validation_contract: "upload-grade gclid/gbraid/wbraid/dclid values preserve case but require bounded non-sentinel non-control plausibility on landing URLs and authenticated server envelopes; recognized semicolon tails are separated, conflicting repeats are rejected, nested URL/body parsing is bounded recursively, and raw-free field/reason/source diagnostics remain observable",
          attribution_cookie_click_age_contract: "click_first_observed_at is immutable and controls the 30-day raw click-evidence age; last_seen_at is separate, repeat activity cannot extend upload evidence indefinitely, and a different fresh primary click replaces recovered upload-grade IDs in the active touch cookie instead of reactivating them under one clock",
          mutation_auth_contract: "v6: server-collect requires X-Eden-Server-Secret and is the only authoritative conversion and stable user/order identity lane; collect/identify/preserve require an allowed browser origin plus a short-lived v2 capability bound to the exact current Eden anonymous ID, session ID, browser host, and collect.eden.health; same-site CORS plus same-origin referer bootstrap may mint missing Worker-owned anonymous/session state but never accepts body-supplied ownership or creates stable identity; browser conversion observations are retained for reconciliation while credentials, stable identity, and direct contact claims remain scrubbed; missing secret configuration fails closed; shadow is temporary mint-before-enforce only",
          max_json_body_bytes: MAX_JSON_BODY_BYTES,
          attribution_model: "first paid touch with organic/direct fallback; current and later paid touches remain separate immutable observations",
          first_touch_atomicity_contract: "the strongly consistent first-touch record is one atomic observation; later touches cannot fill missing campaign fields, while same-observation replay/enrichment requires the same captured_at and an owner-scoped observation SHA-256",
          coverage: "target ~98% meaningful attribution with paid-click evidence plus owner-scoped cookie, KV, order, and user continuity",
          google_upload_evidence_primary_order: AD_CLICK_CLASS_A_GOOGLE_PARAMS,
          google_upload_evidence_preserve_all_valid_fields: true,
          webflow_client_bootstrap_contract: "edge capture and first-party owner/session creation occur before the Webflow origin fetch; a new owner-scoped pointer becomes active only after strongly consistent reservation, Queue custody, and commit, all before the origin fetch; injected client syncs eden_anonymous_id through the active browser analytics adapter, recovers after delayed loading, preserves browser-only fragment/History API click evidence, awaits durable pre-handoff persistence, and retains Google/UTM transport on the first HealthOS request for native trackers independently of later server persistence",
          internal_handoff_pointer_policy: "a short-lived signed assertion bound to the Eden anonymous/session owner, durable pointer, and exact app.eden.health intake destination is required before a transported conflicting query can be quarantined as queue-only diagnostic evidence; fresh browser evidence must first reach Queue plus pointer KV, while a cleaned-URL handoff may reuse an already owner-validated pointer record after owner attribution KV is re-proven; after exact URL validation it continues only in an HttpOnly collector cookie and never authorizes a fresh page landing; it cannot write pointer or reverse KV, emit identity links, annotate events, or overwrite the selected pointer; fresh external landings still win",
          internal_handoff_assertion_query_param: INTERNAL_HANDOFF_QUERY_PARAM,
          internal_handoff_continuation_cookie: INTERNAL_HANDOFF_COOKIE_NAME,
          internal_handoff_assertion_ttl_seconds: INTERNAL_HANDOFF_TTL_SECONDS,
          internal_handoff_assertion_secret_reuse_contract: "domain-separated HMAC signed with the rotating browser capability secret; no raw Google click ID or PII is embedded",
          internal_handoff_assertion_version_contract: "v2 binds full and per-click transport fingerprints; short-lived legacy v1 assertions remain exact-destination compatible only for a clean destination or exactly one primary Google click id matching the owned pointer hash, while changed/additional/unverifiable transport stays event-native and v1 cannot authorize cross-route partial continuation",
          internal_handoff_destination_fingerprint_contract: "exact app.eden.health intake origin, path, and sorted non-attribution business query are bound; the opaque assertion and governed Google/UTM transport are excluded from the pointer-selection fingerprint, but only the assertion is stripped before the origin; product, referral, and other business parameters remain bound",
          internal_handoff_transport_fingerprint_contract: "the signed assertion contains a raw-value-free full-transport SHA-256 plus per-click field hashes; a continuation cookie may match a non-conflicting retained click subset recursively across a later intake SPA route after URL cleanup, while a conflicting or different fresh identifier fails and remains event-native",
          internal_handoff_transport_capture_contract: "governed click IDs and UTMs remain on the first HealthOS request for application-native Google, Segment, and Northbeam observation; signed handoff validation keeps the selected owner-bound pointer authoritative, records a conflicting carried query as diagnostic-only, and labels collector copies as transported_internal_handoff",
          cross_domain_bridge: "same-registrable-domain .eden.health browser continuity uses Eden-owned anonymous/session context; authenticated server-collect attaches stable user/order bridges through strongly consistent coordinator state so the original owner-scoped pointer can survive anonymous/session rotation without making a Google ID a person key; raw ad IDs and _gcl_au are evidence fields, never recovery keys; no tryeden.com or edenrx.co cookie continuity is claimed",
          google_click_bridge_params: GOOGLE_CLICK_BRIDGE_PARAMS,
          google_click_bridge_retired_params: RAW_AD_ID_BRIDGE_RETIRED_PARAMS,
          ad_click_memory_mode: normalizeAdClickMemoryMode(env),
          ad_click_kv_index_mode: normalizeAdClickKVIndexMode(env),
          ad_click_kv_resolver_mode: normalizeAdClickKVResolverMode(env),
          ad_click_kv_resolver_requested_mode: requestedAdClickKVResolverMode(env),
          ad_click_kv_reverse_read_active: shouldReadFullAdClickKVResolver(env),
          ad_click_kv_resolver_policy_version: AD_CLICK_RESOLUTION_POLICY_VERSION,
          ad_click_kv_resolver_implemented: AD_CLICK_FULL_REVERSE_KV_RESOLVER_IMPLEMENTED,
          ad_click_kv_resolver_contract: "pointer_only default; full reverse-KV reads require EDEN_AD_CLICK_KV_RESOLVER_CONTRACT_ACCEPTED=true, resolver provenance, shared-click QA, rollback proof, and dbt upload-safety gates; v2 resolver reads first-party adclick:v2:{anon|session|user|order} keys only — click-value/_gcl_au reverse keys and quarantined v1 indexes are never resolver inputs",
          ad_click_kv_reverse_key_schema: `${AD_CLICK_KV_REVERSE_PREFIX}{anon|session|user|order}:... first-party only; click-value and _gcl_au reverse keys are not written`,
          ad_click_id_scope_contract: "linkable gclid/gbraid/wbraid/dclid ad_click_ids are scoped to Eden first-party context; evidence observed without any Eden owner is instance_random diagnostic-only, never written to pointer/reverse KV, never injected into browser/server events, and never linked; raw click hashes remain globally comparable for downstream replay/conflict QA",
          ad_click_kv_full_index_active: shouldWriteFullAdClickKVIndexes(env),
          ad_click_kv_pointer_write_active: shouldWriteAdClickMemoryKV(env),
          ad_click_reverse_kv_retention_mode: normalizeAdClickReverseKVRetentionMode(env),
          ad_click_reverse_kv_expiration_ttl_seconds: adClickReverseKvTtlSeconds(env),
          ad_click_kv_full_index_warning: shouldWriteFullAdClickKVIndexes(env) ? "EDEN_AD_CLICK_KV_INDEX_MODE=full is live-gated; reverse keys are hashed and require BigQuery/dbt parity, shared-click QA, rollback proof, and upload-safety monitoring" : undefined,
          ad_click_kv_resolver_warning: requestedAdClickKVResolverMode(env) === "full" && !shouldReadFullAdClickKVResolver(env) ? "EDEN_AD_CLICK_KV_RESOLVER_MODE=full requested but reverse-KV reads are blocked until EDEN_AD_CLICK_KV_RESOLVER_CONTRACT_ACCEPTED=true" : undefined,
          ad_click_kv_first_paid_consistency: normalizeAdClickKVIndexMode(env) === "full" ? "first_paid reverse indexes use best-effort KV put-if-absent/eventual consistency; BigQuery snapshot/link tables remain canonical" : undefined,
          ad_click_pointer_cookie: AD_CLICK_POINTER_COOKIE_NAME,
          ad_click_pointer_record_schema: AD_CLICK_POINTER_RECORD_SCHEMA_VERSION,
          ad_click_pointer_integrity: "Durable Object state plus first-party owner hash is authoritative and AD_CLICK_KV is a cache; a reservation validates owner and revocation without activating a pointer, Queue custody precedes commit/cache publication, stable user/order bridges are authenticated-server-only, and legacy unowned, dangling, revoked, copied, and stable-identity-mismatched pointers are rejected",
          ad_click_pointer_mutation_contract: "one ConversionCoordinator object per ad_click_id serializes reserve, commit, cancel, upsert, and revocation; only a committed Queue-custodied reservation becomes active, idempotent commit replay is bounded, revocation is immutable, and a stale cache write cannot follow it",
          ad_click_snapshot_identity: "append-only immutable observation snapshots; ordinary native builds get a unique stable-in-envelope snapshot_id, authenticated conversion retries derive it from the stable delivery key, Queue retries reuse it, multiple distinct observations may share ad_click_id, and recovered ordinary events emit no snapshot",
          ad_click_identity_link_contract: "v2 link_id is a stable relationship key scoped by ad_click_id plus typed endpoints; repeated delivery reuses it, mutable observation provenance may advance, and a later ad click can never reuse the prior click relationship key",
          canonical_anonymous_id: "eden_anonymous_id; eden_anon_id is dual-written/read as a temporary legacy alias and conflicts prefer the canonical value",
          landing_url_fragment_policy: "the authenticated injected client supplies browser-only fragment/History API URLs; governed evidence is extracted, then fragments are removed before Segment/KV/Queue persistence",
          advertising_denial_ledger_schema: ATTRIBUTION_DENIAL_SCHEMA_VERSION,
          advertising_denial_ledger_keying: "HMAC-SHA256 Eden anonymous/session/user/order identities only; Google click IDs are never privacy-ledger join keys",
          advertising_denial_ttl_seconds: ATTRIBUTION_DENIAL_TTL,
          advertising_denial_marker_cookie: ATTRIBUTION_DENIAL_COOKIE_NAME,
          advertising_denial_pointer_policy: "explicit denial immediately sets a first-party marker and revokes an owned ad-click pointer; marker-denied requests heal the durable ledger; explicit allow immediately restores current tracking and clears the marker even when durable tombstone cleanup must be retried, and it never unrevokes the old pointer",
          ad_click_observability_contract: "source_pipeline_version is stamped on queue envelopes and raw snapshot/link/error rows; release verification requires v5.56 to be the sole 100% deployment and never relies on production version overrides or percentage splits",
          ad_click_snapshot_queue_configured: !!getAdClickSnapshotQueue(env),
          ad_click_kv_configured: !!getAdClickMemoryKV(env),
          ad_click_memory: "draft/flag-gated durable first-party ad-click memory; off unless EDEN_AD_CLICK_MEMORY_MODE enables it",
          first_party_cookie: "eden_attr \u2014 browser-readable migration bridge, 30d; retire only after HealthOS opaque-envelope parity",
          identify_flow: "browser /identify is an anonymous consent/session compatibility no-op; it never accepts or forwards a stable user, order, group, email, phone, name, or address; authenticated /server-collect is the sole stable identity authority",
          alias_guard: "browser alias and group mutations are disabled; browser traffic cannot write alias:fired or stable id:link records",
          self_identify: "disabled for browser traffic; authenticated server producers may supply stable user/order identity directly without a browser identity mutation",
          email_kv: "legacy email:user:{userId} browser-derived hashes are quarantined and never read; authenticated producers must send enhanced identity directly on /server-collect",
          legacy_browser_identity_kv_recovery: "disabled: id:link, attr:user, attr:order, and email:user are never resolver inputs",
          trusted_server_attribution_kv: "authenticated /server-collect writes and reads attr:server:v1:{user|order}; browser routes cannot address these namespaces",
          browser_stable_identity_contract: "collect preserves bounded track/page/screen names and useful non-secret properties, including browser-observed purchase signals; identify is a no-op; collect/identify/preserve remove traits plus stable user/order/group/contact/explicit-person-name/address aliases before Segment, KV, or Queue handling; server and warehouse reconciliation remain authoritative",
          server_stable_identity_contract: "only authenticated /server-collect may attach stable user/order identity and persist purchase attribution under trusted user/order keys; eden_identity_id is the preferred canonical user key, source user/patient/customer/member ids are namespace-typed fallback keys, and only same-namespace repeated conflicts are quarantined",
          order_id_sources: "order_id | orderId | master_id | ecommerce.transaction_id | ecommerce.treatmentId",
          resolve_attribution: "v5.56 \u2014 authenticated event anon > trusted-server user > trusted-server order > cookie; legacy browser identity namespaces and every raw ad ID remain non-resolver evidence",
          gpc_policy: "CookieYes/eden_consent_state is the attribution privacy authority; raw GPC is recorded diagnostically unless CookieYes state says denied",
          fixes: "v5.56 \u2014 ownerless Segment suppression, primary-evidence-stable scoped ad_click_id, browser fragment/SPA capture, consent-delayed Segment resync, sliding sessions, signed browser mutation capability, HMAC privacy ledger plus immediate denial marker, owner-bound pointer v2, and append-only observation snapshots",
          channels: CLICK_ID_CONFIG.map((c) => c.label)
        }, readiness.ready ? 200 : 503);
      }
      // AnalyticsBrowser appends its method suffix to apiHost. The Eden app's
      // apiHost=app.eden.health/collect therefore produces /collect/t,
      // /collect/p, /collect/s, and /collect/i. Keep these aliases behind the
      // exact same v5.56 browser capability, privacy, and sanitization gates.
      const browserCollectPaths = /* @__PURE__ */ new Set([
        "/collect", "/collect/t", "/collect/p", "/collect/s",
        "/collect/v1/t", "/collect/v1/p", "/collect/v1/s"
      ]);
      const browserIdentifyPaths = /* @__PURE__ */ new Set([
        "/identify", "/collect/i", "/collect/a", "/collect/g",
        "/collect/v1/i", "/collect/v1/a", "/collect/v1/g"
      ]);
      if (request.method === "OPTIONS") {
        const browserMutationPaths = new Set([
          ...browserCollectPaths,
          ...browserIdentifyPaths,
          "/preserve-attribution"
        ]);
        const origin = request.headers.get("Origin") || "";
        if (!browserMutationPaths.has(url.pathname) || !isAllowedOrigin(origin)) {
          return new Response("Forbidden", { status: 403, headers: { "Cache-Control": "no-store" } });
        }
        return new Response(null, { status: 204, headers: corsHeaders(origin) });
      }
      if (url.pathname === "/browser-capability" && request.method === "GET")
        return handleBrowserCapabilityBootstrap(request, env);
      // Mutation endpoints must route through their own auth/body/consent gates
      // before any bot, synthetic-monitor, or static passthrough predicate. Those
      // predicates are caller-controlled hints and can never be authentication.
      if (url.pathname === "/preserve-attribution" && request.method === "POST")
        return refreshBrowserCapabilityOnSuccess(await handlePreserveAttribution(request, env, ctx), env, url.pathname, request);
      if (browserCollectPaths.has(url.pathname) && request.method === "POST")
        return refreshBrowserCapabilityOnSuccess(await handleCollect(request, env, ctx, url), env, url.pathname, request);
      if (url.pathname === "/server-collect" && request.method === "POST")
        return handleServerCollect(request, env, ctx);
      if (browserIdentifyPaths.has(url.pathname) && request.method === "POST")
        return refreshBrowserCapabilityOnSuccess(await handleIdentify(request, env, ctx), env, url.pathname, request);
      if (isBot(request))
        return fetch(requestForOrigin(request));
      if (isSyntheticMonitor(request, url)) {
        debugLog(env, "synthetic monitor blocked");
        return fetch(requestForOrigin(request));
      }
      if (isStaticAsset(url))
        return fetch(requestForOrigin(request));
      // Await the page path inside this try/catch. Returning the Promise would
      // let an asynchronous cookie/parser rejection escape the controlled
      // fallback boundary.
      return await handlePageRequest(request, env, ctx, url);
    } catch (err) {
      console.error("[eden-analytics] unhandled error:", err);
      try {
        const path = new URL(request.url).pathname;
        if (path === "/server-collect" || path === "/preserve-attribution"
          || path === "/collect" || path.startsWith("/collect/") || path === "/identify") {
          return new Response(JSON.stringify({ ok: false, error: "collector_internal_error" }), { status: 500, headers: { "Content-Type": "application/json" } });
        }
      } catch {}
      // Never forward the opaque handoff assertion. Keep governed click/UTM
      // evidence available to HealthOS/native trackers even when the edge path
      // faults; losing the only observable identifier would be worse than a
      // duplicate diagnostic observation.
      return fetch(requestForOrigin(request));
    }
  }
};
async function handlePageRequest(request, env, ctx, url) {
  const gpcOptOut = isGpcOptOut(request);
  const referrer = sanitizeUrlString(request.headers.get("Referer") || "");
  const existingAnonId = readCanonicalAnonymousId(request);
  const existingSession = readCookie(request, "eden_session_id");
  const isNewVisitor = !existingAnonId;
  const isNewSession = !existingSession;
  const anonId = existingAnonId || crypto.randomUUID();
  const session = existingSession || `${crypto.randomUUID()}_${Date.now()}`;
  const attributionPermission = await resolveAttributionPermissionWithDurableState(env, request, null, { anonId, session });
  const canUseAttribution = attributionPermission.allowed;
  const hasPageInternalHandoffAssertion = !!getCanonicalUrlParam(url, INTERNAL_HANDOFF_QUERY_PARAM, { includeNested: false, includeHash: false });
  const pageInternalHandoff = canUseAttribution
    ? await verifyInternalHandoffAssertion({ env, request, destinationUrl: url, anonId, session })
    : { valid: false, reason: "attribution_suppressed" };
  if (canUseAttribution && !pageInternalHandoff.valid && hasPageInternalHandoffAssertion) {
    debugLog(env, `internal handoff page rejected: ${pageInternalHandoff.reason || "unknown"}`);
  }
  const pageEnrichmentState = resolveEnrichmentState(env, request);
  const extendedPageEnrichmentEnabled = pageEnrichmentState.enabled;
  const clickIds = extractClickIds(url, request, extendedPageEnrichmentEnabled);
  const utms = extractUTMs(url);
  const preAuth = canUseAttribution ? extractPreAuthAttribution(request) : null;
  const organic = detectOrganic(referrer);
  const freshPageClickEvidence = classifyGoogleClickEvidence(clickIds);
  const recoveredPageClickEvidence = classifyGoogleClickEvidence(preAuth || {});
  const recoveredForActiveTouch = { ...preAuth || {} };
  let freshReplacesRecoveredClick = false;
  if (freshPageClickEvidence.has_primary_click_evidence) {
    const freshType = freshPageClickEvidence.primary_click_id_type;
    const recoveredType = recoveredPageClickEvidence.primary_click_id_type;
    const recoveredSameClick = recoveredType === freshType
      && recoveredPageClickEvidence.click_evidence?.[recoveredType] === freshPageClickEvidence.click_evidence?.[freshType];
    freshReplacesRecoveredClick = !recoveredSameClick;
    if (freshReplacesRecoveredClick) {
      // A fresh primary click starts a new active touch. Do not carry an older
      // click or any of its UTMs/ad metadata beside the new click under one
      // shared age clock. First-touch history remains immutable in owner KV and
      // snapshots; only the active-touch view is replaced here.
      clearActivePaidTouchFields(recoveredForActiveTouch);
    }
  }
  const fullAttribution = {
    ...organic || {},
    ...recoveredForActiveTouch,
    ...utms || {},
    ...clickIds,
    ...referrer ? { attribution_referrer: referrer } : {},
    ...url ? { landing_page: sanitizeAdClickLandingUrl(url).toString() } : {},
    ...freshReplacesRecoveredClick ? { _click_first_observed_at: Date.now() } : {}
  };
  // Event-native view excludes eden_pre_auth cookie values: those are same-device
  // recovery, and ad-click resolution provenance must say so.
  const pageEventNativeAttribution = {
    ...organic || {},
    ...utms || {},
    ...clickIds,
    ...referrer ? { attribution_referrer: referrer } : {},
    ...url ? { landing_page: sanitizeAdClickLandingUrl(url).toString() } : {}
  };
  const hasAttribution = Object.keys(fullAttribution).length > 0;
  if (hasAttribution && env.GCLID_KV && canUseAttribution && !pageInternalHandoff.valid) {
    const writes = [
      storeAttribution(env, KV_ANON_PREFIX + anonId, fullAttribution).catch((err) => console.error("[eden-analytics] KV anon store error:", err))
    ];
    ctx.waitUntil(Promise.all(writes));
  }
  const adClickMemory = canUseAttribution ? await safeBuildAdClickMemoryCandidate({
    request,
    env,
    url,
    anonId,
    session,
    attribution: fullAttribution,
    sourceType: "page",
    eventName: "page_view",
    linkReason: "first_request_capture",
    eventNativeAttribution: pageEventNativeAttribution,
    internalHandoff: pageInternalHandoff
  }, "page") : null;
  let adClickPersistence = null;
  if (adClickMemory) {
    try {
      adClickPersistence = await persistAdClickMemory(env, adClickMemory);
    } catch (err) {
      console.error("[eden-analytics] ad-click page persist error:", err);
    }
  }
  let response;
  try {
    response = await fetch(requestForOrigin(request));
  } catch (error) {
    // Preserve the already-created owner/session/pointer cookies even when the
    // Webflow origin connection rejects instead of returning an HTTP 5xx. The
    // Queue/KV tasks were scheduled before this fetch and remain independent.
    console.error(JSON.stringify({
      worker: "eden-analytics",
      event: "origin_fetch_rejected",
      host: url.hostname,
      reason: String(error?.message || "origin_fetch_failed").slice(0, 120)
    }));
    response = new Response("Upstream origin unavailable", {
      status: 502,
      headers: {
        "Content-Type": "text/plain; charset=utf-8",
        "Cache-Control": "no-store"
      }
    });
  }
  const contentType = response.headers.get("content-type") || "";
  const headers = new Headers(response.headers);
  if (isNewVisitor || anonymousCookieAliasesNeedSync(request)) {
    headers.append("Set-Cookie", buildAnonCookie(anonId, url));
    headers.append("Set-Cookie", buildLegacyAnonCookie(anonId, url));
  }
  // Session lifetime is inactivity-based. Refresh the 30-minute cookie on
  // every governed page request instead of expiring an active long intake tab
  // exactly 30 minutes after its first request.
  headers.append("Set-Cookie", buildSessionCookie(session, url));
  if (preAuth)
    headers.append("Set-Cookie", clearCookie("eden_pre_auth", url));
  appendAttributionPermissionCookies(headers, url, attributionPermission);
  if (hasAttribution && canUseAttribution && !pageInternalHandoff.valid) {
    const attrCookieValue = buildAttrCookieValue(fullAttribution);
    if (attrCookieValue) {
      headers.append("Set-Cookie", buildAttrCookie(attrCookieValue, url));
      debugLog(env, "eden_attr set");
    }
  }
  if (pageInternalHandoff.valid && pageInternalHandoff.pointerRecord && !pageInternalHandoff.pointerRecordLag) {
    // Reissue the assertion-selected pointer on the app response. The incoming
    // Cookie header can snapshot the prior cross-subdomain value, and a
    // conflicting transported query is intentionally observation-only, so
    // neither is sufficient to leave the destination on the winning pointer.
    // Verification above already proves exact owner/session, destination,
    // transport, and durable backing-record authority; this writes no new
    // pointer state and cannot promote the transported query.
    headers.append("Set-Cookie", buildAdClickPointerCookie(pageInternalHandoff.pointerId, url, env));
  } else if (adClickMemory?.setPointerCookie && adClickPersistence?.pointer_committed === true
    && (!adClickMemory.snapshot || adClickPersistence.queue_enqueued === true)) {
    headers.append("Set-Cookie", buildAdClickPointerCookie(adClickMemory.ad_click_id, url, env));
  }
  if (pageInternalHandoff.valid && pageInternalHandoff.transport === "query_assertion") {
    headers.append("Set-Cookie", buildInternalHandoffContinuationCookie(pageInternalHandoff.token, pageInternalHandoff.exp));
  }
  if (env.SEGMENT_WRITE_KEY && isNewSession && hasAttribution && canUseAttribution && !pageInternalHandoff.valid) {
    ctx.waitUntil(
      fireFirstTouch(request, env, anonId, session, url, fullAttribution, utms, referrer, isNewVisitor, isNewSession).catch((err) => console.error("[eden-analytics] first_touch error:", err))
    );
  }
  const isEdenDomain = url.hostname === "app.eden.health" || url.hostname === "eden.health" || url.hostname === "www.eden.health";
  if (response.status === 200 && isEdenDomain && contentType.includes("text/html")) {
    try {
      headers.append("Set-Cookie", buildBrowserCapabilityCookie(await mintBrowserCapability(env, {
        anonId,
        session,
        browserHost: url.hostname
      })));
    } catch (error) {
      // Never take the public site down because a security binding is missing,
      // but deployment verification treats this as a hard release failure.
      console.error(JSON.stringify({ worker: "eden-analytics", event: "browser_capability_mint_failed", reason: String(error?.message || "unknown").slice(0, 120) }));
    }
    const cspHeader = response.headers.get("content-security-policy") || "";
    const nonceMatch = cspHeader.match(/nonce-([A-Za-z0-9+/=]+)/);
    const nonce = nonceMatch ? nonceMatch[1] : "";
    // Remove the opaque capability from browser history even when verification
    // fails. This cleanup removes only the assertion (and fail-closed over-depth
    // containers); fresh Google/UTM evidence stays visible to native trackers and
    // is captured above as event-native evidence rather than being discarded.
    const injectedClient = hasPageInternalHandoffAssertion
      ? `${INTERNAL_HANDOFF_CLEANUP_SCRIPT}${PREAUTH_SCRIPT}`
      : PREAUTH_SCRIPT;
    const script = nonce ? injectedClient.replace(/<script>/g, `<script nonce="${nonce}">`) : injectedClient;
    return new HTMLRewriter().on("head", { element(el) {
      el.prepend(script, { html: true });
    } }).transform(new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers
    }));
  }
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers
  });
}
__name(handlePageRequest, "handlePageRequest");
async function fireFirstTouch(request, env, anonId, session, url, clickIds, utms, referrer, isNewVisitor = false, isNewSession = true) {
  const cleanUrl = sanitizeAdClickLandingUrl(url).toString();
  const ua = request.headers.get("User-Agent") || "";
  const portal = url.hostname.includes("app.eden.health") ? "patient" : "marketing";
  const sessionId = session.split("_")[0];
  const organic = !utms && !hasAnyClickId(clickIds) && referrer ? detectOrganic(referrer) : null;
  const attribution = { ...utms || organic || {}, ...clickIds };
  if (!Object.keys(attribution).length && !referrer)
    return;
  const campaignProps = buildCampaignContext(attribution);
  await segmentPost(env.SEGMENT_WRITE_KEY, "track", {
    anonymousId: anonId,
    messageId: `first_touch_${anonId}_${sessionId}`,
    event: "first_touch",
    properties: {
      portal,
      page_path: url.pathname,
      page_url: cleanUrl,
      landing_page: cleanUrl,
      referrer: referrer || void 0,
      session_id: sessionId,
      session_key: sessionId,
      first_touch_scope: isNewVisitor ? "visitor" : "session",
      is_true_first_touch: !!isNewVisitor,
      is_new_visitor: !!isNewVisitor,
      is_new_session: !!isNewSession,
      device_type: isMobile(ua) ? "mobile" : "desktop",
      pipeline_version: PIPELINE_VERSION,
      ...privacyProperties(request),
      ...campaignProps,
      acquisition_channel: deriveAcquisitionChannel(campaignProps),
      attribution_source: campaignProps.utm_source || deriveClickIdSource(campaignProps),
      attribution_medium: campaignProps.utm_medium || void 0,
      attribution_campaign: campaignProps.utm_campaign || void 0
    },
    context: { campaign: campaignProps },
    timestamp: nowUTC()
  });
}
__name(fireFirstTouch, "fireFirstTouch");
async function handleCollect(request, env, ctx, url) {
  const origin = request.headers.get("Origin") || "";
  if (origin && !isAllowedOrigin(origin))
    return new Response("Forbidden", { status: 403 });
  const authFailure = await authorizeBrowserMutationRequest(request, env, "collect");
  if (authFailure) return authFailure;
  const parsedBody = await parseBoundedJsonRequest(request);
  if (parsedBody.response) return parsedBody.response;
  const body = parsedBody.value;
  const browserAdmission = sanitizeBrowserCollectorBody(body);
  if (!browserAdmission.allowed) {
    return new Response(JSON.stringify({ ok: false, error: browserAdmission.error }), {
      status: 422,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store", ...corsHeadersObj(origin) }
    });
  }
  sanitizeUploadGradeClickIdClaimsInObject(body, "browser_collect");
  const gpcOptOut = isGpcOptOut(request);
  if (!body.properties || typeof body.properties !== "object" || Array.isArray(body.properties)) {
    body.properties = {};
  }
  const cookieAnonId = readCanonicalAnonymousId(request);
  const existingSessionCookie = readCookie(request, "eden_session_id");
  // Webflow creates the legacy anonymous cookie before the Worker creates a
  // session. Preserve that owner and mint only the missing session. A session
  // without an anonymous owner remains invalid and fails closed.
  if (!cookieAnonId && existingSessionCookie) {
    return new Response(JSON.stringify({ ok: false, error: "browser_owner_cookie_required" }), {
      status: 409,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store", ...corsHeadersObj(origin) }
    });
  }
  const isNew = !cookieAnonId;
  const needsSessionBootstrap = !existingSessionCookie;
  const anonId = cookieAnonId || crypto.randomUUID();
  const generatedSessionRaw = needsSessionBootstrap ? `${crypto.randomUUID()}_${Date.now()}` : null;
  const effectiveSessionRaw = existingSessionCookie || generatedSessionRaw;
  body.anonymousId = anonId;
  delete body.anonymous_id;
  delete body.anonymoous_id;
  delete body.anonymous_Id;
  delete body.anonymousid;
  await scopeBrowserMessageId(body, anonId);
  const portal = origin.includes("app.eden.health") ? "patient" : "marketing";
  const userId = null;
  const collectOrderId = null;
  const session = buildSessionContext(
    effectiveSessionRaw,
    isNew ? "generated_collect_session" : "eden_session_cookie",
    "client",
    body
  );
  if (!session) {
    return new Response(JSON.stringify({ ok: false, error: "browser_session_cookie_invalid" }), {
      status: 409,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store", ...corsHeadersObj(origin) }
    });
  }
  let freshBootstrapCapability = null;
  if (needsSessionBootstrap) {
    const browserHost = browserCapabilityOriginHost(request);
    if (!browserHost) {
      return new Response(JSON.stringify({ ok: false, error: "browser_owner_cookie_required" }), {
        status: 409,
        headers: { "Content-Type": "application/json", "Cache-Control": "no-store", ...corsHeadersObj(origin) }
      });
    }
    try {
      freshBootstrapCapability = await mintBrowserCapability(env, {
        anonId,
        session: effectiveSessionRaw,
        browserHost
      });
    } catch (error) {
      console.error(JSON.stringify({
        worker: "eden-analytics",
        event: "browser_fresh_session_capability_failed",
        reason: String(error?.message || "unknown").slice(0, 120)
      }));
      return new Response(JSON.stringify({ ok: false, error: "browser_authentication_unavailable", retryable: true }), {
        status: 503,
        headers: { "Content-Type": "application/json", "Cache-Control": "no-store", "Retry-After": "5", ...corsHeadersObj(origin) }
      });
    }
  }
  const attributionPermission = await resolveAttributionPermissionWithDurableState(env, request, body, { anonId, session, userId, orderId: collectOrderId });
  const canUseAttribution = attributionPermission.allowed;
  if (!canUseAttribution) scrubAdvertisingAttributionFromBody(body);
  let freshClickIds = {};
  let freshUTMs = null;
  let pageReferrer = null;
  let collectPageUrl = null;
  const enrichmentState = resolveEnrichmentState(env, request, body);
  const extendedEnrichmentEnabled = enrichmentState.enabled;
  if (canUseAttribution) {
    const pageUrlStr = body?.context?.page?.url;
    const pageRefStr = body?.context?.page?.referrer || body?.context?.referrer || "";
    pageReferrer = sanitizeUrlString(pageRefStr);
    if (pageUrlStr) {
      try {
        collectPageUrl = new URL(pageUrlStr, request.url);
        freshClickIds = extractClickIds(collectPageUrl, request, extendedEnrichmentEnabled);
        freshUTMs = extractUTMs(collectPageUrl);
      } catch {
      }
    }
  }
  const collectInternalHandoffTransportSources = [body?.context?.campaign, body?.properties];
  const collectInternalHandoff = canUseAttribution && collectPageUrl
    ? await verifyInternalHandoffAssertion({
        env,
        request,
        destinationUrl: collectPageUrl,
        transportAttributionSources: collectInternalHandoffTransportSources,
        anonId,
        session,
        userId,
        orderId: collectOrderId,
        allowContinuationCookie: true
      })
    : { valid: false, reason: "internal_handoff_not_present" };
  if (!collectInternalHandoff.valid && collectPageUrl && (
    getCanonicalUrlParam(collectPageUrl, INTERNAL_HANDOFF_QUERY_PARAM, { includeNested: false, includeHash: false })
    || readCookie(request, INTERNAL_HANDOFF_COOKIE_NAME)
  )) {
    debugLog(env, `internal handoff collector rejected: ${collectInternalHandoff.reason || "unknown"}`);
  }
  let transportedInternalHandoffAttribution = null;
  if (collectInternalHandoff.valid) {
    transportedInternalHandoffAttribution = internalHandoffTransportValues(
      collectPageUrl,
      collectInternalHandoffTransportSources
    ) || {};
    // The signed destination query was already captured as a diagnostic edge
    // observation on the app document request. Do not let the same transported
    // values masquerade as fresh/current evidence on Segment collector events.
    scrubAdvertisingAttributionFromBody(body);
    freshClickIds = {};
    freshUTMs = null;
    pageReferrer = null;
  }
  const cookieAttr = canUseAttribution ? extractPreAuthAttribution(request) : null;
  const storedAttribution = env.GCLID_KV && canUseAttribution ? await resolveAttribution(env.GCLID_KV, anonId, null, null, cookieAttr) : cookieAttr ? stripInternalFields(cookieAttr) : null;
  const contextCampaign = canUseAttribution && !collectInternalHandoff.valid ? (body.context || {}).campaign || {} : {};
  const currentAttribution = {
    ...freshClickIds,
    ...freshUTMs || {},
    ...pageReferrer ? { attribution_referrer: pageReferrer } : {}
  };
  const eventNativeAttribution = mergeAttributionPreferFreshPrimary(contextCampaign, currentAttribution);
  const attribution = {
    ...storedAttribution ? stripInternalFields(storedAttribution) : {},
    ...contextCampaign,
    ...freshClickIds,
    ...freshUTMs || {},
    ...pageReferrer && !storedAttribution?.attribution_referrer ? { attribution_referrer: pageReferrer } : {}
  };
  sanitizePersistedEventUrls(body);
  if (!body.properties || typeof body.properties !== "object" || Array.isArray(body.properties)) body.properties = {};
  if (transportedInternalHandoffAttribution && Object.keys(transportedInternalHandoffAttribution).length) {
    body.properties.transported_internal_handoff = true;
    body.properties.transported_internal_handoff_selected_ad_click_id = collectInternalHandoff.pointerId;
    for (const [key, value] of Object.entries(transportedInternalHandoffAttribution)) {
      if (value) body.properties[`transported_${key}`] = value;
    }
  }
  const collectEventName = canonicalizeEventName(resolveEventName(body));
  const browserConversionObservation = isBrowserOutcomeObservationName(collectEventName);
  if (storedAttribution && browserConversionObservation && !collectInternalHandoff.valid) {
    // Attribution-survival contract, client leg: conversion events carry stored
    // first-party-continuity click IDs in properties (mirrors the server-collect merge)
    // so whichever event wins the Segment messageId dedupe keeps upload-grade evidence.
    // applyAttributionProvenance below labels these keys as recovered.
    for (const [k, v] of Object.entries(stripInternalFields(storedAttribution))) {
      if (v && !body.properties[k]) body.properties[k] = v;
    }
  }
  // Only event-native campaign values are stamped into properties/context.campaign;
  // stored-attribution recovery is provenance-labeled via applyAttributionProvenance
  // and survives in the first_touch_* touch model below.
  const campaignProps = buildCampaignContext(eventNativeAttribution);
  enrichPropertiesWithAttribution(body.properties, campaignProps, attribution);
  enrichPropertiesWithTouchModel(
    body.properties,
    storedAttribution || attribution,
    Object.keys(currentAttribution).length ? currentAttribution : null,
    storedAttribution ? "stored_attribution" : "current_event_fallback"
  );
  enrichPropertiesWithSession(body.properties, session);
  if (!body.context)
    body.context = {};
  body.context.campaign = { ...(body.context || {}).campaign || {}, ...campaignProps };
  applyAttributionProvenance(body, attribution, eventNativeAttribution);
  const superProps = {
    portal,
    source_type: "client",
    browser_event_authority: "provisional_observation",
    ...browserConversionObservation ? {
      browser_conversion_observation: true
    } : {},
    gpc_opt_out: gpcOptOut,
    attribution_suppressed: !canUseAttribution,
    transported_internal_handoff: !!transportedInternalHandoffAttribution,
    pipeline_version: PIPELINE_VERSION,
    enrichment_version: ENRICHMENT_VERSION,
    release_revision: RELEASE_REVISION,
    ...sessionSuperProps(session)
  };
  const collectUserId = null;
  const collectAdClickMemory = canUseAttribution ? await safeBuildAdClickMemoryCandidate({
    request,
    env,
    body,
    anonId,
    session,
    attribution,
    sourceType: "client",
    eventName: collectEventName || "collect",
    userId: collectUserId,
    orderId: collectOrderId,
    linkReason: collectEventName === "OS_purchase" ? "browser_purchase_signal" : "collect_event",
    eventNativeAttribution,
    internalHandoff: collectInternalHandoff
  }, "collect") : null;
  let collectAdClickPersistence = null;
  if (collectAdClickMemory) {
    if (shouldAnnotateAdClickMemoryPayload(env)) applyAdClickMemoryToBody(body, collectAdClickMemory);
    try {
      collectAdClickPersistence = await persistAdClickMemory(env, collectAdClickMemory);
    } catch (err) {
      console.error("[eden-analytics] ad-click collect persist error:", err);
      return new Response(JSON.stringify({ ok: false, error: "ad_click_memory_custody_unavailable", retryable: true }), {
        status: 503,
        headers: { "Content-Type": "application/json", "Cache-Control": "no-store", "Retry-After": "5", ...corsHeadersObj(origin) }
      });
    }
  }
  ensureExtendedEventContext(body, request, anonId, session, attribution, superProps, extendedEnrichmentEnabled, enrichmentState.mode, enrichmentState.canary, eventNativeAttribution);
  if (!canUseAttribution) scrubAdvertisingAttributionFromBody(body);
  // Production config selects synchronous delivery so a browser receives 2xx
  // only after Segment accepts the event. Async remains an explicit fallback.
  const synchronousSegmentDelivery = String(env.EDEN_BROWSER_SEGMENT_DELIVERY_MODE || "async").trim().toLowerCase() === "sync";
  if (!env.SEGMENT_WRITE_KEY && synchronousSegmentDelivery) {
    console.error(JSON.stringify({ worker: "eden-analytics", event: "segment_collect_configuration_error" }));
    return new Response(JSON.stringify({ ok: false, error: "segment_delivery_unavailable", retryable: true }), {
      status: 503,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store", "Retry-After": "5", ...corsHeadersObj(origin) }
    });
  }
  if (env.SEGMENT_WRITE_KEY && synchronousSegmentDelivery) {
    try {
      await forwardToSegment(
        env.SEGMENT_WRITE_KEY,
        body,
        anonId,
        superProps,
        canUseAttribution ? eventNativeAttribution : {},
        { preserveEventName: true }
      );
    } catch (error) {
      console.error(JSON.stringify({
        worker: "eden-analytics",
        event: "segment_collect_delivery_failed",
        retryable: true,
        reason: String(error?.message || "unknown").slice(0, 120)
      }));
      return new Response(JSON.stringify({ ok: false, error: "segment_delivery_failed", retryable: true }), {
        status: 503,
        headers: { "Content-Type": "application/json", "Cache-Control": "no-store", "Retry-After": "5", ...corsHeadersObj(origin) }
      });
    }
  } else if (env.SEGMENT_WRITE_KEY) {
    ctx.waitUntil(forwardToSegment(
      env.SEGMENT_WRITE_KEY,
      body,
      anonId,
      superProps,
      canUseAttribution ? eventNativeAttribution : {},
      { preserveEventName: true }
    ).catch((error) => console.error(JSON.stringify({
      worker: "eden-analytics",
      event: "segment_collect_delivery_failed_async",
      retryable: true,
      reason: String(error?.message || "unknown").slice(0, 120)
    }))));
  }
  const respHeaders = new Headers({ "Content-Type": "application/json", "Cache-Control": "no-store", ...corsHeadersObj(origin) });
  const reqUrl = new URL(request.url);
  if (isNew || anonymousCookieAliasesNeedSync(request)) {
    respHeaders.append("Set-Cookie", buildAnonCookie(anonId, reqUrl));
    respHeaders.append("Set-Cookie", buildLegacyAnonCookie(anonId, reqUrl));
    debugLog(env, "collect set canonical anonymous id for new visitor");
  }
  const collectSessionRaw = sessionRawValue(session);
  if (collectSessionRaw) {
    respHeaders.append("Set-Cookie", buildSessionCookie(collectSessionRaw, reqUrl));
    debugLog(env, existingSessionCookie ? "collect refreshed eden_session_id" : "collect set eden_session_id for collector-first session");
  }
  if (freshBootstrapCapability) {
    respHeaders.append("Set-Cookie", buildBrowserCapabilityCookie(freshBootstrapCapability));
  }
  if (collectAdClickMemory?.setPointerCookie && collectAdClickPersistence?.pointer_committed === true
    && (!collectAdClickMemory.snapshot || collectAdClickPersistence.queue_enqueued === true)) {
    respHeaders.append("Set-Cookie", buildAdClickPointerCookie(collectAdClickMemory.ad_click_id, reqUrl, env));
  }
  appendAttributionPermissionCookies(respHeaders, reqUrl, attributionPermission);
  const collectResponse = { ok: true, anonId, releaseRevision: RELEASE_REVISION };
  if (collectInternalHandoff.valid) {
    // Safe canary/producer proof only: expose booleans and a bounded resolution
    // class, never the opaque pointer, assertion, raw click IDs, or identity.
    collectResponse.internalHandoffVerified = true;
    collectResponse.internalHandoffSelectedPointerResolved = !!collectAdClickMemory
      && collectAdClickMemory.ad_click_id === collectInternalHandoff.pointerId;
    collectResponse.internalHandoffResolutionSource = collectAdClickMemory?.resolution?.resolution_source === "pointer_cookie"
      ? "pointer_cookie"
      : "unexpected";
  }
  if (extendedEnrichmentEnabled) {
    collectResponse.enrichmentActive = true;
    collectResponse.enrichmentMode = enrichmentState.mode;
    if (enrichmentState.canary) collectResponse.enrichmentCanary = true;
    collectResponse.workerVersion = PIPELINE_VERSION;
    collectResponse.enrichmentVersion = ENRICHMENT_VERSION;
  }
  return new Response(JSON.stringify(collectResponse), { status: 200, headers: respHeaders });
}
__name(handleCollect, "handleCollect");
async function handleServerCollect(request, env, ctx) {
  const authFailure = await authorizeServerRequest(request, env);
  if (authFailure) return authFailure;
  // Privacy-safe positive proof that a real producer still holds the matching
  // server secret. No event name, identity, URL, click ID, or payload data is
  // logged; sampled Workers Logs can establish authenticated producer health.
  console.log(JSON.stringify({
    worker: "eden-analytics",
    event: "server_collect_authenticated",
    source_pipeline_version: PIPELINE_VERSION
  }));
  const parsedBody = await parseBoundedJsonRequest(request);
  if (parsedBody.response) return parsedBody.response;
  const body = parsedBody.value;
  const gpcOptOut = isGpcOptOut(request);
  if (!body.properties || typeof body.properties !== "object" || Array.isArray(body.properties)) {
    body.properties = {};
  }
  sanitizeUploadGradeClickIdClaimsInObject(body, "server_collect");
  const identity = resolveIdentityFromBody(request, body);
  if (identity.stableIdentityConflict) quarantineConflictingServerUserIdentityClaims(body);
  if (identity.anonymousIdentityConflict) quarantineConflictingAnonymousIdentityClaims(body);
  const anonId = identity.anonymousId || null;
  const userId = identity.userId || null;
  const originalEventName = String(resolveEventName(body) || "").trim();
  const eventName = canonicalizeEventName(originalEventName);
  const eventEnvelopeType = String(body.type || "track").trim().toLowerCase();
  const orderIdDetails = resolveOrderIdDetails(body);
  const orderId = orderIdDetails.value;
  const isConversionEvent = CONVERSION_EVENTS.has(eventName);
  const conversionKeyDetails = isConversionEvent ? resolveConversionKeyDetails(body, eventName) : null;
  const purchaseOrderAliasQuarantined = eventName === "OS_purchase"
    && orderIdDetails.namespace === "order_id"
    && (orderIdDetails.invalid || orderIdDetails.conflict || !orderIdDetails.value);
  if (purchaseOrderAliasQuarantined) quarantineConflictingOrderIdentityClaims(body);
  if (isConversionEvent && eventEnvelopeType !== "track") {
    const failedHeaders = new Headers({ "Content-Type": "application/json", ...corsHeadersObj(request.headers.get("Origin") || "") });
    return new Response(JSON.stringify({
      ok: false,
      error: "conversion_track_envelope_required",
      retryable: false,
      segment_forwarded: false
    }), { status: 422, headers: failedHeaders });
  }
  if (isConversionEvent) {
    body.properties.conversion_business_stage = CONVERSION_BUSINESS_STAGES.get(eventName) || eventName;
    body.properties.conversion_event_canonical = eventName;
    if (originalEventName && originalEventName !== eventName) {
      body.properties.conversion_event_original_alias = originalEventName.slice(0, 128);
    }
  }
  const serverReceivedAt = nowUTC();
  const producerEventTimestamp = validatedProducerEventTimestamp(body) || serverReceivedAt;
  const session = resolveSessionFromRequestBody(request, body, "server");
  const enrichmentState = resolveEnrichmentState(env, request, body);
  const extendedEnrichmentEnabled = enrichmentState.enabled;
  const serverEventAttribution = extractEventCurrentAttribution(request, body, extendedEnrichmentEnabled);
  sanitizePersistedEventUrls(body);
  // Do not recover an anonymous owner from legacy id:link rows. Those rows
  // were historically writable from browser identify traffic. Authenticated
  // producers must carry the Eden anonymous id explicitly when they have it;
  // otherwise trusted server user/order continuity is the only resolver path.
  if (!anonId && !userId)
    console.warn("[eden-analytics] server-collect: no identity for:", eventName);
  if (eventName)
    body.event = eventName;
  if (userId) {
    body.userId = userId;
    if (identity.stableIdentityKeyType === "eden_identity_id") body.eden_identity_id = userId;
  }
  if (anonId)
    body.anonymousId = anonId;
  if (orderId && orderIdDetails.namespace === "order_id" && !body.properties.order_id)
    body.properties.order_id = orderId;
  if (isConversionEvent && orderIdDetails.namespace) {
    body.properties.conversion_reference_namespace = orderIdDetails.namespace;
  }
  if (isConversionEvent && !conversionKeyDetails?.value) {
    const failedHeaders = new Headers({ "Content-Type": "application/json", ...corsHeadersObj(request.headers.get("Origin") || "") });
    return new Response(JSON.stringify({
      ok: false,
      error: conversionKeyDetails?.conflict
        ? "conversion_idempotency_key_conflict"
        : conversionKeyDetails?.invalid
          ? "conversion_idempotency_key_invalid"
          : "conversion_idempotency_key_required",
      retryable: false,
      segment_forwarded: false
    }), { status: 422, headers: failedHeaders });
  }
  if (isConversionEvent && !env.GCLID_KV) {
    const failedHeaders = new Headers({ "Content-Type": "application/json", ...corsHeadersObj(request.headers.get("Origin") || "") });
    return new Response(JSON.stringify({
      ok: false,
      error: "conversion_dedupe_unavailable",
      retryable: true,
      segment_forwarded: false
    }), { status: 503, headers: failedHeaders });
  }
  let conversionCoordinatorLease = null;
  if (isConversionEvent) {
    const lease = await acquireConversionCoordinatorLease(env, conversionKeyDetails.value, eventName);
    if (!lease.acquired) {
      const failedHeaders = new Headers({ "Content-Type": "application/json", ...corsHeadersObj(request.headers.get("Origin") || "") });
      if (Number.isFinite(lease.retryAfterMs)) {
        failedHeaders.set("Retry-After", String(Math.max(1, Math.ceil(lease.retryAfterMs / 1e3))));
      }
      return new Response(JSON.stringify({
        ok: false,
        error: lease.reason === "conversion_in_progress"
          ? "conversion_in_progress"
          : "conversion_coordinator_unavailable",
        retryable: true,
        segment_forwarded: false,
        ...(Number.isFinite(lease.retryAfterMs) ? { retry_after_ms: Math.max(250, Math.ceil(lease.retryAfterMs)) } : {})
      }), { status: 503, headers: failedHeaders });
    }
    conversionCoordinatorLease = lease;
  }
  let conversionPrivacyHistoryRecord = conversionCoordinatorLease?.record || null;
  if (isConversionEvent && conversionCoordinatorLease && !conversionPrivacyHistoryRecord) {
    try {
      conversionCoordinatorLease.kvFallback = await preloadConversionKvFallback(env, {
        eventName,
        orderId,
        body,
        scopeHash: conversionCoordinatorLease.scopeHash
      });
      conversionPrivacyHistoryRecord = conversionCoordinatorLease.kvFallback.record;
    } catch (err) {
      console.error("[eden-analytics] conversion pre-privacy history read failed:", err);
      await releaseConversionCoordinatorLease(conversionCoordinatorLease);
      conversionCoordinatorLease = null;
      const failedHeaders = new Headers({ "Content-Type": "application/json", ...corsHeadersObj(request.headers.get("Origin") || "") });
      return new Response(JSON.stringify({
        ok: false,
        error: "conversion_dedupe_read_failed",
        retryable: true,
        segment_forwarded: false
      }), { status: 503, headers: failedHeaders });
    }
  }
  const conversionPrivacyOwnerHashes = conversionRecordOwnerReferenceHashes(conversionPrivacyHistoryRecord);
  const conversionPrivacyOwnerVerifiable = ["identity:anonymous_id", "identity:user_id", "identity:order_id"]
    .some((key) => !!conversionPrivacyOwnerHashes[key]);
  const conversionPrivacyOwnerConflict = isConversionEvent && conversionPrivacyHistoryRecord && conversionPrivacyOwnerVerifiable
    ? await conversionRecordHasPresentedOwnerConflict(conversionPrivacyHistoryRecord, { anonId, userId, orderId })
    : false;
  // A legacy or malformed KV fallback can prove that this conversion key has
  // history without proving a namespace-safe owner. Treat it as unproven for
  // durable privacy mutation; the request itself still honors explicit choice.
  const conversionPrivacyOwnerUnproven = isConversionEvent
    && !!conversionPrivacyHistoryRecord
    && !conversionPrivacyOwnerVerifiable;
  // A conflicting retry can suppress its current request, but it cannot add,
  // propagate, or clear durable denial state for the accepted transaction
  // owner. Resolve the canonical conversion owner before any privacy-ledger
  // mutation and drop the entire presented identity set when one owner field
  // conflicts; this also prevents a matching order ID from being used as a
  // bridge by the wrong user.
  const privacyIdentity = conversionPrivacyOwnerConflict || conversionPrivacyOwnerUnproven
    ? {}
    : { anonId, session, userId, orderId };
  const attributionPermission = await resolveAttributionPermissionWithDurableState(
    env,
    request,
    body,
    privacyIdentity,
    { durableIdentityMutationAllowed: !conversionPrivacyOwnerConflict && !conversionPrivacyOwnerUnproven }
  );
  const canUseAttribution = attributionPermission.allowed;
  if (conversionPrivacyOwnerConflict) {
    body.properties.conversion_privacy_identity_quarantined = true;
  }
  if (conversionPrivacyOwnerUnproven) {
    body.properties.conversion_privacy_owner_unproven = true;
  }
  if (!canUseAttribution) scrubAdvertisingAttributionFromBody(body);
  try {
  // Legacy email:user rows are quarantined because their provenance is not
  // server-authenticated. Enhanced identity must arrive on this authenticated
  // event or be joined later in governed warehouse models.
  const storedAttributionSources = env.GCLID_KV && canUseAttribution
    ? await resolveAttributionSources(env.GCLID_KV, anonId, userId, orderId, null)
    : null;
  let storedAttribution = storedAttributionSources?.merged || null;
  // extractEventCurrentAttribution already applies fresh-page evidence
  // precedence over body/campaign fallback fields. Do not merge the raw body a
  // second time and accidentally re-promote an older GCLID over a fresh braid.
  let serverCurrentAttribution = serverEventAttribution;
  let conversionDedupPlan = null;
  if (isConversionEvent && conversionKeyDetails?.value) {
    const conversionScopeHash = conversionCoordinatorLease?.scopeHash || await sha256Raw(`eden_conversion_coordinator_v1\0conversion_key\0${conversionKeyDetails.value}`);
    const dedupKey = `dedup:v4:${eventName}:${conversionScopeHash}`;
    let currentSignalState = await buildServerConversionSignalState({
      body,
      anonId,
      session,
      userId,
      orderId,
      conversionScopeHash,
      conversionKeySource: conversionKeyDetails.source,
      storedAttribution,
      eventNativeAttribution: serverCurrentAttribution
    });
    let existingRecord = null;
    let mergedSignalHashes = currentSignalState.hashes;
    let mergedStatusRanks = currentSignalState.statusRanks;
    let acceptedSignalKeys = Object.keys(currentSignalState.hashes);
    let conflictingSignalKeys = [];
    let currentConflictingSignalKeys = [];
    let conflictingSignalHashes = {};
    let repairBaseDelivery = false;
    let persistencePending = false;
    let firstSeenAt = nowUTC();
    let acceptedBeforeAttemptSignalHashes = {};
    let acceptedBeforeAttemptStatusRanks = {};
    let retryingUnacknowledgedDelivery = false;
    let pendingDeliveryKind = null;
    let supersedingPendingEnrichment = false;
    let pendingForwardSignalKeys = [];
    let pendingPayloadFingerprintChanged = false;
    let pendingBasePayloadFingerprintChanged = false;
    let pendingDiagnosticConflictChanged = false;
    let currentDiagnosticConflictSignalKeys = [];
    let pendingRepairEnrichmentSignalKeys = [];
    let pendingPersistenceIntent = null;
    let segmentEventTimestamp = producerEventTimestamp;
    let repairEnrichmentEventTimestamp = producerEventTimestamp;
    try {
      if (conversionCoordinatorLease?.record) {
        existingRecord = conversionCoordinatorLease.record;
        if (existingRecord.event !== eventName || !existingRecord.signal_hashes || !existingRecord.status_ranks) {
          throw new Error("conversion_coordinator_record_invalid");
        }
      } else {
        const preloadedFallback = conversionCoordinatorLease?.kvFallback?.preloaded === true
          ? conversionCoordinatorLease.kvFallback
          : null;
        let existingDedup = preloadedFallback ? preloadedFallback.raw : await env.GCLID_KV.get(dedupKey);
        let existingDedupSource = preloadedFallback ? preloadedFallback.source : existingDedup ? "v4_raw_free" : null;
        let existingDedupReference = preloadedFallback ? preloadedFallback.reference : null;
        // One-time compatibility seed for v5.55 and earlier raw order-key rows.
        // The v5.56 mirror is raw-free and all subsequent reads prefer the
        // strongly consistent coordinator record.
        if (!preloadedFallback && !existingDedup && orderId && eventName !== "OS_purchase") {
          existingDedup = await env.GCLID_KV.get(`dedup:${eventName}:${orderId}`);
          if (existingDedup) {
            existingDedupSource = "legacy_real_order";
            existingDedupReference = orderId;
          }
        }
        // v5.55 overloaded "order" with master/treatment/transaction fallback
        // and retained that row for only one day. Read that exact historical
        // key once during migration so a retry already delivered immediately
        // before cutover is not recreated under the corrected transaction key.
        // Never write this overloaded key again and never use it for a new
        // event after the old TTL naturally expires.
        if (!preloadedFallback && !existingDedup && eventName === "OS_purchase") {
          for (const legacyV555Reference of resolveLegacyV555ConversionReferences(body)) {
            existingDedup = await env.GCLID_KV.get(`dedup:${eventName}:${legacyV555Reference}`);
            if (existingDedup) {
              existingDedupSource = "legacy_v555_overloaded_order";
              existingDedupReference = legacyV555Reference;
              break;
            }
          }
        }
        if (existingDedup) {
          const parsedExisting = preloadedFallback?.record || JSON.parse(existingDedup);
          const legacyRawKeySource = ["legacy_real_order", "legacy_v555_overloaded_order"].includes(existingDedupSource);
          const legacyScopeProven = !legacyRawKeySource || legacyConversionRecordProvesCurrentScope({
            record: parsedExisting,
            eventName,
            reference: existingDedupReference,
            conversionKeyDetails,
            currentSignalState
          });
          if (legacyRawKeySource && !legacyScopeProven) {
            // A treatment/order-scoped v5.55 row does not prove that this
            // charge-authoritative OS_purchase transaction was delivered. At
            // cutover it is safer to let Segment's transaction message ID
            // dedupe a possible replay than to suppress a distinct real charge.
            existingRecord = null;
          } else if (legacyRawKeySource
            && parsedExisting?.event === eventName
            && parsedExisting?.schema_version !== "eden_conversion_dedup_v4"
            && (!parsedExisting?.signal_hashes || typeof parsedExisting.signal_hashes !== "object")) {
            const legacySegmentMessageId = legacyV555SegmentMessageId(eventName, parsedExisting, existingDedupReference);
            if (!legacySegmentMessageId) {
              // Without the exact historical message ID, the row proves only a
              // pre-Segment attempt. It cannot safely suppress or replay this
              // purchase under a different idempotency key.
              existingRecord = null;
            } else {
              // Deployed v5.55 wrote this row before scheduling its asynchronous
              // Segment call. Equality proves transaction scope, not delivery.
              // Treat it as an unknown commit and replay the historical v5.55
              // message ID. Empty accepted/pending signal baselines ensure the
              // current full truth also lands once as a non-conversion
              // enrichment if Segment dedupes a previously successful base.
              const legacyEventTimestamp = parsedExisting.fired_at && Number.isFinite(Date.parse(String(parsedExisting.fired_at)))
                ? new Date(parsedExisting.fired_at).toISOString()
                : producerEventTimestamp;
              existingRecord = {
                schema_version: "eden_conversion_dedup_v4",
                event: eventName,
                signal_hashes: {},
                status_ranks: {},
                accepted_signal_hashes: {},
                accepted_status_ranks: {},
                pending_signal_hashes: {},
                pending_status_ranks: {},
                pending_signal_keys: [],
                pending_delivery_kind: "base",
                pending_message_id: legacySegmentMessageId,
                pending_event_timestamp: legacyEventTimestamp,
                signal_count: 0,
                conflicting_signal_count: 0,
                conflicting_signal_keys: [],
                conflicting_signal_hashes: {},
                attribution_found: !!parsedExisting.attribution_found,
                first_seen_at: legacyEventTimestamp,
                last_enriched_at: nowUTC(),
                delivery_state: "segment_delivery_unacknowledged",
                delivery_event: eventName,
                migration_source: "v555_pre_segment_attempt_unknown_commit"
              };
            }
          } else if (legacyRawKeySource && parsedExisting?.schema_version === "eden_conversion_dedup_v4") {
            // A v5.56 compatibility mirror is scoped only by real order ID and
            // therefore cannot prove that this transaction-scoped OS_purchase
            // was already delivered. Two legitimate charge transactions may
            // share one order. The typed DO/canonical hash key is authoritative;
            // ignore this raw mirror rather than suppressing the second charge.
            existingRecord = null;
          } else {
            existingRecord = parsedExisting;
          }
        }
      }
      if (existingRecord) {
          const existingWasAcknowledged = [
            "segment_acknowledged",
            "segment_acknowledged_pending_persistence"
          ].includes(existingRecord?.delivery_state);
          const identityBaselineHashes = existingWasAcknowledged
            ? existingRecord.signal_hashes || {}
            : existingRecord.accepted_signal_hashes || {};
          const conflictingStoredOwnerKeys = new Set(
            ["identity:anonymous_id", "identity:user_id", "identity:order_id"].filter((key) =>
              identityBaselineHashes[key]
              && currentSignalState.hashes?.[key]
              && identityBaselineHashes[key] !== currentSignalState.hashes[key]
            )
          );
          if (conflictingStoredOwnerKeys.size && storedAttributionSources) {
            // Stored attribution is owner-scoped. Once the conversion ledger
            // proves that a presented user/order/anonymous owner conflicts with
            // the accepted transaction, remove that owner's KV contribution
            // before signal comparison or payload enrichment. A conflicting
            // customer must never import their campaign into another purchase.
            storedAttribution = mergeAttributionSources(storedAttributionSources, {
              includeAnon: !conflictingStoredOwnerKeys.has("identity:anonymous_id"),
              includeUser: !conflictingStoredOwnerKeys.has("identity:user_id"),
              includeOrder: !conflictingStoredOwnerKeys.has("identity:order_id")
            });
            currentSignalState = await buildServerConversionSignalState({
              body,
              anonId,
              session,
              userId,
              orderId,
              conversionScopeHash,
              conversionKeySource: conversionKeyDetails.source,
              storedAttribution,
              eventNativeAttribution: serverCurrentAttribution
            });
          }
          const pendingRetry = validatePendingConversionRetryState(existingRecord, currentSignalState);
          if (!pendingRetry.compatible) {
            console.warn(JSON.stringify({
              worker: "eden-analytics",
              event: "conversion_retry_state_rejected",
              missing_signal_count: pendingRetry.missing,
              regressed_signal_count: pendingRetry.regressed,
              conflicting_signal_count: pendingRetry.conflicting,
              source_pipeline_version: PIPELINE_VERSION
            }));
            const retryHeaders = new Headers({ "Content-Type": "application/json", ...corsHeadersObj(request.headers.get("Origin") || "") });
            appendAttributionPermissionCookies(retryHeaders, new URL(request.url), attributionPermission);
            return new Response(JSON.stringify({
              ok: false,
              error: "conversion_retry_state_incomplete_or_regressed",
              retryable: true,
              refresh_required: true,
              segment_forwarded: false,
              missing_signal_count: pendingRetry.missing,
              regressed_signal_count: pendingRetry.regressed,
              conflicting_signal_count: pendingRetry.conflicting
            }), { status: 409, headers: retryHeaders });
          }
          retryingUnacknowledgedDelivery = existingRecord?.delivery_state === "segment_delivery_unacknowledged";
          persistencePending = existingRecord?.delivery_state === "segment_acknowledged_pending_persistence";
          pendingDeliveryKind = retryingUnacknowledgedDelivery
            ? existingRecord?.pending_delivery_kind || (String(existingRecord?.delivery_event || "").endsWith("_enrichment") ? "enrichment" : "base")
            : null;
          if (retryingUnacknowledgedDelivery
            && existingRecord?.pending_event_timestamp
            && Number.isFinite(Date.parse(String(existingRecord.pending_event_timestamp)))) {
            // Unknown-commit replay must be byte-stable at the business-event
            // level. Reuse the first attempted Segment event time so one
            // purchase message ID cannot land on two reporting dates.
            segmentEventTimestamp = new Date(existingRecord.pending_event_timestamp).toISOString();
          }
          pendingForwardSignalKeys = retryingUnacknowledgedDelivery
            ? conversionForwardSignalKeys(existingRecord, currentSignalState)
            : [];
          pendingPayloadFingerprintChanged = retryingUnacknowledgedDelivery
            && pendingDeliveryKind === "enrichment"
            && !!existingRecord?.pending_payload_fingerprint_sha256
            && existingRecord.pending_payload_fingerprint_sha256 !== currentSignalState.payloadFingerprintSha256;
          pendingBasePayloadFingerprintChanged = retryingUnacknowledgedDelivery
            && pendingDeliveryKind === "base"
            && validPendingSegmentTrackPayload(existingRecord?.pending_segment_payload, existingRecord?.pending_message_id || null)
            && !!existingRecord?.pending_payload_fingerprint_sha256
            && existingRecord.pending_payload_fingerprint_sha256 !== currentSignalState.payloadFingerprintSha256;
          pendingRepairEnrichmentSignalKeys = retryingUnacknowledgedDelivery
            && pendingDeliveryKind === "base"
            && Array.isArray(existingRecord?.pending_repair_enrichment_signal_keys)
            ? existingRecord.pending_repair_enrichment_signal_keys.filter((key) => typeof key === "string" && key.length <= 256)
            : [];
          pendingPersistenceIntent = persistencePending && existingRecord?.persistence_intent && typeof existingRecord.persistence_intent === "object"
            ? existingRecord.persistence_intent
            : null;
          acceptedBeforeAttemptSignalHashes = existingWasAcknowledged
            ? existingRecord.signal_hashes || {}
            : existingRecord.accepted_signal_hashes || {};
          acceptedBeforeAttemptStatusRanks = existingWasAcknowledged
            ? existingRecord.status_ranks || {}
            : existingRecord.accepted_status_ranks || {};
          const acceptedBaselineRecord = {
            ...existingRecord,
            signal_hashes: acceptedBeforeAttemptSignalHashes,
            status_ranks: acceptedBeforeAttemptStatusRanks
          };
          const merged = mergeServerConversionSignalState(acceptedBaselineRecord, currentSignalState);
          mergedSignalHashes = merged.mergedHashes;
          mergedStatusRanks = merged.mergedStatusRanks;
          acceptedSignalKeys = merged.acceptedSignalKeys;
          currentConflictingSignalKeys = merged.conflictingSignalKeys;
          currentDiagnosticConflictSignalKeys = currentConflictingSignalKeys.filter((key) => !CONVERSION_MONOTONIC_STATUS_KEYS.has(key));
          pendingDiagnosticConflictChanged = retryingUnacknowledgedDelivery
            && pendingDeliveryKind === "enrichment"
            && pendingDiagnosticConflictStateChanged(existingRecord, currentSignalState, currentDiagnosticConflictSignalKeys);
          supersedingPendingEnrichment = retryingUnacknowledgedDelivery
            && pendingDeliveryKind === "enrichment"
            && (pendingForwardSignalKeys.length > 0
              || pendingPayloadFingerprintChanged
              || pendingDiagnosticConflictChanged);
          if (supersedingPendingEnrichment) {
            // Any changed forwarded business payload, monotonic/additive
            // signal, or corrected diagnostic conflict safely supersedes an
            // ambiguous non-conversion enrichment. Give the current truth a
            // new message ID and current producer timestamp; never reuse the
            // ambiguous enrichment ID with different payload bytes.
            segmentEventTimestamp = producerEventTimestamp;
          }
          const conflictHistory = mergeConversionConflictHashHistory(existingRecord, currentSignalState, currentDiagnosticConflictSignalKeys);
          conflictingSignalHashes = conflictHistory.history;
          conflictingSignalKeys = [...new Set([
            ...(Array.isArray(existingRecord?.conflicting_signal_keys) ? existingRecord.conflicting_signal_keys : []),
            ...currentConflictingSignalKeys
          ])];
          firstSeenAt = existingRecord?.first_seen_at || existingRecord?.fired_at || firstSeenAt;
          if (retryingUnacknowledgedDelivery
            && pendingDeliveryKind === "enrichment"
            && Array.isArray(existingRecord?.pending_signal_keys)) {
            // An exact retry reuses the attempted enrichment signal set. A
            // superseding retry carries that complete pending state plus only
            // the forward/additive keys under its new message ID. The canonical
            // accepted hash remains unchanged for diagnostic conflicts.
            acceptedSignalKeys = [...new Set([
              ...existingRecord.pending_signal_keys.filter((key) =>
                typeof key === "string" && currentSignalState.hashes?.[key]
              ),
              ...(supersedingPendingEnrichment ? pendingForwardSignalKeys : [])
            ])].sort();
          }
          // v5.56 never treats a pre-acknowledgement compatibility record as
          // proof that Segment received the conversion. Repair any
          // earlier/incomplete record with the same idempotent message id.
          repairBaseDelivery = !existingWasAcknowledged && pendingDeliveryKind !== "enrichment";
          if (repairBaseDelivery && !acceptedSignalKeys.includes("delivery:segment_acknowledgement")) {
            acceptedSignalKeys.push("delivery:segment_acknowledgement");
          }
          // A genuinely new conflicting value is still valuable diagnostic and
          // business-correction evidence. Include it in the enrichment attempt
          // even when the same event also advances an accepted lifecycle field.
          // The canonical accepted hash remains unchanged; this key list records
          // the exact attempted diagnostic payload so an ambiguous response can
          // be retried without losing either the progress or the conflict.
          const hasDeliverableSignalDelta = acceptedSignalKeys.length > 0
            || conflictHistory.newlyObservedKeys.length > 0;
          if (hasDeliverableSignalDelta) {
            // The enrichment body also carries value-free diagnostics for every
            // conflict observed on this request. Include the full current
            // conflict set in the attempted state, not only conflicts that are
            // new to history. Otherwise a later lifecycle progression carrying
            // an already-known conflicting click/identity could receive an
            // ambiguous response and become impossible to retry exactly.
            acceptedSignalKeys = [...new Set([
              ...acceptedSignalKeys,
              ...currentConflictingSignalKeys
            ])].sort();
          }
          if (acceptedSignalKeys.length === 0
            && !persistencePending
            && !(retryingUnacknowledgedDelivery && pendingDeliveryKind === "enrichment")) {
            // Seed the strongly consistent ledger when this order existed only
            // in the legacy KV mirror. All subsequent requests read the DO
            // record first and cannot observe eventual-KV lag.
            if (!conversionCoordinatorLease?.record) {
              const seededRecord = {
                ...existingRecord,
                schema_version: "eden_conversion_dedup_v4",
                event: eventName,
                signal_hashes: mergedSignalHashes,
                status_ranks: mergedStatusRanks,
                delivery_state: "segment_acknowledged",
                delivery_event: existingRecord.delivery_event || eventName
              };
              await writeConversionCoordinatorRecord(conversionCoordinatorLease, eventName, seededRecord);
              await env.GCLID_KV.put(dedupKey, JSON.stringify(seededRecord), { expirationTtl: KV_DEDUP_TTL });
            }
            debugLog(env, "dedup blocked after acknowledged delivery");
            const dedupHeaders = new Headers({ "Content-Type": "application/json", ...corsHeadersObj(request.headers.get("Origin") || "") });
            appendAttributionPermissionCookies(dedupHeaders, new URL(request.url), attributionPermission);
            return new Response(JSON.stringify({ ok: true, deduped: true }), { status: 200, headers: dedupHeaders });
          }
          debugLog(env, "dedup allowed additive, monotonic, or delivery-repair conversion retry");
      }
    } catch (err) {
        // An unreadable prior record means we cannot distinguish a first
        // conversion from a later business-state enrichment. Retrying is safer
        // than sending an enrichment under the base purchase message id and
        // letting Segment discard its new status/product truth.
        console.error("[eden-analytics] conversion dedupe read failed:", err);
        const failedHeaders = new Headers({ "Content-Type": "application/json", ...corsHeadersObj(request.headers.get("Origin") || "") });
        appendAttributionPermissionCookies(failedHeaders, new URL(request.url), attributionPermission);
        return new Response(JSON.stringify({
          ok: false,
          error: "conversion_dedupe_read_failed",
          retryable: true,
          segment_forwarded: false
        }), { status: 503, headers: failedHeaders });
    }
    const skipSegmentDelivery = persistencePending && acceptedSignalKeys.length === 0;
    const isEnrichment = pendingDeliveryKind === "enrichment"
      || (!!existingRecord && !repairBaseDelivery && acceptedSignalKeys.length > 0);
    const replayPendingBaseMessageId = pendingDeliveryKind === "base"
      ? boundedStableIdentifier(existingRecord?.pending_message_id)
      : null;
    const baseSegmentMessageId = replayPendingBaseMessageId || conversionKeyDetails.segmentMessageId
      || (conversionKeyDetails.rawValue
        ? `eden_${eventName}_${conversionKeyDetails.rawValue}`
        : `eden_${eventName}_${conversionScopeHash.slice(0, 40)}`);
    const replayPendingEnrichmentMessageId = pendingDeliveryKind === "enrichment" && !supersedingPendingEnrichment
      ? boundedStableIdentifier(existingRecord?.pending_message_id)
        || await buildConversionEnrichmentMessageId(
          eventName,
          conversionScopeHash,
          existingRecord?.pending_signal_keys || [],
          {
            hashes: existingRecord?.pending_signal_hashes || {},
            payloadFingerprintSha256: existingRecord?.pending_payload_fingerprint_sha256 || null
          }
        )
      : null;
    const segmentMessageId = isEnrichment
      ? replayPendingEnrichmentMessageId || await buildConversionEnrichmentMessageId(eventName, conversionScopeHash, acceptedSignalKeys, currentSignalState)
      : baseSegmentMessageId;
    const replayPendingAttempt = retryingUnacknowledgedDelivery
      && !supersedingPendingEnrichment
      && validPendingSegmentTrackPayload(existingRecord?.pending_segment_payload, segmentMessageId)
      ? {
        segmentPayload: existingRecord.pending_segment_payload,
        signalHashes: existingRecord?.pending_signal_hashes && typeof existingRecord.pending_signal_hashes === "object"
          ? existingRecord.pending_signal_hashes
          : {},
        statusRanks: existingRecord?.pending_status_ranks && typeof existingRecord.pending_status_ranks === "object"
          ? existingRecord.pending_status_ranks
          : {},
        signalKeys: Array.isArray(existingRecord?.pending_signal_keys)
          ? existingRecord.pending_signal_keys.filter((key) => typeof key === "string").sort()
          : [],
        diagnosticConflictSignalKeys: Array.isArray(existingRecord?.pending_diagnostic_conflict_signal_keys)
          ? existingRecord.pending_diagnostic_conflict_signal_keys.filter((key) => typeof key === "string").sort()
          : [],
        payloadFingerprintSha256: existingRecord?.pending_payload_fingerprint_sha256 || null
      }
      : null;
    // If the prior base response was ambiguous, replay the stable base and use
    // a second enrichment solely for new/forward truth. If a prior
    // non-conversion enrichment is still ambiguous, an exact retry reuses its
    // message ID, while forward/additive truth supersedes it under one new
    // enrichment message rather than mutating the old ID and duplicating the
    // correction.
    const repairEnrichmentSignalKeys = retryingUnacknowledgedDelivery
      ? pendingDeliveryKind === "base"
        ? [...new Set([
          ...pendingRepairEnrichmentSignalKeys,
          ...pendingForwardSignalKeys,
          ...currentDiagnosticConflictSignalKeys
        ])].sort()
        : []
      : repairBaseDelivery && existingRecord
        ? conversionForwardSignalKeys(existingRecord, currentSignalState)
        : [];
    const repairEnrichmentRequired = repairEnrichmentSignalKeys.length > 0 || pendingBasePayloadFingerprintChanged;
    const repairEnrichmentMessageId = repairEnrichmentRequired
      ? await buildConversionEnrichmentMessageId(eventName, conversionScopeHash, repairEnrichmentSignalKeys, currentSignalState)
      : null;
    conversionDedupPlan = {
      dedupKey,
      // OS_purchase is transaction-authoritative. Never create a new raw
      // order-scoped compatibility row that could collapse two charges sharing
      // one order. Other order-authoritative milestones may retain the mirror.
      legacyDedupKey: eventName !== "OS_purchase" && orderId ? `dedup:${eventName}:${orderId}` : null,
      currentSignalState,
      mergedSignalHashes,
      mergedStatusRanks,
      acceptedSignalKeys,
      conflictingSignalKeys,
      currentConflictingSignalKeys,
      currentDiagnosticConflictSignalKeys,
      conflictingSignalHashes,
      repairBaseDelivery,
      repairEnrichmentSignalKeys,
      repairEnrichmentMessageId,
      firstSeenAt,
      acceptedBeforeAttemptSignalHashes,
      acceptedBeforeAttemptStatusRanks,
      skipSegmentDelivery,
      persistencePending,
      pendingPersistenceIntent,
      pendingDeliveryKind,
      supersedingPendingEnrichment,
      pendingPayloadFingerprintChanged,
      pendingBasePayloadFingerprintChanged,
      pendingDiagnosticConflictChanged,
      isEnrichment,
      baseSegmentMessageId,
      segmentMessageId,
      segmentEventTimestamp,
      repairEnrichmentEventTimestamp,
      retryingUnacknowledgedDelivery,
      replayPendingAttempt,
      record: {
        schema_version: "eden_conversion_dedup_v4",
        event: eventName,
        signal_hashes: mergedSignalHashes,
        status_ranks: mergedStatusRanks,
        accepted_signal_hashes: mergedSignalHashes,
        accepted_status_ranks: mergedStatusRanks,
        signal_count: Object.keys(mergedSignalHashes).length,
        conflicting_signal_count: conflictingSignalKeys.length,
        conflicting_signal_keys: [...conflictingSignalKeys].sort(),
        conflicting_signal_hashes: conflictingSignalHashes,
        attribution_found: !!storedAttribution || Object.keys(serverCurrentAttribution).length > 0,
        first_seen_at: firstSeenAt,
        last_enriched_at: nowUTC(),
        delivery_state: "segment_acknowledged",
        delivery_event: isEnrichment ? `${eventName}_enrichment` : eventName
      }
    };
  }
  const diagnosticServerCurrentAttribution = { ...serverCurrentAttribution };
  const conversionConflictingClickFields = conversionConflictClickFields(conversionDedupPlan?.currentConflictingSignalKeys);
  const conversionHasIdentityOrClickConflict = hasConversionIdentityOrClickConflict(conversionDedupPlan?.currentConflictingSignalKeys);
  const conversionHasUserIdentityConflict = (conversionDedupPlan?.currentConflictingSignalKeys || []).includes("identity:user_id");
  const conversionHasAnonymousIdentityConflict = (conversionDedupPlan?.currentConflictingSignalKeys || []).includes("identity:anonymous_id");
  const conversionHasOrderIdentityConflict = (conversionDedupPlan?.currentConflictingSignalKeys || []).includes("identity:order_id");
  if (conversionConflictingClickFields.size) {
    // Preserve the conflicting native click as an unowned immutable diagnostic
    // snapshot below, but do not let it enter Segment campaign context, stable
    // conversion identity links, active-pointer KV, or order attribution.
    removeCanonicalFieldsDeep(body, conversionConflictingClickFields);
    serverCurrentAttribution = { ...serverCurrentAttribution };
    for (const field of conversionConflictingClickFields) delete serverCurrentAttribution[field];
  }
  if (conversionDedupPlan?.conflictingSignalKeys?.length) {
    if (conversionHasUserIdentityConflict) quarantineConflictingServerUserIdentityClaims(body);
    if (conversionHasAnonymousIdentityConflict) quarantineConflictingAnonymousIdentityClaims(body);
    if (conversionHasOrderIdentityConflict) quarantineConflictingOrderIdentityClaims(body);
    body.properties.conversion_conflicting_signal_keys = [...conversionDedupPlan.conflictingSignalKeys].sort();
    body.properties.conversion_current_conflicting_signal_keys = [...conversionDedupPlan.currentConflictingSignalKeys || []].sort();
    body.properties.conversion_conflict_quarantined_from_identity_links = conversionHasIdentityOrClickConflict;
  }
  if (storedAttribution) {
    // Attribution-survival contract: stored first-party continuity attribution still
    // merges into server event properties so dbt direct-path Google uploads keep their
    // click IDs. applyAttributionProvenance below labels these keys as recovered so
    // they never masquerade as freshly observed evidence.
    for (const [k, v] of Object.entries(storedAttribution)) {
      if (KV_INTERNAL_FIELDS.has(k))
        continue;
      if (!body.properties[k] && v)
        body.properties[k] = v;
    }
  }
  if (env.GCLID_KV && storedAttribution && canUseAttribution) {
    if (eventName === "OS_purchase") {
      ctx.waitUntil(Promise.all([
        userId && !conversionHasUserIdentityConflict ? storeAttribution(env, KV_TRUSTED_SERVER_USER_PREFIX + userId, storedAttribution).catch(console.error) : Promise.resolve(),
        orderId && !conversionHasOrderIdentityConflict ? storeAttribution(env, KV_TRUSTED_SERVER_ORDER_PREFIX + orderId, storedAttribution).catch(console.error) : Promise.resolve()
      ]));
    }
    if ((eventName === "OS_order_delivered" || eventName === "reorder_completed") && userId) {
      ctx.waitUntil(
        storeAttribution(env, KV_TRUSTED_SERVER_USER_PREFIX + userId, storedAttribution).catch((err) => console.error("[eden-analytics] delivery user-link:", err))
      );
    }
  }
  const segmentEventTimestamp = conversionDedupPlan?.segmentEventTimestamp || producerEventTimestamp;
  body.timestamp = segmentEventTimestamp;
  if (conversionDedupPlan) body.originalTimestamp = segmentEventTimestamp;
  else if (!body.originalTimestamp || !Number.isFinite(Date.parse(String(body.originalTimestamp)))) body.originalTimestamp = producerEventTimestamp;
  body.properties.edge_received_at = serverReceivedAt;
  const attribution = {
    ...storedAttribution ? stripInternalFields(storedAttribution) : {},
    ...serverCurrentAttribution
  };
  if (env.GCLID_KV && canUseAttribution && eventName === "OS_purchase" && Object.keys(attribution).length) {
    ctx.waitUntil(Promise.all([
      userId && !conversionHasUserIdentityConflict ? storeAttribution(env, KV_TRUSTED_SERVER_USER_PREFIX + userId, attribution).catch((err) => console.error("[eden-analytics] authenticated purchase user continuity:", err)) : Promise.resolve(),
      orderId && !conversionHasOrderIdentityConflict ? storeAttribution(env, KV_TRUSTED_SERVER_ORDER_PREFIX + orderId, attribution).catch((err) => console.error("[eden-analytics] authenticated purchase order continuity:", err)) : Promise.resolve()
    ]));
  }
  // serverCurrentAttribution was built before the stored-attribution properties merge,
  // so it is the event-native view; context.campaign stays event-native and recovered
  // values are provenance-labeled instead of stamped as freshly observed.
  const serverEventNativeAttribution = serverCurrentAttribution;
  const campaignProps = buildCampaignContext(serverEventNativeAttribution);
  enrichPropertiesWithAttribution(body.properties, campaignProps, attribution);
  enrichPropertiesWithTouchModel(
    body.properties,
    storedAttribution || attribution,
    Object.keys(serverCurrentAttribution).length ? serverCurrentAttribution : null,
    storedAttribution ? "stored_attribution" : "current_event_fallback"
  );
  enrichPropertiesWithSession(body.properties, session);
  if (!body.context)
    body.context = {};
  body.context.campaign = { ...(body.context || {}).campaign || {}, ...campaignProps };
  applyAttributionProvenance(body, attribution, serverEventNativeAttribution);
  const superProps = {
    portal: "patient",
    source_type: "server",
    gpc_opt_out: gpcOptOut,
    attribution_suppressed: !canUseAttribution,
    pipeline_version: PIPELINE_VERSION,
    enrichment_version: ENRICHMENT_VERSION,
    legacy_identity_kv_recovery: false,
    stable_identity_key_type: identity.stableIdentityKeyType,
    ...sessionSuperProps(session),
    ...identity.identityWarning ? { identity_warning: identity.identityWarning } : {},
    ...!anonId && !userId ? { identity_warning: "no_identity_provided" } : {}
  };
  const serverAdClickMemoryBody = conversionHasIdentityOrClickConflict ? {
    type: "track",
    event: eventName || "server_collect",
    timestamp: body.timestamp,
    originalTimestamp: body.originalTimestamp,
    properties: compactDefined({
      page_url: body.properties?.page_url,
      landing_page: body.properties?.landing_page
    }),
    context: compactDefined({ page: body.context?.page ? { ...body.context.page } : null })
  } : body;
  let serverAdClickMemory = null;
  if (canUseAttribution) {
    try {
      serverAdClickMemory = await safeBuildAdClickMemoryCandidate({
        request,
        env,
        body: serverAdClickMemoryBody,
        anonId: conversionHasIdentityOrClickConflict ? null : anonId,
        session: conversionHasIdentityOrClickConflict ? null : session,
        attribution: conversionHasIdentityOrClickConflict ? diagnosticServerCurrentAttribution : attribution,
        sourceType: "server",
        eventName: eventName || "server_collect",
        userId: conversionHasIdentityOrClickConflict ? null : userId,
        orderId: conversionHasIdentityOrClickConflict ? null : orderId,
        linkReason: conversionHasIdentityOrClickConflict
          ? "server_conversion_conflict_observation"
          : CONVERSION_EVENTS.has(eventName) ? "server_conversion_event" : "server_event",
        eventNativeAttribution: conversionHasIdentityOrClickConflict ? diagnosticServerCurrentAttribution : serverEventNativeAttribution,
        // Snapshot retry identity follows the stable base conversion key, not an
        // enrichment message ID. The immutable seed still includes ad_click_id and
        // click hash, so genuinely different evidence remains a different snapshot.
        observationIdempotencyKey: conversionDedupPlan?.baseSegmentMessageId || null,
        // A pending-persistence retry must rebuild the exact same Queue envelope.
        // Reuse the first attempt's observation time rather than edge receipt time
        // when the producer did not provide its own stable event timestamp.
        observationTimestampOverride: conversionDedupPlan?.pendingPersistenceIntent?.observed_at || null
      }, "server-collect", { failClosed: !!conversionDedupPlan });
    } catch (err) {
      // A healthy build can legitimately return null when this conversion has
      // no ad evidence or owned pointer. A dependency/build exception is
      // different: accepting Segment and finalizing the conversion would make
      // the lost click observation unrecoverable because the producer would
      // receive success. Fail before Segment so the stable conversion key can
      // be retried after the attribution dependency recovers.
      console.error(JSON.stringify({
        worker: "eden-analytics",
        event: "conversion_ad_click_memory_build_failed",
        source_pipeline_version: PIPELINE_VERSION,
        reason: "attribution_dependency_or_candidate_build_error"
      }));
      const failedHeaders = new Headers({
        "Content-Type": "application/json",
        "Cache-Control": "no-store",
        "Retry-After": "5",
        ...corsHeadersObj(request.headers.get("Origin") || "")
      });
      appendAttributionPermissionCookies(failedHeaders, new URL(request.url), attributionPermission);
      return new Response(JSON.stringify({
        ok: false,
        error: "conversion_ad_click_memory_build_unavailable",
        retryable: true,
        segment_forwarded: false
      }), { status: 503, headers: failedHeaders });
    }
  }
  let serverAdClickPersistence = null;
  if (conversionDedupPlan?.persistencePending && serverAdClickMemory) {
    serverAdClickMemory = applyPendingPersistenceRetryMetadata(
      serverAdClickMemory,
      conversionDedupPlan.pendingPersistenceIntent
    );
  }
  const conversionPersistenceIntent = conversionDedupPlan
    ? await buildConversionPersistenceIntent(serverAdClickMemory, env)
    : null;
  if (conversionDedupPlan?.persistencePending) {
    const persistenceRetry = compareConversionPersistenceIntent(
      conversionDedupPlan.pendingPersistenceIntent,
      conversionPersistenceIntent
    );
    if (!persistenceRetry.compatible) {
      console.warn(JSON.stringify({
        worker: "eden-analytics",
        event: "conversion_persistence_retry_rejected",
        reason: persistenceRetry.reason,
        source_pipeline_version: PIPELINE_VERSION
      }));
      const retryHeaders = new Headers({ "Content-Type": "application/json", ...corsHeadersObj(request.headers.get("Origin") || "") });
      appendAttributionPermissionCookies(retryHeaders, new URL(request.url), attributionPermission);
      return new Response(JSON.stringify({
        ok: false,
        error: "conversion_persistence_retry_incomplete_or_conflicting",
        retryable: true,
        refresh_required: true,
        segment_forwarded: true,
        persistence_retry_reason: persistenceRetry.reason
      }), { status: 409, headers: retryHeaders });
    }
  }
  if (serverAdClickMemory && !conversionHasIdentityOrClickConflict) {
    if (shouldAnnotateAdClickMemoryPayload(env)) applyAdClickMemoryToBody(body, serverAdClickMemory);
  }
  ensureExtendedEventContext(body, request, anonId, session, attribution, superProps, extendedEnrichmentEnabled, enrichmentState.mode, enrichmentState.canary, serverEventNativeAttribution);
  // Extended context deliberately adds first-party continuity fields. Reapply
  // cross-delivery conflict quarantine afterwards so a conflicting anonymous
  // claim cannot re-enter Segment through properties/context enrichment.
  if (conversionHasAnonymousIdentityConflict) quarantineConflictingAnonymousIdentityClaims(body);
  if (conversionHasUserIdentityConflict) quarantineConflictingServerUserIdentityClaims(body);
  if (conversionHasOrderIdentityConflict) quarantineConflictingOrderIdentityClaims(body);
  if (!canUseAttribution) scrubAdvertisingAttributionFromBody(body);
  const serverSessionId = sessionRawValue(session);
  const serverSegmentAnonymousId = conversionHasAnonymousIdentityConflict
    ? (orderId && !conversionHasOrderIdentityConflict
      ? `eden_order_${(await sha256Raw(orderId)).slice(0, 32)}`
      : !conversionHasUserIdentityConflict && userId
        ? `eden_user_${(await sha256Raw(userId)).slice(0, 32)}`
        : null)
    : anonId
      || (!conversionHasUserIdentityConflict ? userId : null)
      || (serverSessionId ? `eden_session_${(await sha256Raw(serverSessionId)).slice(0, 32)}` : null)
      || (orderId && !conversionHasOrderIdentityConflict ? `eden_order_${(await sha256Raw(orderId)).slice(0, 32)}` : null)
      || (isConversionEvent && conversionKeyDetails?.rawValue ? `eden_transaction_${(await sha256Raw(conversionKeyDetails.rawValue)).slice(0, 32)}` : null);
  // Authenticated producers also emit operational events that legitimately do
  // not have a person, order, browser session, or conversion transaction yet.
  // Segment still requires either userId or anonymousId for those envelopes.
  // Give only that individual event an opaque delivery identity instead of
  // dropping it or collapsing unrelated traffic onto a shared "server" user.
  // This value is never used for attribution lookup, identity linking, KV,
  // Durable Object ownership, or ad-click memory.
  const serverEventScopedDeliveryId = !serverSegmentAnonymousId
    && eventName
    && Object.keys(serverEventNativeAttribution || {}).length === 0
    ? `eden_event_${(await sha256Raw([
      eventName,
      String(body.messageId || body.message_id || body.eventId || body.event_id || body.originalTimestamp || body.timestamp || serverReceivedAt),
      JSON.stringify(body.properties || {}).slice(0, 4096)
    ].join("\0"))).slice(0, 32)}`
    : null;
  const serverSegmentDeliveryId = serverSegmentAnonymousId || serverEventScopedDeliveryId;
  if (serverEventScopedDeliveryId) {
    body.properties.event_scoped_delivery_identity = true;
    body.properties.identity_scope = "event_only";
    superProps.identity_warning = "event_scoped_delivery_identity";
  }
  const synchronousServerSegmentDelivery = String(env.EDEN_SERVER_SEGMENT_DELIVERY_MODE || "async").trim().toLowerCase() === "sync";
  let segmentForwarded = false;
  let conversionRepairEnrichmentForwarded = false;
  let acknowledgedBaseBeforeRepairEnrichment = null;
  if (conversionDedupPlan?.skipSegmentDelivery) {
    // Segment already acknowledged this exact signal state. The prior request
    // failed only while persisting click evidence or a compatibility ledger,
    // so resume after delivery rather than replaying the business event.
    segmentForwarded = true;
  } else if (env.SEGMENT_WRITE_KEY && serverSegmentAnonymousId && conversionDedupPlan) {
    if (conversionDedupPlan.isEnrichment) {
      markConversionEnrichmentPayload(body, eventName, conversionDedupPlan);
    }
    let currentSegmentPayload;
    try {
      currentSegmentPayload = await buildSegmentTrackPayload(
        body,
        serverSegmentAnonymousId,
        superProps,
        canUseAttribution ? serverEventNativeAttribution : {},
        conversionDedupPlan.segmentMessageId
      );
      if (!validPendingSegmentTrackPayload(currentSegmentPayload, conversionDedupPlan.segmentMessageId)) {
        throw new Error("conversion_segment_payload_invalid");
      }
    } catch (err) {
      console.error("[eden-analytics] conversion Segment payload build failed:", err);
      const failedHeaders = new Headers({ "Content-Type": "application/json", ...corsHeadersObj(request.headers.get("Origin") || "") });
      appendAttributionPermissionCookies(failedHeaders, new URL(request.url), attributionPermission);
      return new Response(JSON.stringify({
        ok: false,
        error: "conversion_delivery_payload_invalid",
        retryable: true,
        segment_forwarded: false
      }), { status: 503, headers: failedHeaders });
    }
    const segmentPayloadForAttempt = conversionDedupPlan.replayPendingAttempt?.segmentPayload || currentSegmentPayload;
    try {
      // Persist the attempted signal state in the strongly consistent order
      // ledger before delivery. If Segment accepts but its response is lost, a
      // later changed-status retry can replay the stable base message and send
      // the new truth separately as an enrichment instead of silently losing it.
      const pendingAttemptState = conversionDedupPlan.replayPendingAttempt
        ? {
          hashes: conversionDedupPlan.replayPendingAttempt.signalHashes,
          statusRanks: conversionDedupPlan.replayPendingAttempt.statusRanks
        }
        : buildPendingConversionAttemptState(
          conversionDedupPlan.mergedSignalHashes,
          conversionDedupPlan.mergedStatusRanks,
          conversionDedupPlan.currentSignalState,
          conversionDedupPlan.acceptedSignalKeys
        );
      const pendingSignalKeys = conversionDedupPlan.replayPendingAttempt
        ? conversionDedupPlan.replayPendingAttempt.signalKeys
        : [...conversionDedupPlan.acceptedSignalKeys].sort();
      const pendingDiagnosticConflictSignalKeys = conversionDedupPlan.replayPendingAttempt
        ? conversionDedupPlan.replayPendingAttempt.diagnosticConflictSignalKeys
        : conversionDedupPlan.currentDiagnosticConflictSignalKeys
          .filter((key) => conversionDedupPlan.acceptedSignalKeys.includes(key))
          .sort();
      await writeConversionCoordinatorRecord(conversionCoordinatorLease, eventName, {
        ...conversionDedupPlan.record,
        signal_hashes: conversionDedupPlan.acceptedBeforeAttemptSignalHashes,
        status_ranks: conversionDedupPlan.acceptedBeforeAttemptStatusRanks,
        accepted_signal_hashes: conversionDedupPlan.acceptedBeforeAttemptSignalHashes,
        accepted_status_ranks: conversionDedupPlan.acceptedBeforeAttemptStatusRanks,
        signal_count: Object.keys(conversionDedupPlan.acceptedBeforeAttemptSignalHashes).length,
        pending_signal_hashes: pendingAttemptState.hashes,
        pending_status_ranks: pendingAttemptState.statusRanks,
        pending_signal_keys: pendingSignalKeys,
        pending_diagnostic_conflict_signal_keys: pendingDiagnosticConflictSignalKeys,
        pending_payload_fingerprint_sha256: conversionDedupPlan.replayPendingAttempt?.payloadFingerprintSha256
          || conversionDedupPlan.currentSignalState.payloadFingerprintSha256,
        pending_segment_payload: segmentPayloadForAttempt,
        pending_delivery_kind: conversionDedupPlan.isEnrichment ? "enrichment" : "base",
        pending_event_timestamp: conversionDedupPlan.segmentEventTimestamp,
        pending_message_id: conversionDedupPlan.segmentMessageId,
        ...(conversionDedupPlan.repairEnrichmentSignalKeys.length
          ? { pending_repair_enrichment_signal_keys: [...conversionDedupPlan.repairEnrichmentSignalKeys].sort() }
          : {}),
        delivery_state: "segment_delivery_unacknowledged",
        delivery_event: conversionDedupPlan.isEnrichment ? `${eventName}_enrichment` : eventName
      });
    } catch (err) {
      console.error("[eden-analytics] conversion coordinator attempt record failed:", err);
      const failedHeaders = new Headers({ "Content-Type": "application/json", ...corsHeadersObj(request.headers.get("Origin") || "") });
      appendAttributionPermissionCookies(failedHeaders, new URL(request.url), attributionPermission);
      return new Response(JSON.stringify({
        ok: false,
        error: "conversion_coordinator_unavailable",
        retryable: true,
        segment_forwarded: false
      }), { status: 503, headers: failedHeaders });
    }
    try {
      // Business conversions are acknowledged synchronously. A transient
      // Segment failure returns retryable 503 and leaves no acknowledged dedupe
      // record. The stable message id makes an unknown-commit retry safe.
      await forwardToSegment(
        env.SEGMENT_WRITE_KEY,
        body,
        serverSegmentAnonymousId,
        superProps,
        canUseAttribution ? serverEventNativeAttribution : {},
        {
          messageId: conversionDedupPlan.segmentMessageId,
          timeoutMs: CONVERSION_SEGMENT_TIMEOUT_MS,
          prebuiltPayload: segmentPayloadForAttempt
        }
      );
      segmentForwarded = true;
      if (conversionDedupPlan.repairEnrichmentMessageId) {
        // The prior base delivery may have reached Segment even though the old
        // ledger lacks acknowledgement. Replay the stable base message for
        // repair, then send any newly accepted lifecycle/product/value truth as
        // a non-conversion enrichment. If Segment dedupes the unknown-commit
        // base replay, the correction still lands exactly once under its own
        // stable enrichment message ID.
        // The base replay is now definitively acknowledged. Persist that fact
        // before even constructing/staging the independent enrichment so a
        // payload-build or delivery failure can never roll the coordinator
        // back past an accepted purchase and cause the base to be sent again.
        acknowledgedBaseBeforeRepairEnrichment = {
          ...conversionDedupPlan.record,
          signal_hashes: conversionDedupPlan.acceptedBeforeAttemptSignalHashes,
          status_ranks: conversionDedupPlan.acceptedBeforeAttemptStatusRanks,
          accepted_signal_hashes: conversionDedupPlan.acceptedBeforeAttemptSignalHashes,
          accepted_status_ranks: conversionDedupPlan.acceptedBeforeAttemptStatusRanks,
          signal_count: Object.keys(conversionDedupPlan.acceptedBeforeAttemptSignalHashes).length,
          delivery_state: "segment_acknowledged",
          delivery_event: eventName
        };
        await writeConversionCoordinatorRecord(
          conversionCoordinatorLease,
          eventName,
          acknowledgedBaseBeforeRepairEnrichment
        );
        const repairEnrichmentBody = JSON.parse(JSON.stringify(body));
        repairEnrichmentBody.timestamp = conversionDedupPlan.repairEnrichmentEventTimestamp;
        repairEnrichmentBody.originalTimestamp = conversionDedupPlan.repairEnrichmentEventTimestamp;
        markConversionEnrichmentPayload(repairEnrichmentBody, eventName, {
          ...conversionDedupPlan,
          acceptedSignalKeys: conversionDedupPlan.repairEnrichmentSignalKeys,
          segmentMessageId: conversionDedupPlan.repairEnrichmentMessageId
        });
        const repairEnrichmentPayload = await buildSegmentTrackPayload(
          repairEnrichmentBody,
          serverSegmentAnonymousId,
          superProps,
          canUseAttribution ? serverEventNativeAttribution : {},
          conversionDedupPlan.repairEnrichmentMessageId
        );
        if (!validPendingSegmentTrackPayload(repairEnrichmentPayload, conversionDedupPlan.repairEnrichmentMessageId)) {
          throw new Error("conversion_repair_enrichment_payload_invalid");
        }
        // This is a second independent Segment delivery. Stage it separately
        // after the base/pending replay is acknowledged so an ambiguous
        // enrichment response never sends the base purchase again.
        const pendingRepairEnrichmentState = buildPendingConversionAttemptState(
          conversionDedupPlan.mergedSignalHashes,
          conversionDedupPlan.mergedStatusRanks,
          conversionDedupPlan.currentSignalState,
          conversionDedupPlan.repairEnrichmentSignalKeys
        );
        await writeConversionCoordinatorRecord(conversionCoordinatorLease, eventName, {
          ...conversionDedupPlan.record,
          signal_hashes: conversionDedupPlan.acceptedBeforeAttemptSignalHashes,
          status_ranks: conversionDedupPlan.acceptedBeforeAttemptStatusRanks,
          accepted_signal_hashes: conversionDedupPlan.acceptedBeforeAttemptSignalHashes,
          accepted_status_ranks: conversionDedupPlan.acceptedBeforeAttemptStatusRanks,
          signal_count: Object.keys(conversionDedupPlan.acceptedBeforeAttemptSignalHashes).length,
          pending_signal_hashes: pendingRepairEnrichmentState.hashes,
          pending_status_ranks: pendingRepairEnrichmentState.statusRanks,
          pending_signal_keys: [...conversionDedupPlan.repairEnrichmentSignalKeys].sort(),
          pending_diagnostic_conflict_signal_keys: conversionDedupPlan.currentDiagnosticConflictSignalKeys
            .filter((key) => conversionDedupPlan.repairEnrichmentSignalKeys.includes(key))
            .sort(),
          pending_payload_fingerprint_sha256: conversionDedupPlan.currentSignalState.payloadFingerprintSha256,
          pending_segment_payload: repairEnrichmentPayload,
          pending_delivery_kind: "enrichment",
          pending_message_id: conversionDedupPlan.repairEnrichmentMessageId,
          pending_event_timestamp: conversionDedupPlan.repairEnrichmentEventTimestamp,
          delivery_state: "segment_delivery_unacknowledged",
          delivery_event: `${eventName}_enrichment`
        });
        await forwardToSegment(
          env.SEGMENT_WRITE_KEY,
          repairEnrichmentBody,
          serverSegmentAnonymousId,
          superProps,
          canUseAttribution ? serverEventNativeAttribution : {},
          {
            messageId: conversionDedupPlan.repairEnrichmentMessageId,
            timeoutMs: CONVERSION_SEGMENT_TIMEOUT_MS,
            prebuiltPayload: repairEnrichmentPayload
          }
        );
        conversionRepairEnrichmentForwarded = true;
      }
    } catch (err) {
      console.error("[eden-analytics] conversion Segment delivery failed:", err);
      if (err?.segmentDefinitiveRejection === true) {
        try {
          // Segment explicitly rejected the request, so this is not an unknown
          // commit. Restore the exact prior authoritative record; only network
          // or timeout ambiguity keeps the pending attempt for correction-safe
          // replay.
          if (acknowledgedBaseBeforeRepairEnrichment && segmentForwarded) {
            await writeConversionCoordinatorRecord(
              conversionCoordinatorLease,
              eventName,
              acknowledgedBaseBeforeRepairEnrichment
            );
          } else {
            await restoreConversionCoordinatorRecord(conversionCoordinatorLease, eventName);
          }
        } catch (restoreError) {
          console.error("[eden-analytics] conversion coordinator definitive-rejection restore failed:", restoreError);
        }
      }
      const failedHeaders = new Headers({ "Content-Type": "application/json", ...corsHeadersObj(request.headers.get("Origin") || "") });
      appendAttributionPermissionCookies(failedHeaders, new URL(request.url), attributionPermission);
      return new Response(JSON.stringify({
        ok: false,
        error: "conversion_delivery_failed",
        retryable: true,
        segment_forwarded: segmentForwarded
      }), { status: 503, headers: failedHeaders });
    }
  } else if (conversionDedupPlan) {
    const failedHeaders = new Headers({ "Content-Type": "application/json", ...corsHeadersObj(request.headers.get("Origin") || "") });
    appendAttributionPermissionCookies(failedHeaders, new URL(request.url), attributionPermission);
    return new Response(JSON.stringify({
      ok: false,
      error: "conversion_delivery_unavailable",
      retryable: true,
      segment_forwarded: false
    }), { status: 503, headers: failedHeaders });
  } else if (env.SEGMENT_WRITE_KEY && serverSegmentDeliveryId) {
    if (synchronousServerSegmentDelivery) {
      try {
        await forwardToSegment(
          env.SEGMENT_WRITE_KEY,
          body,
          serverSegmentDeliveryId,
          superProps,
          canUseAttribution ? serverEventNativeAttribution : {}
        );
        segmentForwarded = true;
      } catch (err) {
        console.error(JSON.stringify({
          worker: "eden-analytics",
          event: "server_segment_delivery_failed",
          retryable: true,
          event_name: eventName || null,
          reason: String(err?.message || "unknown").slice(0, 120)
        }));
        const failedHeaders = new Headers({
          "Content-Type": "application/json",
          "Cache-Control": "no-store",
          "Retry-After": "5",
          ...corsHeadersObj(request.headers.get("Origin") || "")
        });
        appendAttributionPermissionCookies(failedHeaders, new URL(request.url), attributionPermission);
        return new Response(JSON.stringify({
          ok: false,
          error: "server_segment_delivery_failed",
          retryable: true,
          segment_forwarded: false
        }), { status: 503, headers: failedHeaders });
      }
    } else {
      segmentForwarded = true;
      ctx.waitUntil(
        forwardToSegment(
          env.SEGMENT_WRITE_KEY,
          body,
          serverSegmentDeliveryId,
          superProps,
          canUseAttribution ? serverEventNativeAttribution : {}
        ).catch((err) => console.error("[eden-analytics] server-collect error:", err))
      );
    }
  } else if (!env.SEGMENT_WRITE_KEY && synchronousServerSegmentDelivery) {
    console.error(JSON.stringify({ worker: "eden-analytics", event: "server_segment_configuration_error" }));
    return new Response(JSON.stringify({
      ok: false,
      error: "server_segment_delivery_unavailable",
      retryable: true,
      segment_forwarded: false
    }), {
      status: 503,
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": "no-store",
        "Retry-After": "5",
        ...corsHeadersObj(request.headers.get("Origin") || "")
      }
    });
  }
  if (conversionDedupPlan && segmentForwarded && !conversionDedupPlan.skipSegmentDelivery) {
    try {
      // Segment has acknowledged every signal represented by the merged
      // record, but click/compatibility persistence still must finish. This
      // intermediate state lets retries resume after Segment without replaying
      // an already accepted conversion.
      await writeConversionCoordinatorRecord(conversionCoordinatorLease, eventName, {
        ...conversionDedupPlan.record,
        persistence_intent: conversionPersistenceIntent,
        delivery_state: "segment_acknowledged_pending_persistence",
        delivery_event: conversionDedupPlan.isEnrichment ? `${eventName}_enrichment` : eventName
      });
    } catch (err) {
      console.error("[eden-analytics] conversion coordinator Segment-ack record failed:", err);
      const failedHeaders = new Headers({ "Content-Type": "application/json", ...corsHeadersObj(request.headers.get("Origin") || "") });
      appendAttributionPermissionCookies(failedHeaders, new URL(request.url), attributionPermission);
      return new Response(JSON.stringify({
        ok: false,
        error: "conversion_coordinator_commit_failed",
        retryable: true,
        segment_forwarded: true
      }), { status: 503, headers: failedHeaders });
    }
  }
  if (serverAdClickMemory && conversionDedupPlan) {
    try {
      const persistence = await persistAdClickMemory(env, serverAdClickMemory);
      serverAdClickPersistence = persistence;
      const requiresQueue = conversionPersistenceIntent?.queue_required === true;
      const requiresKv = conversionPersistenceIntent?.kv_required === true;
      if (requiresQueue && !persistence.queue_enqueued) throw new Error("conversion_ad_click_queue_enqueue_failed");
      if (requiresKv && !persistence.kv_persisted) throw new Error("conversion_ad_click_kv_persist_failed");
    } catch (err) {
      // Segment has accepted the stable message id, but the conversion's click
      // observation/link is not durable yet. The coordinator remains at
      // segment_acknowledged_pending_persistence, so an exact retry resumes
      // after Segment; deterministic snapshot IDs keep Queue retry idempotent.
      console.error("[eden-analytics] conversion ad-click persistence failed:", err);
      const failedHeaders = new Headers({ "Content-Type": "application/json", ...corsHeadersObj(request.headers.get("Origin") || "") });
      appendAttributionPermissionCookies(failedHeaders, new URL(request.url), attributionPermission);
      return new Response(JSON.stringify({
        ok: false,
        error: "conversion_ad_click_persistence_failed",
        retryable: true,
        segment_forwarded: true
      }), { status: 503, headers: failedHeaders });
    }
  }
  if (conversionDedupPlan && env.GCLID_KV) {
    try {
      const serializedRecord = JSON.stringify(conversionDedupPlan.record);
      await Promise.all([
        env.GCLID_KV.put(
          conversionDedupPlan.dedupKey,
          serializedRecord,
          { expirationTtl: KV_DEDUP_TTL }
        ),
        conversionDedupPlan.legacyDedupKey
          ? env.GCLID_KV.put(conversionDedupPlan.legacyDedupKey, serializedRecord, { expirationTtl: KV_DEDUP_TTL })
          : Promise.resolve()
      ]);
    } catch (err) {
      // Segment already accepted the stable message id. Ask the producer to
      // retry until the KV compatibility mirror commits; the coordinator's
      // pending-persistence stage prevents a duplicate base delivery.
      console.error("[eden-analytics] conversion dedupe commit failed:", err);
      const failedHeaders = new Headers({ "Content-Type": "application/json", ...corsHeadersObj(request.headers.get("Origin") || "") });
      appendAttributionPermissionCookies(failedHeaders, new URL(request.url), attributionPermission);
      return new Response(JSON.stringify({
        ok: false,
        error: "conversion_dedupe_commit_failed",
        retryable: true,
        segment_forwarded: true
      }), { status: 503, headers: failedHeaders });
    }
  }
  if (conversionDedupPlan) {
    try {
      await writeConversionCoordinatorRecord(
        conversionCoordinatorLease,
        eventName,
        conversionDedupPlan.record
      );
    } catch (err) {
      // The Segment message and KV compatibility mirror may already be
      // committed. Keep the DO record at pending persistence and ask for a
      // stable-id retry; it resumes after Segment and repairs canonical state.
      console.error("[eden-analytics] conversion coordinator commit failed:", err);
      const failedHeaders = new Headers({ "Content-Type": "application/json", ...corsHeadersObj(request.headers.get("Origin") || "") });
      appendAttributionPermissionCookies(failedHeaders, new URL(request.url), attributionPermission);
      return new Response(JSON.stringify({
        ok: false,
        error: "conversion_coordinator_commit_failed",
        retryable: true,
        segment_forwarded: true
      }), { status: 503, headers: failedHeaders });
    }
  }
  if (serverAdClickMemory && !conversionDedupPlan) {
    try {
      serverAdClickPersistence = await persistAdClickMemory(env, serverAdClickMemory);
    } catch (err) {
      console.error("[eden-analytics] ad-click server persist error:", err);
      const failedHeaders = new Headers({ "Content-Type": "application/json", "Cache-Control": "no-store", "Retry-After": "5", ...corsHeadersObj(request.headers.get("Origin") || "") });
      appendAttributionPermissionCookies(failedHeaders, new URL(request.url), attributionPermission);
      return new Response(JSON.stringify({ ok: false, error: "ad_click_memory_custody_unavailable", retryable: true, segment_forwarded: segmentForwarded }), { status: 503, headers: failedHeaders });
    }
  }
  const origin = request.headers.get("Origin") || "";
  const serverRespHeaders = new Headers({ "Content-Type": "application/json", ...corsHeadersObj(origin) });
  if (serverAdClickMemory?.setPointerCookie && serverAdClickPersistence?.pointer_committed === true
    && (!serverAdClickMemory.snapshot || serverAdClickPersistence.queue_enqueued === true)) {
    serverRespHeaders.append("Set-Cookie", buildAdClickPointerCookie(serverAdClickMemory.ad_click_id, new URL(request.url), env));
  }
  appendAttributionPermissionCookies(serverRespHeaders, new URL(request.url), attributionPermission);
  return new Response(JSON.stringify({
    ok: true,
    segment_forwarded: segmentForwarded,
    ...(conversionDedupPlan ? { conversion_idempotency_key_source: conversionKeyDetails.source } : {}),
    ...(conversionDedupPlan?.skipSegmentDelivery ? { conversion_segment_delivery_reused: true } : {}),
    ...(conversionDedupPlan?.isEnrichment || conversionRepairEnrichmentForwarded ? {
      conversion_enrichment_forwarded: true,
      conversion_enrichment_event: `${eventName}_enrichment`
    } : {})
  }), {
    status: 200,
    headers: serverRespHeaders
  });
  } finally {
    await releaseConversionCoordinatorLease(conversionCoordinatorLease);
  }
}
__name(handleServerCollect, "handleServerCollect");
async function handleIdentify(request, env, ctx) {
  const origin = request.headers.get("Origin") || "";
  if (origin && !isAllowedOrigin(origin))
    return new Response("Forbidden", { status: 403 });
  const authFailure = await authorizeBrowserMutationRequest(request, env, "identify");
  if (authFailure) return authFailure;
  const parsedBody = await parseBoundedJsonRequest(request);
  if (parsedBody.response) return parsedBody.response;
  const body = parsedBody.value;
  // Browser code cannot prove a stable Eden user, order, group, email, or phone.
  // Treat this legacy endpoint as anonymous consent/session compatibility only;
  // stable identity attachment belongs to authenticated /server-collect producers.
  scrubUntrustedBrowserIdentityClaims(body);
  const cookieAnonId = readCanonicalAnonymousId(request);
  const existingSessionRaw = readCookie(request, "eden_session_id");
  if (!cookieAnonId && existingSessionRaw) {
    return new Response(JSON.stringify({ ok: false, error: "browser_owner_cookie_required" }), {
      status: 409,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store", ...corsHeadersObj(origin) }
    });
  }
  const anonId = cookieAnonId || crypto.randomUUID();
  const sessionRaw = existingSessionRaw || `${crypto.randomUUID()}_${Date.now()}`;
  const identifySession = buildSessionContext(sessionRaw, "eden_session_cookie", "identify", body);
  if (!identifySession) {
    return new Response(JSON.stringify({ ok: false, error: "browser_session_cookie_invalid" }), {
      status: 409,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store", ...corsHeadersObj(origin) }
    });
  }
  const attributionPermission = await resolveAttributionPermissionWithDurableState(
    env,
    request,
    body,
    { anonId, session: identifySession, userId: null, orderId: null }
  );
  if (!attributionPermission.allowed) scrubAdvertisingAttributionFromBody(body);
  const headers = new Headers({
    "Content-Type": "application/json",
    "Cache-Control": "no-store",
    ...corsHeadersObj(origin)
  });
  const requestUrl = new URL(request.url);
  if (!cookieAnonId || anonymousCookieAliasesNeedSync(request)) {
    headers.append("Set-Cookie", buildAnonCookie(anonId, requestUrl));
    headers.append("Set-Cookie", buildLegacyAnonCookie(anonId, requestUrl));
  }
  headers.append("Set-Cookie", buildSessionCookie(sessionRaw, requestUrl));
  if (!existingSessionRaw) {
    const browserHost = browserCapabilityOriginHost(request);
    if (!browserHost) {
      return new Response(JSON.stringify({ ok: false, error: "browser_owner_cookie_required" }), {
        status: 409,
        headers: { "Content-Type": "application/json", "Cache-Control": "no-store", ...corsHeadersObj(origin) }
      });
    }
    try {
      headers.append("Set-Cookie", buildBrowserCapabilityCookie(await mintBrowserCapability(env, {
        anonId,
        session: sessionRaw,
        browserHost
      })));
    } catch (error) {
      console.error(JSON.stringify({
        worker: "eden-analytics",
        event: "browser_identify_bootstrap_failed",
        reason: String(error?.message || "unknown").slice(0, 120)
      }));
      return new Response(JSON.stringify({ ok: false, error: "browser_authentication_unavailable", retryable: true }), {
        status: 503,
        headers: { "Content-Type": "application/json", "Cache-Control": "no-store", "Retry-After": "5", ...corsHeadersObj(origin) }
      });
    }
  }
  appendAttributionPermissionCookies(headers, requestUrl, attributionPermission);
  return new Response(JSON.stringify({
    ok: true,
    skipped: "browser_stable_identity_not_authorized",
    identity_authority: "authenticated_server_collect_only",
    stable_identity_accepted: false
  }), {
    status: 200,
    headers
  });
}
__name(handleIdentify, "handleIdentify");
async function handlePreserveAttribution(request, env, ctx) {
  const origin = request.headers.get("Origin") || "";
  if (origin && !isAllowedOrigin(origin))
    return new Response("Forbidden", { status: 403 });
  const authFailure = await authorizeBrowserMutationRequest(request, env, "preserve");
  if (authFailure) return authFailure;
  const parsedBody = await parseBoundedJsonRequest(request);
  if (parsedBody.response) return parsedBody.response;
  const body = parsedBody.value;
  scrubUntrustedBrowserIdentityClaims(body);
  const gpcOptOut = isGpcOptOut(request);
  const cookieAnonId = readCanonicalAnonymousId(request);
  const sessionRaw = readCookie(request, "eden_session_id");
  if (!cookieAnonId || !sessionRaw) {
    return new Response(JSON.stringify({ ok: false, error: "browser_owner_cookie_required" }), {
      status: 409,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store", ...corsHeadersObj(origin) }
    });
  }
  const anonId = cookieAnonId;
  const userId = null;
  const orderId = null;
  const preserveSession = buildSessionContext(sessionRaw, "eden_session_cookie", "preserve_attribution", body);
  if (!preserveSession) {
    return new Response(JSON.stringify({ ok: false, error: "browser_session_cookie_invalid" }), {
      status: 409,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store", ...corsHeadersObj(origin) }
    });
  }
  const attributionPermission = await resolveAttributionPermissionWithDurableState(env, request, body, { anonId, session: preserveSession, userId, orderId });
  const canUseAttribution = attributionPermission.allowed;
  const reqUrl = new URL(request.url);
  const respHeaders = new Headers({ "Content-Type": "application/json", ...corsHeadersObj(origin) });
  if (!canUseAttribution) {
    appendAttributionPermissionCookies(respHeaders, reqUrl, attributionPermission);
    return new Response(JSON.stringify({ ok: true, skipped: "attribution_suppressed", gpc_opt_out: gpcOptOut }), { status: 200, headers: respHeaders });
  }
  if (!env.GCLID_KV) {
    appendAttributionPermissionCookies(respHeaders, reqUrl, attributionPermission);
    return new Response(JSON.stringify({ ok: true, skipped: "no_kv" }), { status: 200, headers: respHeaders });
  }
  let browserPageAttribution = {};
  const browserPageUrl = body?.pageUrl || body?.page_url || body?.context?.page?.url || null;
  if (browserPageUrl) {
    try {
      const parsedPageUrl = new URL(browserPageUrl, request.url);
      if (isAllowedOrigin(parsedPageUrl.origin)) {
        const browserClickIds = extractClickIds(
          parsedPageUrl,
          request,
          resolveEnrichmentState(env, request, body).enabled
        );
        const browserUtms = extractUTMs(parsedPageUrl);
        browserPageAttribution = {
          ...browserClickIds,
          ...browserUtms || {},
          landing_page: sanitizeAdClickLandingUrl(parsedPageUrl).toString()
        };
      }
    } catch {
      // Malformed or non-Eden browser URLs are not persisted.
    }
  }
  const preserveCookieAttr = extractPreAuthAttribution(request);
  const storedAttribution = await resolveAttribution(
    env.GCLID_KV,
    anonId,
    userId,
    orderId,
    preserveCookieAttr
  );
  // Existing owner-bound attribution remains the immutable first-touch KV
  // truth. The browser's active-touch cookies must instead move to a genuinely
  // fresh fragment/query click so SPA cleanup cannot resurrect the old click.
  const attribution = storedAttribution || (Object.keys(browserPageAttribution).length ? browserPageAttribution : null);
  if (!attribution) {
    appendAttributionPermissionCookies(respHeaders, reqUrl, attributionPermission);
    return new Response(JSON.stringify({ ok: true, skipped: "no_attribution" }), { status: 200, headers: respHeaders });
  }
  const writes = [];
  if (anonId) {
    // Re-prove the owner-scoped attribution row on every authenticated
    // preserve, including a destination handoff after click parameters have
    // already been cleaned from the browser URL. storeAttribution keeps the
    // existing paid first touch immutable and only enriches missing context.
    writes.push(storeAttribution(env, KV_ANON_PREFIX + anonId, attribution));
  }
  // Browser preserve has no stable user/order authority. It writes only the
  // owner-cookie anonymous namespace; authenticated /server-collect owns the
  // trusted stable namespaces.
  // This authenticated browser endpoint is the pre-handoff durability barrier:
  // do not let the client navigate from Webflow until owner-scoped continuity
  // writes and the immutable observation enqueue have completed successfully.
  if (writes.length) await Promise.all(writes);
  const ownerAttributionKvPersisted = writes.length > 0;
  const activeAttribution = mergeAttributionPreferFreshPrimary(attribution, browserPageAttribution);
  const refreshedAttrValue = buildAttrCookieValue(activeAttribution);
  const preserveSessionRaw = sessionRawValue(preserveSession);
  if (preserveSessionRaw) respHeaders.append("Set-Cookie", buildSessionCookie(preserveSessionRaw, reqUrl));
  const preserveAdClickMemory = await safeBuildAdClickMemoryCandidate({
    request,
    env,
    body,
    anonId,
    session: preserveSession,
    attribution: activeAttribution,
    sourceType: "preserve_attribution",
    eventName: "preserve_attribution",
    userId,
    orderId,
    linkReason: "preserve_attribution",
    // The authenticated browser URL can supply fresh fragment evidence that
    // Cloudflare could not see on the original request. Cookie/KV-only values
    // remain recovery and are labeled accordingly.
    eventNativeAttribution: browserPageAttribution
  }, "preserve-attribution");
  const browserEvidence = classifyGoogleClickEvidence(browserPageAttribution);
  if (isAdClickMemoryEnabled(env) && browserEvidence.has_primary_click_evidence && !preserveAdClickMemory?.snapshot) {
    throw new Error("browser_observation_snapshot_build_failed");
  }
  let preservePersistence = { kv_persisted: false, queue_enqueued: false };
  let internalHandoffAssertion = null;
  let internalHandoffDurabilitySource = null;
  let internalHandoffPointerKvPersisted = false;
  if (preserveAdClickMemory) {
    preservePersistence = await persistAdClickMemory(env, preserveAdClickMemory);
    if (preserveAdClickMemory.snapshot && !preservePersistence.queue_enqueued) {
      throw new Error("browser_observation_queue_enqueue_failed");
    }
    if (preserveAdClickMemory.snapshot && preserveAdClickMemory.resolution?.ad_click_id_scope === "first_party_scoped" && !preservePersistence.kv_persisted) {
      throw new Error("browser_observation_pointer_kv_persist_failed");
    }
    if (preserveAdClickMemory.setPointerCookie && preservePersistence.pointer_committed === true) {
      respHeaders.append("Set-Cookie", buildAdClickPointerCookie(preserveAdClickMemory.ad_click_id, reqUrl, env));
    }
    if (
      body?.handoffDestination
      && preserveAdClickMemory.snapshot
      && preserveAdClickMemory.setPointerCookie
      && preservePersistence.pointer_committed === true
      && preservePersistence.queue_enqueued
      && preservePersistence.kv_persisted
      && ownerAttributionKvPersisted
    ) {
      try {
        internalHandoffAssertion = await mintInternalHandoffAssertion(env, {
          adClickId: preserveAdClickMemory.ad_click_id,
          anonId,
          session: preserveSession,
          destinationUrl: body.handoffDestination
        });
        internalHandoffDurabilitySource = "fresh_observation";
        internalHandoffPointerKvPersisted = true;
      } catch (error) {
        console.warn(JSON.stringify({
          worker: "eden-analytics",
          event: "internal_handoff_assertion_mint_failed",
          reason: String(error?.message || "unknown").slice(0, 120)
        }));
      }
    }
  }
  if (body?.handoffDestination && !internalHandoffAssertion && ownerAttributionKvPersisted) {
    // The URL may already be clean by the time a user clicks into HealthOS.
    // In that case there is no new observation to enqueue. Reuse only an
    // existing HttpOnly pointer whose backing KV record is present and still
    // owned by this exact anonymous/session context. Raw stored click evidence
    // is deliberately not used for this ownership check because first paid
    // touch and current/last paid pointer may legitimately differ.
    const existingOwnedPointer = await readOwnedAdClickPointer({
      env,
      request,
      anonId,
      session: preserveSession,
      userId,
      orderId,
      evidence: null
    });
    if (
      existingOwnedPointer.valid
      && existingOwnedPointer.record
      && existingOwnedPointer.record.ad_click_id_scope === "first_party_scoped"
    ) {
      try {
        internalHandoffAssertion = await mintInternalHandoffAssertion(env, {
          adClickId: existingOwnedPointer.adClickId,
          anonId,
          session: preserveSession,
          destinationUrl: body.handoffDestination
        });
        internalHandoffDurabilitySource = "existing_owned_pointer";
        internalHandoffPointerKvPersisted = true;
      } catch (error) {
        console.warn(JSON.stringify({
          worker: "eden-analytics",
          event: "internal_handoff_existing_pointer_mint_failed",
          reason: String(error?.message || "unknown").slice(0, 120)
        }));
      }
    }
  }
  if (refreshedAttrValue) {
    respHeaders.append("Set-Cookie", buildAttrCookie(refreshedAttrValue, reqUrl));
  }
  appendAttributionPermissionCookies(respHeaders, reqUrl, attributionPermission);
  if (!CLICK_ID_PARAMS.some((p) => activeAttribution[p])) {
    return new Response(JSON.stringify({ ok: true, skipped: "no_click_id" }), {
      status: 200,
      headers: respHeaders
    });
  }
  const preAuthValue = encodeURIComponent(JSON.stringify({
    ...activeAttribution._gcl_au ? { _gcl_au: activeAttribution._gcl_au } : {},
    ...activeAttribution.gclid ? { gclid: activeAttribution.gclid } : {},
    ...activeAttribution.gbraid ? { gbraid: activeAttribution.gbraid } : {},
    ...activeAttribution.wbraid ? { wbraid: activeAttribution.wbraid } : {},
    ...activeAttribution.dclid ? { dclid: activeAttribution.dclid } : {},
    ...activeAttribution.srsltid ? { srsltid: activeAttribution.srsltid } : {},
    ...activeAttribution.fbclid ? { fbclid: activeAttribution.fbclid } : {},
    ...activeAttribution.msclkid ? { msclkid: activeAttribution.msclkid } : {},
    ...activeAttribution.ttclid ? { ttclid: activeAttribution.ttclid } : {},
    ...activeAttribution.utm_source ? { utm_source: activeAttribution.utm_source } : {},
    ...activeAttribution.utm_medium ? { utm_medium: activeAttribution.utm_medium } : {},
    ...activeAttribution.utm_campaign ? { utm_campaign: activeAttribution.utm_campaign } : {},
    ...activeAttribution.utm_content ? { utm_content: activeAttribution.utm_content } : {},
    ...activeAttribution.utm_term ? { utm_term: activeAttribution.utm_term } : {}
  }));
  respHeaders.append("Set-Cookie", [
    `eden_pre_auth=${preAuthValue}`,
    "Max-Age=600",
    `Domain=${cookieDomain(reqUrl)}`,
    "Path=/",
    "HttpOnly",
    "Secure",
    "SameSite=Lax"
  ].join("; "));
  return new Response(JSON.stringify({
    ok: true,
    ...preserveAdClickMemory?.snapshot ? {
      ad_click_observation_persisted: preservePersistence.queue_enqueued && preservePersistence.kv_persisted,
      queue_enqueued: preservePersistence.queue_enqueued,
      pointer_kv_persisted: preservePersistence.kv_persisted,
      owner_attribution_kv_persisted: ownerAttributionKvPersisted
    } : {},
    ...body?.handoffDestination ? {
      internal_handoff_durable: !!internalHandoffAssertion,
      pointer_kv_persisted: internalHandoffPointerKvPersisted || preservePersistence.kv_persisted,
      owner_attribution_kv_persisted: ownerAttributionKvPersisted,
      ...(internalHandoffDurabilitySource ? { internal_handoff_durability_source: internalHandoffDurabilitySource } : {}),
      ...(internalHandoffAssertion ? { internal_handoff_assertion: internalHandoffAssertion } : {})
    } : {}
  }), { status: 200, headers: respHeaders });
}
__name(handlePreserveAttribution, "handlePreserveAttribution");
function normalizeAdClickMemoryMode(env) {
  const knownModes = new Set(["off", "shadow", "cookie", "all", "production"]);
  const mode = String(env?.[AD_CLICK_MEMORY_MODE_ENV] || env?.EDEN_AD_CLICK_MEMORY_ENABLED || "off").trim().toLowerCase();
  if (["1", "true", "enabled", "on"].includes(mode)) return "shadow";
  if (["false", "disabled", "0", "none"].includes(mode)) return "off";
  return knownModes.has(mode) ? mode : "off";
}
__name(normalizeAdClickMemoryMode, "normalizeAdClickMemoryMode");
function isAdClickMemoryEnabled(env) {
  return normalizeAdClickMemoryMode(env) !== "off";
}
__name(isAdClickMemoryEnabled, "isAdClickMemoryEnabled");
function normalizeAdClickKVIndexMode(env) {
  const rawMode = String(env?.[AD_CLICK_KV_INDEX_MODE_ENV] || "pointer").trim().toLowerCase();
  if (["off", "none", "disabled", "false", "0"].includes(rawMode)) return "off";
  if (["full", "all", "indexes", "reverse", "reverse_indexes"].includes(rawMode)) return "full";
  return "pointer";
}
__name(normalizeAdClickKVIndexMode, "normalizeAdClickKVIndexMode");
function requestedAdClickKVResolverMode(env) {
  const rawMode = String(env?.[AD_CLICK_KV_RESOLVER_MODE_ENV] || "pointer_only").trim().toLowerCase();
  if (["off", "none", "disabled", "false", "0"].includes(rawMode)) return "off";
  if (["full", "all", "reverse", "reverse_indexes"].includes(rawMode)) return "full";
  if (["diagnostic", "diagnostic_only", "shadow"].includes(rawMode)) return "diagnostic_only";
  return "pointer_only";
}
__name(requestedAdClickKVResolverMode, "requestedAdClickKVResolverMode");
function normalizeAdClickKVResolverMode(env) {
  const requested = requestedAdClickKVResolverMode(env);
  if (requested !== "full") return requested;
  if (!AD_CLICK_FULL_REVERSE_KV_RESOLVER_IMPLEMENTED) return "pointer_only";
  return isEnvFlagEnabled(env?.EDEN_AD_CLICK_KV_RESOLVER_CONTRACT_ACCEPTED) ? "full" : "pointer_only";
}
__name(normalizeAdClickKVResolverMode, "normalizeAdClickKVResolverMode");
function shouldReadAdClickPointerCookie(env) {
  return normalizeAdClickKVResolverMode(env) !== "off";
}
__name(shouldReadAdClickPointerCookie, "shouldReadAdClickPointerCookie");
function shouldReadFullAdClickKVResolver(env) {
  return normalizeAdClickKVResolverMode(env) === "full";
}
__name(shouldReadFullAdClickKVResolver, "shouldReadFullAdClickKVResolver");
function shouldSetAdClickPointerCookie(env) {
  return !!getAdClickMemoryKV(env) && normalizeAdClickKVIndexMode(env) !== "off" && ["cookie", "all", "production"].includes(normalizeAdClickMemoryMode(env));
}
__name(shouldSetAdClickPointerCookie, "shouldSetAdClickPointerCookie");
function shouldAnnotateAdClickMemoryPayload(env) {
  return ["all", "production"].includes(normalizeAdClickMemoryMode(env));
}
__name(shouldAnnotateAdClickMemoryPayload, "shouldAnnotateAdClickMemoryPayload");
function getAdClickMemoryKV(env) {
  return env?.AD_CLICK_KV || null;
}
__name(getAdClickMemoryKV, "getAdClickMemoryKV");
function shouldWriteAdClickMemoryKV(env) {
  return !!getAdClickMemoryKV(env) && normalizeAdClickKVIndexMode(env) !== "off" && ["cookie", "all", "production"].includes(normalizeAdClickMemoryMode(env));
}
__name(shouldWriteAdClickMemoryKV, "shouldWriteAdClickMemoryKV");
function shouldWriteFullAdClickKVIndexes(env) {
  return shouldWriteAdClickMemoryKV(env) && normalizeAdClickKVIndexMode(env) === "full";
}
__name(shouldWriteFullAdClickKVIndexes, "shouldWriteFullAdClickKVIndexes");
function normalizeAdClickReverseKVRetentionMode(env) {
  const raw = String(env?.[AD_CLICK_REVERSE_KV_RETENTION_MODE_ENV] || env?.[AD_CLICK_REVERSE_KV_TTL_SECONDS_ENV] || "ttl").trim().toLowerCase();
  if (["forever", "permanent", "indefinite", "no_expiration", "none", "never", "0"].includes(raw)) return "forever";
  return "ttl";
}
__name(normalizeAdClickReverseKVRetentionMode, "normalizeAdClickReverseKVRetentionMode");
function adClickPointerKvTtlSeconds(env, memory = null) {
  return Number.parseInt(String(memory?.kv_ttl || env?.EDEN_AD_CLICK_POINTER_KV_TTL_SECONDS || AD_CLICK_POINTER_COOKIE_TTL), 10) || AD_CLICK_POINTER_COOKIE_TTL;
}
__name(adClickPointerKvTtlSeconds, "adClickPointerKvTtlSeconds");
function adClickReverseKvTtlSeconds(env, memory = null) {
  if (normalizeAdClickReverseKVRetentionMode(env) === "forever") return null;
  return Number.parseInt(String(memory?.reverse_kv_ttl || env?.[AD_CLICK_REVERSE_KV_TTL_SECONDS_ENV] || memory?.kv_ttl || AD_CLICK_POINTER_COOKIE_TTL), 10) || AD_CLICK_POINTER_COOKIE_TTL;
}
__name(adClickReverseKvTtlSeconds, "adClickReverseKvTtlSeconds");
function adClickKvPutOptions(ttl) {
  return ttl === null ? {} : { expirationTtl: ttl };
}
__name(adClickKvPutOptions, "adClickKvPutOptions");
function getAdClickSnapshotQueue(env) {
  // Do not fall back to EDGE_EVENTS_QUEUE: that queue has a different consumer contract.
  // The ad-click memory path must be explicitly wired to its own queue before production use.
  return env?.AD_CLICK_SNAPSHOT_QUEUE || null;
}
__name(getAdClickSnapshotQueue, "getAdClickSnapshotQueue");
function shouldEnqueueAdClickMemory(env) {
  const queue = getAdClickSnapshotQueue(env);
  return !!queue && typeof queue.send === "function" && ["shadow", "cookie", "all", "production"].includes(normalizeAdClickMemoryMode(env));
}
__name(shouldEnqueueAdClickMemory, "shouldEnqueueAdClickMemory");
function normalizeAdClickPointerId(rawValue) {
  // v2 (adclk2_) ids only: v1 adclk_ ids were minted from click evidence alone, so the
  // pointer cookies and reverse values written before the July 2026 fix can carry one
  // collapsed cross-user gbraid identity. They are quarantined — never resolved again.
  if (!rawValue) return null;
  try {
    const decoded = decodeURIComponent(String(rawValue)).trim();
    return /^adclk2_[A-Za-z0-9_-]{8,128}$/.test(decoded) ? decoded : null;
  } catch {
    // readCookie already performs one bounded decode. A second malformed or
    // double-encoded value is absence, never a collector-wide exception.
    return null;
  }
}
__name(normalizeAdClickPointerId, "normalizeAdClickPointerId");
function isLegacyV1AdClickId(rawValue) {
  if (!rawValue) return false;
  try {
    return /^adclk_[A-Za-z0-9_-]{8,128}$/.test(decodeURIComponent(String(rawValue)).trim());
  } catch {
    return false;
  }
}
__name(isLegacyV1AdClickId, "isLegacyV1AdClickId");
function readAdClickPointerCookie(request) {
  return normalizeAdClickPointerId(readCookie(request, AD_CLICK_POINTER_COOKIE_NAME)) || normalizeAdClickPointerId(readCookie(request, "eden_click_ref"));
}
__name(readAdClickPointerCookie, "readAdClickPointerCookie");
function hasQuarantinedV1AdClickPointer(request) {
  return isLegacyV1AdClickId(readCookie(request, AD_CLICK_POINTER_COOKIE_NAME)) || isLegacyV1AdClickId(readCookie(request, "eden_click_ref"));
}
__name(hasQuarantinedV1AdClickPointer, "hasQuarantinedV1AdClickPointer");
function hasAdClickEvidenceValue(value) {
  return value !== undefined && value !== null && String(value).trim().length > 0;
}
__name(hasAdClickEvidenceValue, "hasAdClickEvidenceValue");
var UPLOAD_GRADE_GOOGLE_CLICK_ID_PARAMS = /* @__PURE__ */ new Set([...AD_CLICK_CLASS_A_GOOGLE_PARAMS, ...AD_CLICK_DESTINATION_SPECIFIC_GOOGLE_PARAMS]);
function validateUploadGradeGoogleClickId(rawValue) {
  if (!hasAdClickEvidenceValue(rawValue)) return { value: null, reason: "missing" };
  let value = String(rawValue).trim();
  if (value.includes(";")) value = value.split(";", 1)[0].trim();
  if (!value) return { value: null, reason: "empty_after_delimiter_split" };
  if (["undefined", "null", "none", "n/a", "na", "nan", "false", "true"].includes(value.toLowerCase())) {
    return { value: null, reason: "sentinel_value" };
  }
  if (value.length < 8) return { value: null, reason: "too_short" };
  if (value.length > 1024) return { value: null, reason: "too_long" };
  if(/[\u0000-\u001f\u007f\s&?#=;]/.test(value)) return { value: null, reason: "invalid_delimiter_or_control" };
  return { value, reason: null };
}
__name(validateUploadGradeGoogleClickId, "validateUploadGradeGoogleClickId");
function sanitizeUploadGradeClickIdClaimsInObject(value, sourceType = "unknown") {
  if (!value || typeof value !== "object") return [];
  const rejected = [];
  const validLocationsByParam = /* @__PURE__ */ new Map();
  const governedClickFields = /* @__PURE__ */ new Set([
    ...UPLOAD_GRADE_GOOGLE_CLICK_ID_PARAMS,
    ...GOOGLE_CLICK_ID_BODY_PARAMS
  ]);
  const pending = [{ value, depth: 0 }];
  const seen = /* @__PURE__ */ new Set();
  while (pending.length) {
    const current = pending.pop();
    if (!current?.value || typeof current.value !== "object" || seen.has(current.value) || current.depth > 8) continue;
    seen.add(current.value);
    if (Array.isArray(current.value)) {
      for (const entry of current.value) if (entry && typeof entry === "object") pending.push({ value: entry, depth: current.depth + 1 });
      continue;
    }
    for (const rawKey of Object.keys(current.value)) {
      const entry = current.value[rawKey];
      const key = canonicalQueryParamName(rawKey);
      if (governedClickFields.has(key)) {
        const validation = UPLOAD_GRADE_GOOGLE_CLICK_ID_PARAMS.has(key)
          ? validateUploadGradeGoogleClickId(entry)
          : { value: evidenceValue(entry, key), reason: "invalid_value" };
        if (!validation.value) {
          if (hasAdClickEvidenceValue(entry)) rejected.push({ field: key, reason: validation.reason });
          delete current.value[rawKey];
        } else {
          current.value[rawKey] = validation.value;
          if (!validLocationsByParam.has(key)) validLocationsByParam.set(key, []);
          validLocationsByParam.get(key).push({ owner: current.value, rawKey, value: validation.value });
        }
      } else if (entry && typeof entry === "object") {
        pending.push({ value: entry, depth: current.depth + 1 });
      }
    }
  }
  // If one authenticated envelope asserts multiple different values for the
  // same Google click-id type, no arbitrary location is authoritative. Remove
  // every copy before campaign extraction, Segment forwarding, KV writes, or
  // order linking. Same-value repeats are harmless and remain normalized.
  for (const [field, locations] of validLocationsByParam.entries()) {
    const uniqueValues = new Set(locations.map((location) => location.value));
    if (uniqueValues.size <= 1) continue;
    for (const location of locations) delete location.owner[location.rawKey];
    rejected.push({ field, reason: "conflicting_repeats" });
  }
  const dedupedRejected = rejected.filter((entry, index, entries) =>
    entries.findIndex((candidate) => candidate.field === entry.field && candidate.reason === entry.reason) === index
  );
  if (dedupedRejected.length) {
    console.warn(JSON.stringify({ worker: "eden-analytics", event: "google_click_evidence_rejected", source_type: sourceType, rejected: dedupedRejected }));
  }
  return dedupedRejected;
}
__name(sanitizeUploadGradeClickIdClaimsInObject, "sanitizeUploadGradeClickIdClaimsInObject");
function removeCanonicalFieldsDeep(value, canonicalFields, maxDepth = 8) {
  if (!value || typeof value !== "object" || !canonicalFields?.size) return;
  const pending = [{ value, depth: 0 }];
  const seen = /* @__PURE__ */ new Set();
  while (pending.length) {
    const current = pending.pop();
    if (!current?.value || typeof current.value !== "object" || seen.has(current.value) || current.depth > maxDepth) continue;
    seen.add(current.value);
    if (Array.isArray(current.value)) {
      for (const entry of current.value) if (entry && typeof entry === "object") pending.push({ value: entry, depth: current.depth + 1 });
      continue;
    }
    for (const rawKey of Object.keys(current.value)) {
      const entry = current.value[rawKey];
      if (canonicalFields.has(canonicalQueryParamName(rawKey))) {
        delete current.value[rawKey];
      } else if (entry && typeof entry === "object") {
        pending.push({ value: entry, depth: current.depth + 1 });
      }
    }
  }
}
__name(removeCanonicalFieldsDeep, "removeCanonicalFieldsDeep");
function evidenceValue(value, key = null) {
  if (!hasAdClickEvidenceValue(value)) return null;
  if (key && UPLOAD_GRADE_GOOGLE_CLICK_ID_PARAMS.has(key)) return validateUploadGradeGoogleClickId(value).value;
  const normalized = String(value).trim();
  if (!normalized || normalized.length > 4096 || /[\u0000-\u001f\u007f]/.test(normalized)) return null;
  return normalized;
}
__name(evidenceValue, "evidenceValue");
function classifyGoogleClickEvidence(attribution = {}) {
  const classA = {};
  const classB = {};
  const classC = {};
  const rejectedClickEvidence = [];
  for (const key of AD_CLICK_CLASS_A_GOOGLE_PARAMS) {
    const validation = validateUploadGradeGoogleClickId(attribution[key]);
    const value = validation.value;
    if (value) classA[key] = value;
    else if (hasAdClickEvidenceValue(attribution[key])) rejectedClickEvidence.push({ field: key, reason: validation.reason });
  }
  for (const key of AD_CLICK_CLASS_B_GOOGLE_PARAMS) {
    const value = evidenceValue(attribution[key], key);
    if (value) classB[key] = value;
  }
  for (const key of AD_CLICK_CLASS_C_CAMPAIGN_PARAMS) {
    const value = evidenceValue(attribution[key], key);
    if (value) classC[key] = value;
  }
  const classDestination = {};
  for (const key of AD_CLICK_DESTINATION_SPECIFIC_GOOGLE_PARAMS) {
    const validation = validateUploadGradeGoogleClickId(attribution[key]);
    const value = validation.value;
    if (value) classDestination[key] = value;
    else if (hasAdClickEvidenceValue(attribution[key])) rejectedClickEvidence.push({ field: key, reason: validation.reason });
  }
  const clickEvidence = { ...classA, ...classDestination };
  const primaryType = classA.gclid ? "gclid" : classA.gbraid ? "gbraid" : classA.wbraid ? "wbraid" : classDestination.dclid ? "dclid" : null;
  return {
    has_class_a: Object.keys(classA).length > 0,
    has_primary_click_evidence: !!primaryType,
    primary_click_id_type: primaryType,
    class_a: classA,
    destination_specific: classDestination,
    click_evidence: clickEvidence,
    class_b: classB,
    class_c: classC,
    rejected_click_evidence: rejectedClickEvidence,
    evidence_classes: {
      ...Object.fromEntries(Object.keys(classA).map((key) => [key, "class_a_google_ads_upload_click_id"])),
      ...Object.fromEntries(Object.keys(classDestination).map((key) => [key, "destination_specific_google_click_id"])),
      ...Object.fromEntries(Object.keys(classB).map((key) => [key, "class_b_diagnostic_session_evidence"])),
      ...Object.fromEntries(Object.keys(classC).map((key) => [key, "class_c_campaign_context"]))
    }
  };
}
__name(classifyGoogleClickEvidence, "classifyGoogleClickEvidence");
function mergeAttributionPreferFreshPrimary(fallbackAttribution = {}, freshAttribution = {}) {
  const fallback = { ...fallbackAttribution || {} };
  const fresh = { ...freshAttribution || {} };
  if (classifyGoogleClickEvidence(fresh).has_primary_click_evidence) {
    clearActivePaidTouchFields(fallback);
  }
  return normalizeGoogleAliases({ ...fallback, ...fresh });
}
__name(mergeAttributionPreferFreshPrimary, "mergeAttributionPreferFreshPrimary");
function clearActivePaidTouchFields(target) {
  if (!target || typeof target !== "object") return target;
  for (const rawKey of Object.keys(target)) {
    if (ACTIVE_PAID_TOUCH_FIELDS.has(canonicalQueryParamName(rawKey))) delete target[rawKey];
  }
  delete target._ts;
  delete target._click_first_observed_at;
  delete target._last_seen_at;
  return target;
}
__name(clearActivePaidTouchFields, "clearActivePaidTouchFields");
async function createAdClickId(seed = {}) {
  return `adclk2_${(await sha256Raw(JSON.stringify({ schema_version: AD_CLICK_MEMORY_SCHEMA_VERSION, ...seed }))).slice(0, 32)}`;
}
__name(createAdClickId, "createAdClickId");
async function createAdClickSnapshotId(seed = {}, idempotencyKey = null) {
  // Observation identity is intentionally unique per native build, not per
  // ad_click_id. Once embedded in the Queue envelope it never changes, so an
  // at-least-once retry reuses the same insertId while a later independent
  // observation receives a different immutable row identity. Authenticated
  // conversion deliveries additionally supply a stable Segment delivery key so
  // an upstream retry cannot create a second snapshot before KV convergence.
  const observationIdentity = idempotencyKey
    ? { idempotency_key_sha256: await sha256Raw(String(idempotencyKey)) }
    : { observation_nonce: crypto.randomUUID() };
  const identitySeed = { ...seed };
  if (idempotencyKey) delete identitySeed.captured_at;
  return `adsnap_${(await sha256Raw(JSON.stringify({ ...observationIdentity, ...identitySeed }))).slice(0, 32)}`;
}
__name(createAdClickSnapshotId, "createAdClickSnapshotId");
async function createAdIdentityLinkId(seed = {}) {
  // A link_id is a stable relationship key for one scoped ad-click object. The
  // same Eden identity edge may legitimately appear under a later ad click, so
  // ad_click_id and the schema contract must participate in the key. Repeated
  // delivery of the same relationship keeps the same id; a different click can
  // never canonicalize onto the prior click's identity relationship.
  return `adlink_${(await sha256Raw(JSON.stringify({
    schema_version: AD_CLICK_IDENTITY_LINK_SCHEMA_VERSION,
    ...seed
  }))).slice(0, 32)}`;
}
__name(createAdIdentityLinkId, "createAdIdentityLinkId");
function buildAdClickResolution({ source = "unresolved", confidence = "unresolved", conflict = false, conflictSources = [], reason = null, idScope = null } = {}) {
  return compactDefined({
    resolution_source: source,
    resolution_confidence: confidence,
    resolution_conflict: !!conflict,
    resolution_conflict_sources: Array.isArray(conflictSources) ? conflictSources : [],
    resolution_policy_version: AD_CLICK_RESOLUTION_POLICY_VERSION,
    resolved_at: nowUTC(),
    resolution_reason: reason,
    ad_click_id_scope: idScope
  });
}
__name(buildAdClickResolution, "buildAdClickResolution");
function unresolvedAdClickResolution(reason) {
  return buildAdClickResolution({ source: "unresolved", confidence: "unresolved", reason });
}
__name(unresolvedAdClickResolution, "unresolvedAdClickResolution");
async function readAdClickPointerRecord(kv, adClickId) {
  if (!kv || !adClickId) return null;
  try {
    const raw = await kv.get(`${AD_CLICK_KV_PREFIX}id:${adClickId}`);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
}
__name(readAdClickPointerRecord, "readAdClickPointerRecord");
async function readCanonicalAdClickPointerRecord(env, adClickId, { repairCache = true } = {}) {
  const pointerId = normalizeAdClickPointerId(adClickId);
  if (!pointerId) return null;
  const kv = getAdClickMemoryKV(env);
  const cacheRecord = await readAdClickPointerRecord(kv, pointerId);
  const namespace = env?.[CONVERSION_COORDINATOR_BINDING];
  if (!namespace || typeof namespace.idFromName !== "function" || typeof namespace.get !== "function") {
    throw new Error("ad_click_pointer_coordinator_missing");
  }
  const stub = namespace.get(namespace.idFromName(`eden_ad_click_pointer_v1:${pointerId}`));
  if (!stub || typeof stub.fetch !== "function") throw new Error("ad_click_pointer_coordinator_stub_missing");
  const response = await stub.fetch("https://conversion-coordinator.internal/pointer/read", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      ad_click_id: pointerId,
      seed_record: cacheRecord,
      ttl_seconds: adClickPointerKvTtlSeconds(env),
      repair_cache: repairCache
    })
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok || result?.ok !== true) {
    throw new Error(`ad_click_pointer_coordinator_read_${response.status}`);
  }
  if (result.found !== true) return null;
  const record = result.record;
  if (!record || typeof record !== "object" || Array.isArray(record)
    || record.schema_version !== AD_CLICK_POINTER_RECORD_SCHEMA_VERSION
    || record.ad_click_id !== pointerId) {
    throw new Error("ad_click_pointer_coordinator_read_invalid");
  }
  return record;
}
__name(readCanonicalAdClickPointerRecord, "readCanonicalAdClickPointerRecord");
async function currentPointerOwnerContext({ anonId = null, session = null, userId = null, orderId = null } = {}) {
  const sessionValue = sessionRawValue(session);
  return {
    anonymous_id_sha256: anonId ? await sha256Raw(anonId) : null,
    session_id_sha256: sessionValue ? await sha256Raw(sessionValue) : null,
    user_id_sha256: userId ? await sha256Raw(userId) : null,
    order_id_sha256: orderId ? await sha256Raw(orderId) : null
  };
}
__name(currentPointerOwnerContext, "currentPointerOwnerContext");
function validateAdClickPointerOwnership(record, adClickId, owner) {
  if (!record) return { valid: false, reason: "pointer_backing_record_missing" };
  if (record.schema_version !== AD_CLICK_POINTER_RECORD_SCHEMA_VERSION) return { valid: false, reason: "legacy_unowned_pointer_record" };
  if (record.ad_click_id !== adClickId) return { valid: false, reason: "pointer_record_id_mismatch" };
  if (record.revoked_at) return { valid: false, reason: "pointer_record_revoked" };
  if (record.claimed_user_id_sha256 && owner.user_id_sha256 && record.claimed_user_id_sha256 !== owner.user_id_sha256) {
    return { valid: false, reason: "pointer_user_owner_mismatch" };
  }
  // Authenticated Eden user/order identity is stronger continuity evidence than
  // a rotated or cleared browser cookie. A stable match may recover the owned
  // pointer while the anonymous mismatch remains diagnostic; a stable conflict
  // above still fails closed.
  if (record.claimed_user_id_sha256 && owner.user_id_sha256 && record.claimed_user_id_sha256 === owner.user_id_sha256) {
    return { valid: true, reason: record.owner_anonymous_id_sha256 && owner.anonymous_id_sha256 && record.owner_anonymous_id_sha256 !== owner.anonymous_id_sha256 ? "pointer_user_owner_match_anonymous_rotated" : "pointer_user_owner_match" };
  }
  if (record.claimed_order_id_sha256 && owner.order_id_sha256 && record.claimed_order_id_sha256 === owner.order_id_sha256) {
    return { valid: true, reason: record.owner_anonymous_id_sha256 && owner.anonymous_id_sha256 && record.owner_anonymous_id_sha256 !== owner.anonymous_id_sha256 ? "pointer_order_owner_match_anonymous_rotated" : "pointer_order_owner_match" };
  }
  if (record.claimed_order_id_sha256 && owner.order_id_sha256 && record.claimed_order_id_sha256 !== owner.order_id_sha256) {
    return { valid: false, reason: "pointer_order_owner_mismatch" };
  }
  if (record.owner_anonymous_id_sha256 && owner.anonymous_id_sha256) {
    if (record.owner_anonymous_id_sha256 !== owner.anonymous_id_sha256) return { valid: false, reason: "pointer_anonymous_owner_mismatch" };
    return { valid: true, reason: "pointer_anonymous_owner_match" };
  }
  if (record.owner_session_id_sha256 && owner.session_id_sha256 && record.owner_session_id_sha256 === owner.session_id_sha256) {
    return { valid: true, reason: "pointer_session_owner_match" };
  }
  return { valid: false, reason: "pointer_owner_context_missing_or_mismatched" };
}
__name(validateAdClickPointerOwnership, "validateAdClickPointerOwnership");
async function readOwnedAdClickPointer({ env, request, anonId = null, session = null, userId = null, orderId = null, evidence = null } = {}) {
  if (!shouldReadAdClickPointerCookie(env)) return { valid: false, reason: "resolver_mode_off" };
  const adClickId = readAdClickPointerCookie(request);
  if (!adClickId) return { valid: false, reason: hasQuarantinedV1AdClickPointer(request) ? "legacy_v1_pointer_quarantined" : "pointer_cookie_missing" };
  const [record, owner] = await Promise.all([
    readCanonicalAdClickPointerRecord(env, adClickId),
    currentPointerOwnerContext({ anonId, session, userId, orderId })
  ]);
  const ownership = validateAdClickPointerOwnership(record, adClickId, owner);
  if (!ownership.valid) return { valid: false, adClickId, record, owner, reason: ownership.reason };
  if (evidence?.primary_click_id_type && record.raw_primary_click_id_sha256) {
    // Compare against the evidence type that created the owned pointer, not
    // blindly against the newly highest-priority type. A later gclid plus the
    // same stored gbraid/wbraid is compatible enrichment of one click object.
    const recordPrimaryType = evidenceValue(record.primary_click_id_type) || evidence.primary_click_id_type;
    const rawValue = evidenceValue((evidence.click_evidence || {})[recordPrimaryType]);
    if (!rawValue) {
      return { valid: false, adClickId, record, owner, reason: "pointer_click_evidence_mismatch" };
    }
    const currentHash = rawValue ? await sha256Raw(rawValue) : null;
    if (currentHash && currentHash !== record.raw_primary_click_id_sha256) {
      return { valid: false, adClickId, record, owner, reason: "pointer_click_evidence_mismatch" };
    }
    return {
      valid: true,
      adClickId,
      record,
      owner,
      reason: recordPrimaryType === evidence.primary_click_id_type
        ? ownership.reason
        : "pointer_click_evidence_compatible_enrichment",
      clickEvidenceMatchesOwnedPointer: true
    };
  }
  return { valid: true, adClickId, record, owner, reason: ownership.reason };
}
__name(readOwnedAdClickPointer, "readOwnedAdClickPointer");
async function identityPointerCoordinatorRequest(env, identityType, identityHash, action, payload = {}) {
  if (!["user_id_sha256", "order_id_sha256"].includes(identityType) || !/^[a-f0-9]{64}$/.test(String(identityHash || ""))) {
    throw new Error("identity_pointer_identity_invalid");
  }
  const namespace = env?.[CONVERSION_COORDINATOR_BINDING];
  if (!namespace || typeof namespace.idFromName !== "function" || typeof namespace.get !== "function") {
    throw new Error("identity_pointer_coordinator_missing");
  }
  const stub = namespace.get(namespace.idFromName(`eden_identity_pointer_v1:${identityType}:${identityHash}`));
  if (!stub || typeof stub.fetch !== "function") throw new Error("identity_pointer_coordinator_stub_missing");
  const response = await stub.fetch(`https://conversion-coordinator.internal/identity-pointer/${action}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ identity_type: identityType, identity_hash: identityHash, ...payload })
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok || result?.ok !== true) throw new Error(`identity_pointer_coordinator_${action}_${response.status}`);
  return result;
}
__name(identityPointerCoordinatorRequest, "identityPointerCoordinatorRequest");
async function readStableIdentityAdClickPointer({ env, anonId = null, session = null, userId = null, orderId = null } = {}) {
  const identities = [];
  if (orderId) identities.push({ type: "order_id_sha256", hash: await sha256Raw(orderId), source: "stable_order_pointer" });
  if (userId) identities.push({ type: "user_id_sha256", hash: await sha256Raw(userId), source: "stable_user_pointer" });
  if (!identities.length) return { valid: false, reason: "stable_identity_missing" };
  const candidates = [];
  for (const identity of identities) {
    const result = await identityPointerCoordinatorRequest(env, identity.type, identity.hash, "read");
    if (result.found === true && result.record?.latest_ad_click_id) {
      candidates.push({ ...identity, bridge: result.record, adClickId: normalizeAdClickPointerId(result.record.latest_ad_click_id) });
    }
  }
  const owner = await currentPointerOwnerContext({ anonId, session, userId, orderId });
  const usable = [];
  for (const candidate of candidates) {
    if (!candidate.adClickId) continue;
    const pointerRecord = await readCanonicalAdClickPointerRecord(env, candidate.adClickId);
    const ownership = validateAdClickPointerOwnership(pointerRecord, candidate.adClickId, owner);
    if (pointerRecord?.ad_click_id_scope === "first_party_scoped" && ownership.valid) {
      usable.push({ ...candidate, pointerRecord, ownership });
    }
  }
  if (!usable.length) return { valid: false, reason: candidates.length ? "stable_identity_pointer_owner_or_scope_rejected" : "stable_identity_pointer_missing" };
  const chosen = usable[0];
  const conflictingSources = usable.filter((candidate) => candidate.adClickId !== chosen.adClickId).map((candidate) => candidate.source);
  return {
    valid: true,
    adClickId: chosen.adClickId,
    pointerRecord: chosen.pointerRecord,
    source: chosen.source,
    reason: chosen.ownership.reason,
    conflictSources: conflictingSources
  };
}
__name(readStableIdentityAdClickPointer, "readStableIdentityAdClickPointer");
async function upsertStableIdentityAdClickPointers(env, memory) {
  if (!memory?.ad_click_id || memory.observation_only || memory.resolution?.ad_click_id_scope !== "first_party_scoped") return 0;
  const capturedAt = Number.isFinite(Date.parse(String(memory.observed_at || ""))) ? new Date(memory.observed_at).toISOString() : nowUTC();
  const identities = [
    ["user_id_sha256", memory.identity_refs?.user_id_sha256],
    ["order_id_sha256", memory.identity_refs?.order_id_sha256]
  ].filter(([, hash]) => /^[a-f0-9]{64}$/.test(String(hash || "")));
  await Promise.all(identities.map(([identityType, identityHash]) => identityPointerCoordinatorRequest(
    env,
    identityType,
    identityHash,
    "upsert",
    { candidate: { ad_click_id: memory.ad_click_id, captured_at: capturedAt } }
  )));
  return identities.length;
}
__name(upsertStableIdentityAdClickPointers, "upsertStableIdentityAdClickPointers");
function firstPartyAdClickScope({ anonId = null, session = null, userId = null, orderId = null } = {}) {
  if (anonId) return { type: "anonymous_id", value: anonId };
  const sessionValue = sessionRawValue(session);
  if (sessionValue) return { type: "session_id", value: sessionValue };
  if (userId) return { type: "user_id", value: userId };
  if (orderId) return { type: "order_id", value: orderId };
  return null;
}
__name(firstPartyAdClickScope, "firstPartyAdClickScope");
async function resolveAdClickIdForMemory({ env, request, evidence, pointerComparisonEvidence = null, anonId = null, session = null, userId = null, orderId = null, clickEvidenceIsEventNative = true, internalHandoff = null }) {
  const ownedPointer = await readOwnedAdClickPointer({
    env,
    request,
    anonId,
    session,
    userId,
    orderId,
    // Compatibility enrichment is allowed only when the secondary evidence
    // that matches the existing pointer is native to this observation. A braid
    // recovered from eden_pre_auth/KV cannot merge a new GCLID into an older
    // click object. Recovered-only events validate the pointer by first-party
    // ownership rather than comparing it with immutable first-touch evidence.
    evidence: clickEvidenceIsEventNative ? pointerComparisonEvidence || evidence : null
  });
  const ownedPointerIsCurrentScope = ownedPointer.valid && ["first_party_scoped", "instance_random"].includes(ownedPointer.record?.ad_click_id_scope);
  const signedHandoffOwnsPointer = !!internalHandoff?.valid
    && !!internalHandoff.pointerRecord
    && !internalHandoff.pointerRecordLag
    && internalHandoff.pointerId === readAdClickPointerCookie(request);
  if (evidence?.has_primary_click_evidence) {
    const clickEvidence = evidence.click_evidence || evidence.class_a || evidence.destination_specific;
    // The object identity is anchored to the selected primary click value only.
    // Secondary evidence is preserved on the immutable snapshot, but adding or
    // losing a secondary braid must not fragment one owner+GCLID into multiple
    // ad_click_ids.
    const primaryClickEvidence = {
      type: evidence.primary_click_id_type,
      value: evidenceValue(clickEvidence[evidence.primary_click_id_type])
    };
    // Provenance-honest source: click evidence that reached this event via stored
    // attribution (KV/cookie continuity) is memory, not a fresh observation. dbt uses
    // this to distinguish direct conversion-path proof from memory-only recovery.
    const freshPrefix = clickEvidenceIsEventNative ? "fresh" : "recovered";
    const source = evidence.has_class_a ? `${freshPrefix}_class_a_click` : `${freshPrefix}_destination_specific_click`;
    if (!clickEvidenceIsEventNative) {
      if (signedHandoffOwnsPointer) {
        return {
          adClickId: internalHandoff.pointerId,
          resolution: buildAdClickResolution({
            source: "pointer_cookie",
            confidence: "high",
            reason: "signed_internal_handoff_transport_suppressed",
            idScope: internalHandoff.pointerRecord?.ad_click_id_scope || "first_party_scoped"
          }),
          pointerRecord: internalHandoff.pointerRecord,
          shouldCreateSnapshot: false,
          pointerValidated: true
        };
      }
      if (ownedPointerIsCurrentScope) {
        return {
          adClickId: ownedPointer.adClickId,
          resolution: buildAdClickResolution({ source: "pointer_cookie", confidence: "high", reason: "recovered_click_matches_owned_pointer", idScope: ownedPointer.record?.ad_click_id_scope || null }),
          pointerRecord: ownedPointer.record,
          shouldCreateSnapshot: false,
          pointerValidated: true
        };
      }
      if (userId || orderId) {
        const stablePointer = await readStableIdentityAdClickPointer({ env, anonId, session, userId, orderId });
        if (stablePointer.valid) {
          return {
            adClickId: stablePointer.adClickId,
            resolution: buildAdClickResolution({
              source: stablePointer.source,
              confidence: "high",
              conflict: stablePointer.conflictSources.length > 0,
              conflictSources: stablePointer.conflictSources,
              reason: stablePointer.reason,
              idScope: stablePointer.pointerRecord.ad_click_id_scope
            }),
            pointerRecord: stablePointer.pointerRecord,
            shouldCreateSnapshot: false,
            pointerValidated: true
          };
        }
      }
      if (shouldReadFullAdClickKVResolver(env)) {
        return await resolveAdClickIdFromFullReverseKV({ env, evidence, anonId, session, userId, orderId });
      }
      // A recovered raw click value cannot mint or recover object identity by
      // itself. Without an owned pointer/reverse key, keep it only in the
      // owner-scoped attribution evidence already carrying the event.
      return {
        adClickId: null,
        resolution: unresolvedAdClickResolution("recovered_click_without_owned_first_party_pointer")
      };
    }
    if (ownedPointerIsCurrentScope && ownedPointer.clickEvidenceMatchesOwnedPointer) {
      return {
        adClickId: ownedPointer.adClickId,
        resolution: buildAdClickResolution({
          source,
          confidence: "high",
          reason: ownedPointer.reason,
          idScope: ownedPointer.record?.ad_click_id_scope || null
        }),
        pointerRecord: ownedPointer.record,
        shouldCreateSnapshot: true,
        pointerValidated: true
      };
    }
    // A raw Google click value is evidence, not an Eden person identifier. Every
    // ad-click object is therefore scoped to first-party context. This removes the
    // read-before-write race where two users could concurrently claim the same
    // global GCLID object. The raw click hash remains globally comparable in the
    // snapshot so dbt can still detect replay or multi-user anomalies.
    const firstPartyScope = firstPartyAdClickScope({ anonId, session, userId, orderId });
    const derivedAdClickId = firstPartyScope
      ? await createAdClickId({
          primary_click_evidence: primaryClickEvidence,
          first_party_scope_type: firstPartyScope.type,
          first_party_scope: firstPartyScope.value
        })
      : await createAdClickId({ primary_click_evidence: primaryClickEvidence, instance: crypto.randomUUID() });
    const idScope = firstPartyScope ? "first_party_scoped" : "instance_random";
    if (signedHandoffOwnsPointer && derivedAdClickId === internalHandoff.pointerId) {
      return {
        adClickId: internalHandoff.pointerId,
        resolution: buildAdClickResolution({
          source,
          confidence: "high",
          reason: internalHandoff.reason,
          idScope: internalHandoff.pointerRecord?.ad_click_id_scope || "first_party_scoped"
        }),
        pointerRecord: internalHandoff.pointerRecord || null,
        shouldCreateSnapshot: true,
        pointerValidated: true
      };
    }
    const existingRecord = await readCanonicalAdClickPointerRecord(env, derivedAdClickId);
    const owner = await currentPointerOwnerContext({ anonId, session, userId, orderId });
    const existingOwnership = validateAdClickPointerOwnership(existingRecord, derivedAdClickId, owner);
    if (existingRecord?.revoked_at) {
      // Explicit denial makes the old relationship immutable, but a later
      // explicit allow plus a genuinely fresh landing may establish a new
      // owner-scoped generation. The nonce prevents any later cycle from
      // deterministically selecting and overwriting a revoked predecessor.
      const regeneratedAdClickId = await createAdClickId({
        primary_click_evidence: primaryClickEvidence,
        revoked_predecessor: derivedAdClickId,
        first_party_scope_type: firstPartyScope?.type || "instance",
        first_party_scope: firstPartyScope?.value || crypto.randomUUID(),
        generation_nonce: crypto.randomUUID()
      });
      return {
        adClickId: regeneratedAdClickId,
        resolution: buildAdClickResolution({
          source,
          confidence: "high",
          reason: "revoked_predecessor_new_generation",
          idScope
        }),
        pointerRecord: null,
        shouldCreateSnapshot: true,
        pointerValidated: false
      };
    }
    if (existingRecord && !existingOwnership.valid) {
      // This can now occur only for a hash collision, corrupted record, or copied
      // first-party scope. Never replace the existing owner; quarantine a new
      // object and make the conflict visible downstream.
      const conflictScope = firstPartyAdClickScope({ anonId, session, userId, orderId });
      const conflictIdScope = conflictScope ? "first_party_scoped" : "instance_random";
      let conflictAdClickId = await createAdClickId({
        primary_click_evidence: primaryClickEvidence,
        ownership_conflict_with: derivedAdClickId,
        ...conflictScope ? {
          first_party_scope_type: conflictScope.type,
          first_party_scope: conflictScope.value
        } : { instance: crypto.randomUUID() }
      });
      let conflictRecord = await readCanonicalAdClickPointerRecord(env, conflictAdClickId);
      let conflictOwnership = validateAdClickPointerOwnership(conflictRecord, conflictAdClickId, owner);
      if (conflictRecord && !conflictOwnership.valid) {
        // A deterministic predecessor that was explicitly revoked is immutable
        // history. Never overwrite/unrevoke it on a later allow-and-recapture
        // cycle; mint a fresh scoped generation instead.
        conflictAdClickId = await createAdClickId({
          primary_click_evidence: primaryClickEvidence,
          ownership_conflict_with: conflictAdClickId,
          first_party_scope_type: conflictScope?.type || "instance",
          first_party_scope: conflictScope?.value || crypto.randomUUID(),
          generation_nonce: crypto.randomUUID()
        });
        conflictRecord = await readCanonicalAdClickPointerRecord(env, conflictAdClickId);
        conflictOwnership = validateAdClickPointerOwnership(conflictRecord, conflictAdClickId, owner);
      }
      const conflictRecordUsable = !!conflictRecord && conflictOwnership.valid;
      return {
        adClickId: conflictAdClickId,
        resolution: buildAdClickResolution({
          source,
          confidence: "conflict",
          conflict: true,
          conflictSources: ["existing_pointer_owner", "current_first_party_owner"],
          reason: existingOwnership.reason || "derived_ad_click_id_owner_mismatch",
          idScope: conflictIdScope
        }),
        pointerRecord: conflictRecordUsable ? conflictRecord : null,
        shouldCreateSnapshot: true,
        pointerValidated: false
      };
    }
    const existingRecordUsable = !!existingRecord && existingOwnership.valid;
    const preserveOwnedPointerForInternalHandoff = signedHandoffOwnsPointer
      && derivedAdClickId !== internalHandoff.pointerId;
    return {
      adClickId: derivedAdClickId,
      resolution: buildAdClickResolution({
        source,
        confidence: preserveOwnedPointerForInternalHandoff ? "diagnostic_only" : "high",
        conflict: preserveOwnedPointerForInternalHandoff,
        conflictSources: preserveOwnedPointerForInternalHandoff
          ? ["owned_pointer_cookie", "signed_internal_handoff", "transported_query_not_selected"]
          : [],
        reason: preserveOwnedPointerForInternalHandoff
          ? "signed_internal_handoff_pointer_click_evidence_mismatch"
          : null,
        idScope
      }),
      pointerRecord: existingRecordUsable ? existingRecord : null,
      shouldCreateSnapshot: true,
      pointerValidated: existingRecordUsable,
      observationOnly: preserveOwnedPointerForInternalHandoff,
      selectedAdClickId: preserveOwnedPointerForInternalHandoff ? internalHandoff.pointerId : null
    };
  }
  if (ownedPointerIsCurrentScope) {
    return {
      adClickId: ownedPointer.adClickId,
      resolution: buildAdClickResolution({ source: "pointer_cookie", confidence: "high", reason: ownedPointer.reason, idScope: ownedPointer.record?.ad_click_id_scope || null }),
      pointerRecord: ownedPointer.record,
      shouldCreateSnapshot: false,
      pointerValidated: true
    };
  }
  if (userId || orderId) {
    const stablePointer = await readStableIdentityAdClickPointer({ env, anonId, session, userId, orderId });
    if (stablePointer.valid) {
      return {
        adClickId: stablePointer.adClickId,
        pointerRecord: stablePointer.pointerRecord,
        shouldCreateSnapshot: false,
        pointerValidated: true,
        resolution: buildAdClickResolution({
          source: stablePointer.source,
          confidence: "high",
          conflict: stablePointer.conflictSources.length > 0,
          conflictSources: stablePointer.conflictSources,
          reason: stablePointer.reason,
          idScope: stablePointer.pointerRecord.ad_click_id_scope
        })
      };
    }
  }
  if (shouldReadFullAdClickKVResolver(env)) {
    return await resolveAdClickIdFromFullReverseKV({ env, evidence, anonId, session, userId, orderId });
  }
  return {
    adClickId: null,
    resolution: unresolvedAdClickResolution(
      ownedPointer.valid
        ? "legacy_non_scoped_pointer_quarantined_by_v3"
        : ownedPointer.reason || "no_class_a_or_owned_pointer"
    )
  };
}
__name(resolveAdClickIdForMemory, "resolveAdClickIdForMemory");
async function readAdClickReverseCandidate(env, kv, key, source, confidence) {
  if (!env || !kv || !key) return null;
  const adClickId = normalizeAdClickPointerId(await kv.get(key));
  if (!adClickId) return null;
  // Reverse indexes are eventually-consistent cache hints only. Every candidate
  // must be re-read through the canonical Durable Object so a stale KV row can
  // never bypass revocation or a changed owner claim.
  const pointerRecord = await readCanonicalAdClickPointerRecord(env, adClickId);
  return { source, confidence, adClickId, pointerRecord, dangling: !pointerRecord };
}
__name(readAdClickReverseCandidate, "readAdClickReverseCandidate");
async function resolveAdClickIdFromFullReverseKV({ env, evidence, anonId = null, session = null, userId = null, orderId = null }) {
  const kv = getAdClickMemoryKV(env);
  if (!kv) return { adClickId: null, resolution: unresolvedAdClickResolution("reverse_kv_not_configured") };
  const candidates = [];
  const add = (candidate) => { if (candidate) candidates.push(candidate); };
  const orderHash = orderId ? await sha256Raw(orderId) : null;
  const sessionValue = sessionRawValue(session);
  const sessionHash = sessionValue ? await sha256Raw(sessionValue) : null;
  const anonHash = anonId ? await sha256Raw(anonId) : null;
  const userHash = userId ? await sha256Raw(userId) : null;
  // v2 resolver contract: only Eden first-party continuity keys (order/session/anon/user)
  // may resolve an ad_click_id. Click-value and _gcl_au keys are never resolver inputs,
  // and v1-prefixed reverse indexes written before the July 2026 regression fix are
  // never read (quarantined), so legacy click-keyed memories cannot bridge users.
  if (orderHash) add(await readAdClickReverseCandidate(env, kv, `${AD_CLICK_KV_REVERSE_PREFIX}order:${orderHash}`, "order_bridge", "high"));
  if (sessionHash) add(await readAdClickReverseCandidate(env, kv, `${AD_CLICK_KV_REVERSE_PREFIX}session:${sessionHash}:current`, "session_bridge", "medium"));
  if (anonHash) {
    add(await readAdClickReverseCandidate(env, kv, `${AD_CLICK_KV_REVERSE_PREFIX}anon:${anonHash}:last_paid`, "anonymous_bridge", "medium"));
    add(await readAdClickReverseCandidate(env, kv, `${AD_CLICK_KV_REVERSE_PREFIX}anon:${anonHash}:first_paid`, "anonymous_bridge", "medium"));
  }
  if (userHash) {
    add(await readAdClickReverseCandidate(env, kv, `${AD_CLICK_KV_REVERSE_PREFIX}user:${userHash}:last_paid`, "user_bridge", "medium"));
    add(await readAdClickReverseCandidate(env, kv, `${AD_CLICK_KV_REVERSE_PREFIX}user:${userHash}:first_paid`, "user_bridge", "medium"));
  }
  const owner = await currentPointerOwnerContext({ anonId, session, userId, orderId });
  const usable = candidates.filter((candidate) =>
    candidate
      && !candidate.dangling
      && candidate.pointerRecord?.ad_click_id_scope === "first_party_scoped"
      && validateAdClickPointerOwnership(candidate.pointerRecord, candidate.adClickId, owner).valid
  );
  const danglingSources = candidates.filter((candidate) => candidate?.dangling).map((candidate) => `${candidate.source}_dangling`);
  const rejectedSources = candidates.filter((candidate) => candidate && !candidate.dangling && !usable.includes(candidate)).map((candidate) => `${candidate.source}_owner_or_scope_rejected`);
  if (!usable.length) {
    const conflictSources = [...new Set([...danglingSources, ...rejectedSources])];
    return { adClickId: null, resolution: buildAdClickResolution({ source: "unresolved", confidence: "unresolved", conflict: conflictSources.length > 0, conflictSources, reason: danglingSources.length ? "dangling_reverse_kv_index" : rejectedSources.length ? "reverse_kv_owner_or_scope_rejected" : "no_reverse_kv_candidate" }) };
  }
  const chosen = usable[0];
  const conflictSources = [...new Set([
    ...danglingSources,
    ...rejectedSources,
    ...usable.filter((candidate) => candidate.adClickId !== chosen.adClickId).map((candidate) => candidate.source)
  ])];
  return {
    adClickId: chosen.adClickId,
    pointerRecord: chosen.pointerRecord,
    shouldCreateSnapshot: false,
    pointerValidated: true,
    resolution: buildAdClickResolution({
      source: chosen.source,
      confidence: chosen.confidence,
      conflict: conflictSources.length > 0,
      conflictSources,
      reason: conflictSources.length ? "reverse_kv_conflict_flagged" : null,
      idScope: chosen.pointerRecord.ad_click_id_scope
    })
  };
}
__name(resolveAdClickIdFromFullReverseKV, "resolveAdClickIdFromFullReverseKV");
function sessionRawValue(session) {
  if (!session) return null;
  if (typeof session === "string") return session;
  return session.raw || session.id || null;
}
__name(sessionRawValue, "sessionRawValue");
function eventPageUrl(body, fallbackUrl) {
  const pageUrl = body?.context?.page?.url || body?.properties?.page_url || body?.properties?.url || body?.properties?.landing_page || null;
  let fallback;
  try { fallback = fallbackUrl instanceof URL ? fallbackUrl : new URL(fallbackUrl || "https://collect.eden.health/"); } catch {
    fallback = new URL("https://collect.eden.health/");
  }
  if (pageUrl) {
    try { return new URL(pageUrl, fallback); } catch {}
  }
  return fallback;
}
__name(eventPageUrl, "eventPageUrl");
function compactDefined(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return value;
  const out = {};
  for (const [key, val] of Object.entries(value)) {
    if (val === undefined || val === null || val === "") continue;
    if (val && typeof val === "object" && !Array.isArray(val)) {
      const nested = compactDefined(val);
      if (nested && Object.keys(nested).length) out[key] = nested;
      continue;
    }
    out[key] = val;
  }
  return out;
}
__name(compactDefined, "compactDefined");
async function buildAdClickSnapshotV1({ request, env, url, body = null, anonId = null, session = null, attribution = {}, sourceType = "unknown", eventName = null, userId = null, orderId = null, adClickId = null, evidence = null, resolution = null, capturedAt = null, observationIdempotencyKey = null }) {
  capturedAt = capturedAt || nowUTC();
  const pageUrl = eventPageUrl(body, url || request.url);
  const classified = evidence || classifyGoogleClickEvidence(attribution);
  if (!adClickId) throw new Error("ad_click_id required for ad-click snapshot");
  const resolvedAdClickId = adClickId;
  const clickHashes = {};
  for (const [key, value] of Object.entries(classified.click_evidence || classified.class_a)) clickHashes[`${key}_sha256`] = await sha256Raw(value);
  const landingUrlSanitized = sanitizeAdClickLandingUrl(pageUrl).toString();
  const snapshotId = await createAdClickSnapshotId({
    schema_version: AD_CLICK_MEMORY_SCHEMA_VERSION,
    ad_click_id: resolvedAdClickId,
    captured_at: capturedAt,
    source_type: sourceType,
    event_name: eventName,
    source_route_host: pageUrl.hostname,
    landing_url_sanitized: landingUrlSanitized,
    primary_click_id_type: classified.primary_click_id_type,
    raw_primary_click_id_sha256: classified.primary_click_id_type ? clickHashes[`${classified.primary_click_id_type}_sha256`] || null : null
  }, observationIdempotencyKey);
  return compactDefined({
    schema_version: AD_CLICK_MEMORY_SCHEMA_VERSION,
    snapshot_id: snapshotId,
    ad_click_id: resolvedAdClickId,
    captured_at: capturedAt,
    source_worker: "eden-analytics",
    source_pipeline_version: PIPELINE_VERSION,
    source_type: sourceType,
    event_name: eventName,
    route_host: new URL(request.url).hostname,
    source_route_host: pageUrl.hostname,
    landing_url_sanitized: landingUrlSanitized,
    raw_query_present: !!pageUrl.search,
    google: { ...(classified.click_evidence || classified.class_a), ...clickHashes },
    diagnostic_google: compactDefined({
      ...classified.class_b,
      rejected_click_evidence: classified.rejected_click_evidence,
      query_param_observation: observeAttributionQueryKeys(pageUrl)
    }),
    campaign: classified.class_c,
    first_party: {
      eden_anonymous_id: anonId,
      // Transitional alias retained for the existing BigQuery landing contract.
      // New browser/runtime consumers must prefer eden_anonymous_id.
      eden_anon_id: anonId,
      eden_session_id: sessionRawValue(session),
      segment_anonymous_id: body?.anonymousId || body?.anonymous_id || body?.context?.anonymousId || null
    },
    identity_refs: {
      user_id_sha256: userId ? await sha256Raw(userId) : null,
      order_id_sha256: orderId ? await sha256Raw(orderId) : null,
      email_sha256: body?.properties?.email_sha256 || body?.traits?.email_sha256 || body?.context?.traits?.email_sha256 || null,
      phone_sha256: body?.properties?.phone_sha256 || body?.traits?.phone_sha256 || body?.context?.traits?.phone_sha256 || null
    },
    evidence: {
      primary_click_id_type: classified.primary_click_id_type,
      raw_primary_click_id_sha256: classified.primary_click_id_type ? clickHashes[`${classified.primary_click_id_type}_sha256`] || null : null,
      evidence_classes: classified.evidence_classes,
      upload_candidate_types: Object.keys(classified.class_a),
      destination_specific_candidate_types: Object.keys(classified.destination_specific || {}),
      diagnostic_only_types: Object.keys(classified.class_b),
      campaign_context_types: Object.keys(classified.class_c),
      rejected_click_evidence: classified.rejected_click_evidence,
      acquisition_channel: deriveAcquisitionChannel({ ...(classified.click_evidence || classified.class_a), ...classified.class_b, ...classified.class_c }),
      attribution_confidence: deriveAttributionConfidence({ ...(classified.click_evidence || classified.class_a), ...classified.class_b, ...classified.class_c }),
      missing_gclid_reason: deriveMissingGclidReason({ ...(classified.click_evidence || classified.class_a), ...classified.class_b })
    },
    governance: {
      gpc_opt_out: isGpcOptOut(request),
      attribution_suppressed: !canUseAttributionForRequest(env, isGpcOptOut(request), request, body),
      resolution_source: resolution?.resolution_source || "fresh_class_a_click",
      resolution_confidence: resolution?.resolution_confidence || "high",
      resolution_conflict: !!resolution?.resolution_conflict,
      resolution_conflict_sources: resolution?.resolution_conflict_sources || [],
      resolution_policy_version: resolution?.resolution_policy_version || AD_CLICK_RESOLUTION_POLICY_VERSION,
      resolved_at: resolution?.resolved_at || capturedAt,
      ad_click_id_scope: resolution?.ad_click_id_scope || null,
      final_upload_eligibility_source: "dbt_google_outbox_validator"
    }
  });
}
__name(buildAdClickSnapshotV1, "buildAdClickSnapshotV1");
async function buildAdIdentityLinks({ adClickId, anonId = null, session = null, userId = null, orderId = null, emailSha256 = null, phoneSha256 = null, sourceType = "unknown", eventName = null, linkReason = "unknown", capturedAt = nowUTC() }) {
  if (!adClickId) return [];
  const sessionValue = sessionRawValue(session);
  const userHash = userId ? await sha256Raw(userId) : null;
  const orderHash = orderId ? await sha256Raw(orderId) : null;
  const linkSpecs = [];
  const add = (fromType, fromId, toType, toId, reason = linkReason) => {
    if (fromId && toId && fromId !== toId) linkSpecs.push({ from_type: fromType, from_id: fromId, to_type: toType, to_id: toId, link_reason: reason });
  };
  add("anonymous_id", anonId, "session_id", sessionValue, "anon_session");
  add("anonymous_id", anonId, "ad_click_id", adClickId, linkReason);
  add("session_id", sessionValue, "ad_click_id", adClickId, linkReason);
  add("anonymous_id", anonId, "user_id_sha256", userHash, "identify");
  add("user_id_sha256", userHash, "order_id_sha256", orderHash, "order_identity");
  add("order_id_sha256", orderHash, "ad_click_id", adClickId, linkReason);
  add("email_sha256", emailSha256, "user_id_sha256", userHash, "email_identity");
  add("phone_sha256", phoneSha256, "user_id_sha256", userHash, "phone_identity");
  const links = [];
  for (const spec of linkSpecs) {
    links.push({
      schema_version: AD_CLICK_IDENTITY_LINK_SCHEMA_VERSION,
      link_id: await createAdIdentityLinkId({ ad_click_id: adClickId, from_type: spec.from_type, from_id: spec.from_id, to_type: spec.to_type, to_id: spec.to_id }),
      linked_at: capturedAt,
      source_worker: "eden-analytics",
      source_pipeline_version: PIPELINE_VERSION,
      source_type: sourceType,
      event_name: eventName,
      confidence: "deterministic",
      ...spec
    });
  }
  return links;
}
__name(buildAdIdentityLinks, "buildAdIdentityLinks");
function validatedProducerEventTimestamp(body = null) {
  for (const candidate of [body?.originalTimestamp, body?.timestamp]) {
    if (!candidate || !Number.isFinite(Date.parse(String(candidate)))) continue;
    const parsed = new Date(candidate);
    const timestamp = parsed.getTime();
    if (timestamp >= Date.UTC(2000, 0, 1) && timestamp <= Date.now() + 864e5) return parsed.toISOString();
  }
  return null;
}
__name(validatedProducerEventTimestamp, "validatedProducerEventTimestamp");
function stableAdClickObservationTimestamp(sourceType = "unknown", body = null) {
  // Only the authenticated /server-collect lane may assert historical event
  // time. Browser payload timestamps and session-start suffixes are mutable
  // client claims; using them here could backdate a newly observed Google click
  // and corrupt first-touch ordering. Browser/page/preserve observations use the
  // edge receipt time instead.
  if (sourceType === "server") {
    const producerTimestamp = validatedProducerEventTimestamp(body);
    if (producerTimestamp) return producerTimestamp;
  }
  return nowUTC();
}
__name(stableAdClickObservationTimestamp, "stableAdClickObservationTimestamp");
function stableConversionSignalValue(value, depth = 0) {
  if (value === void 0 || value === null || depth > 6) return null;
  if (["string", "number", "boolean"].includes(typeof value)) return String(value);
  if (Array.isArray(value)) return JSON.stringify(value.slice(0, 100).map((entry) => stableConversionSignalValue(entry, depth + 1)));
  if (typeof value === "object") {
    const out = {};
    for (const key of Object.keys(value).sort().slice(0, 100)) {
      const normalized = stableConversionSignalValue(value[key], depth + 1);
      if (normalized !== null) out[key] = normalized;
    }
    return JSON.stringify(out);
  }
  return null;
}
__name(stableConversionSignalValue, "stableConversionSignalValue");
function conversionProducerPayloadForFingerprint(body = null) {
  const payload = body && typeof body === "object" && !Array.isArray(body)
    ? JSON.parse(JSON.stringify(body))
    : {};
  // Segment delivery time and the Worker-owned idempotency key are governed
  // separately. A producer may retry the same business payload with a later
  // timestamp or an equivalent source message-id spelling without changing
  // the payload that an enrichment message represents.
  for (const key of ["timestamp", "originalTimestamp", "sentAt", "receivedAt", "messageId", "message_id"]) {
    delete payload[key];
  }
  return payload;
}
__name(conversionProducerPayloadForFingerprint, "conversionProducerPayloadForFingerprint");
function canonicalConversionFingerprintValue(value) {
  if (Array.isArray(value)) return value.map(canonicalConversionFingerprintValue);
  if (value && typeof value === "object") {
    const out = {};
    for (const key of Object.keys(value).sort()) {
      if (value[key] !== void 0) out[key] = canonicalConversionFingerprintValue(value[key]);
    }
    return out;
  }
  return value;
}
__name(canonicalConversionFingerprintValue, "canonicalConversionFingerprintValue");
async function buildServerConversionPayloadFingerprint({ body, anonId, userId, orderId, storedAttribution = null, eventNativeAttribution = null }) {
  const effectiveAttribution = {
    ...stripInternalFields(storedAttribution || {}),
    ...stripInternalFields(eventNativeAttribution || {})
  };
  const canonical = canonicalConversionFingerprintValue({
    producer_payload: conversionProducerPayloadForFingerprint(body),
    resolved_identity: { anonymous_id: anonId, user_id: userId, order_id: orderId },
    // Fingerprint the effective values that can reach Segment, not whether the
    // same value was native on the first attempt and then recovered from
    // first-party continuity on retry. Provenance promotion alone must not
    // create a synthetic business correction.
    effective_attribution: effectiveAttribution
  });
  return sha256Raw(`eden_conversion_forward_payload_v1\0${JSON.stringify(canonical)}`);
}
__name(buildServerConversionPayloadFingerprint, "buildServerConversionPayloadFingerprint");
function conversionRecordOwnerReferenceHashes(record) {
  if (!record || typeof record !== "object") return {};
  if (record.delivery_state === "segment_delivery_unacknowledged") {
    return record.pending_signal_hashes && typeof record.pending_signal_hashes === "object"
      ? record.pending_signal_hashes
      : record.accepted_signal_hashes && typeof record.accepted_signal_hashes === "object"
        ? record.accepted_signal_hashes
        : {};
  }
  return record.signal_hashes && typeof record.signal_hashes === "object"
    ? record.signal_hashes
    : record.accepted_signal_hashes && typeof record.accepted_signal_hashes === "object"
      ? record.accepted_signal_hashes
      : {};
}
__name(conversionRecordOwnerReferenceHashes, "conversionRecordOwnerReferenceHashes");
async function conversionRecordHasPresentedOwnerConflict(record, { anonId = null, userId = null, orderId = null } = {}) {
  const reference = conversionRecordOwnerReferenceHashes(record);
  for (const [key, value] of [
    ["identity:anonymous_id", anonId],
    ["identity:user_id", userId],
    ["identity:order_id", orderId]
  ]) {
    if (!value || !reference[key]) continue;
    const normalized = stableConversionSignalValue(value);
    if (normalized === null || normalized === "") continue;
    const currentHash = await sha256Raw(`eden_conversion_signal_v1\0${key}\0${normalized}`);
    if (currentHash !== reference[key]) return true;
  }
  return false;
}
__name(conversionRecordHasPresentedOwnerConflict, "conversionRecordHasPresentedOwnerConflict");
const CONVERSION_MONOTONIC_STATUS_KEYS = /* @__PURE__ */ new Set([
  "property:payment_status",
  "property:payment_state",
  "property:payment_stage",
  "property:order_status"
]);
const CONVERSION_PROGRESS_STATUS_RANKS = /* @__PURE__ */ new Map([
  ["created", 1],
  ["new", 1],
  ["pending", 1],
  ["processing", 1],
  ["uncaptured", 1],
  ["awaiting_approval", 1],
  ["awaiting_doctor_approval", 1],
  ["requires_capture", 2],
  ["authorized", 2],
  ["approved", 2],
  ["captured", 3],
  ["paid", 3],
  ["success", 3],
  ["succeeded", 3],
  ["complete", 3],
  ["completed", 3]
]);
async function probeConversionCoordinatorHealth(env) {
  const namespace = env?.[CONVERSION_COORDINATOR_BINDING];
  if (!namespace || typeof namespace.idFromName !== "function" || typeof namespace.get !== "function") {
    return { configured: false, ok: false, schemaVersion: null, storageReadable: false };
  }
  try {
    const stub = namespace.get(namespace.idFromName("eden_conversion_coordinator_health_v1"));
    if (!stub || typeof stub.fetch !== "function") return { configured: true, ok: false, schemaVersion: null, storageReadable: false };
    const signal = typeof AbortSignal !== "undefined" && typeof AbortSignal.timeout === "function"
      ? AbortSignal.timeout(2e3)
      : void 0;
    const response = await stub.fetch("https://conversion-coordinator.internal/health", {
      method: "GET",
      ...signal ? { signal } : {}
    });
    const payload = await response.json().catch(() => ({}));
    return {
      configured: true,
      ok: response.ok && payload?.ok === true && payload?.schema_version === "eden_conversion_coordinator_v1" && payload?.storage_readable === true,
      schemaVersion: typeof payload?.schema_version === "string" ? payload.schema_version : null,
      storageReadable: payload?.storage_readable === true
    };
  } catch (error) {
    console.error(JSON.stringify({ worker: "eden-analytics", event: "conversion_coordinator_health_failed", reason: String(error?.message || "unknown").slice(0, 120) }));
    return { configured: true, ok: false, schemaVersion: null, storageReadable: false };
  }
}
__name(probeConversionCoordinatorHealth, "probeConversionCoordinatorHealth");
function evaluateWorkerReadiness(env, conversionCoordinatorHealth) {
  const missing = [];
  const requireValue = (condition, name) => {
    if (!condition) missing.push(name);
  };
  requireValue(!!env?.GCLID_KV, "gclid_kv");
  requireValue(!!env?.PRIVACY_LEDGER_KV, "privacy_ledger_kv");
  requireValue(!!env?.SEGMENT_WRITE_KEY, "segment_write_key");
  requireValue(!!env?.SERVER_API_SECRET, "server_api_secret");
  requireValue(!!env?.[BROWSER_CAPABILITY_SECRET_ENV], "browser_capability_hmac_secret");
  requireValue(!!env?.[PRIVACY_LEDGER_HMAC_SECRET_ENV], "privacy_ledger_hmac_secret");
  requireValue(conversionCoordinatorHealth?.configured === true, "conversion_coordinator_binding");
  requireValue(conversionCoordinatorHealth?.ok === true, "conversion_coordinator_runtime");
  if (isAdClickMemoryEnabled(env)) {
    requireValue(!!getAdClickMemoryKV(env), "ad_click_kv");
    requireValue(!!getAdClickSnapshotQueue(env) && typeof getAdClickSnapshotQueue(env)?.send === "function", "ad_click_snapshot_queue");
    requireValue(shouldConsumeAdClickMemoryQueue(env), "ad_click_queue_consumer_enabled");
    const bigQueryConfig = adClickBigQueryConfig(env);
    requireValue(!!bigQueryConfig.projectId, "ad_click_bigquery_project_id");
    requireValue(!!bigQueryConfig.datasetId, "ad_click_bigquery_dataset_id");
    requireValue(!!bigQueryConfig.serviceAccountKey || !!bigQueryConfig.accessToken, "ad_click_bigquery_auth");
  }
  return { ready: missing.length === 0, missing };
}
__name(evaluateWorkerReadiness, "evaluateWorkerReadiness");
async function acquireConversionCoordinatorLease(env, conversionKey, eventName) {
  const namespace = env?.[CONVERSION_COORDINATOR_BINDING];
  if (!namespace || typeof namespace.idFromName !== "function" || typeof namespace.get !== "function") {
    return { acquired: false, configurationError: true, reason: "conversion_coordinator_binding_missing" };
  }
  // The producer-stable idempotency key is the serialization boundary. It is
  // hashed before Durable Object naming and never stored raw in the coordinator.
  // Distinct canonical business milestones may keep separate records inside the
  // same object; syntactic event aliases normalize before this point.
  const scopeHash = await sha256Raw(`eden_conversion_coordinator_v1\0conversion_key\0${conversionKey}`);
  const stub = namespace.get(namespace.idFromName(scopeHash));
  if (!stub || typeof stub.fetch !== "function") {
    return { acquired: false, configurationError: true, reason: "conversion_coordinator_stub_missing" };
  }
  const token = randomBase64Url(24);
  try {
    const response = await stub.fetch("https://conversion-coordinator.internal/acquire", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ token, event_name: eventName, lease_ttl_ms: CONVERSION_COORDINATOR_LEASE_TTL_MS })
    });
    const payload = await response.json().catch(() => ({}));
    if (response.status === 200 && payload?.acquired === true) {
      return {
        acquired: true,
        stub,
        token,
        scopeHash,
        record: payload?.record && typeof payload.record === "object" && !Array.isArray(payload.record)
          ? payload.record
          : null
      };
    }
    if (response.status === 409) {
      return {
        acquired: false,
        retryable: true,
        reason: "conversion_in_progress",
        retryAfterMs: Number(payload?.retry_after_ms || 1000)
      };
    }
    return { acquired: false, retryable: true, reason: "conversion_coordinator_unavailable" };
  } catch (error) {
    console.error(JSON.stringify({ worker: "eden-analytics", event: "conversion_coordinator_acquire_failed", reason: String(error?.message || "unknown").slice(0, 120) }));
    return { acquired: false, retryable: true, reason: "conversion_coordinator_unavailable" };
  }
}
__name(acquireConversionCoordinatorLease, "acquireConversionCoordinatorLease");
async function preloadConversionKvFallback(env, { eventName, orderId = null, body = null, scopeHash = null } = {}) {
  if (!env?.GCLID_KV || !eventName || !scopeHash) throw new Error("conversion_kv_fallback_configuration_missing");
  const dedupKey = `dedup:v4:${eventName}:${scopeHash}`;
  let raw = await env.GCLID_KV.get(dedupKey);
  let source = raw ? "v4_raw_free" : null;
  let reference = null;
  if (!raw && orderId && eventName !== "OS_purchase") {
    raw = await env.GCLID_KV.get(`dedup:${eventName}:${orderId}`);
    if (raw) {
      source = "legacy_real_order";
      reference = orderId;
    }
  }
  if (!raw && eventName === "OS_purchase") {
    for (const legacyReference of resolveLegacyV555ConversionReferences(body)) {
      raw = await env.GCLID_KV.get(`dedup:${eventName}:${legacyReference}`);
      if (raw) {
        source = "legacy_v555_overloaded_order";
        reference = legacyReference;
        break;
      }
    }
  }
  let record = null;
  if (raw) {
    record = JSON.parse(raw);
    if (!record || typeof record !== "object" || Array.isArray(record)) throw new Error("conversion_kv_fallback_record_invalid");
  }
  return { preloaded: true, dedupKey, raw, source, reference, record };
}
__name(preloadConversionKvFallback, "preloadConversionKvFallback");
async function writeConversionCoordinatorRecord(lease, eventName, record) {
  if (!lease?.acquired || !lease.stub || !lease.token) throw new Error("conversion_coordinator_lease_missing");
  const response = await lease.stub.fetch("https://conversion-coordinator.internal/record", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ token: lease.token, event_name: eventName, record })
  });
  if (!response.ok) throw new Error(`conversion_coordinator_record_${response.status}`);
  const payload = await response.json().catch(() => ({}));
  if (payload?.recorded !== true) throw new Error("conversion_coordinator_record_unconfirmed");
}
__name(writeConversionCoordinatorRecord, "writeConversionCoordinatorRecord");
async function restoreConversionCoordinatorRecord(lease, eventName) {
  if (!lease?.acquired || !lease.stub || !lease.token) throw new Error("conversion_coordinator_lease_missing");
  const response = await lease.stub.fetch("https://conversion-coordinator.internal/restore", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      token: lease.token,
      event_name: eventName,
      record: lease.record || null
    })
  });
  if (!response.ok) throw new Error(`conversion_coordinator_restore_${response.status}`);
  const payload = await response.json().catch(() => ({}));
  if (payload?.restored !== true) throw new Error("conversion_coordinator_restore_unconfirmed");
}
__name(restoreConversionCoordinatorRecord, "restoreConversionCoordinatorRecord");
async function releaseConversionCoordinatorLease(lease) {
  if (!lease?.acquired || !lease.stub || !lease.token) return;
  try {
    const response = await lease.stub.fetch("https://conversion-coordinator.internal/release", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ token: lease.token })
    });
    if (!response.ok) throw new Error(`conversion_coordinator_release_${response.status}`);
  } catch (error) {
    // The lease expires automatically. A release failure may delay a producer
    // retry, but it cannot permit concurrent conversion delivery.
    console.error(JSON.stringify({ worker: "eden-analytics", event: "conversion_coordinator_release_failed", reason: String(error?.message || "unknown").slice(0, 120) }));
  }
}
__name(releaseConversionCoordinatorLease, "releaseConversionCoordinatorLease");
function normalizedConversionStatus(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}
__name(normalizedConversionStatus, "normalizedConversionStatus");
function conversionStatusRank(value) {
  return CONVERSION_PROGRESS_STATUS_RANKS.get(normalizedConversionStatus(value)) || 0;
}
__name(conversionStatusRank, "conversionStatusRank");
async function buildServerConversionSignalState({ body, anonId, session, userId, orderId, conversionScopeHash = null, conversionKeySource = null, storedAttribution = null, eventNativeAttribution = null }) {
  const signalValues = {};
  const statusRanks = {};
  const add = (key, value) => {
    const normalized = stableConversionSignalValue(value);
    if (normalized === null || normalized === "") return;
    signalValues[key] = normalized;
    if (CONVERSION_MONOTONIC_STATUS_KEYS.has(key)) {
      const rank = conversionStatusRank(value);
      if (rank > 0) statusRanks[key] = rank;
    }
  };
  add("identity:anonymous_id", anonId);
  // A customer can legitimately finish one order over multiple sessions.
  // Session churn is continuity evidence, not a new business conversion signal.
  // Keeping it out of the dedupe state prevents a later session from reopening
  // or conflicting with the same order-level conversion.
  add("identity:user_id", userId);
  add("identity:order_id", orderId);
  add("identity:conversion_scope_sha256", conversionScopeHash);
  add("identity:conversion_key_source", conversionKeySource);
  for (const key of [...AD_CLICK_CLASS_A_GOOGLE_PARAMS, ...AD_CLICK_DESTINATION_SPECIFIC_GOOGLE_PARAMS]) {
    // The same click often becomes recoverable immediately after its first
    // event-native attempt. Do not treat that provenance promotion as new
    // business truth on retry; native evidence wins for this signal state.
    if (eventNativeAttribution?.[key]) add(`native_click:${key}`, eventNativeAttribution[key]);
    else add(`resolved_click:${key}`, storedAttribution?.[key]);
  }
  for (const key of AD_CLICK_CLASS_C_CAMPAIGN_PARAMS) {
    // Campaign metadata is part of the immutable attempted conversion payload.
    // A later missing value may enrich the same transaction, but a changed
    // value must not silently mutate an unknown-commit or persistence retry.
    if (eventNativeAttribution?.[key]) add(`native_campaign:${key}`, eventNativeAttribution[key]);
    else add(`resolved_campaign:${key}`, storedAttribution?.[key]);
  }
  const properties = body?.properties && typeof body.properties === "object" ? body.properties : {};
  for (const key of [
    "email", "email_sha256", "phone", "phone_sha256", "first_name", "firstName", "first_name_sha256",
    "last_name", "lastName", "last_name_sha256", "postal_code", "postalCode", "country", "country_code",
    "product_id", "productId", "product_name", "offering_id", "offeringId", "offering_name", "plan_id", "planId",
    "sku", "medication", "medication_name", "dose", "package_id", "conversion_value", "revenue", "value", "currency",
    "payment_status", "payment_state", "payment_stage", "payment_type", "order_status", "customer_type", "is_first_order", "items", "cart_data", "ecommerce"
  ]) add(`property:${normalizeBrowserFieldKey(key)}`, properties[key]);
  const hashes = {};
  for (const [key, value] of Object.entries(signalValues).sort(([left], [right]) => left.localeCompare(right))) {
    hashes[key] = await sha256Raw(`eden_conversion_signal_v1\0${key}\0${value}`);
  }
  const payloadFingerprintSha256 = await buildServerConversionPayloadFingerprint({
    body,
    anonId,
    userId,
    orderId,
    storedAttribution,
    eventNativeAttribution
  });
  return { hashes, statusRanks, payloadFingerprintSha256 };
}
__name(buildServerConversionSignalState, "buildServerConversionSignalState");
function mergeServerConversionSignalState(previousRecord, currentState) {
  const previousHashes = previousRecord?.signal_hashes && typeof previousRecord.signal_hashes === "object" ? previousRecord.signal_hashes : {};
  const previousStatusRanks = previousRecord?.status_ranks && typeof previousRecord.status_ranks === "object" ? previousRecord.status_ranks : {};
  const mergedHashes = { ...previousHashes };
  const mergedStatusRanks = { ...previousStatusRanks };
  const acceptedSignalKeys = [];
  const conflictingSignalKeys = [];
  for (const [key, hash] of Object.entries(currentState.hashes || {})) {
    const previousHash = previousHashes[key];
    if (!previousHash) {
      mergedHashes[key] = hash;
      if (currentState.statusRanks?.[key]) mergedStatusRanks[key] = currentState.statusRanks[key];
      acceptedSignalKeys.push(key);
      continue;
    }
    if (previousHash === hash) continue;
    const currentRank = Number(currentState.statusRanks?.[key] || 0);
    const previousRank = Number(previousStatusRanks[key] || 0);
    if (CONVERSION_MONOTONIC_STATUS_KEYS.has(key) && currentRank > previousRank) {
      mergedHashes[key] = hash;
      mergedStatusRanks[key] = currentRank;
      acceptedSignalKeys.push(key);
      continue;
    }
    // Immutable identity, click, product and financial values cannot silently
    // oscillate on one conversion key. A producer correction must arrive as a
    // governed conversion adjustment/revision contract instead of repeatedly
    // rewriting the same Segment conversion.
    conflictingSignalKeys.push(key);
  }
  return { mergedHashes, mergedStatusRanks, acceptedSignalKeys, conflictingSignalKeys };
}
__name(mergeServerConversionSignalState, "mergeServerConversionSignalState");
function buildPendingConversionAttemptState(mergedHashes, mergedStatusRanks, currentState, signalKeys) {
  const hashes = { ...(mergedHashes || {}) };
  const statusRanks = { ...(mergedStatusRanks || {}) };
  for (const key of signalKeys || []) {
    if (currentState?.hashes?.[key]) hashes[key] = currentState.hashes[key];
    if (currentState?.statusRanks?.[key]) statusRanks[key] = currentState.statusRanks[key];
  }
  return { hashes, statusRanks };
}
__name(buildPendingConversionAttemptState, "buildPendingConversionAttemptState");
function pendingConversionReferenceState(previousRecord) {
  if (previousRecord?.delivery_state === "segment_delivery_unacknowledged") {
    const pendingHashes = previousRecord?.pending_signal_hashes && typeof previousRecord.pending_signal_hashes === "object"
      ? previousRecord.pending_signal_hashes
      : previousRecord?.signal_hashes && typeof previousRecord.signal_hashes === "object"
        ? previousRecord.signal_hashes
        : {};
    const pendingStatusRanks = previousRecord?.pending_status_ranks && typeof previousRecord.pending_status_ranks === "object"
      ? previousRecord.pending_status_ranks
      : previousRecord?.status_ranks && typeof previousRecord.status_ranks === "object"
        ? previousRecord.status_ranks
        : {};
    return { hashes: pendingHashes, statusRanks: pendingStatusRanks };
  }
  if (previousRecord?.delivery_state === "segment_acknowledged_pending_persistence") {
    return {
      hashes: previousRecord?.signal_hashes && typeof previousRecord.signal_hashes === "object" ? previousRecord.signal_hashes : {},
      statusRanks: previousRecord?.status_ranks && typeof previousRecord.status_ranks === "object" ? previousRecord.status_ranks : {}
    };
  }
  return null;
}
__name(pendingConversionReferenceState, "pendingConversionReferenceState");
function validatePendingConversionRetryState(previousRecord, currentState) {
  const reference = pendingConversionReferenceState(previousRecord);
  if (!reference) {
    return { compatible: true, missing: 0, regressed: 0, conflicting: 0 };
  }
  let missing = 0;
  let regressed = 0;
  let conflicting = 0;
  const diagnosticConflictKeys = new Set(
    Array.isArray(previousRecord?.pending_diagnostic_conflict_signal_keys)
      ? previousRecord.pending_diagnostic_conflict_signal_keys
      : []
  );
  const exactPendingBaseReplay = previousRecord?.pending_delivery_kind === "base"
    && validPendingSegmentTrackPayload(previousRecord?.pending_segment_payload, previousRecord?.pending_message_id || null);
  const exactReplayOwnerKeys = new Set([
    "identity:anonymous_id",
    "identity:user_id",
    "identity:order_id",
    "identity:conversion_scope_sha256",
    "identity:conversion_key_source"
  ]);
  for (const [key, pendingHash] of Object.entries(reference.hashes)) {
    // A quarantined conflicting click/identity is diagnostic observation, not
    // canonical business state. Its disappearance on a corrected producer
    // retry must not force the producer to replay known-wrong evidence. The
    // pending-enrichment planner below detects the diagnostic state change and
    // supersedes the ambiguous enrichment under a new idempotency key.
    if (diagnosticConflictKeys.has(key)) continue;
    const currentHash = currentState?.hashes?.[key];
    // Once the exact pending base payload is durable, changed or omitted
    // business fields do not need to be forced back onto the producer. Replay
    // the stored base bytes and deliver current truth under a separate
    // enrichment id. Owner/scope fields remain guarded so one transaction key
    // cannot be used to replay another customer's purchase.
    if (exactPendingBaseReplay && !exactReplayOwnerKeys.has(key)) {
      if (!currentHash) {
        missing += 1;
        continue;
      }
      continue;
    }
    if (exactPendingBaseReplay && exactReplayOwnerKeys.has(key) && !currentHash
      && !["identity:conversion_scope_sha256", "identity:conversion_key_source"].includes(key)) continue;
    if (!currentHash) {
      missing += 1;
      continue;
    }
    if (currentHash === pendingHash) continue;
    if (CONVERSION_MONOTONIC_STATUS_KEYS.has(key)) {
      const pendingRank = Number(reference.statusRanks[key] || 0);
      const currentRank = Number(currentState?.statusRanks?.[key] || 0);
      if (currentRank >= pendingRank && currentRank > 0) continue;
      regressed += 1;
      continue;
    }
    conflicting += 1;
  }
  return { compatible: missing === 0 && regressed === 0 && conflicting === 0, missing, regressed, conflicting };
}
__name(validatePendingConversionRetryState, "validatePendingConversionRetryState");
function pendingDiagnosticConflictStateChanged(previousRecord, currentState, currentConflictKeys = []) {
  if (previousRecord?.delivery_state !== "segment_delivery_unacknowledged") return false;
  const previousKeys = Array.isArray(previousRecord?.pending_diagnostic_conflict_signal_keys)
    ? previousRecord.pending_diagnostic_conflict_signal_keys.filter((key) => typeof key === "string")
    : [];
  const currentKeys = Array.isArray(currentConflictKeys)
    ? currentConflictKeys.filter((key) => typeof key === "string" && !CONVERSION_MONOTONIC_STATUS_KEYS.has(key))
    : [];
  if (previousKeys.length !== currentKeys.length) return true;
  const currentSet = new Set(currentKeys);
  const pendingHashes = previousRecord?.pending_signal_hashes && typeof previousRecord.pending_signal_hashes === "object"
    ? previousRecord.pending_signal_hashes
    : {};
  for (const key of previousKeys) {
    if (!currentSet.has(key)) return true;
    if (pendingHashes[key] !== currentState?.hashes?.[key]) return true;
  }
  return false;
}
__name(pendingDiagnosticConflictStateChanged, "pendingDiagnosticConflictStateChanged");
function conversionForwardSignalKeys(previousRecord, currentState) {
  const reference = pendingConversionReferenceState(previousRecord)
    || {
      hashes: previousRecord?.signal_hashes && typeof previousRecord.signal_hashes === "object" ? previousRecord.signal_hashes : {},
      statusRanks: previousRecord?.status_ranks && typeof previousRecord.status_ranks === "object" ? previousRecord.status_ranks : {}
    };
  const advanced = [];
  for (const [key, currentHash] of Object.entries(currentState?.hashes || {})) {
    const priorHash = reference.hashes[key];
    if (!priorHash) {
      advanced.push(key);
      continue;
    }
    if (priorHash === currentHash) continue;
    if (CONVERSION_MONOTONIC_STATUS_KEYS.has(key)) {
      const currentRank = Number(currentState?.statusRanks?.[key] || 0);
      const priorRank = Number(reference.statusRanks[key] || 0);
      // A same-rank alias such as authorized -> approved still changes the raw
      // Segment payload. Treat it as superseding state so the pending message
      // ID is never reused with different bytes.
      if (currentRank >= priorRank && currentRank > 0) advanced.push(key);
    }
  }
  return advanced.sort();
}
__name(conversionForwardSignalKeys, "conversionForwardSignalKeys");
function mergeConversionConflictHashHistory(previousRecord, currentState, currentConflictKeys) {
  const history = {};
  const previous = previousRecord?.conflicting_signal_hashes && typeof previousRecord.conflicting_signal_hashes === "object"
    ? previousRecord.conflicting_signal_hashes
    : {};
  for (const [key, hashes] of Object.entries(previous)) {
    history[key] = [...new Set(Array.isArray(hashes) ? hashes.filter(Boolean).slice(-20) : [])];
  }
  const newlyObservedKeys = [];
  for (const key of currentConflictKeys || []) {
    const hash = currentState?.hashes?.[key];
    if (!hash) continue;
    const prior = new Set(history[key] || []);
    if (!prior.has(hash)) newlyObservedKeys.push(key);
    prior.add(hash);
    history[key] = [...prior].slice(-20);
  }
  return { history, newlyObservedKeys };
}
__name(mergeConversionConflictHashHistory, "mergeConversionConflictHashHistory");
function conversionConflictClickFields(signalKeys = []) {
  const fields = /* @__PURE__ */ new Set();
  for (const key of signalKeys || []) {
    const match = String(key).match(/^(?:native_click|resolved_click|native_campaign|resolved_campaign):(.+)$/);
    if (match) fields.add(canonicalQueryParamName(match[1]));
  }
  return fields;
}
__name(conversionConflictClickFields, "conversionConflictClickFields");
function hasConversionIdentityOrClickConflict(signalKeys = []) {
  return (signalKeys || []).some((key) =>
    String(key).startsWith("native_click:")
    || String(key).startsWith("resolved_click:")
    || ["identity:anonymous_id", "identity:user_id", "identity:order_id"].includes(String(key))
  );
}
__name(hasConversionIdentityOrClickConflict, "hasConversionIdentityOrClickConflict");
async function buildConversionEnrichmentMessageId(eventName, conversionScopeHash, acceptedSignalKeys, currentSignalState) {
  const accepted = {};
  for (const key of [...acceptedSignalKeys || []].sort()) {
    if (currentSignalState?.hashes?.[key]) accepted[key] = currentSignalState.hashes[key];
  }
  const digest = await sha256Raw(JSON.stringify({
    event: eventName,
    conversion_scope_sha256: conversionScopeHash,
    accepted,
    payload_fingerprint_sha256: currentSignalState?.payloadFingerprintSha256 || null
  }));
  return `eden_${eventName}_enrichment_${digest.slice(0, 32)}`;
}
__name(buildConversionEnrichmentMessageId, "buildConversionEnrichmentMessageId");
function markConversionEnrichmentPayload(body, eventName, plan) {
  if (!body?.properties || typeof body.properties !== "object" || Array.isArray(body.properties)) body.properties = {};
  body.event = `${eventName}_enrichment`;
  body.properties.original_conversion_event = eventName;
  body.properties.conversion_enrichment_only = true;
  body.properties.conversion_enrichment_accepted_signal_keys = [...plan.acceptedSignalKeys || []].sort();
  body.properties.conversion_enrichment_conflicting_signal_keys = [...plan.conflictingSignalKeys || []].sort();
  body.properties.conversion_enrichment_current_conflicting_signal_keys = [...plan.currentConflictingSignalKeys || []].sort();
  body.properties.conversion_enrichment_message_id = plan.segmentMessageId;
}
__name(markConversionEnrichmentPayload, "markConversionEnrichmentPayload");
async function buildConversionPersistenceIntent(memory, env) {
  const queueRequired = !!memory && (!!memory.snapshot || (memory.identity_links || []).length > 0);
  const kvRequired = !!memory
    && !memory.observation_only
    && memory.resolution?.ad_click_id_scope === "first_party_scoped"
    && shouldWriteAdClickMemoryKV(env);
  const identityLinkIds = (memory?.identity_links || []).map((link) => String(link?.link_id || "")).filter(Boolean).sort();
  // Hash the exact immutable Queue envelope, not merely its IDs. Otherwise a
  // retry could reuse one snapshot_id while silently changing campaign,
  // landing-page or diagnostic Google metadata after Segment already accepted
  // the conversion. The raw payload is never stored in the coordinator; only
  // this digest and its non-sensitive observation timestamp are retained.
  const fingerprintSource = queueRequired || kvRequired ? {
    queue_envelope: queueRequired ? buildAdClickMemoryQueueEnvelope(memory) : null,
    kv_input: kvRequired ? {
      ad_click_id: memory?.ad_click_id || null,
      snapshot: memory?.snapshot || null,
      first_party: memory?.first_party || null,
      identity_refs: memory?.identity_refs || null,
      evidence: memory?.evidence || null,
      resolution: memory?.resolution || null,
      observation_only: !!memory?.observation_only
    } : null
  } : null;
  return {
    schema_version: "eden_conversion_persistence_intent_v1",
    queue_required: queueRequired,
    kv_required: kvRequired,
    snapshot_required: !!memory?.snapshot,
    identity_link_count: identityLinkIds.length,
    observed_at: memory?.observed_at || memory?.snapshot?.captured_at || null,
    retry_payload_metadata: {
      resolution: memory?.resolution || null,
      snapshot_governance: memory?.snapshot?.governance || null,
      selected_ad_click_id: memory?.selected_ad_click_id || null
    },
    payload_fingerprint_sha256: fingerprintSource ? await sha256Raw(JSON.stringify(fingerprintSource)) : null
  };
}
__name(buildConversionPersistenceIntent, "buildConversionPersistenceIntent");
function applyPendingPersistenceRetryMetadata(memory, intent) {
  const metadata = intent?.retry_payload_metadata;
  if (!memory || !metadata || typeof metadata !== "object") return memory;
  const restored = { ...memory };
  if (intent?.observed_at && Number.isFinite(Date.parse(String(intent.observed_at)))) {
    restored.observed_at = new Date(intent.observed_at).toISOString();
  }
  if (metadata.resolution && typeof metadata.resolution === "object") {
    restored.resolution = JSON.parse(JSON.stringify(metadata.resolution));
  }
  if (Object.prototype.hasOwnProperty.call(metadata, "selected_ad_click_id")) {
    restored.selected_ad_click_id = metadata.selected_ad_click_id || null;
  }
  if (restored.snapshot && metadata.snapshot_governance && typeof metadata.snapshot_governance === "object") {
    restored.snapshot = {
      ...restored.snapshot,
      governance: JSON.parse(JSON.stringify(metadata.snapshot_governance))
    };
  }
  return restored;
}
__name(applyPendingPersistenceRetryMetadata, "applyPendingPersistenceRetryMetadata");
function compareConversionPersistenceIntent(expected, actual) {
  if (!expected || expected.schema_version !== "eden_conversion_persistence_intent_v1") {
    return { compatible: false, reason: "persistence_intent_missing_or_invalid" };
  }
  for (const field of ["queue_required", "kv_required", "snapshot_required", "identity_link_count"]) {
    if (expected[field] !== actual?.[field]) return { compatible: false, reason: `${field}_mismatch` };
  }
  if ((expected.queue_required || expected.kv_required)
    && (!expected.payload_fingerprint_sha256 || expected.payload_fingerprint_sha256 !== actual?.payload_fingerprint_sha256)) {
    return { compatible: false, reason: "persistence_payload_fingerprint_mismatch" };
  }
  return { compatible: true, reason: "exact_persistence_intent" };
}
__name(compareConversionPersistenceIntent, "compareConversionPersistenceIntent");
async function safeBuildAdClickMemoryCandidate(args, logContext = "unknown", { failClosed = false } = {}) {
  try {
    return await buildAdClickMemoryCandidate(args);
  } catch (err) {
    console.error(`[eden-analytics] ad-click ${logContext} build error:`, err);
    if (failClosed) throw err;
    return null;
  }
}
__name(safeBuildAdClickMemoryCandidate, "safeBuildAdClickMemoryCandidate");
async function buildAdClickMemoryCandidate({ request, env, url = null, body = null, anonId = null, session = null, attribution = {}, sourceType = "unknown", eventName = null, userId = null, orderId = null, linkReason = "unknown", eventNativeAttribution = null, internalHandoff = null, observationIdempotencyKey = null, observationTimestampOverride = null }) {
  if (!isAdClickMemoryEnabled(env)) return null;
  if (!canUseAttributionForRequest(env, isGpcOptOut(request), request, body)) return null;
  const evidence = classifyGoogleClickEvidence(attribution || {});
  const eventNativeEvidence = eventNativeAttribution
    ? classifyGoogleClickEvidence(eventNativeAttribution)
    : evidence;
  const rejectedClickEvidence = [...evidence.rejected_click_evidence || [], ...eventNativeEvidence.rejected_click_evidence || []]
    .filter((entry, index, values) => values.findIndex((candidate) => candidate.field === entry.field && candidate.reason === entry.reason) === index);
  if (rejectedClickEvidence.length) {
    console.warn(JSON.stringify({
      worker: "eden-analytics",
      event: "google_click_evidence_rejected",
      source_type: sourceType,
      rejected: rejectedClickEvidence
    }));
  }
  // Fresh evidence in this observation outranks recovered first-touch evidence,
  // regardless of the global gclid > gbraid > wbraid order. That order applies
  // only among fields native to the same observation. Otherwise a recovered
  // old GCLID can suppress a genuinely new braid click.
  const hasEventNativePrimaryEvidence = !!eventNativeEvidence.has_primary_click_evidence;
  const candidateEvidence = hasEventNativePrimaryEvidence ? eventNativeEvidence : evidence;
  const clickEvidenceIsEventNative = hasEventNativePrimaryEvidence || !eventNativeAttribution;
  const observedAt = observationTimestampOverride && Number.isFinite(Date.parse(String(observationTimestampOverride)))
    ? new Date(observationTimestampOverride).toISOString()
    : stableAdClickObservationTimestamp(sourceType, body);
  const resolved = await resolveAdClickIdForMemory({ env, request, evidence: candidateEvidence, pointerComparisonEvidence: eventNativeEvidence, anonId, session, userId, orderId, clickEvidenceIsEventNative, internalHandoff });
  if (!resolved.adClickId) return null;
  resolved.resolution = { ...resolved.resolution, resolved_at: observedAt };
  const adClickId = resolved.adClickId;
  const sessionId = sessionRawValue(session);
  const emailSha256 = body?.properties?.email_sha256 || body?.traits?.email_sha256 || body?.context?.traits?.email_sha256 || null;
  const phoneSha256 = body?.properties?.phone_sha256 || body?.traits?.phone_sha256 || body?.context?.traits?.phone_sha256 || null;
  const userHash = userId ? await sha256Raw(userId) : null;
  const orderHash = orderId ? await sha256Raw(orderId) : null;
  const snapshot = eventNativeEvidence.has_primary_click_evidence && clickEvidenceIsEventNative && resolved.shouldCreateSnapshot !== false ? await buildAdClickSnapshotV1({
    request,
    env,
    url,
    body,
    anonId,
    session,
    attribution: eventNativeAttribution || attribution,
    sourceType,
    eventName,
    userId,
    orderId,
    adClickId,
    evidence: eventNativeEvidence,
    resolution: resolved.resolution,
    capturedAt: observedAt,
    observationIdempotencyKey
  }) : null;
  const observationOnly = !!resolved.observationOnly;
  const shouldEmitLinks = !observationOnly
    && resolved.resolution?.ad_click_id_scope === "first_party_scoped"
    && (!!snapshot || sourceType === "identify" || sourceType === "preserve_attribution" || CONVERSION_EVENTS.has(canonicalizeEventName(eventName)));
  const memory = {
    ad_click_id: adClickId,
    snapshot,
    // Owner-bound links are constructed only after the canonical Durable Object
    // has admitted this exact owner. This closes the concurrent-first-claim race
    // without publishing a second person's links under the first person's click.
    identity_links: [],
    first_party: {
      eden_anonymous_id: anonId,
      eden_anon_id: anonId,
      eden_session_id: sessionId,
      segment_anonymous_id: body?.anonymousId || body?.anonymous_id || body?.context?.anonymousId || null
    },
    identity_refs: {
      user_id_sha256: userHash,
      order_id_sha256: orderHash,
      email_sha256: emailSha256,
      phone_sha256: phoneSha256
    },
    evidence: snapshot ? eventNativeEvidence : candidateEvidence,
    resolution: resolved.resolution,
    observed_at: observedAt,
    pointer_record: resolved.pointerRecord || null,
    pointer_validated: !!resolved.pointerValidated,
    observation_only: observationOnly,
    selected_ad_click_id: resolved.selectedAdClickId || null,
    mode: normalizeAdClickMemoryMode(env),
    setPointerCookie: !observationOnly
      && resolved.resolution?.ad_click_id_scope === "first_party_scoped"
      && !!snapshot
      && !resolved.resolution?.resolution_conflict
      && shouldSetAdClickPointerCookie(env)
  };
  const admission = await reserveAdClickMemoryPointer(env, memory);
  if (!admission.reserved) {
    console.warn(JSON.stringify({
      worker: "eden-analytics",
      event: "ad_click_pointer_admission_rejected",
      reason: admission.owner_conflict ? "owner_conflict" : admission.revoked ? "revoked" : "not_reserved",
      source_type: sourceType,
      source_pipeline_version: PIPELINE_VERSION
    }));
    // Revocation and owner conflict are intentional policy outcomes. Every
    // other failed admission is transient or malformed coordinator state. A
    // server conversion must not acknowledge Segment while silently dropping
    // otherwise valid click evidence, so surface an exception here; the
    // conversion-only failClosed wrapper returns a retryable pre-Segment 503.
    // Browser and non-conversion callers keep their existing fail-open behavior
    // because their safe wrapper converts this exception to a null candidate.
    if (!admission.owner_conflict && !admission.revoked) {
      const error = new Error("ad_click_pointer_admission_unavailable");
      error.code = admission.error || "pointer_admission_not_reserved";
      throw error;
    }
    return null;
  }
  memory.identity_links = shouldEmitLinks ? await buildAdIdentityLinks({
    adClickId,
    anonId,
    session,
    userId,
    orderId,
    emailSha256,
    phoneSha256,
    sourceType,
    eventName,
    linkReason,
    capturedAt: observedAt
  }) : [];
  return memory;
}
__name(buildAdClickMemoryCandidate, "buildAdClickMemoryCandidate");
function applyAdClickMemoryToBody(body, memory) {
  // A no-owner click observation is useful coverage evidence in BigQuery, but
  // it is not a durable Eden identity. Keep instance_random observations out of
  // Segment/server payloads so no downstream consumer can promote an unowned
  // diagnostic object into customer or conversion truth.
  if (!body || !memory?.ad_click_id || memory.observation_only || memory.resolution?.ad_click_id_scope !== "first_party_scoped") return;
  if (!body.properties || typeof body.properties !== "object" || Array.isArray(body.properties)) body.properties = {};
  if (!body.context || typeof body.context !== "object" || Array.isArray(body.context)) body.context = {};
  body.properties.ad_click_id = memory.ad_click_id;
  body.properties.ad_click_memory_mode = memory.mode;
  body.properties.ad_click_primary_type = memory.evidence?.primary_click_id_type || undefined;
  body.properties.ad_click_evidence_class = memory.evidence?.has_class_a ? "class_a_google_ads_upload_click_id" : memory.evidence?.has_primary_click_evidence ? "destination_specific_google_click_id" : "linked_existing_ad_click";
  body.properties.ad_click_resolution_source = memory.resolution?.resolution_source || undefined;
  body.properties.ad_click_resolution_confidence = memory.resolution?.resolution_confidence || undefined;
  body.properties.ad_click_resolution_conflict = memory.resolution?.resolution_conflict || undefined;
  body.properties.ad_click_id_scope = memory.resolution?.ad_click_id_scope || undefined;
  body.context.ad_click_id = memory.ad_click_id;
  body.context.ad_click_memory_mode = memory.mode;
  body.context.ad_click_id_scope = memory.resolution?.ad_click_id_scope || undefined;
  body.context.ad_click_resolution = memory.resolution || undefined;
}
__name(applyAdClickMemoryToBody, "applyAdClickMemoryToBody");
async function persistAdClickMemory(env, memory) {
  const result = { pointer_admitted: false, pointer_committed: false, kv_persisted: false, queue_enqueued: false, stable_identity_bridges_persisted: 0 };
  if (!memory?.ad_click_id) return result;
  // The coordinator reservation validates owner/revocation without activating
  // a new pointer. Queue custody must happen before commit, cache publication,
  // reverse indexes, identity bridges, or a browser pointer cookie.
  const reservation = await reserveAdClickMemoryPointer(env, memory);
  result.pointer_admitted = reservation.reserved;
  if (!reservation.reserved) return result;
  const queueRequired = !!memory.snapshot || (memory.identity_links || []).length > 0;
  try {
    if (queueRequired) {
      if (!shouldEnqueueAdClickMemory(env)) throw new Error("ad_click_snapshot_queue_unavailable");
      result.queue_enqueued = !!await enqueueAdClickMemory(env, memory);
      if (!result.queue_enqueued) throw new Error("ad_click_snapshot_queue_not_enqueued");
    }
    const commit = await commitAdClickMemoryPointer(env, memory);
    result.pointer_committed = commit.committed;
    result.kv_persisted = commit.cache_persisted;
    if (!commit.committed) throw new Error("ad_click_pointer_commit_unconfirmed");
    if (shouldWriteFullAdClickKVIndexes(env)) {
      await writeAdClickMemoryKVReverseIndexes(
        getAdClickMemoryKV(env),
        memory,
        adClickReverseKvTtlSeconds(env, memory)
      );
    }
    result.stable_identity_bridges_persisted = await upsertStableIdentityAdClickPointers(env, memory);
    memory.pointer_committed = true;
  } catch (error) {
    if (!result.pointer_committed) await cancelAdClickMemoryPointerReservation(env, memory).catch(() => {});
    memory.pointer_committed = false;
    throw error;
  }
  return result;
}
__name(persistAdClickMemory, "persistAdClickMemory");
async function enqueueAdClickMemory(env, memory) {
  const queue = getAdClickSnapshotQueue(env);
  if (!queue || typeof queue.send !== "function") return false;
  if (!memory.snapshot && !(memory.identity_links || []).length) return false;
  await queue.send(buildAdClickMemoryQueueEnvelope(memory), { contentType: "json" });
  return true;
}
__name(enqueueAdClickMemory, "enqueueAdClickMemory");
function buildAdClickMemoryQueueEnvelope(memory) {
  return {
    schema_version: "eden_ad_click_memory_envelope_v1",
    event_type: memory.snapshot ? "ad_click_snapshot" : "ad_click_identity_links",
    ad_click_id: memory.ad_click_id,
    snapshot: memory.snapshot || null,
    identity_links: memory.identity_links || [],
    resolution: memory.resolution || null,
    observation_only: !!memory.observation_only,
    selected_ad_click_id: memory.selected_ad_click_id || null,
    emitted_at: memory.observed_at || nowUTC(),
    source_worker: "eden-analytics",
    source_pipeline_version: PIPELINE_VERSION
  };
}
__name(buildAdClickMemoryQueueEnvelope, "buildAdClickMemoryQueueEnvelope");
async function writeAdClickMemoryKVRecords(kv, memory, { fullIndexes = false, env = null } = {}) {
  if (!memory?.ad_click_id) return false;
  const reverseTtl = adClickReverseKvTtlSeconds(env, memory);
  const admission = await admitAdClickMemoryPointer(env, memory);
  if (!admission.admitted) return false;
  if (fullIndexes) await writeAdClickMemoryKVReverseIndexes(kv, memory, reverseTtl);
  return admission.cache_persisted || fullIndexes;
}
__name(writeAdClickMemoryKVRecords, "writeAdClickMemoryKVRecords");
async function mutateAdClickPointerThroughCoordinator(env, adClickId, action, payload = {}) {
  const namespace = env?.[CONVERSION_COORDINATOR_BINDING];
  if (!namespace || typeof namespace.idFromName !== "function" || typeof namespace.get !== "function") {
    throw new Error("ad_click_pointer_coordinator_missing");
  }
  const pointerId = boundedStableIdentifier(adClickId);
  if (!pointerId || !/^adclk2_[A-Za-z0-9_-]{8,128}$/.test(pointerId)) throw new Error("ad_click_pointer_id_invalid");
  const stub = namespace.get(namespace.idFromName(`eden_ad_click_pointer_v1:${pointerId}`));
  if (!stub || typeof stub.fetch !== "function") throw new Error("ad_click_pointer_coordinator_stub_missing");
  const response = await stub.fetch(`https://conversion-coordinator.internal/pointer/${action}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ad_click_id: pointerId, ...payload })
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) {
    if (result?.revoked === true || result?.ownership_valid === false || result?.owner_conflict === true || result?.error === "pointer_reservation_busy") return result;
    throw new Error(`ad_click_pointer_coordinator_${action}_${response.status}`);
  }
  return result;
}
__name(mutateAdClickPointerThroughCoordinator, "mutateAdClickPointerThroughCoordinator");
function requiresAdClickPointerAdmission(memory) {
  return !!memory?.ad_click_id
    && !memory.observation_only
    && memory.resolution?.ad_click_id_scope === "first_party_scoped"
    && !!(memory.snapshot || memory.pointer_record);
}
__name(requiresAdClickPointerAdmission, "requiresAdClickPointerAdmission");
async function admitAdClickMemoryPointer(env, memory) {
  if (!requiresAdClickPointerAdmission(memory)) {
    return { admitted: true, required: false, cache_persisted: false };
  }
  if (memory.pointer_admission?.admitted === true) return memory.pointer_admission;
  const outcome = await writeAdClickMemoryKVPointerRecord(
    getAdClickMemoryKV(env),
    memory,
    adClickPointerKvTtlSeconds(env, memory),
    env,
    { persistCache: shouldWriteAdClickMemoryKV(env) }
  );
  memory.pointer_admission = outcome;
  if (!outcome.admitted) {
    memory.identity_links = [];
    memory.setPointerCookie = false;
  }
  return outcome;
}
__name(admitAdClickMemoryPointer, "admitAdClickMemoryPointer");
async function buildAdClickMemoryPointerProposal(env, memory, ttl, persistCache) {
  if (!memory?.ad_click_id || (!memory.snapshot && !memory.pointer_record)) {
    return null;
  }
  const snapshot = memory.snapshot || null;
  const kvRecord = await readCanonicalAdClickPointerRecord(env, memory.ad_click_id, { repairCache: persistCache });
  const existing = kvRecord || memory.pointer_record || {};
  const anonId = snapshot?.first_party?.eden_anonymous_id || snapshot?.first_party?.eden_anon_id || memory.first_party?.eden_anonymous_id || memory.first_party?.eden_anon_id || null;
  const sessionId = snapshot?.first_party?.eden_session_id || memory.first_party?.eden_session_id || null;
  const owner = await currentPointerOwnerContext({ anonId, session: sessionId });
  const hasNewPrimaryEvidence = !!memory.evidence?.has_primary_click_evidence;
  const compact = compactDefined({
    schema_version: AD_CLICK_POINTER_RECORD_SCHEMA_VERSION,
    ad_click_id: memory.ad_click_id,
    snapshot_id: snapshot?.snapshot_id || existing.snapshot_id,
    captured_at: snapshot?.captured_at || existing.captured_at || nowUTC(),
    updated_at: nowUTC(),
    source_worker: "eden-analytics",
    primary_click_id_type: hasNewPrimaryEvidence ? memory.evidence?.primary_click_id_type : existing.primary_click_id_type,
    raw_primary_click_id_sha256: snapshot?.evidence?.raw_primary_click_id_sha256 || existing.raw_primary_click_id_sha256,
    evidence_classes: hasNewPrimaryEvidence ? memory.evidence?.evidence_classes : existing.evidence_classes,
    has_class_a: hasNewPrimaryEvidence ? !!memory.evidence?.has_class_a : existing.has_class_a,
    has_primary_click_evidence: hasNewPrimaryEvidence ? true : existing.has_primary_click_evidence,
    owner_anonymous_id_sha256: existing.owner_anonymous_id_sha256 || owner.anonymous_id_sha256,
    owner_session_id_sha256: existing.owner_session_id_sha256 || owner.session_id_sha256,
    claimed_user_id_sha256: existing.claimed_user_id_sha256 || memory.identity_refs?.user_id_sha256,
    claimed_order_id_sha256: existing.claimed_order_id_sha256 || memory.identity_refs?.order_id_sha256,
    ad_click_id_scope: memory.resolution?.ad_click_id_scope || existing.ad_click_id_scope,
    ownership_scope: "first_party_owner_bound"
  });
  return { compact, kvRecord, ttl, persistCache };
}
__name(buildAdClickMemoryPointerProposal, "buildAdClickMemoryPointerProposal");
async function reserveAdClickMemoryPointer(env, memory) {
  if (!requiresAdClickPointerAdmission(memory)) {
    const outcome = { reserved: true, required: false, cache_persisted: false };
    memory.pointer_reservation = outcome;
    return outcome;
  }
  if (memory.pointer_reservation?.reserved === true) return memory.pointer_reservation;
  const ttl = adClickPointerKvTtlSeconds(env, memory);
  const persistCache = shouldWriteAdClickMemoryKV(env);
  const proposal = await buildAdClickMemoryPointerProposal(env, memory, ttl, persistCache);
  if (!proposal) return { reserved: false, required: true, cache_persisted: false };
  const reservationId = `adrsrv_${(await sha256Raw(JSON.stringify({
    ad_click_id: memory.ad_click_id,
    snapshot_id: memory.snapshot?.snapshot_id || proposal.compact.snapshot_id || null,
    captured_at: proposal.compact.captured_at,
    owner_anonymous_id_sha256: proposal.compact.owner_anonymous_id_sha256 || null,
    owner_session_id_sha256: proposal.compact.owner_session_id_sha256 || null,
    claimed_user_id_sha256: proposal.compact.claimed_user_id_sha256 || null,
    claimed_order_id_sha256: proposal.compact.claimed_order_id_sha256 || null
  }))).slice(0, 32)}`;
  let result = null;
  for (let attempt = 0; attempt < 10; attempt += 1) {
    result = await mutateAdClickPointerThroughCoordinator(env, memory.ad_click_id, "reserve", {
      reservation_id: reservationId,
      proposed_record: proposal.compact,
      seed_record: proposal.kvRecord || null,
      ttl_seconds: ttl,
      persist_cache: persistCache
    });
    if (result?.error !== "pointer_reservation_busy") break;
    await new Promise((resolve) => setTimeout(resolve, 10 * (attempt + 1)));
  }
  const outcome = {
    reserved: result?.reserved === true,
    required: true,
    revoked: result?.revoked === true,
    owner_conflict: result?.owner_conflict === true,
    error: typeof result?.error === "string" ? result.error : null,
    reservation_id: result?.reserved === true ? reservationId : null,
    cache_persisted: false
  };
  memory.pointer_reservation = outcome;
  if (!outcome.reserved) {
    memory.identity_links = [];
    memory.setPointerCookie = false;
  }
  return outcome;
}
__name(reserveAdClickMemoryPointer, "reserveAdClickMemoryPointer");
async function commitAdClickMemoryPointer(env, memory) {
  if (!requiresAdClickPointerAdmission(memory)) return { committed: true, required: false, cache_persisted: false };
  const reservationId = memory?.pointer_reservation?.reservation_id;
  if (!reservationId) return { committed: false, required: true, cache_persisted: false };
  const result = await mutateAdClickPointerThroughCoordinator(env, memory.ad_click_id, "commit", {
    reservation_id: reservationId
  });
  return {
    committed: result?.committed === true,
    required: true,
    cache_persisted: result?.cache_persisted === true,
    replay: result?.replay === true
  };
}
__name(commitAdClickMemoryPointer, "commitAdClickMemoryPointer");
async function cancelAdClickMemoryPointerReservation(env, memory) {
  if (!requiresAdClickPointerAdmission(memory) || !memory?.pointer_reservation?.reservation_id) return false;
  const result = await mutateAdClickPointerThroughCoordinator(env, memory.ad_click_id, "cancel", {
    reservation_id: memory.pointer_reservation.reservation_id
  });
  memory.pointer_reservation = null;
  return result?.cancelled === true;
}
__name(cancelAdClickMemoryPointerReservation, "cancelAdClickMemoryPointerReservation");
async function writeAdClickMemoryKVPointerRecord(kv, memory, ttl, env = null, { persistCache = true } = {}) {
  const proposal = await buildAdClickMemoryPointerProposal(env, memory, ttl, persistCache);
  if (!proposal) return { admitted: false, required: false, cache_persisted: false };
  const result = await mutateAdClickPointerThroughCoordinator(env, memory.ad_click_id, "upsert", {
    proposed_record: proposal.compact,
    seed_record: proposal.kvRecord || null,
    ttl_seconds: ttl,
    persist_cache: persistCache
  });
  if (result?.revoked === true) {
    console.error(JSON.stringify({ worker: "eden-analytics", event: "revoked_ad_click_pointer_write_blocked", source_pipeline_version: PIPELINE_VERSION }));
    return { admitted: false, required: true, revoked: true, owner_conflict: false, cache_persisted: false };
  }
  if (result?.owner_conflict === true) {
    console.error(JSON.stringify({ worker: "eden-analytics", event: "conflicting_ad_click_pointer_owner_blocked", source_pipeline_version: PIPELINE_VERSION }));
    return { admitted: false, required: true, revoked: false, owner_conflict: true, cache_persisted: false };
  }
  return {
    admitted: result?.persisted === true,
    required: true,
    revoked: false,
    owner_conflict: false,
    cache_persisted: result?.cache_persisted === true
  };
}
__name(writeAdClickMemoryKVPointerRecord, "writeAdClickMemoryKVPointerRecord");
async function writeAdClickMemoryKVReverseIndexes(kv, memory, ttl) {
  if (!kv || !memory?.ad_click_id) return;
  const snapshot = memory.snapshot || null;
  const firstParty = snapshot?.first_party || memory.first_party || {};
  const identityRefs = snapshot?.identity_refs || memory.identity_refs || {};
  const writes = [];
  const anonId = firstParty.eden_anonymous_id || firstParty.eden_anon_id;
  const sessionId = firstParty.eden_session_id;
  const userHash = identityRefs.user_id_sha256;
  const orderHash = identityRefs.order_id_sha256;
  if (anonId) {
    const anonHash = await sha256Raw(anonId);
    writes.push(putIfAbsent(kv, `${AD_CLICK_KV_REVERSE_PREFIX}anon:${anonHash}:first_paid`, memory.ad_click_id, ttl));
    writes.push(kv.put(`${AD_CLICK_KV_REVERSE_PREFIX}anon:${anonHash}:last_paid`, memory.ad_click_id, adClickKvPutOptions(ttl)));
  }
  if (sessionId) writes.push(kv.put(`${AD_CLICK_KV_REVERSE_PREFIX}session:${await sha256Raw(sessionId)}:current`, memory.ad_click_id, adClickKvPutOptions(ttl)));
  if (userHash) {
    writes.push(putIfAbsent(kv, `${AD_CLICK_KV_REVERSE_PREFIX}user:${userHash}:first_paid`, memory.ad_click_id, ttl));
    writes.push(kv.put(`${AD_CLICK_KV_REVERSE_PREFIX}user:${userHash}:last_paid`, memory.ad_click_id, adClickKvPutOptions(ttl)));
  }
  if (orderHash) writes.push(kv.put(`${AD_CLICK_KV_REVERSE_PREFIX}order:${orderHash}`, memory.ad_click_id, adClickKvPutOptions(ttl)));
  // Click-value and _gcl_au reverse keys are intentionally NOT written. A raw click
  // value (gbraid/wbraid especially) is Google ad evidence, not an Eden identity key;
  // click-keyed reverse indexes were the cross-user bridge in the July 5-7 2026
  // regression. Raw-click comparability lives in the BigQuery snapshot *_sha256 hashes.
  await Promise.all(writes);
}
__name(writeAdClickMemoryKVReverseIndexes, "writeAdClickMemoryKVReverseIndexes");
async function putIfAbsent(kv, key, value, ttl) {
  try {
    const existing = await kv.get(key);
    if (existing) return;
  } catch {}
  await kv.put(key, value, adClickKvPutOptions(ttl));
}
__name(putIfAbsent, "putIfAbsent");
function buildAdClickPointerCookie(adClickId, url, env) {
  return [
    `${AD_CLICK_POINTER_COOKIE_NAME}=${encodeURIComponent(adClickId)}`,
    `Max-Age=${Number.parseInt(String(env?.EDEN_AD_CLICK_COOKIE_TTL || AD_CLICK_POINTER_COOKIE_TTL), 10) || AD_CLICK_POINTER_COOKIE_TTL}`,
    `Domain=${cookieDomain(url)}`,
    "Path=/",
    "HttpOnly",
    "Secure",
    "SameSite=Lax"
  ].join("; ");
}
__name(buildAdClickPointerCookie, "buildAdClickPointerCookie");
function isEnvFlagEnabled(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return ["1", "true", "enabled", "on", "yes"].includes(normalized);
}
__name(isEnvFlagEnabled, "isEnvFlagEnabled");
function shouldConsumeAdClickMemoryQueue(env) {
  return isEnvFlagEnabled(env?.[AD_CLICK_QUEUE_CONSUMER_ENABLED_ENV]);
}
__name(shouldConsumeAdClickMemoryQueue, "shouldConsumeAdClickMemoryQueue");
function normalizeQueueMessageBody(message) {
  if (!message)
    throw new Error("missing_queue_message");
  const body = message.body ?? message;
  if (!body || typeof body !== "object" || Array.isArray(body))
    throw new Error("invalid_queue_message_body");
  return body;
}
__name(normalizeQueueMessageBody, "normalizeQueueMessageBody");
function validateAdClickMemoryEnvelope(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload))
    throw new Error("invalid_ad_click_memory_envelope");
  if (payload.schema_version !== AD_CLICK_QUEUE_ENVELOPE_SCHEMA_VERSION)
    throw new Error(`unsupported_ad_click_memory_schema:${payload.schema_version || "missing"}`);
  if (!["ad_click_snapshot", "ad_click_identity_links"].includes(payload.event_type))
    throw new Error(`unsupported_ad_click_memory_event_type:${payload.event_type || "missing"}`);
  // Accept both v2 (adclk2_) and in-flight legacy v1 (adclk_) envelopes at ingest:
  // BigQuery landing keeps full provenance; v1 quarantine applies to resolution, not storage.
  if (!payload.ad_click_id || !/^adclk2?_/.test(String(payload.ad_click_id)))
    throw new Error("missing_or_invalid_ad_click_id");
  if (!payload.emitted_at || !Number.isFinite(Date.parse(String(payload.emitted_at))))
    throw new Error("missing_or_invalid_envelope_emitted_at");
  const hasSnapshot = !!payload.snapshot;
  const hasLinks = Array.isArray(payload.identity_links) && payload.identity_links.length > 0;
  if (!hasSnapshot && !hasLinks)
    throw new Error("empty_ad_click_memory_envelope");
  if (payload.event_type === "ad_click_snapshot" && !hasSnapshot)
    throw new Error("missing_ad_click_snapshot_payload");
  if (hasSnapshot && (!payload.snapshot.snapshot_id || !/^adsnap_[A-Za-z0-9_-]{8,128}$/.test(String(payload.snapshot.snapshot_id))))
    throw new Error("missing_or_invalid_snapshot_id");
  if (hasSnapshot && payload.snapshot.ad_click_id !== payload.ad_click_id)
    throw new Error("snapshot_envelope_ad_click_id_mismatch");
  if (payload.observation_only) {
    if (!/^adclk2_[A-Za-z0-9_-]{8,128}$/.test(String(payload.selected_ad_click_id || "")))
      throw new Error("observation_only_selected_ad_click_id_missing_or_invalid");
    if (payload.selected_ad_click_id === payload.ad_click_id)
      throw new Error("observation_only_selected_ad_click_id_not_distinct");
    if (hasLinks)
      throw new Error("observation_only_identity_links_forbidden");
    if (!payload.resolution?.resolution_conflict)
      throw new Error("observation_only_conflict_resolution_required");
  }
  return payload;
}
__name(validateAdClickMemoryEnvelope, "validateAdClickMemoryEnvelope");
function adClickBigQueryConfig(env) {
  const projectId = env?.AD_CLICK_BIGQUERY_PROJECT_ID || env?.BIGQUERY_PROJECT_ID || null;
  const datasetId = env?.AD_CLICK_BIGQUERY_DATASET_ID || env?.BIGQUERY_DATASET_ID || null;
  const serviceAccountKey = env?.AD_CLICK_GOOGLE_SERVICE_ACCOUNT_KEY || env?.GOOGLE_SERVICE_ACCOUNT_KEY || null;
  const accessToken = env?.AD_CLICK_BIGQUERY_ACCESS_TOKEN || null;
  return {
    projectId,
    datasetId,
    snapshotTableId: env?.AD_CLICK_SNAPSHOT_BIGQUERY_TABLE_ID || AD_CLICK_SNAPSHOT_BIGQUERY_TABLE_DEFAULT,
    identityLinkTableId: env?.AD_CLICK_IDENTITY_LINK_BIGQUERY_TABLE_ID || AD_CLICK_IDENTITY_LINK_BIGQUERY_TABLE_DEFAULT,
    errorTableId: env?.AD_CLICK_INGEST_ERROR_BIGQUERY_TABLE_ID || AD_CLICK_INGEST_ERROR_BIGQUERY_TABLE_DEFAULT,
    serviceAccountKey,
    accessToken
  };
}
__name(adClickBigQueryConfig, "adClickBigQueryConfig");
function assertAdClickBigQueryConfigured(config) {
  if (!config.projectId || !config.datasetId)
    throw new Error("ad_click_bigquery_project_or_dataset_missing");
  if (!config.serviceAccountKey && !config.accessToken)
    throw new Error("ad_click_bigquery_auth_missing");
}
__name(assertAdClickBigQueryConfigured, "assertAdClickBigQueryConfigured");
var adClickGoogleAccessTokenCache = { token: null, clientEmail: null, expiresAtMs: 0 };
async function createGoogleServiceAccountJWT(serviceAccount) {
  const headerEncoded = btoa(JSON.stringify({ alg: "RS256", typ: "JWT" })).replace(/[+/]/g, (match) => match === "+" ? "-" : "_").replace(/=/g, "");
  const now = Math.floor(Date.now() / 1e3);
  const payloadEncoded = btoa(JSON.stringify({
    iss: serviceAccount.client_email,
    scope: "https://www.googleapis.com/auth/bigquery.insertdata",
    aud: serviceAccount.token_uri || "https://oauth2.googleapis.com/token",
    exp: now + 3600,
    iat: now
  })).replace(/[+/]/g, (match) => match === "+" ? "-" : "_").replace(/=/g, "");
  const message = `${headerEncoded}.${payloadEncoded}`;
  const privateKeyDer = String(serviceAccount.private_key || "").replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "").replace(/\s/g, "");
  const privateKeyArrayBuffer = Uint8Array.from(atob(privateKeyDer), (c) => c.charCodeAt(0));
  const privateKey = await crypto.subtle.importKey(
    "pkcs8",
    privateKeyArrayBuffer,
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const signature = await crypto.subtle.sign("RSASSA-PKCS1-v1_5", privateKey, new TextEncoder().encode(message));
  const signatureBase64 = btoa(String.fromCharCode(...new Uint8Array(signature))).replace(/[+/]/g, (match) => match === "+" ? "-" : "_").replace(/=/g, "");
  return `${message}.${signatureBase64}`;
}
__name(createGoogleServiceAccountJWT, "createGoogleServiceAccountJWT");
async function getAdClickBigQueryAccessToken(config) {
  if (config.accessToken)
    return config.accessToken;
  const nowMs = Date.now();
  const serviceAccount = JSON.parse(atob(config.serviceAccountKey));
  if (adClickGoogleAccessTokenCache.token && adClickGoogleAccessTokenCache.clientEmail === serviceAccount.client_email && adClickGoogleAccessTokenCache.expiresAtMs - nowMs > 6e4)
    return adClickGoogleAccessTokenCache.token;
  const jwt = await createGoogleServiceAccountJWT(serviceAccount);
  const tokenResponse = await fetch(serviceAccount.token_uri || "https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion: jwt
    }),
    signal: AbortSignal.timeout(AD_CLICK_BIGQUERY_EXTERNAL_IO_TIMEOUT_MS)
  });
  if (!tokenResponse.ok)
    throw new Error(`ad_click_bigquery_token_failed:${tokenResponse.status}`);
  const tokenData = await tokenResponse.json();
  const expiresInSeconds = Number(tokenData.expires_in || 3600);
  adClickGoogleAccessTokenCache = {
    clientEmail: serviceAccount.client_email,
    token: tokenData.access_token,
    expiresAtMs: nowMs + Math.max(expiresInSeconds - 300, 60) * 1e3
  };
  return tokenData.access_token;
}
__name(getAdClickBigQueryAccessToken, "getAdClickBigQueryAccessToken");
function adClickBigQueryJson(value) {
  if (!value || typeof value !== "object" || Array.isArray(value))
    return null;
  return Object.keys(value).length ? JSON.stringify(value) : null;
}
__name(adClickBigQueryJson, "adClickBigQueryJson");
function buildAdClickSnapshotBigQueryRow(snapshot, envelope, ingestedAt, rawEnvelopeSha256 = null) {
  if (!snapshot || typeof snapshot !== "object")
    return null;
  return compactDefined({
    schema_version: snapshot.schema_version,
    envelope_event_type: envelope.event_type,
    snapshot_id: snapshot.snapshot_id,
    ad_click_id: snapshot.ad_click_id || envelope.ad_click_id,
    captured_at: snapshot.captured_at,
    source_worker: snapshot.source_worker || envelope.source_worker,
    source_pipeline_version: snapshot.source_pipeline_version || envelope.source_pipeline_version || null,
    source_type: snapshot.source_type,
    event_name: snapshot.event_name,
    route_host: snapshot.route_host,
    source_route_host: snapshot.source_route_host,
    landing_url_sanitized: snapshot.landing_url_sanitized,
    raw_query_present: snapshot.raw_query_present,
    gclid: snapshot.google?.gclid || null,
    gbraid: snapshot.google?.gbraid || null,
    wbraid: snapshot.google?.wbraid || null,
    dclid: snapshot.google?.dclid || null,
    gclid_sha256: snapshot.google?.gclid_sha256 || null,
    gbraid_sha256: snapshot.google?.gbraid_sha256 || null,
    wbraid_sha256: snapshot.google?.wbraid_sha256 || null,
    dclid_sha256: snapshot.google?.dclid_sha256 || null,
    diagnostic_google_json: adClickBigQueryJson(snapshot.diagnostic_google),
    campaign_json: adClickBigQueryJson(snapshot.campaign),
    // The physical landing column remains eden_anon_id for compatibility; the
    // queue envelope carries the canonical eden_anonymous_id alongside it.
    eden_anon_id: snapshot.first_party?.eden_anonymous_id || snapshot.first_party?.eden_anon_id || null,
    eden_session_id: snapshot.first_party?.eden_session_id || null,
    segment_anonymous_id: snapshot.first_party?.segment_anonymous_id || null,
    user_id_sha256: snapshot.identity_refs?.user_id_sha256 || null,
    order_id_sha256: snapshot.identity_refs?.order_id_sha256 || null,
    email_sha256: snapshot.identity_refs?.email_sha256 || null,
    phone_sha256: snapshot.identity_refs?.phone_sha256 || null,
    primary_click_id_type: snapshot.evidence?.primary_click_id_type || null,
    raw_primary_click_id_sha256: snapshot.evidence?.raw_primary_click_id_sha256 || null,
    evidence_classes_json: adClickBigQueryJson(snapshot.evidence?.evidence_classes),
    upload_candidate_types: snapshot.evidence?.upload_candidate_types || [],
    destination_specific_candidate_types: snapshot.evidence?.destination_specific_candidate_types || [],
    diagnostic_only_types: snapshot.evidence?.diagnostic_only_types || [],
    campaign_context_types: snapshot.evidence?.campaign_context_types || [],
    acquisition_channel: snapshot.evidence?.acquisition_channel || null,
    attribution_confidence: snapshot.evidence?.attribution_confidence || null,
    missing_gclid_reason: snapshot.evidence?.missing_gclid_reason || null,
    gpc_opt_out: snapshot.governance?.gpc_opt_out || false,
    attribution_suppressed: snapshot.governance?.attribution_suppressed || false,
    resolution_source: snapshot.governance?.resolution_source || envelope.resolution?.resolution_source || null,
    resolution_confidence: snapshot.governance?.resolution_confidence || envelope.resolution?.resolution_confidence || null,
    resolution_conflict: !!(snapshot.governance?.resolution_conflict || envelope.resolution?.resolution_conflict),
    resolution_conflict_sources_json: adClickBigQueryJson({
      sources: snapshot.governance?.resolution_conflict_sources || envelope.resolution?.resolution_conflict_sources || [],
      observation_only: !!envelope.observation_only,
      selected_ad_click_id: envelope.selected_ad_click_id || null
    }),
    resolution_policy_version: snapshot.governance?.resolution_policy_version || envelope.resolution?.resolution_policy_version || null,
    resolved_at: snapshot.governance?.resolved_at || envelope.resolution?.resolved_at || null,
    // Lands once the additive BigQuery column exists; ignoreUnknownValues=true keeps
    // inserts safe before the owner-approved DDL is applied.
    ad_click_id_scope: snapshot.governance?.ad_click_id_scope || envelope.resolution?.ad_click_id_scope || null,
    final_upload_eligibility_source: snapshot.governance?.final_upload_eligibility_source || "dbt_google_outbox_validator",
    raw_envelope_sha256: rawEnvelopeSha256,
    envelope_ingested_at: ingestedAt
  });
}
__name(buildAdClickSnapshotBigQueryRow, "buildAdClickSnapshotBigQueryRow");
function buildAdClickIdentityLinkBigQueryRows(envelope, ingestedAt, rawEnvelopeSha256 = null) {
  const links = Array.isArray(envelope.identity_links) ? envelope.identity_links : [];
  return links.map((link) => compactDefined({
    schema_version: link.schema_version,
    envelope_event_type: envelope.event_type,
    link_id: link.link_id,
    ad_click_id: envelope.ad_click_id,
    linked_at: link.linked_at,
    source_worker: link.source_worker || envelope.source_worker,
    source_pipeline_version: link.source_pipeline_version || envelope.source_pipeline_version || null,
    source_type: link.source_type,
    event_name: link.event_name,
    from_type: link.from_type,
    from_id: link.from_id,
    to_type: link.to_type,
    to_id: link.to_id,
    link_reason: link.link_reason,
    confidence: link.confidence,
    resolution_source: envelope.resolution?.resolution_source || null,
    resolution_confidence: envelope.resolution?.resolution_confidence || null,
    resolution_conflict: !!envelope.resolution?.resolution_conflict,
    resolution_conflict_sources_json: adClickBigQueryJson({ sources: envelope.resolution?.resolution_conflict_sources || [] }),
    resolution_policy_version: envelope.resolution?.resolution_policy_version || null,
    resolved_at: envelope.resolution?.resolved_at || null,
    ad_click_id_scope: envelope.resolution?.ad_click_id_scope || null,
    raw_envelope_sha256: rawEnvelopeSha256,
    envelope_ingested_at: ingestedAt
  }));
}
__name(buildAdClickIdentityLinkBigQueryRows, "buildAdClickIdentityLinkBigQueryRows");
function adClickInsertIdForRow(row) {
  return row.snapshot_id || row.link_id || row.error_id || row.ad_click_id || void 0;
}
__name(adClickInsertIdForRow, "adClickInsertIdForRow");
async function insertAdClickBigQueryRows(config, tableId, rows, accessToken) {
  const cleanRows = (rows || []).filter(Boolean);
  if (!cleanRows.length)
    return { ok: true, skipped: true, inserted: 0 };
  const insertData = {
    kind: "bigquery#tableDataInsertAllRequest",
    ignoreUnknownValues: true,
    rows: cleanRows.map((row) => ({
      insertId: adClickInsertIdForRow(row),
      json: row
    }))
  };
  const response = await fetch(`https://bigquery.googleapis.com/bigquery/v2/projects/${config.projectId}/datasets/${config.datasetId}/tables/${tableId}/insertAll`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(insertData),
    signal: AbortSignal.timeout(AD_CLICK_BIGQUERY_EXTERNAL_IO_TIMEOUT_MS)
  });
  if (!response.ok)
    throw new Error(`ad_click_bigquery_insert_failed:${tableId}:${response.status}`);
  const result = await response.json().catch(() => ({}));
  if (Array.isArray(result.insertErrors) && result.insertErrors.length > 0) {
    const firstError = result.insertErrors[0]?.errors?.[0] || {};
    const reason = [firstError.reason, firstError.location].filter(Boolean).join(":") || "unknown";
    throw new Error(`ad_click_bigquery_insert_errors:${tableId}:${result.insertErrors.length}:${reason}`);
  }
  return { ok: true, inserted: cleanRows.length };
}
__name(insertAdClickBigQueryRows, "insertAdClickBigQueryRows");
function adClickSafeErrorMessage(error) {
  return String(error?.message || error || "unknown_error").replace(/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/g, "[redacted_email]").slice(0, 500);
}
__name(adClickSafeErrorMessage, "adClickSafeErrorMessage");
async function buildAdClickIngestErrorRow(payload, error, stage = "queue_consumer") {
  const observedAt = nowUTC();
  return {
    error_id: `aderr_${(await sha256Raw(JSON.stringify({ observedAt, stage, ad_click_id: payload?.ad_click_id || null, error: adClickSafeErrorMessage(error) }))).slice(0, 32)}`,
    observed_at: observedAt,
    source_worker: payload?.source_worker || "eden-analytics",
    source_pipeline_version: payload?.source_pipeline_version || payload?.snapshot?.source_pipeline_version || null,
    envelope_event_type: payload?.event_type || null,
    envelope_ad_click_id: payload?.ad_click_id || null,
    error_stage: stage,
    error_message: adClickSafeErrorMessage(error),
    raw_envelope_sha256: payload ? await sha256Raw(JSON.stringify(payload)) : null,
    replay_status: "needs_review"
  };
}
__name(buildAdClickIngestErrorRow, "buildAdClickIngestErrorRow");
async function processAdClickMemoryQueueMessage(message, env, config, accessToken) {
  let payload = null;
  try {
    payload = validateAdClickMemoryEnvelope(normalizeQueueMessageBody(message));
    // The queue is at-least-once. Reuse the immutable envelope timestamp so a
    // retry produces byte-for-byte stable BigQuery rows with the same insertId.
    const ingestedAt = payload.emitted_at;
    // Persist the immutable envelope fingerprint on every successful landing
    // row. Error-ledger recovery can then prove the exact failed envelope was
    // replayed successfully instead of accepting an unrelated later row that
    // merely shares ad_click_id.
    const rawEnvelopeSha256 = await sha256Raw(JSON.stringify(payload));
    const snapshotRows = payload.snapshot ? [buildAdClickSnapshotBigQueryRow(payload.snapshot, payload, ingestedAt, rawEnvelopeSha256)] : [];
    const identityRows = buildAdClickIdentityLinkBigQueryRows(payload, ingestedAt, rawEnvelopeSha256);
    await insertAdClickBigQueryRows(config, config.snapshotTableId, snapshotRows, accessToken);
    await insertAdClickBigQueryRows(config, config.identityLinkTableId, identityRows, accessToken);
    return {
      ok: true,
      ad_click_id: payload.ad_click_id,
      snapshot_rows: snapshotRows.filter(Boolean).length,
      identity_link_rows: identityRows.filter(Boolean).length
    };
  } catch (error) {
    try {
      const errorRow = await buildAdClickIngestErrorRow(payload || message?.body || null, error);
      await insertAdClickBigQueryRows(config, config.errorTableId, [errorRow], accessToken);
    } catch (ledgerError) {
      console.error("[eden-analytics] ad-click queue error-ledger insert failed:", adClickSafeErrorMessage(ledgerError));
    }
    throw error;
  }
}
__name(processAdClickMemoryQueueMessage, "processAdClickMemoryQueueMessage");
async function processAdClickMemoryQueueBatch(batch, env) {
  if (!shouldConsumeAdClickMemoryQueue(env)) {
    if (typeof batch?.retryAll === "function") batch.retryAll();
    else for (const message of batch?.messages || []) if (typeof message?.retry === "function") message.retry();
    console.error(JSON.stringify({ worker: "eden-analytics", event: "ad_click_queue_batch_retried", reason: "consumer_disabled", message_count: batch?.messages?.length || 0 }));
    return { ok: false, retried: batch?.messages?.length || 0, reason: "consumer_disabled" };
  }
  let config;
  let accessToken;
  try {
    config = adClickBigQueryConfig(env);
    assertAdClickBigQueryConfigured(config);
    accessToken = await getAdClickBigQueryAccessToken(config);
  } catch (error) {
    if (typeof batch?.retryAll === "function") batch.retryAll();
    else for (const message of batch?.messages || []) if (typeof message?.retry === "function") message.retry();
    console.error(JSON.stringify({
      worker: "eden-analytics",
      event: "ad_click_queue_batch_retried",
      reason: "consumer_initialization_failed",
      message_count: batch?.messages?.length || 0,
      error: adClickSafeErrorMessage(error)
    }));
    return { ok: false, retried: batch?.messages?.length || 0, reason: "consumer_initialization_failed" };
  }
  const failures = [];
  let acknowledged = 0;
  const messages = [...batch?.messages || []];
  let nextMessageIndex = 0;
  const processNextMessage = async () => {
    while (nextMessageIndex < messages.length) {
      const message = messages[nextMessageIndex++];
      try {
        await processAdClickMemoryQueueMessage(message, env, config, accessToken);
        if (typeof message?.ack === "function") message.ack();
        acknowledged += 1;
      } catch (error) {
        failures.push(error);
        if (typeof message?.retry === "function") message.retry();
        console.error("[eden-analytics] ad-click queue processing failed:", adClickSafeErrorMessage(error));
      }
    }
  };
  await Promise.all(Array.from(
    { length: Math.min(AD_CLICK_QUEUE_BATCH_CONCURRENCY, messages.length) },
    () => processNextMessage()
  ));
  if (failures.length > 0) {
    // Individual retry()/ack() decisions are authoritative. Returning normally
    // avoids turning a selective transient failure into an invocation failure
    // that can unnecessarily reduce consumer concurrency.
    console.error(JSON.stringify({
      worker: "eden-analytics",
      event: "ad_click_queue_batch_partial_failure",
      acknowledged,
      retried: failures.length,
      message_count: batch?.messages?.length || 0
    }));
  }
  return { ok: failures.length === 0, acknowledged, retried: failures.length };
}
__name(processAdClickMemoryQueueBatch, "processAdClickMemoryQueueBatch");
function extractPreAuthAttribution(request) {
  const preAuth = (() => {
    const raw = readCookie(request, "eden_pre_auth");
    if (!raw)
      return null;
    try {
      return JSON.parse(decodeURIComponent(raw));
    } catch {
      return null;
    }
  })();
  const attrCookie = (() => {
    const raw = readCookie(request, ATTR_COOKIE_NAME);
    if (!raw)
      return null;
    try {
      const parsed = JSON.parse(decodeURIComponent(raw));
      const clickFirstObservedAt = Number(parsed._click_first_observed_at || parsed._ts || 0) || null;
      const age = clickFirstObservedAt ? Date.now() - clickFirstObservedAt : Infinity;
      const hasClickId = CLICK_ID_PARAMS.some((p) => parsed[p]);
      if (hasClickId && age > ATTR_COOKIE_CLICK_ID_TTL) {
        const utmsOnly = {};
        for (const k of ["utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term", "utm_id"]) {
          if (parsed[k])
            utmsOnly[k] = parsed[k];
        }
        return Object.keys(utmsOnly).length ? utmsOnly : null;
      }
      const { _ts, _click_first_observed_at, _last_seen_at, ...rest } = parsed;
      if (hasClickId && clickFirstObservedAt) rest._click_first_observed_at = clickFirstObservedAt;
      return Object.keys(rest).length ? rest : null;
    } catch {
      return null;
    }
  })();
  if (preAuth && attrCookie)
    return { ...attrCookie, ...preAuth };
  return preAuth || attrCookie || null;
}
__name(extractPreAuthAttribution, "extractPreAuthAttribution");
async function buildSegmentTrackPayload(body, anonId, superProps, attribution = {}, messageId = null, options = {}) {
  const eventTimestamp = validatedProducerEventTimestamp(body) || nowUTC();
  const mergedContext = {
    ...body.context || {},
    campaign: { ...(body.context || {}).campaign || {}, ...buildCampaignContext(attribution) }
  };
  const resolvedEventName = String(resolveEventName(body) || "").trim();
  const eventName = (options.preserveEventName ? resolvedEventName : canonicalizeEventName(resolvedEventName)) || null;
  const orderIdDetails = resolveOrderIdDetails(body);
  const orderId = orderIdDetails.value;
  if (eventName) body.event = eventName;
  if (orderId && orderIdDetails.namespace === "order_id" && body.properties && !body.properties.order_id) {
    body.properties.order_id = orderId;
  }
  if (!eventName) return null;
  const properties = await hashEmail({ ...body.properties || {}, ...superProps });
  let outboundMessageId = messageId;
  if (messageId) {
    // Segment's Mixpanel destination maps Segment messageId to $insert_id.
    // Mixpanel permits <=36 bytes containing only alphanumeric characters or
    // hyphens, so the destination-facing ID must be the deterministic hash.
    // The original remains available for coordinator validation and retries.
    outboundMessageId = `m-${(await sha256Raw(`eden_mixpanel_insert_id_v1\0${messageId}`)).slice(0, 32)}`;
    properties.mixpanel_insert_id = outboundMessageId;
    properties.segment_source_message_id = messageId;
  }
  return {
    anonymousId: anonId,
    userId: resolveUserIdFromBody(body),
    event: eventName,
    properties,
    context: mergedContext,
    timestamp: eventTimestamp,
    ...outboundMessageId ? { messageId: outboundMessageId } : {}
  };
}
__name(buildSegmentTrackPayload, "buildSegmentTrackPayload");
function validPendingSegmentTrackPayload(payload, expectedMessageId = null) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return false;
  if (!payload.messageId) return false;
  if (expectedMessageId
    && payload.messageId !== expectedMessageId
    && payload.properties?.segment_source_message_id !== expectedMessageId) return false;
  if (!payload.event || canonicalizeEventName(payload.event) !== payload.event) return false;
  if (!payload.timestamp || !Number.isFinite(Date.parse(String(payload.timestamp)))) return false;
  try {
    return new TextEncoder().encode(JSON.stringify(payload)).byteLength <= CONVERSION_PENDING_SEGMENT_PAYLOAD_MAX_BYTES;
  } catch {
    return false;
  }
}
__name(validPendingSegmentTrackPayload, "validPendingSegmentTrackPayload");
async function forwardToSegment(writeKey, body, anonId, superProps, attribution = {}, options = {}) {
  const type = (body.type || "track").toLowerCase();
  if (type === "track" && options.prebuiltPayload) {
    const payload = options.prebuiltPayload;
    if (!validPendingSegmentTrackPayload(payload, options.messageId || null)) {
      throw new Error("invalid_prebuilt_segment_track_payload");
    }
    await segmentPost(writeKey, "track", payload, options);
    return;
  }
  const eventTimestamp = validatedProducerEventTimestamp(body) || nowUTC();
  const mergedContext = {
    ...body.context || {},
    campaign: { ...(body.context || {}).campaign || {}, ...buildCampaignContext(attribution) }
  };
  if (type === "identify") {
    const traits = await hashEmail(body.traits || body.properties || {});
    await segmentPost(writeKey, "identify", {
      anonymousId: anonId,
      userId: resolveUserIdFromBody(body),
      traits,
      context: mergedContext,
      timestamp: eventTimestamp
    });
    return;
  }
  if (type === "page") {
    await segmentPost(writeKey, "page", {
      anonymousId: anonId,
      userId: resolveUserIdFromBody(body),
      name: body.name || body.properties?.name || "",
      properties: await hashEmail({ ...body.properties || {}, ...superProps }),
      context: mergedContext,
      timestamp: eventTimestamp,
      ...body.messageId ? { messageId: body.messageId } : {}
    });
    return;
  }
  if (type === "screen") {
    if (options.preserveEventName) {
      await segmentPost(writeKey, "screen", {
        anonymousId: anonId,
        userId: resolveUserIdFromBody(body),
        name: body.name || body.properties?.name || "",
        properties: await hashEmail({ ...body.properties || {}, ...superProps }),
        context: mergedContext,
        timestamp: eventTimestamp,
        ...body.messageId ? { messageId: body.messageId } : {}
      });
      return;
    }
    await segmentPost(writeKey, "track", {
      anonymousId: anonId,
      userId: resolveUserIdFromBody(body),
      event: `Viewed ${body.name || body.properties?.name || "Unknown Screen"}`,
      properties: await hashEmail({ ...body.properties || {}, ...superProps }),
      context: mergedContext,
      timestamp: eventTimestamp
    });
    return;
  }
  const resolvedEventName = String(resolveEventName(body) || "").trim();
  const eventName = (options.preserveEventName ? resolvedEventName : canonicalizeEventName(resolvedEventName)) || null;
  const orderId = resolveOrderIdDetails(body).value;
  const stableMessageId = options.messageId || body.messageId || (CONVERSION_EVENTS.has(eventName) && orderId ? `eden_${eventName}_${orderId}` : void 0);
  const payload = await buildSegmentTrackPayload(body, anonId, superProps, attribution, stableMessageId, options);
  if (payload) await segmentPost(writeKey, "track", payload, options);
}
__name(forwardToSegment, "forwardToSegment");
function attributionAuthorityOwner(key) {
  const candidates = [
    [KV_ANON_PREFIX, "anonymous_id_sha256"],
    [KV_TRUSTED_SERVER_USER_PREFIX, "user_id_sha256"],
    [KV_TRUSTED_SERVER_ORDER_PREFIX, "order_id_sha256"]
  ];
  for (const [prefix, ownerScope] of candidates) {
    if (!String(key || "").startsWith(prefix)) continue;
    const ownerValue = boundedStableIdentifier(String(key).slice(prefix.length), 512);
    if (ownerValue) return { ownerScope, ownerValue };
  }
  return null;
}
__name(attributionAuthorityOwner, "attributionAuthorityOwner");
async function storeAttribution(env, key, attribution) {
  const kv = env?.GCLID_KV;
  if (!kv || !key || !attribution) return;
  const owner = attributionAuthorityOwner(key);
  const namespace = env?.[CONVERSION_COORDINATOR_BINDING];
  if (!owner || !namespace || typeof namespace.idFromName !== "function" || typeof namespace.get !== "function") {
    throw new Error("attribution_first_touch_authority_missing");
  }
  const record = {};
  const allowedFields = /* @__PURE__ */ new Set([
    ...ATTR_COOKIE_FIELDS,
    ...ATTRIBUTION_TRAIT_KEYS,
    "landing_page",
    "attribution_referrer"
  ]);
  for (const [field, value] of Object.entries(attribution)) {
    if (!allowedFields.has(field)) continue;
    const bounded = boundedStableIdentifier(value, 2048);
    if (bounded) record[field] = bounded;
  }
  if (!Object.keys(record).length) return;
  const observedAtMs = Number(attribution?._click_first_observed_at);
  const capturedAt = Number.isFinite(observedAtMs) && observedAtMs > 0
    ? new Date(observedAtMs).toISOString()
    : Number.isFinite(Date.parse(String(attribution?.stored_at || "")))
      ? new Date(attribution.stored_at).toISOString()
      : nowUTC();
  const ownerHash = await sha256Raw(owner.ownerValue);
  const observationIdSha256 = await sha256Raw(
    `eden_attribution_observation_v1\0${owner.ownerScope}\0${ownerHash}\0${capturedAt}`
  );
  const stub = namespace.get(namespace.idFromName(`eden_attribution_first_touch_v1:${owner.ownerScope}:${ownerHash}`));
  if (!stub || typeof stub.fetch !== "function") throw new Error("attribution_first_touch_authority_stub_missing");
  const response = await stub.fetch("https://conversion-coordinator.internal/attribution/first-touch", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      owner_scope: owner.ownerScope,
      owner_hash: ownerHash,
      record: {
        ...record,
        captured_at: capturedAt,
        observation_id_sha256: observationIdSha256
      }
    })
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok || result?.ok !== true || !result?.record) {
    throw new Error(`attribution_first_touch_authority_${response.status}`);
  }
  const authoritativeAttribution = {};
  for (const field of allowedFields) {
    const bounded = boundedStableIdentifier(result.record[field], 2048);
    if (bounded) authoritativeAttribution[field] = bounded;
  }
  const authoritativeCapturedAt = Number.isFinite(Date.parse(String(result.record.captured_at || "")))
    ? new Date(result.record.captured_at).toISOString()
    : capturedAt;
  authoritativeAttribution.stored_at = authoritativeCapturedAt;
  if (CLICK_ID_PARAMS.some((field) => authoritativeAttribution[field])) {
    authoritativeAttribution._click_first_observed_at = Date.parse(authoritativeCapturedAt);
  }
  authoritativeAttribution._last_seen_at = Date.now();
  await kv.put(key, JSON.stringify(authoritativeAttribution), { expirationTtl: KV_TTL });
}
__name(storeAttribution, "storeAttribution");
async function getAttribution(kv, key) {
  if (!kv || !key)
    return null;
  try {
    const stored = await kv.get(key);
    return stored ? JSON.parse(stored) : null;
  } catch {
    return null;
  }
}
__name(getAttribution, "getAttribution");
// Body extraction is event-native evidence capture (the caller sent the value
// on this event), so it keeps the full Google click-id set. No raw ad ID or
// _gcl_au value is ever transformed into a KV lookup/write key; historical
// attr:gcl:* and attr:click:* records are quarantined by complete non-use.
// The executable raw-ID bridge set is intentionally empty. Keep the explicit
// constant aligned with health/governance so no retired click ID can be
// mistaken for a supported identity or recovery key.
var GOOGLE_CLICK_BRIDGE_PARAMS = [];
var GOOGLE_CLICK_ID_BODY_PARAMS = ["gclid", "gbraid", "wbraid", "dclid", "srsltid"];
function decodeGclAuMaybe(value) {
  if (!value)
    return null;
  const raw = String(value).trim();
  try {
    const b64 = raw.replace(/\./g, "=").replace(/-/g, "+").replace(/_/g, "/");
    const decoded = atob(b64);
    return /^\d+\.\d+$/.test(decoded) ? decoded : null;
  } catch {
    return null;
  }
}
__name(decodeGclAuMaybe, "decodeGclAuMaybe");
function gclAuVariants(value) {
  if (!value)
    return [];
  const variants = /* @__PURE__ */ new Set();
  const add = /* @__PURE__ */ __name((v) => {
    if (v && String(v).trim())
      variants.add(String(v).trim());
  }, "add");
  const raw = String(value).trim();
  add(raw);
  const decoded = decodeGclAuMaybe(raw);
  add(decoded);
  for (const v of Array.from(variants)) {
    const prefixed = v.match(/^1\.\d\.(\d+\.\d+)$/);
    if (prefixed)
      add(prefixed[1]);
    if (/^\d+\.\d+$/.test(v))
      add(`1.1.${v}`);
  }
  return Array.from(variants);
}
__name(gclAuVariants, "gclAuVariants");
function canonicalGclAu(value) {
  const variants = gclAuVariants(value);
  return variants.find((v) => /^\d+\.\d+$/.test(v)) || variants.find((v) => /^1\.\d\.\d+\.\d+$/.test(v)) || variants[0] || null;
}
__name(canonicalGclAu, "canonicalGclAu");
function mergeAttributionSources(sources, { includeAnon = true, includeUser = true, includeOrder = true, includeCookie = true } = {}) {
  if (!sources) return null;
  const merged = {
    ...includeCookie && sources.cookie ? stripInternalFields(sources.cookie) : {},
    // lowest
    ...includeOrder && sources.fromOrder ? sources.fromOrder : {},
    // owner-scoped order continuity
    ...includeUser && sources.fromUser ? sources.fromUser : {},
    // owner-scoped user continuity
    ...includeAnon && sources.fromAnon ? sources.fromAnon : {}
    // owner-scoped anonymous first-touch continuity — WINS ALL
  };
  return Object.keys(merged).length ? merged : null;
}
__name(mergeAttributionSources, "mergeAttributionSources");
async function resolveAttributionSources(kv, anonId, userId, orderId = null, cookieAttribution = null) {
  if (!kv) {
    const cookie = cookieAttribution ? stripInternalFields(cookieAttribution) : null;
    return { cookie, fromAnon: null, fromUser: null, fromOrder: null, merged: cookie };
  }
  const [fromAnon, fromUser, fromOrder] = await Promise.all([
    anonId ? getAttribution(kv, KV_ANON_PREFIX + anonId) : Promise.resolve(null),
    userId ? getAttribution(kv, KV_TRUSTED_SERVER_USER_PREFIX + userId) : Promise.resolve(null),
    orderId ? getAttribution(kv, KV_TRUSTED_SERVER_ORDER_PREFIX + orderId) : Promise.resolve(null)
  ]);
  const sources = {
    cookie: cookieAttribution ? stripInternalFields(cookieAttribution) : null,
    fromAnon,
    fromUser,
    fromOrder
  };
  return { ...sources, merged: mergeAttributionSources(sources) };
}
__name(resolveAttributionSources, "resolveAttributionSources");
async function resolveAttribution(kv, anonId, userId, orderId = null, cookieAttribution = null) {
  return (await resolveAttributionSources(kv, anonId, userId, orderId, cookieAttribution)).merged;
}
__name(resolveAttribution, "resolveAttribution");
async function linkUserAttribution(env, anonId, userId) {
  const kv = env?.GCLID_KV;
  const [anonAttr, existingUser] = await Promise.all([
    getAttribution(kv, KV_ANON_PREFIX + anonId),
    getAttribution(kv, KV_TRUSTED_SERVER_USER_PREFIX + userId)
  ]);
  if (!anonAttr)
    return;
  const userHasClick = existingUser && CLICK_ID_PARAMS.some((p) => existingUser[p]);
  if (userHasClick) {
    return;
  }
  await storeAttribution(env, KV_TRUSTED_SERVER_USER_PREFIX + userId, anonAttr);
}
__name(linkUserAttribution, "linkUserAttribution");
function stripInternalFields(attribution) {
  if (!attribution)
    return {};
  const out = {};
  for (const [k, v] of Object.entries(attribution)) {
    if (!KV_INTERNAL_FIELDS.has(k))
      out[k] = v;
  }
  return out;
}
__name(stripInternalFields, "stripInternalFields");
function normalizeEnrichmentMode(env) {
  const mode = String(env?.EDEN_HEALTH_TRACKING_ENRICHMENT_MODE || "off").trim().toLowerCase();
  if (mode === "true" || mode === "enabled") return "all";
  if (mode === "false" || mode === "disabled") return "off";
  return mode || "off";
}
__name(normalizeEnrichmentMode, "normalizeEnrichmentMode");
function isEnrichmentCanaryRequested(request, body = null) {
  try {
    const reqUrl = new URL(request.url);
    if (reqUrl.searchParams.get(ENRICHMENT_CANARY_PARAM) === "1") return true;
  } catch {}
  if (request.headers.get("X-Eden-Tracking-Enrichment-Canary") === "1") return true;
  const candidates = [
    body?.context?.page?.url,
    body?.properties?.page_url,
    body?.properties?.url,
    body?.properties?.landing_page
  ];
  for (const candidate of candidates) {
    try {
      if (candidate && new URL(candidate).searchParams.get(ENRICHMENT_CANARY_PARAM) === "1") return true;
    } catch {}
  }
  return false;
}
__name(isEnrichmentCanaryRequested, "isEnrichmentCanaryRequested");
function resolveEnrichmentState(env, request, body = null) {
  const mode = normalizeEnrichmentMode(env);
  const canary = mode === "canary" && isEnrichmentCanaryRequested(request, body);
  return {
    mode,
    enabled: mode === "all" || canary,
    canary
  };
}
__name(resolveEnrichmentState, "resolveEnrichmentState");
function isCanaryEnrichmentEnabled(env, request, body = null) {
  return resolveEnrichmentState(env, request, body).enabled;
}
__name(isCanaryEnrichmentEnabled, "isCanaryEnrichmentEnabled");
function readCookieMap(request) {
  const out = {};
  const raw = request?.headers?.get?.("Cookie") || "";
  for (const part of raw.split(";")) {
    const idx = part.indexOf("=");
    if (idx <= 0) continue;
    const key = part.slice(0, idx).trim();
    const value = part.slice(idx + 1).trim();
    if (!key || !value) continue;
    try { out[key] = decodeURIComponent(value); } catch { out[key] = value; }
  }
  return out;
}
__name(readCookieMap, "readCookieMap");
function extractGoogleCookieParamsFromRequest(request) {
  const out = {};
  const cookies = readCookieMap(request);
  for (const [key, value] of Object.entries(cookies)) {
    if (!value) continue;
    if (["_gcl_au", "_gcl_aw", "_gcl_dc", "_gcl_gb", "_gcl_gs", "_ga", "_gid"].includes(key) || /^_gac(_|$)/i.test(key)) {
      out[key] = value;
    }
  }
  return out;
}
__name(extractGoogleCookieParamsFromRequest, "extractGoogleCookieParamsFromRequest");
function normalizeGoogleAliases(target) {
  if (!target || typeof target !== "object") return target || {};
  const aliases = {
    "_gcl_au": "gcl_au",
    "gcl_au": "_gcl_au",
    "_gcl_aw": "gcl_aw",
    "_gcl_dc": "gcl_dc",
    "_gcl_gb": "gcl_gb",
    "_gcl_gs": "gcl_gs",
    "_ga": "ga",
    "_gid": "gid"
  };
  for (const [source, dest] of Object.entries(aliases)) {
    if (target[source] && !target[dest]) target[dest] = target[source];
  }
  const gac = [];
  for (const [key, value] of Object.entries(target)) {
    if (key === "gac" || key === "gac_cookie_names" || key === "gac_values")
      continue;
    if (/^_?gac(_|$)/i.test(key) && value) gac.push([key, value]);
  }
  gac.sort(([a], [b]) => a.localeCompare(b));
  if (gac.length) {
    if (!target.gac) target.gac = gac[0][1];
    target.gac_cookie_names = gac.map(([key]) => key).join(",");
    target.gac_values = gac.map(([, value]) => value).join(",");
  }
  return target;
}
__name(normalizeGoogleAliases, "normalizeGoogleAliases");
function extractEventCurrentAttribution(request, body, includeExtendedGoogle = false) {
  if (!body || typeof body !== "object")
    return {};
  const props = body.properties && typeof body.properties === "object" && !Array.isArray(body.properties) ? body.properties : body.traits && typeof body.traits === "object" && !Array.isArray(body.traits) ? body.traits : {};
  const context = body.context && typeof body.context === "object" && !Array.isArray(body.context) ? body.context : {};
  const page = context.page && typeof context.page === "object" && !Array.isArray(context.page) ? context.page : {};
  const campaign = context.campaign && typeof context.campaign === "object" && !Array.isArray(context.campaign) ? context.campaign : {};
  const pageUrlStr = firstValue(props.page_url, props.url, page.url, context.page_url);
  const urlAttribution = {};
  if (pageUrlStr) {
    try {
      const pageUrl = new URL(pageUrlStr, request?.url || "https://collect.eden.health/");
      Object.assign(urlAttribution, extractClickIds(pageUrl, request, includeExtendedGoogle));
      Object.assign(urlAttribution, extractUTMs(pageUrl));
    } catch {
    }
  } else if (includeExtendedGoogle && request) {
    Object.assign(urlAttribution, extractGoogleCookieParamsFromRequest(request));
  }
  const bodyGclAu = resolveGclAuFromBody(body);
  const bodyAttribution = {
    ...buildCampaignContext(props),
    ...buildCampaignContext(campaign),
    ...(bodyGclAu ? { _gcl_au: bodyGclAu } : {}),
    ...(resolveGoogleClickIdsFromBody(body) || {})
  };
  const out = mergeAttributionPreferFreshPrimary(bodyAttribution, urlAttribution);
  const referrer = firstValue(props.referrer, props.page_referrer, page.referrer, context.referrer);
  if (referrer && !out.attribution_referrer)
    out.attribution_referrer = sanitizeUrlString(referrer);
  const landingPage = firstValue(props.landing_page, pageUrlStr);
  if (landingPage && !out.landing_page)
    out.landing_page = sanitizeUrlString(landingPage);
  return normalizeGoogleAliases(out);
}
__name(extractEventCurrentAttribution, "extractEventCurrentAttribution");
function ensureExtendedEventContext(body, request, anonId, session, attribution, superProps, enabled, enrichmentMode = "unknown", enrichmentIsCanary = false, eventNativeAttribution = null) {
  if (!enabled || !body || typeof body !== "object") return;
  if (!body.properties || typeof body.properties !== "object" || Array.isArray(body.properties)) body.properties = {};
  if (!body.context || typeof body.context !== "object" || Array.isArray(body.context)) body.context = {};
  const props = body.properties;
  const page = body.context.page || {};
  const now = nowUTC();
  // Campaign stamping uses the event-native view when provided so stored-attribution
  // recovery cannot re-enter context.campaign here; full attribution still powers the
  // provenance-labeled first_touch_* touch model below.
  const campaignAttribution = eventNativeAttribution || attribution || {};
  const campaign = normalizeGoogleAliases({ ...buildCampaignContext(campaignAttribution), ...(body.context.campaign || {}) });
  const pageUrl = props.page_url || page.url || props.url || "";
  let parsedPage = null;
  try { if (pageUrl) parsedPage = new URL(pageUrl, request.url); } catch {}
  const cookieAnonId = readCanonicalAnonymousId(request);
  const resolvedAnonId = firstValue(anonId, cookieAnonId);
  const deviceId = firstValue(props.first_party_device_id, props.eden_anonymous_id, props.eden_anon_id, resolvedAnonId);
  const sessionId = firstValue(session?.raw, session?.id, props.eden_session_id, props.session_id, props.session_key);
  const sessionKey = firstValue(session?.id, props.session_key, sessionId);
  const pageHost = firstValue(props.page_host, page.host, parsedPage?.hostname, request.headers.get("Origin") ? new URL(request.headers.get("Origin")).hostname : null);
  const pagePath = firstValue(props.page_path, page.path, parsedPage?.pathname);
  const pageSearch = firstValue(props.page_search, parsedPage?.search);
  const referrer = firstValue(props.referrer, page.referrer, body.context.referrer);
  const landingPage = firstValue(props.landing_page, pageUrl);
  const snapshotId = firstValue(
    props.attribution_snapshot_id,
    makeAttributionSnapshotId(resolvedAnonId || "", sessionKey || "", pageUrl || "", campaign, now)
  );
  setTouchProperty(props, "anonymousId", resolvedAnonId);
  setTouchProperty(props, "eden_anon_id", resolvedAnonId);
  setTouchProperty(props, "eden_anonymous_id", resolvedAnonId);
  setTouchProperty(props, "first_party_device_id", deviceId);
  setTouchProperty(props, "collector_source", "cloudflare_eden_analytics");
  setTouchProperty(props, "source_system", "eden_health_first_party_tracking");
  setTouchProperty(props, "pipeline_version", PIPELINE_VERSION);
  setTouchProperty(props, "enrichment_version", ENRICHMENT_VERSION);
  setTouchProperty(props, "enrichment_mode", enrichmentMode);
  if (enrichmentIsCanary) setTouchProperty(props, "enrichment_canary", true);
  setTouchProperty(props, "page_url", pageUrl || landingPage);
  setTouchProperty(props, "page_path", pagePath);
  setTouchProperty(props, "page_search", pageSearch);
  setTouchProperty(props, "page_host", pageHost);
  setTouchProperty(props, "referrer", referrer);
  setTouchProperty(props, "landing_page", landingPage);
  setTouchProperty(props, "attribution_snapshot_id", snapshotId);
  setTouchProperty(body.context, "anonymousId", resolvedAnonId);
  setTouchProperty(body.context, "eden_anon_id", resolvedAnonId);
  setTouchProperty(body.context, "eden_anonymous_id", resolvedAnonId);
  setTouchProperty(body.context, "eden_session_id", sessionId);
  setTouchProperty(body.context, "eden_session_key", sessionKey);
  setTouchProperty(body.context, "first_party_device_id", deviceId);
  setTouchProperty(body.context, "attribution_snapshot_id", snapshotId);
  setTouchProperty(body.context, "collector_source", "cloudflare_eden_analytics");
  setTouchProperty(body.context, "source_system", "eden_health_first_party_tracking");
  setTouchProperty(body.context, "enrichment_mode", enrichmentMode);
  if (enrichmentIsCanary) setTouchProperty(body.context, "enrichment_canary", true);
  setTouchProperty(campaign, "first_party_device_id", deviceId);
  setTouchProperty(campaign, "eden_session_id", sessionId);
  setTouchProperty(campaign, "eden_session_key", sessionKey);
  setTouchProperty(campaign, "eden_anonymous_id", resolvedAnonId);
  setTouchProperty(campaign, "attribution_snapshot_id", snapshotId);
  setTouchProperty(campaign, "page_url", pageUrl || landingPage);
  setTouchProperty(campaign, "page_path", pagePath);
  setTouchProperty(campaign, "page_search", pageSearch);
  setTouchProperty(campaign, "page_host", pageHost);
  setTouchProperty(campaign, "referrer", referrer);
  setTouchProperty(campaign, "landing_page", landingPage);
  setTouchProperty(campaign, "collector_source", "cloudflare_eden_analytics");
  setTouchProperty(campaign, "source_system", "eden_health_first_party_tracking");
  for (const [k, v] of Object.entries(campaign)) setTouchProperty(props, k, v);
  enrichPropertiesWithAttribution(props, campaign, attribution || campaign);
  enrichPropertiesWithTouchModel(props, attribution || campaign, campaign, attribution ? "stored_attribution" : "current_event_fallback");
  body.context.campaign = { ...(body.context.campaign || {}), ...campaign };
  body.context.page = {
    ...page,
    host: props.page_host || page.host,
    url: props.page_url || page.url,
    path: props.page_path || page.path,
    search: props.page_search || page.search,
    referrer: props.referrer || page.referrer,
  };
  body.context.traits = { ...(body.context.traits || {}) };
  if (props.email_sha256) body.context.traits.email_sha256 = props.email_sha256;
  if (props.phone_sha256) body.context.traits.phone_sha256 = props.phone_sha256;
  if (props.first_name_sha256) body.context.traits.first_name_sha256 = props.first_name_sha256;
  if (props.last_name_sha256) body.context.traits.last_name_sha256 = props.last_name_sha256;
  if (props.postal_code) body.context.traits.postal_code = props.postal_code;
  if (props.country) body.context.traits.country = props.country;
  superProps.enrichment_active = true;
  superProps.enrichment_mode = enrichmentMode;
  if (enrichmentIsCanary) superProps.enrichment_canary = true;
}
__name(ensureExtendedEventContext, "ensureExtendedEventContext");
function makeAttributionSnapshotId(anonId, sessionId, pageUrl, campaign, capturedAt) {
  const seed = JSON.stringify({ anonId, sessionId, pageUrl, campaign: campaign || {}, capturedAt });
  let h1 = 2166136261;
  let h2 = 2166136261;
  let h3 = 2166136261;
  let h4 = 2166136261;
  for (let i = 0; i < seed.length; i++) {
    const left = seed.charCodeAt(i);
    const right = seed.charCodeAt(seed.length - i - 1);
    h1 ^= left;
    h1 = Math.imul(h1, 16777619);
    h2 ^= right;
    h2 = Math.imul(h2, 16777619);
    h3 ^= left + i;
    h3 = Math.imul(h3, 16777619);
    h4 ^= right + seed.length - i;
    h4 = Math.imul(h4, 16777619);
  }
  return `attr_${(h1 >>> 0).toString(16).padStart(8, "0")}${(h2 >>> 0).toString(16).padStart(8, "0")}${(h3 >>> 0).toString(16).padStart(8, "0")}${(h4 >>> 0).toString(16).padStart(8, "0")}`;
}
__name(makeAttributionSnapshotId, "makeAttributionSnapshotId");
function extractClickIds(url, request = null, includeExtendedGoogle = false) {
  const out = {};
  const evidence = extractCanonicalUrlParamEvidence(url, CLICK_ID_PARAMS, { includeNested: true, includeHash: true });
  const params = evidence.values;
  const rejected = Object.keys(evidence.conflicts).map((field) => ({ field, reason: "conflicting_repeats" }));
  for (const { param } of CLICK_ID_CONFIG) {
    if (!includeExtendedGoogle && CANARY_ONLY_GOOGLE_PARAMS.has(param)) continue;
    let v = null;
    if (UPLOAD_GRADE_GOOGLE_CLICK_ID_PARAMS.has(param) && hasAdClickEvidenceValue(params[param])) {
      const validation = validateUploadGradeGoogleClickId(params[param]);
      v = validation.value;
      if (!v) rejected.push({ field: param, reason: validation.reason });
    } else {
      v = evidenceValue(params[param], param);
    }
    if (v) out[param] = v;
  }
  const dedupedRejected = rejected.filter((entry, index, values) =>
    values.findIndex((candidate) => candidate.field === entry.field && candidate.reason === entry.reason) === index
  );
  if (dedupedRejected.length) {
    console.warn(JSON.stringify({ worker: "eden-analytics", event: "google_click_evidence_rejected", source_type: "landing_url", rejected: dedupedRejected }));
  }
  Object.assign(out, extractGoogleAdParams(url, includeExtendedGoogle));
  if (!out.gclid && !out._gcl_au) {
    const gl = getCanonicalUrlParam(url, "_gl", { includeNested: true, includeHash: true });
    if (gl)
      Object.assign(out, extractGlLinker(gl));
  }
  if (includeExtendedGoogle) {
    Object.assign(out, extractGoogleCookieParamsFromRequest(request));
    normalizeGoogleAliases(out);
  }
  if (out._gcl_au)
    out._gcl_au = canonicalGclAu(out._gcl_au) || out._gcl_au;
  return out;
}
__name(extractClickIds, "extractClickIds");
function extractGoogleAdParams(url, includeExtendedGoogle = false) {
  const out = {};
  const params = extractCanonicalUrlParams(url, GOOGLE_AD_PARAM_FIELDS, { includeNested: true, includeHash: true });
  for (const k of GOOGLE_AD_PARAM_FIELDS) {
    if (!includeExtendedGoogle && CANARY_ONLY_GOOGLE_PARAMS.has(k)) continue;
    const v = evidenceValue(params[k], k);
    if (v) out[k] = v;
  }
  return out;
}
__name(extractGoogleAdParams, "extractGoogleAdParams");
function extractGlLinker(gl) {
  const out = {};
  if (!gl)
    return out;
  try {
    const parts = gl.split("*");
    for (let i = 2; i < parts.length - 1; i += 2) {
      const key = parts[i], value = parts[i + 1];
      if (key === "_gcl_au" && value) {
        out._gcl_au = canonicalGclAu(value) || value;
        try {
          const b64 = value.replace(/\./g, "=").replace(/-/g, "+").replace(/_/g, "/");
          const segs = atob(b64).split(".");
          if (segs.length >= 3)
            out._gcl_hash = segs[2];
        } catch {
        }
        if (!out.utm_source)
          out.utm_source = "google";
        if (!out.utm_medium)
          out.utm_medium = "cpc";
      }
    }
  } catch {
  }
  return out;
}
__name(extractGlLinker, "extractGlLinker");
function extractUTMs(url) {
  const out = {};
  const params = extractCanonicalUrlParams(url, ["utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term", "utm_id"], { includeNested: true, includeHash: true });
  for (const k of ["utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term", "utm_id"]) {
    const v = params[k];
    if (v)
      out[k] = v;
  }
  return Object.keys(out).length ? out : null;
}
__name(extractUTMs, "extractUTMs");
function buildCampaignContext(attribution) {
  if (!attribution)
    return {};
  const campaign = {};
  const KEYS = [
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_content",
    "utm_term",
    "utm_id",
    "landing_page",
    "attribution_referrer",
    ...CLICK_ID_PARAMS,
    ...GOOGLE_AD_PARAM_FIELDS,
    ...PARTNER_PARAM_FIELDS
  ];
  for (const k of KEYS) {
    if (attribution[k])
      campaign[k] = attribution[k];
  }
  return campaign;
}
__name(buildCampaignContext, "buildCampaignContext");
function applyAttributionProvenance(body, mergedAttribution, eventNativeAttribution, recoveredSource = "gclid_kv_stored_attribution") {
  // context.campaign must stay event-native: values recovered from stored attribution
  // (KV anon/user/order continuity, _gcl_au bridge, gclid bridge, eden_attr cookie)
  // are surfaced separately with explicit provenance instead of being stamped into the
  // event as if freshly observed.
  if (!body || typeof body !== "object") return null;
  const mergedCampaign = buildCampaignContext(mergedAttribution || {});
  const nativeCampaign = buildCampaignContext(eventNativeAttribution || {});
  const recovered = {};
  for (const [key, value] of Object.entries(mergedCampaign)) {
    if (!nativeCampaign[key]) recovered[key] = value;
  }
  if (!Object.keys(recovered).length) return null;
  if (!body.context || typeof body.context !== "object" || Array.isArray(body.context)) body.context = {};
  body.context.recovered_campaign = { ...(body.context.recovered_campaign || {}), ...recovered };
  body.context.attribution_provenance = {
    provenance_version: "attribution_provenance_v1",
    event_native_keys: Object.keys(nativeCampaign),
    recovered_keys: Object.keys(recovered),
    recovered_source: recoveredSource
  };
  if (body.properties && typeof body.properties === "object" && !Array.isArray(body.properties)) {
    body.properties.attribution_recovered_keys = Object.keys(recovered);
    body.properties.attribution_recovery_source = recoveredSource;
  }
  return recovered;
}
__name(applyAttributionProvenance, "applyAttributionProvenance");
function hasAnyClickId(attribution) {
  return !!(attribution && CLICK_ID_PARAMS.some((p) => attribution[p]));
}
__name(hasAnyClickId, "hasAnyClickId");
function cleanAcquisitionChannel(value) {
  const channel = String(value || "").trim().toLowerCase();
  if (!channel)
    return null;
  if (INVALID_ACQUISITION_CHANNELS.has(channel))
    return null;
  if (!VALID_ACQUISITION_CHANNELS.has(channel))
    return null;
  return channel;
}
__name(cleanAcquisitionChannel, "cleanAcquisitionChannel");
function enrichPropertiesWithAttribution(properties, campaignProps, derivationAttribution = null) {
  // campaignProps should be event-native campaign keys; derivationAttribution may add
  // stored-attribution continuity for derived roll-up labels (channel/source/confidence)
  // without stamping recovered raw click IDs into the event as if freshly observed.
  if (!properties || typeof properties !== "object")
    return;
  const hasCampaignProps = !!(campaignProps && Object.keys(campaignProps).length);
  if (hasCampaignProps) {
    for (const [k, v] of Object.entries(campaignProps)) {
      if (KV_INTERNAL_FIELDS.has(k))
        continue;
      if (v && !properties[k])
        properties[k] = v;
    }
    if (campaignProps._gcl_au && !properties.gcl_au)
      properties.gcl_au = campaignProps._gcl_au;
  }
  const derivationProps = derivationAttribution && Object.keys(buildCampaignContext(derivationAttribution)).length ? buildCampaignContext(derivationAttribution) : campaignProps;
  const hasDerivationProps = !!(derivationProps && Object.keys(derivationProps).length);
  const derivedChannel = hasDerivationProps ? deriveAcquisitionChannel(derivationProps) : null;
  const existingCleanChannel = cleanAcquisitionChannel(properties.acquisition_channel);
  properties.acquisition_channel = derivedChannel && derivedChannel !== "unknown" ? derivedChannel : existingCleanChannel || "direct";
  if (!hasDerivationProps)
    return;
  properties.attribution_source = properties.attribution_source || derivationProps.utm_source || deriveClickIdSource(derivationProps);
  properties.attribution_medium = properties.attribution_medium || derivationProps.utm_medium;
  properties.attribution_campaign = properties.attribution_campaign || derivationProps.utm_campaign;
  properties.attribution_confidence = properties.attribution_confidence || deriveAttributionConfidence(derivationProps);
  const missingGclidReason = deriveMissingGclidReason(derivationProps);
  if (missingGclidReason && !properties.missing_gclid_reason) {
    properties.missing_gclid_reason = missingGclidReason;
  }
}
__name(enrichPropertiesWithAttribution, "enrichPropertiesWithAttribution");
function buildTouchSnapshot(attribution) {
  if (!attribution || !Object.keys(attribution).length)
    return null;
  const campaignProps = buildCampaignContext(attribution);
  if (!Object.keys(campaignProps).length)
    return null;
  return {
    source: campaignProps.utm_source || deriveClickIdSource(campaignProps),
    medium: campaignProps.utm_medium,
    campaign: campaignProps.utm_campaign,
    content: campaignProps.utm_content,
    term: campaignProps.utm_term,
    channel: deriveAcquisitionChannel(campaignProps),
    gclid: campaignProps.gclid,
    gbraid: campaignProps.gbraid,
    wbraid: campaignProps.wbraid,
    dclid: campaignProps.dclid,
    gcl_au: campaignProps._gcl_au,
    msclkid: campaignProps.msclkid,
    fbclid: campaignProps.fbclid,
    landing_page: campaignProps.landing_page,
    referrer: campaignProps.attribution_referrer,
    confidence: deriveAttributionConfidence(campaignProps),
    captured_at: nowUTC()
  };
}
__name(buildTouchSnapshot, "buildTouchSnapshot");
function setTouchProperty(properties, key, value) {
  if (value === void 0 || value === null || value === "")
    return;
  if (properties[key] === void 0 || properties[key] === null || properties[key] === "") {
    properties[key] = value;
  }
}
__name(setTouchProperty, "setTouchProperty");
function writeTouchSnapshot(properties, prefix, touch, capturedAt) {
  if (!touch)
    return;
  setTouchProperty(properties, `${prefix}_source`, touch.source);
  setTouchProperty(properties, `${prefix}_medium`, touch.medium);
  setTouchProperty(properties, `${prefix}_campaign`, touch.campaign);
  setTouchProperty(properties, `${prefix}_content`, touch.content);
  setTouchProperty(properties, `${prefix}_term`, touch.term);
  setTouchProperty(properties, `${prefix}_channel`, touch.channel);
  setTouchProperty(properties, `${prefix}_gclid`, touch.gclid);
  setTouchProperty(properties, `${prefix}_gbraid`, touch.gbraid);
  setTouchProperty(properties, `${prefix}_wbraid`, touch.wbraid);
  setTouchProperty(properties, `${prefix}_dclid`, touch.dclid);
  setTouchProperty(properties, `${prefix}_gcl_au`, touch.gcl_au);
  setTouchProperty(properties, `${prefix}_msclkid`, touch.msclkid);
  setTouchProperty(properties, `${prefix}_fbclid`, touch.fbclid);
  setTouchProperty(properties, `${prefix}_landing_page`, touch.landing_page);
  setTouchProperty(properties, `${prefix}_referrer`, touch.referrer);
  setTouchProperty(properties, `${prefix}_confidence`, touch.confidence);
  setTouchProperty(properties, `${prefix}_at`, capturedAt || touch.captured_at);
}
__name(writeTouchSnapshot, "writeTouchSnapshot");
function enrichPropertiesWithTouchModel(properties, firstAttribution, currentAttribution, firstTouchSource = "stored_attribution") {
  if (!properties || typeof properties !== "object")
    return;
  const first = buildTouchSnapshot(firstAttribution);
  const current = buildTouchSnapshot(currentAttribution);
  if (first) {
    writeTouchSnapshot(properties, "first_touch", first, firstAttribution?.stored_at || first.captured_at);
    setTouchProperty(properties, "first_touch_source_type", firstTouchSource);
    setTouchProperty(properties, "first_touch_from_memory", firstTouchSource === "stored_attribution");
  }
  if (current) {
    writeTouchSnapshot(properties, "current_touch", current, current.captured_at);
    writeTouchSnapshot(properties, "last_touch", current, current.captured_at);
    setTouchProperty(properties, "last_touch_source_type", "current_event");
  }
  if (first || current) {
    setTouchProperty(properties, "attribution_model", "first_touch_primary_last_touch_snapshot");
    setTouchProperty(properties, "touch_model_version", ENRICHMENT_VERSION);
  }
}
__name(enrichPropertiesWithTouchModel, "enrichPropertiesWithTouchModel");
function firstValue(...values) {
  for (const value of values) {
    if (value !== void 0 && value !== null && String(value).trim() !== "")
      return value;
  }
  return null;
}
__name(firstValue, "firstValue");
function boundedStableIdentifier(value, maxBytes = 256) {
  if (value === void 0 || value === null) return null;
  const valueType = typeof value;
  if (!["string", "number", "bigint"].includes(valueType)) return null;
  // JSON numbers outside JavaScript's safe-integer range can round distinct
  // source IDs onto the same value before this Worker sees them. Require
  // numeric identifiers to be exact integers; producers with larger IDs must
  // preserve them as strings.
  if (valueType === "number" && !Number.isSafeInteger(value)) return null;
  const normalized = String(value).trim();
  if (!normalized || /[\u0000-\u001f\u007f]/.test(normalized)) return null;
  if (["null", "undefined", "nan", "infinity", "[object object]"].includes(normalized.toLowerCase())) return null;
  if (new TextEncoder().encode(normalized).byteLength > maxBytes) return null;
  return normalized;
}
__name(boundedStableIdentifier, "boundedStableIdentifier");
function firstBoundedStableIdentifier(...values) {
  for (const value of values) {
    const normalized = boundedStableIdentifier(value);
    if (normalized) return normalized;
  }
  return null;
}
__name(firstBoundedStableIdentifier, "firstBoundedStableIdentifier");
function parseSessionValue(value) {
  const raw = boundedStableIdentifier(value, 512);
  if (!raw)
    return null;
  const str = String(raw).trim();
  const parts = str.split("_");
  const maybeStartedMs = Number(parts[parts.length - 1]);
  const hasStartedMs = Number.isFinite(maybeStartedMs) && maybeStartedMs > 1e12;
  const stableId = hasStartedMs ? parts.slice(0, -1).join("_") : str;
  const startedAt = hasStartedMs ? new Date(maybeStartedMs).toISOString() : null;
  const ageSeconds = hasStartedMs ? Math.max(0, Math.floor((Date.now() - maybeStartedMs) / 1e3)) : null;
  return {
    raw,
    id: stableId || str,
    started_at: startedAt,
    age_seconds: ageSeconds
  };
}
__name(parseSessionValue, "parseSessionValue");
function buildSessionContext(rawSession, source, sourceType, body) {
  const parsed = parseSessionValue(rawSession);
  if (!parsed)
    return null;
  const props = body?.properties || {};
  const page = body?.context?.page || {};
  return {
    ...parsed,
    source,
    source_type: sourceType,
    timeout_minutes: 30,
    page_path: page.path || props.page_path || null,
    page_url: sanitizeUrlString(page.url || props.page_url || "") || null
  };
}
__name(buildSessionContext, "buildSessionContext");
function resolveSessionFromRequestBody(request, body, sourceType) {
  const props = body?.properties || {};
  const context = body?.context || {};
  const traits = context.traits || {};
  const cookieSession = readCookie(request, "eden_session_id");
  const suppliedSession = firstBoundedStableIdentifier(
    cookieSession,
    body?.session_id,
    body?.sessionId,
    body?.session_id_raw,
    props.session_id,
    props.sessionId,
    props.eden_session_id,
    props.session_id_raw,
    context.session_id,
    context.sessionId,
    traits.session_id,
    traits.sessionId
  );
  const source = cookieSession ? "eden_session_cookie" : "payload";
  return buildSessionContext(suppliedSession, source, sourceType, body);
}
__name(resolveSessionFromRequestBody, "resolveSessionFromRequestBody");
function enrichPropertiesWithSession(properties, session) {
  if (!properties || typeof properties !== "object" || !session)
    return;
  setTouchProperty(properties, "session_id", session.raw || session.id);
  setTouchProperty(properties, "eden_session_id", session.raw || session.id);
  setTouchProperty(properties, "session_key", session.id);
  setTouchProperty(properties, "session_id_raw", session.raw);
  setTouchProperty(properties, "session_started_at", session.started_at);
  setTouchProperty(properties, "session_age_seconds", session.age_seconds);
  setTouchProperty(properties, "session_timeout_minutes", session.timeout_minutes);
  setTouchProperty(properties, "session_source", session.source);
  setTouchProperty(properties, "session_source_type", session.source_type);
  setTouchProperty(properties, "session_page_path", session.page_path);
  setTouchProperty(properties, "session_page_url", sanitizeUrlString(session.page_url || "") || null);
  setTouchProperty(properties, "session_model_version", ENRICHMENT_VERSION);
}
__name(enrichPropertiesWithSession, "enrichPropertiesWithSession");
function sessionSuperProps(session) {
  if (!session)
    return {};
  return {
    session_key: session.id,
    ...session.raw ? { session_id_raw: session.raw } : {},
    ...session.started_at ? { session_started_at: session.started_at } : {},
    ...session.age_seconds !== null ? { session_age_seconds: session.age_seconds } : {},
    session_timeout_minutes: session.timeout_minutes,
    session_source: session.source,
    session_model_version: ENRICHMENT_VERSION
  };
}
__name(sessionSuperProps, "sessionSuperProps");
function deriveAttributionConfidence(c) {
  if (!c || !Object.keys(c).length)
    return "low";
  if (c.gclid || c.gbraid || c.wbraid || c.dclid)
    return "high";
  const med = String(c.utm_medium || "").toLowerCase();
  if ((med === "cpc" || med === "search_cpc" || med === "paid_search" || med === "paid") && (c.utm_campaign || c._gcl_au || c.msclkid))
    return "medium";
  if (c._gcl_au || c.srsltid || c.attribution_referrer)
    return "medium";
  return "low";
}
__name(deriveAttributionConfidence, "deriveAttributionConfidence");
function deriveMissingGclidReason(c) {
  if (!c || c.gclid)
    return void 0;
  if (c.gbraid)
    return "gbraid_only";
  if (c.wbraid)
    return "wbraid_only";
  if (c.dclid)
    return "dclid_only";
  if (c._gcl_au)
    return "gcl_au_only";
  if (c.srsltid)
    return "srsltid_only";
  return void 0;
}
__name(deriveMissingGclidReason, "deriveMissingGclidReason");
function deriveClickIdSource(c) {
  if (!c)
    return void 0;
  if (c.gclid || c.gbraid || c.wbraid || c.dclid || c._gcl_au || c.srsltid)
    return "google";
  if (c.fbclid)
    return "meta";
  if (c.msclkid)
    return "microsoft";
  if (c.ttclid)
    return "tiktok";
  if (c.twclid)
    return "twitter";
  if (c.li_fat_id)
    return "linkedin";
  if (c.rdt_cid)
    return "reddit";
  if (c.epik)
    return "pinterest";
  if (c.ScCid)
    return "snapchat";
  if (c.irclickid)
    return "impact_radius";
  if (c.cjevent)
    return "cj_affiliate";
  if (c.click_id)
    return "generic";
  return void 0;
}
__name(deriveClickIdSource, "deriveClickIdSource");
function deriveAcquisitionChannel(c) {
  if (!c || !Object.keys(c).length)
    return "unknown";
  const src = String(c.utm_source || deriveClickIdSource(c) || "").toLowerCase();
  const med = String(c.utm_medium || "").toLowerCase();
  const hasPaidSearchClickId = PAID_SEARCH_CLICK_ID_PARAMS.some((p) => !!c[p]);
  if (hasPaidSearchClickId || med === "cpc" || med === "paid" || med === "paid_search" || med === "search_cpc" || med === "ppc") {
    return "paid_search";
  }
  if (med === "organic")
    return "organic_search";
  if (med === "email")
    return "email";
  if (med === "sms")
    return "sms";
  if (med === "affiliate")
    return "affiliate";
  if (med === "influencer")
    return "influencer";
  if (med === "synthetic")
    return "synthetic";
  if (c.fbclid || c.ttclid || src.includes("facebook") || src.includes("instagram") || src.includes("meta") || src.includes("tiktok"))
    return "paid_social";
  if (c.li_fat_id || src.includes("linkedin"))
    return "paid_social_linkedin";
  if (c.rdt_cid || src.includes("reddit"))
    return "paid_social_reddit";
  if (c.epik || src.includes("pinterest"))
    return "paid_social_pinterest";
  if (c.twclid || src.includes("twitter") || src.includes("x.com"))
    return "paid_social_twitter";
  if (c.irclickid || c.cjevent || med === "affiliate")
    return "affiliate";
  if (c.attribution_referrer) {
    try {
      const rh = new URL(c.attribution_referrer).hostname.toLowerCase();
      if (rh.includes("google") || rh.includes("bing"))
        return "organic_search";
      if (rh.includes("facebook") || rh.includes("instagram") || rh.includes("meta"))
        return "organic_social";
      if (rh.includes("twitter") || rh.includes("x.com"))
        return "organic_social";
      if (rh.includes("linkedin"))
        return "organic_social";
    } catch {
    }
  }
  if (c.srsltid && src === "google")
    return "organic_search";
  return src || "direct";
}
__name(deriveAcquisitionChannel, "deriveAcquisitionChannel");
function canonicalizeEventName(n) {
  if (!n)
    return "";
  const raw = String(n).trim();
  return EVENT_NAME_ALIASES[raw.toLowerCase()] || raw;
}
__name(canonicalizeEventName, "canonicalizeEventName");
function resolveEventName(body) {
  return body.event || body.event_name || body.name || body.properties?.event || body.properties?.event_name || body.properties?.name || "";
}
__name(resolveEventName, "resolveEventName");
function stableIdentifierResolution(values = []) {
  const candidates = /* @__PURE__ */ new Set();
  let invalid = false;
  for (const value of values) {
    if (value === void 0 || value === null) continue;
    if (typeof value === "string" && value.trim() === "") continue;
    const normalized = boundedStableIdentifier(value);
    if (normalized) candidates.add(normalized);
    else invalid = true;
  }
  return {
    value: !invalid && candidates.size === 1 ? [...candidates][0] : null,
    candidates: [...candidates],
    invalid,
    conflict: candidates.size > 1
  };
}
__name(stableIdentifierResolution, "stableIdentifierResolution");
function resolveOrderIdDetails(body) {
  // An order ID is an order ID. Do not relabel a treatment, master record, or
  // payment transaction as an order merely because the producer has not
  // created the order yet. Those namespaces remain useful event properties,
  // while only a real order namespace may create order continuity or an
  // order-scoped identity link.
  const values = [
    body.properties?.order_id, body.properties?.orderId,
    body.properties?.ecommerce?.order_id,
    body.properties?.healthos?.order_id, body.properties?.healthos?.orderId,
    body.context?.traits?.order_id, body.context?.traits?.orderId,
    body.order_id, body.orderId
  ];
  const populated = values.some((value) => value !== void 0 && value !== null && !(typeof value === "string" && value.trim() === ""));
  if (!populated) return { value: null, candidates: [], invalid: false, conflict: false, namespace: null };
  return { ...stableIdentifierResolution(values), namespace: "order_id" };
}
__name(resolveOrderIdDetails, "resolveOrderIdDetails");
function resolveLegacyV555ConversionReferences(body) {
  // Read-only v5.55 migration candidates. Explicit payment transaction values
  // are checked first because only they can independently prove equality with
  // the new charge-authoritative OS_purchase scope. This list is never reused
  // for new order identity or conversion keying.
  const candidates = [
    body?.properties?.transaction_id, body?.properties?.transactionId,
    body?.properties?.charge_id, body?.properties?.chargeId,
    body?.properties?.payment_id, body?.properties?.paymentId,
    body?.properties?.ecommerce?.transaction_id, body?.properties?.ecommerce?.transactionId,
    body?.properties?.healthos?.transaction_id, body?.properties?.healthos?.transactionId,
    body?.properties?.healthos?.charge_id, body?.properties?.healthos?.chargeId,
    body?.transaction_id, body?.transactionId, body?.charge_id, body?.chargeId,
    body?.properties?.order_id, body?.properties?.orderId,
    body?.properties?.master_id, body?.properties?.masterId,
    body?.properties?.treatment_id, body?.properties?.treatmentId,
    body?.properties?.ecommerce?.order_id,
    body?.properties?.ecommerce?.treatmentId,
    body?.properties?.healthos?.order_id, body?.properties?.healthos?.orderId,
    body?.properties?.healthos?.master_id, body?.properties?.healthos?.masterId,
    body?.properties?.healthos?.treatment_id, body?.properties?.healthos?.treatmentId,
    body?.context?.traits?.order_id, body?.context?.traits?.orderId,
    body?.context?.traits?.master_id, body?.context?.traits?.masterId,
    body?.order_id, body?.orderId, body?.master_id, body?.masterId
  ];
  return [...new Set(candidates.map((value) => boundedStableIdentifier(value)).filter(Boolean))];
}
__name(resolveLegacyV555ConversionReferences, "resolveLegacyV555ConversionReferences");
function legacyConversionRecordProvesCurrentScope({ record, eventName, reference, conversionKeyDetails, currentSignalState }) {
  if (eventName !== "OS_purchase") return true;
  if (!record || record.event !== eventName || !conversionKeyDetails?.rawValue) return false;
  const currentScopeSignalHash = currentSignalState?.hashes?.["identity:conversion_scope_sha256"] || null;
  const recordedScopeSignalHash = record?.signal_hashes?.["identity:conversion_scope_sha256"] || null;
  if (currentScopeSignalHash && recordedScopeSignalHash === currentScopeSignalHash) return true;
  const explicitTransaction = stableIdentifierResolution([
    record?.transaction_id, record?.transactionId,
    record?.charge_id, record?.chargeId,
    record?.payment_id, record?.paymentId,
    record?.properties?.transaction_id, record?.properties?.transactionId,
    record?.properties?.charge_id, record?.properties?.chargeId,
    record?.properties?.payment_id, record?.properties?.paymentId
  ]);
  if (explicitTransaction.value && explicitTransaction.value === conversionKeyDetails.rawValue) return true;
  return boundedStableIdentifier(reference) === conversionKeyDetails.rawValue;
}
__name(legacyConversionRecordProvesCurrentScope, "legacyConversionRecordProvesCurrentScope");
function legacyV555SegmentMessageId(eventName, record, reference) {
  // v5.55 ignored an incoming producer messageId and generated
  // `eden_<canonical_event>_<overloaded_order_reference>`. Reuse exactly that
  // historical key for an equality-proven migration repair so Segment can
  // dedupe a prior success while a failed asynchronous delivery is recovered.
  const legacyReference = boundedStableIdentifier(record?.order_id)
    || boundedStableIdentifier(reference);
  if (!legacyReference) return null;
  return boundedStableIdentifier(`eden_${eventName}_${legacyReference}`);
}
__name(legacyV555SegmentMessageId, "legacyV555SegmentMessageId");
function identifierNamespaceResolution(name, values = []) {
  const populated = values.some((value) => value !== void 0 && value !== null && !(typeof value === "string" && value.trim() === ""));
  if (!populated) return { name, populated: false, value: null, invalid: false, conflict: false };
  return { name, populated: true, ...stableIdentifierResolution(values) };
}
__name(identifierNamespaceResolution, "identifierNamespaceResolution");
function resolveConversionKeyDetails(body, eventName) {
  // Conversion idempotency is event-contract-specific so adding a richer ID
  // later can never move the same business event into a different coordinator
  // scope. HealthOS OS_purchase is charge-authoritative and currently emits
  // both transaction_id=charge.id and messageId=OS_purchase:<charge.id>.
  // Qualification/completion milestones are order-authoritative and therefore
  // always use the real order namespace. Master/treatment IDs are relationship
  // evidence, never conversion idempotency fallbacks.
  const message = identifierNamespaceResolution("message_id", [
    body?.messageId, body?.message_id,
    body?.properties?.messageId, body?.properties?.message_id
  ]);
  const transaction = identifierNamespaceResolution("transaction_id", [
    body?.properties?.transaction_id, body?.properties?.transactionId,
    body?.properties?.charge_id, body?.properties?.chargeId,
    body?.properties?.payment_id, body?.properties?.paymentId,
    body?.properties?.ecommerce?.transaction_id, body?.properties?.ecommerce?.transactionId,
    body?.properties?.healthos?.transaction_id, body?.properties?.healthos?.transactionId,
    body?.properties?.healthos?.charge_id, body?.properties?.healthos?.chargeId,
    body?.transaction_id, body?.transactionId, body?.charge_id, body?.chargeId
  ]);
  const order = identifierNamespaceResolution("order_id", [
    body?.properties?.order_id, body?.properties?.orderId,
    body?.properties?.ecommerce?.order_id,
    body?.properties?.healthos?.order_id, body?.properties?.healthos?.orderId,
    body?.context?.traits?.order_id, body?.context?.traits?.orderId,
    body?.order_id, body?.orderId
  ]);
  // OS_purchase is transaction-authoritative. An ordinary producer/Segment
  // messageId is transport metadata when a valid charge/transaction ID exists;
  // it cannot veto the business key. Order-authoritative milestones continue to
  // require the order namespace.
  const requiredNamespaces = eventName === "OS_purchase"
    ? transaction.populated ? [transaction] : [message]
    : [order];
  const malformed = requiredNamespaces.find((namespace) => namespace.populated && (namespace.invalid || namespace.conflict || !namespace.value));
  if (malformed) {
    return {
      value: null,
      source: malformed.name,
      invalid: malformed.invalid,
      conflict: malformed.conflict || !malformed.invalid,
      segmentMessageId: null
    };
  }
  if (eventName === "OS_purchase") {
    if (order.populated && (order.invalid || order.conflict || !order.value)) {
      // The payment/charge transaction is the purchase idempotency authority.
      // Conflicting or malformed order aliases are relationship evidence only:
      // quarantine them from identity linking/enrichment, but never discard an
      // otherwise valid commercial payment event.
      console.warn(JSON.stringify({
        worker: "eden-analytics",
        event: "purchase_order_alias_quarantined",
        order_alias_invalid: !!order.invalid,
        order_alias_conflict: !!order.conflict,
        source_pipeline_version: PIPELINE_VERSION
      }));
    }
    let messageTransactionId = null;
    if (message.value) {
      const match = message.value.match(/^(?:OS_purchase|purchase):(.+)$/i);
      messageTransactionId = match ? boundedStableIdentifier(match[1]) : null;
      if (!messageTransactionId && !transaction.value) {
        return {
          value: null,
          source: "message_id",
          invalid: true,
          conflict: false,
          segmentMessageId: null
        };
      }
    }
    if (transaction.value && message.populated && (!messageTransactionId || messageTransactionId !== transaction.value)) {
      console.warn(JSON.stringify({
        worker: "eden-analytics",
        event: "purchase_message_id_ignored_in_favor_of_transaction_id",
        message_id_canonical: !!messageTransactionId,
        source_pipeline_version: PIPELINE_VERSION
      }));
    }
    const transactionId = transaction.value || messageTransactionId;
    if (!transactionId) {
      return {
        value: null,
        source: "transaction_id",
        invalid: false,
        conflict: false,
        segmentMessageId: null
      };
    }
    const canonicalSegmentMessageId = `OS_purchase:${transactionId}`;
    if (new TextEncoder().encode(canonicalSegmentMessageId).byteLength > 256) {
      return {
        value: null,
        source: "transaction_id",
        invalid: true,
        conflict: false,
        segmentMessageId: null
      };
    }
    return {
      value: `eden_conversion_key_v2:transaction_id:${transactionId}`,
      source: transaction.value ? "transaction_id" : "message_id",
      rawValue: transactionId,
      invalid: false,
      conflict: false,
      // Normalize the syntactic purchase alias onto the one producer-compatible
      // Segment idempotency key used before and after this release.
      segmentMessageId: canonicalSegmentMessageId
    };
  }
  if (!order.value) {
    return {
      value: null,
      source: "order_id",
      invalid: false,
      conflict: false,
      segmentMessageId: null
    };
  }
  return {
    value: `eden_conversion_key_v2:order_id:${order.value}`,
    source: "order_id",
    rawValue: order.value,
    invalid: false,
    conflict: false,
    segmentMessageId: null,
    eventName
  };
}
__name(resolveConversionKeyDetails, "resolveConversionKeyDetails");
function resolveOrderId(body) {
  return resolveOrderIdDetails(body).value;
}
__name(resolveOrderId, "resolveOrderId");
function edenIdentityResolutionFromBody(body) {
  if (!body || typeof body !== "object") return { value: null, candidates: [], invalid: false, conflict: false };
  // Authenticated producers have used several envelopes over time. Detect the
  // canonical key anywhere in the bounded request rather than maintaining a
  // brittle list that can silently miss body.traits, ecommerce camelCase, or a
  // future nested producer wrapper.
  const values = /* @__PURE__ */ new Set();
  let invalid = false;
  const pending = [body];
  const seen = /* @__PURE__ */ new Set();
  while (pending.length) {
    const current = pending.pop();
    if (!current || typeof current !== "object" || seen.has(current)) continue;
    seen.add(current);
    if (Array.isArray(current)) {
      for (const entry of current) if (entry && typeof entry === "object") pending.push(entry);
      continue;
    }
    for (const [rawKey, entry] of Object.entries(current)) {
      if (normalizeBrowserFieldKey(rawKey) === "eden_identity_id") {
        if (entry === void 0 || entry === null) continue;
        if (typeof entry === "string" && entry.trim() === "") continue;
        const normalized = boundedStableIdentifier(entry);
        if (normalized) values.add(normalized);
        else invalid = true;
      } else if (entry && typeof entry === "object") {
        pending.push(entry);
      }
    }
  }
  return {
    value: !invalid && values.size === 1 ? [...values][0] : null,
    candidates: [...values],
    invalid,
    conflict: values.size > 1
  };
}
__name(edenIdentityResolutionFromBody, "edenIdentityResolutionFromBody");
function edenIdentityIdCandidatesFromBody(body) {
  return edenIdentityResolutionFromBody(body).candidates;
}
__name(edenIdentityIdCandidatesFromBody, "edenIdentityIdCandidatesFromBody");
function resolveEdenIdentityIdFromBody(body) {
  return edenIdentityResolutionFromBody(body).value;
}
__name(resolveEdenIdentityIdFromBody, "resolveEdenIdentityIdFromBody");
function resolveSourceUserIdDetails(body) {
  const canonicalSourceIdentity = (namespace, value) => {
    if (typeof value !== "string") return value;
    const trimmed = value.trim();
    const match = trimmed.match(/^source:(user_id|patient_id|customer_id|member_id):(.+)$/);
    if (!match) return value;
    return match[1] === namespace ? match[2] : null;
  };
  const namespaces = [
    {
      name: "user_id",
      values: [
        body.userId, body.user_id,
        body.properties?.userId, body.properties?.user_id,
        body.properties?.ecommerce?.userId,
        body.properties?.healthos?.userId, body.properties?.healthos?.user_id,
        body.context?.traits?.userId, body.context?.traits?.user_id
      ]
    },
    {
      name: "patient_id",
      values: [
        body.properties?.patient_id,
        body.properties?.healthos?.patient_id, body.properties?.healthos?.patientId,
        body.context?.traits?.patient_id, body.context?.traits?.patientId,
        body.patient_id, body.patientId
      ]
    },
    {
      name: "customer_id",
      values: [
        body.properties?.customer_id,
        body.properties?.healthos?.customer_id,
        body.context?.traits?.customer_id,
        body.customer_id
      ]
    },
    {
      name: "member_id",
      values: [
        body.properties?.member_id,
        body.properties?.healthos?.member_id,
        body.context?.traits?.member_id,
        body.member_id
      ]
    }
  ].map((namespace) => ({
    ...namespace,
    resolution: stableIdentifierResolution(namespace.values.map((value) => canonicalSourceIdentity(namespace.name, value)))
  }));
  const invalidNamespaces = namespaces.filter((namespace) => namespace.resolution.invalid);
  const conflictingNamespaces = namespaces.filter((namespace) => namespace.resolution.conflict);
  const selected = namespaces.find((namespace) => namespace.resolution.value) || null;
  const invalid = invalidNamespaces.length > 0;
  const conflict = conflictingNamespaces.length > 0;
  const rawValue = !invalid && !conflict ? selected?.resolution?.value || null : null;
  const value = rawValue
    ? boundedStableIdentifier(`source:${selected.name}:${rawValue}`, 512)
    : null;
  return {
    value,
    rawValue,
    namespace: value ? selected.name : null,
    candidates: namespaces.flatMap((namespace) =>
      namespace.resolution.candidates.map((candidate) => `source:${namespace.name}:${candidate}`)
    ),
    invalid,
    conflict,
    invalidNamespaces: invalidNamespaces.map((namespace) => namespace.name),
    conflictingNamespaces: conflictingNamespaces.map((namespace) => namespace.name)
  };
}
__name(resolveSourceUserIdDetails, "resolveSourceUserIdDetails");
function resolveSourceUserIdFromBody(body) {
  return resolveSourceUserIdDetails(body).value;
}
__name(resolveSourceUserIdFromBody, "resolveSourceUserIdFromBody");
function resolveUserIdFromBody(body) {
  const eden = edenIdentityResolutionFromBody(body);
  const source = resolveSourceUserIdDetails(body);
  // The warehouse-owned Eden identity is canonical. Source IDs are a fallback
  // only when no Eden claim is present; malformed/conflicting fallback aliases
  // cannot invalidate one unambiguous canonical Eden identity.
  if (eden.value) return eden.value;
  if (eden.invalid || eden.conflict || eden.candidates.length > 0 || source.invalid || source.conflict) return null;
  return source.value;
}
__name(resolveUserIdFromBody, "resolveUserIdFromBody");
function resolveEmailFromBody(body) {
  return body.properties?.email || body.properties?.customerEmail || body.properties?.ecommerce?.email || body.traits?.email || body.context?.traits?.email || null;
}
__name(resolveEmailFromBody, "resolveEmailFromBody");
function resolveGclAuFromBody(body) {
  const value = body.properties?._gcl_au || body.properties?.gcl_au || body.properties?.gclAu || body.properties?.ecommerce?._gcl_au || body.properties?.ecommerce?.gcl_au || body.context?.campaign?._gcl_au || body.context?.campaign?.gcl_au || body.context?.traits?._gcl_au || body.context?.traits?.gcl_au || body._gcl_au || body.gcl_au || null;
  return canonicalGclAu(value) || value;
}
__name(resolveGclAuFromBody, "resolveGclAuFromBody");
function resolveGoogleClickIdsFromBody(body, sourceType = "event_body") {
  const out = {};
  const valuesByParam = /* @__PURE__ */ new Map();
  const pending = [{ value: body, depth: 0 }];
  const seen = /* @__PURE__ */ new Set();
  while (pending.length) {
    const current = pending.pop();
    if (!current?.value || typeof current.value !== "object" || seen.has(current.value) || current.depth > 8) continue;
    seen.add(current.value);
    if (Array.isArray(current.value)) {
      for (const entry of current.value) if (entry && typeof entry === "object") pending.push({ value: entry, depth: current.depth + 1 });
      continue;
    }
    for (const [rawKey, entry] of Object.entries(current.value)) {
      const param = canonicalQueryParamName(rawKey);
      if (GOOGLE_CLICK_ID_BODY_PARAMS.includes(param)) {
        const value = evidenceValue(entry, param);
        if (value) {
          if (!valuesByParam.has(param)) valuesByParam.set(param, /* @__PURE__ */ new Set());
          valuesByParam.get(param).add(value);
        }
      }
      if (entry && typeof entry === "object") pending.push({ value: entry, depth: current.depth + 1 });
    }
  }
  const rejected = [];
  for (const [param, values] of valuesByParam.entries()) {
    if (values.size === 1) out[param] = [...values][0];
    else if (values.size > 1) rejected.push({ field: param, reason: "conflicting_repeats" });
  }
  if (rejected.length) {
    console.warn(JSON.stringify({ worker: "eden-analytics", event: "google_click_evidence_rejected", source_type: sourceType, rejected }));
  }
  return Object.keys(out).length ? out : null;
}
__name(resolveGoogleClickIdsFromBody, "resolveGoogleClickIdsFromBody");
function resolveAnonymousIdentityDetails(request, body) {
  const cookieAnonymousId = boundedStableIdentifier(readCanonicalAnonymousId(request));
  const bodyResolution = stableIdentifierResolution([
    body.anonymousId, body.anonymous_id, body.anonymoous_id, body.anonymous_Id, body.anonymousid,
    body.properties?.anonymousId, body.properties?.anonymous_id, body.properties?.anonymoous_id,
    body.context?.traits?.anonymousId, body.context?.traits?.anonymous_id
  ]);
  const conflictsWithCookie = !!cookieAnonymousId && bodyResolution.candidates.some((candidate) => candidate !== cookieAnonymousId);
  const conflict = bodyResolution.conflict || conflictsWithCookie;
  return {
    value: cookieAnonymousId || (!bodyResolution.invalid && !conflict ? bodyResolution.value : null),
    invalid: bodyResolution.invalid,
    conflict,
    candidates: bodyResolution.candidates
  };
}
__name(resolveAnonymousIdentityDetails, "resolveAnonymousIdentityDetails");
function resolveIdentityFromBody(request, body) {
  const edenIdentity = edenIdentityResolutionFromBody(body);
  const sourceIdentity = resolveSourceUserIdDetails(body);
  const anonymousIdentity = resolveAnonymousIdentityDetails(request, body);
  const invalidEdenIdentityClaim = edenIdentity.invalid;
  const stableIdentityConflict = edenIdentity.invalid || edenIdentity.conflict || sourceIdentity.invalid || sourceIdentity.conflict;
  const edenIdentityId = edenIdentity.value || null;
  const edenIdentityClaimPresent = edenIdentity.candidates.length > 0 || edenIdentity.invalid || edenIdentity.conflict;
  const sourceUserId = !edenIdentityId
    && !edenIdentityClaimPresent
    && !sourceIdentity.invalid
    && !sourceIdentity.conflict
    ? sourceIdentity.value
    : null;
  const userId = edenIdentityId || sourceUserId;
  const anonymousId = anonymousIdentity.value;
  const identityWarning = stableIdentityConflict
    ? invalidEdenIdentityClaim ? "invalid_eden_identity_id_quarantined"
      : edenIdentity.conflict ? "conflicting_eden_identity_ids_quarantined"
      : sourceIdentity.invalid ? "invalid_source_user_id_quarantined"
      : "conflicting_source_user_ids_quarantined"
    : edenIdentityId && sourceIdentity.rawValue && edenIdentityId !== sourceIdentity.rawValue
    ? "eden_identity_id_conflicts_with_source_user_id"
    : anonymousIdentity.invalid ? "invalid_anonymous_id_quarantined"
    : anonymousIdentity.conflict ? "conflicting_anonymous_ids_quarantined"
    : anonymousId && userId && anonymousId === userId ? "anonymousId_equals_userId" : void 0;
  return {
    anonymousId,
    userId,
    stableIdentityKeyType: edenIdentityId
        ? "eden_identity_id"
        : sourceUserId
          ? `source_${sourceIdentity.namespace}`
          : stableIdentityConflict
            ? "eden_identity_id_conflict_quarantined"
            : null,
    stableIdentityConflict,
    anonymousIdentityConflict: anonymousIdentity.invalid || anonymousIdentity.conflict,
    identityWarning
  };
}
__name(resolveIdentityFromBody, "resolveIdentityFromBody");
var SERVER_STABLE_USER_IDENTITY_KEYS = /* @__PURE__ */ new Set([
  "eden_identity_id", "user_id", "patient_id", "customer_id", "member_id",
  "person_id", "identity_id", "account_id", "profile_id", "contact_id"
]);
function isServerStablePersonIdentityClaimKey(rawKey) {
  const normalized = normalizeBrowserFieldKey(rawKey);
  if (SERVER_STABLE_USER_IDENTITY_KEYS.has(normalized) || normalized === "external_id") return true;
  const businessObjectKey = /(?:^|_)(?:order|master|treatment|transaction|group)(?:_external)?_?ids?$/.test(normalized);
  return !businessObjectKey && isUntrustedBrowserIdentityKey(rawKey);
}
__name(isServerStablePersonIdentityClaimKey, "isServerStablePersonIdentityClaimKey");
function quarantineConflictingServerUserIdentityClaims(value) {
  if (!value || typeof value !== "object") return value;
  const pending = [value];
  const seen = /* @__PURE__ */ new Set();
  while (pending.length) {
    const current = pending.pop();
    if (!current || typeof current !== "object" || seen.has(current)) continue;
    seen.add(current);
    if (Array.isArray(current)) {
      for (const entry of current) if (entry && typeof entry === "object") pending.push(entry);
      continue;
    }
    for (const rawKey of Object.keys(current)) {
      const normalized = normalizeBrowserFieldKey(rawKey);
      if (isServerStablePersonIdentityClaimKey(rawKey)) {
        delete current[rawKey];
      } else if (current[rawKey] && typeof current[rawKey] === "object") {
        pending.push(current[rawKey]);
      }
    }
  }
  return value;
}
__name(quarantineConflictingServerUserIdentityClaims, "quarantineConflictingServerUserIdentityClaims");
var UNTRUSTED_BROWSER_IDENTITY_KEYS = new Set([
  "user", "userid", "user_id", "external_id", "eden_identity_id",
  "patientid", "patient_id", "customerid", "customer_id", "memberid", "member_id",
  "orderid", "order_id", "masterid", "master_id", "treatmentid", "treatment_id", "transactionid", "transaction_id",
  "chargeid", "charge_id", "paymentid", "payment_id", "paymentintentid", "payment_intent_id",
  "stripepaymentintentid", "stripe_payment_intent_id", "authorizationid", "authorization_id",
  "captureid", "capture_id", "invoiceid", "invoice_id", "subscriptionid", "subscription_id", "checkoutid", "checkout_id",
  "groupid", "group_id",
  "email", "customeremail", "customer_email", "emailhash", "email_hash", "emailsha256", "email_sha256",
  "phone", "phonenumber", "phone_number", "phonehash", "phone_hash", "phonesha256", "phone_sha256",
  "firstname", "first_name", "lastname", "last_name", "fullname", "full_name",
  "address", "address1", "address_1", "address2", "address_2", "postalcode", "postal_code", "zipcode", "zip_code"
]);
function normalizeBrowserFieldKey(rawKey) {
  return safeDecodeQueryKey(rawKey)
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}
__name(normalizeBrowserFieldKey, "normalizeBrowserFieldKey");
var ANONYMOUS_IDENTITY_CLAIM_KEYS = /* @__PURE__ */ new Set([
  "anonymous_id", "anonymoous_id", "eden_anonymous_id", "eden_anon_id", "segment_anonymous_id",
  "first_party_device_id"
]);
function quarantineConflictingAnonymousIdentityClaims(value) {
  if (!value || typeof value !== "object") return value;
  const pending = [value];
  const seen = /* @__PURE__ */ new Set();
  while (pending.length) {
    const current = pending.pop();
    if (!current || typeof current !== "object" || seen.has(current)) continue;
    seen.add(current);
    if (Array.isArray(current)) {
      for (const entry of current) if (entry && typeof entry === "object") pending.push(entry);
      continue;
    }
    for (const rawKey of Object.keys(current)) {
      if (ANONYMOUS_IDENTITY_CLAIM_KEYS.has(normalizeBrowserFieldKey(rawKey))) {
        delete current[rawKey];
      } else if (current[rawKey] && typeof current[rawKey] === "object") {
        pending.push(current[rawKey]);
      }
    }
  }
  return value;
}
__name(quarantineConflictingAnonymousIdentityClaims, "quarantineConflictingAnonymousIdentityClaims");
function quarantineConflictingOrderIdentityClaims(value) {
  if (!value || typeof value !== "object") return value;
  const pending = [value];
  const seen = /* @__PURE__ */ new Set();
  while (pending.length) {
    const current = pending.pop();
    if (!current || typeof current !== "object" || seen.has(current)) continue;
    seen.add(current);
    if (Array.isArray(current)) {
      for (const entry of current) if (entry && typeof entry === "object") pending.push(entry);
      continue;
    }
    for (const rawKey of Object.keys(current)) {
      if (normalizeBrowserFieldKey(rawKey) === "order_id") {
        delete current[rawKey];
      } else if (current[rawKey] && typeof current[rawKey] === "object") {
        pending.push(current[rawKey]);
      }
    }
  }
  return value;
}
__name(quarantineConflictingOrderIdentityClaims, "quarantineConflictingOrderIdentityClaims");
function isUntrustedBrowserIdentityKey(rawKey) {
  const normalized = normalizeBrowserFieldKey(rawKey);
  const compact = normalized.replace(/_/g, "");
  if (ANONYMOUS_IDENTITY_CLAIM_KEYS.has(normalized)) return true;
  if (UNTRUSTED_BROWSER_IDENTITY_KEYS.has(normalized)) return true;
  // Safe telemetry prefixes must not turn a stable identity/PII suffix into an
  // allowed field (for example page_user_id, product_order_id,
  // device_email_sha256, or screen_customer_id).
  if (/(?:^|_)(?:user|patient|customer|member|person|identity|account|profile|contact|order|master|treatment|transaction|group)(?:_external)?_?ids?$/.test(normalized)) return true;
  if (/(?:^|_)(?:(?:customer|contact)_?)?(?:email|phone)(?:_?(?:hash|sha256))?$/.test(normalized)) return true;
  if (/(?:^|_)(?:first|last|full|given|family)_?name$/.test(normalized) || /(?:^|_)surname$/.test(normalized)) return true;
  if (/(?:^|_)(?:(?:shipping|billing|street|mailing|residential|physical)_?)?address(?:_?line)?_?[12]?$/.test(normalized)) return true;
  // Cover common camelCase/snake_case/plural aliases without matching product,
  // click, anonymous, or session identifiers that are legitimate browser
  // telemetry. The browser capability authenticates transport, never a person.
  return /^(?:eden)?(?:external)?(?:user|patient|customer|member|person|identity|account|profile|contact|order|master|treatment|transaction|group)(?:external)?ids?$/.test(compact)
    || /^externalids?$/.test(compact)
    || /^(?:customer|contact)?(?:email|phone)(?:hash|sha256)?$/.test(compact)
    || /^(?:first|last|full|given|family)name$/.test(compact)
    || /^surname$/.test(compact)
    || /^(?:postal|zip)code$/.test(compact)
    || /^(?:shipping|billing|street|mailing|residential|physical)?address(?:line)?[12]?$/.test(compact);
}
__name(isUntrustedBrowserIdentityKey, "isUntrustedBrowserIdentityKey");
function scrubUntrustedBrowserIdentityClaims(value) {
  if (!value || typeof value !== "object") return value;
  const stack = [value];
  const seen = /* @__PURE__ */ new Set();
  while (stack.length) {
    const current = stack.pop();
    if (!current || typeof current !== "object" || seen.has(current)) continue;
    seen.add(current);
    if (Array.isArray(current)) {
      for (const entry of current) if (entry && typeof entry === "object") stack.push(entry);
      continue;
    }
    for (const rawKey of Object.keys(current)) {
      if (isUntrustedBrowserIdentityKey(rawKey)) {
        delete current[rawKey];
        continue;
      }
      if (current[rawKey] && typeof current[rawKey] === "object") stack.push(current[rawKey]);
    }
  }
  return value;
}
__name(scrubUntrustedBrowserIdentityClaims, "scrubUntrustedBrowserIdentityClaims");
var BROWSER_SAFE_PROPERTY_KEYS = new Set([
  "page_url", "url", "href", "page_path", "path", "page_search", "search", "query_string",
  "referrer", "page_referrer", "page_title", "title", "page_name", "screen_name",
  "action", "category", "label", "event_action", "event_category", "event_label",
  "element_id", "element_name", "element_type", "element_text",
  "button_id", "button_name", "button_text", "cta_id", "cta_name", "cta_text",
  "link_url", "link_text", "step", "step_name", "step_number", "funnel", "funnel_name",
  "product", "product_id", "product_name", "product_category",
  "offering", "offering_id", "offering_name", "plan", "plan_id", "plan_name",
  "experiment", "experiment_id", "experiment_name", "variant", "variant_id",
  "feature", "feature_id", "feature_name", "value", "price", "currency", "quantity",
  "locale", "language", "country", "country_code", "device_type", "browser_name", "viewport_width", "viewport_height",
  "consent_state", "eden_privacy"
]);
var BROWSER_BLOCKED_PROPERTY_PATTERN = /(?:^|_)(?:password|passcode|secret|token|credential|credentials|api_key|private_key|client_secret|access_token|refresh_token|auth_token|authorization|bearer|cookie|session_token|card_number|card_cvc|card_cvv|valid_thru|validthru|bank_account|bank_account_number|account_number|routing_number|iban|swift|bic|social_security_number|ssn|date_of_birth|dob|clinical_note|medical_record|free_text|answer_text|mixpanel_insert_id|insert_id|adid|aaid|idfv|advertising_id|advertising_identifier|advertisingidentifier|gaid|idfa|vendor_id|device_id|uuid|latitude|longitude|lat|lng|lon|coordinates|geohash)(?:_|$)/;
// These fields describe the Worker's custody, identity-continuity, provenance,
// or upload decision. Browser producers may send the raw observation (page URL,
// UTM, and validated click IDs), but they cannot pre-assert Eden's derived
// session/touch/provenance state and have it survive merely because collection
// is default-allow.
var BROWSER_EDGE_OWNED_PROPERTY_PATTERN = /^(?:source_type|portal|pipeline_version|gpc_opt_out|attribution_suppressed|collector_source|source_system|first_party_device_id|enrichment_(?:version|active|mode|canary)|session(?:_.*)?|eden_session_.*|attribution_snapshot_id|attribution_model|attribution_recovered_.*|attribution_recovery_.*|first_touch_.*|current_touch_.*|last_touch_.*|touch_model_version|transported_internal_handoff.*|ad_click_.*|google_click_.*|raw_primary_click_id_.*|selected_google_.*|allowed_for_google_.*|upload_.*|.*_for_upload|browser_producer_message_id_.*|browser_message_scope|browser_conversion_observation|browser_event_authority)$/;
var BROWSER_SENSITIVE_PATH_PATTERN = /(?:^|_)(?:traits|profile|person|user|customer|patient|member|contact)_(?:id|external_id|name|full_name|email|phone|address|postal_code|zip_code)(?:_|$)|(?:^|_)(?:credentials?|auth|authorization|oauth|bearer)(?:_[a-z0-9]+)*_(?:token|secret|password|key)(?:_|$)|(?:^|_)(?:payment_)?card(?:_[a-z0-9]+)*_(?:number|pan|cvc|cvv|cvc2|cvv2|security_code|expiration|expiration_date|expiry|expiry_date|exp_month|exp_year|valid_thru|validthru|cardholder_name)(?:_|$)|(?:^|_)(?:device_)?(?:advertising_id|advertisingid|advertising_identifier|advertisingidentifier|adid|aaid|gaid|idfa|idfv|vendor_id|uuid)(?:_|$)|(?:^|_)device_id(?:_|$)|(?:^|_)(?:context_)?(?:location|geo)(?:_[a-z0-9]+)*_(?:latitude|longitude|lat|lng|lon|coordinates|geohash)(?:_|$)/;
function isBrowserSafeAttributionField(key) {
  const canonical = canonicalQueryParamName(key).toLowerCase();
  return CLICK_ID_PARAMS.map((item) => String(item).toLowerCase()).includes(canonical)
    || GOOGLE_AD_PARAM_FIELDS.map((item) => String(item).toLowerCase()).includes(canonical)
    || PARTNER_PARAM_FIELDS.map((item) => String(item).toLowerCase()).includes(canonical)
    || canonical.startsWith("utm_")
    || ["acquisition_channel", "attribution_source", "attribution_medium", "attribution_campaign", "attribution_referrer", "landing_page"].includes(canonical);
}
__name(isBrowserSafeAttributionField, "isBrowserSafeAttributionField");
function isBrowserSafePropertyKey(rawKey, parentPath = []) {
  const key = normalizeBrowserFieldKey(rawKey);
  const pathKey = [...parentPath, key].filter(Boolean).join("_");
  if (isUntrustedBrowserIdentityKey(rawKey) || isUntrustedBrowserIdentityKey(pathKey)) return false;
  if (!key
    || BROWSER_BLOCKED_PROPERTY_PATTERN.test(key)
    || BROWSER_BLOCKED_PROPERTY_PATTERN.test(pathKey)
    || BROWSER_EDGE_OWNED_PROPERTY_PATTERN.test(key)
    || BROWSER_EDGE_OWNED_PROPERTY_PATTERN.test(pathKey)
    || BROWSER_SENSITIVE_PATH_PATTERN.test(pathKey)) return false;
  if (BROWSER_SAFE_PROPERTY_KEYS.has(key) || isBrowserSafeAttributionField(rawKey)) return true;
  return /^(?:page|screen|element|button|cta|link|step|funnel|product|offering|plan|experiment|variant|feature|device|viewport|browser)_[a-z0-9_]{1,48}$/.test(key)
    || /^is_[a-z0-9_]{1,48}$/.test(key)
    // First-party behavior schemas evolve faster than Worker deploys. Retain
    // bounded non-identity scalar fields rather than silently dropping them.
    || /^[a-z][a-z0-9_]{0,63}$/.test(key);
}
__name(isBrowserSafePropertyKey, "isBrowserSafePropertyKey");
function sanitizeBrowserScalar(value, { maxLength = 512 } = {}) {
  if (value === null || typeof value === "boolean" || typeof value === "number") return value;
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim().slice(0, maxLength);
  if (!trimmed) return "";
  // Even an otherwise safe display/label field cannot be used to smuggle
  // direct contact data through the browser collector.
  if (/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i.test(trimmed)) return undefined;
  if (/^(?:\+?\d[\d\s().-]{6,}\d)$/.test(trimmed) && trimmed.replace(/\D/g, "").length >= 7) return undefined;
  return trimmed;
}
__name(sanitizeBrowserScalar, "sanitizeBrowserScalar");
function sanitizeBrowserConsentObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const allowed = new Set([
    "consent_status", "source", "action_taken", "ads", "google_ads", "advertising", "ad_tracking",
    "partner_ad_tracking", "retargeting", "sale_share_targeted_ads", "ads_opted_out",
    "google_ads_allowed", "allowed_for_google_click_id_upload", "basis", "user_choice", "purpose_decisions"
  ]);
  const out = {};
  for (const [rawKey, entry] of Object.entries(value)) {
    const key = normalizeBrowserFieldKey(rawKey);
    if (!allowed.has(key)) continue;
    if (key === "purpose_decisions") {
      const decisions = Array.isArray(entry) ? entry : entry && typeof entry === "object" ? Object.entries(entry).map(([purpose, decision]) => ({ purpose, ...(decision && typeof decision === "object" ? decision : { allowed: decision }) })) : [];
      out.purpose_decisions = decisions.slice(0, 24).map((decision) => ({
        purpose: normalizeBrowserFieldKey(decision?.purpose || ""),
        allowed: typeof decision?.allowed === "boolean" ? decision.allowed : sanitizeBrowserScalar(decision?.allowed),
        basis: sanitizeBrowserScalar(decision?.basis)
      })).filter((decision) => !!decision.purpose);
      continue;
    }
    if (["action_taken", "ads_opted_out", "google_ads_allowed", "allowed_for_google_click_id_upload"].includes(key) && typeof entry === "boolean") {
      out[key] = entry;
      continue;
    }
    const safe = sanitizeBrowserScalar(entry);
    if (safe !== undefined) out[key] = safe;
  }
  return out;
}
__name(sanitizeBrowserConsentObject, "sanitizeBrowserConsentObject");
function sanitizeBrowserDeviceObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const allowed = new Set([
    "type", "category", "platform", "os", "os_name", "os_version",
    "browser", "browser_name", "browser_version", "manufacturer", "model",
    "mobile", "tablet", "desktop"
  ]);
  const out = {};
  for (const [rawKey, entry] of Object.entries(value)) {
    const key = normalizeBrowserFieldKey(rawKey);
    if (!allowed.has(key)) continue;
    const safe = sanitizeBrowserScalar(entry);
    if (safe !== undefined) out[rawKey] = safe;
  }
  return Object.keys(out).length ? out : undefined;
}
__name(sanitizeBrowserDeviceObject, "sanitizeBrowserDeviceObject");
function sanitizeBrowserLocationObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const allowed = new Set([
    "country", "country_code", "region", "region_code", "state", "state_code",
    "city", "timezone", "time_zone", "locale", "language"
  ]);
  const out = {};
  for (const [rawKey, entry] of Object.entries(value)) {
    const key = normalizeBrowserFieldKey(rawKey);
    if (!allowed.has(key)) continue;
    const safe = sanitizeBrowserScalar(entry);
    if (safe !== undefined) out[rawKey] = safe;
  }
  return Object.keys(out).length ? out : undefined;
}
__name(sanitizeBrowserLocationObject, "sanitizeBrowserLocationObject");
function sanitizeBrowserPropertyObject(value, depth = 0, parentPath = []) {
  if (!value || typeof value !== "object" || Array.isArray(value) || depth > 4) return {};
  const out = {};
  for (const [rawKey, entry] of Object.entries(value)) {
    if (Object.keys(out).length >= 100) break;
    const key = normalizeBrowserFieldKey(rawKey);
    if (!isBrowserSafePropertyKey(rawKey, parentPath)) continue;
    const childPath = [...parentPath, key];
    if (key === "consent_state" || key === "eden_privacy") {
      const consent = sanitizeBrowserConsentObject(entry);
      if (consent && Object.keys(consent).length) out[rawKey] = consent;
      continue;
    }
    if (key === "device") {
      const device = sanitizeBrowserDeviceObject(entry);
      if (device) out[rawKey] = device;
      continue;
    }
    if (key === "location" || key === "geo") {
      const location = sanitizeBrowserLocationObject(entry);
      if (location) out[rawKey] = location;
      continue;
    }
    if (entry && typeof entry === "object" && !Array.isArray(entry)) {
      const nested = sanitizeBrowserPropertyObject(entry, depth + 1, childPath);
      if (Object.keys(nested).length) out[rawKey] = nested;
      continue;
    }
    if (Array.isArray(entry)) {
      const safeItems = entry.slice(0, 25).map((item) => {
        if (item && typeof item === "object" && !Array.isArray(item)) {
          const nested = sanitizeBrowserPropertyObject(item, depth + 1, childPath);
          return Object.keys(nested).length ? nested : undefined;
        }
        if (Array.isArray(item) && depth < 4) {
          const nested = item.slice(0, 25).map((nestedItem) => sanitizeBrowserScalar(nestedItem)).filter((nestedItem) => nestedItem !== undefined);
          return nested.length ? nested : undefined;
        }
        return sanitizeBrowserScalar(item);
      }).filter((item) => item !== undefined);
      if (safeItems.length) out[rawKey] = safeItems;
      continue;
    }
    const safe = sanitizeBrowserScalar(entry, { maxLength: isPersistedUrlField(key) ? 2048 : 512 });
    if (safe !== undefined) out[rawKey] = safe;
  }
  return out;
}
__name(sanitizeBrowserPropertyObject, "sanitizeBrowserPropertyObject");
function sanitizeBrowserContext(value) {
  // Context is telemetry too. Preserve bounded nested context so Segment can
  // govern schemas and destinations without requiring a Worker release for
  // each SDK/library/device field. The same recursive secret, direct-contact,
  // and stable-identity scrub used for properties still applies.
  return sanitizeBrowserPropertyObject(value, 0, ["context"]);
}
__name(sanitizeBrowserContext, "sanitizeBrowserContext");
function sanitizeBrowserCollectorBody(body) {
  const type = String(body?.type || "track").trim().toLowerCase();
  if (!["track", "page", "screen"].includes(type)) {
    return { allowed: false, error: type === "identify" ? "browser_identify_message_not_authorized" : "browser_message_type_not_allowed" };
  }
  const originalEventName = sanitizeBrowserScalar(resolveEventName(body), { maxLength: 128 }) || "";
  if (type === "track" && !originalEventName) return { allowed: false, error: "browser_event_name_required" };
  const eventName = originalEventName;
  scrubUntrustedBrowserIdentityClaims(body);
  const incomingMessageId = boundedStableIdentifier(body.messageId || body.message_id, 128);
  const clean = {
    type,
    ...eventName ? { event: eventName } : {},
    ...incomingMessageId ? { messageId: incomingMessageId } : {},
    ...["page", "screen"].includes(type) && typeof body.name === "string" ? { name: sanitizeBrowserScalar(body.name) || "" } : {},
    properties: sanitizeBrowserPropertyObject(body.properties || {}, 0, ["properties"]),
    context: sanitizeBrowserContext(body.context || {})
  };
  for (const key of ["consent_state", "eden_privacy"]) {
    const consent = sanitizeBrowserConsentObject(body[key]);
    if (consent && Object.keys(consent).length) clean[key] = consent;
  }
  for (const key of ["timestamp", "originalTimestamp"]) {
    const candidate = body[key];
    if (candidate && Number.isFinite(Date.parse(String(candidate)))) clean[key] = new Date(candidate).toISOString();
  }
  for (const key of Object.keys(body)) delete body[key];
  Object.assign(body, clean);
  return { allowed: true, eventName };
}
__name(sanitizeBrowserCollectorBody, "sanitizeBrowserCollectorBody");
async function scopeBrowserMessageId(body, anonId) {
  const producerMessageId = boundedStableIdentifier(body?.messageId, 128);
  if (!producerMessageId) return null;
  if (!body.properties || typeof body.properties !== "object" || Array.isArray(body.properties)) body.properties = {};
  const producerHash = await sha256Raw(`eden_browser_producer_message_id_v1\0${producerMessageId}`);
  const ownerScopedHash = await sha256Raw(`eden_browser_segment_message_id_v1\0${anonId}\0${producerMessageId}`);
  // Segment deduplicates by messageId. Namespace the browser observation so a
  // browser OS_purchase cannot suppress the later authoritative server event
  // merely by copying its messageId. The raw producer ID remains out of the
  // payload; its hash supports governed reconciliation without becoming a
  // person/order/payment identity.
  body.messageId = `b-${ownerScopedHash.slice(0, 32)}`;
  body.properties.browser_producer_message_id_sha256 = producerHash;
  body.properties.browser_message_scope = "eden_anonymous_id";
  return body.messageId;
}
__name(scopeBrowserMessageId, "scopeBrowserMessageId");
function isGpcOptOut(request) {
  return request.headers.get("Sec-GPC") === "1";
}
__name(isGpcOptOut, "isGpcOptOut");
function hasAdvertisingDenialMarker(request) {
  return readCookie(request, ATTRIBUTION_DENIAL_COOKIE_NAME) === "1";
}
__name(hasAdvertisingDenialMarker, "hasAdvertisingDenialMarker");
function canUseAttributionForRequest(env, gpcOptOut, request = null, body = null) {
  const privacyState = readEdenConsentState(request, body);
  if (privacyState?.denyAdvertising)
    return false;
  // The marker is set only after a CMP/Eden explicit-denial signal. Raw
  // Sec-GPC remains diagnostic and cannot create this state by itself. An
  // explicit allow may proceed only through the async durable-clear gate.
  if (hasAdvertisingDenialMarker(request) && !privacyState?.explicitAllowAdvertising)
    return false;
  return true;
}
__name(canUseAttributionForRequest, "canUseAttributionForRequest");
async function advertisingDenialKeys(env, { anonId = null, session = null, userId = null, orderId = null } = {}, { includePrevious = false } = {}) {
  const currentSecret = String(env?.[PRIVACY_LEDGER_HMAC_SECRET_ENV] || "");
  if (!currentSecret) throw new Error("privacy_ledger_hmac_secret_missing");
  const secrets = [currentSecret];
  const previousSecret = String(env?.[PRIVACY_LEDGER_HMAC_PREVIOUS_SECRET_ENV] || "");
  if (includePrevious && previousSecret && previousSecret !== currentSecret) secrets.push(previousSecret);
  const values = [
    ["anon", anonId],
    ["session", sessionRawValue(session)],
    ["user", userId],
    ["order", orderId]
  ];
  const keys = [];
  for (const [type, value] of values) {
    if (value) {
      for (const secret of secrets) {
        const digest = await hmacSha256Raw(secret, `advertising-denial:${type}:v1`, value);
        keys.push(`${ATTRIBUTION_DENIAL_PREFIX}${type}:${digest}`);
      }
    }
  }
  return [...new Set(keys)];
}
__name(advertisingDenialKeys, "advertisingDenialKeys");
function getPrivacyLedgerKv(env) {
  // Privacy state is a separate authority. Never silently co-locate it with
  // attribution continuity when a binding is missing or misconfigured.
  return env?.PRIVACY_LEDGER_KV || null;
}
__name(getPrivacyLedgerKv, "getPrivacyLedgerKv");
async function hasDurableAdvertisingDenial(env, identity = {}) {
  const kv = getPrivacyLedgerKv(env);
  if (!kv) throw new Error("privacy_ledger_kv_missing");
  const keys = await advertisingDenialKeys(env, identity, { includePrevious: true });
  if (!keys.length) return false;
  const values = await Promise.all(keys.map((key) => kv.get(key)));
  return values.some(Boolean);
}
__name(hasDurableAdvertisingDenial, "hasDurableAdvertisingDenial");
async function clearDurableAdvertisingDenial(env, identity = {}) {
  const kv = getPrivacyLedgerKv(env);
  if (!kv) throw new Error("privacy_ledger_kv_missing");
  const keys = await advertisingDenialKeys(env, identity, { includePrevious: true });
  if (!keys.length) throw new Error("privacy_ledger_identity_missing");
  await Promise.all(keys.map((key) => kv.delete(key)));
  return keys.length;
}
__name(clearDurableAdvertisingDenial, "clearDurableAdvertisingDenial");
async function revokeOwnedAdClickPointer(env, request, identity = {}) {
  const kv = getAdClickMemoryKV(env);
  const adClickId = readAdClickPointerCookie(request);
  if (!kv || !adClickId) return;
  const [record, owner] = await Promise.all([
    readCanonicalAdClickPointerRecord(env, adClickId),
    currentPointerOwnerContext(identity)
  ]);
  const ownership = validateAdClickPointerOwnership(record, adClickId, owner);
  if (!ownership.valid) return;
  const result = await mutateAdClickPointerThroughCoordinator(env, adClickId, "revoke", {
    seed_record: record,
    owner,
    revoked_at: nowUTC(),
    revocation_reason: "explicit_advertising_denial",
    ttl_seconds: adClickPointerKvTtlSeconds(env)
  });
  if (result?.revoked !== true) throw new Error("ad_click_pointer_revocation_unconfirmed");
}
__name(revokeOwnedAdClickPointer, "revokeOwnedAdClickPointer");
async function persistDurableAdvertisingDenial(env, request, identity = {}) {
  const writes = [];
  const kv = getPrivacyLedgerKv(env);
  if (kv) {
    const keys = await advertisingDenialKeys(env, identity);
    const record = JSON.stringify({
      schema_version: ATTRIBUTION_DENIAL_SCHEMA_VERSION,
      denied_at: nowUTC(),
      source: "cookieyes_or_eden_consent_state"
    });
    writes.push(...keys.map((key) => kv.put(key, record, { expirationTtl: ATTRIBUTION_DENIAL_TTL })));
  }
  writes.push(revokeOwnedAdClickPointer(env, request, identity));
  await Promise.all(writes);
}
__name(persistDurableAdvertisingDenial, "persistDurableAdvertisingDenial");
async function resolveAttributionPermissionWithDurableState(env, request, body = null, identity = {}, options = {}) {
  const privacyState = readEdenConsentState(request, body);
  const markerDenied = hasAdvertisingDenialMarker(request);
  const durableIdentityMutationAllowed = options?.durableIdentityMutationAllowed !== false;
  if (privacyState?.denyAdvertising) {
    if (durableIdentityMutationAllowed) {
      try {
        await persistDurableAdvertisingDenial(env, request, identity);
      } catch (err) {
        console.error("[eden-analytics] durable advertising denial persist failed", err);
      }
    }
    // The response marker is independent of the eventually-consistent ledger,
    // so a failed or not-yet-visible write cannot resurrect attribution on the
    // next no-choice request.
    return { allowed: false, setDenialMarker: true, clearDenialMarker: false, reason: durableIdentityMutationAllowed ? "explicit_denial" : "explicit_denial_identity_quarantined" };
  }
  if (privacyState?.explicitAllowAdvertising) {
    if (!durableIdentityMutationAllowed) {
      return { allowed: true, setDenialMarker: false, clearDenialMarker: true, reason: "explicit_allow_identity_quarantined" };
    }
    try {
      await clearDurableAdvertisingDenial(env, identity);
    } catch (err) {
      console.error("[eden-analytics] durable advertising denial clear failed", err);
      // An affirmative current first-party allow is the authoritative request
      // decision. A transient KV/delete failure is an observability and retry
      // concern; it must not discard otherwise valid marketing evidence.
      return { allowed: true, setDenialMarker: false, clearDenialMarker: true, reason: "explicit_allow_durable_clear_deferred" };
    }
    return { allowed: true, setDenialMarker: false, clearDenialMarker: true, reason: "explicit_allow_durable_clear_succeeded" };
  }
  if (markerDenied) {
    // Heal/propagate the durable ledger opportunistically, but the immediate
    // first-party marker is already sufficient to fail attribution closed.
    if (durableIdentityMutationAllowed) {
      try {
        await persistDurableAdvertisingDenial(env, request, identity);
      } catch (err) {
        console.error("[eden-analytics] advertising denial marker ledger heal failed", err);
      }
    }
    return { allowed: false, setDenialMarker: true, clearDenialMarker: false, reason: durableIdentityMutationAllowed ? "first_party_denial_marker" : "first_party_denial_marker_identity_quarantined" };
  }
  if (!durableIdentityMutationAllowed) {
    return { allowed: true, setDenialMarker: false, clearDenialMarker: false, reason: "privacy_identity_quarantined_no_mutation" };
  }
  try {
    const denied = await hasDurableAdvertisingDenial(env, identity);
    if (!denied) return { allowed: true, setDenialMarker: false, clearDenialMarker: false, reason: "no_durable_denial" };
    // Denial follows the person as first-party identity becomes richer. If an
    // anonymous denial is later presented alongside a user/order identifier,
    // propagate tombstones to every current identity before suppressing. This
    // prevents a later user-only server event from resurrecting attribution.
    try {
      await persistDurableAdvertisingDenial(env, request, identity);
    } catch (err) {
      console.error("[eden-analytics] durable advertising denial propagation failed", err);
    }
    return { allowed: false, setDenialMarker: true, clearDenialMarker: false, reason: "durable_denial" };
  } catch (err) {
    // Default/no-action traffic remains trackable. The current request still
    // honors explicit denial state and the first-party denial marker above, but
    // a missing binding or transient KV read failure cannot globally suppress
    // gclid/gbraid/wbraid, UTMs, or first-party continuity.
    console.error("[eden-analytics] durable advertising denial read failed", err);
    return { allowed: true, setDenialMarker: false, clearDenialMarker: false, reason: "privacy_ledger_unavailable_tracking_continues" };
  }
}
__name(resolveAttributionPermissionWithDurableState, "resolveAttributionPermissionWithDurableState");
async function canUseAttributionWithDurableState(env, request, body = null, identity = {}) {
  return (await resolveAttributionPermissionWithDurableState(env, request, body, identity)).allowed;
}
__name(canUseAttributionWithDurableState, "canUseAttributionWithDurableState");
function normalizeConsentObject(value) {
  if (!value) return null;
  if (typeof value === "object" && !Array.isArray(value)) return value;
  if (typeof value !== "string") return null;
  return parseConsentStateCookie(value);
}
__name(normalizeConsentObject, "normalizeConsentObject");
function consentValueIsDenied(value) {
  if (value === false || value === 0) return true;
  return ["denied", "opted_out", "rejected", "false", "0", "no"].includes(String(value || "").trim().toLowerCase());
}
__name(consentValueIsDenied, "consentValueIsDenied");
function consentValueIsAllowed(value) {
  if (value === true || value === 1) return true;
  return ["allowed", "granted", "accepted", "opted_in", "consented", "true", "1", "yes"].includes(String(value || "").trim().toLowerCase());
}
__name(consentValueIsAllowed, "consentValueIsAllowed");
function consentFlagIsTrue(value) {
  return value === true || value === 1 || ["true", "1", "yes"].includes(String(value ?? "").trim().toLowerCase());
}
__name(consentFlagIsTrue, "consentFlagIsTrue");
function canonicalizeConsentObject(parsed) {
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
  const out = {};
  for (const [rawKey, value] of Object.entries(parsed)) {
    const key = normalizeBrowserFieldKey(rawKey);
    if (key && out[key] === undefined) out[key] = value;
  }
  return out;
}
__name(canonicalizeConsentObject, "canonicalizeConsentObject");
function consentObjectDeniesAdvertising(parsed) {
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return false;
  const canonical = canonicalizeConsentObject(parsed);
  const normalized = Object.fromEntries(Object.entries(canonical).map(([key, value]) => [key, String(value ?? "").trim().toLowerCase()]));
  const status = normalized.consent_status || "";
  const actionTaken = canonical.action_taken === true || ["true", "1", "yes"].includes(normalized.action_taken);
  const adFields = [
    canonical.ads,
    canonical.google_ads,
    canonical.advertising,
    canonical.ad_tracking,
    canonical.partner_ad_tracking,
    canonical.retargeting,
    canonical.sale_share_targeted_ads
  ];
  // Only affirmative first-party opt-out evidence suppresses tracking. Producer
  // defaults (`false`/unknown), raw GPC source labels, state-policy inference,
  // and unsigned URL transport are diagnostics rather than customer choices.
  if (consentFlagIsTrue(canonical.ads_opted_out)) return true;
  if (["rejected", "opted_out", "denied", "do_not_sell"].includes(normalized.user_choice)) return true;
  if (normalized.basis === "denied_by_user") return true;
  if (actionTaken && (["opted_out", "rejected", "denied"].includes(status) || adFields.some(consentValueIsDenied))) return true;
  if (actionTaken && (consentValueIsDenied(canonical.google_ads_allowed) || consentValueIsDenied(canonical.allowed_for_google_click_id_upload))) return true;
  const decisions = canonical.purpose_decisions;
  const decisionList = Array.isArray(decisions) ? decisions : decisions && typeof decisions === "object" ? Object.entries(decisions).map(([purpose, decision]) => ({ purpose, ...(decision && typeof decision === "object" ? decision : { allowed: decision }) })) : [];
  const advertisingPurposes = new Set(["advertising_storage", "ad_user_data", "ad_personalization", "sale_share", "targeted_advertising", "attribution_measurement"]);
  return decisionList.some((decision) => {
    if (!advertisingPurposes.has(normalizeBrowserFieldKey(decision?.purpose || ""))) return false;
    const basis = String(decision?.basis || "").toLowerCase();
    return basis === "denied_by_user" || (actionTaken && (decision?.allowed === false || consentValueIsDenied(decision?.allowed)));
  });
}
__name(consentObjectDeniesAdvertising, "consentObjectDeniesAdvertising");
function consentObjectExplicitlyAllowsAdvertising(parsed) {
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return false;
  const canonical = canonicalizeConsentObject(parsed);
  const normalized = Object.fromEntries(Object.entries(canonical).map(([key, value]) => [key, String(value ?? "").trim().toLowerCase()]));
  const actionTaken = canonical.action_taken === true || ["true", "1", "yes"].includes(normalized.action_taken);
  const status = normalized.consent_status || "";
  const source = normalized.source || "";
  const userChoice = normalized.user_choice || "";
  const adFields = [
    canonical.ads,
    canonical.google_ads,
    canonical.advertising,
    canonical.ad_tracking,
    canonical.partner_ad_tracking,
    canonical.retargeting,
    canonical.sale_share_targeted_ads
  ];
  const decisions = canonical.purpose_decisions;
  const decisionList = Array.isArray(decisions) ? decisions : decisions && typeof decisions === "object" ? Object.entries(decisions).map(([purpose, decision]) => ({ purpose, ...(decision && typeof decision === "object" ? decision : { allowed: decision }) })) : [];
  const advertisingPurposes = new Set(["advertising_storage", "ad_user_data", "ad_personalization", "sale_share", "targeted_advertising", "attribution_measurement"]);
  const explicitPositiveSignal = ["explicit_allowed", "allowed", "opted_in", "accepted", "consented"].includes(status)
    || ["accepted", "allowed", "opted_in", "consented"].includes(userChoice)
    || consentFlagIsTrue(canonical.google_ads_allowed)
    || consentFlagIsTrue(canonical.allowed_for_google_click_id_upload)
    || adFields.some(consentValueIsAllowed)
    || decisionList.some((decision) => advertisingPurposes.has(normalizeBrowserFieldKey(decision?.purpose || ""))
      && (decision?.allowed === true || consentValueIsAllowed(decision?.allowed)));
  return actionTaken
    && !["gpc", "default_allowed_no_choice"].includes(source)
    && explicitPositiveSignal;
}
__name(consentObjectExplicitlyAllowsAdvertising, "consentObjectExplicitlyAllowsAdvertising");
function readEdenConsentState(request, body = null) {
  let cookieState = null;
  try {
    cookieState = normalizeConsentObject(readCookie(request, "eden_consent_state"));
  } catch {}
  const bodyCandidates = [
    body?.properties?.consent_state,
    body?.context?.consent,
    body?.context?.eden_privacy,
    body?.properties?.eden_privacy,
    body?.eden_privacy,
    body?.consent_state
  ].map(normalizeConsentObject).filter(Boolean);
  const consentCandidates = [cookieState, ...bodyCandidates].filter(Boolean);
  const denyAdvertising = consentCandidates.some(consentObjectDeniesAdvertising);
  if (consentCandidates.length === 0 && !denyAdvertising) return null;
  const explicitAllowAdvertising = !denyAdvertising && consentCandidates.some(consentObjectExplicitlyAllowsAdvertising);
  return { explicitAllowAdvertising, denyAdvertising };
}
__name(readEdenConsentState, "readEdenConsentState");
function parseConsentStateCookie(raw) {
  if (!raw)
    return null;
  const value = String(raw).trim();
  const candidates = [value];
  try {
    candidates.push(decodeURIComponent(value));
  } catch {
  }
  if (/^[A-Za-z0-9+/=_-]+$/.test(value) && value.length >= 8) {
    try {
      const base64 = value.replace(/-/g, "+").replace(/_/g, "/");
      const padded = base64.padEnd(Math.ceil(base64.length / 4) * 4, "=");
      candidates.push(atob(padded));
    } catch {
    }
  }
  for (const candidate of candidates) {
    try {
      return JSON.parse(candidate);
    } catch {
    }
  }
  return null;
}
__name(parseConsentStateCookie, "parseConsentStateCookie");
function privacyProperties(request) {
  return { gpc_opt_out: isGpcOptOut(request) };
}
__name(privacyProperties, "privacyProperties");
function detectOrganic(referrer) {
  if (!referrer)
    return null;
  try {
    const h = new URL(referrer).hostname.toLowerCase();
    const engines = {
      google: /^(.+\.)?google\.(com|co\.[a-z]{2}|[a-z]{2,3})(\.[a-z]{2})?$/i,
      bing: /^(.+\.)?bing\.(com|co\.[a-z]{2})$/i,
      yahoo: /^(search\.)?yahoo\.(com|co\.[a-z]{2})$/i,
      duckduckgo: /^(.+\.)?duckduckgo\.(com|co\.[a-z]{2})$/i,
      yandex: /^(.+\.)?yandex\.(com|ru|co\.[a-z]{2})$/i,
      baidu: /^(.+\.)?baidu\.(com|co\.[a-z]{2})$/i,
      brave: /^search\.brave\.(com|co\.[a-z]{2})$/i,
      ecosia: /^(.+\.)?ecosia\.(org|com)$/i
    };
    for (const [engine, pattern] of Object.entries(engines)) {
      if (pattern.test(h)) {
        const ref = new URL(referrer);
        const p = ref.pathname.toLowerCase();
        if (p.includes("search") || p === "/" || ref.searchParams.has("q") || ref.searchParams.has("query")) {
          return { utm_source: engine, utm_medium: "organic" };
        }
      }
    }
  } catch {
  }
  return null;
}
__name(detectOrganic, "detectOrganic");
function isBot(request) {
  const ua = request.headers.get("User-Agent") || "";
  if (BOT_UA_PATTERNS.some((p) => p.test(ua)))
    return true;
  const d = request.cf?.botManagement?.decision;
  if (d && BOT_CF_DECISIONS.has(d))
    return true;
  if (request.cf?.botManagement?.verifiedBot)
    return true;
  return false;
}
__name(isBot, "isBot");
function isSyntheticMonitor(request, url) {
  if (url.searchParams.has("eden_checkly_marker"))
    return true;
  if (url.searchParams.get("utm_medium") === "synthetic")
    return true;
  if ((url.searchParams.get("utm_source") || "").includes("checkly"))
    return true;
  if (/checklyhq/i.test(request.headers.get("User-Agent") || ""))
    return true;
  return false;
}
__name(isSyntheticMonitor, "isSyntheticMonitor");
function isStaticAsset(url) {
  const p = url.pathname.toLowerCase();
  if (STATIC_PREFIXES.some((x) => p.startsWith(x)))
    return true;
  if (STATIC_EXTENSIONS.some((x) => p.endsWith(x)))
    return true;
  return false;
}
__name(isStaticAsset, "isStaticAsset");
function normalizeIdentityEmail(value) {
  if (typeof value !== "string")
    return null;
  const normalized = value.trim().toLowerCase();
  return normalized || null;
}
__name(normalizeIdentityEmail, "normalizeIdentityEmail");
function normalizeIdentityPhone(value) {
  if (typeof value !== "string")
    return null;
  const digits = value.replace(/\D+/g, "");
  return digits.length >= 7 ? digits : null;
}
__name(normalizeIdentityPhone, "normalizeIdentityPhone");
function normalizeIdentityName(value) {
  if (typeof value !== "string")
    return null;
  const normalized = value.trim().toLowerCase().replace(/\s+/g, " ");
  return normalized || null;
}
__name(normalizeIdentityName, "normalizeIdentityName");
function normalizePostalCode(value) {
  if (typeof value !== "string")
    return null;
  const normalized = value.trim().toUpperCase().replace(/\s+/g, "");
  return normalized || null;
}
__name(normalizePostalCode, "normalizePostalCode");
function normalizeCountryCode(value) {
  if (typeof value !== "string")
    return null;
  const normalized = value.trim().toUpperCase();
  return normalized || null;
}
__name(normalizeCountryCode, "normalizeCountryCode");
// Augments payloads with hashed identity fields while preserving source fields for Segment/internal stitching.
async function hashEmail(props) {
  if (!props || typeof props !== "object")
    return props;
  const out = {};
  for (const [k, v] of Object.entries(props)) {
    if ((k === "email" || k === "customerEmail") && typeof v === "string") {
      const normalized = normalizeIdentityEmail(v);
      if (normalized && !out.email_sha256)
        out["email_sha256"] = await sha256(normalized);
      out[k] = v;
      continue;
    }
    if ((k === "phone" || k === "phoneNumber" || k === "customerPhone") && typeof v === "string") {
      const normalized = normalizeIdentityPhone(v);
      if (normalized && !out.phone_sha256)
        out["phone_sha256"] = await sha256(normalized);
      out[k] = v;
      continue;
    }
    if ((k === "first_name" || k === "firstName") && typeof v === "string") {
      const normalized = normalizeIdentityName(v);
      if (normalized && !out.first_name_sha256)
        out["first_name_sha256"] = await sha256(normalized);
      out[k] = v;
      continue;
    }
    if ((k === "last_name" || k === "lastName") && typeof v === "string") {
      const normalized = normalizeIdentityName(v);
      if (normalized && !out.last_name_sha256)
        out["last_name_sha256"] = await sha256(normalized);
      out[k] = v;
      continue;
    }
    if ((k === "postal_code" || k === "postalCode" || k === "zip") && typeof v === "string") {
      const normalized = normalizePostalCode(v);
      if (normalized && !out.postal_code)
        out["postal_code"] = normalized;
      out[k] = v;
      continue;
    }
    if ((k === "country" || k === "countryCode") && typeof v === "string") {
      const normalized = normalizeCountryCode(v);
      if (normalized && !out.country)
        out["country"] = normalized;
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
__name(hashEmail, "hashEmail");
async function sha256(value) {
  const buf = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(String(value).trim().toLowerCase())
  );
  return Array.from(new Uint8Array(buf)).map((b) => b.toString(16).padStart(2, "0")).join("");
}
__name(sha256, "sha256");
async function sha256Raw(value) {
  const buf = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(String(value))
  );
  return Array.from(new Uint8Array(buf)).map((b) => b.toString(16).padStart(2, "0")).join("");
}
__name(sha256Raw, "sha256Raw");
async function hmacSha256Bytes(secret, value) {
  const normalizedSecret = String(secret || "");
  if (!normalizedSecret) throw new Error("hmac_secret_missing");
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(normalizedSecret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  return new Uint8Array(await crypto.subtle.sign(
    "HMAC",
    key,
    encoder.encode(String(value))
  ));
}
__name(hmacSha256Bytes, "hmacSha256Bytes");
async function hmacSha256Raw(secret, purpose, value) {
  if (!secret) throw new Error("privacy_ledger_hmac_secret_missing");
  const signature = await hmacSha256Bytes(secret, `${String(purpose || "default")}\0${String(value)}`);
  return Array.from(signature).map((byte) => byte.toString(16).padStart(2, "0")).join("");
}
__name(hmacSha256Raw, "hmacSha256Raw");
async function readBoundedResponseText(response, maxBytes = 2048) {
  const limit = Math.max(0, Math.min(8192, Number(maxBytes) || 0));
  if (!limit || !response?.body || typeof response.body.getReader !== "function") return "";
  const reader = response.body.getReader();
  const chunks = [];
  let total = 0;
  try {
    while (total < limit) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!(value instanceof Uint8Array) || value.byteLength === 0) continue;
      const take = Math.min(value.byteLength, limit - total);
      chunks.push(value.slice(0, take));
      total += take;
      if (take < value.byteLength) break;
    }
  } finally {
    try { await reader.cancel(); } catch {}
  }
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder("utf-8", { fatal: false }).decode(merged);
}
__name(readBoundedResponseText, "readBoundedResponseText");
function sanitizeUpstreamErrorPrefix(value) {
  return String(value || "")
    .replace(/[\u0000-\u001f\u007f]+/g, " ")
    .replace(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi, "[redacted_email]")
    .replace(/\b(?:https?:\/\/|www\.)\S+/gi, "[redacted_url]")
    .replace(/\b[A-Za-z0-9_-]{32,}\b/g, "[redacted_token]")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 512);
}
__name(sanitizeUpstreamErrorPrefix, "sanitizeUpstreamErrorPrefix");
async function segmentPost(writeKey, endpoint, payload, options = {}) {
  let res;
  try {
    const timeoutMs = Number(options.timeoutMs || 0);
    if (timeoutMs > 0 && (typeof AbortSignal === "undefined" || typeof AbortSignal.timeout !== "function")) {
      throw new Error("segment_request_timeout_unavailable");
    }
    const signal = timeoutMs > 0 && typeof AbortSignal !== "undefined" && typeof AbortSignal.timeout === "function"
      ? AbortSignal.timeout(timeoutMs)
      : void 0;
    res = await fetch(`https://api.segment.io/v1/${endpoint}`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Basic ${btoa(writeKey + ":")}` },
      body: JSON.stringify(payload),
      ...signal ? { signal } : {}
    });
  } catch (err) {
    console.error(`[eden-analytics] segmentPost network error (${endpoint}):`, err);
    throw err;
  }
  if (!res.ok) {
    const txt = sanitizeUpstreamErrorPrefix(await readBoundedResponseText(res, 2048).catch(() => ""));
    console.error(`[eden-analytics] Segment ${endpoint} ${res.status}${txt ? `: ${txt}` : ""}`);
    const error = new Error(`Segment ${endpoint} ${res.status}`);
    // HTTP 5xx can be returned after an upstream accepted the request but lost
    // the acknowledgement. Treat it like a network/timeout ambiguity and keep
    // the staged delivery record. Only a non-transient 4xx proves this request
    // was rejected before commit; retryable/ambiguous 408/409/425/429 remain in
    // the unknown-commit lane as well.
    error.segmentDefinitiveRejection = res.status >= 400
      && res.status < 500
      && ![408, 409, 425, 429].includes(res.status);
    error.segmentStatus = res.status;
    throw error;
  }
}
__name(segmentPost, "segmentPost");
async function timingSafeEqualString(left, right) {
  const [leftDigest, rightDigest] = await Promise.all([
    crypto.subtle.digest("SHA-256", new TextEncoder().encode(String(left || ""))),
    crypto.subtle.digest("SHA-256", new TextEncoder().encode(String(right || "")))
  ]);
  const leftBytes = new Uint8Array(leftDigest);
  const rightBytes = new Uint8Array(rightDigest);
  let difference = leftBytes.length ^ rightBytes.length;
  for (let index = 0; index < Math.max(leftBytes.length, rightBytes.length); index += 1) {
    difference |= (leftBytes[index] || 0) ^ (rightBytes[index] || 0);
  }
  return difference === 0;
}
__name(timingSafeEqualString, "timingSafeEqualString");
function bytesToBase64Url(bytes) {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}
__name(bytesToBase64Url, "bytesToBase64Url");
function utf8ToBase64Url(value) {
  return bytesToBase64Url(new TextEncoder().encode(String(value)));
}
__name(utf8ToBase64Url, "utf8ToBase64Url");
function base64UrlToUtf8(value) {
  const normalized = String(value || "").replace(/-/g, "+").replace(/_/g, "/");
  const padded = `${normalized}${"=".repeat((4 - normalized.length % 4) % 4)}`;
  const bytes = Uint8Array.from(atob(padded), (char) => char.charCodeAt(0));
  return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
}
__name(base64UrlToUtf8, "base64UrlToUtf8");
function randomBase64Url(byteLength = 16) {
  const bytes = new Uint8Array(byteLength);
  crypto.getRandomValues(bytes);
  return bytesToBase64Url(bytes);
}
__name(randomBase64Url, "randomBase64Url");
function normalizeBrowserCapabilityEnforcementMode(env) {
  const mode = String(env?.[BROWSER_CAPABILITY_ENFORCEMENT_ENV] || "enforce").trim().toLowerCase();
  return mode === "shadow" ? "shadow" : "enforce";
}
__name(normalizeBrowserCapabilityEnforcementMode, "normalizeBrowserCapabilityEnforcementMode");
async function signBrowserCapability(secret, signingInput) {
  return bytesToBase64Url(await hmacSha256Bytes(secret, signingInput));
}
__name(signBrowserCapability, "signBrowserCapability");
function isInternalHandoffTransportKey(rawKey, stripAttributionTransport = true) {
  const normalizedKey = canonicalQueryParamName(rawKey).toLowerCase();
  return normalizedKey === INTERNAL_HANDOFF_QUERY_PARAM
    || (stripAttributionTransport && INTERNAL_HANDOFF_TRANSPORT_QUERY_PARAMS.includes(normalizedKey));
}
__name(isInternalHandoffTransportKey, "isInternalHandoffTransportKey");
function serializeInternalHandoffNestedUrl(parsed, raw) {
  if (/^[a-z][a-z0-9+.-]*:/i.test(raw)) return parsed.toString();
  if (raw.startsWith("//")) return `//${parsed.host}${parsed.pathname}${parsed.search}${parsed.hash}`;
  if (raw.startsWith("?")) return `${parsed.search}${parsed.hash}`;
  if (raw.startsWith("/")) return `${parsed.pathname}${parsed.search}${parsed.hash}`;
  if (raw.startsWith("./")) return `./${parsed.pathname.replace(/^\//, "")}${parsed.search}${parsed.hash}`;
  return `${parsed.pathname.replace(/^\//, "")}${parsed.search}${parsed.hash}`;
}
__name(serializeInternalHandoffNestedUrl, "serializeInternalHandoffNestedUrl");
function stripInternalHandoffTransportFromHash(parsed, stripAttributionTransport = true, depth = 0) {
  const rawHash = String(parsed?.hash || "");
  if (!rawHash || !rawHash.includes("=")) return false;
  const fragment = rawHash.slice(1);
  const queryOffset = fragment.indexOf("?");
  const prefix = queryOffset >= 0 ? fragment.slice(0, queryOffset) : "";
  const rawQuery = queryOffset >= 0 ? fragment.slice(queryOffset + 1) : fragment;
  const params = new URLSearchParams(rawQuery);
  const changed = stripInternalHandoffTransportFromSearchParams(params, stripAttributionTransport, depth + 1);
  if (!changed) return false;
  const query = params.toString();
  parsed.hash = prefix
    ? `#${prefix}${query ? `?${query}` : ""}`
    : query ? `#${query}` : "";
  return true;
}
__name(stripInternalHandoffTransportFromHash, "stripInternalHandoffTransportFromHash");
function sanitizeInternalHandoffNestedValue(value, stripAttributionTransport = true, depth = 0) {
  const raw = String(value || "").trim();
  if (!raw) return { changed: false, value: raw };
  if (depth > 6) return { changed: true, value: "" };
  try {
    const queryOnly = raw.startsWith("?");
    const parsed = queryOnly
      ? new URL(`https://app.eden.health/${raw}`)
      : new URL(raw, "https://app.eden.health");
    const searchChanged = stripInternalHandoffTransportFromSearchParams(parsed.searchParams, stripAttributionTransport, depth + 1);
    const hashChanged = stripInternalHandoffTransportFromHash(parsed, stripAttributionTransport, depth + 1);
    if (!searchChanged && !hashChanged) return { changed: false, value: raw };
    return { changed: true, value: serializeInternalHandoffNestedUrl(parsed, raw) };
  } catch {
    return { changed: false, value: raw };
  }
}
__name(sanitizeInternalHandoffNestedValue, "sanitizeInternalHandoffNestedValue");
function stripInternalHandoffTransportFromSearchParams(params, stripAttributionTransport = true, depth = 0) {
  if (!params) return false;
  let changed = false;
  for (const rawKey of [...new Set(params.keys())]) {
    if (isInternalHandoffTransportKey(rawKey, stripAttributionTransport)) {
      params.delete(rawKey);
      changed = true;
      continue;
    }
    const normalizedKey = canonicalQueryParamName(rawKey).toLowerCase();
    if (!QUERY_PARAM_NESTED_CONTAINER_KEYS.includes(normalizedKey)) continue;
    const nestedValues = params.getAll(rawKey).map((value) => sanitizeInternalHandoffNestedValue(value, stripAttributionTransport, depth + 1));
    if (!nestedValues.some((nested) => nested.changed)) continue;
    params.delete(rawKey);
    for (const nested of nestedValues) if (nested.value) params.append(rawKey, nested.value);
    changed = true;
  }
  return changed;
}
__name(stripInternalHandoffTransportFromSearchParams, "stripInternalHandoffTransportFromSearchParams");
function normalizeInternalHandoffDestination(value) {
  try {
    const destination = value instanceof URL ? new URL(value.toString()) : new URL(String(value || ""));
    if (destination.protocol !== "https:" || destination.hostname !== "app.eden.health") return null;
    if (destination.pathname !== "/intake" && !destination.pathname.startsWith("/intake/")) return null;
    destination.hash = "";
    // Attribution transport is excluded from the destination fingerprint so a
    // signed owner/pointer assertion selects the already durable click object
    // rather than trusting a carried query. The raw Google/UTM values still
    // remain on the first HealthOS request for native observation; only the
    // opaque assertion itself is removed before the origin.
    stripInternalHandoffTransportFromSearchParams(destination.searchParams, true);
    const entries = [...destination.searchParams.entries()].sort(([leftKey, leftValue], [rightKey, rightValue]) => {
      const byKey = leftKey.localeCompare(rightKey);
      return byKey || leftValue.localeCompare(rightValue);
    });
    destination.search = "";
    for (const [key, entryValue] of entries) destination.searchParams.append(key, entryValue);
    return destination;
  } catch {
    return null;
  }
}
__name(normalizeInternalHandoffDestination, "normalizeInternalHandoffDestination");
async function internalHandoffDestinationSha256(value) {
  const destination = normalizeInternalHandoffDestination(value);
  if (!destination) return null;
  return await sha256Raw(`${destination.origin}${destination.pathname}${destination.search}`);
}
__name(internalHandoffDestinationSha256, "internalHandoffDestinationSha256");
function internalHandoffTransportValues(value, attributionSources = []) {
  try {
    const destination = value instanceof URL ? new URL(value.toString()) : new URL(String(value || ""));
    const urlEvidence = extractCanonicalUrlParamEvidence(destination, INTERNAL_HANDOFF_TRANSPORT_QUERY_PARAMS, {
      includeNested: true,
      includeHash: true
    });
    if (Object.keys(urlEvidence.conflicts).length > 0) return null;
    const urlTransport = urlEvidence.values;
    const valuesByKey = /* @__PURE__ */ new Map();
    const wanted = /* @__PURE__ */ new Set(INTERNAL_HANDOFF_TRANSPORT_QUERY_PARAMS.map((key) => canonicalQueryParamName(key)));
    const add = /* @__PURE__ */ __name((rawKey, rawValue) => {
      if (rawValue === void 0 || rawValue === null || typeof rawValue === "object") return;
      const key = canonicalQueryParamName(rawKey);
      const entryValue = String(rawValue);
      if (!wanted.has(key) || !entryValue) return;
      if (!valuesByKey.has(key)) valuesByKey.set(key, /* @__PURE__ */ new Set());
      valuesByKey.get(key).add(entryValue);
    }, "addInternalHandoffTransportValue");
    for (const [key, entryValue] of Object.entries(urlTransport)) add(key, entryValue);
    const sourcePending = (Array.isArray(attributionSources) ? attributionSources : [attributionSources])
      .filter((source) => source && typeof source === "object")
      .map((source) => ({ source, depth: 0 }));
    const sourceSeen = /* @__PURE__ */ new Set();
    while (sourcePending.length) {
      const { source, depth } = sourcePending.pop();
      if (!source || typeof source !== "object" || sourceSeen.has(source) || depth > 6) continue;
      sourceSeen.add(source);
      if (Array.isArray(source)) {
        for (const entry of source) if (entry && typeof entry === "object") sourcePending.push({ source: entry, depth: depth + 1 });
        continue;
      }
      for (const [rawKey, entryValue] of Object.entries(source)) {
        add(rawKey, entryValue);
        if (entryValue && typeof entryValue === "object") sourcePending.push({ source: entryValue, depth: depth + 1 });
      }
    }
    if ([...valuesByKey.values()].some((values) => values.size > 1)) return null;
    return Object.fromEntries([...valuesByKey.entries()].map(([key, values]) => [key, [...values][0]]));
  } catch {
    return null;
  }
}
__name(internalHandoffTransportValues, "internalHandoffTransportValues");
async function internalHandoffTransportSha256(value, attributionSources = []) {
  try {
    const transport = internalHandoffTransportValues(value, attributionSources);
    if (!transport) return null;
    return await internalHandoffTransportSha256FromValues(transport);
  } catch {
    return null;
  }
}
__name(internalHandoffTransportSha256, "internalHandoffTransportSha256");
async function internalHandoffTransportSha256FromValues(transport = {}) {
  const entries = Object.entries(transport)
    .sort(([leftKey, leftValue], [rightKey, rightValue]) => {
      const byKey = leftKey.localeCompare(rightKey);
      return byKey || String(leftValue).localeCompare(String(rightValue));
    });
  return entries.length ? await sha256Raw(JSON.stringify(entries)) : null;
}
__name(internalHandoffTransportSha256FromValues, "internalHandoffTransportSha256FromValues");
async function internalHandoffClickTransportHashes(value, attributionSources = []) {
  const transport = internalHandoffTransportValues(value, attributionSources);
  if (!transport) return null;
  return await internalHandoffClickTransportHashesFromValues(transport);
}
__name(internalHandoffClickTransportHashes, "internalHandoffClickTransportHashes");
async function internalHandoffClickTransportHashesFromValues(transport = {}) {
  const clickKeys = /* @__PURE__ */ new Set(CLICK_ID_PARAMS.map((key) => canonicalQueryParamName(key)));
  const out = {};
  for (const [key, entryValue] of Object.entries(transport)) {
    if (!clickKeys.has(key)) continue;
    out[key] = await sha256Raw(`eden_internal_handoff_click_v1\0${key}\0${entryValue}`);
  }
  return out;
}
__name(internalHandoffClickTransportHashesFromValues, "internalHandoffClickTransportHashesFromValues");
async function validateLegacyInternalHandoffTransport(transportValues = {}, pointerRecord = null) {
  // A v1 assertion authenticated only the owner, pointer, and normalized intake
  // destination. Google/UTM/click transport was intentionally removed from that
  // destination hash, so accepting arbitrary transport beside a valid v1 token
  // would let a changed paid touch masquerade as the pointer's original click.
  //
  // Preserve the narrow compatibility window only for a clean destination or a
  // destination carrying exactly the pointer's verified primary click id. Any
  // additional or unverifiable transport remains on the request and is captured
  // as event-native evidence instead of being suppressed as an internal handoff.
  const entries = Object.entries(transportValues || {})
    .map(([rawKey, rawValue]) => [canonicalQueryParamName(rawKey), evidenceValue(rawValue, canonicalQueryParamName(rawKey))])
    .filter(([, rawValue]) => !!rawValue);
  if (entries.length === 0) return { valid: true, reason: "legacy_internal_handoff_clean_destination" };
  const pointerType = canonicalQueryParamName(evidenceValue(pointerRecord?.primary_click_id_type) || "");
  const pointerHash = String(pointerRecord?.raw_primary_click_id_sha256 || "").toLowerCase();
  if (!UPLOAD_GRADE_GOOGLE_CLICK_ID_PARAMS.has(pointerType) || !/^[a-f0-9]{64}$/.test(pointerHash)) {
    return { valid: false, reason: "internal_handoff_legacy_transport_pointer_proof_missing" };
  }
  if (entries.length !== 1 || entries[0][0] !== pointerType) {
    return { valid: false, reason: "internal_handoff_legacy_transport_unverifiable" };
  }
  const observedHash = await sha256Raw(entries[0][1]);
  if (!await timingSafeEqualString(pointerHash, observedHash)) {
    return { valid: false, reason: "internal_handoff_legacy_transport_mismatch" };
  }
  return { valid: true, reason: "legacy_internal_handoff_primary_click_verified" };
}
__name(validateLegacyInternalHandoffTransport, "validateLegacyInternalHandoffTransport");
async function mintInternalHandoffAssertion(env, { adClickId, anonId, session, destinationUrl }, nowSeconds = Math.floor(Date.now() / 1e3)) {
  const secret = String(env?.[BROWSER_CAPABILITY_SECRET_ENV] || "");
  if (!secret) throw new Error("internal_handoff_hmac_secret_missing");
  const normalizedAdClickId = normalizeAdClickPointerId(adClickId);
  const transportValues = internalHandoffTransportValues(destinationUrl);
  if (!transportValues) throw new Error("internal_handoff_transport_ambiguous");
  const [destinationSha256, transportSha256, transportClickHashes] = await Promise.all([
    internalHandoffDestinationSha256(destinationUrl),
    internalHandoffTransportSha256FromValues(transportValues),
    internalHandoffClickTransportHashesFromValues(transportValues)
  ]);
  const owner = await currentPointerOwnerContext({ anonId, session });
  if (!normalizedAdClickId || !destinationSha256 || !owner.anonymous_id_sha256 || !owner.session_id_sha256) {
    throw new Error("internal_handoff_claims_incomplete");
  }
  const payload = {
    v: INTERNAL_HANDOFF_ASSERTION_VERSION,
    aud: INTERNAL_HANDOFF_AUDIENCE,
    iat: nowSeconds,
    exp: nowSeconds + INTERNAL_HANDOFF_TTL_SECONDS,
    jti: randomBase64Url(16),
    ptr: normalizedAdClickId,
    a: owner.anonymous_id_sha256,
    s: owner.session_id_sha256,
    dst: destinationSha256,
    trn: transportSha256,
    trc: transportClickHashes
  };
  const encoded = utf8ToBase64Url(JSON.stringify(payload));
  const signingInput = `h1.${encoded}`;
  return `${signingInput}.${await signBrowserCapability(secret, signingInput)}`;
}
__name(mintInternalHandoffAssertion, "mintInternalHandoffAssertion");
async function verifyInternalHandoffAssertion({ env, request, destinationUrl = null, transportAttributionSources = [], anonId = null, session = null, userId = null, orderId = null, allowContinuationCookie = false }, nowSeconds = Math.floor(Date.now() / 1e3)) {
  // Keep the raw destination for the transported attribution fingerprint. The
  // normalized destination intentionally strips Google/UTM transport before it
  // is hashed for the exact-route claim, so hashing only that normalized URL
  // would make every continuation fingerprint empty.
  const rawDestination = destinationUrl || request.url;
  const destination = normalizeInternalHandoffDestination(rawDestination);
  if (!destination) return { valid: false, reason: "internal_handoff_destination_invalid" };
  const queryToken = getCanonicalUrlParam(new URL(String(rawDestination)), INTERNAL_HANDOFF_QUERY_PARAM, { includeNested: false, includeHash: false });
  const cookieToken = allowContinuationCookie ? readCookie(request, INTERNAL_HANDOFF_COOKIE_NAME) : null;
  const token = queryToken || cookieToken;
  const transport = queryToken ? "query_assertion" : cookieToken ? "continuation_cookie" : null;
  if (!token || token.length > INTERNAL_HANDOFF_MAX_BYTES) return { valid: false, reason: "internal_handoff_assertion_missing_or_oversized" };
  const parts = String(token).split(".");
  if (parts.length !== 3 || parts[0] !== "h1") return { valid: false, reason: "internal_handoff_assertion_format_invalid" };
  const signingInput = `${parts[0]}.${parts[1]}`;
  const currentSecret = String(env?.[BROWSER_CAPABILITY_SECRET_ENV] || "");
  if (!currentSecret) return { valid: false, configurationError: true, reason: "internal_handoff_hmac_secret_missing" };
  const candidateSecrets = [
    ["current", currentSecret],
    ["previous", String(env?.[BROWSER_CAPABILITY_PREVIOUS_SECRET_ENV] || "")]
  ].filter(([, secret]) => !!secret);
  let verifiedBy = null;
  for (const [name, secret] of candidateSecrets) {
    const expected = await signBrowserCapability(secret, signingInput);
    if (await timingSafeEqualString(parts[2], expected)) {
      verifiedBy = name;
      break;
    }
  }
  if (!verifiedBy) return { valid: false, reason: "internal_handoff_assertion_signature_invalid" };
  let payload;
  try {
    payload = JSON.parse(base64UrlToUtf8(parts[1]));
  } catch {
    return { valid: false, reason: "internal_handoff_assertion_payload_invalid" };
  }
  if (!payload || ![1, INTERNAL_HANDOFF_ASSERTION_VERSION].includes(payload.v) || payload.aud !== INTERNAL_HANDOFF_AUDIENCE) {
    return { valid: false, reason: "internal_handoff_assertion_claims_invalid" };
  }
  const assertionVersion = payload.v;
  const hasV2TransportClaims = assertionVersion === INTERNAL_HANDOFF_ASSERTION_VERSION;
  if (hasV2TransportClaims) {
    if (payload.trn !== null && payload.trn !== void 0 && !/^[a-f0-9]{64}$/.test(String(payload.trn))) {
      return { valid: false, reason: "internal_handoff_transport_fingerprint_invalid" };
    }
    if (!payload.trc || typeof payload.trc !== "object" || Array.isArray(payload.trc)
      || Object.keys(payload.trc).length > CLICK_ID_PARAMS.length
      || Object.entries(payload.trc).some(([key, hash]) => !CLICK_ID_PARAMS.map((entry) => canonicalQueryParamName(entry)).includes(canonicalQueryParamName(key)) || !/^[a-f0-9]{64}$/.test(String(hash)))) {
      return { valid: false, reason: "internal_handoff_click_transport_claim_invalid" };
    }
  }
  if (!Number.isInteger(payload.iat) || !Number.isInteger(payload.exp)) return { valid: false, reason: "internal_handoff_assertion_time_invalid" };
  if (payload.iat > nowSeconds + BROWSER_CAPABILITY_CLOCK_SKEW_SECONDS) return { valid: false, reason: "internal_handoff_assertion_issued_in_future" };
  if (payload.exp < nowSeconds - BROWSER_CAPABILITY_CLOCK_SKEW_SECONDS || payload.exp - payload.iat > INTERNAL_HANDOFF_TTL_SECONDS) {
    return { valid: false, reason: "internal_handoff_assertion_expired_or_ttl_invalid" };
  }
  if (!/^[A-Za-z0-9_-]{16,128}$/.test(String(payload.jti || ""))) return { valid: false, reason: "internal_handoff_assertion_jti_invalid" };
  const pointerId = normalizeAdClickPointerId(payload.ptr);
  const requestPointerId = readAdClickPointerCookie(request);
  if (!pointerId || pointerId !== requestPointerId) return { valid: false, reason: "internal_handoff_pointer_mismatch" };
  const transportValues = internalHandoffTransportValues(rawDestination, transportAttributionSources);
  if (transportValues === null) {
    return { valid: false, reason: "internal_handoff_transport_ambiguous" };
  }
  const [owner, destinationSha256, record] = await Promise.all([
    currentPointerOwnerContext({ anonId, session, userId, orderId }),
    internalHandoffDestinationSha256(destination),
    readCanonicalAdClickPointerRecord(env, pointerId)
  ]);
  if (!owner.anonymous_id_sha256 || !owner.session_id_sha256 || payload.a !== owner.anonymous_id_sha256 || payload.s !== owner.session_id_sha256) {
    return { valid: false, reason: "internal_handoff_owner_mismatch" };
  }
  const exactDestinationMatch = !!destinationSha256 && payload.dst === destinationSha256;
  const transportSha256 = hasV2TransportClaims ? await internalHandoffTransportSha256FromValues(transportValues) : null;
  const transportClickHashes = hasV2TransportClaims ? await internalHandoffClickTransportHashesFromValues(transportValues) : {};
  const exactTransportMatch = hasV2TransportClaims && (
    payload.trn === null && transportSha256 === null
    || !!payload.trn && !!transportSha256 && await timingSafeEqualString(payload.trn, transportSha256)
  );
  const observedClickTransportEntries = Object.entries(transportClickHashes || {});
  const partialClickTransportMatch = hasV2TransportClaims && observedClickTransportEntries.length > 0
    && (await Promise.all(observedClickTransportEntries.map(async ([key, hash]) => {
      const claimedHash = payload.trc[canonicalQueryParamName(key)];
      return !!claimedHash && await timingSafeEqualString(claimedHash, hash);
    }))).every(Boolean);
  if (transport === "query_assertion" && hasV2TransportClaims && !exactTransportMatch) {
    return { valid: false, reason: "internal_handoff_transport_mismatch" };
  }
  if (transport === "continuation_cookie" && hasV2TransportClaims && observedClickTransportEntries.length > 0 && !exactTransportMatch && !partialClickTransportMatch) {
    return { valid: false, reason: "internal_handoff_transport_mismatch" };
  }
  const transportedRouteContinuation = allowContinuationCookie
    && transport === "continuation_cookie"
    && hasV2TransportClaims
    && (exactTransportMatch || partialClickTransportMatch);
  if (!exactDestinationMatch && !transportedRouteContinuation) {
    return { valid: false, reason: "internal_handoff_destination_mismatch" };
  }
  if (!record) {
    return { valid: false, reason: "internal_handoff_pointer_record_missing" };
  }
  const ownership = validateAdClickPointerOwnership(record, pointerId, owner);
  if (!ownership.valid) return { valid: false, reason: ownership.reason || "internal_handoff_pointer_owner_invalid" };
  if (!hasV2TransportClaims) {
    const legacyTransport = await validateLegacyInternalHandoffTransport(transportValues, record);
    if (!legacyTransport.valid) return { valid: false, reason: legacyTransport.reason };
  }
  return {
    valid: true,
    verifiedBy,
    pointerId,
    pointerRecord: record,
    pointerRecordLag: false,
    owner,
    destination,
    token,
    transport,
    assertionVersion,
    exactDestinationMatch,
    transportedRouteContinuation,
    exp: payload.exp,
    reason: transportedRouteContinuation
      ? "signed_internal_handoff_transport_continuation_verified"
      : "signed_internal_handoff_verified"
  };
}
__name(verifyInternalHandoffAssertion, "verifyInternalHandoffAssertion");
function buildInternalHandoffContinuationCookie(token, expiresAtSeconds) {
  const nowSeconds = Math.floor(Date.now() / 1e3);
  const maxAge = Math.max(1, Math.min(INTERNAL_HANDOFF_TTL_SECONDS, Number(expiresAtSeconds || nowSeconds) - nowSeconds));
  return [
    `${INTERNAL_HANDOFF_COOKIE_NAME}=${encodeURIComponent(String(token || ""))}`,
    `Max-Age=${maxAge}`,
    "Domain=.eden.health",
    "Path=/",
    "HttpOnly",
    "Secure",
    "SameSite=Strict"
  ].join("; ");
}
__name(buildInternalHandoffContinuationCookie, "buildInternalHandoffContinuationCookie");
function requestWithoutInternalHandoffTransport(request, stripAttributionTransport = false) {
  try {
    const clean = new URL(request.url);
    const changed = stripInternalHandoffTransportFromSearchParams(clean.searchParams, stripAttributionTransport);
    return changed ? new Request(clean.toString(), request) : request;
  } catch {
    return request;
  }
}
__name(requestWithoutInternalHandoffTransport, "requestWithoutInternalHandoffTransport");
function requestForOrigin(request, stripAttributionTransport = false) {
  // Capability and handoff credentials authenticate only the edge. They must
  // never reach Webflow/HealthOS origin logs, application telemetry, or a
  // downstream cache. Sanitize every origin path, including bot, synthetic,
  // static, error-fallback, and ordinary page requests.
  const withoutTransport = requestWithoutInternalHandoffTransport(request, stripAttributionTransport);
  const headers = new Headers(withoutTransport.headers);
  const cookie = headers.get("Cookie") || "";
  if (cookie) {
    const internalNames = /* @__PURE__ */ new Set([
      BROWSER_CAPABILITY_COOKIE_NAME,
      INTERNAL_HANDOFF_COOKIE_NAME
    ]);
    const sanitizedCookie = cookie.split(";").map((part) => part.trim()).filter(Boolean).filter((part) => {
      const separator = part.indexOf("=");
      const name = (separator === -1 ? part : part.slice(0, separator)).trim();
      return !internalNames.has(name);
    }).join("; ");
    if (sanitizedCookie) headers.set("Cookie", sanitizedCookie);
    else headers.delete("Cookie");
  }
  for (const name of [
    "X-Eden-Server-Secret",
    "X-Eden-Tracking-Enrichment-Canary",
    "X-Eden-Internal-Handoff"
  ]) headers.delete(name);
  return new Request(withoutTransport, { headers });
}
__name(requestForOrigin, "requestForOrigin");
function browserCapabilityHost(value) {
  const host = String(value || "").trim().toLowerCase();
  if (!/^[a-z0-9.-]{1,253}$/.test(host)) return null;
  return ALLOWED_ORIGINS.some((origin) => {
    try {
      return new URL(origin).hostname.toLowerCase() === host;
    } catch {
      return false;
    }
  }) ? host : null;
}
__name(browserCapabilityHost, "browserCapabilityHost");
function browserCapabilityOriginHost(request) {
  const origin = request?.headers?.get("Origin") || "";
  if (!origin || !isAllowedOrigin(origin)) return null;
  try {
    return browserCapabilityHost(new URL(origin).hostname);
  } catch {
    return null;
  }
}
__name(browserCapabilityOriginHost, "browserCapabilityOriginHost");
async function browserCapabilityOwnerBinding({ request = null, anonId = null, session = null, browserHost = null } = {}) {
  const normalizedAnonId = boundedStableIdentifier(anonId || (request ? readCanonicalAnonymousId(request) : null), 256);
  const normalizedSession = parseSessionValue(session || (request ? readCookie(request, "eden_session_id") : null));
  const normalizedBrowserHost = browserCapabilityHost(browserHost || (request ? browserCapabilityOriginHost(request) : null));
  if (!normalizedAnonId || !normalizedSession || !normalizedBrowserHost) return null;
  return {
    browserHost: normalizedBrowserHost,
    anonymousIdSha256: await sha256Raw(normalizedAnonId),
    sessionIdSha256: await sha256Raw(normalizedSession.raw)
  };
}
__name(browserCapabilityOwnerBinding, "browserCapabilityOwnerBinding");
async function mintBrowserCapability(env, bindingInput, nowSeconds = Math.floor(Date.now() / 1e3)) {
  const secret = String(env?.[BROWSER_CAPABILITY_SECRET_ENV] || "");
  if (!secret) throw new Error("browser_capability_hmac_secret_missing");
  const binding = await browserCapabilityOwnerBinding(bindingInput);
  if (!binding) throw new Error("browser_capability_owner_binding_invalid");
  const payload = {
    v: BROWSER_CAPABILITY_VERSION,
    aud: BROWSER_CAPABILITY_AUDIENCE,
    iat: nowSeconds,
    exp: nowSeconds + BROWSER_CAPABILITY_TTL_SECONDS,
    jti: randomBase64Url(16),
    scp: ["collect", "identify", "preserve"],
    bh: binding.browserHost,
    ch: BROWSER_CAPABILITY_COLLECTOR_HOST,
    ah: binding.anonymousIdSha256,
    sh: binding.sessionIdSha256
  };
  const encoded = utf8ToBase64Url(JSON.stringify(payload));
  const signingInput = `v${BROWSER_CAPABILITY_VERSION}.${encoded}`;
  return `${signingInput}.${await signBrowserCapability(secret, signingInput)}`;
}
__name(mintBrowserCapability, "mintBrowserCapability");
async function verifyBrowserCapability(env, token, requiredScope, request, nowSeconds = Math.floor(Date.now() / 1e3)) {
  const currentSecret = String(env?.[BROWSER_CAPABILITY_SECRET_ENV] || "");
  if (!currentSecret) return { valid: false, configurationError: true, reason: "browser_capability_hmac_secret_missing" };
  const raw = String(token || "");
  if (!raw || raw.length > BROWSER_CAPABILITY_MAX_BYTES) return { valid: false, reason: "browser_capability_missing_or_oversized" };
  const parts = raw.split(".");
  if (parts.length !== 3 || parts[0] !== `v${BROWSER_CAPABILITY_VERSION}`) return { valid: false, reason: "browser_capability_format_invalid" };
  const signingInput = `${parts[0]}.${parts[1]}`;
  const candidateSecrets = [
    ["current", currentSecret],
    ["previous", String(env?.[BROWSER_CAPABILITY_PREVIOUS_SECRET_ENV] || "")]
  ].filter(([, secret]) => !!secret);
  let verifiedBy = null;
  for (const [name, secret] of candidateSecrets) {
    const expected = await signBrowserCapability(secret, signingInput);
    if (await timingSafeEqualString(parts[2], expected)) {
      verifiedBy = name;
      break;
    }
  }
  if (!verifiedBy) return { valid: false, reason: "browser_capability_signature_invalid" };
  let payload;
  try {
    payload = JSON.parse(base64UrlToUtf8(parts[1]));
  } catch {
    return { valid: false, reason: "browser_capability_payload_invalid" };
  }
  if (!payload || payload.v !== BROWSER_CAPABILITY_VERSION || payload.aud !== BROWSER_CAPABILITY_AUDIENCE) return { valid: false, reason: "browser_capability_claims_invalid" };
  if (!Number.isInteger(payload.iat) || !Number.isInteger(payload.exp)) return { valid: false, reason: "browser_capability_time_invalid" };
  if (payload.iat > nowSeconds + BROWSER_CAPABILITY_CLOCK_SKEW_SECONDS) return { valid: false, reason: "browser_capability_issued_in_future" };
  if (payload.exp < nowSeconds - BROWSER_CAPABILITY_CLOCK_SKEW_SECONDS || payload.exp - payload.iat > BROWSER_CAPABILITY_TTL_SECONDS) return { valid: false, reason: "browser_capability_expired_or_ttl_invalid" };
  if (!Array.isArray(payload.scp) || !payload.scp.includes(requiredScope)) return { valid: false, reason: "browser_capability_scope_missing" };
  if (!/^[A-Za-z0-9_-]{16,128}$/.test(String(payload.jti || ""))) return { valid: false, reason: "browser_capability_jti_invalid" };
  if (!/^[a-f0-9]{64}$/.test(String(payload.ah || "")) || !/^[a-f0-9]{64}$/.test(String(payload.sh || ""))) {
    return { valid: false, reason: "browser_capability_owner_hash_invalid" };
  }
  const requestHost = (() => {
    try {
      return new URL(request.url).hostname.toLowerCase();
    } catch {
      return null;
    }
  })();
  const originHost = browserCapabilityOriginHost(request);
  if (!browserCapabilityHost(payload.bh) || payload.ch !== BROWSER_CAPABILITY_COLLECTOR_HOST) {
    return { valid: false, reason: "browser_capability_host_claim_invalid" };
  }
  if (!originHost || originHost !== payload.bh) return { valid: false, reason: "browser_capability_browser_host_mismatch" };
  if (![payload.bh, payload.ch].includes(requestHost)) return { valid: false, reason: "browser_capability_collector_host_mismatch" };
  const binding = await browserCapabilityOwnerBinding({ request, browserHost: originHost });
  if (!binding) return { valid: false, reason: "browser_capability_owner_binding_missing" };
  if (!await timingSafeEqualString(payload.ah, binding.anonymousIdSha256)) return { valid: false, reason: "browser_capability_anonymous_owner_mismatch" };
  if (!await timingSafeEqualString(payload.sh, binding.sessionIdSha256)) return { valid: false, reason: "browser_capability_session_owner_mismatch" };
  return { valid: true, verifiedBy, exp: payload.exp };
}
__name(verifyBrowserCapability, "verifyBrowserCapability");
function buildBrowserCapabilityCookie(token) {
  // Domain scope is temporary compatibility for the live cross-subdomain
  // collect.eden.health collector. The target contract is a host-only __Host-
  // cookie after every browser collector is same-origin.
  return [
    `${BROWSER_CAPABILITY_COOKIE_NAME}=${encodeURIComponent(token)}`,
    `Max-Age=${BROWSER_CAPABILITY_TTL_SECONDS}`,
    "Domain=.eden.health",
    "Path=/",
    "HttpOnly",
    "Secure",
    "SameSite=Strict"
  ].join("; ");
}
__name(buildBrowserCapabilityCookie, "buildBrowserCapabilityCookie");
async function handleBrowserCapabilityBootstrap(request, env) {
  const requestUrl = new URL(request.url);
  const origin = request.headers.get("Origin") || "";
  if (origin && !isAllowedOrigin(origin)) {
    return new Response("Forbidden", { status: 403, headers: { "Cache-Control": "no-store" } });
  }
  const fetchSite = String(request.headers.get("Sec-Fetch-Site") || "").toLowerCase();
  const fetchMode = String(request.headers.get("Sec-Fetch-Mode") || "").toLowerCase();
  const fetchDest = String(request.headers.get("Sec-Fetch-Dest") || "").toLowerCase();
  if (!["same-origin", "same-site"].includes(fetchSite) || fetchMode !== "cors" || fetchDest !== "empty") {
    return new Response("Forbidden", { status: 403, headers: { "Cache-Control": "no-store" } });
  }
  const referer = request.headers.get("Referer") || "";
  let browserHost = null;
  try {
    const parsed = new URL(referer);
    browserHost = browserCapabilityHost(parsed.hostname);
    if (!browserHost || parsed.origin !== requestUrl.origin) {
      return new Response("Forbidden", { status: 403, headers: { "Cache-Control": "no-store" } });
    }
    if (origin && new URL(origin).hostname.toLowerCase() !== browserHost) {
      return new Response("Forbidden", { status: 403, headers: { "Cache-Control": "no-store" } });
    }
  } catch {
    return new Response("Forbidden", { status: 403, headers: { "Cache-Control": "no-store" } });
  }
  // The bootstrap endpoint is same-site, same-origin-referer guarded and mints
  // only Worker-owned anonymous/session state. It never accepts body-supplied
  // ownership or stable person identity. Creating missing first-party browser
  // state here removes the collector-first / expired-session deadlock without
  // expanding browser business authority.
  const rawCanonicalAnonymousId = readCookie(request, "eden_anonymous_id");
  const rawLegacyAnonymousId = readCookie(request, "eden_anon_id");
  const existingAnonymousId = boundedStableIdentifier(readCanonicalAnonymousId(request), 256);
  if ((rawCanonicalAnonymousId || rawLegacyAnonymousId) && !existingAnonymousId) {
    return new Response("Browser owner invalid", { status: 409, headers: { "Cache-Control": "no-store" } });
  }
  const anonymousId = existingAnonymousId || crypto.randomUUID();
  const rawSession = readCookie(request, "eden_session_id");
  const existingSession = parseSessionValue(rawSession);
  if (rawSession && !existingSession) {
    return new Response("Browser session invalid", { status: 409, headers: { "Cache-Control": "no-store" } });
  }
  const session = existingSession || parseSessionValue(`${crypto.randomUUID()}_${Date.now()}`);
  try {
    const headers = new Headers({ "Cache-Control": "no-store" });
    headers.append("Set-Cookie", buildBrowserCapabilityCookie(await mintBrowserCapability(env, {
      anonId: anonymousId,
      session: session.raw,
      browserHost
    })));
    headers.append("Set-Cookie", buildSessionCookie(session.raw, requestUrl));
    if (!existingAnonymousId || anonymousCookieAliasesNeedSync(request)) {
      headers.append("Set-Cookie", buildAnonCookie(anonymousId, requestUrl));
      headers.append("Set-Cookie", buildLegacyAnonCookie(anonymousId, requestUrl));
    }
    return new Response(null, { status: 204, headers });
  } catch (error) {
    console.error(JSON.stringify({ worker: "eden-analytics", event: "browser_capability_bootstrap_failed", reason: String(error?.message || "unknown").slice(0, 120) }));
    return new Response("Browser authentication unavailable", { status: 503, headers: { "Cache-Control": "no-store" } });
  }
}
__name(handleBrowserCapabilityBootstrap, "handleBrowserCapabilityBootstrap");
async function refreshBrowserCapabilityOnSuccess(response, env, endpoint, request = null) {
  if (!(response instanceof Response) || response.status < 200 || response.status >= 300) return response;
  // Collector-first bootstrap already minted a capability bound to the newly
  // generated session; the incoming request cannot expose that new cookie.
  if (String(response.headers.get("Set-Cookie") || "").includes(`${BROWSER_CAPABILITY_COOKIE_NAME}=`)) {
    return response;
  }
  try {
    if (!request) throw new Error("browser_capability_refresh_request_missing");
    const browserHost = browserCapabilityOriginHost(request);
    const anonymousId = boundedStableIdentifier(readCanonicalAnonymousId(request), 256);
    const session = parseSessionValue(readCookie(request, "eden_session_id"));
    if (!browserHost || !anonymousId || !session) throw new Error("browser_capability_refresh_owner_binding_invalid");
    const headers = new Headers(response.headers);
    headers.append("Set-Cookie", buildBrowserCapabilityCookie(await mintBrowserCapability(env, {
      anonId: anonymousId,
      session: session.raw,
      browserHost
    })));
    const requestUrl = new URL(request.url);
    headers.append("Set-Cookie", buildSessionCookie(session.raw, requestUrl));
    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers
    });
  } catch (error) {
    // The request already passed authentication. Preserve the successful
    // response, but make the refresh failure visible; deployment verification
    // requires the capability secret and blocks promotion on missing cookies.
    console.error(JSON.stringify({ worker: "eden-analytics", event: "browser_capability_refresh_failed", endpoint, reason: String(error?.message || "unknown").slice(0, 120) }));
    return response;
  }
}
__name(refreshBrowserCapabilityOnSuccess, "refreshBrowserCapabilityOnSuccess");
async function authorizeServerRequest(request, env) {
  const expected = String(env?.SERVER_API_SECRET || "");
  const previous = String(env?.SERVER_API_SECRET_PREVIOUS || "");
  if (!expected) {
    console.error(JSON.stringify({ worker: "eden-analytics", event: "auth_configuration_error", endpoint: new URL(request.url).pathname }));
    return new Response("Server authentication unavailable", { status: 503 });
  }
  const provided = request.headers.get("X-Eden-Server-Secret") || "";
  const currentMatches = provided ? await timingSafeEqualString(provided, expected) : false;
  const previousMatches = provided && previous ? await timingSafeEqualString(provided, previous) : false;
  if (!currentMatches && !previousMatches) {
    return new Response("Unauthorized", { status: 401 });
  }
  return null;
}
__name(authorizeServerRequest, "authorizeServerRequest");
async function authorizeBrowserMutationRequest(request, env, requiredScope) {
  const origin = request.headers.get("Origin") || "";
  const mode = normalizeBrowserCapabilityEnforcementMode(env);
  if (!origin) {
    if (mode === "shadow") return await authorizeServerRequest(request, env);
    return new Response("Unauthorized", { status: 401, headers: { "Cache-Control": "no-store" } });
  }
  if (!isAllowedOrigin(origin)) return new Response("Forbidden", { status: 403, headers: { "Cache-Control": "no-store" } });
  if (request.headers.get("Sec-Fetch-Site") === "cross-site") return new Response("Forbidden", { status: 403, headers: { "Cache-Control": "no-store" } });
  const result = await verifyBrowserCapability(
    env,
    readCookie(request, BROWSER_CAPABILITY_COOKIE_NAME),
    requiredScope,
    request
  );
  if (result.valid) return null;
  if (result.configurationError) {
    console.error(JSON.stringify({ worker: "eden-analytics", event: "browser_capability_configuration_error", endpoint: new URL(request.url).pathname }));
    return new Response("Browser authentication unavailable", { status: 503, headers: { "Cache-Control": "no-store" } });
  }
  // Permit one same-site collector-first request only when neither a session
  // nor capability exists. handleCollect/handleIdentify creates Worker-owned
  // state and never accepts body-supplied ownership or stable identity.
  const freshCollectBootstrap = ["collect", "identify"].includes(requiredScope)
    && !readCookie(request, BROWSER_CAPABILITY_COOKIE_NAME)
    && !readCookie(request, "eden_session_id")
    && !request.headers.get("X-Eden-Server-Secret")
    && !!browserCapabilityOriginHost(request)
    && ["", "same-origin", "same-site"].includes(String(request.headers.get("Sec-Fetch-Site") || "").toLowerCase());
  if (freshCollectBootstrap) {
    console.warn(JSON.stringify({
      worker: "eden-analytics",
      event: "browser_fresh_session_bootstrap",
      endpoint: new URL(request.url).pathname,
      capability_mode: mode
    }));
    return null;
  }
  if (mode === "shadow") {
    console.warn(JSON.stringify({ worker: "eden-analytics", event: "browser_capability_shadow_miss", endpoint: new URL(request.url).pathname, reason: result.reason }));
    return null;
  }
  console.warn(JSON.stringify({
    worker: "eden-analytics",
    event: "browser_capability_rejected",
    endpoint: new URL(request.url).pathname,
    reason: result.reason
  }));
  return new Response("Unauthorized", { status: 401, headers: { "Cache-Control": "no-store" } });
}
__name(authorizeBrowserMutationRequest, "authorizeBrowserMutationRequest");
async function readBoundedRequestText(request, maxBytes = MAX_JSON_BODY_BYTES) {
  const contentLength = Number.parseInt(request.headers.get("Content-Length") || "0", 10);
  if (Number.isFinite(contentLength) && contentLength > maxBytes) return { tooLarge: true, text: null };
  if (!request.body) return { tooLarge: false, text: "" };
  const reader = request.body.getReader();
  const decoder = new TextDecoder();
  let total = 0;
  let text = "";
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > maxBytes) {
      await reader.cancel("request_body_too_large").catch(() => {});
      return { tooLarge: true, text: null };
    }
    text += decoder.decode(value, { stream: true });
  }
  text += decoder.decode();
  return { tooLarge: false, text };
}
__name(readBoundedRequestText, "readBoundedRequestText");
async function parseBoundedJsonRequest(request) {
  const contentType = String(request.headers.get("Content-Type") || "").split(";", 1)[0].trim().toLowerCase();
  if (!(contentType === "application/json" || contentType.endsWith("+json"))) {
    return { response: new Response("Content-Type must be application/json", { status: 415 }), value: null };
  }
  const body = await readBoundedRequestText(request);
  if (body.tooLarge) return { response: new Response("Payload Too Large", { status: 413 }), value: null };
  try {
    const value = JSON.parse(body.text);
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("body_not_object");
    return { response: null, value };
  } catch {
    return { response: new Response("Invalid JSON", { status: 400 }), value: null };
  }
}
__name(parseBoundedJsonRequest, "parseBoundedJsonRequest");
function stripAdvertisingQueryFromUrl(value) {
  if (!value) return value;
  try {
    const raw = String(value).trim();
    const hasExplicitScheme = /^[a-z][a-z0-9+.-]*:/i.test(raw);
    const protocolRelative = raw.startsWith("//");
    const url = new URL(raw, "https://www.eden.health");
    for (const key of [...new Set(url.searchParams.keys())]) {
      const normalized = canonicalQueryParamName(key).toLowerCase();
      if (shouldStripAdvertisingKey(normalized) || isBlockedObservationQueryKey(normalized) || ["eden_consent_handoff", "eden_consent_ads"].includes(normalized)) {
        url.searchParams.delete(key);
        continue;
      }
      if (QUERY_PARAM_NESTED_CONTAINER_KEYS.includes(normalized) && url.searchParams.getAll(key).some((entry) => nestedQueryContainsBlockedData(entry, true))) {
        url.searchParams.delete(key);
      }
    }
    url.hash = "";
    if (!hasExplicitScheme && !protocolRelative) return `${url.pathname}${url.search}` || "/";
    return url.toString();
  } catch {
    return "";
  }
}
__name(stripAdvertisingQueryFromUrl, "stripAdvertisingQueryFromUrl");
function isPersistedQueryRedactionKey(key) {
  const normalized = canonicalQueryParamName(key).toLowerCase();
  if (isBlockedObservationQueryKey(normalized)) return true;
  if (normalized.startsWith("_gac_") || normalized.startsWith("gac_")) return true;
  const exact = new Set([
    ...CLICK_ID_PARAMS,
    ...GOOGLE_CLICK_ID_BODY_PARAMS,
    ...AD_CLICK_CLASS_B_GOOGLE_PARAMS,
    "_gl"
  ].map((item) => String(item).toLowerCase()));
  return exact.has(normalized);
}
__name(isPersistedQueryRedactionKey, "isPersistedQueryRedactionKey");
function nestedQueryContainsBlockedData(value, stripAdvertising = false) {
  const queue = [{ value: String(value || ""), depth: 0 }];
  const seen = /* @__PURE__ */ new Set();
  let inspected = 0;
  while (queue.length && inspected < 256) {
    const current = queue.shift();
    if (!current?.value || current.depth > 32) continue;
    const candidates = [];
    let decoded = current.value;
    for (let decodeDepth = 0; decodeDepth < 8; decodeDepth += 1) {
      if (!decoded || seen.has(decoded)) break;
      seen.add(decoded);
      candidates.push(decoded);
      try {
        const next = decodeURIComponent(decoded);
        if (next === decoded) break;
        decoded = next;
      } catch {
        break;
      }
    }
    for (const candidate of candidates) {
      inspected += 1;
      const paramSets = [];
      try {
        const parsed = new URL(candidate, "https://www.eden.health");
        paramSets.push(parsed.searchParams);
        for (const hashParams of hashAttributionSearchParams(parsed)) paramSets.push(hashParams);
      } catch {
      }
      try {
        const query = candidate.startsWith("?") ? candidate.slice(1) : candidate.includes("?") ? candidate.split("?").pop() : candidate;
        if (query.includes("=")) paramSets.push(new URLSearchParams(query.split("#", 1)[0]));
      } catch {
      }
      for (const params of paramSets) {
        for (const [rawKey, nestedValue] of params.entries()) {
          const key = canonicalQueryParamName(rawKey).toLowerCase();
          if (isPersistedQueryRedactionKey(key) || stripAdvertising && shouldStripAdvertisingKey(key)) return true;
          if (nestedValue && QUERY_PARAM_NESTED_CONTAINER_KEYS.includes(key)) {
            queue.push({ value: nestedValue, depth: current.depth + 1 });
          }
        }
      }
    }
  }
  return false;
}
__name(nestedQueryContainsBlockedData, "nestedQueryContainsBlockedData");
function sanitizeSearchString(value, { stripAdvertising = false } = {}) {
  if (!value) return "";
  const raw = String(value).trim();
  if (!raw) return "";
  try {
    let search = raw;
    if (/^[a-z][a-z0-9+.-]*:/i.test(raw) || raw.startsWith("//")) {
      search = new URL(raw, "https://www.eden.health").search;
    }
    search = search.split("#", 1)[0].replace(/^\?/, "");
    const params = new URLSearchParams(search);
    for (const key of [...new Set(params.keys())]) {
      const normalized = canonicalQueryParamName(key).toLowerCase();
      const sensitive = isBlockedObservationQueryKey(normalized);
      const advertising = shouldStripAdvertisingKey(normalized);
      if (stripAdvertising && (advertising || sensitive)) {
        params.delete(key);
      } else if (isPersistedQueryRedactionKey(normalized)) {
        params.set(key, "[redacted]");
      } else if (QUERY_PARAM_NESTED_CONTAINER_KEYS.includes(normalized) && params.getAll(key).some((entry) => nestedQueryContainsBlockedData(entry, stripAdvertising))) {
        if (stripAdvertising) params.delete(key);
        else params.set(key, "[nested_url_redacted]");
      }
    }
    const output = params.toString();
    return output ? `?${output}` : "";
  } catch {
    return "";
  }
}
__name(sanitizeSearchString, "sanitizeSearchString");
function shouldStripAdvertisingKey(key) {
  const normalized = canonicalQueryParamName(key).trim().toLowerCase();
  if (!normalized || normalized === "attribution_suppressed") return false;
  if (normalized.startsWith("utm_") || normalized.startsWith("first_touch_") || normalized.startsWith("last_touch_") || normalized.startsWith("current_touch_")) return true;
  if (normalized.startsWith("ad_click_") || normalized === "ad_click_id") return true;
  if (normalized.startsWith("attribution_") || normalized === "acquisition_channel") return true;
  if (normalized.startsWith("_gac_") || normalized.startsWith("gac_")) return true;
  const clickKeys = new Set([...CLICK_ID_PARAMS, ...GOOGLE_CLICK_ID_BODY_PARAMS, ...AD_CLICK_CLASS_B_GOOGLE_PARAMS, "_gl"].map((item) => String(item).toLowerCase()));
  return clickKeys.has(normalized);
}
__name(shouldStripAdvertisingKey, "shouldStripAdvertisingKey");
function isPersistedUrlField(key) {
  const normalized = canonicalQueryParamName(key).trim().toLowerCase();
  return normalized.endsWith("_url") || [
    "url",
    "href",
    "page_location",
    "landing_page",
    "referrer",
    "page_referrer"
  ].includes(normalized) || QUERY_PARAM_NESTED_CONTAINER_KEYS.includes(normalized);
}
__name(isPersistedUrlField, "isPersistedUrlField");
function isPersistedSearchField(key) {
  return ["search", "page_search", "url_search", "query_string"].includes(canonicalQueryParamName(key).trim().toLowerCase());
}
__name(isPersistedSearchField, "isPersistedSearchField");
function isPersistedPathField(key) {
  return ["path", "page_path", "session_page_path", "url_path"].includes(canonicalQueryParamName(key).trim().toLowerCase());
}
__name(isPersistedPathField, "isPersistedPathField");
function sanitizePathString(value) {
  if (!value) return "";
  const raw = String(value).trim();
  if (!raw) return "";
  try {
    return new URL(raw, "https://www.eden.health").pathname || "/";
  } catch {
    return raw.split(/[?#]/, 1)[0] || "";
  }
}
__name(sanitizePathString, "sanitizePathString");
function scrubAdvertisingAttributionFromBody(value) {
  if (!value || typeof value !== "object") return value;
  // JSON request bodies are capped at 65,536 bytes. Traverse iteratively so a
  // deeply nested object cannot evade privacy scrubbing at an arbitrary depth.
  const stack = [value];
  const seen = /* @__PURE__ */ new Set();
  while (stack.length) {
    const current = stack.pop();
    if (!current || typeof current !== "object" || seen.has(current)) continue;
    seen.add(current);
    if (Array.isArray(current)) {
      for (const entry of current) if (entry && typeof entry === "object") stack.push(entry);
      continue;
    }
    for (const rawKey of Object.keys(current)) {
      const normalized = canonicalQueryParamName(rawKey).toLowerCase();
      if (shouldStripAdvertisingKey(normalized)) {
        delete current[rawKey];
        continue;
      }
      if (isPersistedUrlField(normalized) && typeof current[rawKey] === "string") {
        current[rawKey] = stripAdvertisingQueryFromUrl(current[rawKey]);
        continue;
      }
      if (isPersistedSearchField(normalized) && typeof current[rawKey] === "string") {
        current[rawKey] = sanitizeSearchString(current[rawKey], { stripAdvertising: true });
        continue;
      }
      if (isPersistedPathField(normalized) && typeof current[rawKey] === "string") {
        current[rawKey] = sanitizePathString(current[rawKey]);
        continue;
      }
      if (current[rawKey] && typeof current[rawKey] === "object") stack.push(current[rawKey]);
    }
  }
  if (value.context?.campaign && typeof value.context.campaign === "object") value.context.campaign = {};
  return value;
}
__name(scrubAdvertisingAttributionFromBody, "scrubAdvertisingAttributionFromBody");
function readCookie(request, name) {
  const m = (request.headers.get("Cookie") || "").match(new RegExp(`(?:^|;\\s*)${name}=([^;]+)`));
  if (!m) return null;
  try {
    return decodeURIComponent(m[1]);
  } catch {
    // Malformed owner/capability/pointer cookies are absence, never an
    // exception and never a partially decoded identity claim.
    return null;
  }
}
__name(readCookie, "readCookie");
function readCanonicalAnonymousId(request) {
  const canonical = readCookie(request, "eden_anonymous_id");
  const legacy = readCookie(request, "eden_anon_id");
  if (canonical && legacy && canonical !== legacy) {
    console.warn(JSON.stringify({ worker: "eden-analytics", event: "anonymous_id_alias_conflict", canonical_present: true, legacy_present: true }));
  }
  return canonical || legacy || null;
}
__name(readCanonicalAnonymousId, "readCanonicalAnonymousId");
function anonymousCookieAliasesNeedSync(request) {
  const canonical = readCookie(request, "eden_anonymous_id");
  const legacy = readCookie(request, "eden_anon_id");
  return !canonical || !legacy || canonical !== legacy;
}
__name(anonymousCookieAliasesNeedSync, "anonymousCookieAliasesNeedSync");
function cookieDomain(url) {
  const h = url.hostname;
  if (h === "localhost")
    return "localhost";
  const parts = h.split(".");
  return parts.length >= 2 ? `.${parts.slice(-2).join(".")}` : h;
}
__name(cookieDomain, "cookieDomain");
function buildAnonCookie(id, url) {
  return [
    `eden_anonymous_id=${encodeURIComponent(id)}`,
    "Max-Age=63072000",
    `Domain=${cookieDomain(url)}`,
    "Path=/",
    "HttpOnly",
    "Secure",
    "SameSite=Lax"
  ].join("; ");
}
__name(buildAnonCookie, "buildAnonCookie");
function buildLegacyAnonCookie(id, url) {
  return [
    `eden_anon_id=${encodeURIComponent(id)}`,
    "Max-Age=63072000",
    `Domain=${cookieDomain(url)}`,
    "Path=/",
    "Secure",
    "SameSite=Lax"
  ].join("; ");
}
__name(buildLegacyAnonCookie, "buildLegacyAnonCookie");
function buildSessionCookie(v, url) {
  return [
    `eden_session_id=${encodeURIComponent(v)}`,
    "Max-Age=1800",
    `Domain=${cookieDomain(url)}`,
    "Path=/",
    "Secure",
    "SameSite=Lax"
  ].join("; ");
}
__name(buildSessionCookie, "buildSessionCookie");
function clearCookie(name, url) {
  return [`${name}=`, "Max-Age=0", `Domain=${cookieDomain(url)}`, "Path=/", "Secure", "SameSite=Lax"].join("; ");
}
__name(clearCookie, "clearCookie");
function buildAdvertisingDenialMarkerCookie(url) {
  return [
    `${ATTRIBUTION_DENIAL_COOKIE_NAME}=1`,
    `Max-Age=${ATTRIBUTION_DENIAL_TTL}`,
    `Domain=${cookieDomain(url)}`,
    "Path=/",
    "HttpOnly",
    "Secure",
    "SameSite=Lax"
  ].join("; ");
}
__name(buildAdvertisingDenialMarkerCookie, "buildAdvertisingDenialMarkerCookie");
function appendAttributionRevocationCookies(headers, url) {
  for (const name of [ATTR_COOKIE_NAME, AD_CLICK_POINTER_COOKIE_NAME, INTERNAL_HANDOFF_COOKIE_NAME, "eden_click_ref", "eden_pre_auth"]) {
    headers.append("Set-Cookie", clearCookie(name, url));
  }
}
__name(appendAttributionRevocationCookies, "appendAttributionRevocationCookies");
function appendAttributionPermissionCookies(headers, url, permission) {
  if (!permission?.allowed) appendAttributionRevocationCookies(headers, url);
  if (permission?.setDenialMarker) headers.append("Set-Cookie", buildAdvertisingDenialMarkerCookie(url));
  if (permission?.clearDenialMarker) headers.append("Set-Cookie", clearCookie(ATTRIBUTION_DENIAL_COOKIE_NAME, url));
}
__name(appendAttributionPermissionCookies, "appendAttributionPermissionCookies");
function encodeAttrCookieObject(out) {
  return encodeURIComponent(JSON.stringify(out));
}
__name(encodeAttrCookieObject, "encodeAttrCookieObject");
function maybeTrimAttrCookieUrls(out) {
  const trimmed = { ...out };
  for (const field of ["landing_page", "attribution_referrer"]) {
    if (typeof trimmed[field] === "string" && trimmed[field].length > 768) {
      trimmed[field] = trimmed[field].slice(0, 768);
    }
  }
  return trimmed;
}
__name(maybeTrimAttrCookieUrls, "maybeTrimAttrCookieUrls");
function buildAttrCookieValue(attribution) {
  const out = {};
  for (const k of ATTR_COOKIE_FIELDS) {
    if (attribution[k])
      out[k] = attribution[k];
  }
  if (!Object.keys(out).length)
    return null;
  const now = Date.now();
  const hasClickId = CLICK_ID_PARAMS.some((key) => out[key]);
  const clickFirstObservedAt = Number(attribution?._click_first_observed_at || 0) || now;
  out._ts = hasClickId ? clickFirstObservedAt : now;
  if (hasClickId) out._click_first_observed_at = clickFirstObservedAt;
  out._last_seen_at = now;
  let encoded = encodeAttrCookieObject(out);
  if (encoded.length <= ATTR_COOKIE_MAX_ENCODED_BYTES)
    return encoded;
  const pruned = { ...out, _truncated: "diagnostic" };
  for (const k of ATTR_COOKIE_DIAGNOSTIC_DROP_FIELDS) delete pruned[k];
  encoded = encodeAttrCookieObject(pruned);
  if (encoded.length <= ATTR_COOKIE_MAX_ENCODED_BYTES)
    return encoded;
  for (const k of ATTR_COOKIE_SECONDARY_DROP_FIELDS) delete pruned[k];
  encoded = encodeAttrCookieObject(pruned);
  if (encoded.length <= ATTR_COOKIE_MAX_ENCODED_BYTES)
    return encoded;
  const coreOnly = {
    _ts: out._ts,
    ...out._click_first_observed_at ? { _click_first_observed_at: out._click_first_observed_at } : {},
    _last_seen_at: out._last_seen_at,
    _truncated: "core_only"
  };
  for (const k of ATTR_COOKIE_CORE_FIELDS) {
    if (out[k]) coreOnly[k] = out[k];
  }
  encoded = encodeAttrCookieObject(coreOnly);
  if (encoded.length <= ATTR_COOKIE_MAX_ENCODED_BYTES)
    return encoded;
  const urlTrimmed = maybeTrimAttrCookieUrls(coreOnly);
  encoded = encodeAttrCookieObject(urlTrimmed);
  if (encoded.length <= ATTR_COOKIE_MAX_ENCODED_BYTES)
    return encoded;
  delete urlTrimmed.landing_page;
  delete urlTrimmed.attribution_referrer;
  encoded = encodeAttrCookieObject(urlTrimmed);
  if (encoded.length <= ATTR_COOKIE_MAX_ENCODED_BYTES)
    return encoded;
  // Preserve every native Google Ads upload identifier before falling back to
  // a single primary. This keeps the browser/HealthOS bridge additive when
  // oversized campaign diagnostics, UTMs, or Google cookie context caused the
  // overflow; the complete immutable envelope is still already persisted at
  // the edge.
  const uploadIdsOnly = {
    _ts: out._ts,
    ...out._click_first_observed_at ? { _click_first_observed_at: out._click_first_observed_at } : {},
    _last_seen_at: out._last_seen_at,
    _truncated: "upload_ids_only"
  };
  for (const key of ["gclid", "gbraid", "wbraid", "dclid"]) {
    if (out[key]) uploadIdsOnly[key] = out[key];
  }
  encoded = encodeAttrCookieObject(uploadIdsOnly);
  if (encoded.length <= ATTR_COOKIE_MAX_ENCODED_BYTES)
    return encoded;
  // A maximum-size combination of multiple upload-grade identifiers can still
  // exceed the browser-safe cookie budget after every diagnostic field is gone.
  // Preserve exactly the same deterministic Google precedence used by snapshot
  // classification; never truncate an identifier into an invalid value.
  const primaryType = out.gclid ? "gclid" : out.gbraid ? "gbraid" : out.wbraid ? "wbraid" : out.dclid ? "dclid" : null;
  const primaryOnly = {
    _ts: out._ts,
    ...out._click_first_observed_at ? { _click_first_observed_at: out._click_first_observed_at } : {},
    _last_seen_at: out._last_seen_at,
    _truncated: "primary_only",
    ...primaryType ? { [primaryType]: out[primaryType] } : {}
  };
  encoded = encodeAttrCookieObject(primaryOnly);
  // Fail closed rather than ask the browser to silently reject an oversized
  // cookie. Valid governed click IDs are bounded, so this is a corruption guard.
  return encoded.length <= ATTR_COOKIE_MAX_ENCODED_BYTES ? encoded : null;
}
__name(buildAttrCookieValue, "buildAttrCookieValue");
function buildAttrCookie(encodedValue, url) {
  return [
    `${ATTR_COOKIE_NAME}=${encodedValue}`,
    `Max-Age=${ATTR_COOKIE_TTL}`,
    `Domain=${cookieDomain(url)}`,
    "Path=/",
    "Secure",
    "SameSite=Lax"
  ].join("; ");
}
__name(buildAttrCookie, "buildAttrCookie");
function nowUTC() {
  return new Date(Date.now()).toISOString();
}
__name(nowUTC, "nowUTC");
function isMobile(ua) {
  return /Mobile|Android|iPhone|iPad|IPod/i.test(ua);
}
__name(isMobile, "isMobile");
function sanitizeUrl(url) {
  try {
    const clean = new URL(url.toString());
    for (const k of [...new Set(clean.searchParams.keys())]) {
      const normalized = canonicalQueryParamName(k).toLowerCase();
      if (isBlockedObservationQueryKey(normalized))
        clean.searchParams.set(k, "[redacted]");
      else if (QUERY_PARAM_NESTED_CONTAINER_KEYS.includes(normalized) && clean.searchParams.getAll(k).some((entry) => nestedQueryContainsBlockedData(entry, false)))
        clean.searchParams.set(k, "[nested_url_redacted]");
    }
    clean.hash = "";
    return clean;
  } catch {
    return url;
  }
}
__name(sanitizeUrl, "sanitizeUrl");
function sanitizeAdClickLandingUrl(url) {
  const clean = sanitizeUrl(url);
  try {
    const redactExact = new Set([...AD_CLICK_CLASS_A_GOOGLE_PARAMS, ...AD_CLICK_DESTINATION_SPECIFIC_GOOGLE_PARAMS, ...AD_CLICK_CLASS_B_GOOGLE_PARAMS, "_gl"].map((param) => String(param).toLowerCase()));
    for (const key of [...new Set(clean.searchParams.keys())]) {
      const normalized = canonicalQueryParamName(key).toLowerCase();
      if (redactExact.has(normalized) || normalized.startsWith("_gac_") || normalized.startsWith("gac_")) {
        clean.searchParams.set(key, "[redacted]");
      } else if (QUERY_PARAM_NESTED_CONTAINER_KEYS.includes(normalized)) {
        let redactNested = false;
        for (const value of clean.searchParams.getAll(key)) {
          for (const nested of nestedAttributionSearchParams(value)) {
            for (const [nestedRawKey, nestedRawValue] of nested.entries()) {
              const nestedKey = canonicalQueryParamName(nestedRawKey).toLowerCase();
              if (nestedRawValue && (redactExact.has(nestedKey) || nestedKey.startsWith("_gac_") || nestedKey.startsWith("gac_"))) {
                redactNested = true;
                break;
              }
            }
            if (redactNested) break;
          }
          if (redactNested) break;
        }
        if (redactNested) clean.searchParams.set(key, "[nested_url_redacted]");
      }
    }
    clean.hash = "";
    return clean;
  } catch {
    return clean;
  }
}
__name(sanitizeAdClickLandingUrl, "sanitizeAdClickLandingUrl");
function sanitizeUrlString(v) {
  if (!v)
    return "";
  const raw = String(v).trim();
  if (!raw) return "";
  try {
    const hasExplicitScheme = /^[a-z][a-z0-9+.-]*:/i.test(raw);
    const protocolRelative = raw.startsWith("//");
    const parsed = new URL(raw, "https://www.eden.health");
    if (!["http:", "https:"].includes(parsed.protocol)) return "";
    const clean = sanitizeAdClickLandingUrl(parsed);
    if (!hasExplicitScheme && !protocolRelative) {
      return `${clean.pathname}${clean.search}` || "/";
    }
    return clean.toString();
  } catch {
    // Malformed URLs are dropped rather than persisted with raw query/fragment
    // content. Attribution evidence was extracted before this boundary.
    return "";
  }
}
__name(sanitizeUrlString, "sanitizeUrlString");
function sanitizePersistedEventUrls(value) {
  if (!value || typeof value !== "object") return value;
  // Request JSON is already byte-bounded. Traverse the complete object graph so
  // a deeply nested URL/search/path field cannot bypass persistence redaction.
  const stack = [value];
  const seen = /* @__PURE__ */ new Set();
  while (stack.length) {
    const current = stack.pop();
    if (!current || typeof current !== "object" || seen.has(current)) continue;
    seen.add(current);
    if (Array.isArray(current)) {
      for (const item of current) if (item && typeof item === "object") stack.push(item);
      continue;
    }
    for (const [key, entry] of Object.entries(current)) {
      const normalized = canonicalQueryParamName(key).toLowerCase();
      if (isPersistedUrlField(normalized) && typeof entry === "string") {
        current[key] = sanitizeUrlString(entry);
      } else if (isPersistedSearchField(normalized) && typeof entry === "string") {
        current[key] = sanitizeSearchString(entry);
      } else if (isPersistedPathField(normalized) && typeof entry === "string") {
        current[key] = sanitizePathString(entry);
      } else if (entry && typeof entry === "object") {
        stack.push(entry);
      }
    }
  }
  return value;
}
__name(sanitizePersistedEventUrls, "sanitizePersistedEventUrls");
function isAllowedOrigin(o) {
  return !!o && ALLOWED_ORIGINS.includes(o);
}
__name(isAllowedOrigin, "isAllowedOrigin");
function corsHeadersObj(origin) {
  const headers = {
    "Access-Control-Allow-Methods": "POST, GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Max-Age": "86400"
  };
  if (isAllowedOrigin(origin)) {
    headers["Access-Control-Allow-Origin"] = origin;
    headers["Access-Control-Allow-Credentials"] = "true";
  }
  return headers;
}
__name(corsHeadersObj, "corsHeadersObj");
function corsHeaders(o) {
  return corsHeadersObj(o);
}
__name(corsHeaders, "corsHeaders");
function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
  });
}
__name(jsonResponse, "jsonResponse");

export {
  ConversionCoordinator,
  eden_analytics_worker_default as default
};
//# sourceMappingURL=eden-analytics-worker.js.map
