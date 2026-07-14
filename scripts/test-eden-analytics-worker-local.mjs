#!/usr/bin/env node
import assert from "node:assert/strict";
import productionWorker, { ConversionCoordinator } from "../cloudflare-workers/eden-analytics.js";

const TEST_PRIVACY_LEDGER_HMAC_SECRET = "test-only-privacy-ledger-hmac-secret";
const TEST_BROWSER_CAP_HMAC_SECRET = "test-only-browser-capability-hmac-secret";
const TEST_SERVER_API_SECRET = "test-only-server-api-secret";

function segmentSourceMessageId(payload) {
  return payload?.properties?.segment_source_message_id || payload?.messageId || null;
}

function assertMixpanelSafeMessageId(payload, expectedSourceMessageId) {
  assert.match(payload?.messageId || "", /^m-[a-f0-9]{32}$/, "outgoing Segment messageId must be Mixpanel-safe");
  assert.equal(payload?.properties?.mixpanel_insert_id, payload.messageId, "Mixpanel destination key must match outgoing messageId");
  assert.equal(segmentSourceMessageId(payload), expectedSourceMessageId, "original producer/coordinator idempotency key must remain available");
}
const TEST_INTERNAL_HANDOFF_TRANSPORT_KEYS = new Set([
  "eden_attr_handoff",
  "gclid", "gbraid", "wbraid", "dclid", "srsltid", "fbclid", "msclkid", "ttclid", "twclid",
  "li_fat_id", "rdt_cid", "epik", "sccid", "nbt", "irclickid", "cjevent", "click_id",
  "_gcl_au", "gcl_au", "_gcl_aw", "gcl_aw", "_gcl_dc", "gcl_dc", "_gcl_gb", "gcl_gb",
  "_gcl_gs", "gcl_gs", "gclsrc", "gad_source", "gad_campaignid", "gidrep", "creative", "matchtype",
  "network", "device", "targetid", "feeditemid", "placement", "nb_adtype", "nb_kwd", "nb_ti", "nb_mi",
  "nb_pc", "nb_pi", "nb_ppi", "_ga", "ga", "_gid", "gid", "ga_client_id", "ga_session_id", "gac",
  "gac_cookie_names", "gac_values", "nb_placement", "nb_li_ms", "nb_lp_ms", "nb_fii", "nb_ap", "nb_mt",
  "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term", "utm_id", "_gl",
]);
const TEST_INTERNAL_HANDOFF_NESTED_KEYS = new Set([
  "url", "u", "href", "target", "destination", "dest", "redirect", "redirect_url", "landing_page", "page_url",
  "next", "continue",
]);

class MockKV {
  constructor() { this.map = new Map(); this.getKeys = []; this.putKeys = []; this.putCalls = []; this.deleteKeys = []; }
  async get(key) { this.getKeys.push(key); return this.map.get(key) ?? null; }
  async put(key, value, options = {}) { this.putKeys.push(key); this.putCalls.push({ key, value, options }); this.map.set(key, value); }
  async delete(key) { this.deleteKeys.push(key); this.map.delete(key); }
}

class MockQueue {
  constructor() { this.messages = []; }
  async send(payload, options = {}) {
    this.messages.push({ payload, options });
    return { ok: true };
  }
}

class MockConversionCoordinatorNamespace {
  constructor() {
    this.leases = new Map();
    this.records = new Map();
    this.pointerRecords = new Map();
    this.pointerReservations = new Map();
    this.pointerLastCommits = new Map();
    this.pointerMutationChains = new Map();
    this.firstTouchRecords = new Map();
    this.identityPointerRecords = new Map();
    this.adClickKv = null;
  }
  idFromName(name) { return String(name); }
  get(id) {
    return {
      fetch: async (input, init = {}) => {
        const request = input instanceof Request ? input : new Request(input, init);
        const url = new URL(request.url);
        if (request.method === "GET" && url.pathname === "/health") {
          return new Response(JSON.stringify({ ok: true, schema_version: "eden_conversion_coordinator_v1", storage_readable: true }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          });
        }
        const body = await request.json();
        const nowMs = Date.now();
        if (url.pathname === "/attribution/first-touch") {
          const candidate = body?.record || {};
          const current = this.firstTouchRecords.get(id) || null;
          const next = current ? { ...current } : {
            schema_version: "eden_attribution_first_touch_v1",
            owner_scope: body.owner_scope,
            owner_hash: body.owner_hash,
            ...candidate,
            updated_at: new Date().toISOString(),
          };
          const enrichedFields = [];
          if (current) {
            const sameObservation = /^[a-f0-9]{64}$/.test(String(current.observation_id_sha256 || ""))
              && current.observation_id_sha256 === candidate.observation_id_sha256
              && current.captured_at === candidate.captured_at;
            for (const field of sameObservation ? ["utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content"] : []) {
              if (!next[field] && candidate[field]) {
                next[field] = candidate[field];
                enrichedFields.push(field);
              }
            }
            if (enrichedFields.length) next.updated_at = new Date().toISOString();
          }
          this.firstTouchRecords.set(id, next);
          return new Response(JSON.stringify({
            ok: true,
            record: next,
            created: !current,
            enriched: enrichedFields.length > 0,
            enriched_fields: enrichedFields,
          }), { status: 200, headers: { "Content-Type": "application/json" } });
        }
        if (["/identity-pointer/read", "/identity-pointer/upsert"].includes(url.pathname)) {
          const current = this.identityPointerRecords.get(id) || null;
          if (url.pathname === "/identity-pointer/read") {
            return new Response(JSON.stringify({ ok: true, found: !!current, record: current }), { status: 200, headers: { "Content-Type": "application/json" } });
          }
          const candidate = body?.candidate || {};
          const next = !current ? {
            schema_version: "eden_identity_pointer_v1",
            identity_type: body.identity_type,
            identity_hash: body.identity_hash,
            first_ad_click_id: candidate.ad_click_id,
            first_captured_at: candidate.captured_at,
            latest_ad_click_id: candidate.ad_click_id,
            latest_captured_at: candidate.captured_at,
            updated_at: new Date().toISOString(),
          } : Date.parse(candidate.captured_at) > Date.parse(current.latest_captured_at) ? {
            ...current,
            latest_ad_click_id: candidate.ad_click_id,
            latest_captured_at: candidate.captured_at,
            updated_at: new Date().toISOString(),
          } : current;
          this.identityPointerRecords.set(id, next);
          return new Response(JSON.stringify({ ok: true, record: next, created: !current, latest_updated: next !== current }), { status: 200, headers: { "Content-Type": "application/json" } });
        }
        if (["/pointer/read", "/pointer/upsert", "/pointer/revoke", "/pointer/reserve", "/pointer/commit", "/pointer/cancel"].includes(url.pathname)) {
          const previous = this.pointerMutationChains.get(id) || Promise.resolve();
          const operation = previous.then(async () => {
            const adClickId = body?.ad_click_id;
            const kvKey = `adclick:id:${adClickId}`;
            const seeded = body?.seed_record && body.seed_record.ad_click_id === adClickId
              ? body.seed_record
              : null;
            let current = this.pointerRecords.get(id) || null;
            if (url.pathname === "/pointer/read") {
              const bootstrapped = !current && !!seeded;
              if (bootstrapped) {
                current = seeded;
                this.pointerRecords.set(id, current);
              }
              if (body?.repair_cache !== false && current && !bootstrapped && this.adClickKv && JSON.stringify(current) !== JSON.stringify(seeded)) {
                try {
                  await this.adClickKv.put(kvKey, JSON.stringify(current), { expirationTtl: body.ttl_seconds });
                } catch {}
              }
              return new Response(JSON.stringify({
                ok: true,
                found: !!current,
                record: current,
                bootstrapped_from_cache: bootstrapped,
              }), { status: 200, headers: { "Content-Type": "application/json" } });
            }
            if (url.pathname === "/pointer/reserve") {
              const proposed = body?.proposed_record;
              if (!proposed || proposed.ad_click_id !== adClickId || !body?.reservation_id) {
                return new Response(JSON.stringify({ ok: false, error: "invalid_pointer_reservation" }), { status: 400, headers: { "Content-Type": "application/json" } });
              }
              const sameStableUser = current?.claimed_user_id_sha256 && current.claimed_user_id_sha256 === proposed.claimed_user_id_sha256;
              const sameAnonymousOwner = current?.owner_anonymous_id_sha256 && current.owner_anonymous_id_sha256 === proposed.owner_anonymous_id_sha256;
              const ownerConflictFields = [
                "owner_anonymous_id_sha256",
                "owner_session_id_sha256",
                "claimed_user_id_sha256",
                "claimed_order_id_sha256",
              ].filter((field) => !(field === "claimed_order_id_sha256" && (sameStableUser || sameAnonymousOwner)) && current?.[field] && proposed[field] && current[field] !== proposed[field]);
              if (current?.revoked_at) return new Response(JSON.stringify({ ok: false, reserved: false, revoked: true }), { status: 409, headers: { "Content-Type": "application/json" } });
              if (ownerConflictFields.length) return new Response(JSON.stringify({ ok: false, reserved: false, owner_conflict: true, conflict_fields: ownerConflictFields }), { status: 409, headers: { "Content-Type": "application/json" } });
              const activeReservation = this.pointerReservations.get(id);
              if (activeReservation && activeReservation.reservation_id !== body.reservation_id) {
                return new Response(JSON.stringify({ ok: false, reserved: false, error: "pointer_reservation_busy" }), { status: 409, headers: { "Content-Type": "application/json" } });
              }
              const next = {
                ...proposed,
                captured_at: current?.captured_at || proposed.captured_at,
                owner_anonymous_id_sha256: current?.owner_anonymous_id_sha256 || proposed.owner_anonymous_id_sha256,
                owner_session_id_sha256: current?.owner_session_id_sha256 || proposed.owner_session_id_sha256,
                claimed_user_id_sha256: current?.claimed_user_id_sha256 || proposed.claimed_user_id_sha256,
                claimed_order_id_sha256: current?.claimed_order_id_sha256 || proposed.claimed_order_id_sha256,
              };
              this.pointerReservations.set(id, { reservation_id: body.reservation_id, record: next, persist_cache: body.persist_cache !== false, ttl_seconds: body.ttl_seconds });
              return new Response(JSON.stringify({ ok: true, reserved: true, reservation_id: body.reservation_id }), { status: 200, headers: { "Content-Type": "application/json" } });
            }
            if (url.pathname === "/pointer/commit") {
              const replay = this.pointerLastCommits.get(id);
              const reservation = this.pointerReservations.get(id);
              const committed = reservation?.reservation_id === body?.reservation_id ? reservation : replay?.reservation_id === body?.reservation_id ? replay : null;
              if (!committed) return new Response(JSON.stringify({ ok: false, error: "pointer_reservation_missing" }), { status: 409, headers: { "Content-Type": "application/json" } });
              this.pointerRecords.set(id, committed.record);
              this.pointerLastCommits.set(id, committed);
              this.pointerReservations.delete(id);
              if (committed.persist_cache && this.adClickKv) await this.adClickKv.put(kvKey, JSON.stringify(committed.record), { expirationTtl: committed.ttl_seconds });
              return new Response(JSON.stringify({ ok: true, committed: true, cache_persisted: committed.persist_cache && !!this.adClickKv, replay: committed === replay }), { status: 200, headers: { "Content-Type": "application/json" } });
            }
            if (url.pathname === "/pointer/cancel") {
              const reservation = this.pointerReservations.get(id);
              const cancelled = reservation?.reservation_id === body?.reservation_id;
              if (cancelled) this.pointerReservations.delete(id);
              return new Response(JSON.stringify({ ok: true, cancelled }), { status: 200, headers: { "Content-Type": "application/json" } });
            }
            current ||= seeded;
            let next;
            if (url.pathname === "/pointer/upsert") {
              if (current?.revoked_at) {
                return new Response(JSON.stringify({ ok: false, persisted: false, revoked: true }), {
                  status: 409,
                  headers: { "Content-Type": "application/json" },
                });
              }
              const proposed = body?.proposed_record;
              if (!proposed || proposed.ad_click_id !== adClickId) {
                return new Response(JSON.stringify({ ok: false, error: "invalid_pointer_record" }), {
                  status: 400,
                  headers: { "Content-Type": "application/json" },
                });
              }
              const sameStableUser = current?.claimed_user_id_sha256 && current.claimed_user_id_sha256 === proposed.claimed_user_id_sha256;
              const sameAnonymousOwner = current?.owner_anonymous_id_sha256 && current.owner_anonymous_id_sha256 === proposed.owner_anonymous_id_sha256;
              const ownerConflictFields = [
                "owner_anonymous_id_sha256",
                "owner_session_id_sha256",
                "claimed_user_id_sha256",
                "claimed_order_id_sha256",
              ].filter((field) => !(field === "claimed_order_id_sha256" && (sameStableUser || sameAnonymousOwner)) && current?.[field] && proposed[field] && current[field] !== proposed[field]);
              if (ownerConflictFields.length) {
                return new Response(JSON.stringify({
                  ok: false,
                  persisted: false,
                  owner_conflict: true,
                  error: "owner_conflict",
                  conflict_fields: ownerConflictFields,
                }), { status: 409, headers: { "Content-Type": "application/json" } });
              }
              next = {
                ...proposed,
                captured_at: current?.captured_at || proposed.captured_at,
                owner_anonymous_id_sha256: current?.owner_anonymous_id_sha256 || proposed.owner_anonymous_id_sha256,
                owner_session_id_sha256: current?.owner_session_id_sha256 || proposed.owner_session_id_sha256,
                claimed_user_id_sha256: current?.claimed_user_id_sha256 || proposed.claimed_user_id_sha256,
                claimed_order_id_sha256: current?.claimed_order_id_sha256 || proposed.claimed_order_id_sha256,
              };
            } else {
              const owner = body?.owner || {};
              const ownerValid = !!current && (
                (current.owner_anonymous_id_sha256 && current.owner_anonymous_id_sha256 === owner.anonymous_id_sha256)
                || (current.claimed_user_id_sha256 && current.claimed_user_id_sha256 === owner.user_id_sha256)
                || (current.owner_session_id_sha256 && current.owner_session_id_sha256 === owner.session_id_sha256)
                || (current.claimed_order_id_sha256 && current.claimed_order_id_sha256 === owner.order_id_sha256)
              );
              if (!ownerValid) {
                return new Response(JSON.stringify({ ok: false, revoked: false, ownership_valid: false }), {
                  status: 409,
                  headers: { "Content-Type": "application/json" },
                });
              }
              next = current.revoked_at ? current : {
                ...current,
                revoked_at: body?.revoked_at || new Date().toISOString(),
                revocation_reason: body?.revocation_reason || "explicit_advertising_denial",
              };
            }
            this.pointerRecords.set(id, next);
            const persistCache = body?.persist_cache !== false;
            if (persistCache && this.adClickKv) await this.adClickKv.put(kvKey, JSON.stringify(next), { expirationTtl: body.ttl_seconds });
            return new Response(JSON.stringify({
              ok: true,
              persisted: url.pathname === "/pointer/upsert",
              cache_persisted: persistCache && !!this.adClickKv,
              revoked: !!next.revoked_at,
              ...(url.pathname === "/pointer/revoke" ? { ownership_valid: true } : {}),
            }), { status: 200, headers: { "Content-Type": "application/json" } });
          });
          this.pointerMutationChains.set(id, operation.catch(() => {}));
          return operation;
        }
        if (url.pathname === "/acquire") {
          const current = this.leases.get(id);
          if (current?.token && current.expires_at_ms > nowMs) {
            return new Response(JSON.stringify({
              acquired: false,
              retry_after_ms: Math.max(250, current.expires_at_ms - nowMs),
            }), { status: 409, headers: { "Content-Type": "application/json" } });
          }
          const leaseTtlMs = Math.max(10_000, Math.min(120_000, Number(body?.lease_ttl_ms || 120_000)));
          this.leases.set(id, { token: body.token, expires_at_ms: nowMs + leaseTtlMs });
          return new Response(JSON.stringify({
            acquired: true,
            lease_ttl_ms: leaseTtlMs,
            record: this.records.get(`${id}:${body.event_name}`) || null,
          }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          });
        }
        if (url.pathname === "/record") {
          const current = this.leases.get(id);
          const recorded = !!current?.token && current.token === body?.token && current.expires_at_ms > nowMs;
          if (recorded) this.records.set(`${id}:${body.event_name}`, body.record);
          return new Response(JSON.stringify({ recorded }), {
            status: recorded ? 200 : 409,
            headers: { "Content-Type": "application/json" },
          });
        }
        if (url.pathname === "/restore") {
          const current = this.leases.get(id);
          const restored = !!current?.token && current.token === body?.token && current.expires_at_ms > nowMs;
          if (restored) {
            const key = `${id}:${body.event_name}`;
            if (body.record === null) this.records.delete(key);
            else this.records.set(key, body.record);
          }
          return new Response(JSON.stringify({ restored }), {
            status: restored ? 200 : 409,
            headers: { "Content-Type": "application/json" },
          });
        }
        if (url.pathname === "/release") {
          const current = this.leases.get(id);
          const released = !!current?.token && current.token === body?.token;
          if (released) this.leases.delete(id);
          return new Response(JSON.stringify({ released }), {
            status: released ? 200 : 409,
            headers: { "Content-Type": "application/json" },
          });
        }
        return new Response("Not found", { status: 404 });
      },
    };
  }
}

const defaultConversionCoordinator = new MockConversionCoordinatorNamespace();

// Most fixtures exercise attribution behavior rather than missing-binding
// failure modes. Give them the same required privacy-ledger contract as
// production; dedicated tests below call productionWorker directly to prove
// missing privacy-ledger dependencies cannot suppress default tracking.
const worker = {
  queue: (...args) => productionWorker.queue(...args),
  fetch: (request, env = {}, ctx = {}) => {
    const isServerCollect = new URL(request.url).pathname === "/server-collect";
    const segmentWriteKey = Object.prototype.hasOwnProperty.call(env, "SEGMENT_WRITE_KEY")
      ? env.SEGMENT_WRITE_KEY
      : isServerCollect ? "test-only-segment-write-key" : undefined;
    const gclidKv = Object.prototype.hasOwnProperty.call(env, "GCLID_KV")
      ? env.GCLID_KV
      : isServerCollect ? new MockKV() : undefined;
    const conversionCoordinator = Object.prototype.hasOwnProperty.call(env, "CONVERSION_COORDINATOR")
      ? env.CONVERSION_COORDINATOR
      : defaultConversionCoordinator;
    if (conversionCoordinator && typeof conversionCoordinator === "object") {
      conversionCoordinator.adClickKv = env.AD_CLICK_KV || gclidKv || null;
    }
    return productionWorker.fetch(request, {
      ...env,
      ...(segmentWriteKey !== undefined ? { SEGMENT_WRITE_KEY: segmentWriteKey } : {}),
      ...(gclidKv !== undefined ? { GCLID_KV: gclidKv } : {}),
      SERVER_API_SECRET: env.SERVER_API_SECRET || TEST_SERVER_API_SECRET,
      EDEN_BROWSER_CAP_ENFORCEMENT_MODE: env.EDEN_BROWSER_CAP_ENFORCEMENT_MODE || "shadow",
      BROWSER_CAP_HMAC_SECRET: env.BROWSER_CAP_HMAC_SECRET || TEST_BROWSER_CAP_HMAC_SECRET,
      PRIVACY_LEDGER_HMAC_SECRET: env.PRIVACY_LEDGER_HMAC_SECRET || TEST_PRIVACY_LEDGER_HMAC_SECRET,
      PRIVACY_LEDGER_KV: env.PRIVACY_LEDGER_KV || gclidKv || new MockKV(),
      ...(conversionCoordinator !== undefined ? { CONVERSION_COORDINATOR: conversionCoordinator } : {}),
    }, ctx);
  },
};

function makeCtx() {
  const promises = [];
  return {
    promises,
    waitUntil(promise) { promises.push(Promise.resolve(promise)); },
  };
}

function getAllSetCookie(headers) {
  if (typeof headers.getSetCookie === "function") return headers.getSetCookie();
  const value = headers.get("set-cookie");
  if (!value) return [];
  return value.split(/,(?=\s*[^;,=]+=[^;,]+)/).map((cookie) => cookie.trim());
}

function getSetCookie(headers) {
  return getAllSetCookie(headers).filter((cookie) => !/Max-Age=0(?:;|$)/i.test(cookie));
}

function readCookieFromSetCookie(headers, name) {
  const cookie = getSetCookie(headers).find((value) => value.startsWith(`${name}=`));
  if (!cookie) return null;
  return cookie.slice(name.length + 1).split(";", 1)[0];
}

async function sha256Raw(value) {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(String(value)));
  return Array.from(new Uint8Array(digest)).map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function canonicalConversionDedupKey(eventName, namespace, rawId) {
  const typedKey = `eden_conversion_key_v2:${namespace}:${rawId}`;
  const scopeHash = await sha256Raw(`eden_conversion_coordinator_v1\0conversion_key\0${typedKey}`);
  return `dedup:v4:${eventName}:${scopeHash}`;
}

async function hmacSha256Hex(secret, value) {
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign("HMAC", key, encoder.encode(value));
  return Array.from(new Uint8Array(signature)).map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function signedBrowserCapabilityFixture(secret, { iat, exp, anonId, sessionId, browserHost = "app.eden.health" }) {
  assert.ok(anonId && sessionId, "v2 browser capability fixtures require owner binding");
  const payload = {
    v: 2,
    aud: "eden-analytics-browser",
    iat,
    exp,
    jti: "fixture_browser_capability_jti",
    scp: ["collect", "identify", "preserve"],
    bh: browserHost,
    ch: "collect.eden.health",
    ah: await sha256Raw(anonId),
    sh: await sha256Raw(sessionId),
  };
  const encoded = Buffer.from(JSON.stringify(payload)).toString("base64url");
  const signingInput = `v2.${encoded}`;
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign("HMAC", key, encoder.encode(signingInput));
  return `${signingInput}.${Buffer.from(signature).toString("base64url")}`;
}

function normalizeInternalHandoffTestKey(rawKey) {
  let value = String(rawKey || "").trim();
  for (let index = 0; index < 3; index += 1) {
    try {
      const decoded = decodeURIComponent(value);
      if (decoded === value) break;
      value = decoded;
    } catch {
      break;
    }
  }
  return value.replace(/^(?:amp;)+/i, "").replace(/^[&?]+/, "").toLowerCase();
}

function serializeInternalHandoffTestNestedUrl(parsed, raw) {
  if (/^[a-z][a-z0-9+.-]*:/i.test(raw)) return parsed.toString();
  if (raw.startsWith("//")) return `//${parsed.host}${parsed.pathname}${parsed.search}${parsed.hash}`;
  if (raw.startsWith("?")) return `${parsed.search}${parsed.hash}`;
  if (raw.startsWith("/")) return `${parsed.pathname}${parsed.search}${parsed.hash}`;
  if (raw.startsWith("./")) return `./${parsed.pathname.replace(/^\//, "")}${parsed.search}${parsed.hash}`;
  return `${parsed.pathname.replace(/^\//, "")}${parsed.search}${parsed.hash}`;
}

function cleanInternalHandoffTestHash(parsed, depth = 0) {
  const rawHash = String(parsed.hash || "");
  if (!rawHash || !rawHash.includes("=")) return false;
  const fragment = rawHash.slice(1);
  const queryOffset = fragment.indexOf("?");
  const prefix = queryOffset >= 0 ? fragment.slice(0, queryOffset) : "";
  const rawQuery = queryOffset >= 0 ? fragment.slice(queryOffset + 1) : fragment;
  const params = new URLSearchParams(rawQuery);
  const changed = cleanInternalHandoffTestParams(params, depth + 1);
  if (!changed) return false;
  const query = params.toString();
  parsed.hash = prefix ? `#${prefix}${query ? `?${query}` : ""}` : query ? `#${query}` : "";
  return true;
}

function cleanInternalHandoffTestNestedValue(value, depth = 0) {
  const raw = String(value || "").trim();
  if (!raw) return { changed: false, value: raw };
  if (depth > 6) return { changed: true, value: "" };
  try {
    const parsed = raw.startsWith("?") ? new URL(`https://app.eden.health/${raw}`) : new URL(raw, "https://app.eden.health");
    const searchChanged = cleanInternalHandoffTestParams(parsed.searchParams, depth + 1);
    const hashChanged = cleanInternalHandoffTestHash(parsed, depth + 1);
    if (!searchChanged && !hashChanged) return { changed: false, value: raw };
    return { changed: true, value: serializeInternalHandoffTestNestedUrl(parsed, raw) };
  } catch {
    return { changed: false, value: raw };
  }
}

function cleanInternalHandoffTestParams(params, depth = 0) {
  if (!params) return false;
  let changed = false;
  for (const rawKey of [...params.keys()]) {
    const key = normalizeInternalHandoffTestKey(rawKey);
    if (TEST_INTERNAL_HANDOFF_TRANSPORT_KEYS.has(key)) {
      params.delete(rawKey);
      changed = true;
      continue;
    }
    if (!TEST_INTERNAL_HANDOFF_NESTED_KEYS.has(key)) continue;
    const nested = cleanInternalHandoffTestNestedValue(params.get(rawKey), depth + 1);
    if (!nested.changed) continue;
    if (nested.value) params.set(rawKey, nested.value);
    else params.delete(rawKey);
    changed = true;
  }
  return changed;
}

function internalHandoffTestTransportValues(destination) {
  const parsed = new URL(destination);
  const transport = {};
  const scan = (params, depth = 0) => {
    if (!params) return;
    for (const [rawKey, value] of params.entries()) {
      const key = normalizeInternalHandoffTestKey(rawKey);
      if (key !== "eden_attr_handoff" && TEST_INTERNAL_HANDOFF_TRANSPORT_KEYS.has(key) && transport[key] === undefined && value) {
        transport[key] = value;
      }
      if (depth < 6 && TEST_INTERNAL_HANDOFF_NESTED_KEYS.has(key) && value) {
        try {
          const nested = value.startsWith("?")
            ? new URLSearchParams(value.slice(1))
            : new URL(value, "https://app.eden.health").searchParams;
          scan(nested, depth + 1);
        } catch {}
      }
    }
  };
  scan(parsed.searchParams);
  if (parsed.hash.includes("=")) {
    const fragment = parsed.hash.slice(1);
    const query = fragment.includes("?") ? fragment.slice(fragment.indexOf("?") + 1) : fragment;
    scan(new URLSearchParams(query));
  }
  const entries = Object.entries(transport).sort(([leftKey, leftValue], [rightKey, rightValue]) => {
    const byKey = leftKey.localeCompare(rightKey);
    return byKey || String(leftValue).localeCompare(String(rightValue));
  });
  return Object.fromEntries(entries);
}

async function internalHandoffTestTransportSha256(destination) {
  const entries = Object.entries(internalHandoffTestTransportValues(destination));
  return entries.length ? await sha256Raw(JSON.stringify(entries)) : null;
}

async function internalHandoffTestClickHashes(destination) {
  const clickKeys = new Set(["gclid", "gbraid", "wbraid", "dclid", "gidrep"]);
  const out = {};
  for (const [key, value] of Object.entries(internalHandoffTestTransportValues(destination))) {
    if (clickKeys.has(key)) out[key] = await sha256Raw(`eden_internal_handoff_click_v1\0${key}\0${value}`);
  }
  return out;
}

async function signedInternalHandoffFixture(secret, { iat, exp, pointerId, anonId, sessionId, destination, version = 2, includeTransportClaims = version >= 2 }) {
  const normalizedDestination = new URL(destination);
  normalizedDestination.hash = "";
  cleanInternalHandoffTestParams(normalizedDestination.searchParams);
  const sortedEntries = [...normalizedDestination.searchParams.entries()].sort(([leftKey, leftValue], [rightKey, rightValue]) => {
    const byKey = leftKey.localeCompare(rightKey);
    return byKey || leftValue.localeCompare(rightValue);
  });
  normalizedDestination.search = "";
  for (const [key, value] of sortedEntries) normalizedDestination.searchParams.append(key, value);
  const payload = {
    v: version,
    aud: "eden-analytics-internal-handoff",
    iat,
    exp,
    jti: "fixture_internal_handoff_jti",
    ptr: pointerId,
    a: await sha256Raw(anonId),
    s: await sha256Raw(sessionId),
    dst: await sha256Raw(`${normalizedDestination.origin}${normalizedDestination.pathname}${normalizedDestination.search}`),
    ...(includeTransportClaims ? {
      trn: await internalHandoffTestTransportSha256(destination),
      trc: await internalHandoffTestClickHashes(destination),
    } : {}),
  };
  const encoded = Buffer.from(JSON.stringify(payload)).toString("base64url");
  const signingInput = `h1.${encoded}`;
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign("HMAC", key, encoder.encode(signingInput));
  return `${signingInput}.${Buffer.from(signature).toString("base64url")}`;
}

async function collect({ canaryParam = false, mode = "canary" } = {}) {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push({ url: String(url), init, body: JSON.parse(init.body) });
      return new Response("{}", { status: 200 });
    }
    return new Response("<html><head></head><body>ok</body></html>", { status: 200, headers: { "content-type": "text/html" } });
  };
  try {
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: mode,
      SEGMENT_WRITE_KEY: "test_write_key",
      GCLID_KV: new MockKV(),
    };
    const ctx = makeCtx();
    const suffix = canaryParam ? "?eden_tracking_enrichment_canary=1" : "";
    const pageUrl = `https://app.eden.health/intake${suffix}&gclid=gclid-local&gbraid=gbraid-local&wbraid=wbraid-local&dclid=dclid-local&gclsrc=aw.ds&utm_source=google&utm_medium=cpc&utm_campaign=local_canary`;
    const req = new Request(`https://collect.eden.health/collect${suffix}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": "eden_anon_id=anon-local; eden_session_id=session-local_1780000000000; _gcl_aw=GCLAWLOCAL; _gcl_dc=GCLDCLOCAL; _gcl_gb=GCLGBLOCAL; _gcl_gs=GCLGSLOCAL; _ga=GA1.1.123.456; _gid=GA1.2.333.444; _gac_UA-1=GACLOCAL",
      },
      body: JSON.stringify({
        type: "track",
        event: "test_tracking_enrichment_canary",
        anonymousId: "anon-local",
        properties: {
          page_url: pageUrl,
          email: "test_fixture_email",
          phone: "5550100100",
          first_name: "Testy",
          last_name: "McFixture",
          postalCode: " 10001 ",
          countryCode: "us",
        },
        context: {
          page: { url: pageUrl, path: "/intake", referrer: "https://www.google.com/" },
        },
      }),
    });
    const res = await worker.fetch(req, env, ctx);
    const responseBody = await res.json();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(segmentCalls.length, 1);
    return { segment: segmentCalls[0].body, response: responseBody };
  } finally {
    globalThis.fetch = originalFetch;
  }
}

const canaryResult = await collect({ canaryParam: true, mode: "canary" });
const canaryPayload = canaryResult.segment;
assert.equal(canaryResult.response.enrichmentCanary, true);
assert.equal(canaryResult.response.workerVersion, "5.56");
assert.equal(canaryResult.response.enrichmentVersion, "5.54");
assert.equal(canaryPayload.event, "test_tracking_enrichment_canary");
assert.equal(canaryPayload.properties.enrichment_canary, true);
assert.equal(canaryPayload.properties.enrichment_mode, "canary");
assert.equal(canaryPayload.properties.eden_session_id, "session-local_1780000000000");
assert.equal(canaryPayload.properties.first_party_device_id, "anon-local");
assert.equal(canaryPayload.properties.gcl_aw, "GCLAWLOCAL");
assert.equal(canaryPayload.properties.gcl_dc, "GCLDCLOCAL");
assert.equal(canaryPayload.properties.gcl_gb, "GCLGBLOCAL");
assert.equal(canaryPayload.properties.gcl_gs, "GCLGSLOCAL");
assert.equal(canaryPayload.properties.gac_cookie_names, "_gac_UA-1");
assert.equal(canaryPayload.properties.ga, "GA1.1.123.456");
assert.equal(canaryPayload.properties.gid, "GA1.2.333.444");
assert.equal(canaryPayload.properties.page_host, "app.eden.health");
assert.match(canaryPayload.properties.attribution_snapshot_id, /^attr_[a-f0-9]{32}$/);
assert.equal(canaryPayload.context.campaign.gclid, "gclid-local");
assert.equal(canaryPayload.context.campaign.first_party_device_id, "anon-local");
assert.equal(canaryPayload.context.campaign.eden_session_id, "session-local_1780000000000");
assert.equal(canaryPayload.context.campaign.page_host, "app.eden.health");
assert.equal(canaryPayload.context.campaign.source_system, "eden_health_first_party_tracking");
assert.equal(canaryPayload.context.first_party_device_id, "anon-local");
assert.equal(canaryPayload.context.eden_session_id, "session-local_1780000000000");
assert.equal(canaryPayload.context.source_system, "eden_health_first_party_tracking");
assert.equal(canaryPayload.context.page.host, "app.eden.health");
assert.equal(canaryPayload.properties.email_sha256, undefined, "browser PII cannot become trusted identity evidence");
assert.equal(canaryPayload.properties.phone_sha256, undefined);
assert.equal(canaryPayload.properties.first_name_sha256, undefined);
assert.equal(canaryPayload.properties.last_name_sha256, undefined);
assert.equal(canaryPayload.properties.postal_code, undefined);
assert.equal(canaryPayload.properties.country, "US", "coarse country context may remain without a stable identifier");

const nonCanaryResult = await collect({ canaryParam: false, mode: "canary" });
const nonCanaryPayload = nonCanaryResult.segment;
assert.equal(nonCanaryResult.response.enrichmentCanary, undefined);
assert.notEqual(nonCanaryPayload.properties.enrichment_canary, true);
assert.equal(nonCanaryPayload.properties.enrichment_mode, undefined);
assert.equal(nonCanaryPayload.properties.gcl_aw, undefined);
assert.equal(nonCanaryPayload.properties.page_host, undefined);
assert.equal(nonCanaryPayload.context.campaign.first_party_device_id, undefined);

const allModeResult = await collect({ canaryParam: false, mode: "all" });
const allModePayload = allModeResult.segment;
assert.equal(allModeResult.response.enrichmentActive, true);
assert.equal(allModeResult.response.enrichmentMode, "all");
assert.equal(allModeResult.response.enrichmentCanary, undefined);
assert.equal(allModePayload.properties.enrichment_mode, "all");
assert.notEqual(allModePayload.properties.enrichment_canary, true);
assert.equal(allModePayload.properties.eden_session_id, "session-local_1780000000000");
assert.equal(allModePayload.properties.first_party_device_id, "anon-local");
assert.equal(allModePayload.properties.gcl_aw, "GCLAWLOCAL");
assert.equal(allModePayload.properties.page_host, "app.eden.health");
assert.equal(allModePayload.context.campaign.first_party_device_id, "anon-local");
assert.equal(allModePayload.context.campaign.eden_session_id, "session-local_1780000000000");
assert.equal(allModePayload.context.first_party_device_id, "anon-local");
assert.equal(allModePayload.context.eden_session_id, "session-local_1780000000000");

const offResult = await collect({ canaryParam: true, mode: "off" });
const offPayload = offResult.segment;
assert.equal(offResult.response.enrichmentCanary, undefined);
assert.notEqual(offPayload.properties.enrichment_canary, true);
assert.equal(offPayload.properties.gcl_aw, undefined);
assert.equal(offPayload.context.campaign.first_party_device_id, undefined);

async function serverCollect({ mode = "all" } = {}) {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push({ url: String(url), init, body: JSON.parse(init.body) });
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const adClickQueue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: mode,
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "test_write_key",
      GCLID_KV: new MockKV(),
      AD_CLICK_KV: new MockKV(),
      AD_CLICK_SNAPSHOT_QUEUE: adClickQueue,
    };
    const ctx = makeCtx();
    const pageUrl = "https://app.eden.health/intake/checkout?gclid=gclid-server&gad_source=1&utm_source=google&utm_medium=cpc&utm_campaign=server_context";
    const req = new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "X-Eden-Server-Secret": "test_server_secret",
        "Cookie": "eden_anon_id=anon-server; eden_session_id=session-server_1780000000000; _gcl_aw=GCLAWSERVER; _gac_UA-1=GACSERVER",
      },
      body: JSON.stringify({
        type: "track",
        event: "OS_purchase",
        timestamp: "2026-06-01T12:34:56Z",
        anonymousId: "anon-server",
        userId: "user-server",
        eden_identity_id: "eden_identity_server",
        properties: {
          order_id: "order-server",
          transaction_id: "charge-server",
          page_url: pageUrl,
          email: "server_fixture@example.com",
          phone: "(555) 010-0101",
          firstName: "Server",
          lastName: "Fixture",
        },
        context: {
          page: { url: pageUrl, path: "/intake/checkout", referrer: "https://www.google.com/" },
        },
      }),
    });
    const res = await worker.fetch(req, env, ctx);
    await res.json();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(segmentCalls.length, 1);
    return {
      segment: segmentCalls[0].body,
      snapshots: adClickQueue.messages.map((message) => message.payload).filter((payload) => payload.event_type === "ad_click_snapshot"),
    };
  } finally {
    globalThis.fetch = originalFetch;
  }
}

const serverResult = await serverCollect({ mode: "all" });
const serverPayload = serverResult.segment;
assert.equal(serverPayload.event, "OS_purchase");
assert.equal(serverPayload.timestamp, "2026-06-01T12:34:56.000Z", "authenticated producer event time must survive to Segment");
assert.notEqual(serverPayload.properties.edge_received_at, serverPayload.timestamp, "receipt time must be a separate diagnostic, not overwrite event time");
assert.equal(serverResult.snapshots.length, 1);
assert.equal(serverResult.snapshots[0].snapshot.captured_at, "2026-06-01T12:34:56.000Z", "immutable click snapshot must use producer event time");
assert.equal(serverPayload.userId, "eden_identity_server", "authenticated eden_identity_id must be the canonical stable identity sent downstream");
assert.equal(serverPayload.properties.enrichment_mode, "all");
assert.notEqual(serverPayload.properties.enrichment_canary, true);
assert.equal(serverPayload.properties.eden_session_id, "session-server_1780000000000");
assert.equal(serverPayload.properties.first_party_device_id, "anon-server");
assert.equal(serverPayload.properties.gclid, "gclid-server");
assert.equal(serverPayload.properties.gcl_aw, "GCLAWSERVER");
assert.equal(serverPayload.properties.gac_cookie_names, "_gac_UA-1");
assert.equal(serverPayload.properties.page_host, "app.eden.health");
assert.ok(serverPayload.properties.email_sha256);
assert.ok(serverPayload.properties.phone_sha256);
assert.ok(serverPayload.properties.first_name_sha256);
assert.ok(serverPayload.properties.last_name_sha256);
assert.equal(serverPayload.context.eden_session_id, "session-server_1780000000000");
assert.equal(serverPayload.context.campaign.eden_session_id, "session-server_1780000000000");
assert.equal(serverPayload.context.page.host, "app.eden.health");
assert.equal(serverPayload.properties.stable_identity_key_type, "eden_identity_id");
assert.equal(serverPayload.properties.identity_warning, "eden_identity_id_conflicts_with_source_user_id");

async function browserPayloadCannotBackdateFreshClickObservation() {
  const queue = new MockQueue();
  const startedAt = Date.now();
  const ctx = makeCtx();
  const response = await worker.fetch(new Request("https://collect.eden.health/collect", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Origin": "https://app.eden.health",
      "Cookie": "eden_anonymous_id=browser-time-anon; eden_session_id=browser-time-session_1780000000000",
    },
    body: JSON.stringify({
      type: "track",
      event: "intake_started",
      timestamp: "2000-01-01T00:00:00.000Z",
      properties: {},
      context: {
        page: {
          url: "https://app.eden.health/intake?gclid=BROWSER-TIME-GCLID&utm_source=google&utm_medium=cpc",
        },
      },
    }),
  }), {
    EDEN_AD_CLICK_MEMORY_MODE: "cookie",
    GCLID_KV: new MockKV(),
    AD_CLICK_KV: new MockKV(),
    AD_CLICK_SNAPSHOT_QUEUE: queue,
  }, ctx);
  assert.equal(response.status, 200);
  await response.json();
  await Promise.all(ctx.promises);
  const capturedAt = Date.parse(queue.messages.find((message) => message.payload.event_type === "ad_click_snapshot")?.payload?.snapshot?.captured_at || "");
  assert.ok(Number.isFinite(capturedAt), "browser fresh click must still produce an immutable observation");
  assert.ok(capturedAt >= startedAt - 1000 && capturedAt <= Date.now() + 1000, "browser-supplied historical timestamps cannot backdate first-touch evidence");
}

await browserPayloadCannotBackdateFreshClickObservation();

async function authenticatedServerSanitizesAndRecoversNestedGoogleIds() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const queue = new MockQueue();
    const ctx = makeCtx();
    const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Eden-Server-Secret": "test_server_secret",
        "Cookie": "eden_anonymous_id=server-nested-anon; eden_session_id=server-nested-session_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "payment_authorized",
        anonymousId: "server-nested-anon",
        properties: {
          gclid: "undefined",
          envelope: { attribution: { wbraid: "SERVER-NESTED-WBRAID-12345" } },
        },
        context: { campaign: { gbraid: "null" } },
      }),
    }), {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      GCLID_KV: new MockKV(),
      AD_CLICK_KV: new MockKV(),
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    }, ctx);
    assert.equal(response.status, 200);
    await response.json();
    await Promise.all(ctx.promises);
    assert.equal(segmentCalls.length, 1);
    const payload = segmentCalls[0];
    assert.equal(payload.properties.gclid, undefined, "sentinel server gclid must not reach Segment");
    assert.equal(payload.context.campaign.gbraid, undefined, "sentinel server gbraid must not reach Segment");
    assert.equal(payload.context.campaign.wbraid, "SERVER-NESTED-WBRAID-12345", "valid nested server click evidence must be promoted into governed campaign context");
    const snapshot = queue.messages.find((message) => message.payload.event_type === "ad_click_snapshot")?.payload?.snapshot;
    assert.equal(snapshot?.google?.wbraid, "SERVER-NESTED-WBRAID-12345", "valid nested server evidence must reach immutable ad-click memory");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await authenticatedServerSanitizesAndRecoversNestedGoogleIds();

async function conversionDedupAllowsMonotonicFreshAttributionEnrichment() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const gclidKv = new MockKV();
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    await gclidKv.put("attr:anon:dedup-anon", JSON.stringify({
      gclid: "RECOVERED-GCLID-12345",
      utm_source: "google",
      stored_at: "2026-06-01T00:00:00.000Z",
    }));
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      GCLID_KV: gclidKv,
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const send = async (pageUrl, propertyOverrides = {}, sessionId = "dedup-session_1780000000000") => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Eden-Server-Secret": "test_server_secret",
          Cookie: `eden_anonymous_id=dedup-anon; eden_session_id=${sessionId}`,
        },
        body: JSON.stringify({
          type: "track",
          event: "OS_purchase",
          timestamp: "2026-06-02T12:00:00Z",
          anonymousId: "dedup-anon",
          userId: "dedup-user",
          properties: {
            order_id: "dedup-order",
            transaction_id: "dedup-charge",
            product_id: "semaglutide",
            conversion_value: 138,
            currency: "USD",
            ...propertyOverrides,
          },
          context: { page: { url: pageUrl } },
        }),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };
    const first = await send("https://app.eden.health/intake/checkout", { payment_status: "pending" });
    assert.equal(first.response.status, 200);
    assert.notEqual(first.responseBody.deduped, true);
    const sessionOnlyRetry = await send(
      "https://app.eden.health/intake/checkout",
      { payment_status: "pending" },
      "dedup-session-next_1780003600000",
    );
    assert.equal(sessionOnlyRetry.responseBody.deduped, true, "session churn alone cannot reopen a business conversion");
    const queueAfterFirst = queue.messages.length;
    const second = await send("https://app.eden.health/intake/checkout?gbraid=FRESH-GBRAID-12345&utm_source=google", { payment_status: "authorized" });
    assert.equal(second.response.status, 200);
    assert.notEqual(second.responseBody.deduped, true, "a fresh event-native braid must enrich an earlier recovered-only purchase");
    assert.equal(second.responseBody.segment_forwarded, true, "conversion enrichment must be durably acknowledged by Segment");
    assert.equal(second.responseBody.conversion_enrichment_forwarded, true);
    assert.equal(second.responseBody.conversion_enrichment_event, "OS_purchase_enrichment");
    assert.ok(queue.messages.length > queueAfterFirst, "the richer retry must persist its immutable fresh-evidence observation");
    assert.ok(queue.messages.slice(queueAfterFirst).some((message) => message.payload?.snapshot?.google?.gbraid === "FRESH-GBRAID-12345"));
    const queueAfterSecond = queue.messages.length;
    const third = await send("https://app.eden.health/intake/checkout?gbraid=FRESH-GBRAID-12345&utm_source=google", { payment_status: "authorized" });
    assert.equal(third.responseBody.deduped, true, "an exact repeat with no new signal remains deduped");
    assert.equal(queue.messages.length, queueAfterSecond);
    const regressed = await send("https://app.eden.health/intake/checkout?gbraid=FRESH-GBRAID-12345&utm_source=google", { payment_status: "pending" });
    assert.equal(regressed.responseBody.deduped, true, "a stale payment-state regression must not reopen the conversion");
    const progressed = await send("https://app.eden.health/intake/checkout?gbraid=FRESH-GBRAID-12345&utm_source=google", { payment_status: "succeeded" });
    assert.notEqual(progressed.responseBody.deduped, true, "a true payment-state progression remains a useful enrichment");
    assert.equal(progressed.responseBody.conversion_enrichment_forwarded, true);
    const oscillated = await send("https://app.eden.health/intake/checkout?gbraid=FRESH-GBRAID-12345&utm_source=google", { payment_status: "authorized" });
    assert.equal(oscillated.responseBody.deduped, true, "payment state must not oscillate backward after success");
    assert.equal(segmentCalls.filter((call) => call.event === "OS_purchase").length, 1, "the business conversion must remain exactly once");
    const enrichmentCalls = segmentCalls.filter((call) => call.event === "OS_purchase_enrichment");
    assert.equal(enrichmentCalls.length, 2, "authorized and succeeded lifecycle progress must remain queryable without duplicating OS_purchase");
    assertMixpanelSafeMessageId(segmentCalls[0], "OS_purchase:dedup-charge");
    assert.match(segmentCalls[0].properties.mixpanel_insert_id, /^m-[a-f0-9]{32}$/);
    assert.ok(enrichmentCalls.every((call) => /^m-[a-f0-9]{32}$/.test(call.properties.mixpanel_insert_id)), "every explicit Segment idempotency key gets a deterministic Mixpanel-safe destination key");
    assert.equal(new Set(segmentCalls.map((call) => call.properties.mixpanel_insert_id)).size, segmentCalls.length, "distinct canonical Segment message IDs must keep distinct Mixpanel destination IDs");
    assert.ok(enrichmentCalls.every((call) => call.properties.conversion_enrichment_only === true));
    assert.ok(enrichmentCalls.every((call) => segmentSourceMessageId(call)?.startsWith("eden_OS_purchase_enrichment_")));
    assert.notEqual(enrichmentCalls[0].messageId, enrichmentCalls[1].messageId, "distinct accepted lifecycle states need distinct idempotent enrichment messages");
    const dedupRecord = JSON.parse(await gclidKv.get(await canonicalConversionDedupKey("OS_purchase", "transaction_id", "dedup-charge")));
    assert.equal(dedupRecord.schema_version, "eden_conversion_dedup_v4");
    assert.equal(dedupRecord.delivery_state, "segment_acknowledged");
    assert.equal(dedupRecord.status_ranks["property:payment_status"], 3);
    assert.ok(Object.keys(dedupRecord.signal_hashes).some((key) => key === "native_click:gbraid"));
    assert.equal(JSON.stringify(dedupRecord).includes("FRESH-GBRAID-12345"), false, "dedup KV stores only signal hashes, never raw click values");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await conversionDedupAllowsMonotonicFreshAttributionEnrichment();

async function syntacticPurchaseAliasSharesOneBaseDelivery() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: new MockKV(),
      CONVERSION_COORDINATOR: new MockConversionCoordinatorNamespace(),
    };
    const send = async (event, messageId) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "test_server_secret" },
        body: JSON.stringify({
          type: "track",
          event,
          messageId,
          anonymousId: "purchase-alias-anon",
          userId: "purchase-alias-user",
          properties: {
            transaction_id: "purchase-alias-charge",
            treatment_id: "purchase-alias-treatment",
            payment_status: "authorized",
            product_id: "semaglutide",
            conversion_value: 138,
            currency: "USD",
          },
        }),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };

    const alias = await send("purchase", "purchase:purchase-alias-charge");
    assert.equal(alias.response.status, 200);
    assert.equal(alias.responseBody.conversion_idempotency_key_source, "transaction_id");
    assert.equal(segmentCalls[0].event, "OS_purchase");
    assertMixpanelSafeMessageId(segmentCalls[0], "OS_purchase:purchase-alias-charge");
    assert.equal(segmentCalls[0].properties.conversion_event_original_alias, "purchase");
    const canonical = await send("OS_purchase", "OS_purchase:purchase-alias-charge");
    assert.equal(canonical.response.status, 200);
    assert.equal(canonical.responseBody.deduped, true);
    assert.equal(segmentCalls.length, 1, "purchase and OS_purchase are syntactic aliases of one base delivery");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await syntacticPurchaseAliasSharesOneBaseDelivery();

async function v555OneDayRowsRequireTransactionEqualityBeforeMigration() {
  const segmentCalls = [];
  let failProvenBaseOnce = false;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      const payload = JSON.parse(init.body);
      segmentCalls.push(payload);
      if (failProvenBaseOnce
        && payload.event === "OS_purchase"
        && segmentSourceMessageId(payload) === "eden_OS_purchase_v555-proven-charge") {
        failProvenBaseOnce = false;
        return new Response("{}", { status: 503 });
      }
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const gclidKv = new MockKV();
    await gclidKv.put("dedup:OS_purchase:v555-treatment", JSON.stringify({
      event: "OS_purchase",
      order_id: "v555-treatment",
      attribution_found: true,
      fired_at: "2026-07-10T12:00:00.000Z",
    }), { expirationTtl: 86400 });
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: gclidKv,
      CONVERSION_COORDINATOR: new MockConversionCoordinatorNamespace(),
    };
    const makeRequest = () => new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "test_server_secret" },
      body: JSON.stringify({
        type: "track",
        event: "purchase",
        messageId: "OS_purchase:v556-charge",
        anonymousId: "v556-migration-anon",
        userId: "v556-migration-user",
        properties: {
          transaction_id: "v556-charge",
          treatment_id: "v555-treatment",
          payment_status: "authorized",
          product_id: "semaglutide",
          conversion_value: 138,
          currency: "USD",
        },
      }),
    });

    const first = await worker.fetch(makeRequest(), env, makeCtx());
    const firstBody = await first.json();
    assert.equal(first.status, 200);
    assert.notEqual(firstBody.deduped, true);
    assert.deepEqual(segmentCalls.map((call) => call.event), ["OS_purchase"], "a treatment-scoped v5.55 row cannot suppress a distinct charge without transaction proof");
    assert.ok(gclidKv.putKeys.some((key) => key.startsWith("dedup:v4:OS_purchase:")), "the delivered charge commits only its corrected raw-free transaction scope");

    const second = await worker.fetch(makeRequest(), env, makeCtx());
    assert.equal((await second.json()).deduped, true);
    assert.equal(segmentCalls.length, 1);

    const richerKv = new MockKV();
    await richerKv.put("dedup:OS_purchase:v555-no-attr-treatment", JSON.stringify({
      event: "OS_purchase",
      order_id: "v555-no-attr-treatment",
      attribution_found: false,
      fired_at: "2026-07-10T12:10:00.000Z",
    }), { expirationTtl: 86400 });
    const richerEnv = {
      ...env,
      GCLID_KV: richerKv,
      CONVERSION_COORDINATOR: new MockConversionCoordinatorNamespace(),
    };
    segmentCalls.length = 0;
    const richerCtx = makeCtx();
    const richer = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "test_server_secret" },
      body: JSON.stringify({
        type: "track",
        event: "OS_purchase",
        messageId: "OS_purchase:v556-richer-charge",
        anonymousId: "v556-richer-anon",
        userId: "v556-richer-user",
        properties: {
          transaction_id: "v556-richer-charge",
          treatment_id: "v555-no-attr-treatment",
          payment_status: "authorized",
          product_id: "semaglutide",
          conversion_value: 138,
          currency: "USD",
          gclid: "V556-MIGRATION-RICHER-GCLID",
        },
      }),
    }), richerEnv, richerCtx);
    const richerBody = await richer.json();
    await Promise.all(richerCtx.promises);
    assert.equal(richer.status, 200);
    assert.equal(richerBody.segment_forwarded, true);
    assert.deepEqual(segmentCalls.map((call) => call.event), ["OS_purchase"], "an ambiguous no-attribution treatment row cannot become transaction delivery authority");

    const provenKv = new MockKV();
    await provenKv.put("dedup:OS_purchase:v555-proven-charge", JSON.stringify({
      event: "OS_purchase",
      order_id: "v555-proven-charge",
      attribution_found: true,
      fired_at: "2026-07-10T12:20:00.000Z",
    }), { expirationTtl: 86400 });
    const provenEnv = {
      ...env,
      GCLID_KV: provenKv,
      CONVERSION_COORDINATOR: new MockConversionCoordinatorNamespace(),
    };
    segmentCalls.length = 0;
    const provenRequest = () => new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "test_server_secret" },
      body: JSON.stringify({
        type: "track",
        event: "OS_purchase",
        messageId: "OS_purchase:v555-proven-charge",
        anonymousId: "v555-proven-anon",
        userId: "v555-proven-user",
        properties: {
          transaction_id: "v555-proven-charge",
          payment_status: "authorized",
          conversion_value: 138,
          currency: "USD",
        },
      }),
    });
    failProvenBaseOnce = true;
    const provenCtx = makeCtx();
    const proven = await worker.fetch(provenRequest(), provenEnv, provenCtx);
    const provenBody = await proven.json();
    await Promise.all(provenCtx.promises);
    assert.equal(proven.status, 503);
    assert.equal(provenBody.error, "conversion_delivery_failed");
    assert.equal(provenBody.segment_forwarded, false);
    assert.deepEqual(segmentCalls.map((call) => call.event), ["OS_purchase"]);
    assertMixpanelSafeMessageId(segmentCalls[0], "eden_OS_purchase_v555-proven-charge");
    assert.equal(segmentCalls[0].timestamp, "2026-07-10T12:20:00.000Z");

    const repairedCtx = makeCtx();
    const repaired = await worker.fetch(provenRequest(), provenEnv, repairedCtx);
    const repairedBody = await repaired.json();
    await Promise.all(repairedCtx.promises);
    assert.equal(repaired.status, 200);
    assert.equal(repairedBody.segment_forwarded, true, "a v5.55 row proves an attempted delivery, not a Segment acknowledgement");
    assert.equal(repairedBody.conversion_enrichment_forwarded, true, "the planned current-truth enrichment must survive an ambiguous historical-base repair");
    assert.deepEqual(segmentCalls.map((call) => call.event), ["OS_purchase", "OS_purchase", "OS_purchase_enrichment"]);
    assert.equal(segmentCalls[1].messageId, segmentCalls[0].messageId);
    assert.equal(segmentCalls[1].timestamp, segmentCalls[0].timestamp);
    assert.notEqual(segmentCalls[2].messageId, segmentCalls[0].messageId);

    const exactCtx = makeCtx();
    const exact = await worker.fetch(provenRequest(), provenEnv, exactCtx);
    assert.equal((await exact.json()).deduped, true);
    await Promise.all(exactCtx.promises);
    assert.equal(segmentCalls.length, 3, "the repaired typed ledger blocks later exact retries without trusting the old pre-Segment row");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await v555OneDayRowsRequireTransactionEqualityBeforeMigration();

async function conversionKeyCannotPromoteOrDriftNamespaces() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: new MockKV(),
      CONVERSION_COORDINATOR: new MockConversionCoordinatorNamespace(),
    };
    const send = async (properties, messageId = undefined) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "test_server_secret" },
        body: JSON.stringify({
          type: "track",
          event: "OS_purchase",
          ...(messageId ? { messageId } : {}),
          anonymousId: "stable-scope-anon",
          userId: "stable-scope-user",
          properties: {
            payment_status: "authorized",
            product_id: "semaglutide",
            conversion_value: 138,
            currency: "USD",
            ...properties,
          },
        }),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };

    const orderOnly = await send({ order_id: "unstable-order-only" });
    assert.equal(orderOnly.response.status, 422);
    assert.equal(orderOnly.responseBody.error, "conversion_idempotency_key_required");
    assert.equal(segmentCalls.length, 0, "OS_purchase cannot start in a fallback order/treatment/master scope");

    const mismatchedMessage = await send(
      { transaction_id: "message-diagnostic-authoritative-charge" },
      "550e8400-e29b-41d4-a716-446655440000",
    );
    assert.equal(mismatchedMessage.response.status, 200, "a valid transaction ID must outrank unrelated producer message metadata");
    assert.equal(segmentCalls.length, 1);
    assertMixpanelSafeMessageId(segmentCalls[0], "OS_purchase:message-diagnostic-authoritative-charge");

    const first = await send(
      { transaction_id: "stable-scope-charge", treatment_id: "stable-scope-treatment" },
      "OS_purchase:stable-scope-charge",
    );
    assert.equal(first.response.status, 200);
    const richer = await send(
      {
        transaction_id: "stable-scope-charge",
        treatment_id: "stable-scope-treatment",
        master_id: "stable-scope-master",
        order_id: "stable-scope-order",
      },
      "OS_purchase:stable-scope-charge",
    );
    assert.equal(richer.response.status, 200);
    assert.equal(richer.responseBody.conversion_enrichment_forwarded, true);
    assert.equal(segmentCalls.filter((call) => call.event === "OS_purchase").length, 2, "later relationship IDs cannot create a second base purchase");
    assert.deepEqual(segmentCalls.map((call) => call.event), ["OS_purchase", "OS_purchase", "OS_purchase_enrichment"]);
    assertMixpanelSafeMessageId(segmentCalls[1], "OS_purchase:stable-scope-charge");

    const secondChargeOnSameOrder = await send(
      {
        transaction_id: "stable-scope-second-charge",
        treatment_id: "stable-scope-treatment",
        master_id: "stable-scope-master",
        order_id: "stable-scope-order",
      },
      "OS_purchase:stable-scope-second-charge",
    );
    assert.equal(secondChargeOnSameOrder.response.status, 200);
    assert.equal(segmentCalls.filter((call) => call.event === "OS_purchase").length, 3, "two legitimate charge transactions sharing one order must each produce one base purchase");
    assert.deepEqual(segmentCalls.map((call) => call.event), ["OS_purchase", "OS_purchase", "OS_purchase_enrichment", "OS_purchase"]);
    assertMixpanelSafeMessageId(segmentCalls[3], "OS_purchase:stable-scope-second-charge");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await conversionKeyCannotPromoteOrDriftNamespaces();

async function conversionBusinessEventsRequireTrackEnvelopes() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: new MockKV(),
      CONVERSION_COORDINATOR: new MockConversionCoordinatorNamespace(),
    };
    const send = async (type) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "test_server_secret" },
        body: JSON.stringify({
          type,
          event: "OS_purchase",
          messageId: "OS_purchase:track-envelope-charge",
          anonymousId: "track-envelope-anon",
          userId: "track-envelope-user",
          properties: {
            transaction_id: "track-envelope-charge",
            payment_status: "authorized",
            conversion_value: 138,
            currency: "USD",
          },
        }),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };

    for (const type of ["identify", "page", "screen"]) {
      const rejected = await send(type);
      assert.equal(rejected.response.status, 422);
      assert.equal(rejected.responseBody.error, "conversion_track_envelope_required");
      assert.equal(rejected.responseBody.segment_forwarded, false);
    }
    assert.equal(segmentCalls.length, 0, "non-track envelopes cannot poison the conversion acknowledgement ledger");

    const tracked = await send("track");
    assert.equal(tracked.response.status, 200);
    assert.equal(segmentCalls.length, 1);
    assert.equal(segmentCalls[0].event, "OS_purchase");
    assertMixpanelSafeMessageId(segmentCalls[0], "OS_purchase:track-envelope-charge");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await conversionBusinessEventsRequireTrackEnvelopes();

async function conversionSegmentFailureRemainsRetryableAndDoesNotFanOut() {
  const segmentCalls = [];
  let segmentAttempts = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentAttempts += 1;
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: segmentAttempts === 1 ? 503 : 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const gclidKv = new MockKV();
    const queue = new MockQueue();
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      GCLID_KV: gclidKv,
      AD_CLICK_KV: new MockKV(),
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const send = async (paymentStatus, producerTimestamp = "2026-06-01T12:00:00.000Z") => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Eden-Server-Secret": "test_server_secret",
          Cookie: "eden_anonymous_id=segment-retry-anon; eden_session_id=segment-retry-session_1780000000000",
        },
        body: JSON.stringify({
          type: "track",
          event: "OS_purchase",
          timestamp: producerTimestamp,
          originalTimestamp: producerTimestamp,
          anonymousId: "segment-retry-anon",
          userId: "segment-retry-user",
          properties: {
            order_id: "segment-retry-order",
            transaction_id: "segment-retry-charge",
            product_id: "semaglutide",
            conversion_value: 138,
            currency: "USD",
            payment_status: paymentStatus,
          },
          context: { page: { url: "https://app.eden.health/intake/checkout?gclid=SEGMENT-RETRY-GCLID-12345&utm_source=google" } },
        }),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };

    const failed = await send("succeeded");
    assert.equal(failed.response.status, 503);
    assert.equal(failed.responseBody.error, "conversion_delivery_failed");
    assert.equal(failed.responseBody.retryable, true);
    const segmentRetryDedupKey = await canonicalConversionDedupKey("OS_purchase", "transaction_id", "segment-retry-charge");
    assert.equal(await gclidKv.get(segmentRetryDedupKey), null, "failed Segment delivery cannot finalize dedupe");
    assert.equal(queue.messages.length, 0, "failed business delivery cannot fan out an orphaned ad-click snapshot");

    const retried = await send("succeeded", "2026-06-02T12:00:00.000Z");
    assert.equal(retried.response.status, 200);
    assert.equal(retried.responseBody.segment_forwarded, true);
    assert.equal(segmentCalls.length, 2);
    assert.equal(segmentCalls[0].messageId, segmentCalls[1].messageId, "unknown-commit retries must reuse one Segment idempotency key");
    assert.equal(segmentCalls[0].timestamp, "2026-06-01T12:00:00.000Z");
    assert.equal(segmentCalls[1].timestamp, segmentCalls[0].timestamp, "unknown-commit retries must reuse the first attempted purchase event time so one message ID cannot move reporting dates");
    assert.equal(segmentCalls[0].properties.payment_status, "succeeded");
    assert.equal(segmentCalls[1].properties.payment_status, "succeeded", "an ambiguous 503 retry preserves the complete succeeded purchase payload");
    const record = JSON.parse(await gclidKv.get(segmentRetryDedupKey));
    assert.equal(record.delivery_state, "segment_acknowledged");
    assert.equal(queue.messages.filter((message) => message.payload.event_type === "ad_click_snapshot").length, 1);
    assert.equal(queue.messages[0].payload.snapshot.captured_at, "2026-06-01T12:00:00.000Z");

    const exactRetry = await send("succeeded");
    assert.equal(exactRetry.responseBody.deduped, true);
    assert.equal(segmentCalls.length, 2, "an acknowledged exact retry must not call Segment again");
    assert.equal(queue.messages.filter((message) => message.payload.event_type === "ad_click_snapshot").length, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await conversionSegmentFailureRemainsRetryableAndDoesNotFanOut();

async function preV4ConversionRecordRepairsTheBaseDelivery() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const gclidKv = new MockKV();
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      GCLID_KV: gclidKv,
    };
    const send = async (paymentStatus) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "test_server_secret" },
        body: JSON.stringify({
          type: "track",
          event: "OS_purchase",
          anonymousId: "pre-v4-repair-anon",
          userId: "pre-v4-repair-user",
          properties: {
            order_id: "pre-v4-repair-order",
            transaction_id: "pre-v4-repair-charge",
            product_id: "semaglutide",
            conversion_value: 138,
            currency: "USD",
            payment_status: paymentStatus,
          },
        }),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };

    const first = await send("pending");
    assert.equal(first.response.status, 200);
    const dedupKey = "dedup:OS_purchase:pre-v4-repair-order";
    const canonicalDedupKey = await canonicalConversionDedupKey("OS_purchase", "transaction_id", "pre-v4-repair-charge");
    const oldRecord = JSON.parse(await gclidKv.get(canonicalDedupKey));
    delete oldRecord.delivery_state;
    delete oldRecord.delivery_event;
    oldRecord.schema_version = "eden_conversion_dedup_v3";
    await gclidKv.put(dedupKey, JSON.stringify(oldRecord));
    await gclidKv.delete(canonicalDedupKey);
    env.CONVERSION_COORDINATOR = new MockConversionCoordinatorNamespace();
    segmentCalls.length = 0;

    const repaired = await send("succeeded");
    assert.equal(repaired.response.status, 200);
    assert.equal(repaired.responseBody.segment_forwarded, true);
    assert.equal(repaired.responseBody.conversion_enrichment_forwarded, true, "new lifecycle truth must survive an unknown-commit base repair without becoming a second conversion");
    assert.equal(segmentCalls.length, 2);
    assert.equal(segmentCalls[0].event, "OS_purchase");
    assertMixpanelSafeMessageId(segmentCalls[0], "OS_purchase:pre-v4-repair-charge");
    assert.notEqual(segmentCalls[0].properties.conversion_enrichment_only, true);
    assert.equal(segmentCalls[1].event, "OS_purchase_enrichment");
    assert.equal(segmentCalls[1].properties.payment_status, "succeeded");
    assert.equal(segmentCalls[1].properties.conversion_enrichment_only, true);
    assert.ok(segmentCalls[1].properties.conversion_enrichment_accepted_signal_keys.includes("property:payment_status"));
    assert.match(segmentSourceMessageId(segmentCalls[1]), /^eden_OS_purchase_enrichment_[a-f0-9]{32}$/);
    const repairedRecord = JSON.parse(await gclidKv.get(canonicalDedupKey));
    assert.equal(repairedRecord.schema_version, "eden_conversion_dedup_v4");
    assert.equal(repairedRecord.delivery_state, "segment_acknowledged");
    assert.equal(repairedRecord.delivery_event, "OS_purchase");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await preV4ConversionRecordRepairsTheBaseDelivery();

async function preV4RepairEnrichmentFailureNeverReplaysBase(failureStatus) {
  const segmentCalls = [];
  let failRepairEnrichmentOnce = false;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      const payload = JSON.parse(init.body);
      segmentCalls.push(payload);
      if (failRepairEnrichmentOnce && payload.event === "OS_purchase_enrichment") {
        failRepairEnrichmentOnce = false;
        return new Response("{}", { status: failureStatus });
      }
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const gclidKv = new MockKV();
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: gclidKv,
      CONVERSION_COORDINATOR: new MockConversionCoordinatorNamespace(),
    };
    const send = async (paymentStatus) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "test_server_secret" },
        body: JSON.stringify({
          type: "track",
          event: "OS_purchase",
          anonymousId: "repair-enrichment-retry-anon",
          userId: "repair-enrichment-retry-user",
          properties: {
            order_id: "repair-enrichment-retry-order",
            transaction_id: "repair-enrichment-retry-charge",
            payment_status: paymentStatus,
            product_id: "semaglutide",
            conversion_value: 138,
            currency: "USD",
          },
        }),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };

    await send("pending");
    const dedupKey = "dedup:OS_purchase:repair-enrichment-retry-order";
    const canonicalDedupKey = await canonicalConversionDedupKey("OS_purchase", "transaction_id", "repair-enrichment-retry-charge");
    const oldRecord = JSON.parse(await gclidKv.get(canonicalDedupKey));
    delete oldRecord.delivery_state;
    delete oldRecord.delivery_event;
    oldRecord.schema_version = "eden_conversion_dedup_v3";
    await gclidKv.put(dedupKey, JSON.stringify(oldRecord));
    await gclidKv.delete(canonicalDedupKey);
    env.CONVERSION_COORDINATOR = new MockConversionCoordinatorNamespace();
    segmentCalls.length = 0;
    failRepairEnrichmentOnce = true;

    const failed = await send("succeeded");
    assert.equal(failed.response.status, 503);
    assert.equal(failed.responseBody.error, "conversion_delivery_failed");
    assert.equal(failed.responseBody.segment_forwarded, true, "the successful base replay remains acknowledged even when its independent enrichment fails");
    assert.equal(JSON.parse(await gclidKv.get(dedupKey)).schema_version, "eden_conversion_dedup_v3", "failed correction delivery cannot commit the repaired ledger");
    assert.deepEqual(segmentCalls.map((call) => call.event), ["OS_purchase", "OS_purchase_enrichment"]);
    const failedIds = segmentCalls.map((call) => call.messageId);

    segmentCalls.length = 0;
    const retried = await send("succeeded");
    assert.equal(retried.response.status, 200);
    assert.deepEqual(segmentCalls.map((call) => call.event), ["OS_purchase_enrichment"], "an ambiguous enrichment response must never replay the base purchase");
    assert.equal(segmentCalls[0].messageId, failedIds[1], "the unknown enrichment retry must reuse only its own stable idempotency key");
    const repairedRecord = JSON.parse(await gclidKv.get(canonicalDedupKey));
    assert.equal(repairedRecord.delivery_state, "segment_acknowledged");
    assert.equal(repairedRecord.status_ranks["property:payment_status"], 3);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await preV4RepairEnrichmentFailureNeverReplaysBase(503);
await preV4RepairEnrichmentFailureNeverReplaysBase(400);

async function conversionQueueFailureRetriesWithStableObservationIdentity() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  class FailFirstQueue extends MockQueue {
    constructor() {
      super();
      this.attempts = [];
      this.failNext = true;
    }
    async send(payload, options = {}) {
      this.attempts.push({ payload, options });
      if (this.failNext) {
        this.failNext = false;
        throw new Error("fixture_conversion_queue_failure");
      }
      return super.send(payload, options);
    }
  }
  try {
    const gclidKv = new MockKV();
    await gclidKv.put("attr:anon:queue-retry-anon", JSON.stringify({
      gclid: "QUEUE-RETRY-GCLID-12345",
      utm_source: "google",
      utm_medium: "cpc",
      utm_campaign: "ORIGINAL-CAMPAIGN",
      gad_campaignid: "123456789",
    }));
    const queue = new FailFirstQueue();
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      GCLID_KV: gclidKv,
      AD_CLICK_KV: new MockKV(),
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const send = async ({
      includeClick = true,
      gclid = "QUEUE-RETRY-GCLID-12345",
      utmCampaign = "ORIGINAL-CAMPAIGN",
      gadCampaignId = "123456789",
      producerTimestamp = "2026-06-01T12:00:00.000Z",
    } = {}) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Eden-Server-Secret": "test_server_secret",
          Cookie: "eden_anonymous_id=queue-retry-anon; eden_session_id=queue-retry-session_1780000000000",
        },
        body: JSON.stringify({
          type: "track",
          event: "OS_purchase",
          timestamp: producerTimestamp,
          originalTimestamp: producerTimestamp,
          anonymousId: "queue-retry-anon",
          userId: "queue-retry-user",
          properties: {
            order_id: "queue-retry-order",
            transaction_id: "queue-retry-charge",
            product_id: "tirzepatide",
            conversion_value: 238,
            currency: "USD",
            payment_status: "succeeded",
            ...(includeClick ? { gclid } : {}),
            utm_source: "google",
            utm_medium: "cpc",
            utm_campaign: utmCampaign,
            gad_campaignid: gadCampaignId,
          },
          context: {
            page: {
              url: `https://app.eden.health/intake/checkout?${new URLSearchParams({
                ...(includeClick ? { gclid } : {}),
                utm_source: "google",
                utm_medium: "cpc",
                utm_campaign: utmCampaign,
                gad_campaignid: gadCampaignId,
              }).toString()}`,
            },
          },
        }),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };

    const failed = await send();
    assert.equal(failed.response.status, 503);
    assert.equal(failed.responseBody.error, "conversion_ad_click_persistence_failed");
    assert.equal(failed.responseBody.segment_forwarded, true);
    const queueRetryDedupKey = await canonicalConversionDedupKey("OS_purchase", "transaction_id", "queue-retry-charge");
    assert.equal(await gclidKv.get(queueRetryDedupKey), null, "queue failure cannot acknowledge the conversion KV mirror");
    assert.equal(queue.attempts.length, 1);

    await gclidKv.delete("attr:anon:queue-retry-anon");
    const missingEvidence = await send({ includeClick: false });
    assert.equal(missingEvidence.response.status, 409);
    assert.equal(missingEvidence.responseBody.error, "conversion_retry_state_incomplete_or_regressed");
    assert.equal(missingEvidence.responseBody.refresh_required, true);
    assert.equal(queue.attempts.length, 1, "missing original click evidence cannot finalize the pending Queue snapshot");
    assert.equal(segmentCalls.length, 1, "a pending-persistence retry must never replay the acknowledged purchase");

    const changedEvidence = await send({ gclid: "QUEUE-RETRY-CHANGED-GCLID-99999" });
    assert.equal(changedEvidence.response.status, 409);
    assert.equal(changedEvidence.responseBody.error, "conversion_retry_state_incomplete_or_regressed");
    assert.equal(changedEvidence.responseBody.refresh_required, true);
    assert.equal(queue.attempts.length, 1, "changed click evidence cannot replace the original pending snapshot");
    assert.equal(segmentCalls.length, 1);

    const changedCampaign = await send({ utmCampaign: "CHANGED-CAMPAIGN" });
    assert.equal(changedCampaign.response.status, 409);
    assert.equal(changedCampaign.responseBody.error, "conversion_retry_state_incomplete_or_regressed");
    assert.equal(queue.attempts.length, 1, "changed UTM campaign cannot mutate an acknowledged conversion's pending snapshot");
    assert.equal(segmentCalls.length, 1);

    const changedGoogleCampaign = await send({ gadCampaignId: "999999999" });
    assert.equal(changedGoogleCampaign.response.status, 409);
    assert.equal(changedGoogleCampaign.responseBody.error, "conversion_persistence_retry_incomplete_or_conflicting");
    assert.equal(changedGoogleCampaign.responseBody.persistence_retry_reason, "persistence_payload_fingerprint_mismatch");
    assert.equal(queue.attempts.length, 1, "changed Google campaign metadata cannot mutate the exact pending Queue envelope");
    assert.equal(segmentCalls.length, 1);

    await gclidKv.put("attr:anon:queue-retry-anon", JSON.stringify({
      gclid: "QUEUE-RETRY-GCLID-12345",
      utm_source: "google",
      utm_medium: "cpc",
      utm_campaign: "ORIGINAL-CAMPAIGN",
      gad_campaignid: "123456789",
    }));
    const retried = await send({ producerTimestamp: "2026-06-02T12:00:00.000Z" });
    assert.equal(retried.response.status, 200);
    assert.equal(retried.responseBody.conversion_segment_delivery_reused, true);
    assert.equal(queue.attempts.length, 2);
    assert.equal(queue.messages.length, 1);
    assert.equal(segmentCalls.length, 1, "the retry must resume after the acknowledged base conversion instead of replaying Segment");
    assert.equal(segmentCalls[0].event, "OS_purchase");
    assert.equal(queue.attempts[0].payload.ad_click_id, queue.attempts[1].payload.ad_click_id);
    assert.equal(queue.attempts[0].payload.snapshot.snapshot_id, queue.attempts[1].payload.snapshot.snapshot_id, "unknown-commit queue retries must reuse one deterministic snapshot identity");
    assert.deepEqual(queue.attempts[1].payload, queue.attempts[0].payload, "the successful retry must persist the exact original Queue envelope byte-for-byte");
    assert.equal(queue.messages[0].payload.snapshot.captured_at, "2026-06-01T12:00:00.000Z", "the pending snapshot keeps the first attempt's immutable capture time even if the retry request carries a later timestamp");
    assert.equal(queue.messages[0].payload.snapshot.campaign.utm_campaign, "ORIGINAL-CAMPAIGN");
    const record = JSON.parse(await gclidKv.get(queueRetryDedupKey));
    assert.equal(record.delivery_state, "segment_acknowledged");

    const exactRetry = await send();
    assert.equal(exactRetry.responseBody.deduped, true);
    assert.equal(queue.attempts.length, 2);
    assert.equal(segmentCalls.length, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await conversionQueueFailureRetriesWithStableObservationIdentity();

async function conversionRequiresReadableDedupeLedgerBeforeDelivery() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) segmentCalls.push(JSON.parse(init.body));
    return new Response("{}", { status: 200 });
  };
  const requestFor = (orderId) => new Request("https://collect.eden.health/server-collect", {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "test_server_secret" },
    body: JSON.stringify({
      type: "track",
      event: "OS_purchase",
      userId: "dedupe-ledger-user",
      properties: { order_id: orderId, transaction_id: `charge-${orderId}`, payment_status: "succeeded" },
    }),
  });
  try {
    const missing = await worker.fetch(requestFor("dedupe-ledger-missing"), {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: null,
    }, makeCtx());
    assert.equal(missing.status, 503);
    assert.equal((await missing.json()).error, "conversion_dedupe_unavailable");

    const missingCoordinator = await worker.fetch(requestFor("coordinator-missing"), {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: new MockKV(),
      CONVERSION_COORDINATOR: null,
    }, makeCtx());
    assert.equal(missingCoordinator.status, 503);
    assert.equal((await missingCoordinator.json()).error, "conversion_coordinator_unavailable");

    class FailReadKV extends MockKV {
      async get() { throw new Error("fixture_dedupe_read_failed"); }
    }
    const unreadable = await worker.fetch(requestFor("dedupe-ledger-unreadable"), {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: new FailReadKV(),
    }, makeCtx());
    assert.equal(unreadable.status, 503);
    assert.equal((await unreadable.json()).error, "conversion_dedupe_read_failed");
    assert.equal(segmentCalls.length, 0, "no business conversion may be sent under an ambiguous first-vs-enrichment state");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await conversionRequiresReadableDedupeLedgerBeforeDelivery();

async function conversionIdentifiersRequireUniqueBoundedScalars() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const gclidKv = new MockKV();
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: gclidKv,
    };
    const send = async (body) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "test_server_secret" },
        body: JSON.stringify(body),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };
    const base = {
      type: "track",
      event: "OS_purchase",
      properties: { payment_status: "succeeded", product_id: "semaglutide", conversion_value: 138, currency: "USD" },
    };

    const objectOrderA = await send({ ...base, userId: { id: "USER-A" }, properties: { ...base.properties, order_id: { id: "ORDER-A" }, transaction_id: "CHARGE-A" } });
    const objectOrderB = await send({ ...base, userId: { id: "USER-B" }, properties: { ...base.properties, order_id: { id: "ORDER-B" }, transaction_id: "CHARGE-B" } });
    assert.equal(objectOrderA.response.status, 200, JSON.stringify(objectOrderA.responseBody));
    assert.equal(objectOrderB.response.status, 200, JSON.stringify(objectOrderB.responseBody));
    assert.equal(segmentCalls.length, 2, "valid charge transactions survive malformed relationship aliases");
    assert.match(segmentCalls[0].anonymousId, /^eden_transaction_[a-f0-9]{32}$/);
    assert.match(segmentCalls[1].anonymousId, /^eden_transaction_[a-f0-9]{32}$/);
    assert.equal(JSON.stringify(segmentCalls.slice(0, 2)).includes("ORDER-A"), false);
    assert.equal(JSON.stringify(segmentCalls.slice(0, 2)).includes("ORDER-B"), false);
    assert.equal(JSON.stringify(segmentCalls.slice(0, 2)).includes("USER-A"), false);
    assert.equal(JSON.stringify(segmentCalls.slice(0, 2)).includes("USER-B"), false);
    assert.equal(gclidKv.putKeys.some((key) => key.includes("[object Object]")), false, "objects can never become shared dedupe or trusted-KV identifiers");

    const conflictingOrder = await send({
      ...base,
      properties: {
        ...base.properties,
        order_id: "ORDER-CONFLICT-A",
        transaction_id: "CHARGE-CONFLICT",
        healthos: { orderId: "ORDER-CONFLICT-B" },
      },
    });
    assert.equal(conflictingOrder.response.status, 200, JSON.stringify(conflictingOrder.responseBody));
    assert.equal(segmentCalls.length, 3);
    assertMixpanelSafeMessageId(segmentCalls.at(-1), "OS_purchase:CHARGE-CONFLICT");
    assert.equal(JSON.stringify(segmentCalls.at(-1)).includes("ORDER-CONFLICT-A"), false);
    assert.equal(JSON.stringify(segmentCalls.at(-1)).includes("ORDER-CONFLICT-B"), false);

    const validOrderMalformedUser = await send({
      ...base,
      userId: { id: "MALFORMED-USER" },
      properties: { ...base.properties, order_id: "ORDER-WITHOUT-VALID-USER", transaction_id: "CHARGE-WITHOUT-VALID-USER", user_id: { id: "ALSO-MALFORMED" } },
    });
    assert.equal(validOrderMalformedUser.response.status, 200);
    assert.equal(segmentCalls.length, 4);
    assert.equal(segmentCalls.at(-1).event, "OS_purchase");
    assertMixpanelSafeMessageId(segmentCalls.at(-1), "OS_purchase:CHARGE-WITHOUT-VALID-USER");
    assert.equal(segmentCalls.at(-1).userId, null);
    assert.match(segmentCalls.at(-1).anonymousId, /^eden_order_[a-f0-9]{32}$/);
    assert.equal(JSON.stringify(segmentCalls.at(-1)).includes("MALFORMED-USER"), false);
    assert.equal(await gclidKv.get("attr:server:v1:user:[object Object]"), null);

    const validOrderMalformedAnonymous = await send({
      ...base,
      properties: {
        ...base.properties,
        order_id: "ORDER-WITHOUT-VALID-ANONYMOUS",
        transaction_id: "CHARGE-WITHOUT-VALID-ANONYMOUS",
        anonymous_id: { id: "MALFORMED-ANONYMOUS" },
      },
    });
    assert.equal(validOrderMalformedAnonymous.response.status, 200);
    assert.match(segmentCalls.at(-1).anonymousId, /^eden_order_[a-f0-9]{32}$/);
    assert.equal(segmentCalls.at(-1).properties.anonymous_id, undefined);
    assert.equal(JSON.stringify(segmentCalls.at(-1)).includes("MALFORMED-ANONYMOUS"), false);

    const numericOrder = await send({
      ...base,
      userId: 9001,
      properties: {
        ...base.properties,
        order_id: 12345,
        master_id: "MASTER-999",
        patient_id: "PATIENT-42",
        ecommerce: { transaction_id: "PAYMENT-TRANSACTION-7" },
        healthos: { orderId: null },
      },
    });
    assert.equal(numericOrder.response.status, 200, "finite numeric IDs normalize to bounded scalar strings");
    assertMixpanelSafeMessageId(segmentCalls.at(-1), "OS_purchase:PAYMENT-TRANSACTION-7");
    assert.equal(numericOrder.responseBody.conversion_idempotency_key_source, "transaction_id");
    assert.equal(segmentCalls.at(-1).userId, "source:user_id:9001");

    const sendRaw = async (rawBody) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "test_server_secret" },
        body: rawBody,
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };
    const segmentCountBeforeUnsafeNumbers = segmentCalls.length;
    const unsafeNumericOrderA = await sendRaw('{"type":"track","event":"OS_purchase","userId":"unsafe-number-user-a","properties":{"order_id":9007199254740992,"payment_status":"succeeded"}}');
    const unsafeNumericOrderB = await sendRaw('{"type":"track","event":"OS_purchase","userId":"unsafe-number-user-b","properties":{"order_id":9007199254740993,"payment_status":"succeeded"}}');
    assert.equal(unsafeNumericOrderA.response.status, 422);
    assert.equal(unsafeNumericOrderA.responseBody.error, "conversion_idempotency_key_required");
    assert.equal(unsafeNumericOrderB.response.status, 422);
    assert.equal(unsafeNumericOrderB.responseBody.error, "conversion_idempotency_key_required");
    assert.equal(segmentCalls.length, segmentCountBeforeUnsafeNumbers, "rounded JSON integers cannot reach Segment");
    assert.equal([...gclidKv.map.keys()].some((key) => key.startsWith("dedup:OS_purchase:900719925474099")), false, "rounded JSON integers cannot collide in the dedupe ledger");

    const largeStringOrder = await send({
      ...base,
      userId: "9007199254740995",
      properties: { ...base.properties, order_id: "9007199254740993", transaction_id: "charge-9007199254740993" },
    });
    assert.equal(largeStringOrder.response.status, 200, "large exact identifiers remain valid when producers preserve them as strings");
    assertMixpanelSafeMessageId(segmentCalls.at(-1), "OS_purchase:charge-9007199254740993");
    assert.equal(segmentCalls.at(-1).userId, "source:user_id:9007199254740995");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await conversionIdentifiersRequireUniqueBoundedScalars();

async function conversionCoordinatorClassEnforcesTokenOwnedLease() {
  const records = new Map();
  const storage = {
    async get(key) { return records.get(key); },
    async transaction(callback) {
      return callback({
        get: async (key) => records.get(key),
        put: async (key, value) => records.set(key, value),
        delete: async (key) => records.delete(key),
      });
    },
  };
  const coordinator = new ConversionCoordinator({ storage });
  const call = (path, token, extra = {}) => coordinator.fetch(new Request(`https://conversion-coordinator.internal${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ token, event_name: "OS_purchase", lease_ttl_ms: 120_000, ...extra }),
  }));

  const first = await call("/acquire", "coordinator-token-one-1234");
  assert.equal(first.status, 200);
  assert.equal((await first.json()).acquired, true);
  const second = await call("/acquire", "coordinator-token-two-5678");
  assert.equal(second.status, 409, "a second request cannot enter the same conversion scope while the lease is live");
  assert.ok((await second.json()).retry_after_ms >= 250);
  assert.equal((await call("/release", "coordinator-token-two-5678")).status, 409, "only the lease owner may release it");
  const durableRecord = {
    schema_version: "eden_conversion_dedup_v4",
    event: "OS_purchase",
    signal_hashes: { "property:payment_status": "fixture-hash" },
    status_ranks: { "property:payment_status": 1 },
    delivery_state: "segment_delivery_unacknowledged",
  };
  assert.equal((await call("/record", "coordinator-token-one-1234", { record: durableRecord })).status, 200);
  assert.equal((await call("/release", "coordinator-token-one-1234")).status, 200);
  const reacquired = await call("/acquire", "coordinator-token-two-5678");
  assert.equal(reacquired.status, 200, "the scope is reusable immediately after an owned release");
  assert.deepEqual((await reacquired.json()).record, durableRecord, "the strongly consistent order record survives lease release");
}

await conversionCoordinatorClassEnforcesTokenOwnedLease();

async function concurrentConversionLifecycleIsSerializedWithoutLostTruth() {
  const segmentCalls = [];
  let segmentTimeoutSignalObserved = false;
  let releaseFirstDelivery;
  let markFirstDeliveryStarted;
  const firstDeliveryStarted = new Promise((resolve) => { markFirstDeliveryStarted = resolve; });
  const firstDeliveryRelease = new Promise((resolve) => { releaseFirstDelivery = resolve; });
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      const payload = JSON.parse(init.body);
      segmentCalls.push(payload);
      segmentTimeoutSignalObserved ||= !!init.signal;
      if (segmentCalls.length === 1) {
        markFirstDeliveryStarted();
        await firstDeliveryRelease;
      }
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const gclidKv = new MockKV();
    const coordinator = new MockConversionCoordinatorNamespace();
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: gclidKv,
      CONVERSION_COORDINATOR: coordinator,
    };
    const requestFor = (paymentStatus, event = "OS_purchase") => new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "test_server_secret" },
      body: JSON.stringify({
        type: "track",
        event,
        anonymousId: "concurrent-lifecycle-anon",
        userId: "concurrent-lifecycle-user",
        properties: {
          order_id: "concurrent-lifecycle-order",
          transaction_id: "concurrent-lifecycle-charge",
          payment_status: paymentStatus,
          product_id: "tirzepatide",
          conversion_value: 238,
          currency: "USD",
        },
      }),
    });

    const pendingCtx = makeCtx();
    const pendingPromise = worker.fetch(requestFor("pending"), env, pendingCtx);
    await firstDeliveryStarted;

    const concurrentCtx = makeCtx();
    const concurrent = await worker.fetch(requestFor("succeeded"), env, concurrentCtx);
    const concurrentBody = await concurrent.json();
    await Promise.all(concurrentCtx.promises);
    assert.equal(concurrent.status, 503);
    assert.equal(concurrentBody.error, "conversion_in_progress");
    assert.equal(concurrentBody.retryable, true);
    assert.equal(segmentCalls.length, 1, "the concurrent lifecycle event cannot race the base conversion into Segment");

    const milestoneCtx = makeCtx();
    const distinctMilestone = await worker.fetch(requestFor("authorized", "OS_qualified_first_order"), env, milestoneCtx);
    const distinctMilestoneBody = await distinctMilestone.json();
    await Promise.all(milestoneCtx.promises);
    assert.equal(distinctMilestone.status, 200, "qualification is a separate business milestone, not an OS_purchase alias");
    assert.equal(distinctMilestoneBody.segment_forwarded, true);
    assert.equal(segmentCalls.length, 2);
    assert.equal(segmentCalls[1].event, "OS_qualified_first_order");
    assert.equal(segmentCalls[1].properties.conversion_business_stage, "qualified_first_order");
    assertMixpanelSafeMessageId(segmentCalls[1], "eden_OS_qualified_first_order_concurrent-lifecycle-order");

    releaseFirstDelivery();
    const pending = await pendingPromise;
    assert.equal(pending.status, 200);
    await Promise.all(pendingCtx.promises);

    const retryCtx = makeCtx();
    const retry = await worker.fetch(requestFor("succeeded"), env, retryCtx);
    const retryBody = await retry.json();
    await Promise.all(retryCtx.promises);
    assert.equal(retry.status, 200);
    assert.equal(retryBody.conversion_enrichment_forwarded, true);
    assert.deepEqual(segmentCalls.map((payload) => payload.event), ["OS_purchase", "OS_qualified_first_order", "OS_purchase_enrichment"]);
    assert.equal(segmentCalls[0].properties.payment_status, "pending");
    assert.equal(segmentCalls[2].properties.payment_status, "succeeded");
    assert.notEqual(segmentCalls[0].messageId, segmentCalls[2].messageId);
    assert.equal(segmentTimeoutSignalObserved, true, "synchronous conversion delivery must be bounded inside the coordinator lease");
    const record = JSON.parse(await gclidKv.get(await canonicalConversionDedupKey("OS_purchase", "transaction_id", "concurrent-lifecycle-charge")));
    assert.equal(record.delivery_state, "segment_acknowledged");
    assert.equal(record.status_ranks["property:payment_status"], 3, "the retried lifecycle progress must survive after the base event commits");
  } finally {
    releaseFirstDelivery?.();
    globalThis.fetch = originalFetch;
  }
}

await concurrentConversionLifecycleIsSerializedWithoutLostTruth();

async function durableCoordinatorPreventsDuplicateWhenKvMirrorIsStale() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  class StaleDedupeMirrorKV extends MockKV {
    async get(key) {
      this.getKeys.push(key);
      if (String(key).startsWith("dedup:")) return null;
      return this.map.get(key) ?? null;
    }
  }
  try {
    const gclidKv = new StaleDedupeMirrorKV();
    const coordinator = new MockConversionCoordinatorNamespace();
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: gclidKv,
      CONVERSION_COORDINATOR: coordinator,
    };
    const makeRequest = () => new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "test_server_secret" },
      body: JSON.stringify({
        type: "track",
        event: "OS_purchase",
        anonymousId: "stale-kv-coordinator-anon",
        userId: "stale-kv-coordinator-user",
        properties: {
          order_id: "stale-kv-coordinator-order",
          transaction_id: "stale-kv-coordinator-charge",
          payment_status: "succeeded",
          product_id: "semaglutide",
          conversion_value: 138,
          currency: "USD",
        },
      }),
    });
    const first = await worker.fetch(makeRequest(), env, makeCtx());
    assert.equal(first.status, 200);
    const second = await worker.fetch(makeRequest(), env, makeCtx());
    const secondBody = await second.json();
    assert.equal(second.status, 200);
    assert.equal(secondBody.deduped, true);
    assert.equal(segmentCalls.length, 1, "the canonical Durable Object record must prevent a duplicate even when the KV mirror still reads null");
    assert.ok(gclidKv.putKeys.includes(await canonicalConversionDedupKey("OS_purchase", "transaction_id", "stale-kv-coordinator-charge")), "the raw-free transaction mirror remains observable even when reads are stale");
    assert.equal(gclidKv.putKeys.includes("dedup:OS_purchase:stale-kv-coordinator-order"), false, "OS_purchase must never write an order-scoped compatibility row");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await durableCoordinatorPreventsDuplicateWhenKvMirrorIsStale();

async function unknownCommitThenChangedStatusKeepsTheCorrection() {
  const segmentCalls = [];
  let failFirstAfterPossibleAcceptance = true;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      const payload = JSON.parse(init.body);
      segmentCalls.push(payload);
      if (failFirstAfterPossibleAcceptance) {
        failFirstAfterPossibleAcceptance = false;
        throw new TypeError("fixture_unknown_commit_after_request_write");
      }
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const gclidKv = new MockKV();
    const coordinator = new MockConversionCoordinatorNamespace();
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: gclidKv,
      CONVERSION_COORDINATOR: coordinator,
    };
    const send = async (paymentStatus, includeFullState = true) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "test_server_secret" },
        body: JSON.stringify({
          type: "track",
          event: "OS_purchase",
          anonymousId: "unknown-commit-correction-anon",
          userId: "unknown-commit-correction-user",
          properties: includeFullState ? {
            order_id: "unknown-commit-correction-order",
            transaction_id: "unknown-commit-correction-charge",
            payment_status: paymentStatus,
            product_id: "semaglutide",
            conversion_value: 138,
            currency: "USD",
          } : { order_id: "unknown-commit-correction-order", transaction_id: "unknown-commit-correction-charge" },
        }),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };

    const unknown = await send("pending");
    assert.equal(unknown.response.status, 503);
    assert.equal(unknown.responseBody.error, "conversion_delivery_failed");
    const unknownCommitDedupKey = await canonicalConversionDedupKey("OS_purchase", "transaction_id", "unknown-commit-correction-charge");
    assert.equal(await gclidKv.get(unknownCommitDedupKey), null);

    const incomplete = await send("succeeded", false);
    assert.equal(incomplete.response.status, 409);
    assert.equal(incomplete.responseBody.error, "conversion_retry_state_incomplete_or_regressed");
    assert.equal(incomplete.responseBody.refresh_required, true);
    assert.ok(incomplete.responseBody.missing_signal_count > 0);
    assert.equal(segmentCalls.length, 1, "an incomplete unknown-commit retry cannot erase attempted product/value/status truth");

    const corrected = await send("succeeded");
    assert.equal(corrected.response.status, 200);
    assert.equal(corrected.responseBody.conversion_enrichment_forwarded, true);
    assert.deepEqual(segmentCalls.map((payload) => payload.event), ["OS_purchase", "OS_purchase", "OS_purchase_enrichment"]);
    assert.equal(segmentCalls[0].properties.payment_status, "pending");
    assert.equal(segmentCalls[1].properties.payment_status, "pending", "unknown-commit base retry must replay the exact first attempted payload bytes");
    assert.equal(segmentCalls[2].properties.payment_status, "succeeded");
    assert.equal(segmentCalls[0].messageId, segmentCalls[1].messageId, "unknown-commit base retries reuse the exact conversion idempotency key");
    assert.notEqual(segmentCalls[1].messageId, segmentCalls[2].messageId, "the correction survives under its own non-conversion idempotency key");
    const record = JSON.parse(await gclidKv.get(unknownCommitDedupKey));
    assert.equal(record.delivery_state, "segment_acknowledged");
    assert.equal(record.status_ranks["property:payment_status"], 3);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await unknownCommitThenChangedStatusKeepsTheCorrection();

async function conflictingNestedServerClickClaimsAreFullyQuarantined() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const queue = new MockQueue();
    const ctx = makeCtx();
    const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Eden-Server-Secret": "test_server_secret",
        Cookie: "eden_anonymous_id=nested-conflict-anon; eden_session_id=nested-conflict-session_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "payment_authorized",
        anonymousId: "nested-conflict-anon",
        properties: {
          gclid: "NESTED-CONFLICT-GCLID-A",
          envelope: { attribution: { gclid: "NESTED-CONFLICT-GCLID-B" } },
        },
      }),
    }), {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      GCLID_KV: new MockKV(),
      AD_CLICK_KV: new MockKV(),
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    }, ctx);
    assert.equal(response.status, 200);
    await Promise.all(ctx.promises);
    assert.equal(segmentCalls.length, 1);
    assert.equal(segmentCalls[0].properties.gclid, undefined);
    assert.equal(segmentCalls[0].properties.envelope?.attribution?.gclid, undefined);
    assert.equal(segmentCalls[0].context.campaign?.gclid, undefined, "no arbitrary nested location may become campaign truth");
    assert.equal(queue.messages.length, 0, "one conflicting envelope cannot mint an ad-click object");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await conflictingNestedServerClickClaimsAreFullyQuarantined();

async function conversionClickConflictIsDiagnosticOnlyAndHistoryIsMonotonic() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const gclidKv = new MockKV();
    const queue = new MockQueue();
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      GCLID_KV: gclidKv,
      AD_CLICK_KV: new MockKV(),
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const send = async (gclid, paymentStatus) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Eden-Server-Secret": "test_server_secret",
          Cookie: "eden_anonymous_id=conversion-conflict-anon; eden_session_id=conversion-conflict-session_1780000000000",
        },
        body: JSON.stringify({
          type: "track",
          event: "OS_purchase",
          anonymousId: "conversion-conflict-anon",
          userId: "conversion-conflict-user",
          properties: {
            order_id: "conversion-conflict-order",
            transaction_id: "conversion-conflict-charge",
            product_id: "semaglutide",
            conversion_value: 138,
            currency: "USD",
            payment_status: paymentStatus,
            gclid,
          },
        }),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };

    await send("CONVERSION-CONFLICT-GCLID-A", "pending");
    const beforeConflict = queue.messages.length;
    const conflict = await send("CONVERSION-CONFLICT-GCLID-B", "authorized");
    assert.equal(conflict.response.status, 200);
    assert.equal(conflict.responseBody.conversion_enrichment_forwarded, true);
    const conflictEvent = segmentCalls.find((call) => call.event === "OS_purchase_enrichment" && call.properties.payment_status === "authorized");
    assert.ok(conflictEvent);
    assert.equal(conflictEvent.properties.gclid, "CONVERSION-CONFLICT-GCLID-A", "the order may retain its previously accepted recovered click");
    assert.notEqual(conflictEvent.properties.gclid, "CONVERSION-CONFLICT-GCLID-B", "the conflicting current click cannot replace order attribution");
    assert.equal(conflictEvent.context.campaign?.gclid, undefined);
    assert.ok(conflictEvent.properties.conversion_enrichment_current_conflicting_signal_keys.includes("native_click:gclid"));
    const diagnosticEnvelope = queue.messages.slice(beforeConflict)
      .map((message) => message.payload)
      .find((payload) => payload.snapshot?.google?.gclid === "CONVERSION-CONFLICT-GCLID-B");
    assert.ok(diagnosticEnvelope?.snapshot, "the conflicting click must remain available as an unowned diagnostic observation");
    assert.equal(diagnosticEnvelope.identity_links.length, 0);
    assert.equal(diagnosticEnvelope.snapshot.first_party, undefined);
    assert.equal(diagnosticEnvelope.snapshot.identity_refs, undefined);

    await send("CONVERSION-CONFLICT-GCLID-A", "succeeded");
    const record = JSON.parse(await gclidKv.get(await canonicalConversionDedupKey("OS_purchase", "transaction_id", "conversion-conflict-charge")));
    assert.ok(record.conflicting_signal_keys.includes("native_click:gclid"), "later clean progress cannot erase conflict history");
    assert.equal(record.status_ranks["property:payment_status"], 3);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await conversionClickConflictIsDiagnosticOnlyAndHistoryIsMonotonic();

async function ambiguousConflictingEnrichmentCanRetryExactly() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  const failedStatuses = new Set();
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      const payload = JSON.parse(init.body);
      segmentCalls.push(payload);
      const isConflictingEnrichment = payload.event === "OS_purchase_enrichment"
        && payload.properties?.conversion_enrichment_current_conflicting_signal_keys?.includes("native_click:gclid");
      const paymentStatus = payload.properties?.payment_status;
      if (isConflictingEnrichment && ["authorized", "succeeded"].includes(paymentStatus) && !failedStatuses.has(paymentStatus)) {
        failedStatuses.add(paymentStatus);
        return new Response("{}", { status: 503 });
      }
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const gclidKv = new MockKV();
    const coordinator = new MockConversionCoordinatorNamespace();
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      GCLID_KV: gclidKv,
      AD_CLICK_KV: new MockKV(),
      AD_CLICK_SNAPSHOT_QUEUE: new MockQueue(),
      CONVERSION_COORDINATOR: coordinator,
    };
    const send = async (gclid, paymentStatus) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Eden-Server-Secret": "test_server_secret",
          Cookie: "eden_anonymous_id=ambiguous-conflict-anon; eden_session_id=ambiguous-conflict-session_1780000000000",
        },
        body: JSON.stringify({
          type: "track",
          event: "OS_purchase",
          anonymousId: "ambiguous-conflict-anon",
          userId: "ambiguous-conflict-user",
          properties: {
            order_id: "ambiguous-conflict-order",
            transaction_id: "ambiguous-conflict-charge",
            product_id: "semaglutide",
            conversion_value: 138,
            currency: "USD",
            payment_status: paymentStatus,
            gclid,
          },
        }),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };

    const base = await send("AMBIGUOUS-CONFLICT-GCLID-A", "pending");
    assert.equal(base.response.status, 200);
    const ambiguous = await send("AMBIGUOUS-CONFLICT-GCLID-B", "authorized");
    assert.equal(ambiguous.response.status, 503, "an unknown enrichment commit remains retryable");
    const retried = await send("AMBIGUOUS-CONFLICT-GCLID-B", "authorized");
    assert.equal(retried.response.status, 200, "the exact conflicting enrichment retry must not be rejected as canonical-hash regression");
    assert.equal(retried.responseBody.conversion_enrichment_forwarded, true);

    const enrichments = segmentCalls.filter((call) => call.event === "OS_purchase_enrichment");
    assert.equal(enrichments.length, 2, "only the ambiguous enrichment is retried");
    assert.equal(enrichments[0].messageId, enrichments[1].messageId, "the exact retry reuses the enrichment idempotency key");
    assert.deepEqual(
      enrichments[0].properties.conversion_enrichment_accepted_signal_keys,
      enrichments[1].properties.conversion_enrichment_accepted_signal_keys,
      "the retry preserves the exact attempted signal set",
    );
    assert.ok(enrichments[1].properties.conversion_enrichment_accepted_signal_keys.includes("native_click:gclid"));

    const laterAmbiguous = await send("AMBIGUOUS-CONFLICT-GCLID-B", "succeeded");
    assert.equal(laterAmbiguous.response.status, 503, "a known conflict paired with new lifecycle truth remains retryable after ambiguity");
    const laterRetried = await send("AMBIGUOUS-CONFLICT-GCLID-B", "succeeded");
    assert.equal(laterRetried.response.status, 200, "the repeated known conflict must not dead-end a later status progression");
    const allEnrichments = segmentCalls.filter((call) => call.event === "OS_purchase_enrichment");
    assert.equal(allEnrichments.length, 4);
    assert.equal(allEnrichments[2].messageId, allEnrichments[3].messageId);
    assert.notEqual(allEnrichments[1].messageId, allEnrichments[3].messageId, "distinct lifecycle progress keeps a distinct enrichment idempotency key");
    assert.ok(allEnrichments[3].properties.conversion_enrichment_accepted_signal_keys.includes("native_click:gclid"));
    assert.ok(allEnrichments[3].properties.conversion_enrichment_accepted_signal_keys.includes("property:payment_status"));
    const record = JSON.parse(await gclidKv.get(await canonicalConversionDedupKey("OS_purchase", "transaction_id", "ambiguous-conflict-charge")));
    assert.ok(record.conflicting_signal_keys.includes("native_click:gclid"));
    assert.equal(record.status_ranks["property:payment_status"], 3);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await ambiguousConflictingEnrichmentCanRetryExactly();

async function forwardProgressSupersedesAmbiguousEnrichmentWithoutMutatingItsMessage() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  let failedAuthorized = false;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      const payload = JSON.parse(init.body);
      segmentCalls.push(payload);
      if (payload.event === "OS_purchase_enrichment"
        && payload.properties?.payment_status === "authorized"
        && !failedAuthorized) {
        failedAuthorized = true;
        return new Response("{}", { status: 503 });
      }
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const gclidKv = new MockKV();
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: gclidKv,
      CONVERSION_COORDINATOR: new MockConversionCoordinatorNamespace(),
    };
    const send = async (paymentStatus, timestamp) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Eden-Server-Secret": "test_server_secret",
          Cookie: "eden_anonymous_id=forward-supersede-anon; eden_session_id=forward-supersede-session_1780000000000",
        },
        body: JSON.stringify({
          type: "track",
          event: "OS_purchase",
          timestamp,
          anonymousId: "forward-supersede-anon",
          userId: "forward-supersede-user",
          properties: {
            order_id: "forward-supersede-order",
            transaction_id: "forward-supersede-charge",
            product_id: "semaglutide",
            conversion_value: 138,
            currency: "USD",
            payment_status: paymentStatus,
            gclid: "FORWARD-SUPERSEDE-GCLID-A",
          },
        }),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };

    assert.equal((await send("pending", "2026-06-03T12:00:00Z")).response.status, 200);
    const ambiguous = await send("authorized", "2026-06-03T13:00:00Z");
    assert.equal(ambiguous.response.status, 503);
    const equivalentAlias = await send("approved", "2026-06-03T14:00:00Z");
    assert.equal(equivalentAlias.response.status, 200, "a changed equal-rank status alias should supersede an ambiguous non-conversion enrichment");
    assert.equal(equivalentAlias.responseBody.conversion_enrichment_forwarded, true);
    assert.notEqual(equivalentAlias.responseBody.conversion_repair_enrichment_forwarded, true, "supersession is one current enrichment, not replay plus duplicate correction");

    const enrichments = segmentCalls.filter((call) => call.event === "OS_purchase_enrichment");
    assert.equal(enrichments.length, 2, "the ambiguous authorized attempt is followed by only one approved supersession");
    assert.equal(enrichments[0].properties.payment_status, "authorized");
    assert.equal(enrichments[1].properties.payment_status, "approved");
    assert.notEqual(enrichments[0].messageId, enrichments[1].messageId, "one Segment message ID can never carry two payloads");
    assert.equal(enrichments[0].timestamp, "2026-06-03T13:00:00.000Z");
    assert.equal(enrichments[1].timestamp, "2026-06-03T14:00:00.000Z", "the superseding truth keeps its own producer time");

    const succeeded = await send("succeeded", "2026-06-03T15:00:00Z");
    assert.equal(succeeded.response.status, 200, "a later higher-rank status remains a normal new enrichment");
    const finalEnrichments = segmentCalls.filter((call) => call.event === "OS_purchase_enrichment");
    assert.equal(finalEnrichments.length, 3);
    assert.equal(finalEnrichments[2].properties.payment_status, "succeeded");
    assert.notEqual(finalEnrichments[1].messageId, finalEnrichments[2].messageId);
    const record = JSON.parse(await gclidKv.get(await canonicalConversionDedupKey("OS_purchase", "transaction_id", "forward-supersede-charge")));
    assert.equal(record.status_ranks["property:payment_status"], 3);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await forwardProgressSupersedesAmbiguousEnrichmentWithoutMutatingItsMessage();

async function changedUnlistedBusinessPropertySupersedesAmbiguousEnrichment() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  let failedFirstEnrichment = false;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      const payload = JSON.parse(init.body);
      segmentCalls.push(payload);
      if (payload.event === "OS_purchase_enrichment" && !failedFirstEnrichment) {
        failedFirstEnrichment = true;
        return new Response("{}", { status: 503 });
      }
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const gclidKv = new MockKV();
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: gclidKv,
      CONVERSION_COORDINATOR: new MockConversionCoordinatorNamespace(),
    };
    const send = async (paymentStatus, clinicalOfferRevision) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "test_server_secret" },
        body: JSON.stringify({
          type: "track",
          event: "OS_purchase",
          anonymousId: "payload-fingerprint-anon",
          userId: "payload-fingerprint-user",
          properties: {
            order_id: "payload-fingerprint-order",
            transaction_id: "payload-fingerprint-charge",
            product_id: "semaglutide",
            conversion_value: 138,
            currency: "USD",
            payment_status: paymentStatus,
            clinical_offer_revision: clinicalOfferRevision,
          },
        }),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };

    assert.equal((await send("pending", "offer-base")).response.status, 200);
    const ambiguous = await send("authorized", "offer-a");
    assert.equal(ambiguous.response.status, 503);
    const superseded = await send("authorized", "offer-b");
    assert.equal(superseded.response.status, 200, "a changed forwarded property must supersede an ambiguous enrichment");
    assert.equal(superseded.responseBody.conversion_enrichment_forwarded, true);

    const enrichments = segmentCalls.filter((call) => call.event === "OS_purchase_enrichment");
    assert.equal(enrichments.length, 2);
    assert.equal(enrichments[0].properties.clinical_offer_revision, "offer-a");
    assert.equal(enrichments[1].properties.clinical_offer_revision, "offer-b");
    assert.notEqual(enrichments[0].messageId, enrichments[1].messageId, "one enrichment message ID cannot carry two arbitrary business payloads");
    const record = JSON.parse(await gclidKv.get(await canonicalConversionDedupKey("OS_purchase", "transaction_id", "payload-fingerprint-charge")));
    assert.equal(record.delivery_state, "segment_acknowledged");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await changedUnlistedBusinessPropertySupersedesAmbiguousEnrichment();

async function correctedConflictSupersedesAmbiguousDiagnosticEnrichment() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  let failedConflict = false;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      const payload = JSON.parse(init.body);
      segmentCalls.push(payload);
      if (payload.event === "OS_purchase_enrichment"
        && payload.properties?.conversion_enrichment_current_conflicting_signal_keys?.includes("native_click:gclid")
        && !failedConflict) {
        failedConflict = true;
        return new Response("{}", { status: 503 });
      }
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const gclidKv = new MockKV();
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: gclidKv,
      CONVERSION_COORDINATOR: new MockConversionCoordinatorNamespace(),
    };
    const send = async (gclid, paymentStatus) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "test_server_secret" },
        body: JSON.stringify({
          type: "track",
          event: "OS_purchase",
          anonymousId: "corrected-conflict-anon",
          userId: "corrected-conflict-user",
          properties: {
            order_id: "corrected-conflict-order",
            transaction_id: "corrected-conflict-charge",
            product_id: "semaglutide",
            conversion_value: 138,
            currency: "USD",
            payment_status: paymentStatus,
            gclid,
          },
        }),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };

    assert.equal((await send("CORRECTED-CONFLICT-GCLID-A", "pending")).response.status, 200);
    const ambiguous = await send("CORRECTED-CONFLICT-GCLID-B", "authorized");
    assert.equal(ambiguous.response.status, 503);
    const corrected = await send("CORRECTED-CONFLICT-GCLID-A", "succeeded");
    assert.equal(corrected.response.status, 200, "corrected canonical click evidence must not require replaying the known-wrong conflict");
    assert.equal(corrected.responseBody.conversion_enrichment_forwarded, true);

    const enrichments = segmentCalls.filter((call) => call.event === "OS_purchase_enrichment");
    assert.equal(enrichments.length, 2);
    assert.notEqual(enrichments[0].messageId, enrichments[1].messageId, "corrected diagnostic state supersedes the ambiguous conflict attempt");
    assert.equal(enrichments[1].properties.payment_status, "succeeded");
    assert.equal(enrichments[1].properties.gclid, "CORRECTED-CONFLICT-GCLID-A");
    assert.deepEqual(enrichments[1].properties.conversion_enrichment_current_conflicting_signal_keys, []);
    const record = JSON.parse(await gclidKv.get(await canonicalConversionDedupKey("OS_purchase", "transaction_id", "corrected-conflict-charge")));
    assert.ok(record.conflicting_signal_keys.includes("native_click:gclid"), "correction must retain the historical conflict without requiring its replay");
    assert.equal(record.status_ranks["property:payment_status"], 3);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await correctedConflictSupersedesAmbiguousDiagnosticEnrichment();

async function conversionIdentityConflictCannotAttachTheWrongCustomer() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const gclidKv = new MockKV();
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      GCLID_KV: gclidKv,
    };
    const send = async (userId, paymentStatus, extraProperties = {}) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Eden-Server-Secret": "test_server_secret",
          Cookie: "eden_anonymous_id=identity-order-anon; eden_session_id=identity-order-session_1780000000000",
        },
        body: JSON.stringify({
          type: "track",
          event: "OS_purchase",
          anonymousId: "identity-order-anon",
          userId,
          properties: {
            order_id: "identity-conflict-order",
            transaction_id: "identity-conflict-charge",
            product_id: "semaglutide",
            conversion_value: 138,
            currency: "USD",
            payment_status: paymentStatus,
            ...extraProperties,
          },
        }),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };

    const first = await send("identity-user-one", "pending");
    assert.equal(first.response.status, 200);
    const second = await send("identity-user-two", "authorized", {
      email: "wrong-attachment@example.com",
      phone: "+15555550123",
      first_name: "Wrong",
      postal_code: "10001",
    });
    assert.equal(second.response.status, 200);
    assert.equal(second.responseBody.conversion_enrichment_forwarded, true);
    const enrichment = segmentCalls.find((call) => call.event === "OS_purchase_enrichment");
    assert.ok(enrichment);
    assert.equal(enrichment.userId, null, "a conflicting customer claim cannot attach the order enrichment to the second user");
    assert.equal(enrichment.anonymousId, "identity-order-anon", "non-conflicting first-party anonymous continuity remains usable");
    assert.equal(enrichment.properties.user_id, undefined);
    assert.equal(enrichment.properties.email, undefined);
    assert.equal(enrichment.properties.email_sha256, undefined);
    assert.equal(enrichment.properties.phone, undefined);
    assert.equal(enrichment.properties.phone_sha256, undefined);
    assert.equal(enrichment.properties.first_name, undefined);
    assert.equal(enrichment.properties.postal_code, undefined);
    assert.ok(enrichment.properties.conversion_enrichment_current_conflicting_signal_keys.includes("identity:user_id"));
    assert.equal(await gclidKv.get("attr:server:v1:user:source:user_id:identity-user-two"), null, "the conflicting second user cannot receive trusted attribution continuity");
    const record = JSON.parse(await gclidKv.get(await canonicalConversionDedupKey("OS_purchase", "transaction_id", "identity-conflict-charge")));
    assert.ok(record.conflicting_signal_keys.includes("identity:user_id"));
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await conversionIdentityConflictCannotAttachTheWrongCustomer();

async function conflictingUserCannotImportTheirStoredCampaign() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const gclidKv = new MockKV();
    await gclidKv.put("attr:server:v1:user:source:user_id:stored-campaign-user-two", JSON.stringify({
      gclid: "WRONG-CUSTOMER-STORED-GCLID-B",
      utm_source: "google",
      utm_campaign: "wrong-customer-campaign",
    }));
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      GCLID_KV: gclidKv,
      CONVERSION_COORDINATOR: new MockConversionCoordinatorNamespace(),
    };
    const send = async (userId, paymentStatus, extraProperties = {}) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Eden-Server-Secret": "test_server_secret",
          Cookie: "eden_session_id=stored-campaign-session_1780000000000",
        },
        body: JSON.stringify({
          type: "track",
          event: "OS_purchase",
          userId,
          properties: {
            order_id: "stored-campaign-order",
            transaction_id: "stored-campaign-charge",
            product_id: "semaglutide",
            conversion_value: 138,
            currency: "USD",
            payment_status: paymentStatus,
            ...extraProperties,
          },
        }),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };

    assert.equal((await send("stored-campaign-user-one", "pending", { gclid: "RIGHT-TRANSACTION-GCLID-A" })).response.status, 200);
    const progressed = await send("stored-campaign-user-two", "authorized");
    assert.equal(progressed.response.status, 200);
    const enrichment = segmentCalls.find((call) => call.event === "OS_purchase_enrichment");
    assert.ok(enrichment);
    const serialized = JSON.stringify(enrichment);
    assert.equal(serialized.includes("WRONG-CUSTOMER-STORED-GCLID-B"), false, "the conflicting user's stored click cannot cross-attach");
    assert.equal(serialized.includes("wrong-customer-campaign"), false, "the conflicting user's stored campaign cannot cross-attach");
    assert.equal(enrichment.properties.gclid, "RIGHT-TRANSACTION-GCLID-A", "accepted order continuity remains available");
    assert.equal(enrichment.userId, null);
    assert.ok(enrichment.properties.conversion_enrichment_current_conflicting_signal_keys.includes("identity:user_id"));
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await conflictingUserCannotImportTheirStoredCampaign();

async function conflictingOrderClaimIsQuarantinedEverywhere() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const gclidKv = new MockKV();
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      GCLID_KV: gclidKv,
      CONVERSION_COORDINATOR: new MockConversionCoordinatorNamespace(),
    };
    const send = async (orderId, paymentStatus) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Eden-Server-Secret": "test_server_secret",
          Cookie: "eden_anonymous_id=order-claim-anon; eden_session_id=order-claim-session_1780000000000",
        },
        body: JSON.stringify({
          type: "track",
          event: "OS_purchase",
          anonymousId: "order-claim-anon",
          userId: "order-claim-user",
          properties: {
            order_id: orderId,
            transaction_id: "order-claim-charge",
            master_id: "order-claim-master",
            treatment_id: "order-claim-treatment",
            product_id: "tirzepatide",
            conversion_value: 238,
            currency: "USD",
            payment_status: paymentStatus,
            gclid: "ORDER-CLAIM-GCLID-A",
          },
        }),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };

    assert.equal((await send("trusted-order-one", "pending")).response.status, 200);
    const progressed = await send("conflicting-order-two", "authorized");
    assert.equal(progressed.response.status, 200);
    const enrichment = segmentCalls.find((call) => call.event === "OS_purchase_enrichment");
    assert.ok(enrichment);
    const serialized = JSON.stringify(enrichment);
    assert.equal(serialized.includes("conflicting-order-two"), false, "a conflicting order claim cannot reach Segment through any nested field");
    assert.equal(enrichment.properties.order_id, undefined);
    assert.equal(enrichment.properties.master_id, "order-claim-master", "master relationship evidence remains available");
    assert.equal(enrichment.properties.treatment_id, "order-claim-treatment", "treatment relationship evidence remains available");
    assert.ok(enrichment.properties.conversion_enrichment_current_conflicting_signal_keys.includes("identity:order_id"));
    assert.equal(
      gclidKv.putKeys.includes("attr:server:v1:order:conflicting-order-two"),
      false,
      "the conflicting order cannot receive trusted attribution continuity",
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await conflictingOrderClaimIsQuarantinedEverywhere();

async function conversionAnonymousConflictUsesOrderContinuityWithoutLeakingTheClaim() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const gclidKv = new MockKV();
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      GCLID_KV: gclidKv,
    };
    const send = async (anonymousId, paymentStatus) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "test_server_secret" },
        body: JSON.stringify({
          type: "track",
          event: "OS_purchase",
          anonymousId,
          userId: "anonymous-conflict-stable-user",
          properties: {
            order_id: "anonymous-conflict-order",
            transaction_id: "anonymous-conflict-charge",
            product_id: "tirzepatide",
            conversion_value: 238,
            currency: "USD",
            payment_status: paymentStatus,
            gclid: "ANONYMOUS-CONFLICT-GCLID-12345",
          },
        }),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };

    await send("anonymous-owner-one", "pending");
    const progressed = await send("anonymous-owner-two", "authorized");
    assert.equal(progressed.response.status, 200);
    const enrichment = segmentCalls.find((call) => call.event === "OS_purchase_enrichment");
    assert.ok(enrichment);
    assert.equal(enrichment.userId, "source:user_id:anonymous-conflict-stable-user", "a matching stable user remains usable even when the anonymous claim conflicts");
    assert.match(enrichment.anonymousId, /^eden_order_[a-f0-9]{32}$/);
    assert.equal(enrichment.anonymousId.includes("anonymous-owner-two"), false);
    assert.equal(JSON.stringify(enrichment).includes("anonymous-owner-two"), false, "the conflicting anonymous value must not re-enter through extended properties or context");
    assert.equal(enrichment.properties.first_party_device_id, undefined);
    assert.equal(enrichment.context.first_party_device_id, undefined);
    assert.equal(enrichment.context.campaign?.first_party_device_id, undefined);
    assert.ok(enrichment.properties.conversion_enrichment_current_conflicting_signal_keys.includes("identity:anonymous_id"));
    assert.ok(await gclidKv.get("attr:server:v1:user:source:user_id:anonymous-conflict-stable-user"), "the non-conflicting stable user may retain authenticated attribution continuity");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await conversionAnonymousConflictUsesOrderContinuityWithoutLeakingTheClaim();

async function anonymousConflictBeforeOrderCreationUsesStableUserContinuity() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const env = {
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: new MockKV(),
      CONVERSION_COORDINATOR: new MockConversionCoordinatorNamespace(),
    };
    const send = async (anonymousId, paymentStatus) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "test_server_secret" },
        body: JSON.stringify({
          type: "track",
          event: "OS_purchase",
          anonymousId,
          userId: "pre-order-stable-user",
          properties: {
            transaction_id: "pre-order-stable-charge",
            payment_status: paymentStatus,
            product_id: "semaglutide",
            conversion_value: 138,
            currency: "USD",
          },
        }),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };

    assert.equal((await send("pre-order-anon-one", "pending")).response.status, 200);
    const progressed = await send("pre-order-anon-two", "authorized");
    assert.equal(progressed.response.status, 200, "an anonymous-ID change cannot block payment progress when a stable user is present before order creation");
    const enrichment = segmentCalls.find((call) => call.event === "OS_purchase_enrichment");
    assert.ok(enrichment);
    assert.equal(enrichment.userId, "source:user_id:pre-order-stable-user");
    assert.match(enrichment.anonymousId, /^eden_user_[a-f0-9]{32}$/);
    assert.equal(JSON.stringify(enrichment).includes("pre-order-anon-two"), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await anonymousConflictBeforeOrderCreationUsesStableUserContinuity();

async function conflictingEdenIdentityIdsAreQuarantined() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const ctx = makeCtx();
    const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Eden-Server-Secret": "test_server_secret",
        "Cookie": "eden_anonymous_id=identity-conflict-anon; eden_session_id=identity-conflict-session_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "payment_authorized",
        anonymousId: "identity-conflict-anon",
        eden_identity_id: "eden_identity_one",
        userId: "source-user",
        traits: { edenIdentityId: "eden_identity_two" },
        properties: {
          ecommerce: { edenIdentityId: "eden_identity_two" },
          patient_id: "patient-source",
          customer_id: "customer-source",
          external_id: "external-source",
          email: "conflict@example.com",
          phone: "+15555550123",
          first_name: "Conflict",
          postal_code: "10001",
          payment_stage: "authorized",
        },
      }),
    }), { SERVER_API_SECRET: "test_server_secret", SEGMENT_WRITE_KEY: "fixture" }, ctx);
    assert.equal(response.status, 200);
    await Promise.all(ctx.promises);
    assert.equal(segmentCalls.length, 1, "anonymous continuity may still carry the non-conversion diagnostic event");
    const payload = segmentCalls[0];
    assert.equal(payload.userId, null, "conflicting canonical Eden IDs must quarantine stable attachment");
    assert.equal(payload.properties.eden_identity_id, undefined);
    assert.equal(payload.properties.ecommerce?.edenIdentityId, undefined, "alternate nested Eden ID locations must participate in conflict detection and quarantine");
    assert.equal(payload.properties.patient_id, undefined);
    assert.equal(payload.properties.customer_id, undefined);
    assert.equal(payload.properties.external_id, undefined);
    assert.equal(payload.properties.email, undefined);
    assert.equal(payload.properties.email_sha256, undefined);
    assert.equal(payload.properties.phone, undefined);
    assert.equal(payload.properties.phone_sha256, undefined);
    assert.equal(payload.properties.first_name, undefined);
    assert.equal(payload.properties.first_name_sha256, undefined);
    assert.equal(payload.properties.postal_code, undefined);
    assert.equal(payload.properties.payment_stage, "authorized");
    assert.equal(payload.properties.stable_identity_key_type, "eden_identity_id_conflict_quarantined");
    assert.equal(payload.properties.identity_warning, "conflicting_eden_identity_ids_quarantined");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await conflictingEdenIdentityIdsAreQuarantined();

async function oversizedEdenIdentityIdsCannotCollapseIntoOneClaim() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const sharedPrefix = "e".repeat(256);
    const ctx = makeCtx();
    const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Eden-Server-Secret": "test_server_secret",
        "Cookie": "eden_anonymous_id=oversized-identity-anon; eden_session_id=oversized-identity-session_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "payment_authorized",
        eden_identity_id: `${sharedPrefix}A`,
        traits: { edenIdentityId: `${sharedPrefix}B` },
        userId: "source-user-must-not-win",
        properties: { payment_stage: "authorized" },
      }),
    }), { SERVER_API_SECRET: "test_server_secret", SEGMENT_WRITE_KEY: "fixture" }, ctx);
    assert.equal(response.status, 200);
    await Promise.all(ctx.promises);
    assert.equal(segmentCalls.length, 1);
    assert.equal(segmentCalls[0].userId, null);
    assert.equal(segmentCalls[0].properties.stable_identity_key_type, "eden_identity_id_conflict_quarantined");
    assert.equal(segmentCalls[0].properties.identity_warning, "invalid_eden_identity_id_quarantined");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await oversizedEdenIdentityIdsCannotCollapseIntoOneClaim();

async function identify({ mode = "all" } = {}) {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push({ url: String(url), init, body: JSON.parse(init.body) });
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const kv = new MockKV();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: mode,
      SEGMENT_WRITE_KEY: "test_write_key",
      GCLID_KV: kv,
    };
    const ctx = makeCtx();
    const pageUrl = "https://app.eden.health/intake?gbraid=gbraid-identify&utm_source=google&utm_medium=cpc&utm_campaign=identify_context";
    const req = new Request("https://collect.eden.health/identify", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": "eden_anon_id=anon-identify; eden_session_id=session-identify_1780000000000; _gcl_aw=GCLAWIDENTIFY",
      },
      body: JSON.stringify({
        anonymousId: "anon-identify",
        userId: "user-identify",
        traits: {
          email: "identify_fixture@example.com",
          phone: "555.010.0102",
          first_name: "Identify",
          last_name: "Fixture",
          postal_code: "90210",
          country: "us",
          page_url: pageUrl,
        },
        context: {
          page: { url: pageUrl, path: "/intake", referrer: "https://www.google.com/" },
        },
      }),
    });
    const res = await worker.fetch(req, env, ctx);
    const responseBody = await res.json();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(segmentCalls.length, 0);
    return { responseBody, kv };
  } finally {
    globalThis.fetch = originalFetch;
  }
}

const identifyResult = await identify({ mode: "all" });
assert.equal(identifyResult.responseBody.stable_identity_accepted, false);
assert.equal(identifyResult.responseBody.identity_authority, "authenticated_server_collect_only");
assert.equal(await identifyResult.kv.get("id:link:user-identify"), null, "browser identify cannot write a stable user link");
assert.equal(await identifyResult.kv.get("email:user:user-identify"), null, "browser identify cannot write enhanced identity data");

async function collectWithSelfIdentify() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    const urlStr = String(url);
    if (urlStr.startsWith("https://api.segment.io/")) {
      segmentCalls.push({ url: urlStr, body: JSON.parse(init.body) });
      return new Response("{}", { status: 200 });
    }
    if (urlStr.includes("/identify")) {
      throw new Error(`public identify subrequest should not be used: ${urlStr}`);
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const kv = new MockKV();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      SEGMENT_WRITE_KEY: "test_write_key",
      GCLID_KV: kv,
    };
    const ctx = makeCtx();
    const pageUrl = "https://app.eden.health/intake?gclid=gclid-self&utm_source=google&utm_medium=cpc";
    const req = new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": "eden_anon_id=anon-self; eden_session_id=session-self_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "OS_purchase",
        anonymousId: "anon-self",
        userId: "user-self",
        properties: { order_id: "order-self", email: "self_identify@example.com" },
        context: { page: { url: pageUrl, path: "/intake", referrer: "https://www.google.com/" } },
      }),
    });
    const res = await worker.fetch(req, env, ctx);
    const responseBody = await res.json();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(responseBody.ok, true);
    const browserPurchase = segmentCalls.find((call) => call.url.endsWith("/track"))?.body;
    assert.ok(browserPurchase, "browser purchase signal must reach Segment for reconciliation");
    assert.equal(browserPurchase.event, "OS_purchase");
    assert.equal(browserPurchase.properties.source_type, "client");
    assert.equal(browserPurchase.properties.browser_conversion_observation, true);
    assert.equal(browserPurchase.properties.browser_event_authority, "provisional_observation");
    assert.equal(browserPurchase.userId, null, "browser purchase cannot assert a stable user");
    assert.equal(browserPurchase.properties.order_id, undefined, "browser purchase cannot assert a stable order identity");
    assert.equal(browserPurchase.properties.email, undefined, "browser purchase cannot assert direct contact identity");
    assert.equal(segmentCalls.filter((call) => call.url.endsWith("/identify")).length, 0);
    assert.equal(segmentCalls.filter((call) => call.url.endsWith("/alias")).length, 0);
    assert.equal(await kv.get("id:link:user-self"), null, "browser collect cannot write a stable user link");
    return segmentCalls;
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await collectWithSelfIdentify();

async function pageAttributionCookieCap() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      GCLID_KV: new MockKV(),
    };
    const ctx = makeCtx();
    const huge = "x".repeat(5000);
    const url = new URL("https://www.eden.health/");
    url.searchParams.set("gclid", "gclid-cookie");
    url.searchParams.set("gbraid", "gbraid-cookie");
    url.searchParams.set("wbraid", "wbraid-cookie");
    url.searchParams.set("gidrep", "gidrep-cookie");
    url.searchParams.set("gclsrc", "aw.ds");
    url.searchParams.set("gad_source", "1");
    url.searchParams.set("gad_campaignid", "123456789");
    url.searchParams.set("utm_source", "google");
    url.searchParams.set("utm_medium", "cpc");
    url.searchParams.set("utm_campaign", "cookie_cap");
    url.searchParams.set("sub1", huge);
    url.searchParams.set("gac_values", huge);
    const req = new Request(url.toString(), { headers: { "User-Agent": "Mozilla/5.0" } });
    const res = await worker.fetch(req, env, ctx);
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    const attrCookie = readCookieFromSetCookie(res.headers, "eden_attr");
    assert.ok(attrCookie, "expected eden_attr cookie");
    assert.ok(attrCookie.length <= 3500, `eden_attr cookie value too large: ${attrCookie.length}`);
    const parsed = JSON.parse(decodeURIComponent(attrCookie));
    assert.equal(parsed.gclid, "gclid-cookie");
    assert.equal(parsed.gbraid, "gbraid-cookie");
    assert.equal(parsed.wbraid, "wbraid-cookie");
    assert.equal(parsed.gidrep, "gidrep-cookie");
    assert.equal(parsed.gclsrc, "aw.ds");
    assert.equal(parsed.gad_source, "1");
    assert.equal(parsed.gad_campaignid, "123456789");
    assert.equal(parsed.utm_source, "google");
    assert.equal(parsed.utm_medium, "cpc");
    assert.equal(parsed.utm_campaign, "cookie_cap");
    assert.equal(parsed.sub1, undefined);
    assert.equal(parsed.gac_values, undefined);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await pageAttributionCookieCap();

async function adClickMemoryPageSmoke() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const kv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      AD_CLICK_KV: kv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const ctx = makeCtx();
    const req = new Request("https://www.eden.health/?gclid=GCLID-Case-Sensitive&gbraid=SECONDARY-GBRAID&wbraid=SECONDARY-WBRAID&utm_source=google&utm_medium=cpc&utm_campaign=ad_click_memory", {
      headers: { "User-Agent": "Mozilla/5.0" },
    });
    const res = await worker.fetch(req, env, ctx);
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    const pointerCookie = getSetCookie(res.headers).find((cookie) => cookie.startsWith("__Secure-eden_ad_click_id="));
    assert.ok(pointerCookie, "expected __Secure-eden_ad_click_id pointer cookie");
    assert.match(pointerCookie, /HttpOnly/);
    assert.match(pointerCookie, /Secure/);
    assert.match(pointerCookie, /SameSite=Lax/);
    assert.match(pointerCookie, /Domain=\.eden\.health/);
    assert.equal(pointerCookie.includes("GCLID-Case-Sensitive"), false, "pointer cookie must not contain raw click ID");
    assert.equal(queue.messages.length, 1);
    const envelope = queue.messages[0].payload;
    assert.equal(envelope.event_type, "ad_click_snapshot");
    assert.equal(envelope.source_pipeline_version, "5.56");
    assert.equal(envelope.snapshot.schema_version, "eden_ad_click_v1");
    assert.equal(envelope.snapshot.source_pipeline_version, "5.56");
    assert.match(envelope.snapshot.ad_click_id, /^adclk2_/);
    assert.equal(envelope.snapshot.google.gclid, "GCLID-Case-Sensitive");
    assert.equal(envelope.snapshot.google.gbraid, "SECONDARY-GBRAID");
    assert.equal(envelope.snapshot.google.wbraid, "SECONDARY-WBRAID");
    assert.equal(envelope.snapshot.landing_url_sanitized.includes("GCLID-Case-Sensitive"), false, "snapshot landing URL should redact raw click IDs");
    assert.equal(envelope.snapshot.evidence.primary_click_id_type, "gclid");
    assert.equal(envelope.snapshot.evidence.evidence_classes.gclid, "class_a_google_ads_upload_click_id");
    assert.equal(envelope.snapshot.governance.final_upload_eligibility_source, "dbt_google_outbox_validator");
    assert.equal(Object.prototype.hasOwnProperty.call(envelope.snapshot.governance, "allowed_for_google_upload"), false);
    const adClickKeys = [...kv.map.keys()].filter((key) => key.startsWith("adclick:"));
    assert.equal(adClickKeys.length, 1, "expected only the minimal adclick:id pointer backing record");
    assert.equal(adClickKeys[0], `adclick:id:${envelope.snapshot.ad_click_id}`);
    assert.equal(adClickKeys.some((key) => key.includes("GCLID-Case-Sensitive")), false, "new adclick KV keys must not contain raw click IDs");

    const repeatAnonId = readCookieFromSetCookie(res.headers, "eden_anonymous_id");
    const repeatSessionId = readCookieFromSetCookie(res.headers, "eden_session_id");
    assert.ok(repeatAnonId && repeatSessionId, "first response should establish first-party owner cookies");
    const repeatCtx = makeCtx();
    const repeatRes = await worker.fetch(new Request("https://www.eden.health/?gclid=GCLID-Case-Sensitive&utm_source=google&utm_medium=cpc", {
      headers: {
        "User-Agent": "Mozilla/5.0",
        "Cookie": `eden_anonymous_id=${repeatAnonId}; eden_anon_id=${repeatAnonId}; eden_session_id=${repeatSessionId}`,
      },
    }), env, repeatCtx);
    await repeatRes.text();
    await Promise.all(repeatCtx.promises);
    assert.equal(queue.messages.length, 2, "each independently built native observation must append its own immutable snapshot");
    const repeatEnvelope = queue.messages[1].payload;
    assert.equal(repeatEnvelope.ad_click_id, envelope.ad_click_id, "multiple observations may share one owner-scoped ad_click_id");
    assert.notEqual(repeatEnvelope.snapshot.snapshot_id, envelope.snapshot.snapshot_id, "independent native observations must never reuse one snapshot_id");
    assert.equal(readCookieFromSetCookie(repeatRes.headers, "__Secure-eden_ad_click_id"), envelope.snapshot.ad_click_id, "same owner repeating the same GCLID must reuse the same pointer identity");
    assert.equal(readCookieFromSetCookie(repeatRes.headers, "eden_session_id"), repeatSessionId, "active page requests must refresh the same inactivity-based session id");
    assert.equal(JSON.parse(await kv.get(`adclick:id:${envelope.snapshot.ad_click_id}`)).snapshot_id, repeatEnvelope.snapshot.snapshot_id, "pointer backing should advance to the latest append-only observation");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickMemoryPageSmoke();

async function queueCustodyFailureCannotPublishPointer() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("origin ok", {
    status: 200,
    headers: { "content-type": "text/plain" },
  });
  try {
    const gclidKv = new MockKV();
    const adClickKv = new MockKV();
    const coordinator = new MockConversionCoordinatorNamespace();
    const failingQueue = {
      async send() { throw new Error("fixture_queue_custody_failure"); },
    };
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      GCLID_KV: gclidKv,
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: failingQueue,
      CONVERSION_COORDINATOR: coordinator,
    };

    const pageCtx = makeCtx();
    const pageResponse = await worker.fetch(new Request(
      "https://www.eden.health/?gclid=QUEUE-CUSTODY-PAGE-GCLID&gbraid=QUEUE-CUSTODY-PAGE-GBRAID&utm_source=google&utm_medium=cpc",
      {
        headers: {
          "User-Agent": "Mozilla/5.0",
          Cookie: "eden_anonymous_id=queue-custody-anon; eden_session_id=queue-custody-session_1780000000000",
        },
      },
    ), env, pageCtx);
    await pageResponse.text();
    await Promise.all(pageCtx.promises);
    assert.equal(pageResponse.status, 200, "capture failure must not take the public origin down");
    assert.equal(readCookieFromSetCookie(pageResponse.headers, "__Secure-eden_ad_click_id"), null, "a page must not publish a pointer without Queue custody");
    assert.equal(coordinator.pointerRecords.size, 0, "a failed page enqueue must leave no active canonical pointer");
    assert.equal(coordinator.pointerReservations.size, 0, "a failed page enqueue must cancel its reservation");
    assert.equal([...adClickKv.map.keys()].some((key) => key.startsWith("adclick:id:")), false, "a failed page enqueue must not publish pointer cache state");

    const collectCtx = makeCtx();
    const collectResponse = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://app.eden.health",
        Cookie: "eden_anonymous_id=queue-custody-anon; eden_session_id=queue-custody-session_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "queue_custody_mutation_probe",
        anonymousId: "queue-custody-anon",
        context: { page: { url: "https://app.eden.health/intake?gclid=QUEUE-CUSTODY-COLLECT-GCLID&gbraid=QUEUE-CUSTODY-COLLECT-GBRAID&utm_source=google" } },
      }),
    }), env, collectCtx);
    const collectBody = await collectResponse.json();
    await Promise.all(collectCtx.promises);
    assert.equal(collectResponse.status, 503, "mutation capture must be retryable when Queue custody fails");
    assert.equal(collectBody.error, "ad_click_memory_custody_unavailable");
    assert.equal(collectBody.retryable, true);
    assert.equal(readCookieFromSetCookie(collectResponse.headers, "__Secure-eden_ad_click_id"), null, "a failed mutation enqueue must not publish a pointer cookie");
    assert.equal(coordinator.pointerRecords.size, 0, "a failed mutation enqueue must leave no active canonical pointer");
    assert.equal(coordinator.pointerReservations.size, 0, "a failed mutation enqueue must cancel its reservation");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await queueCustodyFailureCannotPublishPointer();

async function conversionBuildFailureCannotAcknowledgePurchase() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const coordinator = new MockConversionCoordinatorNamespace();
    const failingPointerCoordinator = {
      idFromName: (name) => coordinator.idFromName(name),
      get(id) {
        const delegate = coordinator.get(id);
        return {
          fetch: async (input, init = {}) => {
            const request = input instanceof Request ? input : new Request(input, init);
            if (new URL(request.url).pathname.startsWith("/pointer/")) {
              throw new Error("fixture_pointer_dependency_unavailable");
            }
            return delegate.fetch(request);
          },
        };
      },
    };
    const busyPointerCoordinator = {
      idFromName: (name) => coordinator.idFromName(name),
      get(id) {
        const delegate = coordinator.get(id);
        return {
          fetch: async (input, init = {}) => {
            const request = input instanceof Request ? input : new Request(input, init);
            if (new URL(request.url).pathname === "/pointer/reserve") {
              return new Response(JSON.stringify({
                ok: false,
                reserved: false,
                error: "pointer_reservation_busy",
              }), { status: 409, headers: { "Content-Type": "application/json" } });
            }
            return delegate.fetch(request);
          },
        };
      },
    };
    const gclidKv = new MockKV();
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const baseEnv = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      GCLID_KV: gclidKv,
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const purchaseRequest = (transactionId, withEvidence = true) => new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Eden-Server-Secret": TEST_SERVER_API_SECRET,
      },
      body: JSON.stringify({
        type: "track",
        event: "OS_purchase",
        anonymousId: `build-failure-anon-${transactionId}`,
        userId: `build-failure-user-${transactionId}`,
        messageId: `OS_purchase:${transactionId}`,
        properties: {
          transaction_id: transactionId,
          order_id: `build-failure-order-${transactionId}`,
          payment_status: "authorized",
          ...(withEvidence ? {
            gclid: "BUILD-FAILURE-GCLID",
            gbraid: "BUILD-FAILURE-GBRAID",
            utm_source: "google",
            utm_medium: "cpc",
          } : {}),
        },
        context: withEvidence ? {
          page: { url: "https://app.eden.health/intake?gclid=BUILD-FAILURE-GCLID&gbraid=BUILD-FAILURE-GBRAID&utm_source=google&utm_medium=cpc" },
        } : {},
      }),
    });

    const failedCtx = makeCtx();
    const failed = await worker.fetch(purchaseRequest("build-failure-charge"), {
      ...baseEnv,
      CONVERSION_COORDINATOR: failingPointerCoordinator,
    }, failedCtx);
    const failedBody = await failed.json();
    await Promise.all(failedCtx.promises);
    assert.equal(failed.status, 503, "a conversion with ad evidence must fail before Segment when pointer construction is unavailable");
    assert.equal(failedBody.error, "conversion_ad_click_memory_build_unavailable");
    assert.equal(failedBody.retryable, true);
    assert.equal(failedBody.segment_forwarded, false);
    assert.equal(segmentCalls.length, 0, "the failed build must not deliver or acknowledge the purchase in Segment");
    assert.equal(queue.messages.length, 0, "the failed build must not pretend Queue custody exists");

    const segmentCallsBeforeBusy = segmentCalls.length;
    const busyCtx = makeCtx();
    const busy = await worker.fetch(purchaseRequest("busy-reservation-charge"), {
      ...baseEnv,
      CONVERSION_COORDINATOR: busyPointerCoordinator,
    }, busyCtx);
    const busyBody = await busy.json();
    await Promise.all(busyCtx.promises);
    assert.equal(busy.status, 503, "a busy pointer reservation must be retryable before Segment");
    assert.equal(busyBody.error, "conversion_ad_click_memory_build_unavailable");
    assert.equal(busyBody.retryable, true);
    assert.equal(busyBody.segment_forwarded, false);
    assert.equal(segmentCalls.length, segmentCallsBeforeBusy, "reservation busy must not deliver or acknowledge the purchase in Segment");
    assert.equal(queue.messages.length, 0, "reservation busy must not pretend Queue custody exists");

    const busyRetryCtx = makeCtx();
    const busyRetry = await worker.fetch(purchaseRequest("busy-reservation-charge"), {
      ...baseEnv,
      CONVERSION_COORDINATOR: coordinator,
    }, busyRetryCtx);
    const busyRetryBody = await busyRetry.json();
    await Promise.all(busyRetryCtx.promises);
    assert.equal(busyRetry.status, 200, "a busy reservation retry must succeed after the reservation clears");
    assert.equal(busyRetryBody.segment_forwarded, true);
    assert.equal(
      segmentCalls.filter((payload) => payload.properties?.transaction_id === "busy-reservation-charge").length,
      1,
      "the cleared reservation must deliver exactly one purchase",
    );
    assert.equal(queue.messages.length, 1, "the cleared reservation must obtain Queue custody");

    const retryCtx = makeCtx();
    const retry = await worker.fetch(purchaseRequest("build-failure-charge"), {
      ...baseEnv,
      CONVERSION_COORDINATOR: coordinator,
    }, retryCtx);
    const retryBody = await retry.json();
    await Promise.all(retryCtx.promises);
    assert.equal(retry.status, 200, "the same stable conversion must succeed after the pointer dependency recovers");
    assert.equal(retryBody.segment_forwarded, true);
    assert.equal(
      segmentCalls.filter((payload) => payload.properties?.transaction_id === "build-failure-charge").length,
      1,
      "recovery must deliver exactly one purchase",
    );
    assert.equal(queue.messages.length, 2, "recovery must obtain Queue custody for the purchase evidence");

    const noEvidenceCtx = makeCtx();
    const noEvidence = await worker.fetch(purchaseRequest("no-evidence-charge", false), {
      ...baseEnv,
      CONVERSION_COORDINATOR: failingPointerCoordinator,
    }, noEvidenceCtx);
    const noEvidenceBody = await noEvidence.json();
    await Promise.all(noEvidenceCtx.promises);
    assert.equal(noEvidence.status, 200, "a healthy no-candidate result may succeed even when no pointer route is needed");
    assert.equal(noEvidenceBody.segment_forwarded, true);
    assert.equal(queue.messages.length, 2, "a no-evidence purchase must not fabricate an ad-click snapshot");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await conversionBuildFailureCannotAcknowledgePurchase();

async function webflowNativeObservationSurvivesOrigin5xxWithoutHealthOS() {
  const originalFetch = globalThis.fetch;
  const gclidKv = new MockKV();
  const adClickKv = new MockKV();
  const queue = new MockQueue();
  const ctx = makeCtx();
  const originHosts = [];
  let persistenceDurableBeforeOrigin = false;
  const synthetic = {
    gclid: `GCLID-${crypto.randomUUID()}`,
    gbraid: `GBRAID-${crypto.randomUUID()}`,
    wbraid: `WBRAID-${crypto.randomUUID()}`,
  };
  globalThis.fetch = async (request) => {
    originHosts.push(new URL(request.url || request).hostname);
    persistenceDurableBeforeOrigin = queue.messages.length === 1 && adClickKv.map.size > 0;
    return new Response("origin unavailable", { status: 503, headers: { "content-type": "text/html" } });
  };
  try {
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      GCLID_KV: gclidKv,
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const landing = new URL("https://www.eden.health/");
    landing.searchParams.set("gclid", synthetic.gclid);
    landing.searchParams.set("gbraid", synthetic.gbraid);
    landing.searchParams.set("wbraid", synthetic.wbraid);
    landing.searchParams.set("utm_source", "google");
    landing.searchParams.set("utm_medium", "cpc");
    landing.searchParams.set("utm_campaign", "webflow_origin_5xx_independence");
    const response = await worker.fetch(new Request(landing, {
      headers: { "User-Agent": "Mozilla/5.0" },
    }), env, ctx);
    assert.equal(response.status, 503, "origin 5xx must remain visible rather than being masked by capture");
    assert.equal(persistenceDurableBeforeOrigin, true, "Webflow capture must obtain Queue custody and commit the pointer before calling the origin");
    assert.deepEqual(originHosts, ["www.eden.health"], "the independence fixture must not request app.eden.health or any legacy domain");

    const anonId = readCookieFromSetCookie(response.headers, "eden_anonymous_id");
    const sessionId = readCookieFromSetCookie(response.headers, "eden_session_id");
    const pointerId = readCookieFromSetCookie(response.headers, "__Secure-eden_ad_click_id");
    assert.ok(anonId && sessionId && pointerId, "origin 5xx must still return first-party owner/session/pointer continuity");
    for (const name of ["eden_anonymous_id", "eden_anon_id", "eden_session_id", "eden_attr", "__Secure-eden_ad_click_id"]) {
      const cookie = getSetCookie(response.headers).find((candidate) => candidate.startsWith(`${name}=`));
      assert.ok(cookie && /Domain=\.eden\.health/.test(cookie), `${name} must remain scoped to the eden.health registrable domain`);
    }

    await Promise.all(ctx.promises);
    const snapshotEnvelope = queue.messages.find((message) => message.payload.event_type === "ad_click_snapshot")?.payload;
    assert.ok(snapshotEnvelope?.snapshot, "origin 5xx must not prevent durable native-observation enqueue");
    assert.equal(snapshotEnvelope.snapshot.first_party.eden_anonymous_id, anonId, "queued observation must be owner-scoped");
    assert.equal(snapshotEnvelope.snapshot.first_party.eden_session_id, sessionId, "queued observation must retain the first-party session");
    assert.equal(await sha256Raw(snapshotEnvelope.snapshot.google.gclid), await sha256Raw(synthetic.gclid), "primary evidence must survive without logging its raw value");
    assert.deepEqual(
      [...snapshotEnvelope.snapshot.evidence.upload_candidate_types].sort(),
      ["gclid", "gbraid", "wbraid"].sort(),
      "all valid Google upload evidence types must survive the independent Webflow capture",
    );
    assert.ok(await gclidKv.get(`attr:anon:${anonId}`), "owner-scoped attribution KV must persist despite origin 5xx");
    const pointerRecord = JSON.parse(await adClickKv.get(`adclick:id:${pointerId}`));
    assert.equal(pointerRecord.ad_click_id, pointerId, "owner-bound pointer KV must persist despite origin 5xx");
    assert.equal(pointerRecord.snapshot_id, snapshotEnvelope.snapshot.snapshot_id, "pointer KV must reference the enqueued immutable observation");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await webflowNativeObservationSurvivesOrigin5xxWithoutHealthOS();

async function webflowNativeObservationSurvivesRejectedOriginFetchWithoutHealthOS() {
  const originalFetch = globalThis.fetch;
  const gclidKv = new MockKV();
  const adClickKv = new MockKV();
  const queue = new MockQueue();
  const ctx = makeCtx();
  let persistenceDurableBeforeOrigin = false;
  const syntheticGclid = `GCLID-${crypto.randomUUID()}`;
  globalThis.fetch = async () => {
    persistenceDurableBeforeOrigin = queue.messages.length === 1 && adClickKv.map.size > 0;
    throw new Error("fixture_webflow_connection_rejected");
  };
  try {
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      GCLID_KV: gclidKv,
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const landing = new URL("https://www.eden.health/");
    landing.searchParams.set("gclid", syntheticGclid);
    landing.searchParams.set("utm_source", "google");
    landing.searchParams.set("utm_medium", "cpc");
    landing.searchParams.set("utm_campaign", "webflow_origin_rejection_independence");
    const response = await worker.fetch(new Request(landing, {
      headers: { "User-Agent": "Mozilla/5.0" },
    }), env, ctx);
    assert.equal(response.status, 502, "a rejected Webflow origin fetch must become a controlled gateway response");
    assert.equal(response.headers.get("cache-control"), "no-store");
    assert.equal(persistenceDurableBeforeOrigin, true, "Queue custody and pointer commit must precede a rejecting origin fetch");

    const anonId = readCookieFromSetCookie(response.headers, "eden_anonymous_id");
    const sessionId = readCookieFromSetCookie(response.headers, "eden_session_id");
    const pointerId = readCookieFromSetCookie(response.headers, "__Secure-eden_ad_click_id");
    assert.ok(anonId && sessionId && pointerId, "origin rejection must still return owner/session/pointer continuity cookies");
    await Promise.all(ctx.promises);
    const snapshotEnvelope = queue.messages.find((message) => message.payload.event_type === "ad_click_snapshot")?.payload;
    assert.ok(snapshotEnvelope?.snapshot, "origin rejection must not cancel the independent immutable observation");
    assert.equal(snapshotEnvelope.snapshot.first_party.eden_anonymous_id, anonId);
    assert.equal(snapshotEnvelope.snapshot.first_party.eden_session_id, sessionId);
    assert.equal(await sha256Raw(snapshotEnvelope.snapshot.google.gclid), await sha256Raw(syntheticGclid));
    assert.ok(await gclidKv.get(`attr:anon:${anonId}`), "owner attribution KV must survive an origin connection rejection");
    assert.equal(JSON.parse(await adClickKv.get(`adclick:id:${pointerId}`)).ad_click_id, pointerId);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await webflowNativeObservationSurvivesRejectedOriginFetchWithoutHealthOS();

async function authenticatedBrowserFragmentCaptureIsIndependentOfSegmentAndHealthOS() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200 });
  try {
    const gclidKv = new MockKV();
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      GCLID_KV: gclidKv,
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const ctx = makeCtx();
    const response = await worker.fetch(new Request("https://www.eden.health/preserve-attribution", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://www.eden.health",
        "Cookie": "eden_anonymous_id=fragment-anon; eden_anon_id=fragment-anon; eden_session_id=fragment-session_1780000000000",
      },
      body: JSON.stringify({
        anonymousId: "fragment-anon",
        pageUrl: "https://www.eden.health/#gclid=FRAGMENT-GCLID&gbraid=FRAGMENT-GBRAID&wbraid=FRAGMENT-WBRAID&utm_source=google&utm_medium=cpc&utm_campaign=fragment-marker-local",
      }),
    }), env, ctx);
    const responseBody = await response.json();
    assert.equal(queue.messages.filter((message) => message.payload.event_type === "ad_click_snapshot").length, 1, "preserve response must not return before the fragment observation is durably enqueued");
    assert.ok(await adClickKv.get(`adclick:id:${queue.messages[0].payload.ad_click_id}`), "preserve response must not return before pointer KV persistence");
    await Promise.all(ctx.promises);
    assert.equal(response.status, 200);
    assert.equal(responseBody.ok, true);
    assert.equal(responseBody.ad_click_observation_persisted, true);
    assert.equal(responseBody.queue_enqueued, true);
    assert.equal(responseBody.pointer_kv_persisted, true);
    assert.equal(responseBody.owner_attribution_kv_persisted, true);
    assert.equal(readCookieFromSetCookie(response.headers, "eden_session_id"), "fragment-session_1780000000000");
    const attrCookie = JSON.parse(decodeURIComponent(readCookieFromSetCookie(response.headers, "eden_attr")));
    assert.equal(attrCookie.gclid, "FRAGMENT-GCLID");
    assert.equal(attrCookie.gbraid, "FRAGMENT-GBRAID");
    assert.equal(attrCookie.wbraid, "FRAGMENT-WBRAID");
    const envelope = queue.messages.find((message) => message.payload.event_type === "ad_click_snapshot")?.payload;
    assert.ok(envelope, "browser-only fragment evidence must reach the independent Queue/BigQuery lane before HealthOS");
    assert.equal(envelope.snapshot.evidence.primary_click_id_type, "gclid");
    assert.equal(envelope.snapshot.google.gclid, "FRAGMENT-GCLID");
    assert.equal(envelope.snapshot.campaign.utm_campaign, "fragment-marker-local");
    assert.equal(envelope.snapshot.landing_url_sanitized.includes("FRAGMENT-GCLID"), false);
    assert.ok(envelope.identity_links.some((link) => link.from_type === "session_id" || link.to_type === "session_id"), "browser preserve must link the active session");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await authenticatedBrowserFragmentCaptureIsIndependentOfSegmentAndHealthOS();

async function internalWebflowHandoffPreservesNewerFragmentPointer() {
  const originalFetch = globalThis.fetch;
  const segmentCalls = [];
  const originRequests = [];
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    originRequests.push(url instanceof Request ? url.url : String(url));
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const gclidKv = new MockKV();
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      EDEN_AD_CLICK_KV_INDEX_MODE: "pointer",
      SEGMENT_WRITE_KEY: "test_write_key",
      GCLID_KV: gclidKv,
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };

    const landingCtx = makeCtx();
    const landingResponse = await worker.fetch(new Request(
      "https://www.eden.health/?gclid=HANDOFF-INITIAL-GCLID&gbraid=HANDOFF-INITIAL-GBRAID&utm_source=google&utm_medium=cpc&utm_campaign=handoff_initial",
      { headers: { "User-Agent": "Mozilla/5.0" } },
    ), env, landingCtx);
    await landingResponse.text();
    await Promise.all(landingCtx.promises);
    const anonId = readCookieFromSetCookie(landingResponse.headers, "eden_anonymous_id");
    const sessionId = readCookieFromSetCookie(landingResponse.headers, "eden_session_id");
    const initialPointer = readCookieFromSetCookie(landingResponse.headers, "__Secure-eden_ad_click_id");
    const initialAttr = readCookieFromSetCookie(landingResponse.headers, "eden_attr");
    assert.ok(anonId && sessionId && initialPointer && initialAttr, "initial Webflow click must establish owner, session, pointer, and first-touch bridge");

    const handoffDestination = "https://app.eden.health/intake/weightloss/welcome?plan=weightloss&referral_code=KEEP-REFERRAL&gclid=HANDOFF-INITIAL-GCLID&gbraid=HANDOFF-INITIAL-GBRAID&utm_source=google&utm_medium=cpc&utm_campaign=handoff_initial";
    const preserveCtx = makeCtx();
    const preserveResponse = await worker.fetch(new Request("https://www.eden.health/preserve-attribution", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://www.eden.health",
        "Cookie": `eden_anonymous_id=${anonId}; eden_anon_id=${anonId}; eden_session_id=${sessionId}; eden_attr=${initialAttr}; __Secure-eden_ad_click_id=${initialPointer}`,
      },
      body: JSON.stringify({
        anonymousId: anonId,
        pageUrl: "https://www.eden.health/#gclid=HANDOFF-FRAGMENT-GCLID&utm_source=google&utm_medium=cpc&utm_campaign=handoff_fragment",
        handoffDestination,
      }),
    }), env, preserveCtx);
    const preserveBody = await preserveResponse.json();
    await Promise.all(preserveCtx.promises);
    const fragmentPointer = readCookieFromSetCookie(preserveResponse.headers, "__Secure-eden_ad_click_id");
    const preserveAttr = readCookieFromSetCookie(preserveResponse.headers, "eden_attr") || initialAttr;
    const preserveAttrPayload = JSON.parse(decodeURIComponent(preserveAttr));
    assert.equal(preserveBody.ad_click_observation_persisted, true);
    assert.equal(preserveBody.internal_handoff_durable, true);
    assert.equal(preserveBody.internal_handoff_durability_source, "fresh_observation");
    assert.ok(fragmentPointer && fragmentPointer !== initialPointer, "new fragment evidence must receive its own owner-scoped pointer");
    assert.equal(preserveAttrPayload.gclid, "HANDOFF-FRAGMENT-GCLID", "the active attribution cookie must move to the fresh fragment click");
    assert.equal(preserveAttrPayload.utm_campaign, "handoff_fragment", "the active cookie must carry the new touch's campaign");
    assert.equal(preserveAttrPayload.gbraid, undefined, "the fresh touch must not retain a braid from the prior click");
    assert.match(preserveBody.internal_handoff_assertion, /^h1\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/, "durable preserve must mint a signed destination-bound assertion");
    const assertionPayloadText = Buffer.from(preserveBody.internal_handoff_assertion.split(".")[1], "base64url").toString("utf8");
    assert.equal(assertionPayloadText.includes("HANDOFF-FRAGMENT-GCLID"), false, "signed assertion must not embed raw Google click evidence");
    assert.equal(assertionPayloadText.includes(anonId), false, "signed assertion must not embed raw anonymous identity");
    assert.equal(assertionPayloadText.includes(sessionId), false, "signed assertion must not embed raw session identity");
    const initialRecordBeforeHandoff = JSON.parse(await adClickKv.get(`adclick:id:${initialPointer}`));
    const anonLastPaidKey = `adclick:v2:anon:${await sha256Raw(anonId)}:last_paid`;
    assert.equal(await adClickKv.get(anonLastPaidKey), null, "the production pointer-only profile must not write reverse KV indexes");

    const snapshotsBeforeCleanHandoff = queue.messages
      .map((message) => message.payload)
      .filter((payload) => payload.event_type === "ad_click_snapshot").length;
    const cleanHandoffCtx = makeCtx();
    const cleanHandoffResponse = await worker.fetch(new Request("https://www.eden.health/preserve-attribution", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://www.eden.health",
        "Cookie": `eden_anonymous_id=${anonId}; eden_anon_id=${anonId}; eden_session_id=${sessionId}; eden_attr=${preserveAttr}; __Secure-eden_ad_click_id=${fragmentPointer}`,
      },
      body: JSON.stringify({
        anonymousId: anonId,
        pageUrl: "https://www.eden.health/weight-loss",
        handoffDestination,
      }),
    }), env, cleanHandoffCtx);
    const cleanHandoffBody = await cleanHandoffResponse.json();
    await Promise.all(cleanHandoffCtx.promises);
    assert.equal(cleanHandoffBody.internal_handoff_durable, true, "a cleaned marketing URL must still prove the existing owner-bound pointer before handoff");
    assert.equal(cleanHandoffBody.internal_handoff_durability_source, "existing_owned_pointer");
    assert.equal(cleanHandoffBody.pointer_kv_persisted, true);
    assert.equal(cleanHandoffBody.owner_attribution_kv_persisted, true);
    assert.match(cleanHandoffBody.internal_handoff_assertion, /^h1\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/);
    assert.equal(
      queue.messages.map((message) => message.payload).filter((payload) => payload.event_type === "ad_click_snapshot").length,
      snapshotsBeforeCleanHandoff,
      "a cleaned-URL handoff must not invent a duplicate click observation",
    );

    const signedAppUrl = new URL(handoffDestination);
    signedAppUrl.searchParams.set("eden_attr_handoff", cleanHandoffBody.internal_handoff_assertion);
    const appCtx = makeCtx();
    const appResponse = await worker.fetch(new Request(
      signedAppUrl,
      {
        headers: {
          "User-Agent": "Mozilla/5.0",
          "Sec-Fetch-Dest": "document",
          "Sec-Fetch-Mode": "navigate",
          "Sec-Fetch-Site": "same-site",
          "Cookie": `eden_anonymous_id=${anonId}; eden_anon_id=${anonId}; eden_session_id=${sessionId}; eden_attr=${preserveAttr}; __Secure-eden_ad_click_id=${fragmentPointer}`,
        },
      },
    ), env, appCtx);
    await appResponse.text();
    await Promise.all(appCtx.promises);
    assert.equal(
      readCookieFromSetCookie(appResponse.headers, "__Secure-eden_ad_click_id"),
      fragmentPointer,
      "the signed app response must reissue the newer durable fragment pointer without promoting the older transported query",
    );
    const internalHandoffEnvelope = queue.messages.map((message) => message.payload)
      .filter((payload) => payload.event_type === "ad_click_snapshot")
      .at(-1);
    assert.equal(internalHandoffEnvelope.ad_click_id, initialPointer, "the carried query remains a separately observed click object");
    assert.equal(internalHandoffEnvelope.snapshot.governance.resolution_conflict, true);
    assert.equal(internalHandoffEnvelope.snapshot.governance.resolution_confidence, "diagnostic_only");
    assert.equal(internalHandoffEnvelope.resolution.resolution_reason, "signed_internal_handoff_pointer_click_evidence_mismatch");
    assert.deepEqual(
      internalHandoffEnvelope.snapshot.governance.resolution_conflict_sources,
      ["owned_pointer_cookie", "signed_internal_handoff", "transported_query_not_selected"],
    );
    assert.deepEqual(internalHandoffEnvelope.identity_links, [], "transported query conflict must emit no identity links");
    assert.equal(internalHandoffEnvelope.observation_only, true);
    assert.equal(internalHandoffEnvelope.selected_ad_click_id, fragmentPointer, "diagnostic envelope must record the opaque pointer that won the conflict");
    assert.equal(JSON.parse(await adClickKv.get(`adclick:id:${initialPointer}`)).snapshot_id, initialRecordBeforeHandoff.snapshot_id, "transported query conflict must not advance its pointer KV record");
    assert.equal(await adClickKv.get(anonLastPaidKey), null, "transported query conflict must not create owner reverse KV in the production pointer-only profile");
    assert.ok(await adClickKv.get(`adclick:id:${fragmentPointer}`), "the newer fragment pointer backing record must remain durable");
    const continuationCookie = getSetCookie(appResponse.headers).find((cookie) => cookie.startsWith("__Secure-eden_internal_handoff="));
    assert.ok(continuationCookie, "validated app navigation must mint an HttpOnly collector-continuation cookie");
    assert.match(continuationCookie, /HttpOnly/);
    assert.match(continuationCookie, /SameSite=Strict/);
    const continuationCookiePair = continuationCookie.split(";", 1)[0];
    const cleanedAppUrl = new URL(signedAppUrl);
    cleanedAppUrl.searchParams.delete("eden_attr_handoff");
    const forwardedAppRequest = originRequests.find((requestUrl) => requestUrl.startsWith("https://app.eden.health/intake/weightloss/welcome"));
    assert.ok(forwardedAppRequest && !forwardedAppRequest.includes("eden_attr_handoff="), "opaque handoff assertions must never be forwarded to HealthOS origin");
    const forwardedAppUrl = new URL(forwardedAppRequest);
    assert.equal(forwardedAppUrl.searchParams.get("gclid"), "HANDOFF-INITIAL-GCLID", "HealthOS must receive the governed click evidence for native capture");
    assert.equal(forwardedAppUrl.searchParams.get("gbraid"), "HANDOFF-INITIAL-GBRAID");
    assert.equal(forwardedAppUrl.searchParams.get("utm_source"), "google");
    assert.equal(forwardedAppUrl.searchParams.get("utm_medium"), "cpc");
    assert.equal(forwardedAppUrl.searchParams.get("utm_campaign"), "handoff_initial");
    assert.equal(forwardedAppUrl.searchParams.get("plan"), "weightloss", "business destination parameters must remain bound and reach HealthOS");
    assert.equal(forwardedAppUrl.searchParams.get("referral_code"), "KEEP-REFERRAL", "referral business state must not be mistaken for transported Google evidence");

    const aliasAppUrl = new URL(handoffDestination);
    aliasAppUrl.search = "";
    aliasAppUrl.searchParams.set("plan", "weightloss");
    aliasAppUrl.searchParams.set("referral_code", "KEEP-REFERRAL");
    aliasAppUrl.searchParams.set("amp;GCLID", "HANDOFF-INITIAL-GCLID");
    aliasAppUrl.searchParams.set("amp;GBRAID", "HANDOFF-INITIAL-GBRAID");
    aliasAppUrl.searchParams.set("amp;UTM_SOURCE", "google");
    aliasAppUrl.searchParams.set("amp;UTM_MEDIUM", "cpc");
    aliasAppUrl.searchParams.set("amp;UTM_CAMPAIGN", "handoff_initial");
    aliasAppUrl.searchParams.set("amp;Eden_Attr_Handoff", cleanHandoffBody.internal_handoff_assertion);
    const aliasCtx = makeCtx();
    const aliasResponse = await worker.fetch(new Request(aliasAppUrl, {
      headers: {
        "User-Agent": "Mozilla/5.0",
        "Cookie": `eden_anonymous_id=${anonId}; eden_anon_id=${anonId}; eden_session_id=${sessionId}; eden_attr=${preserveAttr}; __Secure-eden_ad_click_id=${fragmentPointer}`,
      },
    }), env, aliasCtx);
    await aliasResponse.text();
    await Promise.all(aliasCtx.promises);
    assert.equal(readCookieFromSetCookie(aliasResponse.headers, "__Secure-eden_ad_click_id"), fragmentPointer, "canonical assertion aliases must reissue the selected pointer");
    const forwardedAliasUrl = new URL(originRequests.filter((requestUrl) => requestUrl.startsWith("https://app.eden.health/intake/weightloss/welcome")).at(-1));
    assert.deepEqual(
      Object.fromEntries(forwardedAliasUrl.searchParams.entries()),
      {
        plan: "weightloss",
        referral_code: "KEEP-REFERRAL",
        "amp;GCLID": "HANDOFF-INITIAL-GCLID",
        "amp;GBRAID": "HANDOFF-INITIAL-GBRAID",
        "amp;UTM_SOURCE": "google",
        "amp;UTM_MEDIUM": "cpc",
        "amp;UTM_CAMPAIGN": "handoff_initial",
      },
      "only the opaque assertion alias is stripped; governed transport and business parameters reach HealthOS",
    );

    const nowSeconds = Math.floor(Date.now() / 1000);
    const legacyCleanDestination = "https://app.eden.health/intake/weightloss/welcome?plan=weightloss&referral_code=KEEP-REFERRAL";
    const legacyAssertion = await signedInternalHandoffFixture(TEST_BROWSER_CAP_HMAC_SECRET, {
      iat: nowSeconds,
      exp: nowSeconds + 600,
      pointerId: fragmentPointer,
      anonId,
      sessionId,
      destination: legacyCleanDestination,
      version: 1,
      includeTransportClaims: false,
    });
    const legacyAppUrl = new URL(legacyCleanDestination);
    legacyAppUrl.searchParams.set("eden_attr_handoff", legacyAssertion);
    const legacyAppCtx = makeCtx();
    const legacyAppResponse = await worker.fetch(new Request(legacyAppUrl, {
      headers: {
        "User-Agent": "Mozilla/5.0",
        "Cookie": `eden_anonymous_id=${anonId}; eden_anon_id=${anonId}; eden_session_id=${sessionId}; eden_attr=${preserveAttr}; __Secure-eden_ad_click_id=${fragmentPointer}`,
      },
    }), env, legacyAppCtx);
    await legacyAppResponse.text();
    await Promise.all(legacyAppCtx.promises);
    assert.equal(legacyAppResponse.status, 200, "a clean pre-v5.56 exact-destination handoff remains compatible for its short original TTL");
    assert.ok(
      getSetCookie(legacyAppResponse.headers).some((cookie) => cookie.startsWith("__Secure-eden_internal_handoff=")),
      "a clean legacy v1 exact-destination assertion may mint continuation state during the compatibility window",
    );

    const legacyMatchingClickUrl = new URL(legacyCleanDestination);
    legacyMatchingClickUrl.searchParams.set("gclid", "HANDOFF-FRAGMENT-GCLID");
    legacyMatchingClickUrl.searchParams.set("eden_attr_handoff", legacyAssertion);
    const legacyMatchingCtx = makeCtx();
    const legacyMatchingResponse = await worker.fetch(new Request(legacyMatchingClickUrl, {
      headers: {
        "User-Agent": "Mozilla/5.0",
        "Cookie": `eden_anonymous_id=${anonId}; eden_anon_id=${anonId}; eden_session_id=${sessionId}; eden_attr=${preserveAttr}; __Secure-eden_ad_click_id=${fragmentPointer}`,
      },
    }), env, legacyMatchingCtx);
    await legacyMatchingResponse.text();
    await Promise.all(legacyMatchingCtx.promises);
    assert.ok(
      getSetCookie(legacyMatchingResponse.headers).some((cookie) => cookie.startsWith("__Secure-eden_internal_handoff=")),
      "legacy v1 may carry only the primary Google click id proven by the owned pointer record",
    );

    const queueBeforeLegacyMismatch = queue.messages.length;
    const legacyChangedClickUrl = new URL(legacyCleanDestination);
    legacyChangedClickUrl.searchParams.set("gclid", "HANDOFF-LEGACY-CHANGED-GCLID");
    legacyChangedClickUrl.searchParams.set("eden_attr_handoff", legacyAssertion);
    const legacyChangedCtx = makeCtx();
    const legacyChangedResponse = await worker.fetch(new Request(legacyChangedClickUrl, {
      headers: {
        "User-Agent": "Mozilla/5.0",
        "Cookie": `eden_anonymous_id=${anonId}; eden_anon_id=${anonId}; eden_session_id=${sessionId}; eden_attr=${preserveAttr}; __Secure-eden_ad_click_id=${fragmentPointer}`,
      },
    }), env, legacyChangedCtx);
    await legacyChangedResponse.text();
    await Promise.all(legacyChangedCtx.promises);
    assert.equal(
      getSetCookie(legacyChangedResponse.headers).some((cookie) => cookie.startsWith("__Secure-eden_internal_handoff=")),
      false,
      "a legacy route assertion cannot authenticate changed click transport",
    );
    const changedLegacySnapshot = queue.messages.slice(queueBeforeLegacyMismatch)
      .map((message) => message.payload)
      .find((payload) => payload.snapshot?.google?.gclid === "HANDOFF-LEGACY-CHANGED-GCLID");
    assert.ok(changedLegacySnapshot?.snapshot, "the changed Google click must still be captured as fresh event-native evidence");
    assert.notEqual(changedLegacySnapshot.ad_click_id, fragmentPointer, "changed transport cannot be collapsed into the previously selected pointer");

    const conflictingTransportUrl = new URL(cleanedAppUrl);
    conflictingTransportUrl.searchParams.append("gclid", "HANDOFF-INITIAL-GCLID");
    conflictingTransportUrl.searchParams.append("gclid", "HANDOFF-CONFLICTING-GCLID");
    const conflictingTransportCtx = makeCtx();
    const conflictingTransportResponse = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": `eden_anonymous_id=${anonId}; eden_anon_id=${anonId}; eden_session_id=${sessionId}; eden_attr=${preserveAttr}; __Secure-eden_ad_click_id=${fragmentPointer}; ${continuationCookiePair}`,
      },
      body: JSON.stringify({
        type: "track",
        event: "os_question_answered",
        properties: {},
        context: {
          page: { url: conflictingTransportUrl.toString() },
          campaign: { gclid: "HANDOFF-INITIAL-GCLID" },
        },
      }),
    }), env, conflictingTransportCtx);
    const conflictingTransportBody = await conflictingTransportResponse.json();
    await Promise.all(conflictingTransportCtx.promises);
    assert.equal(conflictingTransportResponse.status, 200);
    assert.notEqual(conflictingTransportBody.internalHandoffVerified, true, "conflicting repeated URL click IDs cannot disappear behind one matching campaign copy");
    assert.notEqual(segmentCalls.at(-1).properties.transported_internal_handoff, true, "ambiguous transport must never be classified as a verified handoff");

    const beforeCollector = queue.messages.length;
    const collectorCtx = makeCtx();
    const collectorResponse = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": `eden_anonymous_id=${anonId}; eden_anon_id=${anonId}; eden_session_id=${sessionId}; eden_attr=${preserveAttr}; __Secure-eden_ad_click_id=${fragmentPointer}; ${continuationCookiePair}`,
      },
      body: JSON.stringify({
        type: "track",
        event: "os_question_answered",
        anonymousId: anonId,
        properties: {},
        context: {
          page: { url: cleanedAppUrl.toString(), referrer: "https://www.eden.health/" },
          campaign: { gclid: "HANDOFF-INITIAL-GCLID", utm_source: "google", utm_medium: "cpc" },
        },
      }),
    }), env, collectorCtx);
    const collectorBody = await collectorResponse.json();
    await Promise.all(collectorCtx.promises);
    assert.equal(queue.messages.length, beforeCollector, "follow-on collector event must not recreate the transported-query snapshot or links");
    assert.equal(collectorBody.internalHandoffVerified, true);
    assert.equal(collectorBody.internalHandoffSelectedPointerResolved, true, "production cookie-mode collector must resolve through the signed selected pointer");
    assert.equal(collectorBody.internalHandoffResolutionSource, "pointer_cookie");
    assert.equal(segmentCalls.at(-1).properties.ad_click_id, undefined, "production cookie mode must keep payload annotation disabled");
    assert.equal(segmentCalls.at(-1).context.campaign?.gclid, undefined, "transported query must not masquerade as event-native campaign evidence");
    assert.equal(segmentCalls.at(-1).properties.transported_internal_handoff, true);
    assert.equal(segmentCalls.at(-1).properties.transported_gclid, "HANDOFF-INITIAL-GCLID");
    assert.equal(segmentCalls.at(-1).properties.transported_gbraid, "HANDOFF-INITIAL-GBRAID");
    assert.equal(segmentCalls.at(-1).properties.transported_utm_source, "google");
    assert.equal(segmentCalls.at(-1).properties.transported_internal_handoff_selected_ad_click_id, fragmentPointer);
    assert.equal(await adClickKv.get(anonLastPaidKey), null, "follow-on collector event must not create owner reverse KV");

    const purchaseQueueLengthBefore = queue.messages.length;
    const purchaseCollectorCtx = makeCtx();
    const purchaseCollectorResponse = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": `eden_anonymous_id=${anonId}; eden_anon_id=${anonId}; eden_session_id=${sessionId}; eden_attr=${preserveAttr}; __Secure-eden_ad_click_id=${fragmentPointer}; ${continuationCookiePair}`,
      },
      body: JSON.stringify({
        type: "track",
        event: "OS_purchase",
        anonymousId: anonId,
        properties: {
          order_id: "signed-handoff-order",
          transaction_id: "signed-handoff-charge",
          gclid: "HANDOFF-INITIAL-GCLID",
          utm_source: "google",
          utm_medium: "cpc",
          ecommerce: {
            gclid: "HANDOFF-INITIAL-GCLID",
            gbraid: "HANDOFF-INITIAL-GBRAID",
            utm_campaign: "handoff_initial",
          },
        },
        context: {
          page: { url: cleanedAppUrl.toString(), referrer: "https://www.eden.health/" },
          campaign: {
            gclid: "HANDOFF-INITIAL-GCLID",
            gbraid: "HANDOFF-INITIAL-GBRAID",
            utm_source: "google",
            utm_medium: "cpc",
            utm_campaign: "handoff_initial",
          },
        },
      }),
    }), env, purchaseCollectorCtx);
    const purchaseCollectorBody = await purchaseCollectorResponse.json();
    await Promise.all(purchaseCollectorCtx.promises);
    assert.equal(purchaseCollectorResponse.status, 200);
    assert.equal(purchaseCollectorBody.ok, true);
    const forwardedPurchase = segmentCalls.filter((call) => call.event === "OS_purchase").at(-1);
    assert.ok(forwardedPurchase, "browser purchase signal must reach Segment with edge attribution continuity");
    assert.equal(forwardedPurchase.properties.source_type, "client");
    assert.equal(forwardedPurchase.properties.browser_conversion_observation, true);
    assert.equal(forwardedPurchase.properties.browser_event_authority, "provisional_observation");
    assert.equal(forwardedPurchase.properties.order_id, undefined, "browser purchase cannot assert an order identity");
    assert.equal(forwardedPurchase.properties.transaction_id, undefined, "browser purchase cannot assert a payment identity");
    assert.equal(forwardedPurchase.properties.transported_internal_handoff, true);
    assert.equal(forwardedPurchase.properties.transported_gclid, "HANDOFF-INITIAL-GCLID");
    const purchaseLinkEnvelope = queue.messages.slice(purchaseQueueLengthBefore).map((message) => message.payload)
      .find((payload) => payload.event_type === "ad_click_identity_links");
    assert.ok(purchaseLinkEnvelope, "browser purchase should strengthen provisional anonymous/session/ad-click continuity");
    assert.ok(purchaseLinkEnvelope.identity_links.length >= 2);
    assert.ok(purchaseLinkEnvelope.identity_links.every((link) => ["anonymous_id", "session_id", "ad_click_id"].includes(link.from_type) && ["anonymous_id", "session_id", "ad_click_id"].includes(link.to_type)), "browser purchase links remain edge-owned and provisional");
    assert.equal(purchaseLinkEnvelope.identity_links.some((link) => [link.from_type, link.to_type].some((type) => ["user_id", "order_id", "payment_id"].includes(type))), false, "browser purchase cannot emit stable person/order/payment links");

    const sameTransportDifferentPath = new URL(cleanedAppUrl);
    sameTransportDifferentPath.pathname = "/intake/weightloss/question-2";
    const signedTransportClaim = JSON.parse(Buffer.from(
      cleanHandoffBody.internal_handoff_assertion.split(".")[1],
      "base64url",
    ).toString("utf8")).trn;
    assert.equal(
      signedTransportClaim,
      await internalHandoffTestTransportSha256(sameTransportDifferentPath.toString()),
      "the signed transport hash must remain stable across an intake-only path change",
    );
    const cleanedSpaRoute = new URL(sameTransportDifferentPath);
    cleanedSpaRoute.search = "";
    cleanedSpaRoute.hash = "";
    const routeContinuationQueueLength = queue.messages.length;
    const routeContinuationCtx = makeCtx();
    const routeContinuationResponse = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": `eden_anonymous_id=${anonId}; eden_anon_id=${anonId}; eden_session_id=${sessionId}; eden_attr=${preserveAttr}; __Secure-eden_ad_click_id=${fragmentPointer}; ${continuationCookiePair}`,
      },
      body: JSON.stringify({
        type: "track",
        event: "os_question_answered",
        properties: { step_number: 2 },
        context: {
          // HealthOS commonly cleans transport from the visible SPA URL after
          // its first native observation while the collector retains the same
          // campaign envelope. The continuation assertion must recognize that
          // as transported, not mint the values as a fresh touch.
          page: { url: cleanedSpaRoute.toString(), referrer: cleanedAppUrl.toString() },
          campaign: {
            gclid: "HANDOFF-INITIAL-GCLID",
          },
        },
      }),
    }), env, routeContinuationCtx);
    const routeContinuationBody = await routeContinuationResponse.json();
    await Promise.all(routeContinuationCtx.promises);
    assert.equal(routeContinuationResponse.status, 200);
    assert.equal(routeContinuationBody.internalHandoffVerified, true, "same transported evidence must remain classified across an intake SPA route change");
    assert.equal(readCookieFromSetCookie(routeContinuationResponse.headers, "__Secure-eden_ad_click_id"), null, "transported route continuation must not replace the selected pointer");
    assert.equal(queue.messages.length, routeContinuationQueueLength, "transported route continuation must not create a fresh snapshot");
    assert.equal(segmentCalls.at(-1).context.campaign?.gclid, undefined);
    assert.equal(segmentCalls.at(-1).properties.transported_internal_handoff, true);
    assert.equal(segmentCalls.at(-1).properties.transported_gclid, "HANDOFF-INITIAL-GCLID");

    const nestedTransportRoute = new URL(cleanedSpaRoute);
    nestedTransportRoute.pathname = "/intake/weightloss/question-3";
    const nestedTransportQueueLength = queue.messages.length;
    const nestedTransportCtx = makeCtx();
    const nestedTransportResponse = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": `eden_anonymous_id=${anonId}; eden_anon_id=${anonId}; eden_session_id=${sessionId}; eden_attr=${preserveAttr}; __Secure-eden_ad_click_id=${fragmentPointer}; ${continuationCookiePair}`,
      },
      body: JSON.stringify({
        type: "track",
        event: "os_question_answered",
        properties: { step_number: 3, ecommerce: { gclid: "HANDOFF-INITIAL-GCLID" } },
        context: { page: { url: nestedTransportRoute.toString(), referrer: cleanedSpaRoute.toString() } },
      }),
    }), env, nestedTransportCtx);
    const nestedTransportBody = await nestedTransportResponse.json();
    await Promise.all(nestedTransportCtx.promises);
    assert.equal(nestedTransportResponse.status, 200);
    assert.equal(nestedTransportBody.internalHandoffVerified, true, "nested retained click evidence must remain transported after SPA URL cleanup");
    assert.equal(queue.messages.length, nestedTransportQueueLength, "nested transported evidence must not create a fresh snapshot");
    assert.equal(segmentCalls.at(-1).properties.transported_gclid, "HANDOFF-INITIAL-GCLID");

    const wrongCollectorDestinationCtx = makeCtx();
    const wrongCollectorDestinationResponse = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": `eden_anonymous_id=${anonId}; eden_anon_id=${anonId}; eden_session_id=${sessionId}; eden_attr=${preserveAttr}; __Secure-eden_ad_click_id=${fragmentPointer}; ${continuationCookiePair}`,
      },
      body: JSON.stringify({
        type: "track",
        event: "os_question_answered",
        anonymousId: anonId,
        properties: {},
        context: {
          page: {
            url: "https://app.eden.health/intake/different-step?gclid=HANDOFF-COLLECTOR-FRESH-GCLID&utm_source=google&utm_medium=cpc&utm_campaign=collector_fresh",
            referrer: "https://www.google.com/",
          },
          campaign: {
            gclid: "HANDOFF-COLLECTOR-FRESH-GCLID",
            utm_source: "google",
            utm_medium: "cpc",
            utm_campaign: "collector_fresh",
          },
        },
      }),
    }), env, wrongCollectorDestinationCtx);
    await wrongCollectorDestinationResponse.json();
    await Promise.all(wrongCollectorDestinationCtx.promises);
    const wrongDestinationPointer = readCookieFromSetCookie(wrongCollectorDestinationResponse.headers, "__Secure-eden_ad_click_id");
    assert.ok(
      wrongDestinationPointer && wrongDestinationPointer !== fragmentPointer,
      "a continuation cookie must not suppress fresh evidence on a different intake destination",
    );
    assert.equal(
      segmentCalls.at(-1).context.campaign?.gclid,
      "HANDOFF-COLLECTOR-FRESH-GCLID",
      "wrong-destination continuation must fail safe to ordinary event-native campaign handling",
    );

    const refererOnlyCtx = makeCtx();
    const refererOnlyResponse = await worker.fetch(new Request(
      "https://app.eden.health/intake?gclid=HANDOFF-REFERER-ONLY-GCLID&utm_source=google&utm_medium=cpc",
      {
        headers: {
          "User-Agent": "Mozilla/5.0",
          "Referer": "https://www.eden.health/",
          "Cookie": `eden_anonymous_id=${anonId}; eden_anon_id=${anonId}; eden_session_id=${sessionId}; eden_attr=${preserveAttr}; __Secure-eden_ad_click_id=${fragmentPointer}`,
        },
      },
    ), env, refererOnlyCtx);
    await refererOnlyResponse.text();
    await Promise.all(refererOnlyCtx.promises);
    assert.ok(readCookieFromSetCookie(refererOnlyResponse.headers, "__Secure-eden_ad_click_id"), "Referer alone must never activate internal-handoff precedence");

    const externalCtx = makeCtx();
    const externalResponse = await worker.fetch(new Request(
      "https://app.eden.health/intake?gclid=HANDOFF-NEW-EXTERNAL-GCLID&utm_source=google&utm_medium=cpc&utm_campaign=external_relanding",
      {
        headers: {
          "User-Agent": "Mozilla/5.0",
          "Referer": "https://www.google.com/",
          "Cookie": `eden_anonymous_id=${anonId}; eden_anon_id=${anonId}; eden_session_id=${sessionId}; eden_attr=${preserveAttr}; __Secure-eden_ad_click_id=${fragmentPointer}`,
        },
      },
    ), env, externalCtx);
    await externalResponse.text();
    await Promise.all(externalCtx.promises);
    const externalPointer = readCookieFromSetCookie(externalResponse.headers, "__Secure-eden_ad_click_id");
    assert.ok(externalPointer && externalPointer !== fragmentPointer, "a genuine external re-landing with fresh click evidence must become the current pointer");
    const externalEnvelope = queue.messages.map((message) => message.payload)
      .filter((payload) => payload.event_type === "ad_click_snapshot")
      .at(-1);
    assert.equal(externalEnvelope.ad_click_id, externalPointer);
    assert.equal(externalEnvelope.snapshot.governance.resolution_conflict, false);
    assert.equal(externalEnvelope.snapshot.governance.resolution_confidence, "high");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await internalWebflowHandoffPreservesNewerFragmentPointer();

async function canonicalHandoffAliasesPreserveTransportButRemoveAssertion() {
  const originalFetch = globalThis.fetch;
  const originalHtmlRewriter = globalThis.HTMLRewriter;
  let originRequestUrl = null;
  let injectedScripts = "";
  globalThis.fetch = async (input) => {
    originRequestUrl = input instanceof Request ? input.url : String(input);
    return new Response("<html><head></head><body>ok</body></html>", {
      status: 200,
      headers: { "content-type": "text/html" },
    });
  };
  globalThis.HTMLRewriter = class {
    on(selector, handler) { this.selector = selector; this.handler = handler; return this; }
    transform(response) {
      assert.equal(this.selector, "head");
      this.handler.element({ prepend(value) { injectedScripts += String(value); } });
      return response;
    }
  };
  try {
    const anonId = "canonical-alias-anon";
    const sessionId = "canonical-alias-session_1780000000000";
    const pointerId = "adclk2_canonical_alias_pointer";
    const adClickKv = new MockKV();
    await adClickKv.put(`adclick:id:${pointerId}`, JSON.stringify({
      schema_version: "eden_ad_click_pointer_v2",
      ad_click_id: pointerId,
      snapshot_id: "adsnap_canonical_alias",
      captured_at: "2026-07-10T00:00:00.000Z",
      primary_click_id_type: "gclid",
      raw_primary_click_id_sha256: await sha256Raw("CANONICAL-SELECTED-GCLID"),
      owner_anonymous_id_sha256: await sha256Raw(anonId),
      owner_session_id_sha256: await sha256Raw(sessionId),
      ad_click_id_scope: "first_party_scoped",
      ownership_scope: "first_party_owner_bound",
    }));
    const nestedDestination = "https://app.eden.health/intake/next?plan=nested-plan&gclid=CANONICAL-TRANSPORTED-GCLID&amp;UTM_SOURCE=google#step?gbraid=NESTED-FRAGMENT-GBRAID&tab=keep";
    const canonicalDestinationUrl = new URL("https://app.eden.health/intake/canonical");
    canonicalDestinationUrl.searchParams.set("plan", "weightloss");
    canonicalDestinationUrl.searchParams.set("referral_code", "KEEP-CANONICAL-REFERRAL");
    canonicalDestinationUrl.searchParams.set("next", nestedDestination);
    canonicalDestinationUrl.searchParams.set("gclid", "CANONICAL-TRANSPORTED-GCLID");
    canonicalDestinationUrl.searchParams.set("utm_source", "google");
    canonicalDestinationUrl.searchParams.set("utm_medium", "cpc");
    canonicalDestinationUrl.searchParams.set("utm_campaign", "canonical_alias");
    canonicalDestinationUrl.searchParams.set("gad_source", "1");
    canonicalDestinationUrl.searchParams.set("ga_session_id", "1780000000");
    canonicalDestinationUrl.hash = "step?wbraid=OUTER-FRAGMENT-WBRAID&tab=keep";
    const canonicalDestination = canonicalDestinationUrl.toString();
    const nowSeconds = Math.floor(Date.now() / 1000);
    const assertion = await signedInternalHandoffFixture(TEST_BROWSER_CAP_HMAC_SECRET, {
      iat: nowSeconds,
      exp: nowSeconds + 300,
      pointerId,
      anonId,
      sessionId,
      destination: canonicalDestination,
    });
    const aliasUrl = new URL("https://app.eden.health/intake/canonical");
    aliasUrl.searchParams.set("plan", "weightloss");
    aliasUrl.searchParams.set("referral_code", "KEEP-CANONICAL-REFERRAL");
    aliasUrl.searchParams.set("next", nestedDestination);
    aliasUrl.searchParams.set("amp;GCLID", "CANONICAL-TRANSPORTED-GCLID");
    aliasUrl.searchParams.set("amp;UTM_SOURCE", "google");
    aliasUrl.searchParams.set("amp;UTM_MEDIUM", "cpc");
    aliasUrl.searchParams.set("amp;UTM_CAMPAIGN", "canonical_alias");
    aliasUrl.searchParams.set("amp;GAD_SOURCE", "1");
    aliasUrl.searchParams.set("amp;GA_SESSION_ID", "1780000000");
    aliasUrl.searchParams.set("amp;Eden_Attr_Handoff", assertion);
    aliasUrl.hash = "step?wbraid=OUTER-FRAGMENT-WBRAID&tab=keep";
    const ctx = makeCtx();
    const response = await worker.fetch(new Request(aliasUrl, {
      headers: {
        "User-Agent": "Mozilla/5.0",
        "Cookie": `eden_anonymous_id=${anonId}; eden_anon_id=${anonId}; eden_session_id=${sessionId}; __Secure-eden_ad_click_id=${pointerId}`,
      },
    }), {
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: new MockQueue(),
    }, ctx);
    await response.text();
    await Promise.all(ctx.promises);
    const originUrl = new URL(originRequestUrl);
    assert.equal(originUrl.searchParams.get("plan"), "weightloss");
    assert.equal(originUrl.searchParams.get("referral_code"), "KEEP-CANONICAL-REFERRAL");
    assert.deepEqual(
      [...originUrl.searchParams.keys()].sort(),
      ["amp;GAD_SOURCE", "amp;GA_SESSION_ID", "amp;GCLID", "amp;UTM_CAMPAIGN", "amp;UTM_MEDIUM", "amp;UTM_SOURCE", "next", "plan", "referral_code"],
      "only the signed assertion alias may be removed; click IDs, Google parameters, UTMs, and business parameters must survive the HealthOS origin request",
    );
    const originNested = new URL(originUrl.searchParams.get("next"));
    assert.deepEqual(Object.fromEntries(originNested.searchParams.entries()), {
      plan: "nested-plan",
      gclid: "CANONICAL-TRANSPORTED-GCLID",
      "amp;UTM_SOURCE": "google",
    });
    assert.equal(originNested.hash, "#step?gbraid=NESTED-FRAGMENT-GBRAID&tab=keep", "nested transport attribution and business state must survive the handoff");
    assert.ok(injectedScripts.includes("function normalizedTransportKey(rawKey)"));
    assert.ok(!injectedScripts.includes("current.searchParams.has('eden_attr_handoff')"), "cleanup must not rely on exact assertion spelling");
    const cleanupMatch = injectedScripts.match(/<script>([\s\S]*?)<\/script>/);
    assert.ok(cleanupMatch, "valid signed handoff HTML must prepend the cleanup script");
    let replacedUrl = null;
    const windowStub = {
      location: { href: aliasUrl.toString() },
      history: {
        state: null,
        replaceState(_state, _title, value) { replacedUrl = String(value); },
      },
    };
    new Function("window", "document", "URL", cleanupMatch[1])(
      windowStub,
      { title: "fixture" },
      URL,
    );
    const browserUrl = new URL(replacedUrl, "https://app.eden.health");
    assert.equal(browserUrl.searchParams.get("plan"), "weightloss");
    assert.equal(browserUrl.searchParams.get("referral_code"), "KEEP-CANONICAL-REFERRAL");
    assert.deepEqual(
      [...browserUrl.searchParams.keys()].sort(),
      ["amp;GAD_SOURCE", "amp;GA_SESSION_ID", "amp;GCLID", "amp;UTM_CAMPAIGN", "amp;UTM_MEDIUM", "amp;UTM_SOURCE", "next", "plan", "referral_code"],
    );
    assert.equal(browserUrl.hash, "#step?wbraid=OUTER-FRAGMENT-WBRAID&tab=keep", "browser cleanup must retain transport attribution and fragment state");
    const browserNested = new URL(browserUrl.searchParams.get("next"));
    assert.deepEqual(Object.fromEntries(browserNested.searchParams.entries()), {
      plan: "nested-plan",
      gclid: "CANONICAL-TRANSPORTED-GCLID",
      "amp;UTM_SOURCE": "google",
    });
    assert.equal(browserNested.hash, "#step?gbraid=NESTED-FRAGMENT-GBRAID&tab=keep");
  } finally {
    globalThis.fetch = originalFetch;
    if (originalHtmlRewriter === undefined) delete globalThis.HTMLRewriter;
    else globalThis.HTMLRewriter = originalHtmlRewriter;
  }
}

await canonicalHandoffAliasesPreserveTransportButRemoveAssertion();

async function higherPriorityGclidEnrichesExistingBraidPointerWithoutFalseConflict() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    for (const braidType of ["gbraid", "wbraid"]) {
      const adClickKv = new MockKV();
      const queue = new MockQueue();
      const anonId = `compatible-${braidType}-anon`;
      const sessionId = `compatible-${braidType}-session_1780000000000`;
      const braidValue = `COMPATIBLE-${braidType.toUpperCase()}`;
      const env = {
        EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
        EDEN_AD_CLICK_MEMORY_MODE: "all",
        AD_CLICK_KV: adClickKv,
        AD_CLICK_SNAPSHOT_QUEUE: queue,
      };
      const firstCtx = makeCtx();
      const firstResponse = await worker.fetch(new Request(
        `https://www.eden.health/?${braidType}=${braidValue}&utm_source=google&utm_medium=cpc`,
        { headers: { "User-Agent": "Mozilla/5.0", "Cookie": `eden_anonymous_id=${anonId}; eden_session_id=${sessionId}` } },
      ), env, firstCtx);
      await firstResponse.text();
      await Promise.all(firstCtx.promises);
      const pointerId = readCookieFromSetCookie(firstResponse.headers, "__Secure-eden_ad_click_id");
      assert.ok(pointerId);

      const enrichedCtx = makeCtx();
      const enrichedResponse = await worker.fetch(new Request(
        `https://app.eden.health/intake?gclid=COMPATIBLE-GCLID&${braidType}=${braidValue}&utm_source=google&utm_medium=cpc`,
        {
          headers: {
            "User-Agent": "Mozilla/5.0",
            "Cookie": `eden_anonymous_id=${anonId}; eden_session_id=${sessionId}; __Secure-eden_ad_click_id=${pointerId}`,
          },
        },
      ), env, enrichedCtx);
      await enrichedResponse.text();
      await Promise.all(enrichedCtx.promises);
      assert.equal(readCookieFromSetCookie(enrichedResponse.headers, "__Secure-eden_ad_click_id"), pointerId, `${braidType} enrichment must keep one scoped click object`);
      const enrichedEnvelope = queue.messages.filter((message) => message.payload.event_type === "ad_click_snapshot").at(-1).payload;
      assert.equal(enrichedEnvelope.ad_click_id, pointerId);
      assert.equal(enrichedEnvelope.snapshot.evidence.primary_click_id_type, "gclid");
      assert.equal(enrichedEnvelope.snapshot.governance.resolution_conflict, false);
      assert.equal(enrichedEnvelope.snapshot.governance.resolution_confidence, "high");
      assert.equal(enrichedEnvelope.resolution.resolution_reason, "pointer_click_evidence_compatible_enrichment");
      const updatedRecord = JSON.parse(await adClickKv.get(`adclick:id:${pointerId}`));
      assert.equal(updatedRecord.primary_click_id_type, "gclid");
      assert.equal(updatedRecord.raw_primary_click_id_sha256, await sha256Raw("COMPATIBLE-GCLID"));
    }
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await higherPriorityGclidEnrichesExistingBraidPointerWithoutFalseConflict();

async function freshGclidMustNotMergeThroughRecoveredBraid() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const anonId = "recovered-braid-anon";
    const sessionId = "recovered-braid-session_1780000000000";
    const braidValue = "RECOVERED-OLD-GBRAID";
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      EDEN_AD_CLICK_KV_INDEX_MODE: "pointer",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const firstCtx = makeCtx();
    const firstResponse = await worker.fetch(new Request(
      `https://www.eden.health/?gbraid=${braidValue}&utm_source=google&utm_medium=cpc`,
      { headers: { "User-Agent": "Mozilla/5.0", "Cookie": `eden_anonymous_id=${anonId}; eden_session_id=${sessionId}` } },
    ), env, firstCtx);
    await firstResponse.text();
    await Promise.all(firstCtx.promises);
    const braidPointer = readCookieFromSetCookie(firstResponse.headers, "__Secure-eden_ad_click_id");
    assert.ok(braidPointer);

    const recoveredPreAuth = encodeURIComponent(JSON.stringify({
      gbraid: braidValue,
      utm_source: "google",
      utm_medium: "cpc",
    }));
    const freshCtx = makeCtx();
    const freshResponse = await worker.fetch(new Request(
      "https://app.eden.health/intake?gclid=FRESH-INDEPENDENT-GCLID&utm_source=google&utm_medium=cpc&utm_campaign=fresh_gclid",
      {
        headers: {
          "User-Agent": "Mozilla/5.0",
          "Cookie": `eden_anonymous_id=${anonId}; eden_session_id=${sessionId}; __Secure-eden_ad_click_id=${braidPointer}; eden_pre_auth=${recoveredPreAuth}`,
        },
      },
    ), env, freshCtx);
    await freshResponse.text();
    await Promise.all(freshCtx.promises);
    const freshPointer = readCookieFromSetCookie(freshResponse.headers, "__Secure-eden_ad_click_id");
    assert.ok(freshPointer && freshPointer !== braidPointer, "a new GCLID must remain a separate paid click when the matching braid is recovered rather than event-native");
    const freshEnvelope = queue.messages.filter((message) => message.payload.event_type === "ad_click_snapshot").at(-1).payload;
    assert.equal(freshEnvelope.ad_click_id, freshPointer);
    assert.equal(freshEnvelope.snapshot.google.gclid, "FRESH-INDEPENDENT-GCLID");
    assert.equal(freshEnvelope.snapshot.google.gbraid, undefined, "recovered secondary braid must not be copied into a native GCLID observation");
    assert.equal(freshEnvelope.snapshot.evidence.primary_click_id_type, "gclid");
    assert.equal(freshEnvelope.snapshot.governance.resolution_conflict, false);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await freshGclidMustNotMergeThroughRecoveredBraid();

async function invalidInternalHandoffAssertionsFailSafeToFreshEvidence() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const nowSeconds = Math.floor(Date.now() / 1000);
    const cases = [
      { label: "expired", requestPath: "/intake/expired", signedPath: "/intake/expired", iat: nowSeconds - 600, exp: nowSeconds - 300, requestAnon: "assertion-owner", requestSession: "assertion-session_1780000000000" },
      { label: "wrong destination", requestPath: "/intake/requested", signedPath: "/intake/signed", iat: nowSeconds, exp: nowSeconds + 300, requestAnon: "assertion-owner", requestSession: "assertion-session_1780000000000" },
      { label: "wrong owner", requestPath: "/intake/owner", signedPath: "/intake/owner", iat: nowSeconds, exp: nowSeconds + 300, requestAnon: "different-owner", requestSession: "different-session_1780000000000" },
    ];
    for (const fixture of cases) {
      const ownerAnon = "assertion-owner";
      const ownerSession = "assertion-session_1780000000000";
      const pointerId = `adclk2_assertion_${fixture.label.replace(/\s+/g, "_")}`;
      const adClickKv = new MockKV();
      const queue = new MockQueue();
      await adClickKv.put(`adclick:id:${pointerId}`, JSON.stringify({
        schema_version: "eden_ad_click_pointer_v2",
        ad_click_id: pointerId,
        snapshot_id: `adsnap_${fixture.label}`,
        captured_at: "2026-07-10T00:00:00.000Z",
        primary_click_id_type: "gclid",
        raw_primary_click_id_sha256: await sha256Raw("ASSERTION-SELECTED-GCLID"),
        owner_anonymous_id_sha256: await sha256Raw(ownerAnon),
        owner_session_id_sha256: await sha256Raw(ownerSession),
        ad_click_id_scope: "first_party_scoped",
        ownership_scope: "first_party_owner_bound",
      }));
      const signedDestination = `https://app.eden.health${fixture.signedPath}?gclid=ASSERTION-TRANSPORTED-GCLID&utm_source=google&utm_medium=cpc`;
      const assertion = await signedInternalHandoffFixture(TEST_BROWSER_CAP_HMAC_SECRET, {
        iat: fixture.iat,
        exp: fixture.exp,
        pointerId,
        anonId: ownerAnon,
        sessionId: ownerSession,
        destination: signedDestination,
      });
      const requestUrl = new URL(`https://app.eden.health${fixture.requestPath}?gclid=ASSERTION-TRANSPORTED-GCLID&utm_source=google&utm_medium=cpc`);
      requestUrl.searchParams.set("eden_attr_handoff", assertion);
      const ctx = makeCtx();
      const response = await worker.fetch(new Request(requestUrl, {
        headers: {
          "User-Agent": "Mozilla/5.0",
          "Referer": "https://www.eden.health/",
          "Cookie": `eden_anonymous_id=${fixture.requestAnon}; eden_session_id=${fixture.requestSession}; __Secure-eden_ad_click_id=${pointerId}`,
        },
      }), {
        EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
        EDEN_AD_CLICK_MEMORY_MODE: "all",
        AD_CLICK_KV: adClickKv,
        AD_CLICK_SNAPSHOT_QUEUE: queue,
      }, ctx);
      await response.text();
      await Promise.all(ctx.promises);
      const freshPointer = readCookieFromSetCookie(response.headers, "__Secure-eden_ad_click_id");
      assert.ok(freshPointer && freshPointer !== pointerId, `${fixture.label} assertion must not suppress genuine fresh query handling`);
      const envelope = queue.messages.filter((message) => message.payload.event_type === "ad_click_snapshot").at(-1).payload;
      assert.equal(envelope.snapshot.governance.resolution_conflict, false);
      assert.equal(envelope.snapshot.governance.resolution_confidence, "high");
    }
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await invalidInternalHandoffAssertionsFailSafeToFreshEvidence();

async function replayedGclidCreatesIndependentOwnerScopedPointers() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const kv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      AD_CLICK_KV: kv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const requestForOwner = (anon, session) => new Request("https://www.eden.health/?gclid=REPLAYED-OWNER-GCLID&utm_source=google&utm_medium=cpc", {
      headers: {
        "User-Agent": "Mozilla/5.0",
        "Cookie": `eden_anonymous_id=${anon}; eden_anon_id=${anon}; eden_session_id=${session}`,
      },
    });

    const firstCtx = makeCtx();
    const firstResponse = await worker.fetch(requestForOwner("owner-a-anon", "owner-a-session_1780000000000"), env, firstCtx);
    await firstResponse.text();
    await Promise.all(firstCtx.promises);
    const firstSnapshot = queue.messages[0].payload.snapshot;
    const originalRecordBeforeReplay = JSON.parse(await kv.get(`adclick:id:${firstSnapshot.ad_click_id}`));
    assert.equal(originalRecordBeforeReplay.owner_anonymous_id_sha256, await sha256Raw("owner-a-anon"));

    const secondCtx = makeCtx();
    const secondResponse = await worker.fetch(requestForOwner("owner-b-anon", "owner-b-session_1780000000000"), env, secondCtx);
    await secondResponse.text();
    await Promise.all(secondCtx.promises);
    const secondSnapshot = queue.messages[1].payload.snapshot;
    assert.notEqual(secondSnapshot.ad_click_id, firstSnapshot.ad_click_id, "a replayed GCLID from another first-party owner must create a different ad-click object");
    assert.equal(secondSnapshot.governance.resolution_conflict, false);
    assert.equal(secondSnapshot.governance.resolution_confidence, "high");
    assert.equal(secondSnapshot.governance.ad_click_id_scope, "first_party_scoped");
    assert.equal(readCookieFromSetCookie(secondResponse.headers, "__Secure-eden_ad_click_id"), secondSnapshot.ad_click_id, "the second owner receives only its independent scoped pointer");
    const originalRecordAfterReplay = JSON.parse(await kv.get(`adclick:id:${firstSnapshot.ad_click_id}`));
    assert.equal(originalRecordAfterReplay.owner_anonymous_id_sha256, originalRecordBeforeReplay.owner_anonymous_id_sha256, "second owner must not overwrite the original pointer owner");
    assert.equal(originalRecordAfterReplay.snapshot_id, originalRecordBeforeReplay.snapshot_id, "second owner must not replace the original immutable snapshot");
    const secondOwnerRecord = JSON.parse(await kv.get(`adclick:id:${secondSnapshot.ad_click_id}`));
    assert.equal(secondOwnerRecord.owner_anonymous_id_sha256, await sha256Raw("owner-b-anon"));
    assert.equal(firstSnapshot.evidence.raw_primary_click_id_sha256, secondSnapshot.evidence.raw_primary_click_id_sha256, "global replay QA remains possible through the raw click hash");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await replayedGclidCreatesIndependentOwnerScopedPointers();

class PointerReadBarrierKV extends MockKV {
  constructor(expectedReads = 2) {
    super();
    this.expectedReads = expectedReads;
    this.waiting = 0;
    this.release = null;
    this.barrier = new Promise((resolve) => { this.release = resolve; });
  }
  async get(key) {
    this.getKeys.push(key);
    if (key.startsWith("adclick:id:") && !this.map.has(key) && this.waiting < this.expectedReads) {
      this.waiting += 1;
      if (this.waiting === this.expectedReads) this.release();
      await this.barrier;
    }
    return this.map.get(key) ?? null;
  }
}

async function concurrentGclidScopeAndAtLeastOnceContract() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  const runPair = async ({ sameOwner }) => {
    const kv = new PointerReadBarrierKV(2);
    const queue = new MockQueue();
    const env = {
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      AD_CLICK_KV: kv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const makeRequest = (suffix) => new Request("https://www.eden.health/?gclid=CONCURRENT-GCLID&utm_source=google", {
      headers: {
        "User-Agent": "Mozilla/5.0",
        Cookie: `eden_anonymous_id=concurrent-anon-${sameOwner ? "same" : suffix}; eden_session_id=concurrent-session-${sameOwner ? "same" : suffix}_1780000000000`,
      },
    });
    const contexts = [makeCtx(), makeCtx()];
    const responses = await Promise.all([
      worker.fetch(makeRequest("a"), env, contexts[0]),
      worker.fetch(makeRequest("b"), env, contexts[1]),
    ]);
    await Promise.all(responses.map((response) => response.text()));
    await Promise.all(contexts.flatMap((ctx) => ctx.promises));
    return queue.messages.map((message) => message.payload).filter((payload) => payload.event_type === "ad_click_snapshot");
  };
  try {
    const differentOwners = await runPair({ sameOwner: false });
    assert.equal(differentOwners.length, 2);
    assert.notEqual(differentOwners[0].ad_click_id, differentOwners[1].ad_click_id, "concurrent different owners must never claim one global GCLID object");
    assert.equal(differentOwners[0].snapshot.evidence.raw_primary_click_id_sha256, differentOwners[1].snapshot.evidence.raw_primary_click_id_sha256, "replay detection remains global through the raw click hash");

    const sameOwner = await runPair({ sameOwner: true });
    assert.equal(sameOwner.length, 2, "each independently handled native request is an append-only observation");
    assert.equal(new Set(sameOwner.map((payload) => payload.ad_click_id)).size, 1);
    assert.equal(new Set(sameOwner.map((payload) => payload.snapshot.snapshot_id)).size, 2, "independent builds must receive unique stable-in-envelope snapshot identities");
    assert.notEqual(JSON.stringify(sameOwner[0].snapshot), JSON.stringify(sameOwner[1].snapshot), "different snapshot_ids must prevent conflicting payloads from sharing one storage identity");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await concurrentGclidScopeAndAtLeastOnceContract();

async function adClickMemoryResilientParamExtractionSmoke() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const kv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      AD_CLICK_KV: kv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const nested = encodeURIComponent("https://app.eden.health/intake?dclid=NESTED-DCLID&utm_campaign=nested&new_google_param=future");
    const req = new Request(`https://www.eden.health/?WBRAID=UPPER-WBRAID&amp%3Bgbraid=AMP-GBRAID&redirect=${nested}&customer_email=blocked@example.com&utm_source=Google&utm_medium=cpc`, {
      headers: { "User-Agent": "Mozilla/5.0" },
    });
    const ctx = makeCtx();
    const res = await worker.fetch(req, env, ctx);
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(queue.messages.length, 1);
    const snapshot = queue.messages[0].payload.snapshot;
    assert.equal(snapshot.google.wbraid, "UPPER-WBRAID");
    assert.equal(snapshot.google.gbraid, "AMP-GBRAID");
    assert.equal(snapshot.google.dclid, "NESTED-DCLID");
    assert.deepEqual(snapshot.evidence.upload_candidate_types.sort(), ["gbraid", "wbraid"]);
    assert.deepEqual(snapshot.evidence.destination_specific_candidate_types, ["dclid"]);
    assert.equal(snapshot.governance.ad_click_id_scope, "first_party_scoped", "braid-primary evidence must mint a first-party-scoped identity");
    assert.equal(snapshot.evidence.evidence_classes.dclid, "destination_specific_google_click_id");
    assert.equal(snapshot.landing_url_sanitized.includes("UPPER-WBRAID"), false);
    assert.equal(snapshot.landing_url_sanitized.includes("AMP-GBRAID"), false);
    assert.equal(snapshot.landing_url_sanitized.includes("NESTED-DCLID"), false);
    assert.ok(snapshot.diagnostic_google.query_param_observation.unknown_query_keys.includes("new_google_param"));
    assert.ok(snapshot.diagnostic_google.query_param_observation.blocked_query_keys.includes("customer_email"));
    assert.ok(snapshot.diagnostic_google.query_param_observation.normalized_query_key_aliases.some((alias) => alias.includes("WBRAID=>wbraid")));
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickMemoryResilientParamExtractionSmoke();

async function adClickMemoryDclidOnlyIsDestinationSpecificSmoke() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const kv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      AD_CLICK_KV: kv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const req = new Request("https://www.eden.health/?DCLID=DCLID-ONLY&utm_source=google&utm_medium=cpc&utm_campaign=dclid_only", {
      headers: { "User-Agent": "Mozilla/5.0" },
    });
    const ctx = makeCtx();
    const res = await worker.fetch(req, env, ctx);
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(queue.messages.length, 1);
    const snapshot = queue.messages[0].payload.snapshot;
    assert.equal(snapshot.google.dclid, "DCLID-ONLY");
    assert.equal(snapshot.evidence.primary_click_id_type, "dclid");
    assert.deepEqual(snapshot.evidence.upload_candidate_types, []);
    assert.deepEqual(snapshot.evidence.destination_specific_candidate_types, ["dclid"]);
    assert.equal(snapshot.evidence.missing_gclid_reason, "dclid_only");
    assert.equal(snapshot.governance.resolution_source, "fresh_destination_specific_click");
    assert.equal(snapshot.governance.final_upload_eligibility_source, "dbt_google_outbox_validator");
    assert.equal(Object.prototype.hasOwnProperty.call(snapshot.governance, "allowed_for_google_upload"), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickMemoryDclidOnlyIsDestinationSpecificSmoke();

async function adClickMemoryCollectBodyFragmentSmoke() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push({ url: String(url), body: JSON.parse(init.body) });
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const kv = new MockKV();
    const queue = new MockQueue();
    const pageUrl = "https://app.eden.health/intake#WBRAID=FRAGMENT-WBRAID&utm_source=google&utm_medium=cpc";
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      SEGMENT_WRITE_KEY: "test_write_key",
      AD_CLICK_KV: kv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const req = new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": "eden_anon_id=anon-fragment; eden_session_id=session-fragment_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "fragment_context_smoke",
        anonymousId: "anon-fragment",
        properties: { page_url: pageUrl },
        context: { page: { url: pageUrl, path: "/intake" } },
      }),
    });
    const ctx = makeCtx();
    const res = await worker.fetch(req, env, ctx);
    await res.json();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(segmentCalls.length, 1);
    assert.equal(segmentCalls[0].body.context.campaign.wbraid, "FRAGMENT-WBRAID", "body-supplied page URL fragments should enrich Segment payloads");
    assert.equal(queue.messages.length, 1, "body-supplied fragment click evidence should create ad-click memory");
    assert.equal(queue.messages[0].payload.snapshot.google.wbraid, "FRAGMENT-WBRAID");
    assert.equal(queue.messages[0].payload.snapshot.landing_url_sanitized.includes("#"), false, "snapshot URL must drop the full fragment after click extraction");
    assert.equal(queue.messages[0].payload.snapshot.landing_url_sanitized.includes("FRAGMENT-WBRAID"), false, "snapshot URL must not persist raw fragment click evidence");
    assert.equal(segmentCalls[0].body.properties.page_url.includes("#"), false, "Segment page URL must drop the raw fragment after extraction");
    assert.equal(segmentCalls[0].body.properties.session_page_url.includes("#"), false, "derived session page URL must drop the raw fragment after extraction");
    assert.equal(segmentCalls[0].body.properties.session_page_url.includes("FRAGMENT-WBRAID"), false, "derived session page URL must not leak fragment click evidence");
    assert.equal(segmentCalls[0].body.context.page.url.includes("#"), false, "Segment context.page URL must drop the raw fragment after extraction");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickMemoryCollectBodyFragmentSmoke();

async function relativeAndMalformedEventUrlsAreSanitizedBeforePersistence() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      SEGMENT_WRITE_KEY: "test_write_key",
      AD_CLICK_KV: new MockKV(),
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const send = async (pageUrl, event, anon, session) => {
      const request = new Request("https://collect.eden.health/collect", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Origin": "https://app.eden.health",
          "Cookie": `eden_anonymous_id=${anon}; eden_anon_id=${anon}; eden_session_id=${session}`,
        },
        body: JSON.stringify({
          type: "track",
          event,
          anonymousId: anon,
          properties: { page_url: pageUrl },
          context: { page: { url: pageUrl, path: "/intake" } },
        }),
      });
      const ctx = makeCtx();
      const response = await worker.fetch(request, env, ctx);
      await response.json();
      await Promise.all(ctx.promises);
      assert.equal(response.status, 200);
      return segmentCalls.at(-1);
    };

    const relative = await send(
      "/intake?gclid=RELATIVE-GCLID&utm_source=google#gbraid=RELATIVE-FRAGMENT-BRAID",
      "relative_url_capture",
      "relative-anon",
      "relative-session_1780000000000",
    );
    assert.equal(queue.messages.length, 1, "relative event URL click evidence should still be captured before sanitization");
    assert.equal(queue.messages[0].payload.snapshot.google.gclid, "RELATIVE-GCLID");
    assert.equal(JSON.stringify(relative).includes("RELATIVE-GCLID"), true, "captured click evidence remains in governed attribution fields");
    for (const value of [relative.properties.page_url, relative.properties.session_page_url, relative.context.page.url]) {
      assert.equal(String(value).includes("RELATIVE-GCLID"), false, "relative persisted URLs must not contain raw click IDs");
      assert.equal(String(value).includes("RELATIVE-FRAGMENT-BRAID"), false, "relative persisted URLs must not contain raw fragment evidence");
      assert.equal(String(value).includes("#"), false, "relative persisted URLs must not contain fragments");
    }

    const malformed = await send(
      "https://[malformed?gclid=MALFORMED-GCLID#gbraid=MALFORMED-BRAID",
      "malformed_url_drop",
      "malformed-anon",
      "malformed-session_1780000000000",
    );
    assert.equal(JSON.stringify(malformed).includes("MALFORMED-GCLID"), false, "malformed raw URL must be dropped rather than persisted");
    assert.equal(JSON.stringify(malformed).includes("MALFORMED-BRAID"), false, "malformed raw fragment must be dropped rather than persisted");
    assert.equal(malformed.properties.page_url, "");
    assert.equal(malformed.context.page.url, "");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await relativeAndMalformedEventUrlsAreSanitizedBeforePersistence();

async function adClickMemoryOffModeIsInert() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push({ url: String(url), body: JSON.parse(init.body) });
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const edgeQueue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "off",
      SEGMENT_WRITE_KEY: "test_write_key",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
      EDGE_EVENTS_QUEUE: edgeQueue,
    };
    const ctx = makeCtx();
    const pageUrl = "https://app.eden.health/intake?gclid=gclid-off-mode&utm_source=google&utm_medium=cpc";
    const req = new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": "eden_anon_id=anon-off; eden_session_id=session-off_1780000000000; __Secure-eden_ad_click_id=adclk2_existing_off",
      },
      body: JSON.stringify({
        type: "track",
        event: "os_question_answered",
        anonymousId: "anon-off",
        userId: "user-off",
        properties: { order_id: "order-off" },
        context: { page: { url: pageUrl, path: "/intake" } },
      }),
    });
    const res = await worker.fetch(req, env, ctx);
    await res.json();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(queue.messages.length, 0);
    assert.equal(edgeQueue.messages.length, 0);
    assert.equal([...adClickKv.map.keys()].filter((key) => key.startsWith("adclick:")).length, 0);
    assert.equal(getSetCookie(res.headers).some((cookie) => cookie.startsWith("__Secure-eden_ad_click_id=")), false);
    assert.equal(segmentCalls.length, 1);
    assert.equal(segmentCalls[0].body.properties.ad_click_id, undefined);
    assert.equal(segmentCalls[0].body.properties.ad_click_memory_mode, undefined);
    assert.equal(segmentCalls[0].body.properties.ad_click_primary_type, undefined);
    assert.equal(segmentCalls[0].body.properties.ad_click_evidence_class, undefined);
    assert.equal(segmentCalls[0].body.context.ad_click_id, undefined);
    assert.equal(segmentCalls[0].body.context.ad_click_memory_mode, undefined);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickMemoryOffModeIsInert();

async function adClickMemoryGpcOnlyDoesNotSuppressCapture() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const ctx = makeCtx();
    const req = new Request("https://www.eden.health/?gclid=gclid-gpc-diagnostic&utm_source=google&utm_medium=cpc", {
      headers: { "Sec-GPC": "1", "User-Agent": "Mozilla/5.0" },
    });
    const res = await worker.fetch(req, env, ctx);
    await res.text();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(queue.messages.length, 1, "raw GPC should not suppress unless CookieYes/eden_consent_state says denied");
    assert.equal([...adClickKv.map.keys()].filter((key) => key.startsWith("adclick:id:")).length, 1);
    assert.equal(getSetCookie(res.headers).some((cookie) => cookie.startsWith("__Secure-eden_ad_click_id=")), true);
    assert.equal(getSetCookie(res.headers).some((cookie) => cookie.startsWith("__Secure-eden_ads_denied=")), false, "raw Sec-GPC alone must never set the explicit-denial marker");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickMemoryGpcOnlyDoesNotSuppressCapture();

async function adClickMemoryCookieYesGpcDefaultStatePreservesCapture() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const consent = encodeURIComponent(JSON.stringify({
      source: "gpc",
      action_taken: false,
      consent_status: "opted_out",
      google_ads: "denied",
      advertising: "denied",
      ad_tracking: "denied",
      partner_ad_tracking: "denied",
      retargeting: "denied",
      sale_share_targeted_ads: "opted_out",
    }));
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const ctx = makeCtx();
    const req = new Request("https://www.eden.health/?gclid=gclid-cookieyes-gpc&utm_source=google&utm_medium=cpc", {
      headers: { "Sec-GPC": "1", "Cookie": `eden_consent_state=${consent}; eden_anon_id=anon-cookieyes-gpc`, "User-Agent": "Mozilla/5.0" },
    });
    const res = await worker.fetch(req, env, ctx);
    await res.text();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(queue.messages.length, 1, "CookieYes GPC/default state without a user action is diagnostic, not an opt-out");
    assert.equal([...adClickKv.map.keys()].filter((key) => key.startsWith("adclick:id:")).length, 1);
    assert.equal(getSetCookie(res.headers).some((cookie) => cookie.startsWith("__Secure-eden_ad_click_id=")), true);
    assert.equal(getSetCookie(res.headers).some((cookie) => cookie.startsWith("__Secure-eden_ads_denied=1")), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickMemoryCookieYesGpcDefaultStatePreservesCapture();

async function adClickMemoryConsentOptOutSuppressesCapture() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const consent = encodeURIComponent(JSON.stringify({
      source: "eden_preference_center",
      action_taken: "true",
      ads_opted_out: "true",
      google_ads_allowed: "false",
      allowed_for_google_click_id_upload: "false",
    }));
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const ctx = makeCtx();
    const req = new Request("https://www.eden.health/?gclid=gclid-privacy-denied&utm_source=google&utm_medium=cpc", {
      headers: { "Cookie": `eden_consent_state=${consent}; eden_anon_id=anon-privacy-denied`, "User-Agent": "Mozilla/5.0" },
    });
    const res = await worker.fetch(req, env, ctx);
    await res.text();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(queue.messages.length, 0);
    assert.equal([...adClickKv.map.keys()].filter((key) => key.startsWith("adclick:")).length, 0);
    assert.equal(getSetCookie(res.headers).some((cookie) => cookie.startsWith("__Secure-eden_ad_click_id=")), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickMemoryConsentOptOutSuppressesCapture();

async function adClickMemoryExplicitConsentAllowsWithGpc() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const consent = encodeURIComponent(JSON.stringify({
      source: "eden_preference_center",
      action_taken: true,
      consent_status: "explicit_allowed",
      google_ads: "allowed",
      advertising: "allowed",
      ad_tracking: "allowed",
      partner_ad_tracking: "allowed",
      retargeting: "allowed",
      sale_share_targeted_ads: "allowed",
    }));
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const ctx = makeCtx();
    const req = new Request("https://www.eden.health/?gclid=gclid-explicit-allow&utm_source=google&utm_medium=cpc", {
      headers: { "Sec-GPC": "1", "Cookie": `eden_consent_state=${consent}; eden_anon_id=anon-explicit-allow`, "User-Agent": "Mozilla/5.0" },
    });
    const res = await worker.fetch(req, env, ctx);
    await res.text();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(queue.messages.length, 1);
    assert.equal([...adClickKv.map.keys()].filter((key) => key.startsWith("adclick:id:")).length, 1);
    assert.equal(getSetCookie(res.headers).some((cookie) => cookie.startsWith("__Secure-eden_ad_click_id=")), true);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickMemoryExplicitConsentAllowsWithGpc();

async function adClickMemoryDefaultAllowedConsentAllowsWithGpc() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const consent = encodeURIComponent(JSON.stringify({
      source: "default_allowed_no_choice",
      action_taken: false,
      consent_status: "default_allowed",
      google_ads: "allowed",
      advertising: "allowed",
      ad_tracking: "allowed",
      partner_ad_tracking: "allowed",
      retargeting: "allowed",
      sale_share_targeted_ads: "allowed",
    }));
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const ctx = makeCtx();
    const req = new Request("https://www.eden.health/?gclid=gclid-default-allowed-gpc&utm_source=google&utm_medium=cpc", {
      headers: { "Sec-GPC": "1", "Cookie": `eden_consent_state=${consent}; eden_anon_id=anon-default-allowed-gpc`, "User-Agent": "Mozilla/5.0" },
    });
    const res = await worker.fetch(req, env, ctx);
    await res.text();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(queue.messages.length, 1);
    assert.equal([...adClickKv.map.keys()].filter((key) => key.startsWith("adclick:id:")).length, 1);
    assert.equal(getSetCookie(res.headers).some((cookie) => cookie.startsWith("__Secure-eden_ad_click_id=")), true);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickMemoryDefaultAllowedConsentAllowsWithGpc();

async function adClickMemoryLegacyExplicitAllowAllowsWithGpc() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const consent = encodeURIComponent(JSON.stringify({
      source: "explicit_allow",
      action_taken: true,
      consent_status: "allowed",
      google_ads: "allowed",
      advertising: "allowed",
      ad_tracking: "allowed",
      partner_ad_tracking: "allowed",
      retargeting: "allowed",
      sale_share_targeted_ads: "allowed",
    }));
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const ctx = makeCtx();
    const req = new Request("https://www.eden.health/?gclid=gclid-legacy-explicit-allow&utm_source=google&utm_medium=cpc", {
      headers: { "Sec-GPC": "1", "Cookie": `eden_consent_state=${consent}; eden_anon_id=anon-legacy-explicit-allow`, "User-Agent": "Mozilla/5.0" },
    });
    const res = await worker.fetch(req, env, ctx);
    await res.text();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(queue.messages.length, 1);
    assert.equal([...adClickKv.map.keys()].filter((key) => key.startsWith("adclick:id:")).length, 1);
    assert.equal(getSetCookie(res.headers).some((cookie) => cookie.startsWith("__Secure-eden_ad_click_id=")), true);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickMemoryLegacyExplicitAllowAllowsWithGpc();

async function adClickMemoryMalformedConsentCookieDoesNotSuppress() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const ctx = makeCtx();
    const req = new Request("https://www.eden.health/?gclid=gclid-malformed-consent&utm_source=google&utm_medium=cpc", {
      headers: { "Cookie": "eden_consent_state=%E0%A4%A; eden_anon_id=anon-malformed-consent", "User-Agent": "Mozilla/5.0" },
    });
    const res = await worker.fetch(req, env, ctx);
    await res.text();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(queue.messages.length, 1);
    assert.equal([...adClickKv.map.keys()].filter((key) => key.startsWith("adclick:id:")).length, 1);
    assert.equal(getSetCookie(res.headers).some((cookie) => cookie.startsWith("__Secure-eden_ad_click_id=")), true);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickMemoryMalformedConsentCookieDoesNotSuppress();

async function adClickMemoryDoesNotUseEdgeEventsQueue() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const edgeQueue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "shadow",
      EDGE_EVENTS_QUEUE: edgeQueue,
    };
    const ctx = makeCtx();
    const req = new Request("https://www.eden.health/?gclid=gclid-edge-queue&utm_source=google&utm_medium=cpc", {
      headers: { "User-Agent": "Mozilla/5.0" },
    });
    const res = await worker.fetch(req, env, ctx);
    await res.text();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(edgeQueue.messages.length, 0, "ad-click memory must never fall back to EDGE_EVENTS_QUEUE");
    assert.equal(getSetCookie(res.headers).some((cookie) => cookie.startsWith("__Secure-eden_ad_click_id=")), false, "shadow mode must not set pointer cookie");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickMemoryDoesNotUseEdgeEventsQueue();


async function adClickMemoryCookieModeRequiresKvForPointer() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const ctx = makeCtx();
    const req = new Request("https://www.eden.health/?gclid=gclid-cookie-no-kv&utm_source=google&utm_medium=cpc", {
      headers: { "User-Agent": "Mozilla/5.0" },
    });
    const res = await worker.fetch(req, env, ctx);
    await res.text();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(queue.messages.length, 1, "cookie mode without AD_CLICK_KV can still queue the restricted snapshot");
    assert.equal(getSetCookie(res.headers).some((cookie) => cookie.startsWith("__Secure-eden_ad_click_id=")), false, "cookie mode must not set pointer cookie without AD_CLICK_KV backing");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickMemoryCookieModeRequiresKvForPointer();

async function adClickMemoryCookieModeDoesNotAnnotateSegment() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push({ url: String(url), body: JSON.parse(init.body) });
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      SEGMENT_WRITE_KEY: "test_write_key",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const ctx = makeCtx();
    const pageUrl = "https://app.eden.health/intake?gclid=gclid-cookie-mode&utm_source=google&utm_medium=cpc";
    const req = new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": "eden_anon_id=anon-cookie; eden_session_id=session-cookie_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "os_question_answered",
        anonymousId: "anon-cookie",
        userId: "user-cookie",
        properties: { order_id: "order-cookie" },
        context: { page: { url: pageUrl, path: "/intake" } },
      }),
    });
    const res = await worker.fetch(req, env, ctx);
    await res.json();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(queue.messages.length, 1);
    assert.equal(queue.messages[0].payload.resolution.resolution_source, "fresh_class_a_click");
    assert.equal(queue.messages[0].payload.resolution.resolution_confidence, "high");
    assert.equal(queue.messages[0].payload.resolution.resolution_conflict, false);
    assert.equal(queue.messages[0].payload.snapshot.governance.resolution_source, "fresh_class_a_click");
    const adClickKeys = [...adClickKv.map.keys()].filter((key) => key.startsWith("adclick:"));
    assert.equal(adClickKeys.length, 1, "cookie mode should write only the minimal adclick:id pointer backing record");
    assert.ok(adClickKeys[0].startsWith("adclick:id:adclk2_"));
    assert.equal(getSetCookie(res.headers).some((cookie) => cookie.startsWith("__Secure-eden_ad_click_id=")), true, "cookie mode with AD_CLICK_KV should set pointer cookie");
    assert.equal(segmentCalls.length, 1);
    assert.equal(segmentCalls[0].body.properties.ad_click_id, undefined);
    assert.equal(segmentCalls[0].body.properties.ad_click_memory_mode, undefined);
    assert.equal(segmentCalls[0].body.properties.ad_click_primary_type, undefined);
    assert.equal(segmentCalls[0].body.properties.ad_click_evidence_class, undefined);
    assert.equal(segmentCalls[0].body.context.ad_click_id, undefined);
    assert.equal(segmentCalls[0].body.context.ad_click_memory_mode, undefined);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickMemoryCookieModeDoesNotAnnotateSegment();

async function adClickMemoryIdentityOnlyKvDoesNotClobberSnapshot() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push({ url: String(url), body: JSON.parse(init.body) });
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const existingAdClickId = "adclk2_existing_kv_review";
    await adClickKv.put(`adclick:id:${existingAdClickId}`, JSON.stringify({
      schema_version: "eden_ad_click_pointer_v2",
      ad_click_id: existingAdClickId,
      snapshot_id: "adsnap_existing",
      primary_click_id_type: "gclid",
      evidence_classes: { gclid: "class_a_google_ads_upload_click_id" },
      has_class_a: true,
      has_primary_click_evidence: true,
      owner_anonymous_id_sha256: await sha256Raw("anon-kv-link"),
      owner_session_id_sha256: await sha256Raw("session-kv-link_1780000000000"),
      ad_click_id_scope: "first_party_scoped",
      ownership_scope: "first_party_owner_bound",
    }));
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      SEGMENT_WRITE_KEY: "test_write_key",
      SERVER_API_SECRET: "test_server_secret",
      GCLID_KV: new MockKV(),
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const makeReq = () => new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "X-Eden-Server-Secret": "test_server_secret",
        "Cookie": `eden_anon_id=anon-kv-link; eden_session_id=session-kv-link_1780000000000; __Secure-eden_ad_click_id=${existingAdClickId}`,
      },
      body: JSON.stringify({
        type: "track",
        event: "OS_purchase",
        anonymousId: "anon-kv-link",
        userId: "user-kv-link",
        properties: { order_id: "order-kv-link", transaction_id: "charge-kv-link" },
        context: { page: { url: "https://app.eden.health/intake", path: "/intake" } },
      }),
    });
    const ctx1 = makeCtx();
    const res1 = await worker.fetch(makeReq(), env, ctx1);
    await res1.json();
    await Promise.all(ctx1.promises);
    const ctx2 = makeCtx();
    const res2 = await worker.fetch(makeReq(), env, ctx2);
    await res2.json();
    await Promise.all(ctx2.promises);
    assert.equal(res1.status, 200);
    assert.equal(res2.status, 200);
    const idRecord = JSON.parse(await adClickKv.get(`adclick:id:${existingAdClickId}`));
    assert.equal(idRecord.primary_click_id_type, "gclid", "identity-only KV write must not clobber snapshot id record");
    assert.equal(idRecord.snapshot_id, "adsnap_existing");
    const adClickKeys = [...adClickKv.map.keys()].filter((key) => key.startsWith("adclick:"));
    assert.deepEqual(adClickKeys, [`adclick:id:${existingAdClickId}`], "identity-only events must not add speculative KV reverse indexes before a resolver exists");
    const linkEnvelopes = queue.messages.map((message) => message.payload).filter((envelope) => envelope.event_type === "ad_click_identity_links");
    assert.equal(linkEnvelopes.length, 1, "an exact acknowledged conversion retry must not fan out another identity-link envelope");
    assert.equal(linkEnvelopes[0].resolution.resolution_source, "pointer_cookie");
    assert.equal(linkEnvelopes[0].resolution.resolution_confidence, "high");
    assert.equal(linkEnvelopes[0].resolution.resolution_conflict, false);
    const orderLinks = linkEnvelopes.map((envelope) => envelope.identity_links.find((link) => link.from_type === "order_id_sha256" && link.to_type === "ad_click_id")).filter(Boolean);
    assert.equal(orderLinks.length, 1);
    assert.match(orderLinks[0].link_id, /^adlink_[a-f0-9]{32}$/);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickMemoryIdentityOnlyKvDoesNotClobberSnapshot();

async function adClickMemoryFullKvIndexModeWritesBridgesWithoutClobberingPointer() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const existingAdClickId = "adclk2_existing_full_index";
    await adClickKv.put(`adclick:id:${existingAdClickId}`, JSON.stringify({
      schema_version: "eden_ad_click_pointer_v2",
      ad_click_id: existingAdClickId,
      snapshot_id: "adsnap_full_index_existing",
      primary_click_id_type: "gclid",
      evidence_classes: { gclid: "class_a_google_ads_upload_click_id" },
      has_class_a: true,
      has_primary_click_evidence: true,
      owner_anonymous_id_sha256: await sha256Raw("anon-full-index"),
      owner_session_id_sha256: await sha256Raw("session-full-index_1780000000000"),
      ad_click_id_scope: "first_party_scoped",
      ownership_scope: "first_party_owner_bound",
    }));
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      EDEN_AD_CLICK_KV_INDEX_MODE: "full",
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const req = new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "X-Eden-Server-Secret": "test_server_secret",
        "Cookie": `eden_anon_id=anon-full-index; eden_session_id=session-full-index_1780000000000; __Secure-eden_ad_click_id=${existingAdClickId}`,
      },
      body: JSON.stringify({
        type: "track",
        event: "OS_purchase",
        anonymousId: "anon-full-index",
        userId: "user-full-index",
        properties: { order_id: "order-full-index", transaction_id: "charge-full-index" },
        context: { page: { url: "https://app.eden.health/intake", path: "/intake" } },
      }),
    });
    const ctx = makeCtx();
    const res = await worker.fetch(req, env, ctx);
    await res.json();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    const idRecord = JSON.parse(await adClickKv.get(`adclick:id:${existingAdClickId}`));
    assert.equal(idRecord.snapshot_id, "adsnap_full_index_existing", "full-index identity-only writes must not clobber existing pointer record");
    const adClickKeys = [...adClickKv.map.keys()].filter((key) => key.startsWith("adclick:"));
    assert.ok(adClickKeys.some((key) => key.includes(":anon:")), "full index mode should add anon bridge when explicitly enabled");
    assert.ok(adClickKeys.some((key) => key.includes(":session:")), "full index mode should add session bridge when explicitly enabled");
    assert.ok(adClickKeys.some((key) => key.includes(":user:")), "full index mode should add user bridge when explicitly enabled");
    assert.ok(adClickKeys.some((key) => key.includes(":order:")), "full index mode should add order bridge when explicitly enabled");
    assert.equal(queue.messages.some((message) => message.payload.event_type === "ad_click_identity_links"), true);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickMemoryFullKvIndexModeWritesBridgesWithoutClobberingPointer();

async function adClickHealthWarnsWhenFullKvIndexesAreActive() {
  const env = {
    EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
    EDEN_AD_CLICK_MEMORY_MODE: "cookie",
    EDEN_AD_CLICK_KV_INDEX_MODE: "full",
    AD_CLICK_MEMORY_QUEUE_CONSUMER_ENABLED: "true",
    AD_CLICK_BIGQUERY_PROJECT_ID: "fixture-project",
    AD_CLICK_BIGQUERY_DATASET_ID: "fixture_dataset",
    AD_CLICK_BIGQUERY_ACCESS_TOKEN: "fixture-token",
    SEGMENT_WRITE_KEY: "fixture",
    AD_CLICK_KV: new MockKV(),
    AD_CLICK_SNAPSHOT_QUEUE: new MockQueue(),
    GCLID_KV: new MockKV(),
  };
  const ctx = makeCtx();
  const res = await worker.fetch(new Request("https://collect.eden.health/eden-health-check"), env, ctx);
  const health = await res.json();
  await Promise.all(ctx.promises);
  assert.equal(res.status, 200);
  assert.equal(health.ad_click_memory_mode, "cookie");
  assert.equal(health.ad_click_kv_index_mode, "full");
  assert.equal(health.ad_click_kv_resolver_mode, "pointer_only");
  assert.equal(health.ad_click_kv_resolver_requested_mode, "pointer_only");
  assert.equal(health.ad_click_kv_reverse_read_active, false);
  assert.equal(health.ad_click_kv_resolver_implemented, true);
  assert.equal(health.ad_click_kv_resolver_policy_version, "ad_click_all_first_party_scope_policy_v3");
  assert.deepEqual(health.google_click_bridge_params, [], "all raw ad-ID bridge params must be retired");
  for (const retired of ["gclid", "gbraid", "wbraid", "dclid", "_gcl_au", "fbclid", "msclkid"]) {
    assert.ok(health.google_click_bridge_retired_params.includes(retired), `${retired} must remain evidence-only`);
  }
  assert.match(health.cross_domain_bridge, /same-registrable-domain \.eden\.health/);
  assert.match(health.cross_domain_bridge, /no tryeden\.com or edenrx\.co cookie continuity is claimed/);
  assert.match(health.ad_click_kv_reverse_key_schema, /adclick:v2:\{anon\|session\|user\|order\}/);
  assert.match(health.ad_click_id_scope_contract, /first-party context/);
  assert.equal(health.ad_click_reverse_kv_retention_mode, "ttl");
  assert.equal(health.ad_click_reverse_kv_expiration_ttl_seconds, 7776000);
  assert.equal(health.ad_click_kv_full_index_active, true);
  assert.match(health.ad_click_kv_full_index_warning, /live-gated/);
  assert.match(health.ad_click_kv_first_paid_consistency, /best-effort KV put-if-absent/);
  assert.match(health.mutation_auth_contract, /fails closed/);
  assert.equal(health.max_json_body_bytes, 65536);
  assert.equal(health.ad_click_pointer_record_schema, "eden_ad_click_pointer_v2");
  assert.match(health.ad_click_pointer_integrity, /Durable Object state plus first-party owner hash is authoritative/);
  assert.match(health.ad_click_snapshot_identity, /append-only immutable observation snapshots/);
  assert.match(health.ad_click_snapshot_identity, /unique stable-in-envelope snapshot_id/);
  assert.match(health.conversion_dedup_contract, /eden_conversion_dedup_v4/);
  assert.match(health.conversion_dedup_contract, /strongly consistent stable-conversion-key Durable Object/);
  assert.match(health.conversion_dedup_contract, /synchronous Segment acknowledgement/);
  assert.equal(health.conversion_coordinator_configured, true);
  assert.equal(health.conversion_coordinator_health_ok, true);
  assert.equal(health.conversion_coordinator_schema_version, "eden_conversion_coordinator_v1");
  assert.equal(health.conversion_coordinator_storage_readable, true);
  assert.match(health.conversion_event_semantics, /distinct business milestones/);
  assert.match(health.conversion_serialization_contract, /Durable Object lease/);
  assert.match(health.conversion_serialization_contract, /stable conversion key/);
  assert.match(health.conversion_serialization_contract, /Segment-acknowledged pending persistence/);
  assert.match(health.conversion_serialization_contract, /distinct milestone keeps its own durable record/);
  assert.match(health.conversion_ledger_authority, /Durable Object per stable conversion key is canonical/);
  assert.match(health.conversion_ledger_authority, /raw-free dedup:v4/);
  assert.match(health.conversion_ledger_authority, /still-live v5.55 overloaded one-day rows/);
  assert.match(health.conversion_unknown_commit_retry_contract, /HTTP 5xx ambiguity/);
  assert.match(health.conversion_unknown_commit_retry_contract, /exact bounded Segment payload/);
  assert.match(health.conversion_unknown_commit_retry_contract, /replayed byte-identically/);
  assert.match(health.conversion_unknown_commit_retry_contract, /current changed truth uses a separate stable enrichment/);
  assert.match(health.conversion_unknown_commit_retry_contract, /conversion_retry_state_incomplete_or_regressed/);
  assert.equal(health.conversion_coordinator_lease_ttl_ms, 120000);
  assert.equal(health.conversion_segment_timeout_ms, 30000);
  assert.match(health.stable_identifier_contract, /OS_purchase requires one charge transaction ID/);
  assert.match(health.stable_identifier_contract, /safe integer numbers/);
  assert.equal(health.conversion_dedup_ttl_seconds, 31536000);
  assert.match(health.canonical_anonymous_id, /eden_anonymous_id/);
  assert.match(health.landing_url_fragment_policy, /fragments are removed/);
  assert.equal(health.advertising_denial_ledger_schema, "eden_ads_denial_v1");
  assert.match(health.advertising_denial_ledger_keying, /Google click IDs are never privacy-ledger join keys/);
  assert.match(health.advertising_denial_ledger_keying, /HMAC-SHA256/);
  assert.equal(health.privacy_ledger_hmac_secret_configured, true);
  assert.equal(health.advertising_denial_ttl_seconds, 31536000);
  assert.equal(health.advertising_denial_marker_cookie, "__Secure-eden_ads_denied");
  assert.match(health.advertising_denial_pointer_policy, /immediately restores current tracking/);
  assert.match(health.advertising_denial_pointer_policy, /durable tombstone cleanup must be retried/);
  assert.match(health.advertising_denial_pointer_policy, /never unrevokes the old pointer/);
  assert.match(health.ad_click_observability_contract, /source_pipeline_version/);
}

await adClickHealthWarnsWhenFullKvIndexesAreActive();

async function adClickResolverFullRequestIsBlockedUntilImplementedAndAccepted() {
  const env = {
    EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
    EDEN_AD_CLICK_MEMORY_MODE: "cookie",
    EDEN_AD_CLICK_KV_INDEX_MODE: "full",
    EDEN_AD_CLICK_KV_RESOLVER_MODE: "full",
    AD_CLICK_MEMORY_QUEUE_CONSUMER_ENABLED: "true",
    AD_CLICK_BIGQUERY_PROJECT_ID: "fixture-project",
    AD_CLICK_BIGQUERY_DATASET_ID: "fixture_dataset",
    AD_CLICK_BIGQUERY_ACCESS_TOKEN: "fixture-token",
    SEGMENT_WRITE_KEY: "fixture",
    AD_CLICK_KV: new MockKV(),
    AD_CLICK_SNAPSHOT_QUEUE: new MockQueue(),
    GCLID_KV: new MockKV(),
  };
  const ctx = makeCtx();
  const res = await worker.fetch(new Request("https://collect.eden.health/eden-health-check"), env, ctx);
  const health = await res.json();
  await Promise.all(ctx.promises);
  assert.equal(res.status, 200);
  assert.equal(health.ad_click_kv_resolver_requested_mode, "full");
  assert.equal(health.ad_click_kv_resolver_mode, "pointer_only", "full resolver reads must be blocked until the resolver contract is accepted");
  assert.equal(health.ad_click_kv_resolver_implemented, true);
  assert.equal(health.ad_click_kv_reverse_read_active, false);
  assert.match(health.ad_click_kv_resolver_warning, /CONTRACT_ACCEPTED=true/);

  const acceptedCtx = makeCtx();
  const acceptedRes = await worker.fetch(new Request("https://collect.eden.health/eden-health-check"), {
    ...env,
    EDEN_AD_CLICK_KV_RESOLVER_CONTRACT_ACCEPTED: "true",
  }, acceptedCtx);
  const acceptedHealth = await acceptedRes.json();
  await Promise.all(acceptedCtx.promises);
  assert.equal(acceptedRes.status, 200);
  assert.equal(acceptedHealth.ad_click_kv_resolver_requested_mode, "full");
  assert.equal(acceptedHealth.ad_click_kv_resolver_mode, "full", "contract acceptance should enable the implemented reverse-KV resolver");
  assert.equal(acceptedHealth.ad_click_kv_reverse_read_active, true);
  assert.equal(acceptedHealth.ad_click_kv_resolver_implemented, true);
}

await adClickResolverFullRequestIsBlockedUntilImplementedAndAccepted();

async function adClickReverseKvResolverDisabledRollbackDrill() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const fullEnv = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      EDEN_AD_CLICK_KV_INDEX_MODE: "full",
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const fullReq = new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "X-Eden-Server-Secret": "test_server_secret",
        "Cookie": "eden_anon_id=anon-rollback; eden_session_id=session-rollback_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "OS_purchase",
        anonymousId: "anon-rollback",
        userId: "user-rollback",
        properties: { order_id: "order-rollback", transaction_id: "charge-rollback" },
        context: { page: { url: "https://app.eden.health/intake?gclid=gclid-rollback-full&_gcl_au=1.1.999888.777&utm_source=google&utm_medium=cpc", path: "/intake" } },
      }),
    });
    const fullCtx = makeCtx();
    const fullRes = await worker.fetch(fullReq, fullEnv, fullCtx);
    await fullRes.json();
    await Promise.all(fullCtx.promises);
    assert.equal(fullRes.status, 200);
    const reverseKeysAfterFull = [...adClickKv.map.keys()].filter((key) => /^adclick:v2:(anon|session|user|order):/.test(key));
    assert.ok(reverseKeysAfterFull.length >= 4, "drill setup should create v2 first-party reverse KV keys only while full writer mode is explicitly enabled");
    const clickValueReverseKeys = [...adClickKv.map.keys()].filter((key) => /^adclick:(v2:)?(gclid|gbraid|wbraid|dclid|srsltid|gcl_au):/.test(key));
    assert.equal(clickValueReverseKeys.length, 0, "full writer mode must not create click-value or _gcl_au reverse lookup keys");
    const v1ReverseKeys = [...adClickKv.map.keys()].filter((key) => /^adclick:(anon|session|user|order):/.test(key));
    assert.equal(v1ReverseKeys.length, 0, "full writer mode must not write quarantined v1 reverse key shapes");

    const beforeFullResolverMessages = queue.messages.length;
    const fullResolveEnv = {
      ...fullEnv,
      EDEN_AD_CLICK_KV_RESOLVER_MODE: "full",
      EDEN_AD_CLICK_KV_RESOLVER_CONTRACT_ACCEPTED: "true",
    };
    const fullResolveReq = new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "X-Eden-Server-Secret": "test_server_secret",
        "Cookie": "eden_anon_id=anon-rollback; eden_session_id=session-rollback_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "OS_qualified_first_order",
        anonymousId: "anon-rollback",
        userId: "user-rollback",
        properties: { order_id: "order-rollback" },
        context: { page: { url: "https://app.eden.health/intake", path: "/intake" } },
      }),
    });
    const fullResolveCtx = makeCtx();
    const fullResolveRes = await worker.fetch(fullResolveReq, fullResolveEnv, fullResolveCtx);
    await fullResolveRes.json();
    await Promise.all(fullResolveCtx.promises);
    assert.equal(fullResolveRes.status, 200);
    assert.ok(queue.messages.length > beforeFullResolverMessages, "accepted full resolver should recover an existing reverse KV candidate and enqueue diagnostic links");
    const fullResolverEnvelope = queue.messages.at(-1).payload;
    assert.equal(fullResolverEnvelope.event_type, "ad_click_identity_links");
    assert.ok(
      ["stable_order_pointer", "stable_user_pointer", "anonymous_id_bridge", "session_bridge", "anon_bridge", "user_bridge", "order_bridge"].includes(fullResolverEnvelope.resolution.resolution_source),
      "authenticated server recovery should prefer the strongly consistent stable-identity pointer and retain reverse-KV fallback compatibility",
    );
    assert.ok(["medium", "high"].includes(fullResolverEnvelope.resolution.resolution_confidence));
    assert.equal(fullResolverEnvelope.resolution.resolution_conflict, false);

    adClickKv.getKeys = [];
    const beforePointerRollbackMessages = queue.messages.length;
    const pointerRollbackEnv = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      EDEN_AD_CLICK_KV_INDEX_MODE: "pointer",
      EDEN_AD_CLICK_KV_RESOLVER_MODE: "pointer_only",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const noFreshEvidenceReq = new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": "eden_anon_id=anon-rollback; eden_session_id=session-rollback_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "os_question_answered",
        anonymousId: "anon-rollback",
        userId: "user-rollback",
        properties: { order_id: "order-rollback-2" },
        context: { page: { url: "https://app.eden.health/intake", path: "/intake" } },
      }),
    });
    const pointerCtx = makeCtx();
    const pointerRes = await worker.fetch(noFreshEvidenceReq, pointerRollbackEnv, pointerCtx);
    await pointerRes.json();
    await Promise.all(pointerCtx.promises);
    assert.equal(pointerRes.status, 200);
    assert.equal(queue.messages.length, beforePointerRollbackMessages, "pointer rollback must not recover old reverse KV records into new queue evidence");
    assert.equal(adClickKv.getKeys.some((key) => /^adclick:(v2:)?(anon|session|user|order|gclid|gbraid|wbraid|dclid|srsltid|gcl_au):/.test(key)), false, "pointer rollback must not read v1 or v2 reverse KV prefixes");

    const pointerKeysBeforeFresh = [...adClickKv.map.keys()].filter((key) => key.startsWith("adclick:"));
    const freshPointerReq = new Request("https://www.eden.health/?gclid=gclid-rollback-pointer&utm_source=google&utm_medium=cpc", {
      headers: { "Cookie": "eden_anon_id=anon-rollback-pointer; eden_session_id=session-rollback-pointer_1780000000000", "User-Agent": "Mozilla/5.0" },
    });
    const freshPointerCtx = makeCtx();
    const freshPointerRes = await worker.fetch(freshPointerReq, pointerRollbackEnv, freshPointerCtx);
    await freshPointerRes.text();
    await Promise.all(freshPointerCtx.promises);
    assert.equal(freshPointerRes.status, 200);
    assert.equal(getSetCookie(freshPointerRes.headers).some((cookie) => cookie.startsWith("__Secure-eden_ad_click_id=")), true);
    const newKeysAfterPointer = [...adClickKv.map.keys()].filter((key) => !pointerKeysBeforeFresh.includes(key));
    assert.equal(newKeysAfterPointer.length, 1, "pointer rollback should write only the pointer-backing id key for fresh click evidence");
    assert.ok(newKeysAfterPointer[0].startsWith("adclick:id:"));

    const offEnv = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      EDEN_AD_CLICK_KV_INDEX_MODE: "off",
      EDEN_AD_CLICK_KV_RESOLVER_MODE: "off",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const beforeOffMessages = queue.messages.length;
    const offReq = new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": "eden_anon_id=anon-rollback; eden_session_id=session-rollback_1780000000000; __Secure-eden_ad_click_id=adclk2_existing_after_full",
      },
      body: JSON.stringify({
        type: "track",
        event: "os_question_answered",
        anonymousId: "anon-rollback",
        userId: "user-rollback",
        properties: { order_id: "order-rollback-3" },
        context: { page: { url: "https://app.eden.health/intake", path: "/intake" } },
      }),
    });
    const offCtx = makeCtx();
    const offRes = await worker.fetch(offReq, offEnv, offCtx);
    await offRes.json();
    await Promise.all(offCtx.promises);
    assert.equal(offRes.status, 200);
    assert.equal(queue.messages.length, beforeOffMessages, "resolver off rollback must not read even the existing pointer cookie");
    assert.equal(getSetCookie(offRes.headers).some((cookie) => cookie.startsWith("__Secure-eden_ad_click_id=")), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickReverseKvResolverDisabledRollbackDrill();

async function adClickFullReverseResolverFlagsConflictsAndDanglingIndexes() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const anonHash = await crypto.subtle.digest("SHA-256", new TextEncoder().encode("anon-conflict")).then((buf) => Array.from(new Uint8Array(buf)).map((b) => b.toString(16).padStart(2, "0")).join(""));
    const sessionHash = await crypto.subtle.digest("SHA-256", new TextEncoder().encode("session-conflict_1780000000000")).then((buf) => Array.from(new Uint8Array(buf)).map((b) => b.toString(16).padStart(2, "0")).join(""));
    const orderHash = await crypto.subtle.digest("SHA-256", new TextEncoder().encode("order-conflict")).then((buf) => Array.from(new Uint8Array(buf)).map((b) => b.toString(16).padStart(2, "0")).join(""));
    const userHash = await crypto.subtle.digest("SHA-256", new TextEncoder().encode("source:user_id:user-conflict")).then((buf) => Array.from(new Uint8Array(buf)).map((b) => b.toString(16).padStart(2, "0")).join(""));
    const ownedPointerFixture = (adClickId) => JSON.stringify({
      schema_version: "eden_ad_click_pointer_v2",
      ad_click_id: adClickId,
      ad_click_id_scope: "first_party_scoped",
      owner_anonymous_id_sha256: anonHash,
      owner_session_id_sha256: sessionHash,
      claimed_user_id_sha256: userHash,
      claimed_order_id_sha256: orderHash,
      has_class_a: true,
    });
    await adClickKv.put("adclick:id:adclk2_order_priority", ownedPointerFixture("adclk2_order_priority"));
    await adClickKv.put("adclick:id:adclk2_session_conflict", ownedPointerFixture("adclk2_session_conflict"));
    await adClickKv.put(`adclick:v2:order:${orderHash}`, "adclk2_order_priority");
    await adClickKv.put(`adclick:v2:session:${sessionHash}:current`, "adclk2_session_conflict");
    await adClickKv.put(`adclick:v2:anon:${anonHash}:last_paid`, "adclk2_missing_pointer");
    await adClickKv.put(`adclick:v2:user:${userHash}:last_paid`, "adclk2_order_priority");
    // Quarantined v1 reverse index shapes must never influence resolution.
    await adClickKv.put("adclick:id:adclk_v1_legacy", JSON.stringify({ ad_click_id: "adclk_v1_legacy", has_class_a: true }));
    await adClickKv.put(`adclick:order:${orderHash}`, "adclk_v1_legacy");
    await adClickKv.put(`adclick:gbraid:${orderHash}`, "adclk_v1_legacy");
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      EDEN_AD_CLICK_KV_INDEX_MODE: "full",
      EDEN_AD_CLICK_KV_RESOLVER_MODE: "full",
      EDEN_AD_CLICK_KV_RESOLVER_CONTRACT_ACCEPTED: "true",
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const req = new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "X-Eden-Server-Secret": "test_server_secret",
        "Cookie": "eden_anon_id=anon-conflict; eden_session_id=session-conflict_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "OS_purchase",
        anonymousId: "anon-conflict",
        userId: "user-conflict",
        properties: { order_id: "order-conflict", transaction_id: "charge-conflict" },
        context: { page: { url: "https://app.eden.health/intake", path: "/intake" } },
      }),
    });
    const ctx = makeCtx();
    const res = await worker.fetch(req, env, ctx);
    await res.json();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    const envelope = queue.messages.at(-1).payload;
    assert.equal(envelope.resolution.resolution_source, "order_bridge");
    assert.equal(envelope.resolution.resolution_confidence, "high");
    assert.equal(envelope.resolution.resolution_conflict, true);
    assert.ok(envelope.resolution.resolution_conflict_sources.includes("session_bridge"));
    assert.ok(envelope.resolution.resolution_conflict_sources.includes("anonymous_bridge_dangling"));
    assert.notEqual(envelope.ad_click_id, "adclk_v1_legacy", "quarantined v1 reverse indexes must never resolve an ad_click_id");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickFullReverseResolverFlagsConflictsAndDanglingIndexes();

async function adClickFullReverseKvForeverRetentionLeavesReverseIndexesUnexpired() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      EDEN_AD_CLICK_KV_INDEX_MODE: "full",
      EDEN_AD_CLICK_REVERSE_KV_RETENTION_MODE: "forever",
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "fixture",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const health = await (await worker.fetch(new Request("https://collect.eden.health/eden-health-check"), env, makeCtx())).json();
    assert.equal(health.ad_click_reverse_kv_retention_mode, "forever");
    assert.equal(health.ad_click_reverse_kv_expiration_ttl_seconds, null);
    const req = new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "X-Eden-Server-Secret": "test_server_secret",
        "Cookie": "eden_anon_id=anon-forever; eden_session_id=session-forever_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "OS_purchase",
        anonymousId: "anon-forever",
        userId: "user-forever",
        properties: { order_id: "order-forever", transaction_id: "charge-forever" },
        context: { page: { url: "https://app.eden.health/intake?gclid=gclid-forever-full&utm_source=google&utm_medium=cpc", path: "/intake" } },
      }),
    });
    const ctx = makeCtx();
    const res = await worker.fetch(req, env, ctx);
    await res.json();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    const pointerCalls = adClickKv.putCalls.filter((call) => call.key.startsWith("adclick:id:"));
    const reverseCalls = adClickKv.putCalls.filter((call) => /^adclick:v2:(anon|session|user|order):/.test(call.key));
    assert.ok(pointerCalls.some((call) => call.options.expirationTtl === 7776000), "pointer id record keeps a bounded browser-pointer TTL");
    assert.ok(reverseCalls.length >= 4, "full mode should write v2 first-party reverse indexes");
    assert.equal(reverseCalls.every((call) => !Object.prototype.hasOwnProperty.call(call.options, "expirationTtl")), true, "forever reverse retention omits expirationTtl from reverse KV writes");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickFullReverseKvForeverRetentionLeavesReverseIndexesUnexpired();

async function adClickMemoryKvIndexOffDisablesPointerCookie() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      EDEN_AD_CLICK_KV_INDEX_MODE: "off",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const ctx = makeCtx();
    const req = new Request("https://www.eden.health/?gclid=gclid-kv-off&utm_source=google&utm_medium=cpc", {
      headers: { "User-Agent": "Mozilla/5.0" },
    });
    const res = await worker.fetch(req, env, ctx);
    await res.text();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(queue.messages.length, 1, "KV index off should still allow queue proof in cookie mode");
    assert.equal([...adClickKv.map.keys()].filter((key) => key.startsWith("adclick:")).length, 0);
    assert.equal(getSetCookie(res.headers).some((cookie) => cookie.startsWith("__Secure-eden_ad_click_id=")), false, "KV index off must not set pointer cookie");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickMemoryKvIndexOffDisablesPointerCookie();

async function browserPurchaseSignalCannotEmitStableIdentityLinks() {
  const adClickKv = new MockKV();
  const queue = new MockQueue();
  await adClickKv.put("adclick:id:adclk2_existing_review", JSON.stringify({
    schema_version: "eden_ad_click_pointer_v2",
    ad_click_id: "adclk2_existing_review",
    snapshot_id: "adsnap_existing_review",
    primary_click_id_type: "gclid",
    has_class_a: true,
    has_primary_click_evidence: true,
    owner_anonymous_id_sha256: await sha256Raw("anon-link"),
    owner_session_id_sha256: await sha256Raw("session-link_1780000000000"),
    ad_click_id_scope: "first_party_scoped",
    ownership_scope: "first_party_owner_bound",
  }));
  const env = {
    EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
    EDEN_AD_CLICK_MEMORY_MODE: "shadow",
    AD_CLICK_KV: adClickKv,
    AD_CLICK_SNAPSHOT_QUEUE: queue,
  };
  const makeReq = () => new Request("https://collect.eden.health/collect", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Origin": "https://app.eden.health",
      "Cookie": "eden_anon_id=anon-link; eden_session_id=session-link_1780000000000; __Secure-eden_ad_click_id=adclk2_existing_review",
    },
    body: JSON.stringify({
      type: "track",
      event: "OS_purchase",
      anonymousId: "anon-link",
      userId: "user-link",
      properties: { order_id: "order-link" },
      context: { page: { url: "https://app.eden.health/intake", path: "/intake" } },
    }),
  });
  const ctx = makeCtx();
  const res = await worker.fetch(makeReq(), env, ctx);
  const body = await res.json();
  await Promise.all(ctx.promises);
  assert.equal(res.status, 200);
  assert.equal(body.ok, true);
  assert.equal([...adClickKv.map.keys()].filter((key) => key.startsWith("adclick:")).length, 1, "shadow mode must not add AD_CLICK_KV records beyond the existing owner-bound pointer");
  assert.equal(getSetCookie(res.headers).some((cookie) => cookie.startsWith("__Secure-eden_ad_click_id=")), false, "shadow mode must not set pointer cookie");
  assert.ok(queue.messages.length >= 1, "browser purchase signal may emit provisional anonymous/session/ad-click continuity");
  assert.ok(queue.messages.every((message) => (message.payload.identity_links || []).every((link) => ["anonymous_id", "session_id", "ad_click_id"].includes(link.from_type) && ["anonymous_id", "session_id", "ad_click_id"].includes(link.to_type))), "browser purchase cannot emit stable person/order/payment links");
  const ctx2 = makeCtx();
  const res2 = await worker.fetch(makeReq(), env, ctx2);
  await res2.json();
  await Promise.all(ctx2.promises);
  assert.equal(res2.status, 200);
  assert.ok(queue.messages.every((message) =>
    (message.payload.identity_links || []).every((link) =>
      ["anonymous_id", "session_id", "ad_click_id"].includes(link.from_type)
      && ["anonymous_id", "session_id", "ad_click_id"].includes(link.to_type)
    )
  ));
  assert.equal([...adClickKv.map.keys()].filter((key) => key.startsWith("adclick:")).length, 1, "shadow mode must still not add AD_CLICK_KV records after repeated link");
}

await browserPurchaseSignalCannotEmitStableIdentityLinks();

function makeSharedClickReq(anon, user, order, pageUrl) {
  return new Request("https://collect.eden.health/server-collect", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Origin": "https://app.eden.health",
      "X-Eden-Server-Secret": TEST_SERVER_API_SECRET,
      "Cookie": `eden_anon_id=${anon}; eden_session_id=session-${anon}_1780000000000`,
    },
    body: JSON.stringify({
      type: "track",
      event: "OS_purchase",
      anonymousId: anon,
      userId: user,
      properties: { order_id: order, transaction_id: `charge-${order}` },
      context: { page: { url: pageUrl, path: "/intake" } },
    }),
  });
}

async function adClickMemorySharedGclidIsOwnerScopedWithReplayHash() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "shadow",
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const pageUrl = "https://app.eden.health/intake?gclid=shared-click-id&utm_source=google&utm_medium=cpc";
    const ctx1 = makeCtx();
    const res1 = await worker.fetch(makeSharedClickReq("anon-shared-a", "user-shared-a", "order-shared-a", pageUrl), env, ctx1);
    await res1.json();
    await Promise.all(ctx1.promises);
    const ctx2 = makeCtx();
    const res2 = await worker.fetch(makeSharedClickReq("anon-shared-b", "user-shared-b", "order-shared-b", pageUrl), env, ctx2);
    await res2.json();
    await Promise.all(ctx2.promises);
    assert.equal(res1.status, 200);
    assert.equal(res2.status, 200);
    const snapshots = queue.messages.map((message) => message.payload).filter((payload) => payload.event_type === "ad_click_snapshot");
    assert.equal(snapshots.length, 2);
    assert.notEqual(snapshots[0].ad_click_id, snapshots[1].ad_click_id, "a shared GCLID must never collapse two Eden owners into one ad-click object");
    assert.equal(snapshots[0].snapshot.governance.ad_click_id_scope, "first_party_scoped");
    assert.equal(snapshots[1].snapshot.governance.ad_click_id_scope, "first_party_scoped");
    assert.equal(snapshots[0].snapshot.google.gclid_sha256, snapshots[1].snapshot.google.gclid_sha256, "global replay QA remains possible through the raw GCLID hash");
    const userHashes = new Set(snapshots.flatMap((payload) => payload.identity_links.filter((link) => link.from_type === "user_id_sha256").map((link) => link.from_id)));
    assert.equal(userHashes.size, 2, "each owner-scoped ad-click object keeps its own user identity link");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickMemorySharedGclidIsOwnerScopedWithReplayHash();

async function adClickIdentityRelationshipKeyIsScopedToAdClickObject() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "shadow",
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const owner = ["anon-two-clicks", "user-two-clicks", "order-two-clicks"];
    const ctx1 = makeCtx();
    await (await worker.fetch(makeSharedClickReq(...owner, "https://app.eden.health/intake?gclid=first-owner-click&utm_source=google&utm_medium=cpc"), env, ctx1)).json();
    await Promise.all(ctx1.promises);
    const ctx2 = makeCtx();
    await (await worker.fetch(makeSharedClickReq(owner[0], owner[1], "order-two-clicks-next", "https://app.eden.health/intake?gclid=second-owner-click&utm_source=google&utm_medium=cpc"), env, ctx2)).json();
    await Promise.all(ctx2.promises);

    const snapshots = queue.messages.map((message) => message.payload).filter((payload) => payload.event_type === "ad_click_snapshot");
    assert.equal(snapshots.length, 2);
    assert.notEqual(snapshots[0].ad_click_id, snapshots[1].ad_click_id, "two paid clicks for one Eden owner remain distinct click objects");
    const identityEdges = snapshots.map((payload) => payload.identity_links.find((link) => link.from_type === "anonymous_id" && link.to_type === "user_id_sha256"));
    assert.ok(identityEdges.every(Boolean));
    assert.equal(identityEdges[0].from_id, identityEdges[1].from_id);
    assert.equal(identityEdges[0].to_id, identityEdges[1].to_id);
    assert.equal(identityEdges[0].schema_version, "eden_ad_identity_link_v2");
    assert.equal(identityEdges[1].schema_version, "eden_ad_identity_link_v2");
    assert.notEqual(identityEdges[0].link_id, identityEdges[1].link_id, "the same typed identity edge under a later ad click must receive a different relationship key");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickIdentityRelationshipKeyIsScopedToAdClickObject();

async function adClickMemorySharedGbraidMustNotCollapseAcrossUsers() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "shadow",
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    // gbraid/wbraid are device/campaign-level Google identifiers that legitimately repeat
    // across different people. The July 5-7 2026 regression collapsed them into one global
    // ad_click_id; this test locks the corrected lesson in.
    const pageUrl = "https://app.eden.health/intake?gbraid=shared-gbraid-value&utm_source=google&utm_medium=cpc";
    const ctx1 = makeCtx();
    await (await worker.fetch(makeSharedClickReq("anon-gbraid-a", "user-gbraid-a", "order-gbraid-a", pageUrl), env, ctx1)).json();
    await Promise.all(ctx1.promises);
    const ctx2 = makeCtx();
    await (await worker.fetch(makeSharedClickReq("anon-gbraid-b", "user-gbraid-b", "order-gbraid-b", pageUrl), env, ctx2)).json();
    await Promise.all(ctx2.promises);
    const snapshots = queue.messages.map((message) => message.payload).filter((payload) => payload.event_type === "ad_click_snapshot");
    assert.equal(snapshots.length, 2);
    assert.notEqual(snapshots[0].ad_click_id, snapshots[1].ad_click_id, "same gbraid across two users must NOT create one global Eden ad-click identity");
    assert.equal(snapshots[0].snapshot.governance.ad_click_id_scope, "first_party_scoped");
    assert.equal(snapshots[1].snapshot.governance.ad_click_id_scope, "first_party_scoped");
    assert.equal(snapshots[0].snapshot.google.gbraid_sha256, snapshots[1].snapshot.google.gbraid_sha256, "raw click comparability survives via snapshot hashes");
    assert.equal(snapshots[0].snapshot.evidence.raw_primary_click_id_sha256, snapshots[1].snapshot.evidence.raw_primary_click_id_sha256);

    // Same user/browser re-landing on the same gbraid keeps a stable scoped identity.
    const ctx3 = makeCtx();
    await (await worker.fetch(makeSharedClickReq("anon-gbraid-a", "user-gbraid-a", "order-gbraid-a2", pageUrl), env, ctx3)).json();
    await Promise.all(ctx3.promises);
    const repeatSnapshots = queue.messages.map((message) => message.payload).filter((payload) => payload.event_type === "ad_click_snapshot");
    assert.equal(repeatSnapshots.at(-1).ad_click_id, snapshots[0].ad_click_id, "same first-party context + same gbraid stays one stable Eden ad-click identity");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await adClickMemorySharedGbraidMustNotCollapseAcrossUsers();

async function rawAdIdBridgesAreFullyRetired() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const gclidKv = new MockKV();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      GCLID_KV: gclidKv,
    };
    const pageUrl = "https://app.eden.health/intake?gclid=bridge-gclid&gbraid=bridge-gbraid&wbraid=bridge-wbraid&dclid=bridge-dclid&srsltid=bridge-srsltid&utm_source=google&utm_medium=cpc";
    const ctx = makeCtx();
    await (await worker.fetch(makeSharedClickReq("anon-bridge-writer", "user-bridge-writer", "order-bridge-writer", pageUrl), env, ctx)).json();
    await Promise.all(ctx.promises);
    const rawBridgeKeys = [...gclidKv.map.keys()].filter((key) => key.startsWith("attr:click:") || key.startsWith("attr:gcl:"));
    assert.deepEqual(rawBridgeKeys, [], "gclid, braids, _gcl_au, and every other raw ad ID must never be written as a recovery key");
    assert.ok(gclidKv.map.has("attr:server:v1:user:source:user_id:user-bridge-writer"), "trusted server user continuity must retain the evidence value");
    assert.ok(gclidKv.map.has("attr:server:v1:order:order-bridge-writer"), "trusted server order continuity must retain the evidence value");

    // Plant a legacy cross-user gbraid bridge record (pre-fix key shape). A different
    // user re-landing with the same gbraid must not recover that stored attribution.
    await gclidKv.put("attr:click:gbraid:legacy-shared-gbraid", JSON.stringify({
      gclid: "victim-user-gclid",
      gbraid: "legacy-shared-gbraid",
      utm_campaign: "victim-campaign",
      stored_at: "2026-07-05T00:00:00.000Z",
    }));
    await gclidKv.put("attr:click:gclid:legacy-shared-gclid", JSON.stringify({
      gclid: "legacy-shared-gclid",
      utm_campaign: "victim-gclid-campaign",
      fbclid: "victim-fbclid",
      stored_at: "2026-07-05T00:00:00.000Z",
    }));
    await gclidKv.put("attr:gcl:111.222", JSON.stringify({
      gclid: "victim-gcl-au-gclid",
      _gcl_au: "1.1.111.222",
      utm_campaign: "victim-gcl-au-campaign",
      stored_at: "2026-07-05T00:00:00.000Z",
    }));
    const segmentCalls = [];
    globalThis.fetch = async (url, init = {}) => {
      if (String(url).startsWith("https://api.segment.io/")) {
        segmentCalls.push(JSON.parse(init.body));
        return new Response("{}", { status: 200 });
      }
      return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
    };
    const attackerEnv = { ...env, SEGMENT_WRITE_KEY: "test_write_key" };
    const attackerCtx = makeCtx();
    await (await worker.fetch(makeSharedClickReq("anon-bridge-second-user", "user-bridge-second-user", "order-bridge-second-user", "https://app.eden.health/intake?gbraid=legacy-shared-gbraid&utm_source=google&utm_medium=cpc"), attackerEnv, attackerCtx)).json();
    await Promise.all(attackerCtx.promises);
    const trackCalls = segmentCalls.filter((call) => call.event === "OS_purchase");
    assert.equal(trackCalls.length, 1);
    const forwarded = trackCalls[0];
    assert.equal(forwarded.properties.gclid, undefined, "another user's stored gclid must not be recovered through a shared gbraid");
    assert.equal(forwarded.context.campaign.gclid, undefined, "context.campaign must not carry cross-user bridge recovery");
    assert.notEqual(forwarded.context.campaign.utm_campaign, "victim-campaign");
    assert.equal(forwarded.properties.first_touch_gclid, undefined, "touch model must not inherit another user's click id");
    const legacyRawBridgeReads = gclidKv.getKeys.filter((key) => key.startsWith("attr:click:") || key.startsWith("attr:gcl:"));
    assert.deepEqual(legacyRawBridgeReads, [], "historical raw click/_gcl_au bridge records must never be read");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await rawAdIdBridgesAreFullyRetired();

async function srsltidAloneIsDiagnosticOnlyAndGclAuAloneMintsNoAdClickId() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const gclidKv = new MockKV();
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      GCLID_KV: gclidKv,
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const srsltidReq = new Request("https://www.eden.health/?srsltid=SR-ONLY&utm_source=google", {
      headers: { "User-Agent": "Mozilla/5.0" },
    });
    const ctx = makeCtx();
    const res = await worker.fetch(srsltidReq, env, ctx);
    await res.text();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal([...gclidKv.map.keys()].some((key) => key.startsWith("attr:click:")), false, "srsltid alone must not create a click bridge key");
    assert.equal(queue.messages.filter((message) => message.payload.event_type === "ad_click_snapshot").length, 0, "srsltid alone must not mint an ad-click snapshot");
    assert.equal(getSetCookie(res.headers).some((cookie) => cookie.startsWith("__Secure-eden_ad_click_id=")), false, "srsltid alone must not set an ad-click pointer");

    const gclAuReq = new Request("https://www.eden.health/?_gcl_au=1.1.111.222", {
      headers: { "User-Agent": "Mozilla/5.0" },
    });
    const gclAuCtx = makeCtx();
    const gclAuRes = await worker.fetch(gclAuReq, env, gclAuCtx);
    await gclAuRes.text();
    await Promise.all(gclAuCtx.promises);
    assert.equal(gclAuRes.status, 200);
    assert.equal(queue.messages.filter((message) => message.payload.event_type === "ad_click_snapshot").length, 0, "_gcl_au alone is diagnostic only and must not mint an ad-click identity");
    assert.equal([...adClickKv.map.keys()].some((key) => /^adclick:(v2:)?gcl_au:/.test(key)), false, "_gcl_au must never become an adclick reverse lookup key");
    assert.equal(getSetCookie(gclAuRes.headers).some((cookie) => cookie.startsWith("__Secure-eden_ad_click_id=")), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await srsltidAloneIsDiagnosticOnlyAndGclAuAloneMintsNoAdClickId();

async function contextCampaignStaysEventNativeWithProvenanceLabeledRecovery() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const gclidKv = new MockKV();
    // Same-user KV continuity: this browser's earlier click stored under its anon key.
    await gclidKv.put("attr:anon:anon-provenance", JSON.stringify({
      gclid: "stored-own-gclid",
      utm_source: "google",
      utm_medium: "cpc",
      utm_campaign: "stored-campaign",
      stored_at: "2026-07-01T00:00:00.000Z",
    }));
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      SEGMENT_WRITE_KEY: "test_write_key",
      GCLID_KV: gclidKv,
    };
    const req = new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": "eden_anon_id=anon-provenance; eden_session_id=session-provenance_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "os_question_answered",
        anonymousId: "anon-provenance",
        properties: { page_url: "https://app.eden.health/intake" },
        context: { page: { url: "https://app.eden.health/intake", path: "/intake" } },
      }),
    });
    const ctx = makeCtx();
    const res = await worker.fetch(req, env, ctx);
    await res.json();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(segmentCalls.length, 1);
    const forwarded = segmentCalls[0];
    // Current event campaign means currently observed campaign: no URL click evidence on
    // this event, so recovered values must not be stamped as event-native.
    assert.equal(forwarded.context.campaign.gclid, undefined, "recovered stored gclid must not appear event-native in context.campaign");
    assert.equal(forwarded.properties.gclid, undefined, "recovered stored gclid must not appear event-native in properties");
    assert.equal(forwarded.context.recovered_campaign.gclid, "stored-own-gclid", "recovery surfaces in the provenance-labeled recovered_campaign block");
    assert.ok(forwarded.context.attribution_provenance.recovered_keys.includes("gclid"));
    assert.equal(forwarded.context.attribution_provenance.recovered_source, "gclid_kv_stored_attribution");
    assert.ok(forwarded.properties.attribution_recovered_keys.includes("gclid"));
    // Continuity survives in explicitly labeled surfaces.
    assert.equal(forwarded.properties.first_touch_gclid, "stored-own-gclid");
    assert.equal(forwarded.properties.first_touch_source_type, "stored_attribution");
    assert.equal(forwarded.properties.first_touch_from_memory, true);
    assert.equal(forwarded.properties.acquisition_channel, "paid_search", "derived channel roll-up keeps memory continuity");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await contextCampaignStaysEventNativeWithProvenanceLabeledRecovery();


async function serverCollectStoredAttributionSurvivalIsProvenanceLabeled() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const gclidKv = new MockKV();
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    await gclidKv.put("attr:server:v1:user:source:user_id:user-server-prov", JSON.stringify({
      gclid: "stored-server-gclid",
      utm_source: "google",
      utm_medium: "cpc",
      utm_campaign: "stored-server-campaign",
      stored_at: "2026-07-01T00:00:00.000Z",
    }));
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      SEGMENT_WRITE_KEY: "test_write_key",
      SERVER_API_SECRET: "test_server_secret",
      GCLID_KV: gclidKv,
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const req = new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Origin": "https://app.eden.health", "X-Eden-Server-Secret": "test_server_secret" },
      body: JSON.stringify({
        type: "track",
        event: "OS_purchase",
        userId: "user-server-prov",
        anonymousId: "anon-server-prov",
        properties: { order_id: "order-server-prov", transaction_id: "charge-server-prov", page_url: "https://app.eden.health/intake" },
        context: { page: { url: "https://app.eden.health/intake", path: "/intake" } },
      }),
    });
    const ctx = makeCtx();
    const res = await worker.fetch(req, env, ctx);
    await res.json();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    const forwarded = segmentCalls.find((call) => call.event === "OS_purchase");
    assert.ok(forwarded, "expected forwarded server OS_purchase");
    // Survival contract: stored first-party-continuity click IDs stay in properties for
    // dbt direct-path uploads, but they are provenance-labeled and NOT event-native.
    assert.equal(forwarded.properties.gclid, "stored-server-gclid", "server purchase properties must keep stored click IDs (attribution survival)");
    assert.ok(forwarded.properties.attribution_recovered_keys.includes("gclid"), "recovered stored keys must be labeled");
    assert.equal(forwarded.properties.attribution_recovery_source, "gclid_kv_stored_attribution");
    assert.equal(forwarded.context.campaign.gclid, undefined, "server context.campaign must stay event-native");
    assert.equal(forwarded.context.recovered_campaign.gclid, "stored-server-gclid");
    assert.ok(forwarded.context.attribution_provenance.recovered_keys.includes("gclid"));
    assert.equal(forwarded.properties.first_touch_gclid, "stored-server-gclid");
    const snapshots = queue.messages.map((message) => message.payload).filter((payload) => payload.event_type === "ad_click_snapshot");
    assert.equal(snapshots.length, 0, "memory-recovered click evidence must not claim to be fresh or mint a new observation snapshot");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await serverCollectStoredAttributionSurvivalIsProvenanceLabeled();

async function serverCollectBodyBraidIsEventNativeButLegacyBridgeStaysDead() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const gclidKv = new MockKV();
    // Legacy pre-fix cross-user bridge record: must never be recovered again.
    await gclidKv.put("attr:click:gbraid:BODY-SHARED-GBRAID", JSON.stringify({
      gclid: "victim-server-gclid",
      gbraid: "BODY-SHARED-GBRAID",
      utm_campaign: "victim-server-campaign",
      stored_at: "2026-07-06T00:00:00.000Z",
    }));
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      SERVER_API_SECRET: "test_server_secret",
      SEGMENT_WRITE_KEY: "test_write_key",
      GCLID_KV: gclidKv,
    };
    const req = new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Origin": "https://app.eden.health", "X-Eden-Server-Secret": "test_server_secret" },
      body: JSON.stringify({
        type: "track",
        event: "OS_purchase",
        userId: "user-body-braid",
        anonymousId: "anon-body-braid",
        properties: { order_id: "order-body-braid", transaction_id: "charge-body-braid", ecommerce: { gbraid: "BODY-SHARED-GBRAID" } },
        context: { page: { url: "https://app.eden.health/intake", path: "/intake" } },
      }),
    });
    const ctx = makeCtx();
    const res = await worker.fetch(req, env, ctx);
    await res.json();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    const forwarded = segmentCalls.find((call) => call.event === "OS_purchase");
    assert.ok(forwarded);
    // Body-carried braids are event-native capture and must keep flowing...
    assert.equal(forwarded.properties.gbraid, "BODY-SHARED-GBRAID", "body-carried gbraid is event-native capture and must survive");
    assert.equal(forwarded.properties.current_touch_gbraid, "BODY-SHARED-GBRAID", "current-touch paid signal must survive the bridge retirement");
    assert.equal(forwarded.properties.current_touch_channel, "paid_search");
    // ...but the same value must never act as a KV lookup key into another user's blob.
    assert.equal(forwarded.properties.gclid, undefined, "a shared gbraid must not recover another user's stored gclid");
    assert.notEqual(forwarded.properties.utm_campaign, "victim-server-campaign");
    assert.notEqual(forwarded.context.campaign.utm_campaign, "victim-server-campaign");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await serverCollectBodyBraidIsEventNativeButLegacyBridgeStaysDead();

async function browserIdentifyCannotPromoteStoredAttributionToClaimedUser() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push({ url: String(url), body: JSON.parse(init.body) });
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const gclidKv = new MockKV();
    await gclidKv.put("attr:anon:anon-identify-prov", JSON.stringify({
      gclid: "stored-identify-gclid",
      utm_source: "google",
      utm_medium: "cpc",
      stored_at: "2026-07-01T00:00:00.000Z",
    }));
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      SEGMENT_WRITE_KEY: "test_write_key",
      GCLID_KV: gclidKv,
    };
    const req = new Request("https://collect.eden.health/identify", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": "eden_anon_id=anon-identify-prov; eden_session_id=session-identify-prov_1780000000000",
      },
      body: JSON.stringify({
        userId: "user-identify-prov",
        anonymousId: "anon-identify-prov",
        traits: {},
        context: { page: { url: "https://app.eden.health/account", path: "/account" } },
      }),
    });
    const ctx = makeCtx();
    const res = await worker.fetch(req, env, ctx);
    const responseBody = await res.json();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(responseBody.stable_identity_accepted, false);
    assert.equal(segmentCalls.length, 0, "browser identify must not forward or alias an unverified stable identity");
    assert.equal(await gclidKv.get("id:link:user-identify-prov"), null);
    assert.equal(await gclidKv.get("attr:user:user-identify-prov"), null);
    assert.ok(await gclidKv.get("attr:anon:anon-identify-prov"), "existing anonymous first touch remains unchanged");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await browserIdentifyCannotPromoteStoredAttributionToClaimedUser();

async function onlyV1ReverseKeysNeverResolveEvenWithFullResolverAccepted() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const anonHash = await crypto.subtle.digest("SHA-256", new TextEncoder().encode("anon-v1-only")).then((buf) => Array.from(new Uint8Array(buf)).map((b) => b.toString(16).padStart(2, "0")).join(""));
    const orderHash = await crypto.subtle.digest("SHA-256", new TextEncoder().encode("order-v1-only")).then((buf) => Array.from(new Uint8Array(buf)).map((b) => b.toString(16).padStart(2, "0")).join(""));
    // Production reality after the regression window: ONLY v1 reverse keys exist.
    await adClickKv.put("adclick:id:adclk_v1_only", JSON.stringify({ ad_click_id: "adclk_v1_only", has_class_a: true }));
    await adClickKv.put(`adclick:order:${orderHash}`, "adclk_v1_only");
    await adClickKv.put(`adclick:anon:${anonHash}:last_paid`, "adclk_v1_only");
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      EDEN_AD_CLICK_KV_INDEX_MODE: "full",
      EDEN_AD_CLICK_KV_RESOLVER_MODE: "full",
      EDEN_AD_CLICK_KV_RESOLVER_CONTRACT_ACCEPTED: "true",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const req = new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": "eden_anon_id=anon-v1-only; eden_session_id=session-v1-only_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "os_question_answered",
        anonymousId: "anon-v1-only",
        userId: "user-v1-only",
        properties: { order_id: "order-v1-only" },
        context: { page: { url: "https://app.eden.health/intake", path: "/intake" } },
      }),
    });
    const ctx = makeCtx();
    const res = await worker.fetch(req, env, ctx);
    await res.json();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(queue.messages.length, 0, "a KV containing only quarantined v1 reverse keys must resolve nothing");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await onlyV1ReverseKeysNeverResolveEvenWithFullResolverAccepted();

async function legacyV1PointerCookieIsQuarantined() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      SEGMENT_WRITE_KEY: "test_write_key",
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    // Regression-window browsers carry v1 pointer cookies whose ids may be a collapsed
    // cross-user gbraid identity. They must never resolve or link again.
    const req = new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": "eden_anon_id=anon-v1-cookie; eden_session_id=session-v1-cookie_1780000000000; __Secure-eden_ad_click_id=adclk_july_regression_window_id",
      },
      body: JSON.stringify({
        type: "track",
        event: "os_question_answered",
        anonymousId: "anon-v1-cookie",
        userId: "user-v1-cookie",
        properties: { order_id: "order-v1-cookie" },
        context: { page: { url: "https://app.eden.health/intake", path: "/intake" } },
      }),
    });
    const ctx = makeCtx();
    const res = await worker.fetch(req, env, ctx);
    await res.json();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    assert.equal(queue.messages.length, 0, "quarantined v1 pointer cookies must not produce link envelopes");
    const forwarded = segmentCalls.find((call) => call.event === "os_question_answered");
    assert.ok(forwarded);
    assert.equal(forwarded.properties.ad_click_id, undefined, "quarantined v1 pointer must not annotate events");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await legacyV1PointerCookieIsQuarantined();

async function cookieModeRollbackNeverAnnotatesGbraidEvents() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      SEGMENT_WRITE_KEY: "test_write_key",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const req = new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": "eden_anon_id=anon-rollback-annotation; eden_session_id=session-rollback-annotation_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "os_question_answered",
        anonymousId: "anon-rollback-annotation",
        properties: { page_url: "https://app.eden.health/intake?gbraid=rollback-gbraid&utm_source=google&utm_medium=cpc" },
        context: { page: { url: "https://app.eden.health/intake?gbraid=rollback-gbraid&utm_source=google&utm_medium=cpc", path: "/intake" } },
      }),
    });
    const ctx = makeCtx();
    const res = await worker.fetch(req, env, ctx);
    await res.json();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    const forwarded = segmentCalls.find((call) => call.event === "os_question_answered");
    assert.ok(forwarded);
    // The post-rollback invariant: gbraid events carry no ad_click_id unless payload
    // annotation (mode all/production) is explicitly re-enabled.
    assert.equal(forwarded.properties.ad_click_id, undefined, "cookie-mode rollback posture must not stamp ad_click_id onto gbraid events");
    assert.equal(forwarded.properties.gbraid, "rollback-gbraid", "event-native gbraid still flows");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await cookieModeRollbackNeverAnnotatesGbraidEvents();

async function preserveAttributionUsesOwnerContinuityWithoutRawBridges() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const gclidKv = new MockKV();
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const attrCookie = encodeURIComponent(JSON.stringify({
      gclid: "preserve-own-gclid",
      gbraid: "preserve-own-gbraid",
      utm_source: "google",
      utm_medium: "cpc",
      _ts: Date.now(),
    }));
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      GCLID_KV: gclidKv,
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const req = new Request("https://collect.eden.health/preserve-attribution", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": `eden_anon_id=anon-preserve; eden_session_id=session-preserve_1780000000000; eden_attr=${attrCookie}`,
      },
      body: JSON.stringify({ userId: "user-preserve", orderId: "order-preserve" }),
    });
    const ctx = makeCtx();
    const res = await worker.fetch(req, env, ctx);
    await res.json();
    await Promise.all(ctx.promises);
    assert.equal(res.status, 200);
    const rawBridgeKeys = [...gclidKv.map.keys()].filter((key) => key.startsWith("attr:click:") || key.startsWith("attr:gcl:"));
    assert.deepEqual(rawBridgeKeys, [], "preserve endpoint must not write retired bridge keys, including gclid and _gcl_au");
    assert.equal([...gclidKv.map.keys()].includes("attr:user:user-preserve"), false, "browser preserve cannot claim user continuity");
    assert.equal([...gclidKv.map.keys()].includes("attr:order:order-preserve"), false, "browser preserve cannot claim order continuity");
    const preAuthCookie = getSetCookie(res.headers).find((cookie) => cookie.startsWith("eden_pre_auth="));
    assert.ok(preAuthCookie, "same-device pre-auth cookie handoff stays");
    assert.ok(decodeURIComponent(preAuthCookie).includes("preserve-own-gbraid"), "same-device pre-auth cookie may keep gbraid (cannot cross users)");
    const snapshots = queue.messages.map((message) => message.payload).filter((payload) => payload.event_type === "ad_click_snapshot");
    assert.equal(snapshots.length, 0, "cookie-recovered preserve calls must not create a new observation snapshot");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await preserveAttributionUsesOwnerContinuityWithoutRawBridges();

async function mutationEndpointsFailClosedAndBoundJsonBodies() {
  const originalFetch = globalThis.fetch;
  let originFetches = 0;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) return new Response("{}", { status: 200 });
    originFetches += 1;
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const serverBody = JSON.stringify({ type: "track", event: "OS_purchase", userId: "auth-user", properties: { order_id: "auth-order", transaction_id: "auth-charge" } });
    const makeServer = (headers = {}) => new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...headers },
      body: serverBody,
    });
    assert.equal((await productionWorker.fetch(makeServer(), {}, makeCtx())).status, 503, "missing server secret configuration must fail closed");
    assert.equal((await worker.fetch(makeServer(), { SERVER_API_SECRET: "expected" }, makeCtx())).status, 401, "missing server credential must be unauthorized");
    assert.equal((await worker.fetch(makeServer({ "X-Eden-Server-Secret": "wrong" }), { SERVER_API_SECRET: "expected" }, makeCtx())).status, 401, "wrong server credential must be unauthorized");
    assert.equal((await worker.fetch(makeServer({ "X-Eden-Server-Secret": "expected" }), { SERVER_API_SECRET: "expected" }, makeCtx())).status, 200, "correct server credential must pass");

    for (const path of ["/server-collect", "/collect", "/identify", "/preserve-attribution"]) {
      for (const attackerHint of ["bot", "synthetic"]) {
        const url = attackerHint === "synthetic"
          ? `https://collect.eden.health${path}?eden_checkly_marker=auth-bypass-probe`
          : `https://collect.eden.health${path}`;
        const headers = { "Content-Type": "application/json" };
        if (attackerHint === "bot") headers["User-Agent"] = "Googlebot/2.1";
        const beforeOriginFetches = originFetches;
        const response = await worker.fetch(new Request(url, {
          method: "POST",
          headers,
          body: path === "/server-collect" ? serverBody : "{}",
        }), { SERVER_API_SECRET: "expected" }, makeCtx());
        assert.equal(response.status, 401, `${path} must not let a ${attackerHint} hint bypass mutation authentication`);
        assert.equal(originFetches, beforeOriginFetches, `${path} ${attackerHint} probe must not passthrough to origin`);
      }
    }

    const originlessCollect = (secret) => new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...(secret ? { "X-Eden-Server-Secret": secret } : {}) },
      body: JSON.stringify({ type: "track", event: "originless_collect", properties: {} }),
    });
    assert.equal((await worker.fetch(originlessCollect(), { SERVER_API_SECRET: "expected" }, makeCtx())).status, 401, "originless collect requires server authentication");
    assert.equal((await worker.fetch(originlessCollect("expected"), { SERVER_API_SECRET: "expected" }, makeCtx())).status, 409, "server producers must use /server-collect; browser /collect requires owner cookies");

    const originlessIdentify = new Request("https://collect.eden.health/identify", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ userId: "originless-identify", anonymousId: "originless-anon" }),
    });
    assert.equal((await worker.fetch(originlessIdentify, { SERVER_API_SECRET: "expected" }, makeCtx())).status, 401, "originless identify requires server authentication");
    const originlessPreserve = new Request("https://collect.eden.health/preserve-attribution", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    assert.equal((await worker.fetch(originlessPreserve, { SERVER_API_SECRET: "expected" }, makeCtx())).status, 401, "originless preserve requires server authentication");

    const oversizedBody = JSON.stringify({ type: "track", event: "oversized", properties: { value: "😀".repeat(20_000) } });
    const oversized = new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Origin": "https://app.eden.health" },
      body: oversizedBody,
    });
    assert.equal((await worker.fetch(oversized, {}, makeCtx())).status, 413, "body cap must be byte-based, including multibyte input");
    const wrongType = new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: { "Content-Type": "text/plain", "Origin": "https://app.eden.health" },
      body: "{}",
    });
    assert.equal((await worker.fetch(wrongType, {}, makeCtx())).status, 415, "collector must reject non-JSON content types");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await mutationEndpointsFailClosedAndBoundJsonBodies();

async function signedBrowserCapabilityEnforcementContract() {
  const originalFetch = globalThis.fetch;
  const originalHtmlRewriter = globalThis.HTMLRewriter;
  let originFetches = 0;
  let injectedClientScript = "";
  globalThis.fetch = async (url) => {
    if (String(url).startsWith("https://api.segment.io/")) return new Response("{}", { status: 200 });
    originFetches += 1;
    return new Response("<html><head></head><body>ok</body></html>", {
      status: 200,
      headers: { "content-type": "text/html" },
    });
  };
  globalThis.HTMLRewriter = class {
    on(selector, handler) { this.selector = selector; this.handler = handler; return this; }
    transform(response) {
      assert.equal(this.selector, "head");
      this.handler.element({ prepend(value) { injectedClientScript += String(value); } });
      return response;
    }
  };
  try {
    const privacyKv = new MockKV();
    const baseEnv = {
      EDEN_BROWSER_CAP_ENFORCEMENT_MODE: "enforce",
      BROWSER_CAP_HMAC_SECRET: "browser-cap-old",
      PRIVACY_LEDGER_HMAC_SECRET: TEST_PRIVACY_LEDGER_HMAC_SECRET,
      PRIVACY_LEDGER_KV: privacyKv,
    };
    const bootstrap = await productionWorker.fetch(
      new Request("https://app.eden.health/intake", { headers: { "User-Agent": "Mozilla/5.0" } }),
      baseEnv,
      makeCtx(),
    );
    await bootstrap.text();
    const fullCapabilityCookie = getSetCookie(bootstrap.headers).find((cookie) => cookie.startsWith("__Secure-eden_browser_cap="));
    assert.ok(fullCapabilityCookie, "an injected Eden HTML page must mint a signed browser capability");
    assert.match(fullCapabilityCookie, /Domain=\.eden\.health/i, "temporary cross-subdomain collector compatibility must be explicit");
    assert.match(fullCapabilityCookie, /HttpOnly/i);
    assert.match(fullCapabilityCookie, /Secure/i);
    assert.match(fullCapabilityCookie, /SameSite=Strict/i);
    assert.equal(/denied|gclid|anonymous|user/i.test(fullCapabilityCookie.split(";", 1)[0]), false, "capability payload must contain no attribution or identity values");
    for (const marker of [
      "getCookie('eden_anonymous_id')",
      "window.analytics.setAnonymousId(id)",
      "postJSON('/preserve-attribution'",
      "fetch('/browser-capability'",
      "resp.status === 401 || resp.status === 409",
      "credentials: 'include'",
      "window.addEventListener('pagehide'",
      "pageUrl:     pageUrl",
      "function readPreserveResponse(resp)",
      "var _activePreservePromise = null",
      "var _activePreserveFingerprint = null",
      "_activePreserveFingerprint === fingerprint",
      "function beginPreserveRequest(resolvedOrderId, pageUrl, fingerprint, handoffDestination)",
      "function activatePreservePromise(trackedPromise, fingerprint)",
      "var previousPromise = _activePreservePromise",
      "return activatePreservePromise(serializedPromise, fingerprint)",
      "function onHealthOsIntakeClick(event)",
      "function emitHealthOsHandoffArrival()",
      "event: 'HealthOS Handoff Arrived'",
      "edge_event_source: 'healthos_handoff_browser'",
      "event.metaKey || event.ctrlKey || event.shiftKey || event.altKey",
      "window.location.hostname !== 'www.eden.health'",
      "anchor.hasAttribute('download')",
      "destination.origin !== 'https://app.eden.health'",
      "destination.pathname !== '/intake' && !destination.pathname.startsWith('/intake/')",
      "HEALTHOS_INTAKE_HANDOFF_TIMEOUT_MS = 5000",
      "var pendingPreserve = _activePreservePromise",
      "var destinationHasAttribution = pageUrlHasAttribution(destination.toString())",
      "var preservePageUrl = browserLocationHasAttribution(true)",
      "? String(window.location.href || '')",
      ": destinationHasAttribution ? destination.toString() : null",
      "preserveAttribution(null, preservePageUrl, destination.toString())",
      "destination.searchParams.set('eden_attr_handoff', outcome.handoffAssertion)",
      "Promise.race([durablePromise, timeoutPromise])",
      "window.__edenHealthOsHandoffLastOutcome",
      "window.addEventListener('click', onHealthOsIntakeClick, true)",
      "window.addEventListener('hashchange'",
      "window.addEventListener('popstate'",
      "['pushState', 'replaceState']",
      "var outgoingPageUrl = String(window.location.href || '')",
      "preserveAttribution(null, outgoingPageUrl)",
      "pageUrlAtMutation = new URL(String(arguments[2]), window.location.href).toString()",
      "preserveAttribution(null, pageUrlAtMutation)",
      "setTimeout(ensureSegmentContinuity, 1500)",
    ]) {
      assert.ok(injectedClientScript.includes(marker), `the Webflow/app client bootstrap must retain ${marker}`);
    }
    assert.equal(injectedClientScript.includes("postJSON('/identify'"), false, "the edge bootstrap must never promote browser-claimed stable identity");
    assert.equal(injectedClientScript.includes("hookSegmentIdentify"), false);
    const executableClientSource = injectedClientScript
      .replace(/^\s*<script>/i, "")
      .replace(/<\/script>\s*$/i, "");
    assert.doesNotThrow(() => new Function(executableClientSource), "the injected Webflow handoff client must remain valid JavaScript");
    const preserveFunctionSource = injectedClientScript.slice(
      injectedClientScript.indexOf("function preserveAttribution(orderId, pageUrlOverride, handoffDestinationOverride)"),
      injectedClientScript.indexOf("function browserLocationHasAttribution(includeSearch)"),
    );
    assert.ok(
      preserveFunctionSource.indexOf("_activePreserveFingerprint === fingerprint") >= 0
        && preserveFunctionSource.indexOf("_activePreserveFingerprint === fingerprint")
          < preserveFunctionSource.indexOf("var previousPromise = _activePreservePromise"),
      "same-fingerprint concurrent preserve calls must reuse the active barrier before serializing a changed observation",
    );
    assert.ok(preserveFunctionSource.includes("return activatePreservePromise("), "the initiating preserve call must return its active Promise through the shared barrier tracker");
    const handoffFunctionSource = injectedClientScript.slice(
      injectedClientScript.indexOf("function onHealthOsIntakeClick(event)"),
      injectedClientScript.indexOf("window.addEventListener('click', onHealthOsIntakeClick, true)"),
    );
    const preventDefaultIndex = handoffFunctionSource.indexOf("event.preventDefault()");
    for (const guard of [
      "event.metaKey || event.ctrlKey || event.shiftKey || event.altKey",
      "window.location.hostname !== 'www.eden.health'",
      "anchor.hasAttribute('download')",
      "target !== '_self'",
      "destination.origin !== 'https://app.eden.health'",
      "destination.pathname !== '/intake'",
      "browserLocationHasAttribution(true)",
      "pageUrlHasAttribution(destination.toString())",
    ]) {
      assert.ok(
        handoffFunctionSource.indexOf(guard) >= 0 && handoffFunctionSource.indexOf(guard) < preventDefaultIndex,
        `the Webflow handoff must evaluate ${guard} before preventing navigation`,
      );
    }
    assert.equal(/tryeden\.com|edenrx\.co/i.test(handoffFunctionSource), false, "the CTA gate must not claim or intercept legacy-domain continuity");

    const preserveBootstrapSource = injectedClientScript.slice(
      injectedClientScript.indexOf("function readPreserveResponse(resp)"),
      injectedClientScript.indexOf("function browserLocationHasAttribution(includeSearch)"),
    );
    const fakePreserveResponse = (status, body) => ({
      ok: status >= 200 && status < 300,
      status,
      clone() {
        return { json: () => Promise.resolve(body) };
      },
    });
    const durablePreserveResponse = () => fakePreserveResponse(200, {
      ok: true,
      ad_click_observation_persisted: true,
      queue_enqueued: true,
      pointer_kv_persisted: true,
      owner_attribution_kv_persisted: true,
    });
    let resolvePreserveRequest;
    const pendingPreserveRequest = new Promise((resolve) => { resolvePreserveRequest = resolve; });
    let preserveRequestCount = 0;
    const preserveWindow = { location: { href: "https://www.eden.health/" } };
    const preserveConcurrencyHarness = new Function(
      "window",
      "getOrderIdFromDOM",
      "resolveIds",
      "postJSON",
      "Promise",
      "Date",
      "setTimeout",
      "clearTimeout",
      `${preserveBootstrapSource}\nreturn { preserveAttribution: preserveAttribution, active: function() { return _activePreservePromise; } };`,
    )(
      preserveWindow,
      () => null,
      () => ({ anonId: "owner", userId: null }),
      () => { preserveRequestCount += 1; return pendingPreserveRequest; },
      Promise,
      Date,
      () => 1,
      () => {},
    );
    const preserveForConcurrencyTest = preserveConcurrencyHarness.preserveAttribution;
    const firstPreservePromise = preserveForConcurrencyTest(null);
    const concurrentPreservePromise = preserveForConcurrencyTest(null);
    assert.equal(concurrentPreservePromise, firstPreservePromise, "concurrent preserve calls must return the same active Promise");
    assert.equal(preserveRequestCount, 1, "concurrent preserve calls must issue one request");
    preserveWindow.location.href = "https://www.eden.health/#gclid=NEW-FRAGMENT";
    const changedFingerprintPromise = preserveForConcurrencyTest(null);
    assert.notEqual(changedFingerprintPromise, firstPreservePromise, "a changed browser fingerprint must serialize a new preserve after the active request");
    assert.equal(preserveConcurrencyHarness.active(), changedFingerprintPromise, "the serialized changed fingerprint must replace the older request as the active HealthOS handoff barrier");
    assert.equal(preserveRequestCount, 1, "the changed fingerprint must wait for the active request before issuing another");
    resolvePreserveRequest(durablePreserveResponse());
    assert.equal((await firstPreservePromise).durable, true, "a fully persisted preserve response must resolve durable");
    assert.equal((await changedFingerprintPromise).durable, true, "the serialized changed fingerprint must resolve durable");
    assert.equal(preserveRequestCount, 2, "the changed fingerprint must issue exactly one follow-up preserve request");

    const historyBootstrapStart = injectedClientScript.indexOf("try {\n    ['pushState', 'replaceState']");
    const historyBootstrapEnd = injectedClientScript.indexOf("\n  // Keep long-lived app tabs admitted", historyBootstrapStart);
    const historyBootstrapSource = injectedClientScript.slice(historyBootstrapStart, historyBootstrapEnd);
    const historySequence = [];
    const historyWindow = {
      location: { href: "https://www.eden.health/?gclid=QUERY-BEFORE-CLEANUP" },
      history: {
        pushState(_state, _title, nextUrl) {
          historySequence.push("mutation");
          historyWindow.location.href = new URL(String(nextUrl), historyWindow.location.href).toString();
        },
        replaceState(_state, _title, nextUrl) {
          historySequence.push("mutation");
          historyWindow.location.href = new URL(String(nextUrl), historyWindow.location.href).toString();
        },
      },
    };
    new Function(
      "window",
      "pageUrlHasAttribution",
      "preserveAttribution",
      "preserveBrowserOnlyAttribution",
      "setTimeout",
      "URL",
      historyBootstrapSource,
    )(
      historyWindow,
      (value) => String(value).includes("gclid="),
      (_orderId, pageUrl) => { historySequence.push(`preserve:${pageUrl}`); return Promise.resolve(true); },
      () => {},
      (callback) => { callback(); return 1; },
      URL,
    );
    historyWindow.history.replaceState({}, "", "/");
    assert.deepEqual(
      historySequence,
      ["preserve:https://www.eden.health/?gclid=QUERY-BEFORE-CLEANUP", "mutation"],
      "an attributed landing URL must start preserving synchronously before Webflow/VWO removes its query",
    );
    assert.equal(historyWindow.location.href, "https://www.eden.health/", "the fixture must actually clean the visible query before the CTA phase");

    for (const [label, response] of [
      ["500 response", fakePreserveResponse(500, { ok: false })],
      ["401 response", fakePreserveResponse(401, { ok: false })],
      ["200 skipped response", fakePreserveResponse(200, { ok: true, skipped: "no_attribution" })],
      ["200 partial durability", fakePreserveResponse(200, {
        ok: true,
        ad_click_observation_persisted: false,
        queue_enqueued: true,
        pointer_kv_persisted: true,
        owner_attribution_kv_persisted: true,
      })],
    ]) {
      const preserveForFailureTest = new Function(
        "window",
        "getOrderIdFromDOM",
        "resolveIds",
        "postJSON",
        "Promise",
        "Date",
        "setTimeout",
        "clearTimeout",
        `${preserveBootstrapSource}\nreturn preserveAttribution;`,
      )(
        { location: { href: "https://www.eden.health/#gclid=FAILURE-TEST" } },
        () => null,
        () => ({ anonId: "owner", userId: null }),
        () => Promise.resolve(response),
        Promise,
        Date,
        () => 1,
        () => {},
      );
      assert.equal((await preserveForFailureTest(null)).durable, false, `${label} must not satisfy the durable handoff barrier`);
    }

    const handoffListenerMarker = "window.addEventListener('click', onHealthOsIntakeClick, true)";
    const handoffBootstrapStart = injectedClientScript.indexOf("var HEALTHOS_INTAKE_HANDOFF_TIMEOUT_MS = 5000");
    const handoffBootstrapEnd = injectedClientScript.indexOf(handoffListenerMarker) + handoffListenerMarker.length;
    const handoffBootstrapSource = injectedClientScript.slice(handoffBootstrapStart, handoffBootstrapEnd);
    const createHandoffHarness = ({
      attributionPresent = true,
      continuityCookiePresent = attributionPresent,
      button = 0,
      currentHost = "www.eden.health",
      destination = "https://app.eden.health/intake",
      download = false,
      metaKey = false,
      pendingPreserve = false,
      target = "",
    } = {}) => {
      const assignments = [];
      const timeoutCallbacks = [];
      let clickListener = null;
      let preserveCalls = 0;
      const preserveArguments = [];
      let resolvePreserve;
      const preservePromise = new Promise((resolve) => { resolvePreserve = resolve; });
      const documentStub = {
        addEventListener(type, listener) { if (type === "click") clickListener = listener; },
      };
      const windowStub = {
        addEventListener(type, listener) { if (type === "click") clickListener = listener; },
        location: {
          hostname: currentHost,
          href: `https://${currentHost}/`,
          assign(value) { assignments.push(value); },
        },
      };
      const anchor = {
        tagName: "A",
        href: destination,
        parentElement: documentStub,
        getAttribute(name) {
          if (name === "href") return destination;
          if (name === "target") return target;
          return null;
        },
        hasAttribute(name) { return name === "download" && download; },
      };
      const event = {
        altKey: false,
        button,
        ctrlKey: false,
        defaultPrevented: false,
        metaKey,
        shiftKey: false,
        target: anchor,
        preventDefault() { this.defaultPrevented = true; },
      };
      new Function(
        "window",
        "document",
        "browserLocationHasAttribution",
        "pageUrlHasAttribution",
        "getCookie",
        "preserveAttribution",
        "_activePreservePromise",
        "URL",
        "Promise",
        "setTimeout",
        "clearTimeout",
        handoffBootstrapSource,
      )(
        windowStub,
        documentStub,
        () => attributionPresent,
        (value) => /[?&#](?:gclid|gbraid|wbraid)=/i.test(String(value)),
        (name) => name === "eden_attr" && continuityCookiePresent ? "fixture-attribution" : null,
        (...args) => { preserveCalls += 1; preserveArguments.push(args); return preservePromise; },
        pendingPreserve ? preservePromise : null,
        URL,
        Promise,
        (callback, delay) => {
          if (delay === 50) { callback(); return 0; }
          timeoutCallbacks.push(callback);
          return timeoutCallbacks.length;
        },
        (timerId) => { timeoutCallbacks[Number(timerId) - 1] = null; },
      );
      return {
        assignments,
        event,
        fire() { clickListener(event); },
        fireTimeout() { timeoutCallbacks.shift()?.(); },
        preserveCalls: () => preserveCalls,
        preserveArguments,
        resolvePreserve,
      };
    };

    for (const [label, options] of [
      ["modifier click", { metaKey: true }],
      ["middle click", { button: 1 }],
      ["new-tab target", { target: "_blank" }],
      ["download", { download: true }],
      ["no governed attribution", { attributionPresent: false }],
      ["non-Webflow current host", { currentHost: "app.eden.health" }],
      ["non-intake app path", { destination: "https://app.eden.health/account" }],
      ["non-default app port", { destination: "https://app.eden.health:444/intake" }],
    ]) {
      const harness = createHandoffHarness(options);
      harness.fire();
      assert.equal(harness.event.defaultPrevented, false, `${label} must retain native browser behavior`);
      assert.equal(harness.preserveCalls(), 0, `${label} must not schedule preserve`);
      assert.deepEqual(harness.assignments, [], `${label} must not schedule same-tab navigation`);
    }

    const gatedHandoff = createHandoffHarness();
    gatedHandoff.fire();
    assert.equal(gatedHandoff.event.defaultPrevented, true, "allowlisted attributed Webflow CTA must be gated");
    assert.equal(gatedHandoff.preserveCalls(), 1, "allowlisted attributed Webflow CTA must preserve exactly once");
    assert.deepEqual(gatedHandoff.assignments, [], "HealthOS navigation must not begin while preserve is pending");
    gatedHandoff.resolvePreserve({ durable: true, handoffAssertion: "h1.fixture.signature" });
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(new URL(gatedHandoff.assignments[0]).searchParams.get("eden_attr_handoff"), "h1.fixture.signature", "HealthOS navigation may begin only with the destination-bound assertion");

    const currentFragmentWinsTransportHandoff = createHandoffHarness({
      attributionPresent: true,
      destination: "https://app.eden.health/intake?gclid=OLDER-TRANSPORT-GCLID&utm_source=google",
    });
    currentFragmentWinsTransportHandoff.fire();
    assert.equal(currentFragmentWinsTransportHandoff.preserveCalls(), 1);
    assert.equal(
      currentFragmentWinsTransportHandoff.preserveArguments[0][1],
      "https://www.eden.health/",
      "current browser evidence must select the pointer when the destination also carries older paid transport",
    );
    assert.equal(
      currentFragmentWinsTransportHandoff.preserveArguments[0][2],
      "https://app.eden.health/intake?gclid=OLDER-TRANSPORT-GCLID&utm_source=google",
      "the complete destination must remain separately bound into the signed handoff assertion",
    );
    currentFragmentWinsTransportHandoff.resolvePreserve({ durable: true, handoffAssertion: "h1.fixture.signature" });
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(new URL(currentFragmentWinsTransportHandoff.assignments[0]).searchParams.get("gclid"), "OLDER-TRANSPORT-GCLID");

    const cleanedUrlContinuityHandoff = createHandoffHarness({ attributionPresent: false, continuityCookiePresent: true });
    cleanedUrlContinuityHandoff.fire();
    assert.equal(cleanedUrlContinuityHandoff.event.defaultPrevented, true, "a cleaned URL with first-party attribution continuity must still request a signed handoff");
    assert.equal(cleanedUrlContinuityHandoff.preserveCalls(), 1);
    cleanedUrlContinuityHandoff.resolvePreserve({ durable: true, handoffAssertion: "h1.fixture.signature" });
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(new URL(cleanedUrlContinuityHandoff.assignments[0]).searchParams.get("eden_attr_handoff"), "h1.fixture.signature");

    const cleanedUrlPendingHandoff = createHandoffHarness({ attributionPresent: false, pendingPreserve: true });
    cleanedUrlPendingHandoff.fire();
    assert.equal(cleanedUrlPendingHandoff.event.defaultPrevented, true, "a cleaned URL must remain gated while its fragment/SPA preserve is pending");
    assert.equal(cleanedUrlPendingHandoff.preserveCalls(), 1, "the cleaned-URL handoff must serialize a destination-bound assertion after the active preserve");
    assert.deepEqual(cleanedUrlPendingHandoff.assignments, [], "cleaned-URL handoff must wait for the active durable preserve");
    cleanedUrlPendingHandoff.resolvePreserve({ durable: true, handoffAssertion: "h1.fixture.signature" });
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(new URL(cleanedUrlPendingHandoff.assignments[0]).searchParams.get("eden_attr_handoff"), "h1.fixture.signature", "cleaned-URL handoff may proceed only with destination binding");

    const destinationOnlyHandoff = createHandoffHarness({
      attributionPresent: false,
      continuityCookiePresent: false,
      destination: "https://app.eden.health/intake?gclid=DESTINATION-ONLY-GCLID&utm_source=google",
    });
    destinationOnlyHandoff.fire();
    assert.equal(destinationOnlyHandoff.event.defaultPrevented, true, "a CTA carrying its own paid evidence must be preserved before navigation");
    assert.equal(destinationOnlyHandoff.preserveCalls(), 1);
    assert.equal(destinationOnlyHandoff.preserveArguments[0][1], "https://app.eden.health/intake?gclid=DESTINATION-ONLY-GCLID&utm_source=google", "destination-only paid evidence must be the preserve page URL");
    destinationOnlyHandoff.resolvePreserve({ durable: true, handoffAssertion: "h1.fixture.signature" });
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(new URL(destinationOnlyHandoff.assignments[0]).searchParams.get("gclid"), "DESTINATION-ONLY-GCLID");
    assert.equal(new URL(destinationOnlyHandoff.assignments[0]).searchParams.get("eden_attr_handoff"), "h1.fixture.signature");

    const failedHandoff = createHandoffHarness();
    failedHandoff.fire();
    failedHandoff.resolvePreserve({ durable: false, handoffAssertion: null });
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(failedHandoff.assignments, ["https://app.eden.health/intake"], "a definitive non-durable result must fail open immediately rather than adding an unnecessary delay");
    failedHandoff.fireTimeout();
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(failedHandoff.assignments, ["https://app.eden.health/intake"], "the cleared timeout must not navigate a second time");

    const timedHandoff = createHandoffHarness();
    timedHandoff.fire();
    timedHandoff.fireTimeout();
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(timedHandoff.assignments, ["https://app.eden.health/intake"], "bounded timeout must eventually release a stalled handoff");
    const bootstrapCookiePairs = getSetCookie(bootstrap.headers).map((cookie) => cookie.split(";", 1)[0]);
    const capabilityCookiePair = bootstrapCookiePairs.find((cookie) => cookie.startsWith("__Secure-eden_browser_cap="));
    const ownerCookiePairs = bootstrapCookiePairs.filter((cookie) => [
      "eden_anonymous_id=",
      "eden_anon_id=",
      "eden_session_id=",
    ].some((prefix) => cookie.startsWith(prefix))).join("; ");
    const capabilityPair = `${capabilityCookiePair}; ${ownerCookiePairs}`;
    assert.ok(capabilityPair.includes("eden_anonymous_id="));
    assert.ok(capabilityPair.includes("eden_session_id="));
    const ownerAnonId = decodeURIComponent(ownerCookiePairs.split("; ").find((cookie) => cookie.startsWith("eden_anonymous_id="))?.split("=", 2)[1] || "");
    const ownerSessionId = decodeURIComponent(ownerCookiePairs.split("; ").find((cookie) => cookie.startsWith("eden_session_id="))?.split("=", 2)[1] || "");
    const browserBootstrapHeaders = {
      Cookie: ownerCookiePairs,
      Referer: "https://app.eden.health/intake",
      "Sec-Fetch-Site": "same-origin",
      "Sec-Fetch-Mode": "cors",
      "Sec-Fetch-Dest": "empty",
    };

    const bootstrapEndpoint = await productionWorker.fetch(new Request("https://app.eden.health/browser-capability", {
      headers: browserBootstrapHeaders,
    }), baseEnv, makeCtx());
    assert.equal(bootstrapEndpoint.status, 204, "same-origin bootstrap must recover a browser capability without reloading the SPA");
    const bootstrapCapability = getSetCookie(bootstrapEndpoint.headers).find((cookie) => cookie.startsWith("__Secure-eden_browser_cap="));
    assert.ok(bootstrapCapability);
    const crossSiteBootstrap = await productionWorker.fetch(new Request("https://app.eden.health/browser-capability", {
      headers: {
        Origin: "https://evil.example",
        Referer: "https://evil.example/",
        "Sec-Fetch-Site": "cross-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
      },
    }), baseEnv, makeCtx());
    assert.equal(crossSiteBootstrap.status, 403);
    assert.equal(getSetCookie(crossSiteBootstrap.headers).some((cookie) => cookie.startsWith("__Secure-eden_browser_cap=")), false);
    assert.equal(
      (await productionWorker.fetch(new Request("https://app.eden.health/browser-capability", { headers: { Cookie: ownerCookiePairs } }), baseEnv, makeCtx())).status,
      403,
      "headerless non-browser bootstrap must not mint a capability",
    );
    const oversizedOwnerHeaders = { ...browserBootstrapHeaders, Cookie: `eden_anonymous_id=${"a".repeat(400)}; eden_session_id=${ownerSessionId}` };
    assert.equal(
      (await productionWorker.fetch(new Request("https://app.eden.health/browser-capability", { headers: oversizedOwnerHeaders }), baseEnv, makeCtx())).status,
      409,
      "oversized owner cookies must not mint a capability",
    );
    assert.equal(
      (await productionWorker.fetch(new Request("https://app.eden.health/browser-capability", { headers: browserBootstrapHeaders }), { ...baseEnv, BROWSER_CAP_HMAC_SECRET: "" }, makeCtx())).status,
      503,
      "bootstrap must fail closed when the signing key is unavailable",
    );

    const makeBrowserCollect = (cookie = capabilityPair, extraHeaders = {}, body = { type: "track", event: "capability_probe", properties: {} }) => new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        ...(cookie ? { Cookie: cookie } : {}),
        ...extraHeaders,
      },
      body: JSON.stringify(body),
    });

    const authorizedCollect = await productionWorker.fetch(makeBrowserCollect(), baseEnv, makeCtx());
    assert.equal(authorizedCollect.status, 200, "valid signed capability must authorize the cross-subdomain browser collector");
    assert.ok(
      getSetCookie(authorizedCollect.headers).some((cookie) => cookie.startsWith("__Secure-eden_browser_cap=")),
      "a successful browser mutation must refresh the short-lived capability for long-lived app sessions",
    );
    assert.ok(
      getSetCookie(authorizedCollect.headers).some((cookie) => cookie.startsWith("eden_session_id=")),
      "a successful active mutation must slide the 30-minute session cookie",
    );
    const missing = await productionWorker.fetch(makeBrowserCollect(""), baseEnv, makeCtx());
    assert.equal(missing.status, 200, "a fresh allowlisted browser with no owner state may perform one collector bootstrap");
    assert.equal(missing.headers.get("cache-control"), "no-store");
    const freshBootstrapCookies = getSetCookie(missing.headers);
    assert.ok(freshBootstrapCookies.some((cookie) => cookie.startsWith("__Secure-eden_browser_cap=")), "fresh collection must mint a signed capability");
    assert.ok(freshBootstrapCookies.some((cookie) => cookie.startsWith("eden_anonymous_id=")), "fresh collection must mint a Worker-owned anonymous ID");
    assert.ok(freshBootstrapCookies.some((cookie) => cookie.startsWith("eden_session_id=")), "fresh collection must mint a Worker-owned session");

    const shadowAdmission = await productionWorker.fetch(
      makeBrowserCollect(ownerCookiePairs),
      { ...baseEnv, EDEN_BROWSER_CAP_ENFORCEMENT_MODE: "shadow" },
      makeCtx(),
    );
    assert.equal(shadowAdmission.status, 200, "shadow mode must preserve an existing capability-less browser mutation");
    assert.ok(
      getSetCookie(shadowAdmission.headers).some((cookie) => cookie.startsWith("__Secure-eden_browser_cap=")),
      "the first capability-less shadow mutation must self-migrate the active browser session",
    );
    const tamperedCapabilityPair = `${capabilityCookiePair.slice(0, -1)}${capabilityCookiePair.endsWith("A") ? "B" : "A"}`;
    const tamperedPair = `${tamperedCapabilityPair}; ${ownerCookiePairs}`;
    assert.equal((await productionWorker.fetch(makeBrowserCollect(tamperedPair), baseEnv, makeCtx())).status, 401, "tampered capability must fail");
    const replayedForOtherOwner = `${capabilityCookiePair}; eden_anonymous_id=other-owner; eden_session_id=other-session_1780000000000`;
    assert.equal((await productionWorker.fetch(makeBrowserCollect(replayedForOtherOwner), baseEnv, makeCtx())).status, 401, "capability replay under a different owner must fail");
    assert.equal((await productionWorker.fetch(makeBrowserCollect(capabilityPair, { Origin: "https://www.eden.health" }), baseEnv, makeCtx())).status, 401, "capability replay from a different browser host must fail");
    assert.equal((await productionWorker.fetch(makeBrowserCollect(capabilityPair, { "Sec-Fetch-Site": "cross-site" }), baseEnv, makeCtx())).status, 403, "cross-site Fetch Metadata must fail even with a valid capability");
    assert.equal((await productionWorker.fetch(makeBrowserCollect("", { "X-Eden-Server-Secret": "server-secret" }), { ...baseEnv, SERVER_API_SECRET: "server-secret" }, makeCtx())).status, 401, "server secret must not authorize browser endpoints in enforce mode");

    const nowSeconds = Math.floor(Date.now() / 1000);
    const expiredToken = await signedBrowserCapabilityFixture("browser-cap-old", {
      iat: nowSeconds - 8_000,
      exp: nowSeconds - 100,
      anonId: ownerAnonId,
      sessionId: ownerSessionId,
    });
    assert.equal(
      (await productionWorker.fetch(makeBrowserCollect(`__Secure-eden_browser_cap=${expiredToken}; ${ownerCookiePairs}`), baseEnv, makeCtx())).status,
      401,
      "an expired capability must fail",
    );
    const recoveredBootstrap = await productionWorker.fetch(new Request("https://app.eden.health/browser-capability", {
      headers: browserBootstrapHeaders,
    }), baseEnv, makeCtx());
    const recoveredCapabilityPair = getSetCookie(recoveredBootstrap.headers).find((cookie) => cookie.startsWith("__Secure-eden_browser_cap="))?.split(";", 1)[0];
    assert.ok(recoveredCapabilityPair, "an expired long-lived tab must be able to bootstrap a replacement capability");
    assert.equal((await productionWorker.fetch(makeBrowserCollect(`${recoveredCapabilityPair}; ${ownerCookiePairs}`), baseEnv, makeCtx())).status, 200, "the replacement capability must restore collection without a page reload");

    const anonymousOnlyCookies = ownerCookiePairs.split("; ").filter((cookie) => !cookie.startsWith("eden_session_id=")).join("; ");
    const expiredSessionBootstrap = await productionWorker.fetch(new Request("https://app.eden.health/browser-capability", {
      headers: {
        Cookie: anonymousOnlyCookies,
        Referer: "https://app.eden.health/intake",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
      },
    }), baseEnv, makeCtx());
    assert.equal(expiredSessionBootstrap.status, 204, "same-site bootstrap must recover a missing/expired anonymous-session binding without page reload");
    const recoveredLongTabCookies = getSetCookie(expiredSessionBootstrap.headers).map((cookie) => cookie.split(";", 1)[0]);
    const recoveredLongTabCapability = recoveredLongTabCookies.find((cookie) => cookie.startsWith("__Secure-eden_browser_cap="));
    const recoveredLongTabSession = recoveredLongTabCookies.find((cookie) => cookie.startsWith("eden_session_id="));
    assert.ok(recoveredLongTabCapability, "bootstrap mints a new capability bound to the existing anonymous owner");
    assert.ok(recoveredLongTabSession, "bootstrap mints only a new Worker-owned anonymous session");

    const collectorFirstBootstrap = await productionWorker.fetch(new Request("https://app.eden.health/browser-capability", {
      headers: {
        Referer: "https://app.eden.health/intake",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
      },
    }), baseEnv, makeCtx());
    assert.equal(collectorFirstBootstrap.status, 204, "a genuine first-party collector-first browser may bootstrap Worker-owned anonymous/session state");
    const collectorFirstCookies = getSetCookie(collectorFirstBootstrap.headers);
    assert.ok(collectorFirstCookies.some((cookie) => cookie.startsWith("eden_anonymous_id=")));
    assert.ok(collectorFirstCookies.some((cookie) => cookie.startsWith("eden_anon_id=")));
    assert.ok(collectorFirstCookies.some((cookie) => cookie.startsWith("eden_session_id=")));
    assert.ok(collectorFirstCookies.some((cookie) => cookie.startsWith("__Secure-eden_browser_cap=")));

    const rotatedEnv = {
      ...baseEnv,
      BROWSER_CAP_HMAC_SECRET: "browser-cap-new",
      BROWSER_CAP_HMAC_SECRET_PREVIOUS: "browser-cap-old",
    };
    assert.equal((await productionWorker.fetch(makeBrowserCollect(), rotatedEnv, makeCtx())).status, 200, "previous browser key must support bounded rotation");

    assert.equal((await productionWorker.fetch(makeBrowserCollect(), { ...baseEnv, BROWSER_CAP_HMAC_SECRET: "" }, makeCtx())).status, 503, "missing browser signing secret must fail closed");
    const capOnServer = new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", Cookie: capabilityPair },
      body: JSON.stringify({ type: "track", event: "OS_purchase", properties: { order_id: "cap-server" } }),
    });
    assert.equal((await productionWorker.fetch(capOnServer, { ...baseEnv, SERVER_API_SECRET: "server-secret" }, makeCtx())).status, 401, "browser capability must never authorize server-collect");

    const oversizedBody = { type: "track", event: "oversized", properties: { value: "😀".repeat(20_000) } };
    assert.equal((await productionWorker.fetch(makeBrowserCollect(capabilityPair, {}, oversizedBody), baseEnv, makeCtx())).status, 413, "body cap still applies after capability authentication");
    const wrongType = new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: { "Content-Type": "text/plain", Origin: "https://app.eden.health", Cookie: capabilityPair },
      body: "{}",
    });
    assert.equal((await productionWorker.fetch(wrongType, baseEnv, makeCtx())).status, 415);

    const beforePrefixProbe = originFetches;
    assert.equal((await productionWorker.fetch(new Request("https://collect.eden.health/collection", {
      method: "POST",
      headers: { "Content-Type": "application/json", Origin: "https://app.eden.health", Cookie: capabilityPair },
      body: "{}",
    }), baseEnv, makeCtx())).status, 200);
    assert.equal(originFetches, beforePrefixProbe + 1, "only exact /collect may enter the mutation collector");
    const serverPreflight = await productionWorker.fetch(new Request("https://collect.eden.health/server-collect", {
      method: "OPTIONS",
      headers: { Origin: "https://app.eden.health" },
    }), baseEnv, makeCtx());
    assert.equal(serverPreflight.status, 403, "server-collect must not expose browser preflight");
    assert.equal(serverPreflight.headers.get("access-control-allow-origin"), null);
  } finally {
    globalThis.fetch = originalFetch;
    if (originalHtmlRewriter === undefined) delete globalThis.HTMLRewriter;
    else globalThis.HTMLRewriter = originalHtmlRewriter;
  }
}

await signedBrowserCapabilityEnforcementContract();

async function missingPrivacyLedgerBindingsPreserveTracking() {
  const originalFetch = globalThis.fetch;
  const originalHtmlRewriter = globalThis.HTMLRewriter;
  const segmentCalls = [];
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("<html><head></head><body>ok</body></html>", { status: 200, headers: { "content-type": "text/html" } });
  };
  globalThis.HTMLRewriter = class {
    on() { return this; }
    transform(response) { return response; }
  };
  try {
    const cases = [
      {
        name: "missing_hmac_secret",
        env: { PRIVACY_LEDGER_KV: new MockKV() },
      },
      {
        name: "missing_privacy_kv",
        env: { PRIVACY_LEDGER_HMAC_SECRET: TEST_PRIVACY_LEDGER_HMAC_SECRET },
      },
    ];
    for (const testCase of cases) {
      const env = {
        EDEN_BROWSER_CAP_ENFORCEMENT_MODE: "enforce",
        BROWSER_CAP_HMAC_SECRET: TEST_BROWSER_CAP_HMAC_SECRET,
        SEGMENT_WRITE_KEY: "test_write_key",
        ...testCase.env,
      };
      const bootstrap = await productionWorker.fetch(new Request("https://app.eden.health/intake"), env, makeCtx());
      await bootstrap.text();
      const browserCookies = getSetCookie(bootstrap.headers)
        .map((cookie) => cookie.split(";", 1)[0])
        .filter((cookie) => ["__Secure-eden_browser_cap=", "eden_anonymous_id=", "eden_anon_id=", "eden_session_id="].some((prefix) => cookie.startsWith(prefix)))
        .join("; ");
      assert.ok(browserCookies.includes("__Secure-eden_browser_cap="), `${testCase.name} must not prevent the public page from minting browser admission security`);
      const ctx = makeCtx();
      const response = await productionWorker.fetch(new Request("https://collect.eden.health/collect", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Origin: "https://app.eden.health",
          Cookie: browserCookies,
        },
        body: JSON.stringify({
          type: "track",
          event: `privacy_unavailable_tracking_continues_${testCase.name}`,
          anonymousId: `anon-${testCase.name}`,
          properties: { gclid: "PRIVACY-MISCONFIG-GCLID", page_search: "?gclid=PRIVACY-MISCONFIG-GCLID" },
        }),
      }), env, ctx);
      await response.json();
      await Promise.all(ctx.promises);
      assert.equal(response.status, 200, `${testCase.name} keeps the tracking request available`);
      const forwarded = segmentCalls.at(-1);
      assert.equal(forwarded.properties.attribution_suppressed, false);
      assert.equal(JSON.stringify(forwarded).includes("PRIVACY-MISCONFIG-GCLID"), true, `${testCase.name} must not discard valid Google evidence`);
    }
  } finally {
    globalThis.fetch = originalFetch;
    if (originalHtmlRewriter === undefined) delete globalThis.HTMLRewriter;
    else globalThis.HTMLRewriter = originalHtmlRewriter;
  }
}

await missingPrivacyLedgerBindingsPreserveTracking();

async function nonAuthoritativeConsentSignalsKeepTracking() {
  const originalFetch = globalThis.fetch;
  const segmentCalls = [];
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const cases = [
      {
        name: "unsigned_handoff",
        event: "unsigned_consent_handoff_ignored",
        url: "https://collect.eden.health/collect?eden_consent_handoff=1&eden_consent_ads=denied",
        consentState: null,
        headers: {},
      },
      {
        name: "no_action_false_fields",
        event: "no_action_false_consent_preserves_tracking",
        url: "https://collect.eden.health/collect",
        consentState: {
          source: "default_allowed_no_choice",
          action_taken: false,
          advertising: false,
          google_ads_allowed: false,
          allowed_for_google_click_id_upload: false,
        },
        headers: {},
      },
      {
        name: "gpc_source_only",
        event: "gpc_source_only_preserves_tracking",
        url: "https://collect.eden.health/collect",
        consentState: { source: "gpc", action_taken: false, advertising: false },
        headers: { "Sec-GPC": "1" },
      },
    ];
    for (const [index, testCase] of cases.entries()) {
      const queue = new MockQueue();
      const clickId = `NON-AUTH-CONSENT-GCLID-${index}`;
      const ctx = makeCtx();
      const response = await worker.fetch(new Request(testCase.url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Origin: "https://app.eden.health",
          Cookie: `eden_anonymous_id=non-auth-consent-anon-${index}; eden_session_id=non-auth-consent-session-${index}_1780000000000`,
          ...testCase.headers,
        },
        body: JSON.stringify({
          type: "track",
          event: testCase.event,
          properties: {
            gclid: clickId,
            page_url: `https://app.eden.health/intake?gclid=${clickId}`,
            ...(testCase.consentState ? { consent_state: testCase.consentState } : {}),
          },
          context: {
            page: { url: `https://app.eden.health/intake?gclid=${clickId}` },
            campaign: { gclid: clickId },
          },
        }),
      }), {
        SEGMENT_WRITE_KEY: "test_write_key",
        EDEN_AD_CLICK_MEMORY_MODE: "all",
        AD_CLICK_KV: new MockKV(),
        GCLID_KV: new MockKV(),
        AD_CLICK_SNAPSHOT_QUEUE: queue,
      }, ctx);
      await response.json();
      await Promise.all(ctx.promises);
      assert.equal(response.status, 200, `${testCase.name} remains trackable`);
      assert.ok(queue.messages.length >= 1, `${testCase.name} must preserve edge evidence`);
      assert.equal(segmentCalls.at(-1).properties.attribution_suppressed, false);
      assert.equal(JSON.stringify(segmentCalls.at(-1)).includes(clickId), true, `${testCase.name} must preserve the Google identifier`);
      assert.equal(getSetCookie(response.headers).some((cookie) => cookie.startsWith("__Secure-eden_ads_denied=1")), false, `${testCase.name} cannot mint a denial marker`);
    }
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await nonAuthoritativeConsentSignalsKeepTracking();

async function deniedConsentHandoffAndBodyStateSuppressAndScrub() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const gclidKv = new MockKV();
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      SEGMENT_WRITE_KEY: "test_write_key",
      GCLID_KV: gclidKv,
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    await adClickKv.put("adclick:id:adclk2_denied_pointer", JSON.stringify({
      schema_version: "eden_ad_click_pointer_v2",
      ad_click_id: "adclk2_denied_pointer",
      snapshot_id: "adsnap_denied_pointer",
      captured_at: "2026-07-09T00:00:00.000Z",
      primary_click_id_type: "gclid",
      has_class_a: true,
      has_primary_click_evidence: true,
      owner_anonymous_id_sha256: await sha256Raw("denied-anon"),
      owner_session_id_sha256: await sha256Raw("denied-session_1780000000000"),
      ad_click_id_scope: "first_party_scoped",
      ownership_scope: "first_party_owner_bound",
    }));
    const attrCookie = encodeURIComponent(JSON.stringify({ gclid: "DENIED-GCLID", utm_source: "google", _ts: Date.now() }));
    const deniedRequest = new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": `eden_anonymous_id=denied-anon; eden_anon_id=denied-anon; eden_session_id=denied-session_1780000000000; eden_attr=${attrCookie}; __Secure-eden_ad_click_id=adclk2_denied_pointer; __Secure-eden_internal_handoff=h1.denied.fixture`,
      },
      body: JSON.stringify({
        type: "track",
        event: "denied_handoff_event",
        anonymousId: "denied-anon",
        properties: {
          gclid: "DENIED-GCLID",
          utm_source: "google",
          ad_click_id: "adclk2_caller_supplied",
          consent_state: {
            consent_status: "opted_out",
            source: "cookieyes",
            action_taken: true,
            user_choice: "opted_out",
            ads: "denied",
          },
          page_url: "https://app.eden.health/intake?gclid=DENIED-GCLID&utm_source=google#gbraid=DENIED-BRAID",
          href: "/intake?gclid=DENIED-GCLID&email=denied%40example.com",
          original_url: "https://app.eden.health/intake?gclid=DENIED-GCLID&phone=5550100999",
          page_search: "?gclid=DENIED-GCLID&utm_source=google&email=denied%40example.com",
        },
        context: {
          campaign: { gclid: "DENIED-GCLID", utm_source: "google" },
          page: {
            url: "https://app.eden.health/intake?gclid=DENIED-GCLID&utm_source=google#gbraid=DENIED-BRAID",
            href: "/intake?gclid=DENIED-GCLID&order_id=order-leak",
            path: "/intake?gclid=DENIED-GCLID#gbraid=DENIED-BRAID",
            search: "?gclid=DENIED-GCLID&utm_source=google&phone=5550100999",
          },
        },
      }),
    });
    const deniedCtx = makeCtx();
    const deniedResponse = await worker.fetch(deniedRequest, env, deniedCtx);
    await deniedResponse.json();
    await Promise.all(deniedCtx.promises);
    assert.equal(deniedResponse.status, 200);
    assert.equal(queue.messages.length, 0, "affirmative first-party denial must suppress attribution writes");
    const denialMarkerCookie = getSetCookie(deniedResponse.headers).find((cookie) => cookie.startsWith("__Secure-eden_ads_denied=1"));
    assert.ok(denialMarkerCookie, "explicit denial must immediately set the first-party advertising-denial marker");
    assert.match(denialMarkerCookie, /Max-Age=31536000/);
    assert.match(denialMarkerCookie, /HttpOnly/);
    assert.match(denialMarkerCookie, /Domain=\.eden\.health/);
    assert.equal(denialMarkerCookie.includes("DENIED-GCLID"), false, "denial marker must contain no raw advertising identifier");
    assert.ok(
      getAllSetCookie(deniedResponse.headers).some((cookie) => cookie.startsWith("__Secure-eden_internal_handoff=") && /Max-Age=0/.test(cookie)),
      "explicit denial must clear the signed handoff continuation cookie",
    );
    const denialMarkerPair = denialMarkerCookie.split(";", 1)[0];
    const revokedPointer = JSON.parse(await adClickKv.get("adclick:id:adclk2_denied_pointer"));
    assert.ok(revokedPointer.revoked_at, "explicit denial must durably revoke the owned pointer record");
    assert.equal(revokedPointer.revocation_reason, "explicit_advertising_denial");
    const denialLedgerKeys = [...gclidKv.map.keys()].filter((key) => key.startsWith("privacy:ads_denied:v1:"));
    assert.equal(denialLedgerKeys.length, 2, "denial must be keyed to the hashed anonymous and session identities");
    assert.equal(denialLedgerKeys.some((key) => key.includes("denied-anon") || key.includes("denied-session")), false, "privacy ledger keys must not expose raw first-party IDs");
    const expectedAnonDenialKey = `privacy:ads_denied:v1:anon:${await hmacSha256Hex(TEST_PRIVACY_LEDGER_HMAC_SECRET, "advertising-denial:anon:v1\0denied-anon")}`;
    assert.ok(denialLedgerKeys.includes(expectedAnonDenialKey), "privacy ledger key must use the domain-separated HMAC contract");
    assert.equal(denialLedgerKeys.includes(`privacy:ads_denied:v1:anon:${await sha256Raw("denied-anon")}`), false, "legacy unkeyed SHA denial keys must not be written");
    assert.equal(segmentCalls.length, 1, "behavior event may still reach Segment after advertising fields are scrubbed");
    const forwarded = segmentCalls[0];
    assert.equal(forwarded.properties.gclid, undefined);
    assert.equal(forwarded.properties.utm_source, undefined);
    assert.equal(forwarded.properties.ad_click_id, undefined);
    assert.equal(forwarded.properties.attribution_suppressed, true);
    assert.deepEqual(forwarded.context.campaign, {});
    assert.equal(JSON.stringify(forwarded).includes("DENIED-GCLID"), false, "denied click IDs must not survive anywhere in the forwarded envelope");
    assert.equal(JSON.stringify(forwarded).includes("DENIED-BRAID"), false, "denied fragment click IDs must not survive anywhere in the forwarded envelope");
    assert.equal(JSON.stringify(forwarded).includes("denied@example.com"), false, "sensitive URL/search values must not survive denial scrubbing");
    assert.equal(JSON.stringify(forwarded).includes("5550100999"), false, "sensitive search aliases must be scrubbed");
    assert.equal(forwarded.context.page.path, "/intake", "path aliases must persist pathname only");
    for (const cookieName of ["eden_attr", "__Secure-eden_ad_click_id", "eden_pre_auth"]) {
      const cleared = getAllSetCookie(deniedResponse.headers).find((cookie) => cookie.startsWith(`${cookieName}=`));
      assert.ok(cleared && /Max-Age=0/.test(cleared), `${cookieName} should be explicitly revoked`);
    }

    // Simulate an eventually-consistent or failed durable write: remove the
    // original identity tombstones, then prove the immediate marker alone
    // still suppresses and heals the ledger on the next no-choice request.
    gclidKv.map.delete(expectedAnonDenialKey);
    const expectedSessionDenialKey = `privacy:ads_denied:v1:session:${await hmacSha256Hex(TEST_PRIVACY_LEDGER_HMAC_SECRET, "advertising-denial:session:v1\0denied-session_1780000000000")}`;
    gclidKv.map.delete(expectedSessionDenialKey);

    const defaultAfterDenial = new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": `eden_anonymous_id=denied-anon; eden_anon_id=denied-anon; eden_session_id=denied-session_1780000000000; eden_attr=${attrCookie}; __Secure-eden_ad_click_id=adclk2_denied_pointer; ${denialMarkerPair}`,
      },
      body: JSON.stringify({
        type: "track",
        event: "default_after_explicit_denial",
        anonymousId: "denied-anon",
        properties: { page_url: "https://app.eden.health/intake?gclid=DENIED-GCLID" },
      }),
    });
    const defaultCtx = makeCtx();
    const defaultResponse = await worker.fetch(defaultAfterDenial, env, defaultCtx);
    await defaultResponse.json();
    await Promise.all(defaultCtx.promises);
    assert.equal(queue.messages.length, 0, "a later no-choice request must inherit the durable denial and emit no attribution memory");
    assert.equal(segmentCalls.at(-1).properties.attribution_suppressed, true);
    assert.equal(JSON.stringify(segmentCalls.at(-1)).includes("DENIED-GCLID"), false, "durable denial must prevent old cookie attribution from resurrecting");
    assert.ok(gclidKv.map.has(expectedAnonDenialKey), "marker-denied request should heal the anonymous durable tombstone");
    assert.ok(gclidKv.map.has(expectedSessionDenialKey), "marker-denied request should heal the session durable tombstone");

    const anonymousOnlyCtx = makeCtx();
    const anonymousOnlyResponse = await worker.fetch(new Request("https://collect.eden.health/identify", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": `eden_anonymous_id=denied-anon; eden_session_id=denied-session_1780000000000; eden_attr=${attrCookie}; __Secure-eden_ad_click_id=adclk2_denied_pointer; ${denialMarkerPair}`,
      },
      body: JSON.stringify({ anonymousId: "denied-anon", traits: { plan: "anonymous-only" } }),
    }), env, anonymousOnlyCtx);
    const anonymousOnlyBody = await anonymousOnlyResponse.json();
    await Promise.all(anonymousOnlyCtx.promises);
    assert.equal(anonymousOnlyBody.skipped, "browser_stable_identity_not_authorized");
    for (const cookieName of ["eden_attr", "__Secure-eden_ad_click_id"]) {
      const cleared = getAllSetCookie(anonymousOnlyResponse.headers).find((cookie) => cookie.startsWith(`${cookieName}=`));
      assert.ok(cleared && /Max-Age=0/.test(cleared), `anonymous-only identify must clear stale ${cookieName}`);
    }

    const promotedIdentity = new Request("https://collect.eden.health/identify", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": "eden_anonymous_id=denied-anon; eden_session_id=denied-session_1780000000000",
      },
      body: JSON.stringify({
        anonymousId: "denied-anon",
        userId: "denied-user",
        orderId: "denied-order",
        traits: { href: "/intake?gclid=DENIED-GCLID" },
      }),
    });
    const promotedCtx = makeCtx();
    const promotedResponse = await worker.fetch(promotedIdentity, env, promotedCtx);
    await promotedResponse.json();
    await Promise.all(promotedCtx.promises);
    assert.equal(promotedResponse.status, 200);
    const expectedUserDenialKey = `privacy:ads_denied:v1:user:${await hmacSha256Hex(TEST_PRIVACY_LEDGER_HMAC_SECRET, "advertising-denial:user:v1\0source:user_id:denied-user")}`;
    const expectedOrderDenialKey = `privacy:ads_denied:v1:order:${await hmacSha256Hex(TEST_PRIVACY_LEDGER_HMAC_SECRET, "advertising-denial:order:v1\0denied-order")}`;
    assert.equal(gclidKv.map.has(expectedUserDenialKey), false, "browser claims cannot create stable-user denial keys");
    assert.equal(gclidKv.map.has(expectedOrderDenialKey), false, "browser claims cannot create stable-order denial keys");

    const authenticatedPromotionCtx = makeCtx();
    const authenticatedPromotion = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Eden-Server-Secret": "denial-server-secret",
        "Cookie": "eden_anonymous_id=denied-anon; eden_session_id=denied-session_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "authenticated_identity_promotion",
        anonymousId: "denied-anon",
        userId: "denied-user",
        properties: { order_id: "denied-order", gclid: "DENIED-GCLID" },
      }),
    }), { ...env, SERVER_API_SECRET: "denial-server-secret" }, authenticatedPromotionCtx);
    await authenticatedPromotion.json();
    await Promise.all(authenticatedPromotionCtx.promises);
    assert.equal(authenticatedPromotion.status, 200);
    assert.ok(gclidKv.map.has(expectedUserDenialKey), "authenticated server identity promotion must propagate the inherited denial to user identity");
    assert.ok(gclidKv.map.has(expectedOrderDenialKey), "authenticated server identity promotion must propagate the inherited denial to order identity");

    for (const identityBody of [
      { event: "authenticated_user_denial_check", userId: "denied-user" },
      { event: "OS_purchase", properties: { order_id: "denied-order", transaction_id: "denied-charge" } },
    ]) {
      const serverCtx = makeCtx();
      const serverResponse = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Eden-Server-Secret": "denial-server-secret",
        },
        body: JSON.stringify({
          type: "track",
          event: identityBody.event,
          properties: { gclid: "DENIED-GCLID", page_search: "?gclid=DENIED-GCLID", ...(identityBody.properties || {}) },
          ...identityBody.userId ? { userId: identityBody.userId } : {},
        }),
      }), { ...env, SERVER_API_SECRET: "denial-server-secret" }, serverCtx);
      await serverResponse.json();
      await Promise.all(serverCtx.promises);
      assert.equal(serverResponse.status, 200);
      assert.equal(segmentCalls.at(-1).properties.attribution_suppressed, true, "later user/order-only server events must inherit the propagated denial");
      assert.equal(JSON.stringify(segmentCalls.at(-1)).includes("DENIED-GCLID"), false, JSON.stringify(segmentCalls.at(-1)));
    }
    const denialCountBeforeAllow = [...gclidKv.map.keys()].filter((key) => key.startsWith("privacy:ads_denied:v1:")).length;

    const explicitAllow = new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": `eden_anonymous_id=denied-anon; eden_anon_id=denied-anon; eden_session_id=denied-session_1780000000000; __Secure-eden_ad_click_id=adclk2_denied_pointer; ${denialMarkerPair}`,
      },
      body: JSON.stringify({
        type: "track",
        event: "explicit_allow_after_denial",
        anonymousId: "denied-anon",
        userId: "denied-user",
        properties: {
          order_id: "denied-order",
          consent_state: {
            consent_status: "explicit_allowed",
            source: "cookieyes",
            action_taken: true,
            ads: "allowed",
            google_ads: "allowed",
            advertising: "allowed",
            ad_tracking: "allowed",
            partner_ad_tracking: "allowed",
            retargeting: "allowed",
            sale_share_targeted_ads: "allowed",
          },
        },
      }),
    });
    const allowCtx = makeCtx();
    const allowResponse = await worker.fetch(explicitAllow, env, allowCtx);
    await allowResponse.json();
    await Promise.all(allowCtx.promises);
    assert.equal([...gclidKv.map.keys()].filter((key) => key.startsWith("privacy:ads_denied:v1:")).length, denialCountBeforeAllow - 2, "browser explicit allow may clear only matching anonymous/session identities");
    assert.equal(gclidKv.deleteKeys.filter((key) => key.startsWith("privacy:ads_denied:v1:")).length, 2, "stable user/order tombstones require authenticated server authority to clear");
    assert.ok(gclidKv.map.has(expectedUserDenialKey));
    assert.ok(gclidKv.map.has(expectedOrderDenialKey));
    const clearedDenialMarker = getAllSetCookie(allowResponse.headers).find((cookie) => cookie.startsWith("__Secure-eden_ads_denied="));
    assert.ok(clearedDenialMarker && /Max-Age=0/.test(clearedDenialMarker), "explicit allow may clear the marker only after durable tombstone deletion succeeds");
    assert.equal(queue.messages.length, 0, "explicit allow must not reactivate a pointer that was revoked by the prior denial");
    assert.ok(JSON.parse(await adClickKv.get("adclick:id:adclk2_denied_pointer")).revoked_at, "old pointer remains revoked after explicit allow");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await deniedConsentHandoffAndBodyStateSuppressAndScrub();

async function denialPropagationWriteFailureStillSuppressesCurrentRequest() {
  class FailUserTombstoneKV extends MockKV {
    constructor() {
      super();
      this.failUserWrites = false;
    }
    async put(key, value, options = {}) {
      if (this.failUserWrites && key.startsWith("privacy:ads_denied:v1:user:")) {
        throw new Error("fixture_user_tombstone_write_failed");
      }
      return super.put(key, value, options);
    }
  }
  const originalFetch = globalThis.fetch;
  const segmentCalls = [];
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const privacyKv = new FailUserTombstoneKV();
    const env = {
      SEGMENT_WRITE_KEY: "test_write_key",
      GCLID_KV: privacyKv,
    };
    const deniedCtx = makeCtx();
    const initialDeniedResponse = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://app.eden.health",
        Cookie: "eden_anonymous_id=fail-propagation-anon; eden_session_id=fail-propagation-session_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "deny_before_failed_promotion",
        anonymousId: "fail-propagation-anon",
        properties: { consent_state: { ads: "denied", action_taken: true } },
      }),
    }), env, deniedCtx);
    await initialDeniedResponse.json();
    await Promise.all(deniedCtx.promises);
    assert.ok(getSetCookie(initialDeniedResponse.headers).some((cookie) => cookie.startsWith("__Secure-eden_ads_denied=1")));
    const markerPair = getSetCookie(initialDeniedResponse.headers).find((cookie) => cookie.startsWith("__Secure-eden_ads_denied=1")).split(";", 1)[0];
    privacyKv.failUserWrites = true;

    const promotedCtx = makeCtx();
    const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Eden-Server-Secret": "denial-server-secret",
        Cookie: `eden_anonymous_id=fail-propagation-anon; eden_session_id=fail-propagation-session_1780000000000; ${markerPair}`,
      },
      body: JSON.stringify({
        type: "track",
        event: "authenticated_failed_denial_promotion",
        anonymousId: "fail-propagation-anon",
        userId: "fail-propagation-user",
        properties: { page_url: "/intake?gclid=FAIL-PROPAGATION-GCLID", gclid: "FAIL-PROPAGATION-GCLID" },
      }),
    }), { ...env, SERVER_API_SECRET: "denial-server-secret" }, promotedCtx);
    const responseBody = await response.json();
    await Promise.all(promotedCtx.promises);
    assert.equal(response.status, 200);
    assert.ok(getSetCookie(response.headers).some((cookie) => cookie.startsWith("__Secure-eden_ads_denied=1")), "failed denial propagation write must still return the immediate marker");
    assert.equal(segmentCalls.at(-1).context?.campaign?.gclid, undefined);
    assert.equal(JSON.stringify(segmentCalls.at(-1)).includes("FAIL-PROPAGATION-GCLID"), false, "a failed propagation write must fail the current request closed for attribution");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await denialPropagationWriteFailureStillSuppressesCurrentRequest();

async function explicitAllowRestoresTrackingWhenDurableClearIsDeferred() {
  class FailDeletePrivacyKV extends MockKV {
    async delete(key) {
      this.deleteKeys.push(key);
      throw new Error("fixture_privacy_tombstone_delete_failed");
    }
  }
  const originalFetch = globalThis.fetch;
  const segmentCalls = [];
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const privacyKv = new FailDeletePrivacyKV();
    const env = { SEGMENT_WRITE_KEY: "test_write_key", GCLID_KV: privacyKv };
    const denyCtx = makeCtx();
    const denyResponse = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://app.eden.health",
        Cookie: "eden_anonymous_id=clear-failure-anon; eden_session_id=clear-failure-session_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "clear_failure_denial",
        anonymousId: "clear-failure-anon",
        properties: { consent_state: { ads: "denied", action_taken: true } },
      }),
    }), env, denyCtx);
    await denyResponse.json();
    await Promise.all(denyCtx.promises);
    const markerPair = getSetCookie(denyResponse.headers).find((cookie) => cookie.startsWith("__Secure-eden_ads_denied=1"))?.split(";", 1)[0];
    assert.ok(markerPair);

    const defaultConsentCookie = encodeURIComponent(JSON.stringify({
      consent_status: "allowed",
      source: "default_allowed_no_choice",
      action_taken: false,
    }));
    const allowCtx = makeCtx();
    const allowResponse = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://app.eden.health",
        Cookie: `eden_anonymous_id=clear-failure-anon; eden_session_id=clear-failure-session_1780000000000; eden_consent_state=${defaultConsentCookie}; ${markerPair}`,
      },
      body: JSON.stringify({
        type: "track",
        event: "clear_failure_allow",
        anonymousId: "clear-failure-anon",
        properties: {
          gclid: "CLEAR-FAILURE-GCLID",
          consent_state: {
            source: "cookieyes",
            action_taken: true,
            google_ads_allowed: true,
            allowed_for_google_click_id_upload: true,
          },
        },
      }),
    }), env, allowCtx);
    await allowResponse.json();
    await Promise.all(allowCtx.promises);
    assert.equal(allowResponse.status, 200);
    const responseCookies = getAllSetCookie(allowResponse.headers);
    assert.equal(responseCookies.some((cookie) => cookie.startsWith("__Secure-eden_ads_denied=1")), false, "an affirmative allow must not retain the request marker because a KV delete was transiently unavailable");
    assert.ok(responseCookies.some((cookie) => cookie.startsWith("__Secure-eden_ads_denied=") && /Max-Age=0/.test(cookie)), "an affirmative allow clears the first-party request marker while durable cleanup is retried by later allowed requests");
    assert.equal(segmentCalls.at(-1).properties.attribution_suppressed, false);
    assert.equal(JSON.stringify(segmentCalls.at(-1)).includes("CLEAR-FAILURE-GCLID"), true, "a transient durable-clear failure must not discard current valid Google evidence");
    assert.equal(segmentCalls.at(-1).event, "clear_failure_allow", "a default cookie must not hide the current partial-but-authoritative boolean allow candidate");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await explicitAllowRestoresTrackingWhenDurableClearIsDeferred();

async function canonicalAnonymousIdMigrationPrefersLongName() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const queue = new MockQueue();
    const env = { EDEN_AD_CLICK_MEMORY_MODE: "shadow", AD_CLICK_SNAPSHOT_QUEUE: queue };
    const legacyOnly = new Request("https://www.eden.health/?gclid=CANONICAL-MIGRATION-GCLID", {
      headers: { "User-Agent": "Mozilla/5.0", "Cookie": "eden_anon_id=legacy-anon; eden_session_id=legacy-session_1780000000000" },
    });
    const legacyCtx = makeCtx();
    const legacyResponse = await worker.fetch(legacyOnly, env, legacyCtx);
    await legacyResponse.text();
    await Promise.all(legacyCtx.promises);
    assert.ok(getSetCookie(legacyResponse.headers).some((cookie) => cookie.startsWith("eden_anonymous_id=legacy-anon")), "legacy-only browser must receive the canonical cookie with the same value");
    assert.ok(getSetCookie(legacyResponse.headers).some((cookie) => cookie.startsWith("eden_anon_id=legacy-anon")), "legacy alias remains dual-written during migration");

    const conflict = new Request("https://www.eden.health/?gclid=CANONICAL-CONFLICT-GCLID", {
      headers: { "User-Agent": "Mozilla/5.0", "Cookie": "eden_anonymous_id=canonical-winner; eden_anon_id=legacy-loser; eden_session_id=conflict-session_1780000000000" },
    });
    const conflictCtx = makeCtx();
    const conflictResponse = await worker.fetch(conflict, env, conflictCtx);
    await conflictResponse.text();
    await Promise.all(conflictCtx.promises);
    assert.equal(queue.messages.at(-1).payload.snapshot.first_party.eden_anonymous_id, "canonical-winner", "queue envelopes must carry the canonical long-name ID");
    assert.equal(queue.messages.at(-1).payload.snapshot.first_party.eden_anon_id, "canonical-winner", "the existing BigQuery landing alias remains in the migration envelope");
    assert.ok(getSetCookie(conflictResponse.headers).some((cookie) => cookie.startsWith("eden_anon_id=canonical-winner")), "losing legacy alias must be repaired, not linked");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await canonicalAnonymousIdMigrationPrefersLongName();

async function pointerOwnershipRejectsDanglingCopiedRevokedAndAccountSwitch() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) return new Response("{}", { status: 200 });
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  const makePointerRecord = async (id, anon, session, extra = {}) => ({
    schema_version: "eden_ad_click_pointer_v2",
    ad_click_id: id,
    snapshot_id: `adsnap_${id}`,
    captured_at: "2026-07-09T00:00:00.000Z",
    primary_click_id_type: "gclid",
    has_class_a: true,
    has_primary_click_evidence: true,
    owner_anonymous_id_sha256: await sha256Raw(anon),
    owner_session_id_sha256: await sha256Raw(session),
    ad_click_id_scope: "first_party_scoped",
    ownership_scope: "first_party_owner_bound",
    ...extra,
  });
  const makePurchase = (pointer, anon, session, user, order) => new Request("https://collect.eden.health/server-collect", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Eden-Server-Secret": "pointer-ownership-server-secret",
      "Cookie": `eden_anonymous_id=${anon}; eden_anon_id=${anon}; eden_session_id=${session}; __Secure-eden_ad_click_id=${pointer}`,
    },
    body: JSON.stringify({ type: "track", event: "OS_purchase", anonymousId: anon, userId: user, properties: { order_id: order, transaction_id: `charge-${order}` } }),
  });
  try {
    const kv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      AD_CLICK_KV: kv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
      SERVER_API_SECRET: "pointer-ownership-server-secret",
    };
    const validId = "adclk2_owner_bound_valid";
    await kv.put(`adclick:id:${validId}`, JSON.stringify(await makePointerRecord(validId, "owner-anon", "owner-session_1780000000000")));
    const validCtx = makeCtx();
    const validResponse = await worker.fetch(makePurchase(validId, "owner-anon", "owner-session_1780000000000", "owner-user-a", "owner-order-a"), env, validCtx);
    await validResponse.json();
    await Promise.all(validCtx.promises);
    assert.ok(queue.messages.some((message) => message.payload.event_type === "ad_click_identity_links"), "valid owner-bound pointer should link the purchase");
    const claimedRecord = JSON.parse(await kv.get(`adclick:id:${validId}`));
    assert.equal(claimedRecord.claimed_user_id_sha256, await sha256Raw("source:user_id:owner-user-a"));
    const beforeSwitch = queue.messages.length;
    const switchCtx = makeCtx();
    const switchResponse = await worker.fetch(makePurchase(validId, "owner-anon", "owner-session_1780000000000", "owner-user-b", "owner-order-b"), env, switchCtx);
    await switchResponse.json();
    await Promise.all(switchCtx.promises);
    assert.equal(queue.messages.length, beforeSwitch, "same browser switching to a different user must not inherit the first user's pointer");

    const copiedId = "adclk2_owner_bound_copied";
    await kv.put(`adclick:id:${copiedId}`, JSON.stringify(await makePointerRecord(copiedId, "original-anon", "original-session_1780000000000")));
    const copiedCtx = makeCtx();
    const copiedResponse = await worker.fetch(makePurchase(copiedId, "copied-anon", "copied-session_1780000000000", "copied-user", "copied-order"), env, copiedCtx);
    await copiedResponse.json();
    await Promise.all(copiedCtx.promises);
    assert.equal(queue.messages.length, beforeSwitch, "copied pointer with a different anonymous owner must be rejected");

    const missingCtx = makeCtx();
    const missingResponse = await worker.fetch(makePurchase("adclk2_missing_backing", "missing-anon", "missing-session_1780000000000", "missing-user", "missing-order"), env, missingCtx);
    await missingResponse.json();
    await Promise.all(missingCtx.promises);
    assert.equal(queue.messages.length, beforeSwitch, "dangling pointer without a backing record must be rejected");

    const revokedId = "adclk2_owner_bound_revoked";
    await kv.put(`adclick:id:${revokedId}`, JSON.stringify(await makePointerRecord(revokedId, "revoked-anon", "revoked-session_1780000000000", { revoked_at: "2026-07-09T01:00:00.000Z" })));
    const revokedCtx = makeCtx();
    const revokedResponse = await worker.fetch(makePurchase(revokedId, "revoked-anon", "revoked-session_1780000000000", "revoked-user", "revoked-order"), env, revokedCtx);
    await revokedResponse.json();
    await Promise.all(revokedCtx.promises);
    assert.equal(queue.messages.length, beforeSwitch, "revoked pointer must not resolve");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await pointerOwnershipRejectsDanglingCopiedRevokedAndAccountSwitch();

async function recoveredEvidenceDoesNotFanOutSnapshotsOrOrdinaryLinks() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const gclidKv = new MockKV();
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      SEGMENT_WRITE_KEY: "test_write_key",
      SERVER_API_SECRET: "fanout-server-secret",
      GCLID_KV: gclidKv,
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const nativeBody = JSON.stringify({
      type: "track",
      event: "native_click_capture",
      anonymousId: "fanout-anon",
      properties: { page_url: "https://app.eden.health/intake?gclid=FANOUT-GCLID&utm_source=google&utm_medium=cpc" },
      context: { page: { url: "https://app.eden.health/intake?gclid=FANOUT-GCLID&utm_source=google&utm_medium=cpc" } },
    });
    const nativeRequest = () => new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Origin": "https://app.eden.health", "Cookie": "eden_anonymous_id=fanout-anon; eden_anon_id=fanout-anon; eden_session_id=fanout-session_1780000000000" },
      body: nativeBody,
    });
    const firstCtx = makeCtx();
    const firstResponse = await worker.fetch(nativeRequest(), env, firstCtx);
    const firstResult = await firstResponse.json();
    await Promise.all(firstCtx.promises);
    const pointerId = readCookieFromSetCookie(firstResponse.headers, "__Secure-eden_ad_click_id");
    assert.ok(pointerId);
    assert.equal(queue.messages.filter((message) => message.payload.event_type === "ad_click_snapshot").length, 1);
    const firstSnapshotId = queue.messages.find((message) => message.payload.event_type === "ad_click_snapshot").payload.snapshot.snapshot_id;
    assert.ok(firstResult.anonId);

    const repeatedNativeCtx = makeCtx();
    const repeatedNativeResponse = await worker.fetch(nativeRequest(), env, repeatedNativeCtx);
    await repeatedNativeResponse.json();
    await Promise.all(repeatedNativeCtx.promises);
    const nativeSnapshots = queue.messages.filter((message) => message.payload.event_type === "ad_click_snapshot");
    assert.equal(nativeSnapshots.length, 2, "each sequential native observation must append a new snapshot");
    const secondSnapshotId = nativeSnapshots[1].payload.snapshot.snapshot_id;
    assert.notEqual(secondSnapshotId, firstSnapshotId, "independent native observations must have unique snapshot IDs");
    assert.equal(nativeSnapshots[1].payload.ad_click_id, pointerId, "append-only observations may share the same scoped ad_click_id");
    assert.equal(JSON.parse(await adClickKv.get(`adclick:id:${pointerId}`)).snapshot_id, secondSnapshotId, "pointer backing record should reference the latest observation");

    const attrCookie = encodeURIComponent(JSON.stringify({ gclid: "FANOUT-GCLID", utm_source: "google", utm_medium: "cpc", _ts: Date.now() }));
    const recoveredRequest = (event, userId = null, orderId = null) => new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": `eden_anonymous_id=fanout-anon; eden_anon_id=fanout-anon; eden_session_id=fanout-session_1780000000000; eden_attr=${attrCookie}; __Secure-eden_ad_click_id=${pointerId}`,
      },
      body: JSON.stringify({
        type: "track",
        event,
        anonymousId: "fanout-anon",
        ...(userId ? { userId } : {}),
        properties: { page_url: "https://app.eden.health/intake", ...(orderId ? { order_id: orderId } : {}) },
        context: { page: { url: "https://app.eden.health/intake" } },
      }),
    });
    const ordinaryBefore = queue.messages.length;
    const ordinaryCtx = makeCtx();
    const ordinaryResponse = await worker.fetch(recoveredRequest("os_question_answered"), env, ordinaryCtx);
    await ordinaryResponse.json();
    await Promise.all(ordinaryCtx.promises);
    assert.equal(queue.messages.length, ordinaryBefore, "ordinary recovered event must emit neither another snapshot nor redundant identity links");

    const purchaseCtx = makeCtx();
    const purchaseResponse = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Eden-Server-Secret": "fanout-server-secret",
        "Cookie": `eden_anonymous_id=fanout-anon; eden_anon_id=fanout-anon; eden_session_id=fanout-session_1780000000000; eden_attr=${attrCookie}; __Secure-eden_ad_click_id=${pointerId}`,
      },
      body: JSON.stringify({
        type: "track",
        event: "OS_purchase",
        anonymousId: "fanout-anon",
        userId: "fanout-user",
        properties: { order_id: "fanout-order", transaction_id: "fanout-charge", page_url: "https://app.eden.health/intake" },
        context: { page: { url: "https://app.eden.health/intake" } },
      }),
    }), env, purchaseCtx);
    await purchaseResponse.json();
    await Promise.all(purchaseCtx.promises);
    assert.equal(queue.messages.filter((message) => message.payload.event_type === "ad_click_snapshot").length, 2, "purchase recovery must not create another observation snapshot");
    assert.ok(queue.messages.some((message) => message.payload.event_type === "ad_click_identity_links" && message.payload.ad_click_id === pointerId), "purchase recovery should emit only deterministic identity links");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await recoveredEvidenceDoesNotFanOutSnapshotsOrOrdinaryLinks();

async function googleEvidencePrioritySurvivesReturnSessionAndServerAuthorization() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const gclidKv = new MockKV();
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      EDEN_AD_CLICK_KV_INDEX_MODE: "full",
      EDEN_AD_CLICK_KV_RESOLVER_MODE: "pointer_only",
      SEGMENT_WRITE_KEY: "test_write_key",
      SERVER_API_SECRET: "test_server_secret",
      GCLID_KV: gclidKv,
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const firstCtx = makeCtx();
    const firstResponse = await worker.fetch(new Request(
      "https://www.eden.health/?gclid=PRIORITY-GCLID&gbraid=PRIORITY-GBRAID&wbraid=PRIORITY-WBRAID&utm_source=google&utm_medium=cpc&utm_campaign=priority_fixture",
      {
        headers: {
          "User-Agent": "Mozilla/5.0",
          "Cookie": "eden_anonymous_id=multi-session-anon; eden_session_id=multi-session-a_1780000000000",
        },
      },
    ), env, firstCtx);
    await firstResponse.text();
    await Promise.all(firstCtx.promises);

    const firstSnapshotEnvelope = queue.messages.find((message) => message.payload.event_type === "ad_click_snapshot")?.payload;
    assert.ok(firstSnapshotEnvelope, "the first edge request must create one immutable click snapshot");
    assert.equal(firstSnapshotEnvelope.snapshot.evidence.primary_click_id_type, "gclid", "primary Google evidence must prefer gclid when all three upload IDs are present");
    assert.deepEqual(
      [...firstSnapshotEnvelope.snapshot.evidence.upload_candidate_types].sort(),
      ["gclid", "gbraid", "wbraid"].sort(),
      "primary selection must not discard valid braid evidence",
    );
    assert.equal(firstSnapshotEnvelope.snapshot.google.gclid, "PRIORITY-GCLID");
    assert.equal(firstSnapshotEnvelope.snapshot.google.gbraid, "PRIORITY-GBRAID");
    assert.equal(firstSnapshotEnvelope.snapshot.google.wbraid, "PRIORITY-WBRAID");

    const returnCtx = makeCtx();
    const returnResponse = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": "eden_anonymous_id=multi-session-anon; eden_session_id=multi-session-b_1780003600000",
      },
      body: JSON.stringify({
        type: "track",
        event: "return_session_identified",
        anonymousId: "multi-session-anon",
        userId: "multi-session-user",
        properties: { page_url: "https://app.eden.health/intake" },
        context: { page: { url: "https://app.eden.health/intake", path: "/intake" } },
      }),
    }), env, returnCtx);
    await returnResponse.json();
    await Promise.all(returnCtx.promises);
    const returnEvent = segmentCalls.find((call) => call.event === "return_session_identified");
    assert.ok(returnEvent, "the return-session behavior event must reach Segment");
    assert.equal(returnEvent.properties.first_touch_gclid, "PRIORITY-GCLID", "a later session must retain the original first paid touch");
    assert.equal(returnEvent.properties.first_touch_gbraid, "PRIORITY-GBRAID");
    assert.equal(returnEvent.properties.first_touch_wbraid, "PRIORITY-WBRAID");
    assert.equal(returnEvent.properties.current_touch_gclid, undefined, "a direct return session must not masquerade as a fresh paid click");
    assert.equal(returnEvent.context.campaign.gclid, undefined, "event-native campaign context stays honest on a return session");
    assert.equal(returnEvent.context.recovered_campaign.gclid, "PRIORITY-GCLID");
    assert.equal(queue.messages.filter((message) => message.payload.event_type === "ad_click_snapshot").length, 1, "the recovered return session must not create another observation snapshot");

    const serverCtx = makeCtx();
    const serverResponse = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Eden-Server-Secret": "test_server_secret",
      },
      body: JSON.stringify({
        type: "track",
        event: "OS_purchase",
        messageId: "OS_purchase:multi-session-charge",
        anonymousId: "multi-session-anon",
        userId: "multi-session-user",
        properties: {
          order_id: "multi-session-order",
          transaction_id: "multi-session-charge",
          conversion_stage: "payment_authorized",
          eden_session_id: "multi-session-b_1780003600000",
          edge_session_id: "multi-session-b_1780003600000",
          attribution_snapshot_id: firstSnapshotEnvelope.snapshot.snapshot_id,
          edge_join_key_version: "edge_join_v1",
          edge_join_key_source: "server_session",
        },
      }),
    }), env, serverCtx);
    await serverResponse.json();
    await Promise.all(serverCtx.promises);
    const serverEvent = [...segmentCalls].reverse().find((call) => call.event === "OS_purchase");
    assert.ok(serverEvent, "the server payment-authorization event must reach Segment");
    assert.equal(serverEvent.properties.first_touch_gclid, "PRIORITY-GCLID", "server payment authorization must retain the first Google click evidence");
    assert.equal(serverEvent.properties.gclid, "PRIORITY-GCLID", "conversion properties retain upload-grade evidence with recovered provenance");
    assert.equal(serverEvent.context.campaign.gclid, undefined, "recovered server attribution must not be reported as event-native");
    assert.equal(serverEvent.context.recovered_campaign.gclid, "PRIORITY-GCLID");
    assert.equal(queue.messages.filter((message) => message.payload.event_type === "ad_click_snapshot").length, 1, "server authorization must not fan out another click snapshot");
    const serverLinkEnvelope = [...queue.messages].reverse().find((message) =>
      message.payload.event_type === "ad_click_identity_links"
      && message.payload.identity_links?.some((link) => link.source_type === "server"),
    )?.payload;
    assert.equal(serverLinkEnvelope, undefined, "without an owned pointer or enabled first-party resolver, server authorization must not recover ad_click_id from raw Google evidence");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await googleEvidencePrioritySurvivesReturnSessionAndServerAuthorization();

async function authenticatedStableIdentityRecoversAcrossAnonymousRotationAndLoss() {
  const originalFetch = globalThis.fetch;
  const segmentCalls = [];
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200, headers: { "Content-Type": "application/json" } });
    }
    return new Response("origin ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const gclidKv = new MockKV();
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const coordinator = new MockConversionCoordinatorNamespace();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      SEGMENT_WRITE_KEY: "test_write_key",
      SERVER_API_SECRET: "stable-bridge-server-secret",
      GCLID_KV: gclidKv,
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
      CONVERSION_COORDINATOR: coordinator,
    };

    const landingCtx = makeCtx();
    const landing = await worker.fetch(new Request(
      "https://www.eden.health/?gclid=STABLE-BRIDGE-GCLID&gbraid=STABLE-BRIDGE-GBRAID&utm_source=google&utm_medium=cpc&utm_campaign=stable_bridge",
      {
        headers: {
          "User-Agent": "Mozilla/5.0",
          Cookie: "eden_anonymous_id=stable-bridge-anon-a; eden_session_id=stable-bridge-session-a_1780000000000",
        },
      },
    ), env, landingCtx);
    await landing.text();
    await Promise.all(landingCtx.promises);
    const pointerId = readCookieFromSetCookie(landing.headers, "__Secure-eden_ad_click_id");
    const attrCookie = readCookieFromSetCookie(landing.headers, "eden_attr");
    assert.ok(pointerId && attrCookie, "initial edge capture must establish pointer and first-touch continuity");

    const stableUserId = "stable-bridge-user";
    const firstOrderId = "stable-bridge-order-a";
    const purchaseCtx = makeCtx();
    const purchase = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Eden-Server-Secret": "stable-bridge-server-secret",
        Cookie: `eden_anonymous_id=stable-bridge-anon-a; eden_session_id=stable-bridge-session-a_1780000000000; eden_attr=${attrCookie}; __Secure-eden_ad_click_id=${pointerId}`,
      },
      body: JSON.stringify({
        type: "track",
        event: "OS_purchase",
        messageId: "OS_purchase:stable-bridge-charge-a",
        anonymousId: "stable-bridge-anon-a",
        userId: stableUserId,
        properties: { order_id: firstOrderId, transaction_id: "stable-bridge-charge-a", conversion_stage: "payment_authorized" },
      }),
    }), env, purchaseCtx);
    await purchase.json();
    await Promise.all(purchaseCtx.promises);
    assert.equal(purchase.status, 200);
    const purchaseEnvelope = queue.messages.map((message) => message.payload).at(-1);
    const userHash = purchaseEnvelope.identity_links.find((link) => link.from_type === "user_id_sha256")?.from_id
      || purchaseEnvelope.identity_links.find((link) => link.to_type === "user_id_sha256")?.to_id;
    assert.match(userHash || "", /^[a-f0-9]{64}$/, "purchase envelope must expose the canonical authenticated user hash");
    const userBridge = coordinator.identityPointerRecords.get(`eden_identity_pointer_v1:user_id_sha256:${userHash}`);
    assert.equal(userBridge?.latest_ad_click_id, pointerId, "authenticated purchase must create the stable Eden user bridge");

    const sendFollowup = async ({ suffix, anonymousId, cookie }) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Eden-Server-Secret": "stable-bridge-server-secret",
          ...(cookie ? { Cookie: cookie } : {}),
        },
        body: JSON.stringify({
          type: "track",
          event: "OS_qualified_first_order",
          ...(anonymousId ? { anonymousId } : {}),
          userId: stableUserId,
          properties: { order_id: `stable-bridge-order-${suffix}`, fixture_suffix: suffix },
        }),
      }), env, ctx);
      await response.json();
      await Promise.all(ctx.promises);
      assert.equal(response.status, 200);
      return queue.messages.map((message) => message.payload).at(-1);
    };

    const rotated = await sendFollowup({
      suffix: "rotated",
      anonymousId: "stable-bridge-anon-b",
      cookie: "eden_anonymous_id=stable-bridge-anon-b; eden_session_id=stable-bridge-session-b_1780003600000",
    });
    assert.equal(rotated.event_type, "ad_click_identity_links");
    assert.equal(rotated.ad_click_id, pointerId);
    assert.equal(rotated.resolution.resolution_source, "stable_user_pointer");
    assert.equal(rotated.resolution.resolution_reason, "pointer_user_owner_match_anonymous_rotated");

    const cookieless = await sendFollowup({ suffix: "cookieless", anonymousId: null, cookie: null });
    assert.equal(cookieless.event_type, "ad_click_identity_links");
    assert.equal(cookieless.ad_click_id, pointerId, "authenticated user identity must recover the click even after browser owner cookies disappear");
    assert.equal(cookieless.resolution.resolution_source, "stable_user_pointer");
    assert.equal(
      queue.messages.filter((message) => message.payload.event_type === "ad_click_snapshot").length,
      1,
      "stable-identity recovery must link the original observation rather than fan out recovered snapshots",
    );
    assert.equal(
      segmentCalls.filter((call) => call.event === "OS_qualified_first_order" && call.properties?.fixture_suffix).length,
      2,
      "both authenticated follow-up milestones must reach Segment exactly once",
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await authenticatedStableIdentityRecoversAcrossAnonymousRotationAndLoss();

async function unownedGoogleEvidenceRemainsDiagnosticOnly() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const adClickKv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      EDEN_AD_CLICK_KV_INDEX_MODE: "full",
      SEGMENT_WRITE_KEY: "test_write_key",
      SERVER_API_SECRET: "test_server_secret",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const ctx = makeCtx();
    const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Eden-Server-Secret": "test_server_secret",
      },
      body: JSON.stringify({
        type: "track",
        event: "unowned_google_evidence_diagnostic_fixture",
        properties: {
          page_url: "https://app.eden.health/intake?gclid=UNOWNED-DIAGNOSTIC-GCLID&utm_source=google&utm_medium=cpc",
        },
        context: {
          page: { url: "https://app.eden.health/intake?gclid=UNOWNED-DIAGNOSTIC-GCLID&utm_source=google&utm_medium=cpc" },
        },
      }),
    }), env, ctx);
    const responseBody = await response.json();
    await Promise.all(ctx.promises);

    assert.equal(response.status, 200);
    const envelope = queue.messages.find((message) => message.payload.event_type === "ad_click_snapshot")?.payload;
    assert.ok(envelope, "unowned click evidence should remain observable in the queue/BigQuery diagnostic path");
    assert.equal(envelope.snapshot.governance.ad_click_id_scope, "instance_random");
    assert.deepEqual(envelope.identity_links, [], "an unowned diagnostic observation must not create identity links");
    assert.equal(adClickKv.putKeys.length, 0, "an unowned diagnostic observation must not write pointer or reverse KV state");
    assert.equal(readCookieFromSetCookie(response.headers, "__Secure-eden_ad_click_id"), null, "an unowned diagnostic observation must not set a browser pointer");
    assert.equal(responseBody?.segment_forwarded, false, "ownerless server events must not collapse onto a synthetic Segment identity");
    assert.equal(segmentCalls.length, 0, "raw ownerless Google evidence must remain outside Segment");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await unownedGoogleEvidenceRemainsDiagnosticOnly();

async function browserStableIdentityClaimsCannotPoisonLaterServerAttribution() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const kv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      EDEN_AD_CLICK_KV_INDEX_MODE: "full",
      SEGMENT_WRITE_KEY: "test_write_key",
      SERVER_API_SECRET: "poisoning-server-secret",
      GCLID_KV: kv,
      AD_CLICK_KV: kv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    };
    const attackerAnon = "browser-poison-attacker-anon";
    const attackerSession = "browser-poison-attacker-session_1780000000000";
    const landingCtx = makeCtx();
    const landing = await worker.fetch(new Request(
      "https://www.eden.health/?gclid=BROWSER-POISON-ATTACKER-GCLID&utm_source=google&utm_medium=cpc",
      { headers: { "User-Agent": "Mozilla/5.0", Cookie: `eden_anonymous_id=${attackerAnon}; eden_session_id=${attackerSession}` } },
    ), env, landingCtx);
    await landing.text();
    await Promise.all(landingCtx.promises);
    const attackerPointer = readCookieFromSetCookie(landing.headers, "__Secure-eden_ad_click_id");
    assert.ok(attackerPointer);

    const browserCookies = `eden_anonymous_id=${attackerAnon}; eden_session_id=${attackerSession}; __Secure-eden_ad_click_id=${attackerPointer}`;
    const claimedUser = "victim-stable-user-id";
    const claimedOrder = "victim-stable-order-id";
    const identifyCtx = makeCtx();
    const identifyResponse = await worker.fetch(new Request("https://collect.eden.health/identify", {
      method: "POST",
      headers: { "Content-Type": "application/json", Origin: "https://app.eden.health", Cookie: browserCookies },
      body: JSON.stringify({
        type: "identify",
        userId: claimedUser,
        traits: { email: "victim@example.test", phone: "+15555550123" },
        properties: { order_id: claimedOrder },
      }),
    }), env, identifyCtx);
    const identifyBody = await identifyResponse.json();
    await Promise.all(identifyCtx.promises);
    assert.equal(identifyResponse.status, 200);
    assert.equal(identifyBody.stable_identity_accepted, false);

    const collectCtx = makeCtx();
    const collectResponse = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", Origin: "https://app.eden.health", Cookie: browserCookies },
      body: JSON.stringify({
        type: "track",
        event: "browser_identity_poison_attempt",
        anonymousId: attackerAnon,
        userId: claimedUser,
        properties: {
          order_id: claimedOrder,
          email_sha256: "victim-email-hash",
          nested: { customer_id: claimedUser, phone_sha256: "victim-phone-hash" },
        },
      }),
    }), env, collectCtx);
    await collectResponse.json();
    await Promise.all(collectCtx.promises);
    assert.equal(collectResponse.status, 200);
    const preServerQueue = JSON.stringify(queue.messages);
    assert.equal(preServerQueue.includes(claimedUser), false);
    assert.equal(preServerQueue.includes(claimedOrder), false);
    assert.equal(preServerQueue.includes("victim-email-hash"), false);
    assert.equal(preServerQueue.includes("victim-phone-hash"), false);
    assert.equal(kv.putKeys.some((key) => key.includes(claimedUser) || key.includes(claimedOrder)), false, "browser claims must never create stable KV indexes");
    assert.equal(segmentCalls.some((call) => call.type === "identify" || call.type === "alias"), false, "browser claims must never emit Segment identity mutations");

    const purchaseCtx = makeCtx();
    const purchaseResponse = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "poisoning-server-secret" },
      body: JSON.stringify({
        type: "track",
        event: "OS_purchase",
        userId: claimedUser,
        properties: { order_id: claimedOrder, transaction_id: `charge-${claimedOrder}` },
      }),
    }), env, purchaseCtx);
    await purchaseResponse.json();
    await Promise.all(purchaseCtx.promises);
    const purchase = [...segmentCalls].reverse().find((call) => call.event === "OS_purchase");
    assert.ok(purchase);
    assert.equal(JSON.stringify(purchase).includes("BROWSER-POISON-ATTACKER-GCLID"), false, "later authenticated purchase must not recover the attacker's browser attribution");
    const poisonedStableLink = queue.messages.some((message) =>
      message.payload.event_type === "ad_click_identity_links"
      && message.payload.ad_click_id === attackerPointer
      && message.payload.identity_links?.some((link) =>
        ["user_id_sha256", "order_id_sha256", "email_sha256", "phone_sha256"].includes(link.from_type)
        || ["user_id_sha256", "order_id_sha256", "email_sha256", "phone_sha256"].includes(link.to_type)
      ),
    );
    assert.equal(poisonedStableLink, false, "browser claims must not bind attacker attribution to a stable user/order");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await browserStableIdentityClaimsCannotPoisonLaterServerAttribution();

async function signedHandoffRequiresBackingPointerRecord() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) return new Response("{}", { status: 200 });
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const anonId = "dangling-handoff-anon";
    const sessionId = "dangling-handoff-session_1780000000000";
    const pointerId = "adclk2_dangling_signed_pointer";
    const destination = "https://app.eden.health/intake/dangling?plan=weightloss";
    const nowSeconds = Math.floor(Date.now() / 1000);
    const assertion = await signedInternalHandoffFixture(TEST_BROWSER_CAP_HMAC_SECRET, {
      iat: nowSeconds,
      exp: nowSeconds + 300,
      pointerId,
      anonId,
      sessionId,
      destination,
    });
    const requestUrl = new URL(destination);
    requestUrl.searchParams.set("eden_attr_handoff", assertion);
    const kv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      AD_CLICK_KV: kv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
      SERVER_API_SECRET: "dangling-server-secret",
      SEGMENT_WRITE_KEY: "test_write_key",
    };
    const pageCtx = makeCtx();
    const response = await worker.fetch(new Request(requestUrl, {
      headers: {
        "User-Agent": "Mozilla/5.0",
        Cookie: `eden_anonymous_id=${anonId}; eden_session_id=${sessionId}; __Secure-eden_ad_click_id=${pointerId}`,
      },
    }), env, pageCtx);
    await response.text();
    await Promise.all(pageCtx.promises);
    assert.equal(readCookieFromSetCookie(response.headers, "__Secure-eden_internal_handoff"), null, "a signed assertion without its pointer record must not mint continuation state");

    const purchaseCtx = makeCtx();
    const purchase = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Eden-Server-Secret": "dangling-server-secret",
        Cookie: `eden_anonymous_id=${anonId}; eden_session_id=${sessionId}; __Secure-eden_ad_click_id=${pointerId}`,
      },
      body: JSON.stringify({ type: "track", event: "OS_purchase", anonymousId: anonId, userId: "dangling-user", properties: { order_id: "dangling-order", transaction_id: "dangling-charge" } }),
    }), env, purchaseCtx);
    await purchase.json();
    await Promise.all(purchaseCtx.promises);
    assert.equal(
      queue.messages.some((message) => message.payload.ad_click_id === pointerId || message.payload.selected_ad_click_id === pointerId),
      false,
      "a dangling signed pointer must never attach to a conversion",
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await signedHandoffRequiresBackingPointerRecord();

async function freshBraidOutranksRecoveredGclid() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    for (const braidType of ["gbraid", "wbraid"]) {
      const kv = new MockKV();
      const queue = new MockQueue();
      const anonId = `fresh-${braidType}-anon`;
      const sessionId = `fresh-${braidType}-session_1780000000000`;
      const env = {
        EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
        EDEN_AD_CLICK_MEMORY_MODE: "cookie",
        EDEN_AD_CLICK_KV_INDEX_MODE: "pointer",
        AD_CLICK_KV: kv,
        AD_CLICK_SNAPSHOT_QUEUE: queue,
      };
      const oldCtx = makeCtx();
      const oldResponse = await worker.fetch(new Request(
        "https://www.eden.health/?gclid=RECOVERED-OLDER-GCLID&utm_source=google&utm_medium=cpc",
        { headers: { "User-Agent": "Mozilla/5.0", Cookie: `eden_anonymous_id=${anonId}; eden_session_id=${sessionId}` } },
      ), env, oldCtx);
      await oldResponse.text();
      await Promise.all(oldCtx.promises);
      const oldPointer = readCookieFromSetCookie(oldResponse.headers, "__Secure-eden_ad_click_id");
      assert.ok(oldPointer);
      const recoveredPreAuth = encodeURIComponent(JSON.stringify({ gclid: "RECOVERED-OLDER-GCLID", utm_source: "google", utm_medium: "cpc" }));
      const freshValue = `FRESH-${braidType.toUpperCase()}-NATIVE`;
      const freshCtx = makeCtx();
      const freshResponse = await worker.fetch(new Request(
        `https://app.eden.health/intake?${braidType}=${freshValue}&utm_source=google&utm_medium=cpc&utm_campaign=fresh_braid`,
        {
          headers: {
            "User-Agent": "Mozilla/5.0",
            Cookie: `eden_anonymous_id=${anonId}; eden_session_id=${sessionId}; __Secure-eden_ad_click_id=${oldPointer}; eden_pre_auth=${recoveredPreAuth}`,
          },
        },
      ), env, freshCtx);
      await freshResponse.text();
      await Promise.all(freshCtx.promises);
      const freshPointer = readCookieFromSetCookie(freshResponse.headers, "__Secure-eden_ad_click_id");
      assert.ok(freshPointer && freshPointer !== oldPointer, `fresh ${braidType} must create a new native observation instead of inheriting recovered GCLID identity`);
      const freshEnvelope = queue.messages.filter((message) => message.payload.event_type === "ad_click_snapshot").at(-1).payload;
      assert.equal(freshEnvelope.ad_click_id, freshPointer);
      assert.equal(freshEnvelope.snapshot.google[braidType], freshValue);
      assert.equal(freshEnvelope.snapshot.google.gclid, undefined, "recovered GCLID must not be copied into a fresh braid snapshot");
      assert.equal(freshEnvelope.snapshot.evidence.primary_click_id_type, braidType);
    }
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await freshBraidOutranksRecoveredGclid();

async function aliasedDenialScrubsEveryNestedAdvertisingField() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const queue = new MockQueue();
    const deep = { keep: "business-state" };
    let cursor = deep;
    for (let depth = 0; depth < 20; depth += 1) {
      cursor.child = { keep: depth };
      cursor = cursor.child;
    }
    cursor["AmP;GcLiD"] = "DENIED-DEEP-GCLID";
    cursor.destination = "https://app.eden.health/intake?gclid=DENIED-DESTINATION-GCLID&utm_source=google";
    const body = {
      type: "track",
      event: "aliased_denial_deep_scrub",
      anonymousId: "aliased-denial-anon",
      properties: {
        "amp;GCLID": "DENIED-ALIASED-GCLID",
        destination: "https://app.eden.health/intake?gclid=DENIED-TOP-DESTINATION-GCLID&utm_campaign=denied",
        deep,
        consent_state: {
          consent_status: "opted_out",
          source: "cookieyes",
          action_taken: true,
          user_choice: "opted_out",
          ads: "denied",
        },
      },
      context: { page: { url: "https://app.eden.health/intake?gclid=DENIED-PAGE-GCLID&utm_source=google" } },
    };
    const ctx = makeCtx();
    const response = await worker.fetch(new Request(
      "https://collect.eden.health/collect?amp;Eden_Consent_Handoff=1&amp;Eden_Consent_Ads=denied",
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Origin: "https://app.eden.health",
          Cookie: "eden_anonymous_id=aliased-denial-anon; eden_session_id=aliased-denial-session_1780000000000",
        },
        body: JSON.stringify(body),
      },
    ), {
      SEGMENT_WRITE_KEY: "test_write_key",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      AD_CLICK_KV: new MockKV(),
      AD_CLICK_SNAPSHOT_QUEUE: queue,
    }, ctx);
    await response.json();
    await Promise.all(ctx.promises);
    assert.equal(response.status, 200);
    assert.equal(queue.messages.length, 0, "aliased explicit denial must suppress all attribution queue writes");
    assert.ok(getSetCookie(response.headers).some((cookie) => cookie.startsWith("__Secure-eden_ads_denied=1")));
    const forwarded = segmentCalls.at(-1);
    assert.ok(forwarded);
    const serialized = JSON.stringify(forwarded);
    for (const forbidden of [
      "DENIED-ALIASED-GCLID",
      "DENIED-DEEP-GCLID",
      "DENIED-DESTINATION-GCLID",
      "DENIED-TOP-DESTINATION-GCLID",
      "DENIED-PAGE-GCLID",
    ]) assert.equal(serialized.includes(forbidden), false, `${forbidden} must be scrubbed under denied consent`);
    assert.equal(forwarded.properties.attribution_suppressed, true);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await aliasedDenialScrubsEveryNestedAdvertisingField();

async function deepSignedHandoffTransportFailsClosed() {
  const originalFetch = globalThis.fetch;
  const originalHtmlRewriter = globalThis.HTMLRewriter;
  let originRequestUrl = null;
  let injectedScripts = "";
  globalThis.fetch = async (input) => {
    originRequestUrl = input instanceof Request ? input.url : String(input);
    return new Response("<html><head></head><body>ok</body></html>", { status: 200, headers: { "content-type": "text/html" } });
  };
  globalThis.HTMLRewriter = class {
    on(selector, handler) { this.selector = selector; this.handler = handler; return this; }
    transform(response) {
      this.handler.element({ prepend(value) { injectedScripts += String(value); } });
      return response;
    }
  };
  try {
    const anonId = "deep-handoff-anon";
    const sessionId = "deep-handoff-session_1780000000000";
    const pointerId = "adclk2_deep_handoff_pointer";
    const kv = new MockKV();
    await kv.put(`adclick:id:${pointerId}`, JSON.stringify({
      schema_version: "eden_ad_click_pointer_v2",
      ad_click_id: pointerId,
      snapshot_id: "adsnap_deep_handoff",
      captured_at: "2026-07-10T00:00:00.000Z",
      primary_click_id_type: "gclid",
      raw_primary_click_id_sha256: await sha256Raw("DEEP-SELECTED-GCLID"),
      owner_anonymous_id_sha256: await sha256Raw(anonId),
      owner_session_id_sha256: await sha256Raw(sessionId),
      ad_click_id_scope: "first_party_scoped",
      ownership_scope: "first_party_owner_bound",
    }));
    let nested = "https://app.eden.health/intake/deepest?deep_business_sentinel=DROP-WITH-UNTRUSTED-SUBTREE&gclid=DEEP-NESTED-GCLID&utm_source=google";
    for (let depth = 0; depth < 9; depth += 1) {
      const wrapper = new URL(`https://app.eden.health/intake/depth-${depth}`);
      wrapper.searchParams.set("keep", String(depth));
      wrapper.searchParams.set("next", nested);
      nested = wrapper.toString();
    }
    const destinationUrl = new URL("https://app.eden.health/intake/deep-handoff");
    destinationUrl.searchParams.set("plan", "weightloss");
    destinationUrl.searchParams.set("next", nested);
    const nowSeconds = Math.floor(Date.now() / 1000);
    const assertion = await signedInternalHandoffFixture(TEST_BROWSER_CAP_HMAC_SECRET, {
      iat: nowSeconds,
      exp: nowSeconds + 300,
      pointerId,
      anonId,
      sessionId,
      destination: destinationUrl.toString(),
      version: 2,
      includeTransportClaims: true,
    });
    destinationUrl.searchParams.set("amp;Eden_Attr_Handoff", assertion);
    const ctx = makeCtx();
    const response = await worker.fetch(new Request(destinationUrl, {
      headers: {
        "User-Agent": "Mozilla/5.0",
        Cookie: `eden_anonymous_id=${anonId}; eden_session_id=${sessionId}; __Secure-eden_ad_click_id=${pointerId}`,
      },
    }), { EDEN_AD_CLICK_MEMORY_MODE: "cookie", AD_CLICK_KV: kv, AD_CLICK_SNAPSHOT_QUEUE: new MockQueue() }, ctx);
    await response.text();
    await Promise.all(ctx.promises);
    const decodeRepeatedly = (value) => {
      let out = String(value || "");
      for (let index = 0; index < 20; index += 1) {
        try {
          const decoded = decodeURIComponent(out);
          if (decoded === out) break;
          out = decoded;
        } catch { break; }
      }
      return out;
    };
    const assertTransportGone = (label, value) => {
      const decoded = decodeRepeatedly(value);
      assert.equal(/gclid|utm_source|eden_attr_handoff|DEEP-NESTED-GCLID/i.test(decoded), false, `${label} must contain no deep attribution transport`);
      assert.equal(decoded.includes("DROP-WITH-UNTRUSTED-SUBTREE"), false, `${label} must drop the over-depth untrusted subtree rather than preserve it unsanitized`);
    };
    assertTransportGone("HealthOS origin URL", originRequestUrl);
    assert.equal(new URL(originRequestUrl).searchParams.get("plan"), "weightloss", "top-level business state must survive fail-closed cleanup");
    const cleanupMatch = [...injectedScripts.matchAll(/<script>([\s\S]*?)<\/script>/g)]
      .find((match) => match[1].includes("normalizedTransportKey") && match[1].includes("history.replaceState"));
    assert.ok(cleanupMatch, "the dedicated browser-visible handoff cleanup script must be injected");
    let replacedUrl = null;
    new Function("window", "document", "URL", cleanupMatch[1])(
      {
        location: { href: destinationUrl.toString() },
        history: { state: null, replaceState(_state, _title, value) { replacedUrl = String(value); } },
      },
      { title: "fixture" },
      URL,
    );
    assert.ok(replacedUrl);
    assertTransportGone("browser-visible URL", replacedUrl);
    assert.equal(new URL(replacedUrl, "https://app.eden.health").searchParams.get("plan"), "weightloss");
  } finally {
    globalThis.fetch = originalFetch;
    if (originalHtmlRewriter === undefined) delete globalThis.HTMLRewriter;
    else globalThis.HTMLRewriter = originalHtmlRewriter;
  }
}

await deepSignedHandoffTransportFailsClosed();

async function validBrowserCapabilityStillCannotAssertStableIdentity() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const nowSeconds = Math.floor(Date.now() / 1000);
    const anonId = "capability-owner-anon";
    const sessionId = "capability-owner-session_1780000000000";
    const capability = await signedBrowserCapabilityFixture(TEST_BROWSER_CAP_HMAC_SECRET, { iat: nowSeconds, exp: nowSeconds + 300, anonId, sessionId });
    const stableClaims = [
      "CAPABILITY-CLAIMED-USER",
      "CAPABILITY-CLAIMED-ORDER",
      "CAPABILITY-CLAIMED-EMAIL",
      "CAPABILITY-CLAIMED-PHONE",
      "CAPABILITY-CLAIMED-EXTERNAL-ID",
      "CAPABILITY-CLAIMED-EDEN-IDENTITY-ID",
      "CAPABILITY-CLAIMED-ACCOUNT-ID",
    ];
    const cookies = `__Secure-eden_browser_cap=${capability}; eden_anonymous_id=${anonId}; eden_session_id=${sessionId}`;
    const kv = new MockKV();
    const queue = new MockQueue();
    const env = {
      EDEN_BROWSER_CAP_ENFORCEMENT_MODE: "enforce",
      BROWSER_CAP_HMAC_SECRET: TEST_BROWSER_CAP_HMAC_SECRET,
      EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      GCLID_KV: kv,
      AD_CLICK_KV: kv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
      SEGMENT_WRITE_KEY: "test_write_key",
    };
    const deeplyNestedAllowedEvent = { keep: "allowed-business-state" };
    let allowedCursor = deeplyNestedAllowedEvent;
    for (let depth = 0; depth < 20; depth += 1) {
      allowedCursor.child = { keep: depth };
      allowedCursor = allowedCursor.child;
    }
    allowedCursor.destination = "https://app.eden.health/intake?gclid=ALLOWED-DEEP-PERSISTENCE-GCLID&utm_source=google";
    const claimedPayload = {
      userId: stableClaims[0],
      properties: {
        order_id: stableClaims[1],
        email: stableClaims[2],
        nested: {
          phone: stableClaims[3],
          customer_id: stableClaims[0],
          externalId: stableClaims[4],
          edenIdentityId: stableClaims[5],
          accountIds: [stableClaims[6]],
        },
        deeply_nested_allowed_event: deeplyNestedAllowedEvent,
      },
      traits: { email: stableClaims[2], phone: stableClaims[3] },
    };
    const identifyCtx = makeCtx();
    const identify = await worker.fetch(new Request("https://collect.eden.health/identify", {
      method: "POST",
      headers: { "Content-Type": "application/json", Origin: "https://app.eden.health", Cookie: cookies },
      body: JSON.stringify({ type: "identify", anonymousId: anonId, ...claimedPayload }),
    }), env, identifyCtx);
    const identifyResult = await identify.json();
    await Promise.all(identifyCtx.promises);
    assert.equal(identify.status, 200);
    assert.equal(identifyResult.stable_identity_accepted, false);

    const collectCtx = makeCtx();
    const collect = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", Origin: "https://app.eden.health", Cookie: cookies },
      body: JSON.stringify({
        type: "track",
        event: "capability_stable_claim_probe",
        anonymousId: anonId,
        ...claimedPayload,
        context: { page: { url: "https://app.eden.health/intake?gclid=CAPABILITY-NATIVE-GCLID" } },
      }),
    }), env, collectCtx);
    await collect.json();
    await Promise.all(collectCtx.promises);
    assert.equal(collect.status, 200);
    const pointerId = readCookieFromSetCookie(collect.headers, "__Secure-eden_ad_click_id");
    assert.ok(pointerId);

    const marketingHostCapability = await signedBrowserCapabilityFixture(TEST_BROWSER_CAP_HMAC_SECRET, {
      iat: nowSeconds,
      exp: nowSeconds + 300,
      anonId,
      sessionId,
      browserHost: "www.eden.health",
    });
    const preserveCtx = makeCtx();
    const preserve = await worker.fetch(new Request("https://collect.eden.health/preserve-attribution", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://www.eden.health",
        Cookie: `__Secure-eden_browser_cap=${marketingHostCapability}; eden_anonymous_id=${anonId}; eden_session_id=${sessionId}; __Secure-eden_ad_click_id=${pointerId}`,
      },
      body: JSON.stringify({
        ...claimedPayload,
        pageUrl: "https://www.eden.health/?gclid=CAPABILITY-NATIVE-GCLID",
        handoffDestination: "https://app.eden.health/intake",
      }),
    }), env, preserveCtx);
    await preserve.json();
    await Promise.all(preserveCtx.promises);
    assert.equal(preserve.status, 200);

    assert.equal(segmentCalls.some((call) => call.type === "identify" || call.type === "alias"), false);
    const browserTrack = segmentCalls.find((call) => call.event === "capability_stable_claim_probe");
    assert.ok(browserTrack);
    assert.equal(JSON.stringify(browserTrack).includes("ALLOWED-DEEP-PERSISTENCE-GCLID"), false, "deep allowed-event URLs must still be sanitized before Segment persistence");
    const persisted = `${JSON.stringify(browserTrack)}\n${JSON.stringify(queue.messages)}\n${JSON.stringify(kv.putCalls)}`;
    for (const claim of stableClaims) assert.equal(persisted.includes(claim), false, `browser capability must not authorize stable claim ${claim}`);
    const forbiddenLink = queue.messages.some((message) => message.payload.identity_links?.some((link) =>
      [link.from_type, link.to_type].some((type) => ["user_id_sha256", "order_id_sha256", "email_sha256", "phone_sha256"].includes(type))
    ));
    assert.equal(forbiddenLink, false, "browser-capability traffic may link anonymous/session/ad-click only");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await validBrowserCapabilityStillCannotAssertStableIdentity();

async function legacyBrowserIdentityKvCannotEnrichAuthenticatedPurchase() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const kv = new MockKV();
    const attackerAnon = "legacy-attacker-anon";
    const victimUser = "legacy-victim-user";
    const victimOrder = "legacy-victim-order";
    await kv.put(`id:link:${victimUser}`, JSON.stringify({ anonId: attackerAnon }));
    await kv.put(`attr:anon:${attackerAnon}`, JSON.stringify({ gclid: "LEGACY-ATTACKER-GCLID", utm_source: "google" }));
    await kv.put(`attr:user:${victimUser}`, JSON.stringify({ gclid: "LEGACY-USER-POISON-GCLID" }));
    await kv.put(`attr:order:${victimOrder}`, JSON.stringify({ gclid: "LEGACY-ORDER-POISON-GCLID" }));
    await kv.put(`email:user:${victimUser}`, "LEGACY-POISON-EMAIL-SHA256");
    kv.getKeys = [];
    const env = {
      SEGMENT_WRITE_KEY: "test_write_key",
      SERVER_API_SECRET: "legacy-poison-server-secret",
      GCLID_KV: kv,
      EDEN_AD_CLICK_MEMORY_MODE: "off",
    };
    const poisonedCtx = makeCtx();
    const poisonedResponse = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "legacy-poison-server-secret" },
      body: JSON.stringify({ type: "track", event: "OS_purchase", userId: victimUser, properties: { order_id: victimOrder, transaction_id: `charge-${victimOrder}` } }),
    }), env, poisonedCtx);
    await poisonedResponse.json();
    await Promise.all(poisonedCtx.promises);
    assert.equal(poisonedResponse.status, 200);
    const poisonedPurchase = segmentCalls.at(-1);
    const serializedPoisoned = JSON.stringify(poisonedPurchase);
    for (const forbidden of ["LEGACY-ATTACKER-GCLID", "LEGACY-USER-POISON-GCLID", "LEGACY-ORDER-POISON-GCLID", "LEGACY-POISON-EMAIL-SHA256", attackerAnon]) {
      assert.equal(serializedPoisoned.includes(forbidden), false, `authenticated purchase must ignore ${forbidden}`);
    }
    assert.equal(
      kv.getKeys.some((key) => key.startsWith("id:link:") || key.startsWith("attr:user:") || key.startsWith("attr:order:") || key.startsWith("email:user:")),
      false,
      "server purchase must never read a legacy browser-derived stable-identity namespace",
    );

    const trustedUser = "trusted-server-user";
    await kv.put(`attr:server:v1:user:source:user_id:${trustedUser}`, JSON.stringify({ gclid: "TRUSTED-SERVER-GCLID", utm_source: "google", stored_at: new Date().toISOString() }));
    const trustedCtx = makeCtx();
    const trustedResponse = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "legacy-poison-server-secret" },
      body: JSON.stringify({ type: "track", event: "OS_purchase", userId: trustedUser, properties: { order_id: "trusted-server-order", transaction_id: "trusted-server-charge" } }),
    }), env, trustedCtx);
    await trustedResponse.json();
    await Promise.all(trustedCtx.promises);
    assert.equal(trustedResponse.status, 200);
    const trustedPurchase = segmentCalls.at(-1);
    assert.equal(trustedPurchase.properties.gclid, "TRUSTED-SERVER-GCLID", "new authenticated server namespace must remain a valid continuity source");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await legacyBrowserIdentityKvCannotEnrichAuthenticatedPurchase();

async function browserCollectorRejectsIdentityTypesAndSchemaDropsPiiAliases() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const nowSeconds = Math.floor(Date.now() / 1000);
    const capability = await signedBrowserCapabilityFixture(TEST_BROWSER_CAP_HMAC_SECRET, {
      iat: nowSeconds,
      exp: nowSeconds + 300,
      anonId: "schema-owner-anon",
      sessionId: "schema-owner-session_1780000000000",
    });
    const cookies = `__Secure-eden_browser_cap=${capability}; eden_anonymous_id=schema-owner-anon; eden_session_id=schema-owner-session_1780000000000`;
    const env = {
      EDEN_BROWSER_CAP_ENFORCEMENT_MODE: "enforce",
      BROWSER_CAP_HMAC_SECRET: TEST_BROWSER_CAP_HMAC_SECRET,
      SEGMENT_WRITE_KEY: "test_write_key",
      GCLID_KV: new MockKV(),
    };
    const identifyCtx = makeCtx();
    const identifyResponse = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", Origin: "https://app.eden.health", Cookie: cookies },
      body: JSON.stringify({ type: "identify", traits: { contact_email: "victim@example.test" } }),
    }), env, identifyCtx);
    const identifyBody = await identifyResponse.json();
    await Promise.all(identifyCtx.promises);
    assert.equal(identifyResponse.status, 422);
    assert.equal(identifyBody.error, "browser_identify_message_not_authorized");

    const trackCtx = makeCtx();
    const trackResponse = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", Origin: "https://app.eden.health", Cookie: cookies },
      body: JSON.stringify({
        type: "track",
        event: "safe_browser_behavior_probe",
        properties: {
          label: "safe-label",
          externalUserId: "PII-EXTERNAL-USER",
          contactId: "PII-CONTACT-ID",
          contact_email: "victim@example.test",
          given_name: "Victim",
          shippingAddress: { line1: "PII-STREET" },
          billing_address: { street_address: "PII-BILLING" },
        },
        traits: { email: "victim@example.test" },
        context: { page: { url: "https://app.eden.health/intake" }, traits: { contact_email: "victim@example.test" } },
      }),
    }), env, trackCtx);
    await trackResponse.json();
    await Promise.all(trackCtx.promises);
    assert.equal(trackResponse.status, 200);
    const track = segmentCalls.find((call) => call.event === "safe_browser_behavior_probe");
    assert.ok(track);
    assert.equal(track.properties.label, "safe-label");
    const serialized = JSON.stringify(track);
    for (const forbidden of ["PII-EXTERNAL-USER", "PII-CONTACT-ID", "victim@example.test", "Victim", "PII-STREET", "PII-BILLING"]) {
      assert.equal(serialized.includes(forbidden), false, `browser schema must drop ${forbidden}`);
    }
    assert.equal(segmentCalls.some((call) => call.type === "identify"), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await browserCollectorRejectsIdentityTypesAndSchemaDropsPiiAliases();

async function collectorFreshBraidsOutrankOlderCampaignGclid() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200 });
  try {
    for (const braidType of ["gbraid", "wbraid"]) {
      const adClickKv = new MockKV();
      const queue = new MockQueue();
      const value = `COLLECTOR-FRESH-${braidType.toUpperCase()}`;
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/collect", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Origin: "https://app.eden.health",
          Cookie: `eden_anonymous_id=collector-${braidType}-anon; eden_session_id=collector-${braidType}-session_1780000000000`,
        },
        body: JSON.stringify({
          type: "track",
          event: "fresh_braid_collector_probe",
          properties: {},
          context: {
            page: { url: `https://app.eden.health/intake?${braidType}=${value}&utm_source=google&utm_medium=cpc` },
            campaign: { gclid: "OLDER-CONTEXT-GCLID", utm_source: "google", utm_medium: "cpc" },
          },
        }),
      }), {
        EDEN_HEALTH_TRACKING_ENRICHMENT_MODE: "all",
        EDEN_AD_CLICK_MEMORY_MODE: "cookie",
        EDEN_AD_CLICK_KV_INDEX_MODE: "pointer",
        AD_CLICK_KV: adClickKv,
        AD_CLICK_SNAPSHOT_QUEUE: queue,
      }, ctx);
      await response.json();
      await Promise.all(ctx.promises);
      assert.equal(response.status, 200);
      const envelope = queue.messages.find((message) => message.payload.event_type === "ad_click_snapshot")?.payload;
      assert.ok(envelope);
      assert.equal(envelope.snapshot.evidence.primary_click_id_type, braidType);
      assert.equal(envelope.snapshot.google[braidType], value);
      assert.equal(envelope.snapshot.google.gclid, undefined, "older campaign GCLID must not suppress fresh braid evidence on /collect");
    }
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await collectorFreshBraidsOutrankOlderCampaignGclid();

async function consentObjectAliasesAllDenyAdvertising() {
  const originalFetch = globalThis.fetch;
  try {
    for (const alias of ["Google_Ads", "amp;google_ads", "%67oogle_ads"]) {
      const segmentCalls = [];
      globalThis.fetch = async (url, init = {}) => {
        if (String(url).startsWith("https://api.segment.io/")) {
          segmentCalls.push(JSON.parse(init.body));
          return new Response("{}", { status: 200 });
        }
        return new Response("ok", { status: 200 });
      };
      const queue = new MockQueue();
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/collect", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Origin: "https://app.eden.health",
          Cookie: `eden_anonymous_id=consent-${encodeURIComponent(alias)}-anon; eden_session_id=consent-alias-session_1780000000000`,
        },
        body: JSON.stringify({
          type: "track",
          event: "consent_alias_probe",
          properties: { consent_state: { [alias]: "denied", action_taken: true } },
          context: { page: { url: `https://app.eden.health/intake?gclid=DENIED-${encodeURIComponent(alias)}-GCLID&utm_source=google` } },
        }),
      }), {
        SEGMENT_WRITE_KEY: "test_write_key",
        EDEN_AD_CLICK_MEMORY_MODE: "all",
        AD_CLICK_KV: new MockKV(),
        AD_CLICK_SNAPSHOT_QUEUE: queue,
        PRIVACY_LEDGER_KV: new MockKV(),
      }, ctx);
      await response.json();
      await Promise.all(ctx.promises);
      assert.equal(response.status, 200);
      assert.equal(queue.messages.length, 0, `${alias} must suppress queue writes`);
      assert.ok(getSetCookie(response.headers).some((cookie) => cookie.startsWith("__Secure-eden_ads_denied=1")));
      const serialized = JSON.stringify(segmentCalls.at(-1));
      assert.equal(serialized.includes("GCLID"), false, `${alias} must scrub click evidence`);
      assert.equal(segmentCalls.at(-1).properties.attribution_suppressed, true);
    }
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await consentObjectAliasesAllDenyAdvertising();

async function deniedDeepNestedUrlCannotLeakClickEvidence() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    let nested = "https://app.eden.health/intake/final?gclid=FIVE-LAYER-DENIED-GCLID&utm_source=google";
    for (let depth = 0; depth < 6; depth += 1) {
      const wrapper = new URL(`https://app.eden.health/intake/layer-${depth}`);
      wrapper.searchParams.set("next", nested);
      nested = wrapper.toString();
    }
    const ctx = makeCtx();
    const response = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://app.eden.health",
        Cookie: "eden_anonymous_id=deep-denied-anon; eden_session_id=deep-denied-session_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "deep_denied_url_probe",
        properties: { consent_state: { ads: "denied", action_taken: true } },
        context: { page: { url: nested } },
      }),
    }), {
      SEGMENT_WRITE_KEY: "test_write_key",
      PRIVACY_LEDGER_KV: new MockKV(),
      AD_CLICK_KV: new MockKV(),
      AD_CLICK_SNAPSHOT_QUEUE: new MockQueue(),
      EDEN_AD_CLICK_MEMORY_MODE: "all",
    }, ctx);
    await response.json();
    await Promise.all(ctx.promises);
    assert.equal(response.status, 200);
    const serialized = JSON.stringify(segmentCalls.at(-1));
    assert.equal(serialized.includes("FIVE-LAYER-DENIED-GCLID"), false);
    assert.equal(serialized.toLowerCase().includes("gclid"), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await deniedDeepNestedUrlCannotLeakClickEvidence();

async function revokedPointerCannotBeResurrectedAcrossRepeatedAllowCycles() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const adClickKv = new MockKV();
    const privacyKv = new MockKV();
    const env = {
      EDEN_AD_CLICK_MEMORY_MODE: "cookie",
      EDEN_AD_CLICK_KV_INDEX_MODE: "pointer",
      AD_CLICK_KV: adClickKv,
      AD_CLICK_SNAPSHOT_QUEUE: new MockQueue(),
      PRIVACY_LEDGER_KV: privacyKv,
      BROWSER_CAP_HMAC_SECRET: TEST_BROWSER_CAP_HMAC_SECRET,
    };
    const ownerCookies = "eden_anonymous_id=revocation-owner-anon; eden_session_id=revocation-owner-session_1780000000000";
    const landingUrl = "https://www.eden.health/?gclid=REVOCATION-CYCLE-GCLID&utm_source=google&utm_medium=cpc";
    const land = async (pointer = null) => {
      const beforeKeys = new Set([...adClickKv.map.keys()].filter((key) => key.startsWith("adclick:id:")));
      const ctx = makeCtx();
      const response = await worker.fetch(new Request(landingUrl, {
        headers: { "User-Agent": "Mozilla/5.0", Cookie: `${ownerCookies}${pointer ? `; __Secure-eden_ad_click_id=${pointer}` : ""}` },
      }), env, ctx);
      await response.text();
      await Promise.all(ctx.promises);
      const cookiePointer = readCookieFromSetCookie(response.headers, "__Secure-eden_ad_click_id");
      const newKey = [...adClickKv.map.keys()].find((key) => key.startsWith("adclick:id:") && !beforeKeys.has(key));
      return cookiePointer || (newKey ? newKey.slice("adclick:id:".length) : null);
    };
    const consent = async (pointer, state, marker = false) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/collect", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Origin: "https://app.eden.health",
          Cookie: `${ownerCookies}; __Secure-eden_ad_click_id=${pointer}${marker ? "; __Secure-eden_ads_denied=1" : ""}`,
        },
        body: JSON.stringify({ type: "track", event: "consent_cycle_probe", properties: { consent_state: state }, context: { page: { url: "https://app.eden.health/intake" } } }),
      }), env, ctx);
      await response.json();
      await Promise.all(ctx.promises);
      assert.equal(response.status, 200);
    };
    const denied = { ads: "denied", action_taken: true };
    const allowed = {
      consent_status: "explicit_allowed", source: "cookieyes", action_taken: true,
      ads: "allowed", google_ads: "allowed", advertising: "allowed", ad_tracking: "allowed",
      partner_ad_tracking: "allowed", retargeting: "allowed", sale_share_targeted_ads: "allowed",
    };
    const first = await land();
    assert.ok(first);
    await consent(first, denied);
    assert.ok(JSON.parse(await adClickKv.get(`adclick:id:${first}`)).revoked_at);
    await consent(first, allowed, true);
    const second = await land(first);
    assert.ok(second && second !== first);
    await consent(second, denied);
    assert.ok(JSON.parse(await adClickKv.get(`adclick:id:${second}`)).revoked_at);
    await consent(second, allowed, true);
    const third = await land(second);
    assert.ok(third && third !== first && third !== second, "second recapture cycle must mint a new generation rather than overwrite a revoked deterministic conflict id");
    assert.ok(JSON.parse(await adClickKv.get(`adclick:id:${first}`)).revoked_at, "first revoked record remains immutable");
    assert.ok(JSON.parse(await adClickKv.get(`adclick:id:${second}`)).revoked_at, "second revoked record remains immutable");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await revokedPointerCannotBeResurrectedAcrossRepeatedAllowCycles();

async function malformedCookiesAreTotalAndHandoffTransportIsNotForwarded() {
  const originalFetch = globalThis.fetch;
  let originRequest = null;
  globalThis.fetch = async (input) => {
    originRequest = input instanceof Request ? input : new Request(String(input));
    return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  };
  try {
    const ctx = makeCtx();
    const response = await worker.fetch(new Request("https://app.eden.health/intake?eden_attr_handoff=opaque-malformed-token&plan=weightloss", {
      headers: {
        "User-Agent": "Mozilla/5.0",
        Cookie: "eden_anonymous_id=%; eden_anon_id=%E0%A4%A; eden_session_id=%; keep_origin_cookie=keep; __Secure-eden_browser_cap=%; __Secure-eden_internal_handoff=opaque-cookie; __Secure-eden_ad_click_id=%252",
        "X-Eden-Server-Secret": "edge-only-server-secret",
        "X-Eden-Tracking-Enrichment-Canary": "edge-only-canary",
        "X-Eden-Internal-Handoff": "edge-only-handoff",
      },
    }), {
      PRIVACY_LEDGER_KV: new MockKV(),
      BROWSER_CAP_HMAC_SECRET: TEST_BROWSER_CAP_HMAC_SECRET,
    }, ctx);
    await response.text();
    await Promise.all(ctx.promises);
    assert.equal(response.status, 200);
    assert.ok(originRequest);
    const origin = new URL(originRequest.url);
    assert.equal(origin.searchParams.has("eden_attr_handoff"), false, "malformed cookies cannot cause opaque handoff transport to reach origin");
    assert.equal(origin.searchParams.get("plan"), "weightloss");
    const originCookie = originRequest.headers.get("Cookie") || "";
    assert.equal(originCookie.includes("__Secure-eden_browser_cap"), false, "browser capability credentials are edge-only");
    assert.equal(originCookie.includes("__Secure-eden_internal_handoff"), false, "handoff credentials are edge-only");
    assert.equal(originCookie.includes("keep_origin_cookie=keep"), true, "unrelated application cookies must survive origin sanitization");
    assert.equal(originRequest.headers.has("X-Eden-Server-Secret"), false);
    assert.equal(originRequest.headers.has("X-Eden-Tracking-Enrichment-Canary"), false);
    assert.equal(originRequest.headers.has("X-Eden-Internal-Handoff"), false);
    assert.ok(getSetCookie(response.headers).some((cookie) => cookie.startsWith("eden_anonymous_id=")), "malformed owner cookie is replaced safely");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await malformedCookiesAreTotalAndHandoffTransportIsNotForwarded();

async function browserTrackEventsAreDefaultAllowAndPreserveProducerSemantics() {
  const headers = {
    "Content-Type": "application/json",
    Origin: "https://app.eden.health",
    Cookie: "eden_anonymous_id=event-default-allow-anon; eden_session_id=event-default-allow-session_1780000000000",
  };
  for (const event of [
    "order-completed", "orderCompleted", "payment_succeeded", "paymentSucceeded",
    "payment_authorized", "payment_failed", "payment_succeeded_clicked", "order_paid_viewed",
    "order_approved", "first_order", "customer_acquired",
    "lead_qualified", "conversion_completed",
  ]) {
    const response = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers,
      body: JSON.stringify({ type: "track", event, properties: {} }),
    }), {}, makeCtx());
    assert.equal(response.status, 200, `${event} must enter the bounded browser telemetry lane`);
  }

  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const ctx = makeCtx();
    const newBehavior = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers,
      body: JSON.stringify({
        type: "track",
        event: "pricing_comparison_opened",
        properties: {
          comparison_surface: "weight_loss_pricing",
          competitor_count: 4,
          interaction_context: { placement: "hero", is_return_visit: true },
          gclid: "undefined",
          email: "must-not-pass@example.com",
          access_token: "must-not-pass",
          validThru: "12/30",
          misc: { validThru: "12/30", placement: "checkout" },
          payment_display: {
            method: "card",
            card: { validThru: "12/30", expirationDate: "12/30" },
          },
          bank_display: { provider: "bank", iban: "must-not-pass", swift: "must-not-pass" },
          device: {
            type: "mobile",
            adid: "must-not-pass",
            aaid: "must-not-pass",
            idfv: "must-not-pass",
            advertisingIdentifier: "must-not-pass",
            uuid: "must-not-pass",
          },
          geo: { country: "US", latitude: 39.7392, coordinates: [-104.9903, 39.7392], geohash: "must-not-pass" },
        },
        context: {
          library: { name: "analytics.js", version: "next" },
          device: {
            type: "mobile",
            advertising_id: "must-not-pass",
            adid: "must-not-pass",
            aaid: "must-not-pass",
            idfv: "must-not-pass",
            advertisingIdentifier: "must-not-pass",
            uuid: "must-not-pass",
            id: "must-not-pass",
            platform: "ios",
          },
          credentials: { token: "must-not-pass" },
          location: {
            latitude: 39.7392,
            longitude: -104.9903,
            coordinates: [-104.9903, 39.7392],
            geohash: "must-not-pass",
            country: "US",
          },
          geo: { lat: 39.7392, lng: -104.9903, country_code: "US" },
        },
      }),
    }), { SEGMENT_WRITE_KEY: "fixture" }, ctx);
    assert.equal(newBehavior.status, 200, "a new bounded event must be captured without a Worker allowlist release");
    await Promise.all(ctx.promises);
    assert.equal(segmentCalls.length, 1);
    assert.equal(segmentCalls[0].event, "pricing_comparison_opened");
    assert.equal(segmentCalls[0].properties.browser_original_event_name, undefined, "Segment owns event registration; Worker does not add a shadow schema name");
    assert.equal(segmentCalls[0].properties.browser_event_schema_status, undefined, "Segment owns schema registration status");
    assert.equal(segmentCalls[0].properties.comparison_surface, "weight_loss_pricing");
    assert.equal(segmentCalls[0].properties.competitor_count, 4);
    assert.deepEqual(segmentCalls[0].properties.interaction_context, { placement: "hero", is_return_visit: true });
    assert.deepEqual(segmentCalls[0].context.library, { name: "analytics.js", version: "next" }, "bounded SDK context must survive");
    assert.deepEqual(segmentCalls[0].context.device, { type: "mobile", platform: "ios" }, "useful bounded device context survives without stable device identifiers");
    assert.equal(segmentCalls[0].context.credentials, undefined, "browser credentials never reach Segment");
    assert.deepEqual(segmentCalls[0].context.location, { country: "US" }, "coarse location survives while precise coordinates are removed");
    assert.deepEqual(segmentCalls[0].context.geo, { country_code: "US" }, "alternate geo paths retain only coarse location");
    assert.deepEqual(segmentCalls[0].properties.payment_display, { method: "card" }, "card expiry aliases are removed from nested payment display data");
    assert.equal(segmentCalls[0].properties.validThru, undefined, "card expiry aliases are removed outside recognized card containers too");
    assert.deepEqual(segmentCalls[0].properties.misc, { placement: "checkout" }, "nested global card-expiry aliases are removed without dropping unrelated telemetry");
    assert.deepEqual(segmentCalls[0].properties.bank_display, { provider: "bank" }, "bank identifiers are removed while non-secret method telemetry survives");
    assert.deepEqual(segmentCalls[0].properties.device, { type: "mobile" }, "device identity aliases are removed from properties");
    assert.deepEqual(segmentCalls[0].properties.geo, { country: "US" }, "precise geo aliases are removed from properties");
    assert.equal(segmentCalls[0].properties.browser_event_authority, "provisional_observation", "every browser event is explicitly non-authoritative");
    assert.equal(segmentCalls[0].properties.email, undefined, "direct browser contact identity remains blocked");
    assert.equal(segmentCalls[0].properties.access_token, undefined, "browser credentials remain blocked");
    assert.equal(segmentCalls[0].properties.gclid, undefined, "invalid browser click claims must not reach Segment or upload surfaces");

    for (const event of ["order_paid", "payment_processed", "checkout_success"]) {
      const eventCtx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/collect", {
        method: "POST",
        headers,
        body: JSON.stringify({
          type: "track",
          event,
          properties: {
            interaction_surface: "qa",
            revenue: 238,
            payment_status: "succeeded",
            conversion_value: 238,
            value: 238,
            price: 238,
            currency: "USD",
            amount: 238,
            total: 238,
            status: "succeeded",
            transaction_status: "succeeded",
            product_context: { product_price: 238, product_currency: "USD" },
          },
        }),
      }), { SEGMENT_WRITE_KEY: "fixture" }, eventCtx);
      assert.equal(response.status, 200, `${event} must reach Segment as browser-observed telemetry`);
      await Promise.all(eventCtx.promises);
      const payload = segmentCalls.at(-1);
      assert.equal(payload.event, event);
      assert.equal(payload.properties.source_type, "client", "downstream reconciliation can distinguish browser observation from server authority");
      assert.equal(payload.properties.browser_event_authority, "provisional_observation", "outcome-like browser events remain explicitly provisional");
      assert.equal(payload.properties.browser_conversion_observation, true, "outcome-like names are tagged without being renamed or blocked");
      assert.equal(payload.properties.revenue, 238);
      assert.equal(payload.properties.payment_status, "succeeded");
      assert.equal(payload.properties.conversion_value, 238);
      assert.deepEqual(payload.properties.product_context, { product_price: 238, product_currency: "USD" });
    }

    const purchaseCtx = makeCtx();
    const purchaseResponse = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers,
      body: JSON.stringify({
        type: "track",
        event: "purchase",
        messageId: "OS_purchase:browser-collision-charge",
        userId: "browser-cannot-assert-user",
        properties: {
          order_id: "browser-cannot-assert-order",
          transaction_id: "browser-cannot-assert-charge",
          charge_id: "browser-cannot-assert-charge-alias",
          payment_id: "browser-cannot-assert-payment",
          stripe_payment_intent_id: "browser-cannot-assert-stripe-payment",
          source_type: "server",
          browser_conversion_observation: false,
          browser_event_authority: "server_authoritative",
          session_id: "browser-cannot-assert-session",
          attribution_snapshot_id: "browser-cannot-assert-snapshot",
          first_touch_gclid: "browser-cannot-assert-first-touch",
          ad_click_id: "browser-cannot-assert-ad-click-object",
          allowed_for_google_click_id_upload: true,
          revenue: 138,
          currency: "USD",
          payment: {
            card: {
              number: 4111111111111111,
              pan: "4111111111111111",
              cvc: 123,
              cvc2: 123,
              cvv2: 123,
              security_code: 123,
              expiration: "12/30",
              exp_month: 12,
              exp_year: 2030,
            },
            charge_id: "browser-cannot-assert-nested-charge",
          },
          auth: { token: "browser-cannot-smuggle-token" },
          credentials: { token: "browser-cannot-smuggle-credential-token" },
          device: { id: "browser-cannot-assert-device", advertisingId: "browser-cannot-assert-ad-id", type: "mobile" },
          profile: { id: "browser-cannot-assert-profile", name: "Jane Patient", email: "must-not-pass@example.com" },
          products: [
            { product_id: "sema", product_name: "Semaglutide", price: 138, quantity: 1 },
            { product_id: "support", product_name: "Care support", price: 0, quantity: 1, card_number: "must-not-pass" },
          ],
        },
        context: {
          anonymousId: "browser-cannot-assert-anonymous-owner",
          library: { name: "analytics.js", version: "next" },
          traits: { name: "Jane Patient", customer_id: "browser-cannot-assert-customer" },
        },
      }),
    }), { SEGMENT_WRITE_KEY: "fixture" }, purchaseCtx);
    assert.equal(purchaseResponse.status, 200);
    await Promise.all(purchaseCtx.promises);
    const purchasePayload = segmentCalls.at(-1);
    assert.equal(purchasePayload.event, "purchase", "browser event names must not be canonicalized into server event names");
    assert.match(segmentSourceMessageId(purchasePayload), /^b-[a-f0-9]{32}$/, "browser transport idempotency must survive under an edge-owned namespace");
    assert.notEqual(segmentSourceMessageId(purchasePayload), "OS_purchase:browser-collision-charge", "a browser message ID cannot collide with an authoritative server message ID");
    assert.match(purchasePayload.properties.browser_producer_message_id_sha256, /^[a-f0-9]{64}$/, "the producer message ID remains available as raw-free reconciliation evidence");
    assert.equal(purchasePayload.properties.browser_message_scope, "eden_anonymous_id");
    assert.equal(purchasePayload.properties.source_type, "client");
    assert.equal(purchasePayload.properties.browser_conversion_observation, true, "Worker marks a browser purchase as a provisional conversion observation");
    assert.equal(purchasePayload.properties.browser_event_authority, "provisional_observation", "browser purchase never becomes server authority");
    assert.equal(purchasePayload.properties.revenue, 138);
    assert.equal(purchasePayload.properties.currency, "USD");
    assert.equal(purchasePayload.properties.order_id, undefined, "browser cannot establish stable order identity");
    assert.equal(purchasePayload.properties.transaction_id, undefined, "browser cannot establish stable payment identity");
    assert.equal(purchasePayload.properties.charge_id, undefined, "browser cannot establish charge identity through an alias");
    assert.equal(purchasePayload.properties.payment_id, undefined, "browser cannot establish payment identity through an alias");
    assert.equal(purchasePayload.properties.stripe_payment_intent_id, undefined, "browser cannot establish processor identity through an alias");
    assert.equal(purchasePayload.properties.payment, undefined, "path-aware scrubbing removes nested card and payment identity fields");
    assert.equal(purchasePayload.properties.auth, undefined, "path-aware scrubbing removes nested credentials");
    assert.equal(purchasePayload.properties.credentials, undefined, "credential aliases are scrubbed recursively");
    assert.deepEqual(purchasePayload.properties.device, { type: "mobile" }, "stable device identifiers are scrubbed while coarse device telemetry survives");
    assert.equal(purchasePayload.properties.profile, undefined, "path-aware scrubbing removes nested profile identity");
    assert.equal(purchasePayload.userId, null, "browser cannot establish stable user identity");
    assert.equal(purchasePayload.properties.source_type, "client", "browser cannot override the edge source classification");
    assert.notEqual(purchasePayload.properties.session_id, "browser-cannot-assert-session", "browser cannot override edge-owned session continuity");
    assert.notEqual(purchasePayload.properties.attribution_snapshot_id, "browser-cannot-assert-snapshot", "browser cannot mint an attribution snapshot");
    assert.notEqual(purchasePayload.properties.first_touch_gclid, "browser-cannot-assert-first-touch", "browser cannot pre-assert derived first-touch state");
    assert.equal(purchasePayload.properties.ad_click_id, undefined, "browser cannot mint an ad-click memory object");
    assert.equal(purchasePayload.properties.allowed_for_google_click_id_upload, undefined, "browser cannot make an upload eligibility decision");
    assert.equal(purchasePayload.context.anonymousId, undefined, "browser cannot override edge-owned anonymous identity in context");
    assert.equal(purchasePayload.context.traits, undefined, "browser cannot smuggle person identity through nested traits");
    assert.deepEqual(purchasePayload.context.library, { name: "analytics.js", version: "next" });
    assert.deepEqual(purchasePayload.properties.products, [
      { product_id: "sema", product_name: "Semaglutide", price: 138, quantity: 1 },
      { product_id: "support", product_name: "Care support", price: 0, quantity: 1 },
    ], "bounded product arrays survive while nested card data is removed");
    assert.match(purchasePayload.properties.mixpanel_insert_id, /^m-[a-f0-9]{32}$/, "Worker derives destination dedupe from the preserved message ID");

    const browserCollisionCtx = makeCtx();
    const browserCollisionResponse = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers,
      body: JSON.stringify({
        type: "track",
        event: "OS_purchase",
        messageId: "OS_purchase:browser-collision-charge",
        properties: { transaction_id: "browser-collision-charge", revenue: 138, currency: "USD" },
      }),
    }), { SEGMENT_WRITE_KEY: "fixture" }, browserCollisionCtx);
    assert.equal(browserCollisionResponse.status, 200);
    await Promise.all(browserCollisionCtx.promises);
    const browserCollisionPayload = segmentCalls.at(-1);
    assert.equal(browserCollisionPayload.event, "OS_purchase");
    assert.match(segmentSourceMessageId(browserCollisionPayload), /^b-[a-f0-9]{32}$/);
    assert.equal(browserCollisionPayload.properties.transaction_id, undefined);
    assert.equal(browserCollisionPayload.properties.browser_conversion_observation, true);
    assert.equal(browserCollisionPayload.properties.browser_event_authority, "provisional_observation");

    const serverCollisionCtx = makeCtx();
    const serverCollisionResponse = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Eden-Server-Secret": TEST_SERVER_API_SECRET,
      },
      body: JSON.stringify({
        type: "track",
        event: "OS_purchase",
        messageId: "OS_purchase:browser-collision-charge",
        anonymousId: "event-default-allow-anon",
        userId: "authoritative-user",
        properties: {
          transaction_id: "browser-collision-charge",
          order_id: "authoritative-order",
          payment_status: "authorized",
          conversion_value: 138,
          currency: "USD",
        },
      }),
    }), {
      SERVER_API_SECRET: TEST_SERVER_API_SECRET,
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: new MockKV(),
      CONVERSION_COORDINATOR: new MockConversionCoordinatorNamespace(),
    }, serverCollisionCtx);
    assert.equal(serverCollisionResponse.status, 200, "the later authoritative server purchase must survive a copied browser message ID");
    await Promise.all(serverCollisionCtx.promises);
    const serverCollisionPayload = segmentCalls.at(-1);
    assert.equal(serverCollisionPayload.event, "OS_purchase");
    assertMixpanelSafeMessageId(serverCollisionPayload, "OS_purchase:browser-collision-charge");
    assert.equal(serverCollisionPayload.properties.transaction_id, "browser-collision-charge");
    assert.notEqual(segmentSourceMessageId(serverCollisionPayload), segmentSourceMessageId(browserCollisionPayload));

    const pageCtx = makeCtx();
    const pageResponse = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers,
      body: JSON.stringify({
        type: "page",
        name: "Weight Loss",
        messageId: "browser-page-signal-123",
        properties: { page_category: "product" },
      }),
    }), { SEGMENT_WRITE_KEY: "fixture" }, pageCtx);
    assert.equal(pageResponse.status, 200);
    await Promise.all(pageCtx.promises);
    const pageCall = segmentCalls.at(-1);
    assert.equal(pageCall.name, "Weight Loss");
    assert.match(segmentSourceMessageId(pageCall), /^b-[a-f0-9]{32}$/, "native Segment page message IDs must survive under the browser namespace");
    assert.match(pageCall.properties.browser_producer_message_id_sha256, /^[a-f0-9]{64}$/);

    const screenCtx = makeCtx();
    const screenResponse = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers,
      body: JSON.stringify({
        type: "screen",
        name: "Checkout Review",
        messageId: "browser-screen-signal-123",
        properties: { screen_stage: "checkout" },
      }),
    }), { SEGMENT_WRITE_KEY: "fixture" }, screenCtx);
    assert.equal(screenResponse.status, 200);
    await Promise.all(screenCtx.promises);
    const screenCall = segmentCalls.at(-1);
    assert.equal(screenCall.name, "Checkout Review");
    assert.match(segmentSourceMessageId(screenCall), /^b-[a-f0-9]{32}$/, "native Segment screen message IDs must survive under the browser namespace");
    assert.match(screenCall.properties.browser_producer_message_id_sha256, /^[a-f0-9]{64}$/);
    assert.equal(screenCall.event, undefined, "browser screen calls must not be renamed into synthetic track events");

    for (const event of [
      "Page Engaged", "Scroll Depth Reached", "Article Read", "Login Clicked",
      "CTA to Intake Clicked", "Nav Menu Clicked", "FAQ Opened",
      "CTA to Product Clicked", "Form Submitted", "health_info",
      "OS_intake_started", "OS_begin_checkout", "purchase_cta_clicked", "OS_purchase",
    ]) {
      const eventCtx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/collect", {
        method: "POST",
        headers,
        body: JSON.stringify({
          type: "track",
          event,
          properties: {
            interaction_surface: "incident-regression",
            price: 138,
            currency: "USD",
            revenue: 999,
            mixpanel_insert_id: "browser-cannot-control-destination-dedupe",
            email: "must-not-pass@example.com",
          },
        }),
      }), { SEGMENT_WRITE_KEY: "fixture" }, eventCtx);
      assert.equal(response.status, 200, `${event} must preserve its original name`);
      await Promise.all(eventCtx.promises);
      const payload = segmentCalls.at(-1);
      assert.equal(payload.event, event);
      assert.equal(payload.properties.browser_original_event_name, undefined);
      assert.equal(payload.properties.browser_event_schema_status, undefined);
      assert.equal(payload.properties.interaction_surface, "incident-regression");
      assert.equal(payload.properties.price, 138);
      assert.equal(payload.properties.currency, "USD");
      assert.equal(payload.properties.revenue, 999, "commercial telemetry reaches Segment for downstream governance and reconciliation");
      assert.equal(payload.properties.mixpanel_insert_id, undefined, "browser cannot choose the destination dedupe key");
      assert.equal(payload.properties.email, undefined, "direct browser contact identity remains stripped");
    }
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await browserTrackEventsAreDefaultAllowAndPreserveProducerSemantics();

async function browserTelemetryPrefixesCannotSmuggleStableIdentity() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const ctx = makeCtx();
    const response = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://app.eden.health",
        Cookie: "eden_anonymous_id=prefix-scrub-anon; eden_session_id=prefix-scrub-session_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "safe_browser_behavior_probe",
        properties: {
          page_user_id: "victim-user",
          product_order_id: "victim-order",
          device_email_sha256: "a".repeat(64),
          screen_customer_id: "victim-customer",
          product_id: "safe-product",
        },
      }),
    }), { SEGMENT_WRITE_KEY: "test_write_key" }, ctx);
    await response.json();
    await Promise.all(ctx.promises);
    assert.equal(response.status, 200);
    const properties = segmentCalls.at(-1).properties;
    assert.equal(properties.page_user_id, undefined);
    assert.equal(properties.product_order_id, undefined);
    assert.equal(properties.device_email_sha256, undefined);
    assert.equal(properties.screen_customer_id, undefined);
    assert.equal(properties.product_id, "safe-product");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await browserTelemetryPrefixesCannotSmuggleStableIdentity();

async function duplicateNestedUrlsCannotHideDeniedClickEvidence() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const page = new URL("https://app.eden.health/intake");
    page.searchParams.append("next", "https://app.eden.health/intake/safe?plan=weightloss");
    page.searchParams.append("next", "https://app.eden.health/intake/poisoned?gclid=DUPLICATE-DENIED-GCLID&utm_source=google");
    const ctx = makeCtx();
    const response = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://app.eden.health",
        Cookie: "eden_anonymous_id=duplicate-url-anon; eden_session_id=duplicate-url-session_1780000000000",
      },
      body: JSON.stringify({
        type: "track",
        event: "deep_denied_url_probe",
        properties: { consent_state: { ads: "denied", action_taken: true } },
        context: { page: { url: page.toString() } },
      }),
    }), { SEGMENT_WRITE_KEY: "test_write_key", PRIVACY_LEDGER_KV: new MockKV() }, ctx);
    await response.json();
    await Promise.all(ctx.promises);
    assert.equal(response.status, 200);
    const serialized = JSON.stringify(segmentCalls.at(-1));
    assert.equal(serialized.includes("DUPLICATE-DENIED-GCLID"), false);
    assert.equal(serialized.toLowerCase().includes("gclid"), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await duplicateNestedUrlsCannotHideDeniedClickEvidence();

async function concurrentPointerRevocationCannotReportKvDurability() {
  class RevocationRaceKV extends MockKV {
    constructor() { super(); this.pointerReads = 0; }
    async get(key) {
      this.getKeys.push(key);
      if (key.startsWith("adclick:id:")) {
        this.pointerReads += 1;
        if (this.pointerReads >= 2) return JSON.stringify({
          schema_version: "eden_ad_click_pointer_v2",
          ad_click_id: key.slice("adclick:id:".length),
          revoked_at: "2026-07-10T00:00:00.000Z",
        });
        return null;
      }
      return this.map.get(key) ?? null;
    }
  }
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200 });
  try {
    const kv = new RevocationRaceKV();
    const queue = new MockQueue();
    const ctx = makeCtx();
    const response = await worker.fetch(new Request("https://collect.eden.health/preserve-attribution", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://www.eden.health",
        Cookie: "eden_anonymous_id=revocation-race-anon; eden_session_id=revocation-race-session_1780000000000",
      },
      body: JSON.stringify({ pageUrl: "https://www.eden.health/?gclid=REVOCATION-RACE-GCLID&utm_source=google" }),
    }), {
      GCLID_KV: kv,
      AD_CLICK_KV: kv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      EDEN_AD_CLICK_KV_INDEX_MODE: "pointer",
    }, ctx);
    await response.text();
    await Promise.allSettled(ctx.promises);
    assert.equal(response.status, 500, "revoked pointer race must fail the durability barrier");
    assert.equal(kv.putKeys.some((key) => key.startsWith("adclick:id:")), false, "revoked pointer is never rewritten");
    assert.equal(queue.messages.length, 0, "a rejected/revoked pointer must publish no Queue envelope");
    assert.equal(queue.messages.flatMap((message) => message.payload?.identity_links || []).length, 0, "a rejected/revoked pointer must emit zero identity links");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await concurrentPointerRevocationCannotReportKvDurability();

async function durablePointerCoordinatorSerializesUpsertBeforeRevocation() {
  const records = new Map();
  const storage = {
    async get(key) { return records.get(key); },
    async put(key, value) { records.set(key, value); },
    async transaction(callback) {
      return callback({
        get: async (key) => records.get(key),
        put: async (key, value) => records.set(key, value),
        delete: async (key) => records.delete(key),
      });
    },
  };
  class SlowFirstPointerPutKV extends MockKV {
    constructor() { super(); this.pointerPutCount = 0; }
    async put(key, value, options = {}) {
      if (key.startsWith("adclick:id:") && this.pointerPutCount++ === 0) {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      return super.put(key, value, options);
    }
  }
  const kv = new SlowFirstPointerPutKV();
  const coordinator = new ConversionCoordinator({ storage }, { AD_CLICK_KV: kv });
  const adClickId = "adclk2_serialized_revocation";
  const anonymousHash = await sha256Raw("serialized-revocation-anon");
  const proposed = {
    schema_version: "eden_ad_click_pointer_v2",
    ad_click_id: adClickId,
    snapshot_id: "adsnap_serialized_revocation",
    captured_at: "2026-07-10T00:00:00.000Z",
    owner_anonymous_id_sha256: anonymousHash,
    ad_click_id_scope: "first_party_scoped",
    ownership_scope: "first_party_owner_bound",
  };
  const upsert = coordinator.fetch(new Request("https://conversion-coordinator.internal/pointer/upsert", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ad_click_id: adClickId, proposed_record: proposed, ttl_seconds: 86400 }),
  }));
  const revoke = coordinator.fetch(new Request("https://conversion-coordinator.internal/pointer/revoke", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      ad_click_id: adClickId,
      owner: { anonymous_id_sha256: anonymousHash },
      revoked_at: "2026-07-10T00:00:01.000Z",
      ttl_seconds: 86400,
    }),
  }));
  const [upsertResponse, revokeResponse] = await Promise.all([upsert, revoke]);
  assert.equal(upsertResponse.status, 200);
  assert.equal(revokeResponse.status, 200);
  const durableRecord = await storage.get("ad_click_pointer");
  const cachedRecord = JSON.parse(await kv.get(`adclick:id:${adClickId}`));
  assert.equal(durableRecord.revoked_at, "2026-07-10T00:00:01.000Z");
  assert.equal(cachedRecord.revoked_at, durableRecord.revoked_at, "a delayed stale upsert cannot overwrite the serialized revocation in KV");
  assert.equal(kv.putCalls.filter((call) => call.key === `adclick:id:${adClickId}`).length, 2);
}

await durablePointerCoordinatorSerializesUpsertBeforeRevocation();

async function durablePointerReadRejectsStaleKvAfterPartialRevocationWrite() {
  const records = new Map();
  const storage = {
    async get(key) { return records.get(key); },
    async put(key, value) { records.set(key, value); },
    async transaction(callback) {
      return callback({
        get: async (key) => records.get(key),
        put: async (key, value) => records.set(key, value),
        delete: async (key) => records.delete(key),
      });
    },
  };
  class FailingPointerCacheKV extends MockKV {
    constructor() { super(); this.failPointerWrites = false; }
    async put(key, value, options = {}) {
      if (this.failPointerWrites && key.startsWith("adclick:id:")) throw new Error("fixture_pointer_cache_write_failed");
      return super.put(key, value, options);
    }
  }
  const kv = new FailingPointerCacheKV();
  const coordinator = new ConversionCoordinator({ storage }, { AD_CLICK_KV: kv });
  const namespace = {
    idFromName(name) { return String(name); },
    get() {
      return {
        fetch(input, init = {}) {
          return coordinator.fetch(input instanceof Request ? input : new Request(input, init));
        },
      };
    },
  };
  const adClickId = "adclk2_partial_revocation_cache";
  const anonymousId = "partial-revocation-anon";
  const sessionId = "partial-revocation-session_1780000000000";
  const anonymousHash = await sha256Raw(anonymousId);
  const sessionHash = await sha256Raw(sessionId);
  const proposed = {
    schema_version: "eden_ad_click_pointer_v2",
    ad_click_id: adClickId,
    snapshot_id: "adsnap_partial_revocation_cache",
    captured_at: "2026-07-10T00:00:00.000Z",
    owner_anonymous_id_sha256: anonymousHash,
    owner_session_id_sha256: sessionHash,
    ad_click_id_scope: "first_party_scoped",
    ownership_scope: "first_party_owner_bound",
  };
  const upsert = await coordinator.fetch(new Request("https://conversion-coordinator.internal/pointer/upsert", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ad_click_id: adClickId, proposed_record: proposed, ttl_seconds: 86400 }),
  }));
  assert.equal(upsert.status, 200);
  kv.failPointerWrites = true;
  const revoke = await coordinator.fetch(new Request("https://conversion-coordinator.internal/pointer/revoke", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      ad_click_id: adClickId,
      owner: { anonymous_id_sha256: anonymousHash },
      revoked_at: "2026-07-10T00:00:01.000Z",
      ttl_seconds: 86400,
    }),
  }));
  assert.equal(revoke.status, 503, "the simulated cache failure must remain visible to the writer");
  const canonicalRecord = await storage.get("ad_click_pointer");
  const staleCacheRecord = JSON.parse(await kv.get(`adclick:id:${adClickId}`));
  assert.equal(canonicalRecord.revoked_at, "2026-07-10T00:00:01.000Z");
  assert.equal(staleCacheRecord.revoked_at, undefined, "the fixture must leave a valid-looking stale KV pointer");
  const canonicalRead = await coordinator.fetch(new Request("https://conversion-coordinator.internal/pointer/read", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ad_click_id: adClickId, seed_record: staleCacheRecord, ttl_seconds: 86400 }),
  }));
  assert.equal(canonicalRead.status, 200, "a failed cache repair cannot hide authoritative revocation state");
  const canonicalReadBody = await canonicalRead.json();
  assert.equal(canonicalReadBody.record.revoked_at, canonicalRecord.revoked_at);
  assert.equal(canonicalReadBody.cache_repair_attempted, true);
  assert.equal(canonicalReadBody.cache_repair_succeeded, false);

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url) => String(url).startsWith("https://api.segment.io/")
    ? new Response("{}", { status: 200 })
    : new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const queue = new MockQueue();
    const ctx = makeCtx();
    const response = await worker.fetch(new Request("https://collect.eden.health/collect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://app.eden.health",
        "Cookie": `eden_anon_id=${anonymousId}; eden_session_id=${sessionId}; __Secure-eden_ad_click_id=${adClickId}`,
      },
      body: JSON.stringify({
        type: "track",
        event: "os_question_answered",
        anonymousId,
        properties: {},
        context: { page: { url: "https://app.eden.health/intake", path: "/intake" } },
      }),
    }), {
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: new MockKV(),
      AD_CLICK_KV: kv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      CONVERSION_COORDINATOR: namespace,
    }, ctx);
    await response.json();
    await Promise.all(ctx.promises);
    assert.equal(response.status, 200);
    assert.equal(queue.messages.length, 0, "the Worker must reject stale non-revoked KV when canonical DO state is revoked");
    assert.equal(getSetCookie(response.headers).some((cookie) => cookie.startsWith("__Secure-eden_ad_click_id=")), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await durablePointerReadRejectsStaleKvAfterPartialRevocationWrite();

async function sourceIdentityNamespacesNeverCollapseTheSameRawId() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const gclidKv = new MockKV();
    const env = {
      SEGMENT_WRITE_KEY: "fixture",
      SERVER_API_SECRET: "source-namespace-server-secret",
      GCLID_KV: gclidKv,
      CONVERSION_COORDINATOR: new MockConversionCoordinatorNamespace(),
    };
    const send = async (namespace, transactionId) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "source-namespace-server-secret" },
        body: JSON.stringify({
          type: "track",
          event: "OS_purchase",
          properties: {
            [namespace]: "shared-raw-123",
            order_id: `order-${namespace}`,
            transaction_id: transactionId,
            payment_status: "authorized",
          },
        }),
      }), env, ctx);
      await response.json();
      await Promise.all(ctx.promises);
      assert.equal(response.status, 200);
    };
    await send("patient_id", "source-namespace-patient-charge");
    await send("customer_id", "source-namespace-customer-charge");
    assert.deepEqual(
      segmentCalls.map((call) => call.userId),
      ["source:patient_id:shared-raw-123", "source:customer_id:shared-raw-123"],
      "equal raw values from distinct source systems must remain distinct typed identities",
    );
    assert.ok(gclidKv.map.has("attr:server:v1:user:source:patient_id:shared-raw-123") === false, "no attribution row is synthesized without attribution evidence");
    assert.notEqual(segmentCalls[0].userId, segmentCalls[1].userId);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await sourceIdentityNamespacesNeverCollapseTheSameRawId();

async function conflictingConversionRetryCannotMutateAcceptedOwnerPrivacyLedger() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url) => String(url).startsWith("https://api.segment.io/")
    ? new Response("{}", { status: 200 })
    : new Response("ok", { status: 200 });
  try {
    const kv = new MockKV();
    const coordinator = new MockConversionCoordinatorNamespace();
    const env = {
      SEGMENT_WRITE_KEY: "fixture",
      SERVER_API_SECRET: "privacy-owner-server-secret",
      GCLID_KV: kv,
      PRIVACY_LEDGER_KV: kv,
      PRIVACY_LEDGER_HMAC_SECRET: TEST_PRIVACY_LEDGER_HMAC_SECRET,
      CONVERSION_COORDINATOR: coordinator,
    };
    const send = async ({ userId, anonymousId, consentState = null, transactionId = "privacy-owner-charge", event = "OS_purchase" }) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "privacy-owner-server-secret" },
        body: JSON.stringify({
          type: "track",
          event,
          anonymousId,
          userId,
          properties: {
            order_id: "privacy-owner-order",
            transaction_id: transactionId,
            payment_status: "authorized",
            ...(consentState ? { consent_state: consentState } : {}),
          },
        }),
      }), env, ctx);
      await response.json();
      await Promise.all(ctx.promises);
      return response;
    };
    await send({
      userId: "privacy-owner-a",
      anonymousId: "privacy-owner-anon-a",
      transactionId: "privacy-owner-denial-seed",
      event: "authenticated_privacy_owner_seed",
      consentState: { ads: "denied", action_taken: true },
    });
    const ownerUserKey = `privacy:ads_denied:v1:user:${await hmacSha256Hex(TEST_PRIVACY_LEDGER_HMAC_SECRET, "advertising-denial:user:v1\0source:user_id:privacy-owner-a")}`;
    assert.ok(kv.map.has(ownerUserKey));
    assert.equal((await send({ userId: "privacy-owner-a", anonymousId: "privacy-owner-anon-a" })).status, 200);
    const keysBeforeConflict = new Set([...kv.map.keys()].filter((key) => key.startsWith("privacy:ads_denied:v1:")));
    const allowConflict = await send({
      userId: "privacy-owner-b",
      anonymousId: "privacy-owner-anon-b",
      consentState: { ads: "granted", action_taken: true },
    });
    assert.equal(allowConflict.status, 200);
    assert.ok(kv.map.has(ownerUserKey), "a conflicting retry cannot clear the accepted owner's denial tombstone");
    const denyConflict = await send({
      userId: "privacy-owner-b",
      anonymousId: "privacy-owner-anon-b",
      consentState: { ads: "denied", action_taken: true },
    });
    assert.equal(denyConflict.status, 200);
    const keysAfterConflict = new Set([...kv.map.keys()].filter((key) => key.startsWith("privacy:ads_denied:v1:")));
    assert.deepEqual(keysAfterConflict, keysBeforeConflict, "a conflicting retry cannot add, clear, or propagate privacy state for either identity set");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await conflictingConversionRetryCannotMutateAcceptedOwnerPrivacyLedger();

async function kvBootstrapConversionOwnerProtectsPrivacyBeforeDurableObjectSeed() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url) => String(url).startsWith("https://api.segment.io/")
    ? new Response("{}", { status: 200 })
    : new Response("ok", { status: 200 });
  try {
    const kv = new MockKV();
    const coordinator = new MockConversionCoordinatorNamespace();
    const env = {
      SEGMENT_WRITE_KEY: "fixture",
      SERVER_API_SECRET: "privacy-kv-bootstrap-secret",
      GCLID_KV: kv,
      PRIVACY_LEDGER_KV: kv,
      PRIVACY_LEDGER_HMAC_SECRET: TEST_PRIVACY_LEDGER_HMAC_SECRET,
      CONVERSION_COORDINATOR: coordinator,
    };
    const send = async ({ event = "OS_purchase", userId, anonymousId, transactionId, consentState = null }) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "privacy-kv-bootstrap-secret" },
        body: JSON.stringify({
          type: "track",
          event,
          anonymousId,
          userId,
          properties: {
            order_id: "privacy-kv-bootstrap-order",
            transaction_id: transactionId,
            payment_status: "authorized",
            ...(consentState ? { consent_state: consentState } : {}),
          },
        }),
      }), env, ctx);
      await response.json();
      await Promise.all(ctx.promises);
      return response;
    };
    await send({
      event: "authenticated_privacy_kv_seed",
      userId: "privacy-kv-owner-a",
      anonymousId: "privacy-kv-anon-a",
      transactionId: "privacy-kv-denial-seed",
      consentState: { ads: "denied", action_taken: true },
    });
    assert.equal((await send({
      userId: "privacy-kv-owner-a",
      anonymousId: "privacy-kv-anon-a",
      transactionId: "privacy-kv-charge",
    })).status, 200);
    assert.ok([...kv.map.keys()].some((key) => key.startsWith("dedup:v4:OS_purchase:")), "the accepted transaction must have a KV migration record");
    coordinator.records.clear();
    coordinator.leases.clear();
    assert.equal(coordinator.records.size, 0, "the regression must exercise an empty conversion Durable Object");
    const privacyKeysBefore = [...kv.map.keys()].filter((key) => key.startsWith("privacy:ads_denied:v1:")).sort();
    const deleteCountBefore = kv.deleteKeys.length;
    const conflictingAllow = await send({
      userId: "privacy-kv-owner-b",
      anonymousId: "privacy-kv-anon-b",
      transactionId: "privacy-kv-charge",
      consentState: { ads: "granted", action_taken: true },
    });
    assert.equal(conflictingAllow.status, 200);
    assert.deepEqual(
      [...kv.map.keys()].filter((key) => key.startsWith("privacy:ads_denied:v1:")).sort(),
      privacyKeysBefore,
      "KV-only accepted owner history must be resolved before an explicit allow can mutate privacy tombstones",
    );
    assert.equal(kv.deleteKeys.length, deleteCountBefore, "the conflicting KV-bootstrap retry cannot delete any privacy key");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await kvBootstrapConversionOwnerProtectsPrivacyBeforeDurableObjectSeed();

async function legacyUnprovenConversionHistoryCannotAuthorizePrivacyMutation() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url) => String(url).startsWith("https://api.segment.io/")
    ? new Response("{}", { status: 200 })
    : new Response("ok", { status: 200 });
  try {
    const kv = new MockKV();
    const env = {
      SEGMENT_WRITE_KEY: "fixture",
      SERVER_API_SECRET: "legacy-privacy-owner-secret",
      GCLID_KV: kv,
      PRIVACY_LEDGER_KV: kv,
      PRIVACY_LEDGER_HMAC_SECRET: TEST_PRIVACY_LEDGER_HMAC_SECRET,
      CONVERSION_COORDINATOR: new MockConversionCoordinatorNamespace(),
    };
    const send = async ({ event, consentState = null }) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "legacy-privacy-owner-secret" },
        body: JSON.stringify({
          type: "track",
          event,
          anonymousId: "legacy-privacy-anon",
          userId: "legacy-privacy-user",
          properties: {
            order_id: "legacy-privacy-order",
            transaction_id: event === "OS_purchase" ? "legacy-privacy-charge" : "legacy-privacy-denial-seed",
            payment_status: "authorized",
            ...(consentState ? { consent_state: consentState } : {}),
          },
        }),
      }), env, ctx);
      await response.json();
      await Promise.all(ctx.promises);
      return response;
    };
    await send({ event: "authenticated_legacy_privacy_seed", consentState: { ads: "denied", action_taken: true } });
    await kv.put("dedup:OS_purchase:legacy-privacy-charge", JSON.stringify({
      event: "OS_purchase",
      order_id: "legacy-privacy-charge",
      attribution_found: true,
      fired_at: "2026-07-10T12:20:00.000Z",
    }), { expirationTtl: 86400 });
    const privacyKeysBefore = [...kv.map.keys()].filter((key) => key.startsWith("privacy:ads_denied:v1:")).sort();
    const deleteCountBefore = kv.deleteKeys.length;
    const explicitAllow = await send({ event: "OS_purchase", consentState: { ads: "granted", action_taken: true } });
    assert.ok([200, 503].includes(explicitAllow.status), "legacy repair outcome does not change privacy ownership safety");
    assert.deepEqual(
      [...kv.map.keys()].filter((key) => key.startsWith("privacy:ads_denied:v1:")).sort(),
      privacyKeysBefore,
      "a namespace-unproven legacy row cannot authorize durable privacy deletion",
    );
    assert.equal(kv.deleteKeys.length, deleteCountBefore);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await legacyUnprovenConversionHistoryCannotAuthorizePrivacyMutation();

async function unknownCommitProductCorrectionReplaysExactBaseThenEnriches() {
  const segmentCalls = [];
  const segmentRawBodies = [];
  let failFirst = true;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentRawBodies.push(String(init.body));
      segmentCalls.push(JSON.parse(init.body));
      if (failFirst) {
        failFirst = false;
        throw new TypeError("fixture_unknown_commit_product_change");
      }
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const env = {
      SEGMENT_WRITE_KEY: "fixture",
      SERVER_API_SECRET: "product-correction-server-secret",
      GCLID_KV: new MockKV(),
      CONVERSION_COORDINATOR: new MockConversionCoordinatorNamespace(),
    };
    const send = async (productId, offerLabel, paymentStatus) => {
      const ctx = makeCtx();
      const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "product-correction-server-secret" },
        body: JSON.stringify({
          type: "track",
          event: "OS_purchase",
          anonymousId: "product-correction-anon",
          userId: "product-correction-user",
          properties: {
            order_id: "product-correction-order",
            transaction_id: "product-correction-charge",
            product_id: productId,
            offer_label: offerLabel,
            payment_status: paymentStatus,
            conversion_value: 138,
            currency: "USD",
          },
        }),
      }), env, ctx);
      const responseBody = await response.json();
      await Promise.all(ctx.promises);
      return { response, responseBody };
    };
    assert.equal((await send("semaglutide", "offer-a", "pending")).response.status, 503);
    const corrected = await send("tirzepatide", "offer-b", "authorized");
    assert.equal(corrected.response.status, 200);
    assert.equal(corrected.responseBody.conversion_enrichment_forwarded, true);
    assert.equal(segmentRawBodies[1], segmentRawBodies[0], "the stable conversion message ID must replay the exact request bytes");
    assert.deepEqual(segmentCalls[1], segmentCalls[0]);
    assert.equal(segmentCalls[2].event, "OS_purchase_enrichment");
    assert.equal(segmentCalls[2].properties.product_id, "tirzepatide");
    assert.equal(segmentCalls[2].properties.offer_label, "offer-b");
    assert.equal(segmentCalls[2].properties.payment_status, "authorized");
    assert.notEqual(segmentCalls[2].messageId, segmentCalls[0].messageId);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await unknownCommitProductCorrectionReplaysExactBaseThenEnriches();

async function clickIdPlausibilityAndNestedExtractionAreBounded() {
  const originalFetch = globalThis.fetch;
  const originalWarn = console.warn;
  const warnings = [];
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  console.warn = (...args) => warnings.push(args.map(String).join(" "));
  try {
    const capture = async (url) => {
      const queue = new MockQueue();
      const ctx = makeCtx();
      const response = await worker.fetch(new Request(url, { headers: { "User-Agent": "Mozilla/5.0" } }), {
        EDEN_AD_CLICK_MEMORY_MODE: "cookie",
        AD_CLICK_KV: new MockKV(),
        AD_CLICK_SNAPSHOT_QUEUE: queue,
      }, ctx);
      await response.text();
      await Promise.all(ctx.promises);
      return queue.messages.map((message) => message.payload).filter((payload) => payload.event_type === "ad_click_snapshot");
    };
    for (const [label, url] of [
      ["sentinel", "https://www.eden.health/?gclid=undefined"],
      ["oversized", `https://www.eden.health/?gclid=${"X".repeat(5000)}`],
      ["short contaminated", "https://www.eden.health/?gclid=G;utm_source=google"],
      ["conflicting repeated", "https://www.eden.health/?gclid=VALID-GCLID-ONE&gclid=VALID-GCLID-TWO"],
    ]) {
      assert.equal((await capture(url)).length, 0, `${label} click evidence must remain diagnostic and never become upload-grade`);
    }
    const semicolon = await capture("https://www.eden.health/?gclid=VALID-GCLID-12345;utm_source=google;utm_medium=cpc");
    assert.equal(semicolon.length, 1);
    assert.equal(semicolon[0].snapshot.google.gclid, "VALID-GCLID-12345", "recognized semicolon tail must not contaminate the click ID value");
    assert.equal(semicolon[0].snapshot.campaign.utm_source, "google");
    const deepest = encodeURIComponent("https://www.eden.health/?gclid=DOUBLE-NESTED-GCLID-12345&utm_source=google");
    const inner = encodeURIComponent(`https://app.eden.health/intake?redirect_url=${deepest}`);
    const nested = await capture(`https://www.eden.health/?redirect_url=${inner}`);
    assert.equal(nested.length, 1, "bounded recursive nested URL extraction must recover a double-nested click ID");
    assert.equal(nested[0].snapshot.google.gclid, "DOUBLE-NESTED-GCLID-12345");
    const rejectionDiagnostics = warnings
      .filter((entry) => entry.includes('"event":"google_click_evidence_rejected"'))
      .map((entry) => {
        try { return JSON.parse(entry); } catch { return null; }
      })
      .filter(Boolean);
    assert.ok(
      rejectionDiagnostics.some((entry) => entry.source_type === "landing_url" && entry.rejected?.some((item) => item.field === "gclid" && item.reason === "sentinel_value")),
      "invalid public landing IDs must emit a raw-free field/reason diagnostic",
    );
    assert.ok(
      rejectionDiagnostics.some((entry) => entry.rejected?.some((item) => item.field === "gclid" && item.reason === "conflicting_repeats")),
      "conflicting repeated landing IDs must remain observable without logging their values",
    );
  } finally {
    globalThis.fetch = originalFetch;
    console.warn = originalWarn;
  }
}

await clickIdPlausibilityAndNestedExtractionAreBounded();

async function recoveredClickAgeDoesNotSlideForever() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const send = async (firstObservedAt, requestUrl = "https://www.eden.health/return") => {
      const queue = new MockQueue();
      const attr = encodeURIComponent(JSON.stringify({
        gclid: "AGE-BOUND-GCLID-12345",
        utm_source: "google",
        utm_medium: "cpc",
        _ts: firstObservedAt,
        _click_first_observed_at: firstObservedAt,
        _last_seen_at: Date.now(),
      }));
      const ctx = makeCtx();
      const response = await worker.fetch(new Request(requestUrl, {
        headers: {
          "User-Agent": "Mozilla/5.0",
          Cookie: `eden_anonymous_id=age-bound-anon; eden_session_id=age-bound-session_1780000000000; eden_attr=${attr}`,
        },
      }), {
        EDEN_AD_CLICK_MEMORY_MODE: "cookie",
        AD_CLICK_KV: new MockKV(),
        AD_CLICK_SNAPSHOT_QUEUE: queue,
      }, ctx);
      await response.text();
      await Promise.all(ctx.promises);
      const attrCookie = getSetCookie(response.headers).find((cookie) => cookie.startsWith("eden_attr="));
      return { attrCookie, queue };
    };
    const twentyNineDaysAgo = Date.now() - 29 * 86400 * 1000;
    const active = await send(twentyNineDaysAgo);
    assert.ok(active.attrCookie);
    const activePayload = JSON.parse(decodeURIComponent(active.attrCookie.split(";", 1)[0].slice("eden_attr=".length)));
    assert.equal(activePayload.gclid, "AGE-BOUND-GCLID-12345");
    assert.equal(activePayload._click_first_observed_at, twentyNineDaysAgo, "repeat activity must not refresh the original click-age clock");
    assert.ok(activePayload._last_seen_at > twentyNineDaysAgo);
    const thirtyOneDaysAgo = Date.now() - 31 * 86400 * 1000;
    const expired = await send(thirtyOneDaysAgo);
    assert.ok(expired.attrCookie);
    const expiredPayload = JSON.parse(decodeURIComponent(expired.attrCookie.split(";", 1)[0].slice("eden_attr=".length)));
    assert.equal(expiredPayload.gclid, undefined, "expired click evidence drops even when the visitor remains active");
    assert.equal(expiredPayload.utm_source, "google", "non-click campaign context may remain for diagnostics");
    assert.equal(expired.queue.messages.filter((message) => message.payload.event_type === "ad_click_snapshot").length, 0);
    const staleGclidThenFreshBraid = await send(
      twentyNineDaysAgo,
      "https://www.eden.health/return?gbraid=FRESH-ACTIVE-GBRAID-12345&utm_source=google&utm_medium=cpc&utm_campaign=fresh_braid",
    );
    const mixedPayload = JSON.parse(decodeURIComponent(staleGclidThenFreshBraid.attrCookie.split(";", 1)[0].slice("eden_attr=".length)));
    assert.equal(mixedPayload.gclid, undefined, "a fresh braid must replace, not reactivate, an older recovered gclid in the active touch cookie");
    assert.equal(mixedPayload.gbraid, "FRESH-ACTIVE-GBRAID-12345");
    assert.ok(mixedPayload._click_first_observed_at > Date.now() - 5000, "the new active touch gets its own fresh immutable age clock");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await recoveredClickAgeDoesNotSlideForever();

async function durablePointerCoordinatorRejectsConcurrentOwnerClaims() {
  const records = new Map();
  const storage = {
    async get(key) { return records.get(key); },
    async put(key, value) { records.set(key, value); },
    async transaction(callback) {
      return callback({
        get: async (key) => records.get(key),
        put: async (key, value) => records.set(key, value),
        delete: async (key) => records.delete(key),
      });
    },
  };
  const kv = new MockKV();
  const coordinator = new ConversionCoordinator({ storage }, { AD_CLICK_KV: kv });
  const adClickId = "adclk2_concurrent_owner_claim";
  const common = {
    schema_version: "eden_ad_click_pointer_v2",
    ad_click_id: adClickId,
    snapshot_id: "adsnap_concurrent_owner_claim",
    captured_at: "2026-07-11T00:00:00.000Z",
    owner_anonymous_id_sha256: await sha256Raw("shared-browser-anon"),
    owner_session_id_sha256: await sha256Raw("shared-browser-session"),
    ad_click_id_scope: "first_party_scoped",
    ownership_scope: "first_party_owner_bound",
  };
  const claim = (user, order) => coordinator.fetch(new Request("https://conversion-coordinator.internal/pointer/upsert", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      ad_click_id: adClickId,
      proposed_record: {
        ...common,
        claimed_user_id_sha256: user,
        claimed_order_id_sha256: order,
      },
      ttl_seconds: 86400,
    }),
  }));
  const userA = await sha256Raw("source:user_id:concurrent-user-a");
  const userB = await sha256Raw("source:user_id:concurrent-user-b");
  const [first, second] = await Promise.all([
    claim(userA, await sha256Raw("concurrent-order-a")),
    claim(userB, await sha256Raw("concurrent-order-b")),
  ]);
  assert.deepEqual([first.status, second.status].sort(), [200, 409]);
  const rejected = first.status === 409 ? await first.json() : await second.json();
  assert.equal(rejected.owner_conflict, true);
  assert.equal(rejected.error, "owner_conflict");
  assert.ok(rejected.conflict_fields.includes("claimed_user_id_sha256"));
  const canonical = await storage.get("ad_click_pointer");
  assert.ok([userA, userB].includes(canonical.claimed_user_id_sha256));
  const cached = JSON.parse(await kv.get(`adclick:id:${adClickId}`));
  assert.equal(cached.claimed_user_id_sha256, canonical.claimed_user_id_sha256, "KV cache must contain only the admitted canonical owner");
  assert.equal(kv.putCalls.filter((call) => call.key === `adclick:id:${adClickId}`).length, 1, "rejected owner emits no cache write");
}

await durablePointerCoordinatorRejectsConcurrentOwnerClaims();

async function canonicalEdenIdentityOutranksConflictingFallbackAliases() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const ctx = makeCtx();
    const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "eden-wins-secret" },
      body: JSON.stringify({
        type: "track",
        event: "canonical_eden_identity_probe",
        eden_identity_id: "eden-identity-canonical",
        userId: "fallback-user-a",
        properties: { healthos: { user_id: "fallback-user-b" } },
      }),
    }), {
      SERVER_API_SECRET: "eden-wins-secret",
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: new MockKV(),
    }, ctx);
    await response.json();
    await Promise.all(ctx.promises);
    assert.equal(response.status, 200);
    assert.equal(segmentCalls.length, 1);
    assert.equal(segmentCalls[0].userId, "eden-identity-canonical");
    assert.equal(segmentCalls[0].properties.stable_identity_key_type, "eden_identity_id");
    assert.equal(segmentCalls[0].properties.identity_warning, "conflicting_source_user_ids_quarantined");
    assert.equal(JSON.stringify(segmentCalls[0]).includes("fallback-user-a"), false);
    assert.equal(JSON.stringify(segmentCalls[0]).includes("fallback-user-b"), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await canonicalEdenIdentityOutranksConflictingFallbackAliases();

async function fullReverseKvAlwaysHonorsCanonicalRevocation() {
  const segmentCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: 200 });
    }
    return new Response("ok", { status: 200 });
  };
  try {
    const kv = new MockKV();
    const queue = new MockQueue();
    const coordinator = new MockConversionCoordinatorNamespace();
    const adClickId = "adclk2_authoritative_revocation";
    const typedUser = "source:user_id:reverse-revoked-user";
    const userHash = await sha256Raw(typedUser);
    const stale = {
      schema_version: "eden_ad_click_pointer_v2",
      ad_click_id: adClickId,
      snapshot_id: "adsnap_authoritative_revocation",
      captured_at: "2026-07-11T00:00:00.000Z",
      claimed_user_id_sha256: userHash,
      ad_click_id_scope: "first_party_scoped",
      ownership_scope: "first_party_owner_bound",
    };
    await kv.put(`adclick:id:${adClickId}`, JSON.stringify(stale));
    await kv.put(`adclick:v2:user:${userHash}:last_paid`, adClickId);
    coordinator.pointerRecords.set(`eden_ad_click_pointer_v1:${adClickId}`, {
      ...stale,
      revoked_at: "2026-07-11T00:01:00.000Z",
      revocation_reason: "explicit_advertising_denial",
    });
    const ctx = makeCtx();
    const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "reverse-revoked-secret" },
      body: JSON.stringify({ type: "track", event: "reverse_revocation_probe", userId: "reverse-revoked-user", properties: {} }),
    }), {
      SERVER_API_SECRET: "reverse-revoked-secret",
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: kv,
      AD_CLICK_KV: kv,
      AD_CLICK_SNAPSHOT_QUEUE: queue,
      CONVERSION_COORDINATOR: coordinator,
      EDEN_AD_CLICK_MEMORY_MODE: "all",
      EDEN_AD_CLICK_KV_INDEX_MODE: "full",
      EDEN_AD_CLICK_KV_RESOLVER_MODE: "full",
      EDEN_AD_CLICK_KV_RESOLVER_CONTRACT_ACCEPTED: "true",
    }, ctx);
    await response.json();
    await Promise.all(ctx.promises);
    assert.equal(response.status, 200);
    assert.equal(queue.messages.length, 0, "a stale reverse index must not bypass canonical revocation");
    assert.equal(segmentCalls.length, 1);
    assert.equal(segmentCalls[0].properties.ad_click_id, undefined);
    assert.ok(JSON.parse(await kv.get(`adclick:id:${adClickId}`)).revoked_at, "authoritative read repairs the stale pointer cache");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await fullReverseKvAlwaysHonorsCanonicalRevocation();

async function attrCookieOverflowPreservesEveryNativeGoogleUploadIdBeforePrimaryFallback() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const url = new URL("https://www.eden.health/");
    url.searchParams.set("gclid", `G${"A".repeat(199)}`);
    url.searchParams.set("gbraid", `B${"B".repeat(199)}`);
    url.searchParams.set("wbraid", `W${"C".repeat(199)}`);
    url.searchParams.set("_gcl_aw", `1.1.${"D".repeat(1500)}`);
    url.searchParams.set("srsltid", `S${"E".repeat(1500)}`);
    url.searchParams.set("utm_content", `U${"F".repeat(1500)}`);
    const response = await worker.fetch(new Request(url, { headers: { "User-Agent": "Mozilla/5.0" } }), {}, makeCtx());
    await response.text();
    const cookie = getSetCookie(response.headers).find((entry) => entry.startsWith("eden_attr="));
    assert.ok(cookie);
    const encoded = cookie.split(";", 1)[0].slice("eden_attr=".length);
    assert.ok(encoded.length <= 3500, `encoded eden_attr must be <= 3500 bytes, got ${encoded.length}`);
    const decoded = JSON.parse(decodeURIComponent(encoded));
    assert.equal(decoded._truncated, "upload_ids_only");
    assert.equal(decoded.gclid, url.searchParams.get("gclid"));
    assert.equal(decoded.gbraid, url.searchParams.get("gbraid"));
    assert.equal(decoded.wbraid, url.searchParams.get("wbraid"));
    assert.equal(decoded._gcl_aw, undefined);
    assert.equal(decoded.srsltid, undefined);
    assert.equal(decoded.utm_content, undefined);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await attrCookieOverflowPreservesEveryNativeGoogleUploadIdBeforePrimaryFallback();

async function attrCookieAlwaysFitsConfiguredBudget() {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
  try {
    const url = new URL("https://www.eden.health/");
    url.searchParams.set("gclid", `G${"A".repeat(1023)}`);
    url.searchParams.set("gbraid", `B${"B".repeat(1023)}`);
    url.searchParams.set("wbraid", `W${"C".repeat(1023)}`);
    url.searchParams.set("dclid", `D${"D".repeat(1023)}`);
    url.searchParams.set("utm_source", "google");
    const response = await worker.fetch(new Request(url, { headers: { "User-Agent": "Mozilla/5.0" } }), {}, makeCtx());
    await response.text();
    const cookie = getSetCookie(response.headers).find((entry) => entry.startsWith("eden_attr="));
    assert.ok(cookie);
    const encoded = cookie.split(";", 1)[0].slice("eden_attr=".length);
    assert.ok(encoded.length <= 3500, `encoded eden_attr must be <= 3500 bytes, got ${encoded.length}`);
    const decoded = JSON.parse(decodeURIComponent(encoded));
    assert.equal(decoded._truncated, "primary_only");
    assert.ok(decoded.gclid);
    assert.equal(decoded.gbraid, undefined);
    assert.equal(decoded.wbraid, undefined);
    assert.equal(decoded.dclid, undefined);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await attrCookieAlwaysFitsConfiguredBudget();

async function segmentErrorLoggingIsBoundedAndSanitized() {
  const originalFetch = globalThis.fetch;
  const originalError = console.error;
  const errors = [];
  let bytesEnqueued = 0;
  let pulls = 0;
  const secret = "S".repeat(80);
  const chunk = new TextEncoder().encode(`{\"error\":\"user@example.com https://sensitive.example/path ${secret} ${"X".repeat(850)}\"}`);
  globalThis.fetch = async (url) => {
    if (!String(url).startsWith("https://api.segment.io/")) return new Response("ok", { status: 200 });
    const body = new ReadableStream({
      pull(controller) {
        pulls += 1;
        if (pulls > 1000) return controller.close();
        bytesEnqueued += chunk.byteLength;
        controller.enqueue(chunk);
      },
    });
    return new Response(body, { status: 400 });
  };
  console.error = (...args) => errors.push(args.map((entry) => String(entry)).join(" "));
  try {
    const ctx = makeCtx();
    const response = await worker.fetch(new Request("https://collect.eden.health/server-collect", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Eden-Server-Secret": "bounded-segment-secret" },
      body: JSON.stringify({ type: "track", event: "bounded_segment_error_probe", userId: "bounded-segment-user", properties: {} }),
    }), {
      SERVER_API_SECRET: "bounded-segment-secret",
      SEGMENT_WRITE_KEY: "fixture",
      GCLID_KV: new MockKV(),
    }, ctx);
    await response.json();
    await Promise.allSettled(ctx.promises);
    assert.equal(response.status, 200);
    assert.ok(bytesEnqueued <= 4096, `Segment error reader must stop after a bounded prefix, got ${bytesEnqueued} bytes`);
    const serialized = errors.join("\n");
    assert.equal(serialized.includes("user@example.com"), false);
    assert.equal(serialized.includes("sensitive.example"), false);
    assert.equal(serialized.includes(secret), false);
    assert.ok(serialized.includes("[redacted_email]"));
    assert.ok(serialized.includes("[redacted_url]"));
  } finally {
    globalThis.fetch = originalFetch;
    console.error = originalError;
  }
}

await segmentErrorLoggingIsBoundedAndSanitized();

async function appWebflowAliasesAndSynchronousDeliveryRemainCompatible() {
  const originalFetch = globalThis.fetch;
  const segmentCalls = [];
  let segmentStatus = 200;
  globalThis.fetch = async (url, init = {}) => {
    if (String(url).startsWith("https://api.segment.io/")) {
      segmentCalls.push(JSON.parse(init.body));
      return new Response("{}", { status: segmentStatus });
    }
    return new Response("ok", { status: 200 });
  };
  const kv = new MockKV();
  const env = {
    SEGMENT_WRITE_KEY: "fixture",
    EDEN_BROWSER_SEGMENT_DELIVERY_MODE: "sync",
    EDEN_BROWSER_CAP_ENFORCEMENT_MODE: "enforce",
    BROWSER_CAP_HMAC_SECRET: TEST_BROWSER_CAP_HMAC_SECRET,
    PRIVACY_LEDGER_HMAC_SECRET: TEST_PRIVACY_LEDGER_HMAC_SECRET,
    PRIVACY_LEDGER_KV: kv,
    GCLID_KV: kv,
    CONVERSION_COORDINATOR: new MockConversionCoordinatorNamespace(),
  };
  const request = (path, body, cookie = "", origin = "https://app.eden.health") => new Request(`https://collect.eden.health${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Origin: origin,
      "Sec-Fetch-Site": "same-site",
      ...(cookie ? { Cookie: cookie } : {}),
    },
    body: JSON.stringify(body),
  });
  try {
    const track = await productionWorker.fetch(request("/collect/t", {
      type: "track",
      event: "OS_intake_started",
      messageId: "app-intake-alias-1",
      properties: { step_name: "welcome" },
    }), env, makeCtx());
    assert.equal(track.status, 200, "AnalyticsBrowser /collect/t must be accepted");
    assert.equal(segmentCalls.at(-1).event, "OS_intake_started", "safe OS behavior names must remain exact");
    const ownerCookies = getSetCookie(track.headers).map((cookie) => cookie.split(";", 1)[0]).join("; ");
    assert.match(ownerCookies, /eden_anonymous_id=/);
    assert.match(ownerCookies, /eden_session_id=/);
    assert.match(ownerCookies, /__Secure-eden_browser_cap=/);

    const page = await productionWorker.fetch(request("/collect/p", {
      type: "page",
      name: "Intake Welcome",
      messageId: "app-page-alias-1",
      properties: { page_path: "/intake/weightloss/welcome" },
    }, ownerCookies), env, makeCtx());
    assert.equal(page.status, 200, "AnalyticsBrowser /collect/p must be accepted");
    assert.equal(segmentCalls.at(-1).name, "Intake Welcome");

    const identify = await productionWorker.fetch(request("/collect/i", {
      type: "identify",
      userId: "browser-must-not-own-stable-id",
      traits: { email: "must-not-forward@example.com" },
    }, ownerCookies), env, makeCtx());
    assert.equal(identify.status, 200, "AnalyticsBrowser /collect/i must reach the guarded identify compatibility path");
    assert.equal((await identify.json()).stable_identity_accepted, false);

    const legacyOnly = await productionWorker.fetch(request("/collect", {
      type: "track",
      event: "webflow_legacy_owner_probe",
      properties: {},
    }, "eden_anon_id=legacy-webflow-owner", "https://www.eden.health"), env, makeCtx());
    assert.equal(legacyOnly.status, 200, "Webflow legacy anonymous owner must be able to bootstrap its missing session");
    assert.equal((await legacyOnly.json()).anonId, "legacy-webflow-owner");
    assert.ok(getSetCookie(legacyOnly.headers).some((cookie) => cookie.startsWith("eden_session_id=")));

    segmentStatus = 503;
    const failed = await productionWorker.fetch(request("/collect/t", {
      type: "track",
      event: "segment_failure_probe",
      properties: {},
    }, ownerCookies), env, makeCtx());
    assert.equal(failed.status, 503, "synchronous browser delivery must expose Segment failure");
    assert.equal((await failed.json()).error, "segment_delivery_failed");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

await appWebflowAliasesAndSynchronousDeliveryRemainCompatible();

console.log("PASS eden analytics worker local canary/all/off/server/identify enrichment/ad-click memory tests");
