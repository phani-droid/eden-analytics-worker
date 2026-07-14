// Strongly-consistent per-transaction and per-ad-click coordination for eden-analytics.
//
// This class is isolated so the migration bootstrap and every rollback artifact
// can carry the exact same Durable Object class without importing v5.56 request
// behavior. KV remains a read cache; Durable Object storage is authoritative.

const AD_CLICK_POINTER_RECORD_SCHEMA_VERSION = "eden_ad_click_pointer_v2";
const FIRST_TOUCH_RECORD_SCHEMA_VERSION = "eden_attribution_first_touch_v1";
const IDENTITY_POINTER_RECORD_SCHEMA_VERSION = "eden_identity_pointer_v1";
const AD_CLICK_KV_PREFIX = "adclick:";
const CONVERSION_COORDINATOR_LEASE_TTL_MS = 120_000;
// Durable Object values are limited to 128 KiB. Pending conversion records can
// temporarily retain an exact Segment payload for unknown-commit replay.
const CONVERSION_COORDINATOR_RECORD_MAX_BYTES = 120_000;
const COORDINATION_REQUEST_MAX_BYTES = 16_384;
const COORDINATION_STATE_MAX_BYTES = 32_768;
const SHA256_PATTERN = /^[a-f0-9]{64}$/;
const AD_CLICK_ID_PATTERN = /^adclk2_[A-Za-z0-9_-]{8,128}$/;
const AD_CLICK_POINTER_RESERVATION_SCHEMA_VERSION = "eden_ad_click_pointer_reservation_v1";
const AD_CLICK_POINTER_RESERVATION_ID_PATTERN = /^adrsrv_[A-Za-z0-9_-]{8,128}$/;
const AD_CLICK_POINTER_RESERVATION_TTL_MS = 120_000;
const FIRST_TOUCH_OWNER_SCOPES = new Set([
  "anonymous_id_sha256",
  "user_id_sha256",
  "order_id_sha256"
]);
const STABLE_IDENTITY_TYPES = new Set(["user_id_sha256", "order_id_sha256"]);
const FIRST_TOUCH_OBSERVATION_ID_FIELD = "observation_id_sha256";
const FIRST_TOUCH_ATTRIBUTION_FIELDS = [...new Set([
  "gclid", "gbraid", "wbraid", "dclid", "_gcl_au", "gcl_au", "_gcl_aw", "gcl_aw",
  "_gcl_dc", "gcl_dc", "_gcl_gb", "gcl_gb", "_gcl_gs", "gcl_gs", "srsltid",
  "fbclid", "msclkid", "ttclid", "twclid", "li_fat_id", "rdt_cid", "epik", "ScCid",
  "nbt", "irclickid", "cjevent", "click_id", "utm_source", "utm_medium", "utm_campaign",
  "utm_content", "utm_term", "utm_id", "gclsrc", "gad_source", "gad_campaignid", "gidrep",
  "creative", "matchtype", "network", "device", "targetid", "feeditemid", "placement",
  "nb_adtype", "nb_kwd", "nb_ti", "nb_mi", "nb_pc", "nb_pi", "nb_ppi", "_ga", "ga",
  "_gid", "gid", "ga_client_id", "ga_session_id", "gac", "gac_cookie_names", "gac_values",
  "nb_placement", "nb_li_ms", "nb_lp_ms", "nb_fii", "nb_ap", "nb_mt", "upfluence_id",
  "influencer_id", "creator_id", "partner_id", "affiliate_id", "referral_code", "referral_id",
  "ref", "source", "sub_id", "subid", "sub1", "sub2", "sub3", "sub4", "sub5",
  "campaign_id", "adgroup_id", "ad_group_id", "keyword", "search_term", "landing_page",
  "attribution_referrer", "acquisition_channel", "attribution_source", "attribution_medium",
  "attribution_campaign"
])];

function nowUTC() {
  return new Date(Date.now()).toISOString();
}

function jsonBytes(value) {
  try {
    return new TextEncoder().encode(JSON.stringify(value)).byteLength;
  } catch {
    return Infinity;
  }
}

function jsonResponse(value, status = 200) {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
  });
}

function canonicalTimestamp(value) {
  if (typeof value !== "string" || value.length < 20 || value.length > 35) return null;
  const timestampMs = Date.parse(value);
  return Number.isFinite(timestampMs) ? new Date(timestampMs).toISOString() : null;
}

function boundedNonemptyString(value, maxLength) {
  return typeof value === "string"
    && value.length > 0
    && value.length <= maxLength
    && !/[\u0000-\u001f\u007f]/.test(value);
}

function firstTouchIdentity(body) {
  const ownerScope = body?.owner_scope || body?.owner_type;
  const ownerHash = body?.owner_hash;
  return FIRST_TOUCH_OWNER_SCOPES.has(ownerScope) && typeof ownerHash === "string" && SHA256_PATTERN.test(ownerHash)
    ? { ownerScope, ownerHash }
    : null;
}

function firstTouchCandidate(body, identity) {
  const input = body?.record || body?.first_touch || body?.proposed_record || body;
  if (!input || typeof input !== "object" || Array.isArray(input)) return null;
  const allowedFields = new Set(["schema_version", "owner_scope", "owner_type", "owner_hash", "captured_at", FIRST_TOUCH_OBSERVATION_ID_FIELD, ...FIRST_TOUCH_ATTRIBUTION_FIELDS]);
  if (Object.keys(input).some((field) => !allowedFields.has(field))) return null;
  if (input.schema_version !== void 0 && input.schema_version !== FIRST_TOUCH_RECORD_SCHEMA_VERSION) return null;
  if (input.owner_scope !== void 0 && input.owner_scope !== identity.ownerScope) return null;
  if (input.owner_type !== void 0 && input.owner_type !== identity.ownerScope) return null;
  if (input.owner_hash !== void 0 && input.owner_hash !== identity.ownerHash) return null;
  const capturedAt = canonicalTimestamp(input.captured_at || body?.captured_at);
  if (!capturedAt) return null;
  const candidate = {
    schema_version: FIRST_TOUCH_RECORD_SCHEMA_VERSION,
    owner_scope: identity.ownerScope,
    owner_hash: identity.ownerHash,
    captured_at: capturedAt
  };
  const observationId = input[FIRST_TOUCH_OBSERVATION_ID_FIELD] ?? body?.[FIRST_TOUCH_OBSERVATION_ID_FIELD];
  if (observationId !== void 0) {
    if (typeof observationId !== "string" || !SHA256_PATTERN.test(observationId)) return null;
    candidate[FIRST_TOUCH_OBSERVATION_ID_FIELD] = observationId;
  }
  for (const field of FIRST_TOUCH_ATTRIBUTION_FIELDS) {
    const value = input[field] ?? body?.[field];
    if (value === void 0) continue;
    if (!boundedNonemptyString(value, 2048)) return null;
    candidate[field] = value;
  }
  return jsonBytes(candidate) <= COORDINATION_STATE_MAX_BYTES ? candidate : null;
}

function firstTouchRecordValid(record, identity) {
  if (!record || typeof record !== "object" || Array.isArray(record)) return false;
  const allowedFields = new Set(["schema_version", "owner_scope", "owner_hash", "captured_at", "updated_at", FIRST_TOUCH_OBSERVATION_ID_FIELD, ...FIRST_TOUCH_ATTRIBUTION_FIELDS]);
  if (Object.keys(record).some((field) => !allowedFields.has(field))) return false;
  if (record.schema_version !== FIRST_TOUCH_RECORD_SCHEMA_VERSION) return false;
  if (record.owner_scope !== identity.ownerScope || record.owner_hash !== identity.ownerHash) return false;
  if (!canonicalTimestamp(record.captured_at) || !canonicalTimestamp(record.updated_at)) return false;
  if (record[FIRST_TOUCH_OBSERVATION_ID_FIELD] !== void 0
    && (typeof record[FIRST_TOUCH_OBSERVATION_ID_FIELD] !== "string" || !SHA256_PATTERN.test(record[FIRST_TOUCH_OBSERVATION_ID_FIELD]))) return false;
  for (const field of FIRST_TOUCH_ATTRIBUTION_FIELDS) {
    if (record[field] !== void 0 && !boundedNonemptyString(record[field], 2048)) return false;
  }
  return jsonBytes(record) <= COORDINATION_STATE_MAX_BYTES;
}

function stableIdentity(body) {
  const identityType = body?.identity_type;
  const identityHash = body?.identity_hash;
  return STABLE_IDENTITY_TYPES.has(identityType) && typeof identityHash === "string" && SHA256_PATTERN.test(identityHash)
    ? { identityType, identityHash }
    : null;
}

function identityPointerCandidate(body) {
  const input = body?.candidate || body?.pointer || body?.proposed_record || body;
  if (!input || typeof input !== "object" || Array.isArray(input)) return null;
  const adClickId = input.ad_click_id;
  const capturedAt = canonicalTimestamp(input.captured_at || input.capture_timestamp || body?.captured_at || body?.capture_timestamp);
  return typeof adClickId === "string" && AD_CLICK_ID_PATTERN.test(adClickId) && capturedAt
    ? { adClickId, capturedAt }
    : null;
}

function identityPointerRecordValid(record, identity) {
  if (!record || typeof record !== "object" || Array.isArray(record)) return false;
  const allowedFields = new Set([
    "schema_version", "identity_type", "identity_hash", "first_ad_click_id", "first_captured_at",
    "latest_ad_click_id", "latest_captured_at", "updated_at"
  ]);
  if (Object.keys(record).some((field) => !allowedFields.has(field))) return false;
  if (record.schema_version !== IDENTITY_POINTER_RECORD_SCHEMA_VERSION) return false;
  if (record.identity_type !== identity.identityType || record.identity_hash !== identity.identityHash) return false;
  if (typeof record.first_ad_click_id !== "string" || !AD_CLICK_ID_PATTERN.test(record.first_ad_click_id)) return false;
  if (typeof record.latest_ad_click_id !== "string" || !AD_CLICK_ID_PATTERN.test(record.latest_ad_click_id)) return false;
  const firstAt = canonicalTimestamp(record.first_captured_at);
  const latestAt = canonicalTimestamp(record.latest_captured_at);
  if (!firstAt || !latestAt || !canonicalTimestamp(record.updated_at)) return false;
  if (Date.parse(firstAt) > Date.parse(latestAt)) return false;
  return jsonBytes(record) <= COORDINATION_STATE_MAX_BYTES;
}

async function readBoundedJson(request, maxBytes) {
  const contentLength = request.headers.get("Content-Length");
  if (contentLength !== null) {
    const declaredBytes = Number(contentLength);
    if (!Number.isInteger(declaredBytes) || declaredBytes < 0 || declaredBytes > maxBytes) {
      return { ok: false, status: 413, error: "request_too_large" };
    }
  }
  if (!request.body) return { ok: false, status: 400, error: "invalid_json" };
  const reader = request.body.getReader();
  const chunks = [];
  let byteLength = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      byteLength += value.byteLength;
      if (byteLength > maxBytes) {
        await reader.cancel();
        return { ok: false, status: 413, error: "request_too_large" };
      }
      chunks.push(value);
    }
  } catch {
    return { ok: false, status: 400, error: "invalid_json" };
  }
  const bytes = new Uint8Array(byteLength);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  try {
    const body = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
    return body && typeof body === "object" && !Array.isArray(body)
      ? { ok: true, body }
      : { ok: false, status: 400, error: "invalid_json" };
  } catch {
    return { ok: false, status: 400, error: "invalid_json" };
  }
}

export class ConversionCoordinator {
  constructor(state, env = {}) {
    this.state = state;
    this.env = env;
    this.pointerMutationChain = Promise.resolve();
  }
  pointerRecordValid(record, adClickId) {
    if (!record || typeof record !== "object" || Array.isArray(record)) return false;
    if (record.schema_version !== AD_CLICK_POINTER_RECORD_SCHEMA_VERSION) return false;
    if (record.ad_click_id !== adClickId) return false;
    return jsonBytes(record) <= COORDINATION_STATE_MAX_BYTES;
  }
  pointerOwnerValid(record, owner = {}) {
    if (!record || !owner || typeof owner !== "object") return false;
    if (record.claimed_user_id_sha256 && owner.user_id_sha256 && record.claimed_user_id_sha256 !== owner.user_id_sha256) return false;
    if (record.claimed_user_id_sha256 && owner.user_id_sha256) return record.claimed_user_id_sha256 === owner.user_id_sha256;
    if (record.claimed_order_id_sha256 && owner.order_id_sha256 && record.claimed_order_id_sha256 !== owner.order_id_sha256) return false;
    if (record.claimed_order_id_sha256 && owner.order_id_sha256) return record.claimed_order_id_sha256 === owner.order_id_sha256;
    if (record.owner_anonymous_id_sha256 && owner.anonymous_id_sha256) return record.owner_anonymous_id_sha256 === owner.anonymous_id_sha256;
    if (record.owner_session_id_sha256 && owner.session_id_sha256) return record.owner_session_id_sha256 === owner.session_id_sha256;
    return false;
  }
  pointerOwnerConflicts(current, proposed) {
    if (!current || !proposed) return [];
    const immutableOwnerFields = [
      "owner_anonymous_id_sha256",
      "owner_session_id_sha256",
      "claimed_user_id_sha256",
      "claimed_order_id_sha256"
    ];
    const sameStableUser = current.claimed_user_id_sha256
      && proposed.claimed_user_id_sha256
      && current.claimed_user_id_sha256 === proposed.claimed_user_id_sha256;
    const sameAnonymousOwner = current.owner_anonymous_id_sha256
      && proposed.owner_anonymous_id_sha256
      && current.owner_anonymous_id_sha256 === proposed.owner_anonymous_id_sha256;
    return immutableOwnerFields.filter((field) =>
      !(field === "claimed_order_id_sha256" && (sameStableUser || sameAnonymousOwner))
      &&
      current[field]
      && proposed[field]
      && current[field] !== proposed[field]
    );
  }
  async executePointerRead(body) {
    const adClickId = typeof body?.ad_click_id === "string" && AD_CLICK_ID_PATTERN.test(body.ad_click_id)
      ? body.ad_click_id
      : null;
    const ttlSeconds = Number(body?.ttl_seconds);
    if (!adClickId || !Number.isInteger(ttlSeconds) || ttlSeconds < 60 || ttlSeconds > 31536e3) {
      return new Response(JSON.stringify({ ok: false, error: "invalid_pointer_read" }), { status: 400, headers: { "Content-Type": "application/json" } });
    }
    const seedRecord = this.pointerRecordValid(body?.seed_record, adClickId) ? body.seed_record : null;
    const canonical = await this.state.storage.transaction(async (txn) => {
      const storedRecord = await txn.get("ad_click_pointer");
      if (storedRecord !== void 0 && storedRecord !== null && !this.pointerRecordValid(storedRecord, adClickId)) {
        return { invalid: true };
      }
      if (storedRecord) return { record: storedRecord, bootstrappedFromCache: false };
      if (!seedRecord) return { record: null, bootstrappedFromCache: false };
      await txn.put("ad_click_pointer", seedRecord);
      return { record: seedRecord, bootstrappedFromCache: true };
    });
    if (canonical.invalid) {
      // An invalid canonical record must never fall back to a valid-looking KV
      // cache row. This object is scoped to one ad_click_id, so any mismatch is
      // corruption and must fail closed until an operator repairs it.
      return new Response(JSON.stringify({ ok: false, error: "pointer_state_invalid" }), { status: 503, headers: { "Content-Type": "application/json", "Cache-Control": "no-store" } });
    }
    const current = canonical.record;
    const bootstrappedFromCache = canonical.bootstrappedFromCache;
    let cacheRepairAttempted = false;
    let cacheRepairSucceeded = false;
    if (body?.repair_cache !== false && current && !bootstrappedFromCache && JSON.stringify(current) !== JSON.stringify(seedRecord)) {
      const kv = this.env?.AD_CLICK_KV;
      if (kv && typeof kv.put === "function") {
        cacheRepairAttempted = true;
        try {
          await kv.put(`${AD_CLICK_KV_PREFIX}id:${adClickId}`, JSON.stringify(current), { expirationTtl: ttlSeconds });
          cacheRepairSucceeded = true;
        } catch {
          // The authoritative read still succeeds. Returning the DO record is
          // fail-closed for a revoked pointer and lets a later read repair KV.
        }
      }
    }
    return new Response(JSON.stringify({
      ok: true,
      found: !!current,
      record: current,
      bootstrapped_from_cache: bootstrappedFromCache,
      cache_repair_attempted: cacheRepairAttempted,
      cache_repair_succeeded: cacheRepairSucceeded
    }), { status: 200, headers: { "Content-Type": "application/json", "Cache-Control": "no-store" } });
  }
  async executePointerReserve(body) {
    const adClickId = typeof body?.ad_click_id === "string" && AD_CLICK_ID_PATTERN.test(body.ad_click_id) ? body.ad_click_id : null;
    const reservationId = typeof body?.reservation_id === "string" && AD_CLICK_POINTER_RESERVATION_ID_PATTERN.test(body.reservation_id) ? body.reservation_id : null;
    const ttlSeconds = Number(body?.ttl_seconds);
    const proposed = this.pointerRecordValid(body?.proposed_record, adClickId) ? body.proposed_record : null;
    const seedRecord = this.pointerRecordValid(body?.seed_record, adClickId) ? body.seed_record : null;
    if (!adClickId || !reservationId || !proposed || !Number.isInteger(ttlSeconds) || ttlSeconds < 60 || ttlSeconds > 31536e3) {
      return jsonResponse({ ok: false, error: "invalid_pointer_reservation" }, 400);
    }
    const nowMs = Date.now();
    const outcome = await this.state.storage.transaction(async (txn) => {
      const storedRecord = await txn.get("ad_click_pointer");
      if (storedRecord !== void 0 && storedRecord !== null && !this.pointerRecordValid(storedRecord, adClickId)) return { invalid: true };
      const current = storedRecord || seedRecord || null;
      if (current?.revoked_at) return { revoked: true };
      const ownerConflictFields = this.pointerOwnerConflicts(current, proposed);
      if (ownerConflictFields.length) return { ownerConflictFields };
      const existingReservation = await txn.get("ad_click_pointer_reservation");
      if (existingReservation && Number(existingReservation.expires_at_ms || 0) > nowMs) {
        const reservationBoundToCurrent = Object.prototype.hasOwnProperty.call(existingReservation, "canonical_record_at_reserve")
          && JSON.stringify(existingReservation.canonical_record_at_reserve) === JSON.stringify(storedRecord || null);
        if (!reservationBoundToCurrent) {
          await txn.delete("ad_click_pointer_reservation");
        } else {
          const same = existingReservation.schema_version === AD_CLICK_POINTER_RESERVATION_SCHEMA_VERSION
            && existingReservation.reservation_id === reservationId
            && existingReservation.ad_click_id === adClickId
            && JSON.stringify(existingReservation.proposed_record) === JSON.stringify({
              ...proposed,
              captured_at: current?.captured_at || proposed.captured_at,
              owner_anonymous_id_sha256: current?.owner_anonymous_id_sha256 || proposed.owner_anonymous_id_sha256,
              owner_session_id_sha256: current?.owner_session_id_sha256 || proposed.owner_session_id_sha256,
              claimed_user_id_sha256: current?.claimed_user_id_sha256 || proposed.claimed_user_id_sha256,
              claimed_order_id_sha256: current?.claimed_order_id_sha256 || proposed.claimed_order_id_sha256,
              updated_at: existingReservation.proposed_record?.updated_at
            });
          return same ? { reservation: existingReservation, reused: true } : { busy: true };
        }
      }
      if (existingReservation) await txn.delete("ad_click_pointer_reservation");
      const nextRecord = {
        ...proposed,
        captured_at: current?.captured_at || proposed.captured_at,
        owner_anonymous_id_sha256: current?.owner_anonymous_id_sha256 || proposed.owner_anonymous_id_sha256,
        owner_session_id_sha256: current?.owner_session_id_sha256 || proposed.owner_session_id_sha256,
        claimed_user_id_sha256: current?.claimed_user_id_sha256 || proposed.claimed_user_id_sha256,
        claimed_order_id_sha256: current?.claimed_order_id_sha256 || proposed.claimed_order_id_sha256,
        updated_at: nowUTC()
      };
      const reservation = {
        schema_version: AD_CLICK_POINTER_RESERVATION_SCHEMA_VERSION,
        reservation_id: reservationId,
        ad_click_id: adClickId,
        canonical_record_at_reserve: storedRecord || null,
        proposed_record: nextRecord,
        persist_cache: body?.persist_cache !== false,
        ttl_seconds: ttlSeconds,
        expires_at_ms: nowMs + AD_CLICK_POINTER_RESERVATION_TTL_MS
      };
      await txn.put("ad_click_pointer_reservation", reservation);
      return { reservation, reused: false };
    });
    if (outcome.invalid) return jsonResponse({ ok: false, error: "pointer_state_invalid" }, 503);
    if (outcome.revoked) return jsonResponse({ ok: false, reserved: false, revoked: true }, 409);
    if (outcome.ownerConflictFields) return jsonResponse({ ok: false, reserved: false, owner_conflict: true, conflict_fields: outcome.ownerConflictFields }, 409);
    if (outcome.busy) return jsonResponse({ ok: false, reserved: false, error: "pointer_reservation_busy" }, 409);
    return jsonResponse({ ok: true, reserved: true, reservation_id: reservationId, reused: outcome.reused === true });
  }
  async executePointerCommit(body) {
    const adClickId = typeof body?.ad_click_id === "string" && AD_CLICK_ID_PATTERN.test(body.ad_click_id) ? body.ad_click_id : null;
    const reservationId = typeof body?.reservation_id === "string" && AD_CLICK_POINTER_RESERVATION_ID_PATTERN.test(body.reservation_id) ? body.reservation_id : null;
    if (!adClickId || !reservationId) return jsonResponse({ ok: false, error: "invalid_pointer_commit" }, 400);
    const outcome = await this.state.storage.transaction(async (txn) => {
      const storedRecord = await txn.get("ad_click_pointer");
      if (storedRecord !== void 0 && storedRecord !== null && !this.pointerRecordValid(storedRecord, adClickId)) return { invalid: true };
      const lastCommit = await txn.get("ad_click_pointer_last_commit");
      if (lastCommit?.reservation_id === reservationId && lastCommit?.ad_click_id === adClickId && this.pointerRecordValid(lastCommit.record, adClickId)) {
        if (JSON.stringify(storedRecord || null) !== JSON.stringify(lastCommit.record)) {
          await txn.delete("ad_click_pointer_last_commit");
          return { stale: true };
        }
        return { record: lastCommit.record, persistCache: lastCommit.persist_cache === true, ttlSeconds: lastCommit.ttl_seconds, replay: true };
      }
      const reservation = await txn.get("ad_click_pointer_reservation");
      if (!reservation || reservation.schema_version !== AD_CLICK_POINTER_RESERVATION_SCHEMA_VERSION
        || reservation.reservation_id !== reservationId || reservation.ad_click_id !== adClickId
        || !this.pointerRecordValid(reservation.proposed_record, adClickId)) return { missing: true };
      if (Number(reservation.expires_at_ms || 0) < Date.now()) {
        await txn.delete("ad_click_pointer_reservation");
        return { expired: true };
      }
      const reservationBoundToCurrent = Object.prototype.hasOwnProperty.call(reservation, "canonical_record_at_reserve")
        && JSON.stringify(reservation.canonical_record_at_reserve) === JSON.stringify(storedRecord || null);
      if (!reservationBoundToCurrent) {
        await txn.delete("ad_click_pointer_reservation");
        return { stale: true };
      }
      await txn.put("ad_click_pointer", reservation.proposed_record);
      await txn.put("ad_click_pointer_last_commit", {
        reservation_id: reservationId,
        ad_click_id: adClickId,
        record: reservation.proposed_record,
        persist_cache: reservation.persist_cache === true,
        ttl_seconds: reservation.ttl_seconds,
        committed_at: nowUTC()
      });
      await txn.delete("ad_click_pointer_reservation");
      return { record: reservation.proposed_record, persistCache: reservation.persist_cache === true, ttlSeconds: reservation.ttl_seconds, replay: false };
    });
    if (outcome.invalid) return jsonResponse({ ok: false, error: "pointer_state_invalid" }, 503);
    if (outcome.missing) return jsonResponse({ ok: false, error: "pointer_reservation_missing" }, 409);
    if (outcome.expired) return jsonResponse({ ok: false, error: "pointer_reservation_expired" }, 409);
    if (outcome.stale) return jsonResponse({ ok: false, error: "pointer_reservation_stale" }, 409);
    let cachePersisted = false;
    if (outcome.persistCache) {
      const kv = this.env?.AD_CLICK_KV;
      if (!kv || typeof kv.put !== "function") return jsonResponse({ ok: false, error: "pointer_kv_missing" }, 503);
      try {
        await kv.put(`${AD_CLICK_KV_PREFIX}id:${adClickId}`, JSON.stringify(outcome.record), { expirationTtl: outcome.ttlSeconds });
        cachePersisted = true;
      } catch {
        return jsonResponse({ ok: false, error: "pointer_kv_write_failed" }, 503);
      }
    }
    return jsonResponse({ ok: true, committed: true, cache_persisted: cachePersisted, replay: outcome.replay === true });
  }
  async executePointerCancel(body) {
    const adClickId = typeof body?.ad_click_id === "string" && AD_CLICK_ID_PATTERN.test(body.ad_click_id) ? body.ad_click_id : null;
    const reservationId = typeof body?.reservation_id === "string" && AD_CLICK_POINTER_RESERVATION_ID_PATTERN.test(body.reservation_id) ? body.reservation_id : null;
    if (!adClickId || !reservationId) return jsonResponse({ ok: false, error: "invalid_pointer_cancel" }, 400);
    const cancelled = await this.state.storage.transaction(async (txn) => {
      const reservation = await txn.get("ad_click_pointer_reservation");
      if (!reservation || reservation.reservation_id !== reservationId || reservation.ad_click_id !== adClickId) return false;
      await txn.delete("ad_click_pointer_reservation");
      return true;
    });
    return jsonResponse({ ok: true, cancelled });
  }
  async executePointerMutation(pathname, body) {
    const adClickId = typeof body?.ad_click_id === "string" && AD_CLICK_ID_PATTERN.test(body.ad_click_id)
      ? body.ad_click_id
      : null;
    const ttlSeconds = Number(body?.ttl_seconds);
    if (!adClickId || !Number.isInteger(ttlSeconds) || ttlSeconds < 60 || ttlSeconds > 31536e3) {
      return new Response(JSON.stringify({ ok: false, error: "invalid_pointer_mutation" }), { status: 400, headers: { "Content-Type": "application/json" } });
    }
    const seedRecord = this.pointerRecordValid(body?.seed_record, adClickId) ? body.seed_record : null;
    const proposed = pathname === "/pointer/upsert" && this.pointerRecordValid(body?.proposed_record, adClickId)
      ? body.proposed_record
      : null;
    if (pathname === "/pointer/upsert" && !proposed) return new Response(JSON.stringify({ ok: false, error: "invalid_pointer_record" }), { status: 400, headers: { "Content-Type": "application/json" } });
    const mutation = await this.state.storage.transaction(async (txn) => {
      const storedRecord = await txn.get("ad_click_pointer");
      if (storedRecord !== void 0 && storedRecord !== null && !this.pointerRecordValid(storedRecord, adClickId)) return { error: "pointer_state_invalid", status: 503 };
      const current = this.pointerRecordValid(storedRecord, adClickId) ? storedRecord : seedRecord;
      let nextRecord;
      if (pathname === "/pointer/upsert") {
        if (current?.revoked_at) return { response: { ok: false, persisted: false, revoked: true }, status: 409 };
        const ownerConflictFields = this.pointerOwnerConflicts(current, proposed);
        if (ownerConflictFields.length) {
          return { response: { ok: false, persisted: false, owner_conflict: true, error: "owner_conflict", conflict_fields: ownerConflictFields }, status: 409 };
        }
        nextRecord = {
          ...proposed,
          captured_at: current?.captured_at || proposed.captured_at,
          owner_anonymous_id_sha256: current?.owner_anonymous_id_sha256 || proposed.owner_anonymous_id_sha256,
          owner_session_id_sha256: current?.owner_session_id_sha256 || proposed.owner_session_id_sha256,
          claimed_user_id_sha256: current?.claimed_user_id_sha256 || proposed.claimed_user_id_sha256,
          claimed_order_id_sha256: current?.claimed_order_id_sha256 || proposed.claimed_order_id_sha256,
          updated_at: nowUTC()
        };
      } else {
        if (!current) return { response: { ok: false, revoked: false, ownership_valid: false, error: "pointer_missing" }, status: 409 };
        if (!this.pointerOwnerValid(current, body?.owner || {})) return { response: { ok: false, revoked: false, ownership_valid: false }, status: 409 };
        nextRecord = current.revoked_at ? current : {
          ...current,
          revoked_at: typeof body?.revoked_at === "string" && Number.isFinite(Date.parse(body.revoked_at)) ? new Date(body.revoked_at).toISOString() : nowUTC(),
          revocation_reason: String(body?.revocation_reason || "explicit_advertising_denial").slice(0, 128),
          updated_at: nowUTC()
        };
      }
      await txn.put("ad_click_pointer", nextRecord);
      if (pathname === "/pointer/revoke") await txn.delete("ad_click_pointer_reservation");
      return { nextRecord };
    });
    if (!mutation.nextRecord) return jsonResponse(mutation.response || { ok: false, error: mutation.error }, mutation.status);
    const nextRecord = mutation.nextRecord;
    const persistCache = body?.persist_cache !== false;
    if (!persistCache) {
      return new Response(JSON.stringify({
        ok: true,
        persisted: pathname === "/pointer/upsert",
        cache_persisted: false,
        revoked: !!nextRecord.revoked_at,
        ownership_valid: pathname === "/pointer/revoke" ? true : void 0
      }), { status: 200, headers: { "Content-Type": "application/json", "Cache-Control": "no-store" } });
    }
    const kv = this.env?.AD_CLICK_KV;
    if (!kv || typeof kv.put !== "function") {
      return new Response(JSON.stringify({ ok: false, error: "pointer_kv_missing" }), { status: 503, headers: { "Content-Type": "application/json" } });
    }
    try {
      await kv.put(`${AD_CLICK_KV_PREFIX}id:${adClickId}`, JSON.stringify(nextRecord), { expirationTtl: ttlSeconds });
    } catch {
      return new Response(JSON.stringify({ ok: false, error: "pointer_kv_write_failed" }), { status: 503, headers: { "Content-Type": "application/json" } });
    }
    return new Response(JSON.stringify({
      ok: true,
      persisted: pathname === "/pointer/upsert",
      cache_persisted: true,
      revoked: !!nextRecord.revoked_at,
      ownership_valid: pathname === "/pointer/revoke" ? true : void 0
    }), { status: 200, headers: { "Content-Type": "application/json", "Cache-Control": "no-store" } });
  }
  async queuePointerOperation(pathname, body) {
    const operation = this.pointerMutationChain.then(() => {
      if (pathname === "/pointer/read") return this.executePointerRead(body);
      if (pathname === "/pointer/reserve") return this.executePointerReserve(body);
      if (pathname === "/pointer/commit") return this.executePointerCommit(body);
      if (pathname === "/pointer/cancel") return this.executePointerCancel(body);
      return this.executePointerMutation(pathname, body);
    });
    this.pointerMutationChain = operation.catch(() => {});
    return operation;
  }
  async executeFirstTouch(body) {
    const identity = firstTouchIdentity(body);
    if (!identity) return jsonResponse({ ok: false, error: "invalid_first_touch_owner" }, 400);
    const candidate = firstTouchCandidate(body, identity);
    if (!candidate) return jsonResponse({ ok: false, error: "invalid_first_touch_record" }, 400);
    const outcome = await this.state.storage.transaction(async (txn) => {
      const stored = await txn.get("attribution_first_touch");
      if (stored !== void 0 && stored !== null && !firstTouchRecordValid(stored, identity)) return { invalid: true };
      if (!stored) {
        const created = { ...candidate, updated_at: nowUTC() };
        await txn.put("attribution_first_touch", created);
        return { record: created, created: true, enriched: false, enriched_fields: [] };
      }
      const next = { ...stored };
      const enrichedFields = [];
      const sameObservation = boundedNonemptyString(stored[FIRST_TOUCH_OBSERVATION_ID_FIELD], 64)
        && stored[FIRST_TOUCH_OBSERVATION_ID_FIELD] === candidate[FIRST_TOUCH_OBSERVATION_ID_FIELD]
        && stored.captured_at === candidate.captured_at;
      for (const field of sameObservation ? FIRST_TOUCH_ATTRIBUTION_FIELDS : []) {
        if (!boundedNonemptyString(next[field], 2048) && boundedNonemptyString(candidate[field], 2048)) {
          next[field] = candidate[field];
          enrichedFields.push(field);
        }
      }
      if (enrichedFields.length) {
        next.updated_at = nowUTC();
        await txn.put("attribution_first_touch", next);
      }
      return { record: next, created: false, enriched: enrichedFields.length > 0, enriched_fields: enrichedFields };
    });
    if (outcome.invalid) return jsonResponse({ ok: false, error: "first_touch_state_invalid" }, 503);
    return jsonResponse({ ok: true, ...outcome });
  }
  async executeIdentityPointer(pathname, body) {
    const identity = stableIdentity(body);
    if (!identity) return jsonResponse({ ok: false, error: "invalid_stable_identity" }, 400);
    if (pathname === "/identity-pointer/read") {
      const stored = await this.state.storage.get("identity_pointer");
      if (stored !== void 0 && stored !== null && !identityPointerRecordValid(stored, identity)) {
        return jsonResponse({ ok: false, error: "identity_pointer_state_invalid" }, 503);
      }
      return jsonResponse({ ok: true, found: !!stored, record: stored || null });
    }
    const candidate = identityPointerCandidate(body);
    if (!candidate) return jsonResponse({ ok: false, error: "invalid_identity_pointer_candidate" }, 400);
    const outcome = await this.state.storage.transaction(async (txn) => {
      const stored = await txn.get("identity_pointer");
      if (stored !== void 0 && stored !== null && !identityPointerRecordValid(stored, identity)) return { invalid: true };
      if (!stored) {
        const created = {
          schema_version: IDENTITY_POINTER_RECORD_SCHEMA_VERSION,
          identity_type: identity.identityType,
          identity_hash: identity.identityHash,
          first_ad_click_id: candidate.adClickId,
          first_captured_at: candidate.capturedAt,
          latest_ad_click_id: candidate.adClickId,
          latest_captured_at: candidate.capturedAt,
          updated_at: nowUTC()
        };
        await txn.put("identity_pointer", created);
        return { record: created, created: true, latest_updated: true };
      }
      if (Date.parse(candidate.capturedAt) <= Date.parse(stored.latest_captured_at)) {
        return { record: stored, created: false, latest_updated: false };
      }
      const next = {
        ...stored,
        latest_ad_click_id: candidate.adClickId,
        latest_captured_at: candidate.capturedAt,
        updated_at: nowUTC()
      };
      await txn.put("identity_pointer", next);
      return { record: next, created: false, latest_updated: true };
    });
    if (outcome.invalid) return jsonResponse({ ok: false, error: "identity_pointer_state_invalid" }, 503);
    return jsonResponse({ ok: true, ...outcome });
  }
  async fetch(request) {
    const url = new URL(request.url);
    if (request.method === "GET" && url.pathname === "/health") {
      let storageReadable = false;
      try {
        await this.state.storage.get("__eden_conversion_health_probe__");
        storageReadable = true;
      } catch {}
      return new Response(JSON.stringify({ ok: storageReadable, schema_version: "eden_conversion_coordinator_v1", storage_readable: storageReadable }), {
        status: storageReadable ? 200 : 503,
        headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
      });
    }
    if (request.method !== "POST" || !["/acquire", "/record", "/restore", "/release", "/pointer/read", "/pointer/upsert", "/pointer/revoke", "/pointer/reserve", "/pointer/commit", "/pointer/cancel", "/attribution/first-touch", "/identity-pointer/upsert", "/identity-pointer/read"].includes(url.pathname)) {
      return new Response("Not found", { status: 404 });
    }
    let body;
    try {
      if (["/attribution/first-touch", "/identity-pointer/upsert", "/identity-pointer/read"].includes(url.pathname)) {
        const parsed = await readBoundedJson(request, COORDINATION_REQUEST_MAX_BYTES);
        if (!parsed.ok) return jsonResponse({ ok: false, error: parsed.error }, parsed.status);
        body = parsed.body;
      } else {
        body = await request.json();
      }
    } catch {
      return new Response(JSON.stringify({ ok: false, error: "invalid_json" }), { status: 400, headers: { "Content-Type": "application/json" } });
    }
    if (url.pathname === "/attribution/first-touch") return this.executeFirstTouch(body);
    if (["/identity-pointer/upsert", "/identity-pointer/read"].includes(url.pathname)) return this.executeIdentityPointer(url.pathname, body);
    if (["/pointer/read", "/pointer/upsert", "/pointer/revoke", "/pointer/reserve", "/pointer/commit", "/pointer/cancel"].includes(url.pathname)) {
      return this.queuePointerOperation(url.pathname, body);
    }
    const token = typeof body?.token === "string" && /^[A-Za-z0-9_-]{16,128}$/.test(body.token) ? body.token : null;
    if (!token) return new Response(JSON.stringify({ ok: false, error: "invalid_token" }), { status: 400, headers: { "Content-Type": "application/json" } });
    const nowMs = Date.now();
    if (url.pathname === "/acquire") {
      const eventName = typeof body?.event_name === "string" && /^[A-Za-z][A-Za-z0-9_]{0,127}$/.test(body.event_name)
        ? body.event_name
        : null;
      if (!eventName) return new Response(JSON.stringify({ ok: false, error: "invalid_event_name" }), { status: 400, headers: { "Content-Type": "application/json" } });
      const requestedTtl = Number(body?.lease_ttl_ms || CONVERSION_COORDINATOR_LEASE_TTL_MS);
      const leaseTtlMs = Math.max(1e4, Math.min(CONVERSION_COORDINATOR_LEASE_TTL_MS, Number.isFinite(requestedTtl) ? requestedTtl : CONVERSION_COORDINATOR_LEASE_TTL_MS));
      const outcome = await this.state.storage.transaction(async (txn) => {
        const current = await txn.get("lease");
        if (current?.token && Number(current.expires_at_ms || 0) > nowMs) {
          return { acquired: false, retry_after_ms: Math.max(250, Number(current.expires_at_ms) - nowMs) };
        }
        const record = await txn.get(`conversion:${eventName}`);
        await txn.put("lease", { token, acquired_at_ms: nowMs, expires_at_ms: nowMs + leaseTtlMs });
        return { acquired: true, lease_ttl_ms: leaseTtlMs, record: record || null };
      });
      return new Response(JSON.stringify(outcome), {
        status: outcome.acquired ? 200 : 409,
        headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
      });
    }
    if (url.pathname === "/record") {
      const eventName = typeof body?.event_name === "string" && /^[A-Za-z][A-Za-z0-9_]{0,127}$/.test(body.event_name)
        ? body.event_name
        : null;
      const record = body?.record;
      const recordJson = record && typeof record === "object" && !Array.isArray(record)
        ? JSON.stringify(record)
        : "";
      const validRecord = !!eventName
        && recordJson.length > 0
        && new TextEncoder().encode(recordJson).byteLength <= CONVERSION_COORDINATOR_RECORD_MAX_BYTES
        && record.schema_version === "eden_conversion_dedup_v4"
        && record.event === eventName
        && record.signal_hashes && typeof record.signal_hashes === "object" && !Array.isArray(record.signal_hashes)
        && record.status_ranks && typeof record.status_ranks === "object" && !Array.isArray(record.status_ranks)
        && ["segment_delivery_unacknowledged", "segment_acknowledged_pending_persistence", "segment_acknowledged"].includes(record.delivery_state);
      if (!validRecord) return new Response(JSON.stringify({ ok: false, error: "invalid_conversion_record" }), { status: 400, headers: { "Content-Type": "application/json" } });
      const recorded = await this.state.storage.transaction(async (txn) => {
        const current = await txn.get("lease");
        if (!current?.token || current.token !== token || Number(current.expires_at_ms || 0) <= nowMs) return false;
        await txn.put(`conversion:${eventName}`, record);
        return true;
      });
      return new Response(JSON.stringify({ recorded }), {
        status: recorded ? 200 : 409,
        headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
      });
    }
    if (url.pathname === "/restore") {
      const eventName = typeof body?.event_name === "string" && /^[A-Za-z][A-Za-z0-9_]{0,127}$/.test(body.event_name)
        ? body.event_name
        : null;
      const record = body?.record;
      const recordJson = record && typeof record === "object" && !Array.isArray(record)
        ? JSON.stringify(record)
        : "";
      const validPriorRecord = record === null || (!!eventName
        && recordJson.length > 0
        && new TextEncoder().encode(recordJson).byteLength <= CONVERSION_COORDINATOR_RECORD_MAX_BYTES
        && record.schema_version === "eden_conversion_dedup_v4"
        && record.event === eventName
        && record.signal_hashes && typeof record.signal_hashes === "object" && !Array.isArray(record.signal_hashes)
        && record.status_ranks && typeof record.status_ranks === "object" && !Array.isArray(record.status_ranks));
      if (!eventName || !validPriorRecord) return new Response(JSON.stringify({ ok: false, error: "invalid_conversion_restore" }), { status: 400, headers: { "Content-Type": "application/json" } });
      const restored = await this.state.storage.transaction(async (txn) => {
        const current = await txn.get("lease");
        if (!current?.token || current.token !== token || Number(current.expires_at_ms || 0) <= nowMs) return false;
        if (record === null) await txn.delete(`conversion:${eventName}`);
        else await txn.put(`conversion:${eventName}`, record);
        return true;
      });
      return new Response(JSON.stringify({ restored }), {
        status: restored ? 200 : 409,
        headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
      });
    }
    const released = await this.state.storage.transaction(async (txn) => {
      const current = await txn.get("lease");
      if (!current?.token || current.token !== token) return false;
      await txn.delete("lease");
      return true;
    });
    return new Response(JSON.stringify({ released }), {
      status: released ? 200 : 409,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
    });
  }
};
