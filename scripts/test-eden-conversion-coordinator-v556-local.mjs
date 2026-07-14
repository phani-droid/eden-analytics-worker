#!/usr/bin/env node
import assert from "node:assert/strict";
import { ConversionCoordinator } from "../cloudflare-workers/eden-conversion-coordinator.js";

function clone(value) {
  return value === undefined ? undefined : structuredClone(value);
}

class TransactionalMemoryStorage {
  constructor(seed = {}) {
    this.records = new Map(Object.entries(seed).map(([key, value]) => [key, clone(value)]));
    this.transactionChain = Promise.resolve();
    this.transactionCount = 0;
    this.directMutationCount = 0;
  }

  async get(key) {
    return clone(this.records.get(key));
  }

  async put() {
    this.directMutationCount += 1;
    throw new Error("test storage forbids mutation outside transaction");
  }

  async delete() {
    this.directMutationCount += 1;
    throw new Error("test storage forbids mutation outside transaction");
  }

  seed(key, value) {
    this.records.set(key, clone(value));
  }

  async transaction(callback) {
    const operation = this.transactionChain.then(async () => {
      const working = new Map([...this.records].map(([key, value]) => [key, clone(value)]));
      const txn = {
        get: async (key) => clone(working.get(key)),
        put: async (key, value) => working.set(key, clone(value)),
        delete: async (key) => working.delete(key)
      };
      const result = await callback(txn);
      this.records = working;
      this.transactionCount += 1;
      return result;
    });
    this.transactionChain = operation.catch(() => {});
    return operation;
  }
}

function makeCoordinator(seed = {}, env = {}) {
  const storage = new TransactionalMemoryStorage(seed);
  return { storage, coordinator: new ConversionCoordinator({ storage }, env) };
}

async function post(coordinator, pathname, body) {
  const response = await coordinator.fetch(new Request(`https://conversion-coordinator.internal${pathname}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  }));
  return { response, body: await response.json() };
}

function pointerRecord(adClickId, ownerHash, capturedAt = "2026-07-11T10:00:00.000Z") {
  return {
    schema_version: "eden_ad_click_pointer_v2",
    ad_click_id: adClickId,
    captured_at: capturedAt,
    owner_anonymous_id_sha256: ownerHash
  };
}

async function testFirstTouchIsAtomicAcrossGoogleBingAndAffiliateTouches() {
  const { storage, coordinator } = makeCoordinator();
  const ownerHash = "f".repeat(64);
  const initial = await post(coordinator, "/attribution/first-touch", {
    owner_scope: "anonymous_id_sha256",
    owner_hash: ownerHash,
    captured_at: "2026-07-11T09:00:00.000Z",
    observation_id_sha256: "1".repeat(64),
    gclid: "fixture-gclid-first-touch",
    gbraid: "fixture-gbraid-first-touch",
    utm_source: "google",
    utm_medium: "cpc",
    utm_campaign: "first-campaign"
  });
  assert.equal(initial.response.status, 200);
  assert.equal(initial.body.created, true);
  assert.equal(initial.body.record.gclid, "fixture-gclid-first-touch");
  assert.equal(initial.body.record.gbraid, "fixture-gbraid-first-touch");

  const bing = await post(coordinator, "/attribution/first-touch", {
    owner_scope: "anonymous_id_sha256",
    owner_hash: ownerHash,
    captured_at: "2026-07-11T10:00:00.000Z",
    observation_id_sha256: "2".repeat(64),
    msclkid: "fixture-msclkid-later",
    utm_source: "bing",
    utm_term: "bing-must-not-fill"
  });
  assert.equal(bing.response.status, 200);
  assert.equal(bing.body.created, false);
  assert.equal(bing.body.enriched, false);
  assert.deepEqual(bing.body.enriched_fields, []);

  const affiliate = await post(coordinator, "/attribution/first-touch", {
    owner_scope: "anonymous_id_sha256",
    owner_hash: ownerHash,
    captured_at: "2026-07-11T11:00:00.000Z",
    observation_id_sha256: "3".repeat(64),
    irclickid: "fixture-affiliate-click-later",
    utm_source: "affiliate",
    utm_term: "affiliate-must-not-fill",
    utm_content: "affiliate-content-must-not-fill"
  });
  assert.equal(affiliate.response.status, 200);
  assert.equal(affiliate.body.enriched, false);
  assert.deepEqual(affiliate.body.record, initial.body.record, "later touches must not create a hybrid first-touch envelope");
  assert.equal(affiliate.body.record.msclkid, undefined);
  assert.equal(affiliate.body.record.irclickid, undefined);
  assert.equal(affiliate.body.record.utm_term, undefined);
  assert.equal(affiliate.body.record.utm_content, undefined);
  assert.equal(storage.directMutationCount, 0);
}

async function testConcurrentFirstTouchAndImmutableEnrichment() {
  const { storage, coordinator } = makeCoordinator();
  const ownerHash = "a".repeat(64);
  const first = {
    owner_scope: "anonymous_id_sha256",
    owner_hash: ownerHash,
    captured_at: "2026-07-11T10:00:00.000Z",
    utm_source: "google",
    utm_campaign: "spring"
  };
  const competing = {
    owner_scope: "anonymous_id_sha256",
    owner_hash: ownerHash,
    captured_at: "2026-07-11T10:01:00.000Z",
    utm_source: "bing",
    utm_medium: "cpc"
  };
  const [left, right] = await Promise.all([
    post(coordinator, "/attribution/first-touch", first),
    post(coordinator, "/attribution/first-touch", competing)
  ]);
  assert.equal(left.response.status, 200);
  assert.equal(right.response.status, 200);
  assert.equal(Number(left.body.created) + Number(right.body.created), 1, "exactly one concurrent write establishes first touch");

  const winningResponse = left.body.created ? left.body : right.body;
  const authoritative = winningResponse.record;
  assert.ok(["google", "bing"].includes(authoritative.utm_source));
  assert.equal(
    authoritative.captured_at,
    authoritative.utm_source === "google" ? first.captured_at : competing.captured_at,
    "the winning first-touch timestamp remains paired with its immutable source"
  );
  assert.equal(authoritative.utm_campaign, authoritative.utm_source === "google" ? "spring" : undefined);
  assert.equal(authoritative.utm_medium, authoritative.utm_source === "bing" ? "cpc" : undefined);

  const enriched = await post(coordinator, "/attribution/first-touch", {
    ...competing,
    captured_at: "2026-07-11T11:00:00.000Z",
    utm_source: "affiliate",
    utm_campaign: "replacement-must-not-win",
    utm_term: "glp-1",
    utm_content: "hero"
  });
  assert.equal(enriched.response.status, 200);
  assert.equal(enriched.body.created, false);
  assert.equal(enriched.body.enriched, false);
  assert.deepEqual(enriched.body.enriched_fields, []);
  assert.equal(enriched.body.record.utm_source, authoritative.utm_source);
  assert.equal(enriched.body.record.utm_campaign, authoritative.utm_campaign);
  assert.equal(enriched.body.record.captured_at, authoritative.captured_at);

  const conflict = await post(coordinator, "/attribution/first-touch", {
    ...first,
    utm_source: "meta",
    utm_medium: "paid_social",
    utm_campaign: "overwrite-attempt",
    utm_term: "overwrite-attempt",
    utm_content: "overwrite-attempt"
  });
  assert.equal(conflict.body.enriched, false);
  assert.deepEqual(conflict.body.record, enriched.body.record, "conflicting nonempty values are retained unchanged");
  assert.equal(storage.directMutationCount, 0);
  assert.ok(storage.transactionCount >= 4);
}

async function testFirstTouchIdempotentSameObservationReplayAndEnrichment() {
  const { storage, coordinator } = makeCoordinator();
  const ownerHash = "9".repeat(64);
  const observationId = "8".repeat(64);
  const first = {
    owner_scope: "anonymous_id_sha256",
    owner_hash: ownerHash,
    captured_at: "2026-07-11T12:00:00.000Z",
    observation_id_sha256: observationId,
    gclid: "fixture-idempotent-gclid",
    utm_source: "google"
  };
  const created = await post(coordinator, "/attribution/first-touch", first);
  const replay = await post(coordinator, "/attribution/first-touch", first);
  assert.equal(created.body.created, true);
  assert.equal(replay.body.created, false);
  assert.equal(replay.body.enriched, false);
  assert.deepEqual(replay.body.record, created.body.record);

  const enriched = await post(coordinator, "/attribution/first-touch", {
    ...first,
    utm_medium: "cpc",
    utm_campaign: "same-observation-campaign"
  });
  assert.equal(enriched.body.enriched, true);
  assert.deepEqual(enriched.body.enriched_fields.sort(), ["utm_campaign", "utm_medium"]);
  assert.equal(enriched.body.record.gclid, "fixture-idempotent-gclid");
  assert.equal(enriched.body.record.utm_medium, "cpc");

  const enrichedReplay = await post(coordinator, "/attribution/first-touch", {
    ...first,
    utm_medium: "cpc",
    utm_campaign: "same-observation-campaign"
  });
  assert.equal(enrichedReplay.body.enriched, false);
  assert.deepEqual(enrichedReplay.body.record, enriched.body.record);
  assert.equal(storage.directMutationCount, 0);
}

async function testFirstTouchValidationAndCorruptionFailClosed() {
  const ownerHash = "b".repeat(64);
  const malformed = makeCoordinator();
  const oversizedRequest = await post(malformed.coordinator, "/attribution/first-touch", {
    owner_scope: "anonymous_id_sha256",
    owner_hash: ownerHash,
    captured_at: "2026-07-11T10:00:00.000Z",
    utm_campaign: "x".repeat(17_000)
  });
  assert.equal(oversizedRequest.response.status, 413);
  assert.equal(oversizedRequest.body.error, "request_too_large");

  for (const corruptRecord of [
    { schema_version: "wrong" },
    {
      schema_version: "eden_attribution_first_touch_v1",
      owner_scope: "anonymous_id_sha256",
      owner_hash: ownerHash,
      captured_at: "2026-07-11T10:00:00.000Z",
      updated_at: "2026-07-11T10:00:00.000Z",
      utm_campaign: "x".repeat(33_000)
    }
  ]) {
    const { storage, coordinator } = makeCoordinator({ attribution_first_touch: corruptRecord });
    const result = await post(coordinator, "/attribution/first-touch", {
      owner_scope: "anonymous_id_sha256",
      owner_hash: ownerHash,
      captured_at: "2026-07-11T10:01:00.000Z",
      utm_source: "google"
    });
    assert.equal(result.response.status, 503);
    assert.equal(result.body.error, "first_touch_state_invalid");
    assert.deepEqual(await storage.get("attribution_first_touch"), corruptRecord, "corrupt state must never be overwritten");
    assert.equal(storage.directMutationCount, 0);
  }
}

async function testStableIdentityValidationAndPointerOrdering() {
  const { storage, coordinator } = makeCoordinator();
  const identityHash = "c".repeat(64);
  for (const invalidIdentity of [
    { identity_type: "anonymous_id_sha256", identity_hash: identityHash },
    { identity_type: "user_id_sha256", identity_hash: "C".repeat(64) },
    { identity_type: "order_id_sha256", identity_hash: "short" }
  ]) {
    const result = await post(coordinator, "/identity-pointer/upsert", {
      ...invalidIdentity,
      ad_click_id: "adclk2_candidate001",
      captured_at: "2026-07-11T10:00:00.000Z"
    });
    assert.equal(result.response.status, 400);
    assert.equal(result.body.error, "invalid_stable_identity");
  }

  const initial = await post(coordinator, "/identity-pointer/upsert", {
    identity_type: "user_id_sha256",
    identity_hash: identityHash,
    ad_click_id: "adclk2_first0001",
    captured_at: "2026-07-11T10:00:00.000Z"
  });
  assert.equal(initial.response.status, 200);
  assert.equal(initial.body.created, true);
  assert.equal(initial.body.latest_updated, true);

  const latest = await post(coordinator, "/identity-pointer/upsert", {
    identity_type: "user_id_sha256",
    identity_hash: identityHash,
    ad_click_id: "adclk2_latest002",
    captured_at: "2026-07-11T12:00:00.000Z"
  });
  assert.equal(latest.body.latest_updated, true);
  assert.equal(latest.body.record.first_ad_click_id, "adclk2_first0001");
  assert.equal(latest.body.record.first_captured_at, "2026-07-11T10:00:00.000Z");
  assert.equal(latest.body.record.latest_ad_click_id, "adclk2_latest002");
  assert.equal(latest.body.record.latest_captured_at, "2026-07-11T12:00:00.000Z");

  const older = await post(coordinator, "/identity-pointer/upsert", {
    identity_type: "user_id_sha256",
    identity_hash: identityHash,
    ad_click_id: "adclk2_older0003",
    captured_at: "2026-07-11T11:00:00.000Z"
  });
  assert.equal(older.response.status, 200);
  assert.equal(older.body.latest_updated, false);
  assert.deepEqual(older.body.record, latest.body.record, "an older capture cannot replace latest");

  const read = await post(coordinator, "/identity-pointer/read", {
    identity_type: "user_id_sha256",
    identity_hash: identityHash
  });
  assert.equal(read.response.status, 200);
  assert.equal(read.body.found, true);
  assert.deepEqual(read.body.record, latest.body.record);
  assert.equal(storage.directMutationCount, 0);
  assert.equal(storage.transactionCount, 3, "only valid upserts mutate through transactions");
}

async function testIdentityPointerCorruptionFailClosed() {
  const identityHash = "d".repeat(64);
  const corruptRecord = {
    schema_version: "eden_identity_pointer_v1",
    identity_type: "order_id_sha256",
    identity_hash: identityHash,
    first_ad_click_id: "adclk2_first0001",
    first_captured_at: "2026-07-11T10:00:00.000Z",
    latest_ad_click_id: "adclk2_latest002",
    latest_captured_at: "2026-07-11T12:00:00.000Z",
    updated_at: "2026-07-11T12:00:00.000Z",
    corrupt_payload: "x".repeat(33_000)
  };
  const { storage, coordinator } = makeCoordinator({ identity_pointer: corruptRecord });
  const result = await post(coordinator, "/identity-pointer/upsert", {
    identity_type: "order_id_sha256",
    identity_hash: identityHash,
    ad_click_id: "adclk2_newer0004",
    captured_at: "2026-07-11T13:00:00.000Z"
  });
  assert.equal(result.response.status, 503);
  assert.equal(result.body.error, "identity_pointer_state_invalid");
  assert.deepEqual(await storage.get("identity_pointer"), corruptRecord);
  assert.equal(storage.directMutationCount, 0);
}

async function testPointerReserveCommitBoundaryAndReplay() {
  const { storage, coordinator } = makeCoordinator();
  const adClickId = "adclk2_reservecommit01";
  const reservationId = "adrsrv_reservecommit01";
  const proposed = pointerRecord(adClickId, "1".repeat(64));

  const reserved = await post(coordinator, "/pointer/reserve", {
    ad_click_id: adClickId,
    reservation_id: reservationId,
    ttl_seconds: 3600,
    persist_cache: false,
    proposed_record: proposed
  });
  assert.equal(reserved.response.status, 200);
  assert.deepEqual(reserved.body, {
    ok: true,
    reserved: true,
    reservation_id: reservationId,
    reused: false
  });
  assert.equal(await storage.get("ad_click_pointer"), undefined, "reserve must not create or activate the canonical pointer");
  assert.equal((await storage.get("ad_click_pointer_reservation")).ad_click_id, adClickId);

  const committed = await post(coordinator, "/pointer/commit", {
    ad_click_id: adClickId,
    reservation_id: reservationId
  });
  assert.equal(committed.response.status, 200);
  assert.deepEqual(committed.body, {
    ok: true,
    committed: true,
    cache_persisted: false,
    replay: false
  });
  const canonical = await storage.get("ad_click_pointer");
  assert.equal(canonical.ad_click_id, adClickId);
  assert.equal(canonical.owner_anonymous_id_sha256, proposed.owner_anonymous_id_sha256);
  assert.equal(await storage.get("ad_click_pointer_reservation"), undefined);

  const replay = await post(coordinator, "/pointer/commit", {
    ad_click_id: adClickId,
    reservation_id: reservationId
  });
  assert.equal(replay.response.status, 200);
  assert.deepEqual(replay.body, {
    ok: true,
    committed: true,
    cache_persisted: false,
    replay: true
  });
  assert.deepEqual(await storage.get("ad_click_pointer"), canonical, "idempotent replay must not alter canonical state");
  assert.equal(storage.directMutationCount, 0);
}

async function testPointerReservationCancel() {
  const { storage, coordinator } = makeCoordinator();
  const adClickId = "adclk2_cancel000001";
  const reservationId = "adrsrv_cancel000001";
  const reserved = await post(coordinator, "/pointer/reserve", {
    ad_click_id: adClickId,
    reservation_id: reservationId,
    ttl_seconds: 3600,
    persist_cache: false,
    proposed_record: pointerRecord(adClickId, "2".repeat(64))
  });
  assert.equal(reserved.response.status, 200);

  const cancelled = await post(coordinator, "/pointer/cancel", {
    ad_click_id: adClickId,
    reservation_id: reservationId
  });
  assert.equal(cancelled.response.status, 200);
  assert.deepEqual(cancelled.body, { ok: true, cancelled: true });
  assert.equal(await storage.get("ad_click_pointer_reservation"), undefined);
  assert.equal(await storage.get("ad_click_pointer"), undefined);

  const commitAfterCancel = await post(coordinator, "/pointer/commit", {
    ad_click_id: adClickId,
    reservation_id: reservationId
  });
  assert.equal(commitAfterCancel.response.status, 409);
  assert.equal(commitAfterCancel.body.error, "pointer_reservation_missing");
}

async function testPointerReservationBusyAndOwnerConflict() {
  const adClickId = "adclk2_busyowner001";
  const ownerA = "3".repeat(64);
  const ownerB = "4".repeat(64);
  const { storage, coordinator } = makeCoordinator();
  const first = await post(coordinator, "/pointer/reserve", {
    ad_click_id: adClickId,
    reservation_id: "adrsrv_busyowner0001",
    ttl_seconds: 3600,
    persist_cache: false,
    proposed_record: pointerRecord(adClickId, ownerA)
  });
  assert.equal(first.response.status, 200);

  const busy = await post(coordinator, "/pointer/reserve", {
    ad_click_id: adClickId,
    reservation_id: "adrsrv_busyowner0002",
    ttl_seconds: 3600,
    persist_cache: false,
    proposed_record: pointerRecord(adClickId, ownerA)
  });
  assert.equal(busy.response.status, 409);
  assert.equal(busy.body.error, "pointer_reservation_busy");
  assert.equal((await storage.get("ad_click_pointer_reservation")).reservation_id, "adrsrv_busyowner0001");
  assert.equal(await storage.get("ad_click_pointer"), undefined);

  const seeded = makeCoordinator({ ad_click_pointer: pointerRecord(adClickId, ownerA) });
  const conflict = await post(seeded.coordinator, "/pointer/reserve", {
    ad_click_id: adClickId,
    reservation_id: "adrsrv_ownerconflict1",
    ttl_seconds: 3600,
    persist_cache: false,
    proposed_record: pointerRecord(adClickId, ownerB)
  });
  assert.equal(conflict.response.status, 409);
  assert.equal(conflict.body.owner_conflict, true);
  assert.deepEqual(conflict.body.conflict_fields, ["owner_anonymous_id_sha256"]);
  assert.equal(await seeded.storage.get("ad_click_pointer_reservation"), undefined);
  assert.equal((await seeded.storage.get("ad_click_pointer")).owner_anonymous_id_sha256, ownerA);
}

async function testPointerRevocationInvalidatesQueuedReservation() {
  const adClickId = "adclk2_revokerace001";
  const ownerHash = "6".repeat(64);
  const canonical = pointerRecord(adClickId, ownerHash);
  const { storage, coordinator } = makeCoordinator({ ad_click_pointer: canonical });
  const reservationId = "adrsrv_revokerace001";
  const reserved = await post(coordinator, "/pointer/reserve", {
    ad_click_id: adClickId,
    reservation_id: reservationId,
    ttl_seconds: 3600,
    persist_cache: false,
    proposed_record: { ...canonical, utm_campaign: "queued-campaign" }
  });
  assert.equal(reserved.response.status, 200);

  const revoked = await post(coordinator, "/pointer/revoke", {
    ad_click_id: adClickId,
    ttl_seconds: 3600,
    persist_cache: false,
    owner: { anonymous_id_sha256: ownerHash },
    revocation_reason: "adversarial_test"
  });
  assert.equal(revoked.response.status, 200);
  assert.equal(revoked.body.revoked, true);
  assert.equal(await storage.get("ad_click_pointer_reservation"), undefined, "revocation must invalidate the queued reservation atomically");

  const staleCommit = await post(coordinator, "/pointer/commit", {
    ad_click_id: adClickId,
    reservation_id: reservationId
  });
  assert.equal(staleCommit.response.status, 409);
  assert.equal(staleCommit.body.error, "pointer_reservation_missing");
  const after = await storage.get("ad_click_pointer");
  assert.equal(after.revoked_at !== undefined, true);
  assert.equal(after.utm_campaign, undefined, "stale commit must not erase revocation with queued attribution");
}

async function testPointerCommitRejectsInterveningOwnerMutation() {
  const adClickId = "adclk2_ownerrace0001";
  const anonymousHash = "7".repeat(64);
  const userHash = "8".repeat(64);
  const canonical = pointerRecord(adClickId, anonymousHash);
  const { storage, coordinator } = makeCoordinator({ ad_click_pointer: canonical });
  const reservationId = "adrsrv_ownerrace0001";
  const reserved = await post(coordinator, "/pointer/reserve", {
    ad_click_id: adClickId,
    reservation_id: reservationId,
    ttl_seconds: 3600,
    persist_cache: false,
    proposed_record: { ...canonical, utm_campaign: "reserved-before-owner-claim" }
  });
  assert.equal(reserved.response.status, 200);

  const ownerMutation = await post(coordinator, "/pointer/upsert", {
    ad_click_id: adClickId,
    ttl_seconds: 3600,
    persist_cache: false,
    proposed_record: { ...canonical, claimed_user_id_sha256: userHash }
  });
  assert.equal(ownerMutation.response.status, 200);
  assert.equal(ownerMutation.body.persisted, true);

  const staleCommit = await post(coordinator, "/pointer/commit", {
    ad_click_id: adClickId,
    reservation_id: reservationId
  });
  assert.equal(staleCommit.response.status, 409);
  assert.equal(staleCommit.body.error, "pointer_reservation_stale");
  assert.equal(await storage.get("ad_click_pointer_reservation"), undefined, "stale reservation must be cancelled during compare-and-reject");
  const after = await storage.get("ad_click_pointer");
  assert.equal(after.claimed_user_id_sha256, userHash);
  assert.equal(after.utm_campaign, undefined, "stale commit must not erase the intervening owner mutation");
}

async function testExpiredPointerReservationCanRetry() {
  const realDateNow = Date.now;
  let nowMs = Date.parse("2026-07-11T10:00:00.000Z");
  Date.now = () => nowMs;
  try {
    const { storage, coordinator } = makeCoordinator();
    const adClickId = "adclk2_expiryretry01";
    const firstReservationId = "adrsrv_expiryretry01";
    const secondReservationId = "adrsrv_expiryretry02";
    const proposed = pointerRecord(adClickId, "5".repeat(64));
    const first = await post(coordinator, "/pointer/reserve", {
      ad_click_id: adClickId,
      reservation_id: firstReservationId,
      ttl_seconds: 3600,
      persist_cache: false,
      proposed_record: proposed
    });
    assert.equal(first.response.status, 200);

    nowMs += 120_001;
    const expiredCommit = await post(coordinator, "/pointer/commit", {
      ad_click_id: adClickId,
      reservation_id: firstReservationId
    });
    assert.equal(expiredCommit.response.status, 409);
    assert.equal(expiredCommit.body.error, "pointer_reservation_expired");
    assert.equal(await storage.get("ad_click_pointer"), undefined);

    const retry = await post(coordinator, "/pointer/reserve", {
      ad_click_id: adClickId,
      reservation_id: secondReservationId,
      ttl_seconds: 3600,
      persist_cache: false,
      proposed_record: proposed
    });
    assert.equal(retry.response.status, 200);
    assert.equal(retry.body.reused, false);
    assert.equal((await storage.get("ad_click_pointer_reservation")).reservation_id, secondReservationId);

    const committed = await post(coordinator, "/pointer/commit", {
      ad_click_id: adClickId,
      reservation_id: secondReservationId
    });
    assert.equal(committed.response.status, 200);
    assert.equal(committed.body.replay, false);
    assert.equal((await storage.get("ad_click_pointer")).ad_click_id, adClickId);
  } finally {
    Date.now = realDateNow;
  }
}

async function testExistingCoordinatorContractsRemainUsable() {
  const { storage, coordinator } = makeCoordinator();
  const acquired = await post(coordinator, "/acquire", {
    token: "legacy_token_00000001",
    event_name: "OS_purchase",
    lease_ttl_ms: 120_000
  });
  assert.equal(acquired.response.status, 200);
  assert.equal(acquired.body.acquired, true);
  const conversionRecord = {
    schema_version: "eden_conversion_dedup_v4",
    event: "OS_purchase",
    signal_hashes: { "property:payment_status": "fixture-hash" },
    status_ranks: { "property:payment_status": 1 },
    delivery_state: "segment_delivery_unacknowledged"
  };
  const recorded = await post(coordinator, "/record", {
    token: "legacy_token_00000001",
    event_name: "OS_purchase",
    record: conversionRecord
  });
  assert.equal(recorded.response.status, 200);
  assert.equal(recorded.body.recorded, true);
  const released = await post(coordinator, "/release", {
    token: "legacy_token_00000001",
    event_name: "OS_purchase"
  });
  assert.equal(released.response.status, 200);
  assert.equal(released.body.released, true);
  const reacquired = await post(coordinator, "/acquire", {
    token: "legacy_token_00000002",
    event_name: "OS_purchase",
    lease_ttl_ms: 120_000
  });
  assert.equal(reacquired.response.status, 200);
  assert.deepEqual(reacquired.body.record, conversionRecord, "legacy conversion records remain authoritative after release");

  const pointer = await post(coordinator, "/pointer/upsert", {
    ad_click_id: "adclk2_legacy001",
    ttl_seconds: 3600,
    persist_cache: false,
    proposed_record: {
      schema_version: "eden_ad_click_pointer_v2",
      ad_click_id: "adclk2_legacy001",
      captured_at: "2026-07-11T10:00:00.000Z",
      owner_anonymous_id_sha256: "e".repeat(64)
    }
  });
  assert.equal(pointer.response.status, 200);
  assert.equal(pointer.body.persisted, true);
  assert.equal(pointer.body.cache_persisted, false);
  assert.equal(storage.directMutationCount, 0, "legacy pointer and conversion mutations also remain transactional");
}

const tests = [
  ["atomic Google first touch rejects Bing and affiliate field filling", testFirstTouchIsAtomicAcrossGoogleBingAndAffiliateTouches],
  ["concurrent first-touch writes and immutable enrichment", testConcurrentFirstTouchAndImmutableEnrichment],
  ["idempotent same-observation first-touch replay and enrichment", testFirstTouchIdempotentSameObservationReplayAndEnrichment],
  ["first-touch validation and corruption fail-closed", testFirstTouchValidationAndCorruptionFailClosed],
  ["stable identity validation and latest-pointer ordering", testStableIdentityValidationAndPointerOrdering],
  ["identity-pointer corruption fail-closed", testIdentityPointerCorruptionFailClosed],
  ["pointer reserve -> commit boundary and idempotent replay", testPointerReserveCommitBoundaryAndReplay],
  ["pointer reservation cancel", testPointerReservationCancel],
  ["pointer reservation busy and owner conflict", testPointerReservationBusyAndOwnerConflict],
  ["pointer revocation invalidates queued reservation", testPointerRevocationInvalidatesQueuedReservation],
  ["pointer commit rejects intervening owner mutation", testPointerCommitRejectsInterveningOwnerMutation],
  ["expired pointer reservation retry", testExpiredPointerReservationCanRetry],
  ["existing pointer and conversion behavior", testExistingCoordinatorContractsRemainUsable]
];

for (const [name, test] of tests) {
  await test();
  console.log(`ok - ${name}`);
}
console.log(`PASS ${tests.length}/${tests.length} ConversionCoordinator v5.56 local contract tests`);
