#!/usr/bin/env node
import assert from "node:assert/strict";

const url = process.argv[2] || "https://app.eden.health/eden-health-check";
const expectedRevision = "v556-phani-analytics-compat-20260715";

let response;
for (let attempt = 1; attempt <= 6; attempt += 1) {
  try {
    response = await fetch(url, { headers: { "Cache-Control": "no-cache" } });
    if (response.ok) break;
  } catch (error) {
    if (attempt === 6) throw error;
  }
  await new Promise((resolve) => setTimeout(resolve, 5_000));
}

assert.ok(response?.ok, `health endpoint did not return 2xx: ${response?.status || "network failure"}`);
const health = await response.json();
assert.equal(health.ok, true, "Worker health is not ok");
assert.equal(health.version, "5.56", "unexpected Worker version");
assert.equal(health.release_revision, expectedRevision, "deployed release revision does not match this repository");
assert.equal(health.browser_segment_delivery_mode, "sync", "browser Segment delivery is not synchronous");
assert.equal(health.segment_write_key_configured, true, "Segment write key is missing");
assert.equal(health.ready, true, `Worker readiness failed: ${(health.readiness_missing || []).join(", ")}`);

console.log(`PASS live health ${health.version} ${health.release_revision}`);
