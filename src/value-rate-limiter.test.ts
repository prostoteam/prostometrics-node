import assert from "node:assert/strict";
import test from "node:test";
import { MAX_VALUE_EVENTS_PER_SECOND, ValueRateLimiter } from "./value-rate-limiter.js";

test("value rate limiter caps one second window and resets", () => {
  const limiter = new ValueRateLimiter();
  const now = 1_700_000_000_000;

  for (let i = 0; i < MAX_VALUE_EVENTS_PER_SECOND; i += 1) {
    assert.equal(limiter.allow(now), true);
  }
  assert.equal(limiter.allow(now), false);
  assert.equal(limiter.allow(now + 1_000), true);
  assert.equal(limiter.allow(now), false);
});
