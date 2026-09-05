import assert from "node:assert/strict";
import test from "node:test";
import { encodeLinePayloadV5, newDictionaryState } from "./dictionary.js";
import { label } from "./labels.js";
import type { Payload } from "./payload.js";

test("encodeLinePayloadV5 sends only new series definitions", () => {
  const state = newDictionaryState();
  state.sessionID = "test-session";

  const first: Payload = {
    counters: [{ metric: "requests", value: 1, labels: [label("env", "prod")], timestamp: 1730000000 }],
    values: [],
    uniques: [],
  };
  const firstBody = encodeLinePayloadV5(first, state).toString("utf8");
  assert.match(firstBody, /^H\|5\|s\|test-session\|1\n/);
  assert.equal((firstBody.match(/\nS\|/g) ?? []).length, 1);

  const second: Payload = {
    counters: [
      { metric: "requests", value: 2, labels: [label("env", "prod")], timestamp: 1730000001 },
      { metric: "latency_ms", value: 123, labels: [label("env", "prod")], timestamp: 1730000001 },
    ],
    values: [],
    uniques: [],
  };
  const secondBody = encodeLinePayloadV5(second, state).toString("utf8");
  assert.equal((secondBody.match(/\nS\|/g) ?? []).length, 1);
  assert.match(secondBody, /\nS\|1\|latency_ms\|/);
  assert.doesNotMatch(secondBody, /\nS\|0\|requests\|/);
});

test("encodeLinePayloadV5 uses the dedicated sparse event type", () => {
  const state = newDictionaryState();
  state.sessionID = "test-session";
  const payload: Payload = {
    counters: [],
    values: [{ metric: "capacity_kb", value: 1024, sparse: true, success: false, labels: ["mount=/"], timestamp: 1730000000 }],
    uniques: [],
  };

  const body = encodeLinePayloadV5(payload, state).toString("utf8");
  assert.match(body, /\nS\|0\|capacity_kb\|mount=\/\n/);
  assert.match(body, /\ns\|0\|1024\|1730000000\n/);
});

test("encodeLinePayloadV5 uses the outcome event type for success", () => {
  const state = newDictionaryState();
  state.sessionID = "test-session";
  const payload: Payload = {
    counters: [],
    values: [
      { metric: "payment", value: 100, sparse: false, success: true, labels: ["provider=stripe"], timestamp: 1730000000 },
      { metric: "payment", value: 0, sparse: false, success: true, labels: ["provider=stripe"], timestamp: 1730000001 },
    ],
    uniques: [],
  };

  const body = encodeLinePayloadV5(payload, state).toString("utf8");
  assert.match(body, /\nS\|0\|payment\|provider=stripe\n/);
  assert.match(body, /\no\|0\|100\|1730000000\n/);
  assert.match(body, /\no\|0\|0\|1730000001\n/);
});
