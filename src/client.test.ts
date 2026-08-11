import assert from "node:assert/strict";
import test from "node:test";
import { Client } from "./client.js";
import {
  DEFAULT_CLIENT_BACKOFF_MAX_DELAY_MS,
  DEFAULT_OUTAGE_BUFFER_MAX_AGE_MS,
  DEFAULT_OUTAGE_BUFFER_MAX_BYTES,
  DEFAULT_OUTAGE_BUFFER_MAX_EVENTS,
  DEFAULT_REPLAY_INTERVAL_MS,
  DEFAULT_RETRY_BASE_DELAY_MS,
} from "./constants.js";
import { HTTPTransportError, StopIngestError } from "./errors.js";
import { label } from "./labels.js";
import type { Payload } from "./payload.js";
import type { Transport } from "./transport.js";
import { HTTPTransport } from "./transport.js";

class MemoryTransport implements Transport {
  readonly batches: Payload[] = [];

  async send(payload: Payload): Promise<void> {
    this.batches.push(payload);
  }
}

class ScriptedTransport implements Transport {
  readonly batchIDs: string[] = [];
  calls = 0;

  constructor(private readonly errs: unknown[]) {}

  async send(payload: Payload): Promise<void> {
    this.calls += 1;
    this.batchIDs.push(payload.batchID ?? "");
    const err = this.errs.shift();
    if (err) {
      throw err;
    }
  }
}

test("client aggregates counters and forwards raw values", async () => {
  const transport = new MemoryTransport();
  const client = new Client("example-app", { transport, logger: { printf() {} } });

  for (let i = 0; i < 5; i += 1) {
    client.count("requests", 1, label("method", "GET"));
    client.value("latency_ms", 123.4, label("method", "GET"));
  }
  await client.close();

  assert.equal(transport.batches.length, 1);
  assert.equal(transport.batches[0]!.counters.length, 1);
  assert.equal(transport.batches[0]!.counters[0]!.value, 5);
  assert.equal(transport.batches[0]!.values.length, 5);
});

test("client total uses first sample as baseline and emits positive deltas", async () => {
  const transport = new MemoryTransport();
  const client = new Client("api", { transport, logger: { printf() {} } });

  client.total("host.net.kb", 100, "iface=eth0");
  client.total("host.net.kb", 125, "iface=eth0");
  client.total("host.net.kb", 125, "iface=eth0");
  client.total("host.net.kb", 130, "iface=eth0");
  await client.close();

  assert.equal(transport.batches.length, 1);
  assert.deepEqual(
    transport.batches[0]!.counters.map((counter) => counter.value),
    [30],
  );
});

test("client deduplicates unique events within a batch", async () => {
  const transport = new MemoryTransport();
  const client = new Client("api", { transport, logger: { printf() {} } });

  client.countUnique("00042", "daily_active_users", "service=api");
  client.countUnique(42n, "daily_active_users", "service=api");
  client.countUnique(43, "daily_active_users", "service=api");
  await client.close();

  assert.equal(transport.batches[0]!.uniques.length, 2);
  assert.deepEqual(
    transport.batches[0]!.uniques.map((event) => event.uniqueID),
    ["42", "43"],
  );
});

test("client drops unique IDs outside uint64 range", async () => {
  const transport = new MemoryTransport();
  const client = new Client("api", { transport, logger: { printf() {} } });

  client.countUnique((1n << 64n) - 1n, "daily_active_users");
  client.countUnique(1n << 64n, "daily_active_users");
  client.countUnique("18446744073709551616", "daily_active_users");
  await client.close();

  assert.equal(transport.batches[0]!.uniques.length, 1);
  assert.equal(transport.batches[0]!.uniques[0]!.uniqueID, "18446744073709551615");
});

test("shared HTTPTransport keeps per-client workload", async () => {
  const gotWorkloads: string[] = [];
  const base = new HTTPTransport({
    endpoint: "http://prostometrics.test/api/i/batch",
    apiKey: "1_secret",
    fetch: async (_input, init) => {
      const headers = new Headers(init?.headers);
      gotWorkloads.push(headers.get("X-PM-Workload") ?? "");
      return new Response("", { status: 202 });
    },
  });

  const clientA = new Client("api-a", { transport: base, logger: { printf() {} } });
  const clientB = new Client("api-b", { transport: base, logger: { printf() {} } });

  clientA.count("requests", 1);
  clientB.count("requests", 1);
  await clientA.close();
  await clientB.close();

  assert.deepEqual(gotWorkloads, ["api-a", "api-b"]);
  assert.equal(base.workload, "");
});

test("client does not retry non-retryable failures", async () => {
  const transport = new ScriptedTransport([new HTTPTransportError({ endpoint: "http://prostometrics.test", statusCode: 400, status: "400 Bad Request" })]);
  const client = new Client("api", { transport, logger: { printf() {} } });
  client.value("latency_ms", 1);
  await client.close();
  assert.equal(transport.calls, 1);
});

test("client retries with the original batch ID and event timestamp", async () => {
  const timestamps: number[] = [];
  let calls = 0;
  const transport: Transport = {
    async send(payload) {
      calls += 1;
      timestamps.push(payload.values[0]!.timestamp);
      if (calls === 1) {
        throw new HTTPTransportError({ endpoint: "http://prostometrics.test", statusCode: 503 });
      }
    },
  };
  const client = new Client("api", { transport, logger: { printf() {} } });
  const payload: Payload = {
    batchID: "batch-1",
    counters: [],
    values: [{ metric: "latency_ms", value: 1, sparse: false, labels: [], timestamp: 1730000000 }],
    uniques: [],
  };
  const internals = client as unknown as {
    nextSendAttempt: number;
    retryQueue: Array<{ nextAttempt: number }>;
    sendPayload(payload: Payload, attempt: number, fromRetry: boolean): Promise<void>;
    flushRetryQueue(ignoreBackoff: boolean): Promise<void>;
  };

  await internals.sendPayload(payload, 1, false);
  internals.nextSendAttempt = 0;
  internals.retryQueue[0]!.nextAttempt = 0;
  await internals.flushRetryQueue(false);
  await client.close();

  assert.equal(calls, 2);
  assert.deepEqual(timestamps, [1730000000, 1730000000]);
});

test("client drops the oldest buffered batch when the outage buffer is full", async () => {
  const client = new Client("api", {
    transport: { async send() {} },
    logger: { printf() {} },
  });
  const internals = client as unknown as {
    retryQueue: Array<{ payload: Payload; attempts: number; nextAttempt: number; bufferedAt: number; eventCount: number; estimatedBytes: number }>;
    enqueueRetry(payload: Payload, attempt: number, err: unknown): boolean;
  };
  const makePayload = (batchID: string): Payload => ({
    batchID,
    counters: [{ metric: "requests", value: 1, labels: [], timestamp: 1730000000 }],
    values: [],
    uniques: [],
  });

  internals.retryQueue.push({
    payload: makePayload("batch-1"),
    attempts: 1,
    nextAttempt: 0,
    bufferedAt: Date.now() - 60_000,
    eventCount: DEFAULT_OUTAGE_BUFFER_MAX_EVENTS,
    estimatedBytes: 1,
  });
  internals.enqueueRetry(makePayload("batch-2"), 1, new Error("offline"));

  assert.equal(internals.retryQueue.length, 1);
  assert.equal(internals.retryQueue[0]!.payload.batchID, "batch-2");
  assert.equal(client.stats().retryDropped, DEFAULT_OUTAGE_BUFFER_MAX_EVENTS);
  await client.close({ signal: AbortSignal.abort() });
});

test("client expires batches outside the rolling outage window", async () => {
  const client = new Client("api", { transport: { async send() {} }, logger: { printf() {} } });
  const internals = client as unknown as {
    retryQueue: Array<{ payload: Payload; attempts: number; nextAttempt: number; bufferedAt: number; eventCount: number; estimatedBytes: number }>;
    enqueueRetry(payload: Payload, attempt: number, err: unknown): boolean;
  };
  const payload = makeCounterPayload("fresh");
  internals.retryQueue.push({
    payload: makeCounterPayload("expired"),
    attempts: 1,
    nextAttempt: 0,
    bufferedAt: Date.now() - DEFAULT_OUTAGE_BUFFER_MAX_AGE_MS - 1000,
    eventCount: 1,
    estimatedBytes: 1,
  });

  assert.equal(internals.enqueueRetry(payload, 1, new Error("offline")), true);
  assert.deepEqual(internals.retryQueue.map((item) => item.payload.batchID), ["fresh"]);
  assert.equal(client.stats().retryDropped, 1);
  await client.close({ signal: AbortSignal.abort() });
});

test("client expires a failed requeue before recovery succeeds", async () => {
  const client = new Client("api", { transport: { async send() {} }, logger: { printf() {} } });
  const internals = client as unknown as {
    retryQueue: Array<{ payload: Payload }>;
    enqueueRetry(payload: Payload, attempt: number, err: unknown, bufferedAt?: number): boolean;
  };
  const bufferedAt = Date.now() - DEFAULT_OUTAGE_BUFFER_MAX_AGE_MS - 1000;

  assert.equal(internals.enqueueRetry(makeCounterPayload("expired"), 10, new Error("offline"), bufferedAt), false);
  assert.equal(internals.retryQueue.length, 0);
  assert.equal(client.stats().retryDropped, 1);
  await client.close();
});

test("recovery snapshot may age after the first successful replay", async () => {
  const transport = new MemoryTransport();
  const client = new Client("api", { transport, logger: { printf() {} } });
  const internals = client as unknown as {
    recoveryConfirmed: boolean;
    nextReplayAttempt: number;
    retryQueue: Array<{ payload: Payload; attempts: number; nextAttempt: number; bufferedAt: number; eventCount: number; estimatedBytes: number }>;
    flushRetryQueue(ignoreBackoff: boolean): Promise<void>;
  };
  const bufferedAt = Date.now() - 29 * 60_000;
  for (const batchID of ["first", "second"]) {
    internals.retryQueue.push({ payload: makeCounterPayload(batchID), attempts: 1, nextAttempt: 0, bufferedAt, eventCount: 1, estimatedBytes: 128 });
  }

  await internals.flushRetryQueue(false);
  assert.equal(internals.recoveryConfirmed, true);
  internals.retryQueue[0]!.bufferedAt = Date.now() - DEFAULT_OUTAGE_BUFFER_MAX_AGE_MS - 60_000;
  internals.nextReplayAttempt = 0;
  await internals.flushRetryQueue(false);

  assert.deepEqual(transport.batches.map((payload) => payload.batchID), ["first", "second"]);
  assert.equal(internals.retryQueue.length, 0);
  await client.close();
});

test("client drops the oldest batch at the estimated byte limit", async () => {
  const client = new Client("api", { transport: { async send() {} }, logger: { printf() {} } });
  const internals = client as unknown as {
    retryQueue: Array<{ payload: Payload; attempts: number; nextAttempt: number; bufferedAt: number; eventCount: number; estimatedBytes: number }>;
    enqueueRetry(payload: Payload, attempt: number, err: unknown): boolean;
  };
  internals.retryQueue.push({
    payload: makeCounterPayload("old"),
    attempts: 1,
    nextAttempt: 0,
    bufferedAt: Date.now() - 60_000,
    eventCount: 1,
    estimatedBytes: DEFAULT_OUTAGE_BUFFER_MAX_BYTES,
  });

  assert.equal(internals.enqueueRetry(makeCounterPayload("fresh"), 1, new Error("offline")), true);
  assert.deepEqual(internals.retryQueue.map((item) => item.payload.batchID), ["fresh"]);
  await client.close({ signal: AbortSignal.abort() });
});

test("client sends at most one replay batch per replay interval", async () => {
  const transport = new MemoryTransport();
  const client = new Client("api", { transport, logger: { printf() {} } });
  const internals = client as unknown as {
    nextReplayAttempt: number;
    enqueueRetryAt(payload: Payload, attempts: number, nextAttempt: number, err: unknown, bufferedAt?: number): boolean;
    flushRetryQueue(ignoreBackoff: boolean): Promise<void>;
  };
  const makePayload = (batchID: string): Payload => ({
    batchID,
    counters: [{ metric: "requests", value: 1, labels: [], timestamp: 1730000000 }],
    values: [],
    uniques: [],
  });
  const bufferedAt = Date.now();
  internals.enqueueRetryAt(makePayload("batch-new"), 1, 0, new Error("offline"), bufferedAt);
  internals.enqueueRetryAt(makePayload("batch-old"), 1, 0, new Error("offline"), bufferedAt - 60_000);

  const beforeReplay = Date.now();
  await internals.flushRetryQueue(false);
  const replayDeadline = internals.nextReplayAttempt;
  await internals.flushRetryQueue(false);
  assert.equal(transport.batches.length, 1);
  assert.equal(transport.batches[0]!.batchID, "batch-old");
  assert.ok(replayDeadline >= beforeReplay + DEFAULT_REPLAY_INTERVAL_MS);
  assert.ok(replayDeadline <= Date.now() + DEFAULT_REPLAY_INTERVAL_MS);

  internals.nextReplayAttempt = 0;
  await internals.flushRetryQueue(false);
  assert.equal(transport.batches.length, 2);
  await client.close();
});

test("Retry-After controls client and batch scheduling", async () => {
  const client = new Client("api", { transport: { async send() {} }, logger: { printf() {} } });
  const internals = client as unknown as {
    nextSendAttempt: number;
    retryQueue: Array<{ nextAttempt: number }>;
    noteTransientFailure(err: unknown): void;
    enqueueRetry(payload: Payload, attempt: number, err: unknown): boolean;
  };
  const retryAfterMs = 120_000;
  const err = new HTTPTransportError({ endpoint: "https://collector.example.com", statusCode: 503, retryAfterMs });

  const beforeBackoff = Date.now();
  internals.noteTransientFailure(err);
  assert.ok(internals.nextSendAttempt >= beforeBackoff + retryAfterMs);
  assert.ok(internals.nextSendAttempt <= Date.now() + retryAfterMs);

  const beforeRetry = Date.now();
  assert.equal(internals.enqueueRetry(makeCounterPayload("batch-1"), 1, err), true);
  assert.ok(internals.retryQueue[0]!.nextAttempt >= beforeRetry + retryAfterMs);
  assert.ok(internals.retryQueue[0]!.nextAttempt <= Date.now() + retryAfterMs);
  await client.close({ signal: AbortSignal.abort() });
});

test("client backoff jitter stays within its internal bounds", async () => {
  const client = new Client("api", { transport: { async send() {} }, logger: { printf() {} } });
  const internals = client as unknown as {
    transientBackoffAttempt: number;
    nextSendAttempt: number;
    noteTransientFailure(err: unknown): void;
  };
  const transient = new HTTPTransportError({ endpoint: "https://collector.example.com", statusCode: 503 });

  for (let i = 0; i < 100; i += 1) {
    internals.transientBackoffAttempt = 0;
    internals.noteTransientFailure(transient);
    const firstDelay = internals.nextSendAttempt - Date.now();
    assert.ok(firstDelay >= DEFAULT_RETRY_BASE_DELAY_MS - 10);
    assert.ok(firstDelay <= DEFAULT_CLIENT_BACKOFF_MAX_DELAY_MS);

    internals.transientBackoffAttempt = 100;
    internals.noteTransientFailure(transient);
    const cappedDelay = internals.nextSendAttempt - Date.now();
    assert.ok(cappedDelay >= DEFAULT_CLIENT_BACKOFF_MAX_DELAY_MS - 10);
    assert.ok(cappedDelay <= DEFAULT_CLIENT_BACKOFF_MAX_DELAY_MS);
  }
  await client.close();
});

test("close drains buffered batches sequentially with replay pacing", async () => {
  const transport = new MemoryTransport();
  const client = new Client("api", { transport, logger: { printf() {} } });
  const internals = client as unknown as {
    retryQueue: Array<{ payload: Payload; attempts: number; nextAttempt: number; bufferedAt: number; eventCount: number; estimatedBytes: number }>;
  };
  const bufferedAt = Date.now();
  for (const batchID of ["first", "second"]) {
    internals.retryQueue.push({ payload: makeCounterPayload(batchID), attempts: 1, nextAttempt: 0, bufferedAt, eventCount: 1, estimatedBytes: 128 });
  }

  const started = Date.now();
  await client.close();

  assert.deepEqual(transport.batches.map((payload) => payload.batchID), ["first", "second"]);
  assert.ok(Date.now() - started >= DEFAULT_REPLAY_INTERVAL_MS);
  assert.equal(internals.retryQueue.length, 0);
});

test("close stops buffered drain after a send failure", async () => {
  const transport = new ScriptedTransport([new HTTPTransportError({ endpoint: "https://collector.example.com", statusCode: 503 })]);
  const client = new Client("api", { transport, logger: { printf() {} } });
  const internals = client as unknown as {
    retryQueue: Array<{ payload: Payload; attempts: number; nextAttempt: number; bufferedAt: number; eventCount: number; estimatedBytes: number }>;
  };
  const bufferedAt = Date.now();
  for (const batchID of ["first", "second"]) {
    internals.retryQueue.push({ payload: makeCounterPayload(batchID), attempts: 1, nextAttempt: 0, bufferedAt, eventCount: 1, estimatedBytes: 128 });
  }

  await client.close();

  assert.equal(transport.calls, 1);
  assert.equal(internals.retryQueue.length, 2);
});

test("close waits for an active background flush without concurrent sends", async () => {
  let inFlight = 0;
  let maxInFlight = 0;
  let releaseFirst!: () => void;
  let firstStarted!: () => void;
  const started = new Promise<void>((resolve) => {
    firstStarted = resolve;
  });
  const firstGate = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  let calls = 0;
  const transport: Transport = {
    async send() {
      calls += 1;
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
      if (calls === 1) {
        firstStarted();
        await firstGate;
      }
      inFlight -= 1;
    },
  };
  const client = new Client("api", { transport, logger: { printf() {} } });
  const internals = client as unknown as {
    retryQueue: Array<{ payload: Payload; attempts: number; nextAttempt: number; bufferedAt: number; eventCount: number; estimatedBytes: number }>;
    flushDue(ignoreRetryBackoff: boolean): Promise<void>;
  };
  internals.retryQueue.push({ payload: makeCounterPayload("buffered"), attempts: 1, nextAttempt: Date.now() + 10, bufferedAt: Date.now(), eventCount: 1, estimatedBytes: 128 });
  client.count("live", 1);

  const background = internals.flushDue(false);
  await started;
  const closing = client.close();
  await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(maxInFlight, 1);
  releaseFirst();
  await background;
  await closing;

  assert.equal(calls, 2);
  assert.equal(maxInFlight, 1);
});

test("close stops when the active background send fails", async () => {
  let release!: () => void;
  let markStarted!: () => void;
  const started = new Promise<void>((resolve) => {
    markStarted = resolve;
  });
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let calls = 0;
  const transport: Transport = {
    async send() {
      calls += 1;
      markStarted();
      await gate;
      throw new HTTPTransportError({ endpoint: "https://collector.example.com", statusCode: 503 });
    },
  };
  const client = new Client("api", { transport, logger: { printf() {} } });
  const internals = client as unknown as {
    retryQueue: Array<unknown>;
    flushDue(ignoreRetryBackoff: boolean): Promise<void>;
  };
  client.count("live", 1);

  const background = internals.flushDue(false);
  await started;
  const closing = client.close();
  release();
  await background;
  await closing;

  assert.equal(calls, 1);
  assert.equal(internals.retryQueue.length, 1);
});

function makeCounterPayload(batchID: string): Payload {
  return {
    batchID,
    counters: [{ metric: "requests", value: 1, labels: [], timestamp: 1730000000 }],
    values: [],
    uniques: [],
  };
}

test("client stops sending after stop ingest error", async () => {
  const transport = new ScriptedTransport([new StopIngestError("unauthorized", { code: 401 }), undefined]);
  const client = new Client("api", { transport, logger: { printf() {} } });
  client.value("latency_ms", 1);
  await client.close();
  client.value("latency_ms", 2);
  await client.close();
  assert.equal(transport.calls, 1);
});

test("client drops silently when queue is full", async () => {
  const transport = new MemoryTransport();
  const client = new Client("api", { transport, logger: { printf() {} } });
  for (let i = 0; i < 70_000; i += 1) {
    client.count("requests", 1);
  }
  assert.ok(client.dropped() > 0);
  await client.close();
});

test("client skips sparse values before queueing when the limiter rejects", async () => {
  const transport = new MemoryTransport();
  const client = new Client("api", { transport, logger: { printf() {} } });
  const internals = client as unknown as { valueRateLimiter: { allow(): boolean }; queue: { length: number } };
  internals.valueRateLimiter = { allow: () => false };

  client.valueSparse("load", 1, "host=example");
  assert.equal(internals.queue.length, 0);
  await client.close();
});

test("client marks sparse values without adding a synthetic label", async () => {
  const transport = new MemoryTransport();
  const client = new Client("api", { transport, logger: { printf() {} } });

  client.valueSparse("capacity_kb", 1024, "mount=/");
  await client.close();

  assert.equal(transport.batches[0]!.values[0]!.sparse, true);
  assert.deepEqual(transport.batches[0]!.values[0]!.labels, ["mount=/"]);
});
