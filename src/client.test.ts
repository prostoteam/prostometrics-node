import assert from "node:assert/strict";
import test from "node:test";
import { Client } from "./client.js";
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
