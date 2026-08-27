import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import test from "node:test";
import { Client } from "./client.js";
import { HTTPTransport } from "./transport.js";
import { normalizeLabels, normalizeMetric } from "./labels.js";
import type { Payload } from "./payload.js";

const listen = (server: ReturnType<typeof createServer>): Promise<void> =>
  new Promise((resolve) => server.listen(0, "127.0.0.1", () => resolve()));

const endpoint = (server: ReturnType<typeof createServer>): string =>
  `http://127.0.0.1:${(server.address() as AddressInfo).port}/api/i/batch`;

const counterPayload = (metric: string): Payload => ({
  batchID: `b-${metric}`,
  counters: [{ metric, value: 1, labels: [], timestamp: 1730000000 }],
  values: [],
  uniques: [],
});

const seriesLines = (body: string): string[] =>
  body.split("\n").filter((line) => line.startsWith("S|"));

/**
 * A request that failed never reached the server, so the series definitions it
 * carried must travel again with the retry. Remembering them leaves the retry
 * pointing at series the server cannot resolve, which it rejects per event
 * while still reporting the batch as accepted.
 */
test("a retried batch resends the series definitions the server never received", async () => {
  const bodies: string[] = [];
  let failNext = false;
  const server = createServer((req, res) => {
    let body = "";
    req.setEncoding("utf8");
    req.on("data", (chunk) => {
      body += chunk;
    });
    req.on("end", () => {
      bodies.push(body);
      if (failNext) {
        failNext = false;
        res.writeHead(503).end('{"error":"ingest queue full"}');
        return;
      }
      res.writeHead(202).end();
    });
  });
  await listen(server);
  try {
    const transport = new HTTPTransport({ endpoint: endpoint(server), apiKey: "1_k", workload: "api" });

    await transport.send(counterPayload("first"));
    assert.deepEqual(seriesLines(bodies[0]!), ["S|0|first"]);

    failNext = true;
    await assert.rejects(() => transport.send(counterPayload("second")));
    assert.deepEqual(seriesLines(bodies[1]!), ["S|1|second"]);

    await transport.send(counterPayload("second"));
    assert.deepEqual(seriesLines(bodies[2]!), ["S|1|second"], "the retry must restate the unacknowledged series");
  } finally {
    server.close();
  }
});

test("a cumulative total beyond the counter ceiling still reports its delta", () => {
  const client = new Client("api", { transport: { async send() {} } });
  const applyTotal = (client as unknown as { applyTotal(event: unknown): unknown }).applyTotal.bind(client);

  const base = { type: "total" as const, metric: "bytes_sent_kb", labels: [], timestamp: 1 };
  assert.equal(applyTotal({ ...base, value: 1e10 }), undefined, "the first reading is only a baseline");
  const delta = applyTotal({ ...base, value: 1e10 + 1500 }) as { type: string; value: number };
  assert.equal(delta.type, "counter");
  assert.equal(delta.value, 1500);
});

test("a total delta beyond the counter ceiling is dropped", () => {
  const client = new Client("api", { transport: { async send() {} } });
  const applyTotal = (client as unknown as { applyTotal(event: unknown): unknown }).applyTotal.bind(client);
  const base = { type: "total" as const, metric: "bytes", labels: [], timestamp: 1 };
  applyTotal({ ...base, value: 0 });
  assert.equal(applyTotal({ ...base, value: 5_000_000_000 }), undefined);
});

test("metric names carrying control characters or the field delimiter are rejected", () => {
  const tab = String.fromCharCode(9);
  const del = String.fromCharCode(127);
  for (const metric of [`request${tab}count`, `request${del}count`, "pipe|name", "", "   "]) {
    assert.equal(normalizeMetric(metric), undefined, `expected ${JSON.stringify(metric)} to be rejected`);
  }
  assert.equal(normalizeMetric("  request.count  "), "request.count");
  assert.equal(normalizeMetric("x".repeat(101)), undefined);
});

test("labels the server would refuse are rejected instead of being sent", () => {
  const tab = String.fromCharCode(9);
  assert.equal(normalizeLabels(["novalue"]), undefined, "a label needs name=value");
  assert.equal(normalizeLabels(["=novalue"]), undefined);
  assert.equal(normalizeLabels(["workload=other"]), undefined, "workload is reserved");
  assert.equal(normalizeLabels(["m=GET", "m=POST"]), undefined, "duplicate label names");
  assert.equal(normalizeLabels(["bad|token=1"]), undefined);
  assert.equal(normalizeLabels([`bad${tab}token=1`]), undefined);
  assert.equal(normalizeLabels([`k=${"v".repeat(600)}`]), undefined);
  assert.equal(normalizeLabels(Array.from({ length: 9 }, (_, i) => `l${i}=v`)), undefined, "at most 8 labels");

  // Order is preserved: it is part of how existing series are keyed.
  assert.deepEqual(normalizeLabels(["status=200", "method=GET"]), ["status=200", "method=GET"]);
});
