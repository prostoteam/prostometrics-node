import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import test from "node:test";
import { gunzipSync } from "node:zlib";

import { init } from "./client.js";
import { DEFAULT_MAX_BATCH_SERIES, MAX_TOP_ITEM_BYTES } from "./constants.js";
import { label } from "./labels.js";
import type { Payload } from "./payload.js";
import { HTTPTransport } from "./transport.js";

interface Received {
  encoding: string;
  sent: number;
  body: string;
}

// A server that reads what the transport actually put on the wire, and what it
// claimed about it.
const recordingServer = (received: Received[]) => {
  const server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => chunks.push(chunk));
    req.on("end", () => {
      const raw = Buffer.concat(chunks);
      const encoding = String(req.headers["content-encoding"] ?? "");
      const body = encoding === "gzip" ? gunzipSync(raw).toString("utf8") : raw.toString("utf8");
      received.push({ encoding, sent: raw.byteLength, body });
      res.writeHead(202).end();
    });
  });
  return server;
};

const listen = (server: ReturnType<typeof createServer>): Promise<void> =>
  new Promise((resolve) => server.listen(0, "127.0.0.1", () => resolve()));

const close = (server: ReturnType<typeof createServer>): Promise<void> =>
  new Promise((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));

const endpoint = (server: ReturnType<typeof createServer>): string =>
  `http://127.0.0.1:${(server.address() as AddressInfo).port}/api/i/batch`;

const topPayload = (events: number): Payload => ({
  counters: [],
  values: [],
  uniques: [],
  tops: Array.from({ length: events }, (_, i) => ({
    metric: "nginx.pages",
    uniqueID: String(BigInt(i) * 11400714819323198485n),
    item: `/guides/how-to-instrument-a-service-${i % 64}`,
    labels: [],
    timestamp: 1730000000 + i,
  })),
});

// The body has to arrive as the batch it stands for, and the header has to say
// what was done to it: a server reading it as plain text would see rubbish.
test("a large body goes up compressed and says so", async () => {
  const received: Received[] = [];
  const server = recordingServer(received);
  await listen(server);
  try {
    const transport = new HTTPTransport({ endpoint: endpoint(server), apiKey: "1_secret", workload: "test" });
    await transport.send(topPayload(400));
  } finally {
    await close(server);
  }

  assert.equal(received.length, 1);
  assert.equal(received[0]!.encoding, "gzip");
  assert.ok(received[0]!.body.startsWith("H|5|s|"), "decompressed body is not a v5 batch");
  assert.equal(received[0]!.body.split("\nt|").length - 1, 400);
  assert.ok(received[0]!.sent < received[0]!.body.length, "compression saved nothing");
});

// Below the threshold the saving is smaller than the request's own headers, and
// it would be paid for with the caller's processor.
test("a small body is left alone", async () => {
  const received: Received[] = [];
  const server = recordingServer(received);
  await listen(server);
  try {
    const transport = new HTTPTransport({ endpoint: endpoint(server), apiKey: "1_secret", workload: "test" });
    await transport.send({
      counters: [{ metric: "signups", value: 1, labels: [], timestamp: 1730000000 }],
      values: [],
      uniques: [],
      tops: [],
    });
  } finally {
    await close(server);
  }

  assert.equal(received.length, 1);
  assert.equal(received[0]!.encoding, "");
});

// Counting only events would let a few thousand long top-list items build a
// body the endpoint refuses whole, so the batch closes on size as well.
test("no body passes the endpoint's ceiling however long the items", async () => {
  const received: Received[] = [];
  const server = recordingServer(received);
  await listen(server);

  const item = "p".repeat(MAX_TOP_ITEM_BYTES);
  try {
    const client = init("api", { apiKey: "1_secret", endpoint: endpoint(server), silent: true });
    for (let i = 0; i < 3000; i += 1) {
      client.countTop(i, "nginx.pages", item);
    }
    await client.close();
  } finally {
    await close(server);
  }

  assert.ok(received.length >= 2, `3000 items of ${MAX_TOP_ITEM_BYTES} bytes went in ${received.length} body`);
  for (const request of received) {
    assert.ok(request.body.length <= 265 * 1024, `a body of ${request.body.length} bytes passed the endpoint's ceiling`);
  }
  const events = received.reduce((total, request) => total + request.body.split("\nt|").length - 1, 0);
  assert.equal(events, 3000, "events were lost splitting the batch");
});

// An article name in a non-Latin script is two bytes a character, so counting
// characters would let a batch close at half the size the endpoint measures.
test("measures an item in bytes, not characters", async () => {
  const received: Received[] = [];
  const server = recordingServer(received);
  await listen(server);

  // 128 Cyrillic characters is 256 bytes, the largest item allowed.
  const item = "страница".repeat(16);
  try {
    const client = init("api", { apiKey: "1_secret", endpoint: endpoint(server), silent: true });
    for (let i = 0; i < 3000; i += 1) {
      client.countTop(i, "nginx.pages", item);
    }
    await client.close();
  } finally {
    await close(server);
  }

  for (const request of received) {
    const bytes = Buffer.byteLength(request.body, "utf8");
    assert.ok(bytes <= 265 * 1024, `a body of ${bytes} bytes passed the endpoint's ceiling`);
  }
  const events = received.reduce((total, request) => total + request.body.split("\nt|").length - 1, 0);
  assert.equal(events, 3000, "events were lost splitting the batch");
});

// The endpoint refuses a batch defining more series than it allows, and a first
// flush defines every series it touches.
test("closes a batch before it defines more series than allowed", async () => {
  const received: Received[] = [];
  const server = recordingServer(received);
  await listen(server);

  const series = 1500;
  try {
    const client = init("api", { apiKey: "1_secret", endpoint: endpoint(server), silent: true });
    for (let i = 0; i < series; i += 1) {
      client.count(`metric.${i}`, 1);
    }
    await client.close();
  } finally {
    await close(server);
  }

  let total = 0;
  for (const request of received) {
    const definitions = request.body.split("\nS|").length - 1;
    assert.ok(
      definitions <= DEFAULT_MAX_BATCH_SERIES,
      `a request defines ${definitions} series, past the ${DEFAULT_MAX_BATCH_SERIES} the endpoint allows`,
    );
    total += definitions;
  }
  assert.equal(total, series, "series definitions were lost");
});

// A definition carries the metric name and every label value into the same body
// as the events that need it, and on a first flush there is one per series.
// This client has no splitter, so a budget that counts only event lines is the
// difference between a batch that arrives and one refused whole, forever.
test("stays inside the endpoint ceiling when the definitions are heavy", async () => {
  const received: Received[] = [];
  const server = recordingServer(received);
  await listen(server);

  const heavy = "v".repeat(200);
  const labels = ["a", "b", "c", "d", "e", "f", "g", "h"].map((k) => label(k, heavy));
  try {
    const client = init("api", { apiKey: "1_secret", endpoint: endpoint(server), silent: true });
    for (let i = 0; i < 1000; i += 1) client.count(`metric.${i}`, 1, ...labels);
    await client.close();
  } finally {
    await close(server);
  }

  const ceiling = 265 * 1024;
  for (const request of received) {
    const bytes = Buffer.byteLength(request.body, "utf8");
    assert.ok(bytes <= ceiling, `a body of ${bytes} bytes passed the ${ceiling} the endpoint accepts`);
  }
  const events = received.reduce((total, r) => total + r.body.split("\nc|").length - 1, 0);
  assert.equal(events, 1000, "events were lost splitting the batch");
});
