import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import test from "node:test";
import { HTTPTransport } from "./transport.js";
import { HTTPTransportError } from "./errors.js";
import { label } from "./labels.js";
import type { Payload } from "./payload.js";

const payload: Payload = {
  batchID: "batch-123",
  counters: [{ metric: "requests", value: 1, labels: [label("env", "test")], timestamp: 1730000000 }],
  values: [],
  uniques: [],
};

test("HTTPTransport sets auth, workload, batch headers and v5 body", async () => {
  let body = "";
  let authorization = "";
  let workload = "";
  let batchID = "";
  const server = createServer((req, res) => {
    authorization = req.headers.authorization ?? "";
    workload = String(req.headers["x-pm-workload"] ?? "");
    batchID = String(req.headers["x-pm-batch-id"] ?? "");
    req.setEncoding("utf8");
    req.on("data", (chunk) => {
      body += chunk;
    });
    req.on("end", () => {
      res.writeHead(202).end();
    });
  });
  await listen(server);
  try {
    const transport = new HTTPTransport({
      endpoint: endpoint(server),
      apiKey: "123_secret-token",
      workload: "api-a",
    });
    await transport.send(payload);

    assert.equal(authorization, "123_secret-token");
    assert.equal(workload, "api-a");
    assert.equal(batchID, "batch-123");
    assert.match(body, /^H\|5\|s\|/);
    assert.match(body, /\nS\|0\|requests\|env=test\n/);
    assert.match(body, /\nc\|0\|1\|1730000000\n/);
  } finally {
    await close(server);
  }
});

test("HTTPTransport reuses dictionary and resyncs on unknown dictionary", async () => {
  const bodies: string[] = [];
  const server = createServer((req, res) => {
    let body = "";
    req.setEncoding("utf8");
    req.on("data", (chunk) => {
      body += chunk;
    });
    req.on("end", () => {
      bodies.push(body);
      if (bodies.length === 2) {
        res.writeHead(409, { "Content-Type": "application/json" }).end(JSON.stringify({ code: "unknown_series_dictionary" }));
        return;
      }
      res.writeHead(202).end();
    });
  });
  await listen(server);
  try {
    const transport = new HTTPTransport({
      endpoint: endpoint(server),
      apiKey: "123_secret-token",
      workload: "api-a",
      logger: { printf() {} },
    });

    await transport.send(payload);
    await transport.send(payload);

    assert.equal(bodies.length, 3);
    assert.match(bodies[0]!, /\nS\|0\|requests\|/);
    assert.doesNotMatch(bodies[1]!, /\nS\|0\|requests\|/);
    assert.match(bodies[2]!, /\nS\|0\|requests\|/);
  } finally {
    await close(server);
  }
});

test("HTTPTransport caps error body diagnostics", async () => {
  const server = createServer((req, res) => {
    req.resume();
    res.writeHead(500, { "Content-Type": "text/plain" }).end("x".repeat(20_000));
  });
  await listen(server);
  try {
    const transport = new HTTPTransport({
      endpoint: endpoint(server),
      apiKey: "123_secret-token",
      workload: "api-a",
    });

    await assert.rejects(
      () => transport.send(payload),
      (err) => err instanceof HTTPTransportError && err.detail.length <= 4096,
    );
  } finally {
    await close(server);
  }
});

test("HTTPTransport exposes Retry-After to the client backoff", async () => {
  const transport = new HTTPTransport({
    endpoint: "https://collector.example.com/api/i/batch",
    apiKey: "123_secret-token",
    workload: "api-a",
    fetch: async () => new Response("unavailable", { status: 503, headers: { "Retry-After": "60" } }),
  });

  await assert.rejects(
    () => transport.send(payload),
    (err) => err instanceof HTTPTransportError && err.retryAfterMs === 60_000,
  );
});

function listen(server: ReturnType<typeof createServer>): Promise<void> {
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
}

function endpoint(server: ReturnType<typeof createServer>): string {
  const address = server.address() as AddressInfo;
  return `http://127.0.0.1:${address.port}/api/i/batch`;
}

function close(server: ReturnType<typeof createServer>): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((err) => (err ? reject(err) : resolve()));
  });
}
