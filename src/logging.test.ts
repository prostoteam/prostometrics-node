import assert from "node:assert/strict";
import test from "node:test";
import { Client } from "./client.js";
import { DEFAULT_OUTAGE_BUFFER_MAX_EVENTS } from "./constants.js";
import { HTTPTransportError } from "./errors.js";
import type { Payload } from "./payload.js";
import { HTTPTransport } from "./transport.js";

function captureLogs(): { lines: string[]; logger: { printf(format: string, ...args: unknown[]): void } } {
  const lines: string[] = [];
  return {
    lines,
    logger: {
      printf(format: string, ...args: unknown[]) {
        lines.push(format.replace(/%[sdv]/g, () => String(args.shift())));
      },
    },
  };
}

test("dictionary resync is verbose unless repeated", () => {
  const captured = captureLogs();
  const transport = new HTTPTransport({ endpoint: "https://collector.example.com", logger: captured.logger });
  const internals = transport as unknown as { logDictionaryResync(cause: HTTPTransportError): void };
  const cause = new HTTPTransportError({
    endpoint: "https://collector.example.com",
    statusCode: 409,
    responseCode: "unknown_series_dictionary",
  });

  internals.logDictionaryResync(cause);
  internals.logDictionaryResync(cause);
  assert.deepEqual(captured.lines, []);
  internals.logDictionaryResync(cause);
  assert.equal(captured.lines.length, 1);
  assert.match(captured.lines[0]!, /repeated ingester dictionary resyncs/);

  const verboseCaptured = captureLogs();
  const verboseTransport = new HTTPTransport({ endpoint: "https://collector.example.com", logger: verboseCaptured.logger, verbose: true });
  (verboseTransport as unknown as { logDictionaryResync(cause: HTTPTransportError): void }).logDictionaryResync(cause);
  assert.match(verboseCaptured.lines[0]!, /dictionary miss recovered by resync/);
});

test("local drop warnings are rate limited", async () => {
  const captured = captureLogs();
  const client = new Client("api", { transport: { async send() {} }, logger: captured.logger });

  client.countUnique(-1, "users");
  client.countUnique(-2, "users");
  await client.close();

  assert.equal(captured.lines.filter((line) => line.includes("invalid_unique_id")).length, 1);
});

test("outage buffer drop logs once", async () => {
  const captured = captureLogs();
  const client = new Client("api", {
    transport: { async send() {} },
    logger: captured.logger,
  });
  const internals = client as unknown as {
    retryQueue: Array<{ payload: Payload; attempts: number; nextAttempt: number; bufferedAt: number; eventCount: number; estimatedBytes: number }>;
    enqueueRetry(payload: Payload, attempt: number, err: unknown): boolean;
  };

  for (let i = 1; i <= 3; i += 1) {
    if (internals.retryQueue.length === 0) {
      internals.retryQueue.push({
        payload: { batchID: "full", counters: [], values: [], uniques: [] },
        attempts: 1,
        nextAttempt: 0,
        bufferedAt: Date.now() - 60_000,
        eventCount: DEFAULT_OUTAGE_BUFFER_MAX_EVENTS,
        estimatedBytes: 1,
      });
    } else {
      internals.retryQueue[0]!.eventCount = DEFAULT_OUTAGE_BUFFER_MAX_EVENTS;
    }
    internals.enqueueRetry(
      {
        batchID: `batch-${i}`,
        counters: [{ metric: "requests", value: 1, labels: [], timestamp: 1730000000 }],
        values: [],
        uniques: [],
      },
      1,
      new HTTPTransportError({ endpoint: "https://collector.example.com", statusCode: 503 }),
    );
  }
  await client.close({ signal: AbortSignal.abort() });

  assert.equal(captured.lines.filter((line) => line.includes("outage_buffer_full")).length, 1);
});
