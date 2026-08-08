import assert from "node:assert/strict";
import test from "node:test";
import { Client } from "./client.js";
import { DEFAULT_RETRY_MAX_ATTEMPTS } from "./constants.js";
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

test("terminal retry failure logs once", async () => {
  const captured = captureLogs();
  const transport = {
    async send() {
      throw new HTTPTransportError({ endpoint: "https://collector.example.com", statusCode: 503, status: "503 Service Unavailable" });
    },
  };
  const client = new Client("api", { transport, logger: captured.logger });
  const payload: Payload = {
    batchID: "batch-1",
    counters: [{ metric: "requests", value: 1, labels: [], timestamp: 1730000000 }],
    values: [],
    uniques: [],
  };
  const internals = client as unknown as {
    sendPayload(payload: Payload, attempt: number, fromRetry: boolean): Promise<void>;
  };

  await internals.sendPayload(payload, DEFAULT_RETRY_MAX_ATTEMPTS, true);
  await client.close();

  assert.equal(captured.lines.filter((line) => line.includes("retry budget exhausted")).length, 1);
  assert.equal(captured.lines.filter((line) => line.includes("flush failed")).length, 0);
});
