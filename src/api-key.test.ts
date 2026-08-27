import assert from "node:assert/strict";
import test from "node:test";
import { apiKeyLooksLikeClientKey, apiKeyRefusalHint } from "./api-key.js";
import { Client } from "./client.js";
import { HTTPTransportError, StopIngestError } from "./errors.js";
import type { Payload, Transport } from "./index.js";

class ScriptedTransport implements Transport {
  calls = 0;

  // `repeatLast` keeps a transport refusing past the scripted attempts, which is
  // what a genuinely wrong key does — a finite script would let the drain during
  // close succeed and quietly resolve the very state under test.
  constructor(private readonly errs: unknown[], private readonly repeatLast = false) {}

  async send(): Promise<void> {
    this.calls += 1;
    const err = this.errs.length > 1 || !this.repeatLast ? this.errs.shift() : this.errs[0];
    if (err) {
      throw err;
    }
  }
}

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

function unauthorizedStopError(): StopIngestError {
  return new StopIngestError("prostometrics: stop ingesting after HTTP 401", {
    code: 401,
    cause: new HTTPTransportError({
      endpoint: "https://collector.example.com",
      statusCode: 401,
      status: "401 Unauthorized",
      responseCode: "unauthorized",
    }),
  });
}

function counterPayload(batchID: string): Payload {
  return {
    batchID,
    counters: [{ metric: "requests", value: 1, labels: [], timestamp: 1730000000 }],
    values: [],
    uniques: [],
  };
}

test("a client key is recognised by its marker", () => {
  assert.equal(apiKeyLooksLikeClientKey("1_J790Vup6NxClhwkr5IirkbsSYLy"), false);
  assert.equal(apiKeyLooksLikeClientKey("1_pk_CILo8eLaRH7lP-iPiN3wxMtltpWAu0kt"), true);
  assert.equal(apiKeyLooksLikeClientKey("  42_pk_abc  "), true);
  assert.equal(apiKeyLooksLikeClientKey(""), false);
  assert.equal(apiKeyRefusalHint("1_J790Vup6"), "");
  assert.match(apiKeyRefusalHint("1_pk_abc"), /client key/);
});

// A client key cannot start working, so the grace only delays an accurate error.
test("a client key skips the startup grace and is named as the mistake", async () => {
  const transport = new ScriptedTransport([unauthorizedStopError()]);
  const captured = captureLogs();
  const client = new Client("api", { transport, apiKey: "1_pk_CILo8eLaRH7lP", logger: captured.logger });
  const internals = client as unknown as {
    stopSending: boolean;
    retryQueue: Array<unknown>;
    sendPayload(payload: Payload, attempt: number, fromRetry: boolean): Promise<boolean>;
  };

  await internals.sendPayload(counterPayload("batch-1"), 1, false);

  assert.equal(internals.stopSending, true);
  assert.equal(internals.retryQueue.length, 0);
  assert.ok(
    captured.lines.some((line) => /ingest disabled/.test(line) && /this is a client key/.test(line)),
    `logs: ${captured.lines.join(" | ")}`,
  );
  await client.close();
});

// A short-lived process must not exit on the reassuring "retrying" line alone.
test("close reports a refusal that was still unresolved at shutdown", async () => {
  const transport = new ScriptedTransport([unauthorizedStopError()], true);
  const captured = captureLogs();
  const client = new Client("api", { transport, apiKey: "1_J790Vup6NxClhwkr5IirkbsSYLy", logger: captured.logger });
  const internals = client as unknown as {
    authRefusalPending: boolean;
    sendPayload(payload: Payload, attempt: number, fromRetry: boolean): Promise<boolean>;
  };

  await internals.sendPayload(counterPayload("batch-1"), 1, false);
  assert.equal(internals.authRefusalPending, true);

  await client.close();

  assert.ok(
    captured.lines.some((line) => /shutting down while the API key was still refused/.test(line)),
    `logs: ${captured.lines.join(" | ")}`,
  );
});

// A key that was merely propagating resolves, and shutdown then says nothing.
test("close stays quiet once a refused key is accepted", async () => {
  const transport = new ScriptedTransport([unauthorizedStopError(), undefined]);
  const captured = captureLogs();
  const client = new Client("api", { transport, apiKey: "1_J790Vup6NxClhwkr5IirkbsSYLy", logger: captured.logger });
  const internals = client as unknown as {
    authRefusalPending: boolean;
    nextSendAttempt: number;
    retryQueue: Array<{ nextAttempt: number }>;
    sendPayload(payload: Payload, attempt: number, fromRetry: boolean): Promise<boolean>;
    flushRetryQueue(ignoreBackoff: boolean): Promise<void>;
  };

  await internals.sendPayload(counterPayload("batch-1"), 1, false);
  internals.nextSendAttempt = 0;
  internals.retryQueue[0]!.nextAttempt = Date.now() - 1;
  await internals.flushRetryQueue(false);

  assert.equal(internals.authRefusalPending, false);
  await client.close();
  assert.ok(
    !captured.lines.some((line) => /shutting down while the API key was still refused/.test(line)),
    `logs: ${captured.lines.join(" | ")}`,
  );
});
