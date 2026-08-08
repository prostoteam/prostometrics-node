import assert from "node:assert/strict";
import test from "node:test";
import { APIKeyAuthorizationConflictError, MissingAPIKeyError } from "./errors.js";
import { endpointFromHost, applyDefaults, consoleLogger, noopLogger } from "./config.js";
import { HTTPTransport } from "./transport.js";

test("endpointFromHost appends ingest path", () => {
  assert.equal(endpointFromHost("prostometrics.ru"), "https://prostometrics.ru/api/i/batch");
  assert.equal(endpointFromHost("https://prostometrics.ru"), "https://prostometrics.ru/api/i/batch");
  assert.equal(endpointFromHost("http://localhost:8085"), "http://localhost:8085/api/i/batch");
  assert.equal(endpointFromHost("https://collector.example.com/api/i/batch"), "https://collector.example.com/api/i/batch");
});

test("applyDefaults requires API key for HTTP transport", () => {
  assert.throws(() => applyDefaults("api", { endpoint: "https://collector.example.com/api/i/batch" }), MissingAPIKeyError);
});

test("applyDefaults allows custom non-HTTP transport without API key", () => {
  const transport = { async send() {} };
  assert.equal(applyDefaults("api", { transport }).transport, transport);
});

test("applyDefaults logs warnings by default and supports silent mode", () => {
  const transport = { async send() {} };
  assert.equal(applyDefaults("api", { transport }).logger, consoleLogger);
  assert.equal(applyDefaults("api", { transport, silent: true }).logger, noopLogger);
});

test("applyDefaults propagates verbose mode to HTTP transport", () => {
  const transport = new HTTPTransport({ endpoint: "https://collector.example.com/api/i/batch", apiKey: "42_secret" });
  const config = applyDefaults("api", { transport, verbose: true });
  assert.equal(config.verbose, true);
  assert.equal(transport.verbose, true);
});

test("applyDefaults sends API keys opaquely", () => {
  const cfg = applyDefaults("api", {
    endpoint: "https://collector.example.com/api/i/batch",
    apiKey: "Bearer 123_secret",
  });
  assert.equal(cfg.apiKey, "Bearer 123_secret");
});

test("applyDefaults rejects API key and custom Authorization conflict", () => {
  assert.throws(
    () =>
      applyDefaults("api", {
        endpoint: "https://collector.example.com/api/i/batch",
        apiKey: "42_secret",
        headers: { Authorization: "Bearer custom" },
      }),
    APIKeyAuthorizationConflictError,
  );
});

test("applyDefaults rejects existing HTTP transport Authorization conflict", () => {
  const transport = new HTTPTransport({
    endpoint: "https://collector.example.com/api/i/batch",
    headers: { Authorization: "Bearer custom" },
  });
  assert.throws(() => applyDefaults("api", { apiKey: "42_secret", transport }), APIKeyAuthorizationConflictError);
});
