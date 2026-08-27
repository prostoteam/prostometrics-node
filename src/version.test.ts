import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { version } from "./client.js";
import { VERSION } from "./version.js";

// VERSION is what the service sees as X-PM-Client, and the version it attributes
// ingest traffic to. It once shipped a release behind package.json unnoticed,
// because the constant lived inline in client.ts and nothing tied the two
// together. This test is that tie.
test("the reported version matches the published one", () => {
  const packageJSON = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8")) as { version: string };
  assert.equal(VERSION, packageJSON.version);
  assert.equal(version(), packageJSON.version);
});
