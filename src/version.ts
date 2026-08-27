// Sent on every request as X-PM-Client, and the version the service attributes
// ingest traffic to. Keep in step with package.json; `version.test.ts` fails the
// build when the two drift, which is how this constant silently went stale once.
export const VERSION = "0.2.2";
