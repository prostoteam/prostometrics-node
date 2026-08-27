# Prostometrics Node Client

Node.js client for sending application metrics to Prostometrics.

## Install

```bash
npm install @prostoteam/prostometrics-node
```

## Quick Start

```ts
import { init, count, countUnique, total, value, valueSparse, label } from "@prostoteam/prostometrics-node";

const client = init("payments-api", {
  apiKey: "your-prostometrics-api-key",
});

count("requests", 1, "service=api", label("method", "GET"));
countUnique(42n, "daily_active_users", "service=api");
total("host.net.kb", 2048, "iface=eth0", "dir=rx");
value("latency_ms", 123.4, "service=api", "endpoint=/login");
valueSparse("host.fs.capacity_kb", 1024 * 1024, "mount=/");

await client.close();
```

## API

```ts
const client = new Client(workload, config);
const client = init(workload, config);

client.count(metric, delta, ...labels);
client.countUnique(uniqueID, metric, ...labels);
client.total(metric, total, ...labels);
client.value(metric, value, ...labels);
client.valueSparse(metric, value, ...labels);
await client.close();
```

Package-level helpers (`count`, `countUnique`, `total`, `value`, `valueSparse`) use the client created by `init`.

`uniqueID` accepts non-negative safe integers, `bigint`, decimal strings, `Buffer`, or `Uint8Array`.

## Configuration

```ts
type Config = {
  apiKey?: string;
  logger?: { printf?: (format: string, ...args: unknown[]) => void };
  verbose?: boolean;
  silent?: boolean;
};
```

Warnings that indicate metric loss or disabled ingestion are written to stderr
by default. Routine recovery, retry, version, and flush diagnostics are logged
only when `verbose` is enabled. Set `silent` to disable all SDK logs.

During temporary outages, the client buffers up to 30 minutes of metrics in
memory and replays them gradually with their original timestamps. The buffer is
bounded and is lost when the process exits.

`apiKey` is required unless you supply your own `transport`, and `init` throws
`MissingAPIKeyError` when it is absent. Configuration errors are raised at
construction on purpose — a missing key is a deployment mistake worth failing
loudly at boot, not one to discover from a silent metric later. There is no
environment-variable fallback here; read the key yourself if you want one.

A rejected API key disables ingestion for the rest of the process, with one
exception: for the first 30 seconds after the client is created, a rejection is
retried instead. A key created moments earlier takes a few seconds to become
usable, so a process started right after the key was pasted in keeps its first
metrics rather than going quiet.

Two things narrow that window rather than widening it. A client key — the
`_pk_` kind, which belongs in a browser and can never authenticate here — is
refused immediately and named as such, because no amount of waiting will make
it work. And a process that ends while a rejection is still unresolved says so
during `close()`, naming the events it never delivered, so a short script
written to check that a key works cannot exit quietly on the "retrying" line
alone.

Use `valueSparse` for values whose last observation should carry across missing time buckets.
