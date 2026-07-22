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
};
```

Use `valueSparse` for values whose last observation should carry across missing time buckets.
