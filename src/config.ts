import { DEFAULT_ENDPOINT_HOST, DEFAULT_INGEST_PATH } from "./constants.js";
import { APIKeyAuthorizationConflictError, MissingAPIKeyError, NoTransportError } from "./errors.js";
import type { Logger, Transport } from "./transport.js";
import { HTTPTransport } from "./transport.js";
import { validateWorkload } from "./workload.js";

export interface Config {
  endpoint?: string;
  apiKey?: string;
  transport?: Transport;
  logger?: Logger;
  verbose?: boolean;
  headers?: Record<string, string | readonly string[]>;
  fetch?: typeof globalThis.fetch;
}

export interface ResolvedConfig {
  endpoint: string;
  apiKey: string;
  transport: Transport;
  logger: Logger;
  verbose: boolean;
}

export const noopLogger: Logger = {
  printf() {
    // intentionally silent
  },
};

export const consoleLogger: Logger = {
  printf(format: string, ...args: unknown[]) {
    const rendered = format.replace(/%[sdv]/g, () => String(args.shift()));
    console.error(rendered, ...args);
  },
};

export function endpointFromHost(host: string): string {
  const trimmed = host.trim();
  if (trimmed === "") {
    return "";
  }
  if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) {
    return ensureIngestPath(trimmed);
  }
  return ensureIngestPath(`https://${trimmed}`);
}

export const EndpointFromHost = endpointFromHost;

export function ensureIngestPath(endpoint: string): string {
  if (endpoint === "") {
    return "";
  }
  try {
    const url = new URL(endpoint);
    if (url.pathname === "" || url.pathname === "/") {
      url.pathname = DEFAULT_INGEST_PATH;
    }
    return url.toString();
  } catch {
    return endpoint;
  }
}

export function applyDefaults(workload: string, config: Config = {}): ResolvedConfig {
  const trimmedWorkload = workload.trim();
  validateWorkload(trimmedWorkload);

  let endpoint = config.endpoint ?? "";
  let transport = config.transport;
  const apiKey = config.apiKey ?? "";
  const logger = config.logger ?? consoleLogger;

  if (endpoint === "" && transport === undefined) {
    endpoint = endpointFromHost(DEFAULT_ENDPOINT_HOST);
  }
  if (endpoint !== "") {
    endpoint = ensureIngestPath(endpoint);
  }

  if (transport === undefined && endpoint !== "") {
    transport = new HTTPTransport({
      endpoint,
      apiKey,
      workload: trimmedWorkload,
      headers: config.headers,
      logger,
      fetch: config.fetch,
    });
  }

  if (transport === undefined) {
    throw new NoTransportError();
  }

  if (transport instanceof HTTPTransport) {
    const effectiveHeaders = config.headers ?? transport.headers;
    if (hasAuthorizationHeader(effectiveHeaders) && (apiKey !== "" || transport.apiKey !== "")) {
      throw new APIKeyAuthorizationConflictError();
    }
    if (transport.apiKey === "" && apiKey !== "") {
      transport.apiKey = apiKey;
    }
    if (transport.apiKey === "") {
      throw new MissingAPIKeyError();
    }
    transport.logger = transport.logger ?? logger;
    endpoint = endpoint || transport.endpoint;
    transport = new WorkloadHTTPTransport(transport, trimmedWorkload);
  }

  return {
    endpoint,
    apiKey,
    transport,
    logger,
    verbose: config.verbose ?? false,
  };
}

class WorkloadHTTPTransport implements Transport {
  constructor(
    private readonly base: HTTPTransport,
    private readonly workload: string,
  ) {}

  async send(payload: Parameters<Transport["send"]>[0], options: Parameters<Transport["send"]>[1] = {}): Promise<void> {
    await this.base.send(payload, { ...options, workload: this.workload });
  }
}

function hasAuthorizationHeader(headers: Record<string, string | readonly string[]>): boolean {
  for (const [key, raw] of Object.entries(headers)) {
    if (key.toLowerCase() !== "authorization") {
      continue;
    }
    const values = Array.isArray(raw) ? raw : [raw];
    return values.some((value) => String(value).trim() !== "");
  }
  return false;
}
