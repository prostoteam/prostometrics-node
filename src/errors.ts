export class ProstometricsError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class NoTransportError extends ProstometricsError {
  constructor() {
    super("prostometrics: no transport configured");
  }
}

export class ClientClosedError extends ProstometricsError {
  constructor() {
    super("prostometrics: client closed");
  }
}

export class InvalidWorkloadError extends ProstometricsError {
  constructor() {
    super("prostometrics: invalid workload");
  }
}

export class MissingAPIKeyError extends ProstometricsError {
  constructor() {
    super("prostometrics: API key is required for HTTP transport");
  }
}

export class APIKeyAuthorizationConflictError extends ProstometricsError {
  constructor() {
    super("prostometrics: API key conflicts with custom Authorization header");
  }
}

export class StopIngestError extends ProstometricsError {
  readonly code?: number;
  readonly cause?: unknown;

  constructor(message: string, options: { code?: number; cause?: unknown } = {}) {
    super(message);
    this.code = options.code;
    this.cause = options.cause;
  }

  stopIngest(): boolean {
    return true;
  }
}

export function isStopIngestError(err: unknown): err is StopIngestError {
  return Boolean(err && typeof err === "object" && "stopIngest" in err && (err as StopIngestError).stopIngest());
}

export interface HTTPTransportErrorOptions {
  method?: string;
  endpoint: string;
  batchID?: string;
  statusCode?: number;
  status?: string;
  responseCode?: string;
  detail?: string;
  accepted?: number;
  dropped?: number;
  rejected?: number;
  requestBytes?: number;
  dictionarySession?: string;
  dictionaryRevision?: number;
  dictionarySeries?: number;
  seriesDefinitions?: number;
  eventLines?: number;
}

export class HTTPTransportError extends ProstometricsError {
  readonly method: string;
  readonly endpoint: string;
  readonly batchID: string;
  readonly statusCode: number;
  readonly status: string;
  readonly responseCode: string;
  readonly detail: string;
  readonly accepted: number;
  readonly dropped: number;
  readonly rejected: number;
  readonly requestBytes: number;
  readonly dictionarySession: string;
  readonly dictionaryRevision: number;
  readonly dictionarySeries: number;
  readonly seriesDefinitions: number;
  readonly eventLines: number;

  constructor(options: HTTPTransportErrorOptions) {
    const method = options.method ?? "POST";
    const status = options.status ?? "";
    const requestBytes = options.requestBytes ?? 0;
    const detail = options.detail ?? "";
    const requestSuffix = requestBytes > 0 ? ` (request_bytes=${requestBytes})` : "";
    const statusPart = status === "" ? "failed" : status;
    const detailPart = detail === "" ? "" : `: ${detail}`;
    super(`${method} ${options.endpoint}: ${statusPart}${detailPart}${requestSuffix}`);
    this.method = method;
    this.endpoint = options.endpoint;
    this.batchID = options.batchID ?? "";
    this.statusCode = options.statusCode ?? 0;
    this.status = status;
    this.responseCode = options.responseCode ?? "";
    this.detail = detail;
    this.accepted = options.accepted ?? -1;
    this.dropped = options.dropped ?? -1;
    this.rejected = options.rejected ?? -1;
    this.requestBytes = requestBytes;
    this.dictionarySession = options.dictionarySession ?? "";
    this.dictionaryRevision = options.dictionaryRevision ?? 0;
    this.dictionarySeries = options.dictionarySeries ?? 0;
    this.seriesDefinitions = options.seriesDefinitions ?? 0;
    this.eventLines = options.eventLines ?? 0;
  }
}
