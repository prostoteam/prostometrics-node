export const DEFAULT_QUEUE_SIZE = 64 * 1024;
export const DEFAULT_MAX_BATCH_SIZE = 512;
export const DEFAULT_MAX_SERIES_PER_BATCH = 2048;
export const DEFAULT_MAX_DICTIONARY_SERIES = DEFAULT_MAX_SERIES_PER_BATCH;
export const DEFAULT_MAX_TOTAL_SERIES = DEFAULT_MAX_SERIES_PER_BATCH;
export const DEFAULT_FLUSH_INTERVAL_MS = 500;
export const DEFAULT_FLUSH_TIMEOUT_MS = 5000;
export const DEFAULT_RETRY_QUEUE_SIZE = 4096;
export const DEFAULT_RETRY_FLUSH_MAX_SENDS = 1;
export const DEFAULT_RETRY_BASE_DELAY_MS = 1000;
export const DEFAULT_RETRY_MAX_DELAY_MS = 8000;
export const DEFAULT_RETRY_JITTER_WINDOW_MS = 1000;
export const DEFAULT_OUTAGE_BUFFER_MAX_AGE_MS = 30 * 60 * 1000;
export const DEFAULT_OUTAGE_BUFFER_MAX_EVENTS = 256 * 1024;
export const DEFAULT_OUTAGE_BUFFER_MAX_BYTES = 64 * 1024 * 1024;
export const DEFAULT_REPLAY_INTERVAL_MS = 1000;
export const DEFAULT_RECOVERY_JITTER_WINDOW_MS = 30 * 1000;
export const DEFAULT_CLIENT_BACKOFF_MAX_DELAY_MS = 30000;
export const DEFAULT_CLIENT_BACKOFF_JITTER_WINDOW_MS = 5000;
export const DEFAULT_ENDPOINT_HOST = "prostometrics.ru";
export const DEFAULT_INGEST_PATH = "/api/i/batch";
export const HTTP_STATUS_UNAUTHORIZED = 401;
export const DEFAULT_STOP_STATUS_CODE = HTTP_STATUS_UNAUTHORIZED;
export const RESPONSE_CODE_UNAUTHORIZED = "unauthorized";
export const DEFAULT_STOP_RESPONSE_CODES = [RESPONSE_CODE_UNAUTHORIZED, "unsupported_protocol_version"] as const;

// A key created moments before the process started may not have reached the
// ingester's served key set yet, so the first requests of a brand-new project
// can be refused. Retrying those for a short window turns that race into a
// delay instead of a process that never reports a single metric. See the
// startup authentication grace in the ingest protocol specification.
export const DEFAULT_AUTH_GRACE_WINDOW_MS = 30_000;
export const DEFAULT_AUTH_GRACE_RETRY_INTERVAL_MS = 2_000;
export const BATCH_ID_HEADER_NAME = "X-PM-Batch-Id";
export const WORKLOAD_HEADER_NAME = "X-PM-Workload";
export const ACCEPTED_HEADER_NAME = "X-PM-Accepted";
export const DROPPED_HEADER_NAME = "X-PM-Dropped";
export const REJECTED_HEADER_NAME = "X-PM-Rejected";
export const WORKLOAD_MAX_LEN = 100;
