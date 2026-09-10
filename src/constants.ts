export const DEFAULT_QUEUE_SIZE = 64 * 1024;

// A batch closes on whichever of these it reaches first, and both are what the
// ingest endpoint accepts rather than a guess. The event count matters for
// unique and top metrics, whose repeats are dropped per batch: a bigger batch
// collapses more of them and so sends less.
export const DEFAULT_MAX_BATCH_SIZE = 4096;

// Well under the 265 KiB the endpoint takes, because the series definitions and
// the header ride along with the events and are not counted here.
export const DEFAULT_MAX_BATCH_BYTES = 160 * 1024;

// How many distinct series one batch may carry. The endpoint refuses a batch
// defining more than this many, and a batch defines every series in it that the
// dictionary has not seen -- which on a first flush is all of them. Bounding
// events alone is not enough: 4096 events can be 4096 series.
export const DEFAULT_MAX_BATCH_SERIES = 1024;

// Three separate ceilings that are not that one, and must not be conflated with
// it: how many counter series one batch aggregates into single events, how
// large the dictionary grows before it is thrown away and every definition
// re-sent, and how many distinct cumulative totals one process may track for
// the whole of its life.
export const DEFAULT_MAX_SERIES_PER_BATCH = 2048;
export const DEFAULT_MAX_DICTIONARY_SERIES = DEFAULT_MAX_SERIES_PER_BATCH;
export const DEFAULT_MAX_TOTAL_SERIES = DEFAULT_MAX_SERIES_PER_BATCH;

// Nothing reads a metric faster than this: the finest chart bucket is ten
// seconds and the dashboard refetches at most every few. Flushing oftener than
// anything can be seen only costs requests, and costs the batching that makes
// unique and top metrics affordable.
export const DEFAULT_FLUSH_INTERVAL_MS = 2000;

// Below this a compressed body saves less than the request's own headers cost,
// and spends the caller's processor to do it.
export const COMPRESS_MIN_BYTES = 1024;
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
export const MAX_METRIC_BYTES = 100;
export const MAX_LABELS_PER_SERIES = 8;
export const MAX_LABEL_BYTES = 512;
export const MAX_TOP_ITEM_BYTES = 256;
export const RESERVED_LABEL_NAME = "workload";
export const MAX_COUNTER_VALUE = 4294967295;
export const MAX_SAMPLE_VALUE = Math.floor(4294967295 / 10);
