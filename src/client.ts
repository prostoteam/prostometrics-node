import {
  DEFAULT_CLIENT_BACKOFF_JITTER_WINDOW_MS,
  DEFAULT_CLIENT_BACKOFF_MAX_DELAY_MS,
  DEFAULT_FLUSH_INTERVAL_MS,
  DEFAULT_MAX_BATCH_SIZE,
  DEFAULT_MAX_TOTAL_SERIES,
  DEFAULT_QUEUE_SIZE,
  DEFAULT_RETRY_BASE_DELAY_MS,
  DEFAULT_RETRY_FLUSH_MAX_SENDS,
  DEFAULT_RETRY_JITTER_WINDOW_MS,
  DEFAULT_RETRY_MAX_ATTEMPTS,
  DEFAULT_RETRY_MAX_DELAY_MS,
  DEFAULT_RETRY_QUEUE_SIZE,
} from "./constants.js";
import { applyDefaults, type Config, type ResolvedConfig } from "./config.js";
import { HTTPTransportError, isStopIngestError } from "./errors.js";
import { BatchBuilder } from "./batch-builder.js";
import { hasWorkloadLabel, normalizeLabels } from "./labels.js";
import type { Event, Payload } from "./payload.js";
import { payloadIsEmpty } from "./payload.js";
import { RingBuffer } from "./ring-buffer.js";
import { seriesKey } from "./series.js";
import { log } from "./transport.js";
import { canonicalUniqueID } from "./unique-id.js";
import { ValueRateLimiter } from "./value-rate-limiter.js";

export interface CloseOptions {
  signal?: AbortSignal;
}

interface RetryBatch {
  payload: Payload;
  attempts: number;
  nextAttempt: number;
}

export class Client {
  private readonly queue = new RingBuffer<Event>(DEFAULT_QUEUE_SIZE);
  private readonly retryQueue: RetryBatch[] = [];
  private readonly totals = new Map<string, number>();
  private readonly valueRateLimiter = new ValueRateLimiter();
  private readonly config: ResolvedConfig;
  private readonly batchSession = Date.now().toString(36);
  private batchSeq = 0;
  private droppedCount = 0;
  private closed = false;
  private closing = false;
  private stopSending = false;
  private flushing = false;
  private flushAgain = false;
  private transientBackoffAttempt = 0;
  private nextSendAttempt = 0;
  private readonly timer: NodeJS.Timeout;

  constructor(private readonly workload: string, private readonly rawConfig: Config = {}) {
    this.config = applyDefaults(this.workload, this.rawConfig);
    if (this.config.verbose) {
      log(this.config.logger, "prostometrics: client version %s", version());
    }
    this.timer = setInterval(() => {
      void this.flushDue(false);
    }, DEFAULT_FLUSH_INTERVAL_MS);
    this.timer.unref?.();
  }

  count(metric: string, delta: number, ...labels: string[]): void {
    this.enqueue("counter", metric, delta, labels);
  }

  Count(metric: string, delta: number, ...labels: string[]): void {
    this.count(metric, delta, ...labels);
  }

  countUnique(uniqueID: unknown, metric: string, ...labels: string[]): void {
    const encodedID = canonicalUniqueID(uniqueID);
    if (!encodedID) {
      return;
    }
    this.enqueue("unique", metric, 0, labels, encodedID);
  }

  CountUnique(uniqueID: unknown, metric: string, ...labels: string[]): void {
    this.countUnique(uniqueID, metric, ...labels);
  }

  total(metric: string, total: number, ...labels: string[]): void {
    this.enqueue("total", metric, total, labels);
  }

  Total(metric: string, total: number, ...labels: string[]): void {
    this.total(metric, total, ...labels);
  }

  value(metric: string, value: number, ...labels: string[]): void {
    if (!this.allowValue(metric)) {
      return;
    }
    this.enqueue("value", metric, value, labels);
  }

  Value(metric: string, value: number, ...labels: string[]): void {
    this.value(metric, value, ...labels);
  }

  valueSparse(metric: string, value: number, ...labels: string[]): void {
    if (!this.allowValue(metric)) {
      return;
    }
    this.enqueue("value_sparse", metric, value, labels);
  }

  ValueSparse(metric: string, value: number, ...labels: string[]): void {
    this.valueSparse(metric, value, ...labels);
  }

  dropped(): number {
    return this.droppedCount;
  }

  Dropped(): number {
    return this.dropped();
  }

  async close(options: CloseOptions = {}): Promise<void> {
    if (this.closed || this.closing) {
      return;
    }
    this.closing = true;
    clearInterval(this.timer);
    while (this.queue.length > 0 && !options.signal?.aborted) {
      await this.flushOneBatch(true, options.signal);
    }
    await this.flushRetryQueue(true, options.signal);
    this.closed = true;
  }

  async Close(options: CloseOptions = {}): Promise<void> {
    await this.close(options);
  }

  // Check the value limit before ValueSparse creates its additional label array.
  private allowValue(metric: string): boolean {
    if (metric === "" || this.closed || this.closing || this.stopSending) {
      return false;
    }
    return this.valueRateLimiter.allow();
  }

  private enqueue(type: Event["type"], metric: string, value: number, labels: readonly string[], uniqueID?: string): void {
    try {
      if (metric === "" || this.closed || this.closing || this.stopSending) {
        return;
      }
      const normalizedLabels = normalizeLabels(labels);
      if (hasWorkloadLabel(normalizedLabels)) {
        log(this.config.logger, "prostometrics: workload label is reserved; drop metric %s", metric);
        return;
      }
      const event: Event = {
        type,
        metric,
        value,
        uniqueID,
        labels: normalizedLabels,
        timestamp: Math.floor(Date.now() / 1000),
      };
      if (!this.queue.push(event)) {
        this.droppedCount += 1;
        return;
      }
      if (this.queue.length >= DEFAULT_MAX_BATCH_SIZE) {
        this.scheduleFlush();
      }
    } catch {
      // Metric calls must never throw into the application.
    }
  }

  private scheduleFlush(): void {
    const immediate = setImmediate(() => {
      void this.flushDue(false);
    });
    immediate.unref?.();
  }

  private async flushDue(ignoreRetryBackoff: boolean): Promise<void> {
    if (this.flushing) {
      this.flushAgain = true;
      return;
    }
    this.flushing = true;
    try {
      await this.flushOneBatch(false);
      await this.flushRetryQueue(ignoreRetryBackoff);
    } finally {
      this.flushing = false;
    }
    if (this.flushAgain && !this.closed) {
      this.flushAgain = false;
      this.scheduleFlush();
    }
  }

  private async flushOneBatch(ignoreRetryBackoff: boolean, signal?: AbortSignal): Promise<void> {
    const events: Event[] = [];
    while (events.length < DEFAULT_MAX_BATCH_SIZE) {
      const event = this.queue.shift();
      if (!event) {
        break;
      }
      if (event.type === "total") {
        const converted = this.applyTotal(event);
        if (!converted) {
          continue;
        }
        events.push(converted);
        continue;
      }
      events.push(event);
    }
    if (events.length === 0) {
      return;
    }
    const payload = this.buildPayload(events);
    if (!payload) {
      return;
    }
    await this.sendPayload(payload, 1, false, signal);
    if (ignoreRetryBackoff) {
      return;
    }
  }

  private applyTotal(event: Event): Event | undefined {
    const key = seriesKey(event.metric, event.labels);
    const previous = this.totals.get(key);
    if (previous === undefined) {
      if (this.totals.size >= DEFAULT_MAX_TOTAL_SERIES) {
        return undefined;
      }
      this.totals.set(key, event.value);
      return undefined;
    }
    if (event.value < previous) {
      this.totals.set(key, event.value);
      return undefined;
    }
    const delta = event.value - previous;
    this.totals.set(key, event.value);
    if (delta <= 0) {
      return undefined;
    }
    return { ...event, type: "counter", value: delta };
  }

  private buildPayload(events: readonly Event[]): Payload | undefined {
    const builder = new BatchBuilder();
    for (const event of events) {
      builder.add(event);
    }
    const payload = builder.build();
    if (!payload) {
      return undefined;
    }
    payload.batchID = this.nextBatchID();
    return payload;
  }

  private nextBatchID(): string {
    this.batchSeq += 1;
    return `${this.batchSession}-${this.batchSeq.toString(36)}`;
  }

  private async sendPayload(payload: Payload, attempt: number, fromRetry: boolean, signal?: AbortSignal): Promise<void> {
    if (payloadIsEmpty(payload) || this.stopSending || signal?.aborted) {
      return;
    }
    if (!payload.batchID) {
      payload.batchID = this.nextBatchID();
    }
    if (this.config.verbose && !fromRetry) {
      log(this.config.logger, "prostometrics: flushing %s events", payload.counters.length + payload.values.length + payload.uniques.length);
    }
    if (this.deferForClientBackoff(payload, attempt)) {
      return;
    }
    try {
      await this.config.transport.send(payload, { signal });
      this.resetTransientBackoff();
    } catch (err) {
      if (isStopIngestError(err)) {
        this.stopSending = true;
        log(this.config.logger, "prostometrics: ingest disabled after non-retryable transport response: %s", String(err));
        return;
      }
      if (this.shouldRetryTransport(err)) {
        this.noteTransientFailure();
        if (this.enqueueRetry(payload, attempt, err)) {
          return;
        }
      }
      log(this.config.logger, "prostometrics: flush failed: %s; %s", String(err), this.flushFailureDetails(payload));
    }
  }

  private deferForClientBackoff(payload: Payload, attempt: number): boolean {
    if (this.nextSendAttempt === 0 || Date.now() >= this.nextSendAttempt) {
      return false;
    }
    this.enqueueRetryAt(payload, Math.max(0, attempt - 1), this.nextSendAttempt, new Error("prostometrics: transient ingest backoff active"));
    return true;
  }

  private noteTransientFailure(): void {
    this.transientBackoffAttempt += 1;
    this.nextSendAttempt = Date.now() + clientBackoffDelay(this.transientBackoffAttempt);
  }

  private resetTransientBackoff(): void {
    this.transientBackoffAttempt = 0;
    this.nextSendAttempt = 0;
  }

  private enqueueRetry(payload: Payload, attempt: number, err: unknown): boolean {
    return this.enqueueRetryAt(payload, attempt, Date.now() + retryDelay(attempt), err);
  }

  private enqueueRetryAt(payload: Payload, attempts: number, nextAttempt: number, err: unknown): boolean {
    if (attempts >= DEFAULT_RETRY_MAX_ATTEMPTS) {
      log(this.config.logger, "prostometrics: retry budget exhausted for batchId=%s attempts=%s; %s", payload.batchID ?? "", attempts, String(err));
      return false;
    }
    if (this.retryQueue.length >= DEFAULT_RETRY_QUEUE_SIZE) {
      log(this.config.logger, "prostometrics: retry queue full, dropping batchId=%s pending=%s; %s", payload.batchID ?? "", this.retryQueue.length, String(err));
      return false;
    }
    this.retryQueue.push({ payload, attempts, nextAttempt });
    return true;
  }

  private async flushRetryQueue(ignoreBackoff: boolean, signal?: AbortSignal): Promise<void> {
    if (this.retryQueue.length === 0 || this.stopSending || signal?.aborted) {
      return;
    }
    const now = Date.now();
    let processed = 0;
    const processLimit = Math.min(this.retryQueue.length, DEFAULT_RETRY_FLUSH_MAX_SENDS);
    for (let i = 0; i < this.retryQueue.length && processed < processLimit; ) {
      const item = this.retryQueue[i]!;
      if (!ignoreBackoff && item.nextAttempt > now) {
        i += 1;
        continue;
      }
      this.retryQueue.splice(i, 1);
      await this.sendPayload(item.payload, item.attempts + 1, true, signal);
      processed += 1;
    }
  }

  private shouldRetryTransport(err: unknown): boolean {
    if (err instanceof HTTPTransportError) {
      return [408, 429, 500, 502, 503, 504].includes(err.statusCode);
    }
    if (err instanceof Error) {
      return err.name === "AbortError" || err.name === "TimeoutError" || /ECONNRESET|ECONNREFUSED|ETIMEDOUT|EPIPE|fetch failed/i.test(err.message);
    }
    return false;
  }

  private flushFailureDetails(payload: Payload): string {
    return [
      `batchId=${payload.batchID ?? ""}`,
      `events=${payload.counters.length + payload.values.length + payload.uniques.length}`,
      `counters=${payload.counters.length}`,
      `values=${payload.values.length}`,
      `uniques=${payload.uniques.length}`,
      `queueDepth=${this.queue.length}`,
      `dropped=${this.droppedCount}`,
    ].join(" ");
  }
}

function retryDelay(attempt: number): number {
  const safeAttempt = Math.max(1, attempt);
  let delay = DEFAULT_RETRY_BASE_DELAY_MS;
  for (let step = 1; step < safeAttempt && delay < DEFAULT_RETRY_MAX_DELAY_MS; step += 1) {
    delay = Math.min(delay * 2, DEFAULT_RETRY_MAX_DELAY_MS);
  }
  return delay + jitter(DEFAULT_RETRY_JITTER_WINDOW_MS);
}

function clientBackoffDelay(attempt: number): number {
  const safeAttempt = Math.max(1, attempt);
  let delay = DEFAULT_RETRY_BASE_DELAY_MS;
  for (let step = 1; step < safeAttempt && delay < DEFAULT_CLIENT_BACKOFF_MAX_DELAY_MS; step += 1) {
    delay = Math.min(delay * 2, DEFAULT_CLIENT_BACKOFF_MAX_DELAY_MS);
  }
  return delay + jitter(DEFAULT_CLIENT_BACKOFF_JITTER_WINDOW_MS);
}

function jitter(windowMs: number): number {
  return windowMs <= 0 ? 0 : Date.now() % windowMs;
}

export function version(): string {
  return "0.1.0";
}

let defaultClient: Client | undefined;

export function init(workload: string, config: Config = {}): Client {
  const client = new Client(workload, config);
  const previous = defaultClient;
  defaultClient = client;
  if (previous) {
    void previous.close();
  }
  return client;
}

export const Init = init;

export function Default(): Client | undefined {
  return defaultClient;
}

export function count(metric: string, delta: number, ...labels: string[]): void {
  defaultClient?.count(metric, delta, ...labels);
}

export const Count = count;

export function countUnique(uniqueID: unknown, metric: string, ...labels: string[]): void {
  defaultClient?.countUnique(uniqueID, metric, ...labels);
}

export const CountUnique = countUnique;

export function total(metric: string, currentTotal: number, ...labels: string[]): void {
  defaultClient?.total(metric, currentTotal, ...labels);
}

export const Total = total;

export function value(metric: string, sample: number, ...labels: string[]): void {
  defaultClient?.value(metric, sample, ...labels);
}

export const Value = value;

export function valueSparse(metric: string, sample: number, ...labels: string[]): void {
  defaultClient?.valueSparse(metric, sample, ...labels);
}

export const ValueSparse = valueSparse;
