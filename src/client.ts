import {
  DEFAULT_CLIENT_BACKOFF_JITTER_WINDOW_MS,
  DEFAULT_CLIENT_BACKOFF_MAX_DELAY_MS,
  DEFAULT_FLUSH_INTERVAL_MS,
  DEFAULT_MAX_BATCH_SIZE,
  DEFAULT_MAX_TOTAL_SERIES,
  DEFAULT_OUTAGE_BUFFER_MAX_AGE_MS,
  DEFAULT_OUTAGE_BUFFER_MAX_BYTES,
  DEFAULT_OUTAGE_BUFFER_MAX_EVENTS,
  DEFAULT_RECOVERY_JITTER_WINDOW_MS,
  DEFAULT_QUEUE_SIZE,
  DEFAULT_REPLAY_INTERVAL_MS,
  DEFAULT_RETRY_BASE_DELAY_MS,
  DEFAULT_RETRY_FLUSH_MAX_SENDS,
  DEFAULT_RETRY_JITTER_WINDOW_MS,
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
  bufferedAt: number;
  eventCount: number;
  estimatedBytes: number;
}

export interface ClientStats {
  queueDropped: number;
  retryDropped: number;
  queueDepth: number;
  bufferedEvents: number;
  bufferedBytes: number;
  bufferedBatches: number;
  ingestDisabled: boolean;
}

const LOCAL_DROP_LOG_INTERVAL_MS = 60_000;

export class Client {
  private readonly queue = new RingBuffer<Event>(DEFAULT_QUEUE_SIZE);
  private readonly retryQueue: RetryBatch[] = [];
  private readonly totals = new Map<string, number>();
  private readonly valueRateLimiter = new ValueRateLimiter();
  private readonly config: ResolvedConfig;
  private readonly batchSession = Date.now().toString(36);
  private batchSeq = 0;
  private droppedCount = 0;
  private retryDroppedCount = 0;
  private sendFailureCount = 0;
  private readonly localDropCounts = new Map<string, number>();
  private readonly localDropLogAt = new Map<string, number>();
  private closed = false;
  private closing = false;
  private stopSending = false;
  private flushAgain = false;
  private activeFlush: Promise<void> | undefined;
  private closePromise: Promise<void> | undefined;
  private transientBackoffAttempt = 0;
  private nextSendAttempt = 0;
  private nextReplayAttempt = 0;
  private recoveryConfirmed = false;
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
      this.logLocalDrop("invalid_unique_id", metric);
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

  stats(): ClientStats {
    const usage = retryQueueUsage(this.retryQueue);
    return {
      queueDropped: this.droppedCount,
      retryDropped: this.retryDroppedCount,
      queueDepth: this.queue.length,
      bufferedEvents: usage.events,
      bufferedBytes: usage.bytes,
      bufferedBatches: this.retryQueue.length,
      ingestDisabled: this.stopSending,
    };
  }

  Stats(): ClientStats {
    return this.stats();
  }

  close(options: CloseOptions = {}): Promise<void> {
    if (this.closePromise) {
      return this.closePromise;
    }
    this.closePromise = this.performClose(options);
    return this.closePromise;
  }

  private async performClose(options: CloseOptions): Promise<void> {
    const failureBaseline = this.sendFailureCount;
    this.closing = true;
    clearInterval(this.timer);
    try {
      if (this.activeFlush) {
        await this.activeFlush;
      }
      if (this.sendFailureCount > failureBaseline) {
        return;
      }
      while (this.queue.length > 0 && !options.signal?.aborted) {
        await this.flushOneBatch(true, options.signal);
      }
      await this.drainRetryQueue(options.signal);
    } finally {
      this.closed = true;
    }
  }

  async Close(options: CloseOptions = {}): Promise<void> {
    await this.close(options);
  }

  // Check the value limit before ValueSparse creates its additional label array.
  private allowValue(metric: string): boolean {
    if (metric === "" || this.closed || this.closing || this.stopSending) {
      return false;
    }
    if (!this.valueRateLimiter.allow()) {
      this.logLocalDrop("value_rate_limit", metric);
      return false;
    }
    return true;
  }

  private enqueue(type: Event["type"], metric: string, value: number, labels: readonly string[], uniqueID?: string): void {
    try {
      if (metric === "" || this.closed || this.closing || this.stopSending) {
        return;
      }
      const normalizedLabels = normalizeLabels(labels);
      if (hasWorkloadLabel(normalizedLabels)) {
        this.logLocalDrop("reserved_workload_label", metric);
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
        this.logLocalDrop("queue_full", metric, this.droppedCount);
        return;
      }
      if (this.queue.length >= DEFAULT_MAX_BATCH_SIZE) {
        this.scheduleFlush();
      }
    } catch {
      // Metric calls must never throw into the application.
      this.logLocalDrop("invalid_event", metric);
    }
  }

  // Emit one warning per drop reason per minute to preserve signal without spam.
  private logLocalDrop(reason: string, metric: string, total?: number): void {
    const count = total ?? (this.localDropCounts.get(reason) ?? 0) + 1;
    this.localDropCounts.set(reason, count);
    const now = Date.now();
    const last = this.localDropLogAt.get(reason) ?? 0;
    if (last !== 0 && now - last < LOCAL_DROP_LOG_INTERVAL_MS) {
      return;
    }
    this.localDropLogAt.set(reason, now);
    log(this.config.logger, "prostometrics: local metric drop reason=%s metric=%s total=%s", reason, String(metric).trim(), count);
  }

  private scheduleFlush(): void {
    if (this.closing || this.closed) {
      return;
    }
    const immediate = setImmediate(() => {
      void this.flushDue(false);
    });
    immediate.unref?.();
  }

  private flushDue(ignoreRetryBackoff: boolean): Promise<void> {
    if (this.closing || this.closed) {
      return Promise.resolve();
    }
    if (this.activeFlush) {
      this.flushAgain = true;
      return this.activeFlush;
    }
    const operation = this.performFlush(ignoreRetryBackoff);
    this.activeFlush = operation;
    return operation;
  }

  private async performFlush(ignoreRetryBackoff: boolean): Promise<void> {
    try {
      await this.flushOneBatch(false);
      await this.flushRetryQueue(ignoreRetryBackoff);
    } finally {
      this.activeFlush = undefined;
    }
    if (this.flushAgain && !this.closing && !this.closed) {
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

  private async sendPayload(payload: Payload, attempt: number, fromRetry: boolean, signal?: AbortSignal, bufferedAt = 0, ignoreClientBackoff = false): Promise<boolean> {
    if (payloadIsEmpty(payload) || this.stopSending || signal?.aborted) {
      return false;
    }
    if (!payload.batchID) {
      payload.batchID = this.nextBatchID();
    }
    if (this.config.verbose && !fromRetry) {
      log(this.config.logger, "prostometrics: flushing %s events", payload.counters.length + payload.values.length + payload.uniques.length);
    }
    if (!ignoreClientBackoff && this.deferForClientBackoff(payload, attempt, bufferedAt)) {
      return false;
    }
    try {
      await this.config.transport.send(payload, { signal });
      this.resetTransientBackoff();
    } catch (err) {
      this.sendFailureCount += 1;
      if (isStopIngestError(err)) {
        this.stopSending = true;
        log(this.config.logger, "prostometrics: ingest disabled after non-retryable transport response: %s", String(err));
        return false;
      }
      if (this.shouldRetryTransport(err)) {
        this.noteTransientFailure(err);
        if (this.enqueueRetry(payload, attempt, err, bufferedAt)) {
          return false;
        }
        // enqueueRetry reports terminal retry loss with the specific cause.
        return false;
      }
      log(this.config.logger, "prostometrics: flush failed: %s; %s", String(err), this.flushFailureDetails(payload));
      return false;
    }
    return true;
  }

  private deferForClientBackoff(payload: Payload, attempt: number, bufferedAt: number): boolean {
    if (this.nextSendAttempt === 0 || Date.now() >= this.nextSendAttempt) {
      return false;
    }
    this.enqueueRetryAt(payload, Math.max(0, attempt - 1), this.nextSendAttempt, new Error("prostometrics: transient ingest backoff active"), bufferedAt);
    return true;
  }

  private noteTransientFailure(err: unknown): void {
    this.transientBackoffAttempt += 1;
    const delay = Math.max(clientBackoffDelay(this.transientBackoffAttempt), retryAfterDelay(err));
    this.nextSendAttempt = Date.now() + delay;
  }

  private resetTransientBackoff(): void {
    this.transientBackoffAttempt = 0;
    this.nextSendAttempt = 0;
  }

  private enqueueRetry(payload: Payload, attempt: number, err: unknown, bufferedAt = 0): boolean {
    const delay = Math.max(retryDelay(attempt), retryAfterDelay(err));
    return this.enqueueRetryAt(payload, attempt, Date.now() + delay, err, bufferedAt);
  }

  private enqueueRetryAt(payload: Payload, attempts: number, nextAttempt: number, err: unknown, bufferedAt = 0): boolean {
    const now = Date.now();
    const preserved = bufferedAt > 0;
    const candidate: RetryBatch = {
      payload,
      attempts,
      nextAttempt,
      bufferedAt: preserved ? bufferedAt : now,
      eventCount: payloadEventCount(payload),
      estimatedBytes: estimatePayloadBytes(payload),
    };
    if (candidate.eventCount > DEFAULT_OUTAGE_BUFFER_MAX_EVENTS || candidate.estimatedBytes > DEFAULT_OUTAGE_BUFFER_MAX_BYTES) {
      this.dropRetryBatch(candidate, "outage_buffer_batch_too_large");
      return false;
    }

    if (!this.recoveryConfirmed) {
      const cutoff = now - DEFAULT_OUTAGE_BUFFER_MAX_AGE_MS;
      this.pruneExpiredRetryBatches(cutoff);
      if (candidate.bufferedAt < cutoff) {
        this.dropRetryBatch(candidate, "outage_buffer_expired");
        return false;
      }
    }

    while (retryQueueWouldOverflow(this.retryQueue, candidate, DEFAULT_OUTAGE_BUFFER_MAX_EVENTS, DEFAULT_OUTAGE_BUFFER_MAX_BYTES)) {
      if (this.retryQueue.length === 0) {
        this.dropRetryBatch(candidate, "outage_buffer_full");
        return false;
      }
      const oldest = oldestRetryBatchIndex(this.retryQueue);
      this.dropRetryBatch(this.retryQueue.splice(oldest, 1)[0]!, "outage_buffer_full");
    }
    this.retryQueue.push(candidate);
    if (this.config.verbose) {
      log(this.config.logger, "prostometrics: queued retry for batchId=%s nextAttempt=%s delayMs=%s; %s", payload.batchID ?? "", attempts + 1, Math.max(0, nextAttempt - now), String(err));
    }
    return true;
  }

  private async flushRetryQueue(ignoreBackoff: boolean, signal?: AbortSignal): Promise<void> {
    if (this.retryQueue.length === 0 || this.stopSending || signal?.aborted) {
      return;
    }
    const now = Date.now();
    if (!this.recoveryConfirmed) {
      this.pruneExpiredRetryBatches(now - DEFAULT_OUTAGE_BUFFER_MAX_AGE_MS);
      if (this.retryQueue.length === 0) {
        return;
      }
    }
    if (!ignoreBackoff && this.nextReplayAttempt > now) {
      return;
    }
    let processed = 0;
    const processLimit = Math.min(this.retryQueue.length, DEFAULT_RETRY_FLUSH_MAX_SENDS);
    while (this.retryQueue.length > 0 && processed < processLimit) {
      const i = oldestDueRetryBatchIndex(this.retryQueue, now, ignoreBackoff);
      if (i < 0) {
        break;
      }
      const item = this.retryQueue[i]!;
      this.retryQueue.splice(i, 1);
      if (await this.sendPayload(item.payload, item.attempts + 1, true, signal, item.bufferedAt)) {
        this.recoveryConfirmed = true;
      }
      processed += 1;
    }
    if (processed > 0 && !ignoreBackoff) {
      this.nextReplayAttempt = Date.now() + DEFAULT_REPLAY_INTERVAL_MS;
    }
    if (this.retryQueue.length === 0) {
      this.recoveryConfirmed = false;
    }
  }

  private pruneExpiredRetryBatches(cutoff: number): void {
    for (let i = 0; i < this.retryQueue.length; ) {
      if (this.retryQueue[i]!.bufferedAt < cutoff) {
        this.dropRetryBatch(this.retryQueue.splice(i, 1)[0]!, "outage_buffer_expired");
        continue;
      }
      i += 1;
    }
  }

  private async drainRetryQueue(signal?: AbortSignal): Promise<void> {
    while (this.retryQueue.length > 0 && !this.stopSending && !signal?.aborted) {
      const now = Date.now();
      if (!this.recoveryConfirmed) {
        this.pruneExpiredRetryBatches(now - DEFAULT_OUTAGE_BUFFER_MAX_AGE_MS);
        if (this.retryQueue.length === 0) {
          break;
        }
      }

      const itemIndex = oldestRetryBatchIndex(this.retryQueue);
      const item = this.retryQueue[itemIndex]!;
      let due = Math.max(item.nextAttempt, this.nextSendAttempt, this.nextReplayAttempt);
      if (!this.recoveryConfirmed) {
        due = Math.min(due, item.bufferedAt + DEFAULT_OUTAGE_BUFFER_MAX_AGE_MS);
      }
      if (!(await waitUntil(due, signal))) {
        return;
      }
      if (!this.recoveryConfirmed && Date.now() > item.bufferedAt + DEFAULT_OUTAGE_BUFFER_MAX_AGE_MS) {
        continue;
      }

      this.retryQueue.splice(itemIndex, 1);
      const succeeded = await this.sendPayload(item.payload, item.attempts + 1, true, signal, item.bufferedAt, true);
      if (!succeeded) {
        return;
      }
      this.recoveryConfirmed = true;
      this.nextReplayAttempt = Date.now() + DEFAULT_REPLAY_INTERVAL_MS;
    }
    if (this.retryQueue.length === 0) {
      this.recoveryConfirmed = false;
    }
  }

  private dropRetryBatch(item: RetryBatch, reason: string): void {
    this.retryDroppedCount += item.eventCount;
    this.logLocalDrop(reason, "", this.retryDroppedCount);
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

function payloadEventCount(payload: Payload): number {
  return payload.counters.length + payload.values.length + payload.uniques.length;
}

function estimatePayloadBytes(payload: Payload): number {
  let bytes = 128 + (payload.batchID?.length ?? 0);
  const addLabels = (metric: string, labels: readonly string[]): void => {
    bytes += 64 + Buffer.byteLength(metric);
    for (const label of labels) {
      bytes += 16 + Buffer.byteLength(label);
    }
  };
  for (const event of payload.counters) {
    addLabels(event.metric, event.labels);
  }
  for (const event of payload.values) {
    addLabels(event.metric, event.labels);
  }
  for (const event of payload.uniques) {
    addLabels(event.metric, event.labels);
    bytes += Buffer.byteLength(event.uniqueID);
  }
  return bytes;
}

function retryQueueUsage(queue: readonly RetryBatch[]): { events: number; bytes: number } {
  let events = 0;
  let bytes = 0;
  for (const item of queue) {
    events += item.eventCount;
    bytes += item.estimatedBytes;
  }
  return { events, bytes };
}

function retryQueueWouldOverflow(queue: readonly RetryBatch[], candidate: RetryBatch, maxEvents: number, maxBytes: number): boolean {
  if (queue.length >= DEFAULT_RETRY_QUEUE_SIZE) {
    return true;
  }
  const usage = retryQueueUsage(queue);
  return usage.events + candidate.eventCount > maxEvents || usage.bytes + candidate.estimatedBytes > maxBytes;
}

function oldestRetryBatchIndex(queue: readonly RetryBatch[]): number {
  let oldest = 0;
  for (let i = 1; i < queue.length; i += 1) {
    if (queue[i]!.bufferedAt < queue[oldest]!.bufferedAt) {
      oldest = i;
    }
  }
  return oldest;
}

function oldestDueRetryBatchIndex(queue: readonly RetryBatch[], now: number, ignoreBackoff: boolean): number {
  let oldest = -1;
  for (let i = 0; i < queue.length; i += 1) {
    if (!ignoreBackoff && queue[i]!.nextAttempt > now) {
      continue;
    }
    if (oldest < 0 || queue[i]!.bufferedAt < queue[oldest]!.bufferedAt) {
      oldest = i;
    }
  }
  return oldest;
}

function retryAfterDelay(err: unknown): number {
  return err instanceof HTTPTransportError ? Math.max(0, err.retryAfterMs) : 0;
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
  const jitterWindow = safeAttempt === 1 ? DEFAULT_RECOVERY_JITTER_WINDOW_MS : DEFAULT_CLIENT_BACKOFF_JITTER_WINDOW_MS;
  return Math.min(delay + jitter(jitterWindow), DEFAULT_CLIENT_BACKOFF_MAX_DELAY_MS);
}

function jitter(windowMs: number): number {
  return windowMs <= 0 ? 0 : Math.floor(Math.random() * windowMs);
}

function waitUntil(due: number, signal?: AbortSignal): Promise<boolean> {
  const delay = due - Date.now();
  if (delay <= 0) {
    return Promise.resolve(!signal?.aborted);
  }
  if (signal?.aborted) {
    return Promise.resolve(false);
  }
  return new Promise((resolve) => {
    const timer = setTimeout(() => finish(true), delay);
    const onAbort = (): void => finish(false);
    const finish = (completed: boolean): void => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      resolve(completed);
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

export function version(): string {
  return "0.2.0";
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
