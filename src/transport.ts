import {
  ACCEPTED_HEADER_NAME,
  BATCH_ID_HEADER_NAME,
  DROPPED_HEADER_NAME,
  DEFAULT_STOP_RESPONSE_CODES,
  DEFAULT_STOP_STATUS_CODE,
  DEFAULT_FLUSH_TIMEOUT_MS,
  REJECTED_HEADER_NAME,
  WORKLOAD_HEADER_NAME,
} from "./constants.js";
import { HTTPTransportError, StopIngestError } from "./errors.js";
import type { Payload } from "./payload.js";
import { payloadIsEmpty } from "./payload.js";
import { analyzeLinePayloadV5, encodeLinePayloadV5, newDictionaryState, shouldResetDictionary } from "./dictionary.js";
import type { DictionaryState } from "./dictionary.js";
import { validateWorkload } from "./workload.js";

export interface Logger {
  Printf?: (format: string, ...args: unknown[]) => void;
  printf?: (format: string, ...args: unknown[]) => void;
}

export interface TransportSendOptions {
  signal?: AbortSignal;
  workload?: string;
}

export interface Transport {
  send(payload: Payload, options?: TransportSendOptions): Promise<void>;
}

export interface HTTPTransportOptions {
  endpoint: string;
  apiKey?: string;
  workload?: string;
  headers?: Record<string, string | readonly string[]>;
  logger?: Logger;
  verbose?: boolean;
  fetch?: typeof globalThis.fetch;
  stopStatusCodes?: readonly number[];
  stopResponseCodes?: readonly string[];
}

interface SendResult {
  statusCode: number;
  responseCode: string;
  accepted: number;
  dropped: number;
  rejected: number;
}

const MAX_ERROR_BODY_BYTES = 4096;
const DICTIONARY_RESYNC_WARNING_THRESHOLD = 3;
const DICTIONARY_RESYNC_WARNING_WINDOW_MS = 5 * 60 * 1000;

export class HTTPTransport implements Transport {
  endpoint: string;
  apiKey: string;
  workload: string;
  headers: Record<string, string | readonly string[]>;
  logger?: Logger;
  verbose: boolean;
  stopStatusCodes: readonly number[];
  stopResponseCodes: readonly string[];

  private readonly fetchImpl: typeof globalThis.fetch;
  private dict?: DictionaryState;
  private dictionaryResyncWindowStart = 0;
  private dictionaryResyncCount = 0;

  constructor(options: HTTPTransportOptions) {
    this.endpoint = options.endpoint;
    this.apiKey = options.apiKey ?? "";
    this.workload = options.workload ?? "";
    this.headers = options.headers ?? {};
    this.logger = options.logger;
    this.verbose = options.verbose ?? false;
    this.fetchImpl = options.fetch ?? globalThis.fetch;
    this.stopStatusCodes = options.stopStatusCodes ?? [DEFAULT_STOP_STATUS_CODE];
    this.stopResponseCodes = options.stopResponseCodes ?? DEFAULT_STOP_RESPONSE_CODES;
  }

  async send(payload: Payload, options: TransportSendOptions = {}): Promise<void> {
    if (this.endpoint === "") {
      throw new Error("prostometrics: HTTP endpoint is empty");
    }
    const workload = options.workload ?? this.workload;
    validateWorkload(workload);
    if (payloadIsEmpty(payload)) {
      return;
    }
    if (typeof this.fetchImpl !== "function") {
      throw new Error("prostometrics: fetch is not available");
    }

    if (shouldResetDictionary(this.dict)) {
      this.dict = newDictionaryState();
    }

    const body = encodeLinePayloadV5(payload, this.dict!, false);
    if (body.length === 0) {
      return;
    }

    try {
      await this.sendBody(body, payload.batchID ?? "", workload, options.signal);
      return;
    } catch (err) {
      if (err instanceof HTTPTransportError && err.statusCode === 413) {
        this.dict = newDictionaryState();
        throw err;
      }
      if (err instanceof HTTPTransportError && err.statusCode === 409 && err.responseCode === "unknown_series_dictionary") {
        this.dict = newDictionaryState();
        const resyncBody = encodeLinePayloadV5(payload, this.dict, false);
        try {
          await this.sendBody(resyncBody, payload.batchID ?? "", workload, options.signal);
          this.logDictionaryResync(err);
          return;
        } catch (retryErr) {
          if (retryErr instanceof HTTPTransportError && retryErr.statusCode === 413) {
            this.dict = newDictionaryState();
          }
          throw retryErr;
        }
      }
      throw err;
    }
  }

  // Routine resync is verbose-only; repeated resyncs warn about likely cache churn.
  private logDictionaryResync(cause: HTTPTransportError): void {
    const now = Date.now();
    if (this.dictionaryResyncWindowStart === 0 || now - this.dictionaryResyncWindowStart > DICTIONARY_RESYNC_WARNING_WINDOW_MS) {
      this.dictionaryResyncWindowStart = now;
      this.dictionaryResyncCount = 0;
    }
    this.dictionaryResyncCount += 1;
    if (this.verbose) {
      log(this.logger, "prostometrics: ingester dictionary miss recovered by resync; %s", String(cause));
      return;
    }
    if (this.dictionaryResyncCount === DICTIONARY_RESYNC_WARNING_THRESHOLD) {
      log(
        this.logger,
        "prostometrics: repeated ingester dictionary resyncs count=%s windowMs=%s; check ingester cache churn or load balancing",
        this.dictionaryResyncCount,
        DICTIONARY_RESYNC_WARNING_WINDOW_MS,
      );
    }
  }

  private async sendBody(body: Buffer, batchID: string, workload: string, parentSignal: AbortSignal | undefined): Promise<SendResult | undefined> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), DEFAULT_FLUSH_TIMEOUT_MS);
    timeout.unref?.();
    const abort = () => controller.abort(parentSignal?.reason);
    if (parentSignal) {
      if (parentSignal.aborted) {
        controller.abort(parentSignal.reason);
      } else {
        parentSignal.addEventListener("abort", abort, { once: true });
      }
    }

    try {
      const headers = new Headers();
      headers.set("Content-Type", "text/plain; charset=utf-8");
      for (const [key, raw] of Object.entries(this.headers)) {
        const values = Array.isArray(raw) ? raw : [raw];
        for (const value of values) {
          headers.append(key, String(value));
        }
      }
      if (this.apiKey !== "") {
        headers.set("Authorization", this.apiKey);
      }
      if (workload.trim() !== "") {
        headers.set(WORKLOAD_HEADER_NAME, workload.trim());
      }
      if (batchID.trim() !== "") {
        headers.set(BATCH_ID_HEADER_NAME, batchID.trim());
      }

      const response = await this.fetchImpl(this.endpoint, {
        method: "POST",
        headers,
        body,
        signal: controller.signal,
      });

      const result: SendResult = {
        statusCode: response.status,
        responseCode: "",
        accepted: parseIngestCountHeader(response.headers.get(ACCEPTED_HEADER_NAME)),
        dropped: parseIngestCountHeader(response.headers.get(DROPPED_HEADER_NAME)),
        rejected: parseIngestCountHeader(response.headers.get(REJECTED_HEADER_NAME)),
      };

      if (response.status >= 200 && response.status < 300) {
        if (result.dropped > 0 || result.rejected > 0) {
          log(
            this.logger,
            "prostometrics: ingest accepted partial batch batchId=%s accepted=%s dropped=%s rejected=%s endpoint=%s",
            batchID,
            Math.max(result.accepted, 0),
            Math.max(result.dropped, 0),
            Math.max(result.rejected, 0),
            this.endpoint,
          );
        }
        return undefined;
      }

      const detail = compactErrorBody(await readErrorText(response));
      result.responseCode = extractResponseCode(detail);
      const diagnostics = analyzeLinePayloadV5(body, this.dict);
      const transportError = new HTTPTransportError({
        endpoint: this.endpoint,
        batchID,
        statusCode: response.status,
        status: `${response.status} ${response.statusText}`.trim(),
        responseCode: result.responseCode,
        detail,
        accepted: result.accepted,
        dropped: result.dropped,
        rejected: result.rejected,
        retryAfterMs: parseRetryAfterMs(response.headers.get("Retry-After")),
        requestBytes: body.length,
        dictionarySession: diagnostics.dictionarySession,
        dictionaryRevision: diagnostics.dictionaryRevision,
        dictionarySeries: diagnostics.dictionarySeries,
        seriesDefinitions: diagnostics.seriesDefinitions,
        eventLines: diagnostics.eventLines,
      });

      if (this.shouldStopOnStatus(response.status) || this.shouldStopOnResponseCode(result.responseCode)) {
        throw new StopIngestError(`prostometrics: stop ingesting after HTTP ${response.status}`, {
          code: response.status,
          cause: transportError,
        });
      }
      throw transportError;
    } finally {
      clearTimeout(timeout);
      if (parentSignal) {
        parentSignal.removeEventListener("abort", abort);
      }
    }
  }

  private shouldStopOnStatus(status: number): boolean {
    return this.stopStatusCodes.includes(status);
  }

  private shouldStopOnResponseCode(code: string): boolean {
    const normalized = code.trim().toLowerCase();
    if (normalized === "") {
      return false;
    }
    return this.stopResponseCodes.some((candidate) => candidate.trim().toLowerCase() === normalized);
  }
}

function parseRetryAfterMs(raw: string | null, nowMs = Date.now()): number {
  if (raw === null || raw.trim() === "") {
    return 0;
  }
  const trimmed = raw.trim();
  if (/^\d+$/.test(trimmed)) {
    return Number.parseInt(trimmed, 10) * 1000;
  }
  const when = Date.parse(trimmed);
  return Number.isFinite(when) && when > nowMs ? when - nowMs : 0;
}

function parseIngestCountHeader(raw: string | null): number {
  if (raw === null || raw.trim() === "") {
    return -1;
  }
  const value = Number.parseInt(raw.trim(), 10);
  return Number.isNaN(value) ? -1 : value;
}

async function readErrorText(response: Response): Promise<string> {
  if (!response.body) {
    return "";
  }
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (total < MAX_ERROR_BODY_BYTES) {
      const { value, done } = await reader.read();
      if (done || !value) {
        break;
      }
      const remaining = MAX_ERROR_BODY_BYTES - total;
      const chunk = value.byteLength > remaining ? value.slice(0, remaining) : value;
      chunks.push(chunk);
      total += chunk.byteLength;
      if (value.byteLength > remaining) {
        break;
      }
    }
  } catch {
    return "";
  } finally {
    reader.cancel().catch(() => undefined);
  }
  return Buffer.concat(chunks, total).toString("utf8");
}

function compactErrorBody(raw: string): string {
  return raw.trim().split(/\s+/).filter(Boolean).join(" ");
}

function extractResponseCode(raw: string): string {
  if (raw === "") {
    return "";
  }
  try {
    const parsed = JSON.parse(raw) as { code?: unknown };
    return typeof parsed.code === "string" ? parsed.code.trim().toLowerCase() : "";
  } catch {
    return "";
  }
}

export function log(logger: Logger | undefined, format: string, ...args: unknown[]): void {
  if (!logger) {
    return;
  }
  if (typeof logger.printf === "function") {
    logger.printf(format, ...args);
    return;
  }
  if (typeof logger.Printf === "function") {
    logger.Printf(format, ...args);
  }
}
