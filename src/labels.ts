import {
  MAX_LABEL_BYTES,
  MAX_LABELS_PER_SERIES,
  MAX_METRIC_BYTES,
  RESERVED_LABEL_NAME,
} from "./constants.js";

const labelPattern = /[=|\n\r]/g;

// The server rejects any name carrying a byte below 0x20 or 0x7f, so the client
// matches that rule exactly instead of leaving it to be discovered as a failed
// request that cannot be retried.
const controlCharacters = /[\u0000-\u001f\u007f]/;

/** Reports whether a string is safe to place in the line protocol. */
export function isWireSafe(text: string): boolean {
  return !text.includes("|") && !controlCharacters.test(text);
}

/** Validates a metric name, returning the trimmed name or undefined. */
export function normalizeMetric(metric: unknown): string | undefined {
  if (typeof metric !== "string") {
    return undefined;
  }
  const trimmed = metric.trim();
  if (trimmed === "" || Buffer.byteLength(trimmed, "utf8") > MAX_METRIC_BYTES || !isWireSafe(trimmed)) {
    return undefined;
  }
  return trimmed;
}

/**
 * Validates a numeric sample. Cumulative totals are exempt from the ceiling
 * because only the delta they produce travels on the wire; applyTotal checks
 * that instead.
 */
export function isValidSample(value: unknown, limit: number): boolean {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= limit;
}

export function label(key: string, value: string): string {
  return `${key.replace(labelPattern, "_")}=${value.replace(labelPattern, "_")}`;
}

export const Label = label;

export function cloneLabels(labels: readonly string[] | undefined): string[] {
  if (!labels || labels.length === 0) {
    return [];
  }
  return labels.slice();
}

/**
 * Validates the labels for one event, or returns undefined to drop it.
 *
 * A label the server refuses or silently ignores makes the stored series wrong,
 * so it is better to report the drop locally than to record something the
 * caller did not mean. Label order is preserved: it is part of how existing
 * series are keyed, so reordering here would split them.
 */
export function normalizeLabels(labels: readonly string[]): string[] | undefined {
  const out: string[] = [];
  const seen = new Set<string>();
  for (const raw of labels) {
    const trimmed = String(raw).trim();
    if (trimmed === "") {
      continue;
    }
    if (Buffer.byteLength(trimmed, "utf8") > MAX_LABEL_BYTES || !isWireSafe(trimmed)) {
      return undefined;
    }
    const separator = trimmed.indexOf("=");
    if (separator < 0) {
      return undefined;
    }
    const name = trimmed.slice(0, separator).trim();
    if (name === "" || name === RESERVED_LABEL_NAME || seen.has(name)) {
      return undefined;
    }
    seen.add(name);
    out.push(trimmed);
  }
  return out.length > MAX_LABELS_PER_SERIES ? undefined : out;
}

export function hasWorkloadLabel(labels: readonly string[]): boolean {
  for (const raw of labels) {
    const trimmed = String(raw).trim();
    if (trimmed === "" || trimmed === undefined) {
      continue;
    }
    if (trimmed === "workload") {
      return true;
    }
    const idx = trimmed.indexOf("=");
    if (idx >= 0 && trimmed.slice(0, idx) === "workload") {
      return true;
    }
  }
  return false;
}
