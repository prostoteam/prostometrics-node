import { randomBytes } from "node:crypto";
import { DEFAULT_MAX_DICTIONARY_SERIES } from "./constants.js";
import type { Payload } from "./payload.js";
import { payloadIsEmpty } from "./payload.js";
import type { SeriesDefinition } from "./series.js";
import { seriesKey } from "./series.js";

export interface DictionaryState {
  sessionID: string;
  revision: number;
  seriesMap: Map<string, number>;
  series: SeriesDefinition[];
}

export function newDictionaryState(): DictionaryState {
  return {
    sessionID: randomBytes(16).toString("hex"),
    revision: 0,
    seriesMap: new Map(),
    series: [],
  };
}

export function shouldResetDictionary(state: DictionaryState | undefined): boolean {
  return state === undefined || state.series.length >= DEFAULT_MAX_DICTIONARY_SERIES;
}

export function encodeLinePayloadV5(payload: Payload | undefined, state: DictionaryState, forceDefinitions = false): Buffer {
  if (payloadIsEmpty(payload)) {
    return Buffer.alloc(0);
  }

  type EncodedEvent = {
    metricType: "c" | "v" | "s" | "u";
    seriesID: number;
    value: number | string;
    timestamp: number;
  };

  const events: EncodedEvent[] = [];
  const newSeriesIDs: number[] = [];
  let seriesChanged = false;

  const getSeriesID = (metric: string, labels: readonly string[]): number => {
    const key = seriesKey(metric, labels);
    const existing = state.seriesMap.get(key);
    if (existing !== undefined) {
      return existing;
    }
    const id = state.series.length;
    state.seriesMap.set(key, id);
    state.series.push({ metric, labels: labels.slice() });
    newSeriesIDs.push(id);
    seriesChanged = true;
    return id;
  };

  for (const counter of payload!.counters) {
    events.push({
      metricType: "c",
      seriesID: getSeriesID(counter.metric, counter.labels),
      value: roundAwayFromZero(counter.value),
      timestamp: counter.timestamp,
    });
  }
  for (const value of payload!.values) {
    events.push({
      metricType: value.sparse ? "s" : "v",
      seriesID: getSeriesID(value.metric, value.labels),
      value: value.value,
      timestamp: value.timestamp,
    });
  }
  for (const unique of payload!.uniques) {
    events.push({
      metricType: "u",
      seriesID: getSeriesID(unique.metric, unique.labels),
      value: unique.uniqueID,
      timestamp: unique.timestamp,
    });
  }

  if (seriesChanged) {
    state.revision += 1;
  }

  const lines: string[] = [`H|5|s|${state.sessionID}|${state.revision}`];
  if (forceDefinitions) {
    for (let id = 0; id < state.series.length; id += 1) {
      lines.push(seriesDefinitionLine(id, state.series[id]!));
    }
  } else if (seriesChanged) {
    for (const id of newSeriesIDs) {
      lines.push(seriesDefinitionLine(id, state.series[id]!));
    }
  }

  for (const event of events) {
    lines.push(`${event.metricType}|${event.seriesID}|${formatEventValue(event.value)}|${event.timestamp}`);
  }

  return Buffer.from(`${lines.join("\n")}\n`, "utf8");
}

function seriesDefinitionLine(id: number, series: SeriesDefinition): string {
  let line = `S|${id}|${series.metric}`;
  for (const label of series.labels) {
    if (label !== "") {
      line += `|${label}`;
    }
  }
  return line;
}

function formatEventValue(value: number | string): string {
  if (typeof value === "string") {
    return value;
  }
  return Number.isFinite(value) ? String(value) : String(value);
}

function roundAwayFromZero(value: number): number {
  if (!Number.isFinite(value) || value === 0) {
    return value;
  }
  return value > 0 ? Math.floor(value + 0.5) : Math.ceil(value - 0.5);
}

export interface LinePayloadDiagnostics {
  dictionarySession: string;
  dictionaryRevision: number;
  dictionarySeries: number;
  seriesDefinitions: number;
  eventLines: number;
}

export function analyzeLinePayloadV5(body: Buffer, state: DictionaryState | undefined): LinePayloadDiagnostics {
  const diagnostics: LinePayloadDiagnostics = {
    dictionarySession: "",
    dictionaryRevision: 0,
    dictionarySeries: state?.series.length ?? 0,
    seriesDefinitions: 0,
    eventLines: 0,
  };

  for (const line of body.toString("utf8").split("\n")) {
    if (line === "") {
      continue;
    }
    if (line.startsWith("H|")) {
      const parts = line.split("|");
      diagnostics.dictionarySession = parts[3] ?? "";
      diagnostics.dictionaryRevision = Number.parseInt(parts[4] ?? "0", 10) || 0;
      continue;
    }
    if (line.startsWith("S|")) {
      diagnostics.seriesDefinitions += 1;
      continue;
    }
    diagnostics.eventLines += 1;
  }

  return diagnostics;
}
