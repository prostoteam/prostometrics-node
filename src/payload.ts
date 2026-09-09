export type MetricType = "counter" | "value" | "value_sparse" | "success" | "total" | "unique" | "top";

export interface Event {
  readonly type: MetricType;
  readonly metric: string;
  value: number;
  readonly uniqueID?: string;
  readonly item?: string;
  readonly labels: string[];
  readonly timestamp: number;
}

export interface Payload {
  batchID?: string;
  counters: CounterEvent[];
  values: ValueEvent[];
  uniques: UniqueEvent[];
  // Optional, like `success` on a value event, so a payload built by hand for
  // a custom transport keeps type-checking; absent means no top-list events.
  tops?: TopEvent[];
}

export interface CounterEvent {
  metric: string;
  value: number;
  labels: string[];
  timestamp: number;
}

export interface ValueEvent {
  metric: string;
  value: number;
  sparse: boolean;
  // A success outcome: the sample is 100 or 0 and the server presents the
  // metric as a success rate. Optional so payloads built by hand (custom
  // transports and their tests) keep type-checking; absent means false.
  success?: boolean;
  labels: string[];
  timestamp: number;
}

export interface UniqueEvent {
  metric: string;
  uniqueID: string;
  labels: string[];
  timestamp: number;
}

/**
 * One person (uniqueID) touched one item, for a top list ranking items by
 * distinct people. The item travels verbatim. Labels are always empty in this
 * protocol version and are kept only so the event mirrors UniqueEvent.
 */
export interface TopEvent {
  metric: string;
  uniqueID: string;
  item: string;
  labels: string[];
  timestamp: number;
}

export function emptyPayload(): Payload {
  return { counters: [], values: [], uniques: [], tops: [] };
}

export function payloadIsEmpty(payload: Payload | undefined): boolean {
  return !payload || (payload.counters.length === 0 && payload.values.length === 0 && payload.uniques.length === 0 && (payload.tops?.length ?? 0) === 0);
}
