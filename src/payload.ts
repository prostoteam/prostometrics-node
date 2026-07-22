export type MetricType = "counter" | "value" | "value_sparse" | "total" | "unique";

export interface Event {
  readonly type: MetricType;
  readonly metric: string;
  value: number;
  readonly uniqueID?: string;
  readonly labels: string[];
  readonly timestamp: number;
}

export interface Payload {
  batchID?: string;
  counters: CounterEvent[];
  values: ValueEvent[];
  uniques: UniqueEvent[];
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
  labels: string[];
  timestamp: number;
}

export interface UniqueEvent {
  metric: string;
  uniqueID: string;
  labels: string[];
  timestamp: number;
}

export function emptyPayload(): Payload {
  return { counters: [], values: [], uniques: [] };
}

export function payloadIsEmpty(payload: Payload | undefined): boolean {
  return !payload || (payload.counters.length === 0 && payload.values.length === 0 && payload.uniques.length === 0);
}
