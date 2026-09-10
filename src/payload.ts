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

// About what this event costs as a line: the type letter, the series id, the
// payload, the timestamp and the separators between them. It is an estimate,
// used only to close a batch before it grows past what the ingest endpoint
// accepts, so it rounds up rather than down.
export const eventWireSize = (event: Event): number => {
  // t|<series>|<payload>|<seconds>\n, with room for a six-digit series id and
  // an eleven-digit timestamp.
  const framing = 1 + 1 + 6 + 1 + 1 + 11 + 1;
  switch (event.type) {
    case "unique":
      // Unique ids are decimal digits, so characters and bytes agree.
      return framing + (event.uniqueID?.length ?? 0);
    case "top":
      // The item is whatever a visitor touched, and an article name in a
      // non-Latin script is two or three bytes a character. Counting characters
      // would let a batch close at a third of the size the endpoint measures.
      return framing + (event.uniqueID?.length ?? 0) + 1 + (event.item === undefined ? 0 : Buffer.byteLength(event.item, "utf8"));
    default:
      // The widest a number is written as. This client encodes with String(),
      // which picks the shorter of the fixed and exponent forms, so even the
      // extremes are short -- unlike the Go client, whose encoder never uses an
      // exponent and writes 1e-300 as three hundred characters.
      return framing + 26;
  }
};

// What a series costs the first time a batch mentions it: the S| line naming
// the metric and every label value. A batch defines every series the server's
// dictionary has not seen, which on a first flush is all of them, and those
// lines can outweigh the events that need them -- a metric name plus eight
// labels is kilobytes where an event line is tens of bytes. Counting only
// events is how a batch reaches several times what the endpoint accepts while
// believing it is in range, and this client has no splitter to catch it.
export const definitionWireSize = (metric: string, labels: readonly string[]): number => {
  // S|<series>|<metric>\n, with room for a six-digit series id.
  let size = 1 + 1 + 6 + 1 + Buffer.byteLength(metric, "utf8") + 1;
  for (const label of labels) {
    if (label !== "") {
      size += 1 + Buffer.byteLength(label, "utf8");
    }
  }
  return size;
};

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
