import { DEFAULT_MAX_SERIES_PER_BATCH } from "./constants.js";
import { cloneLabels } from "./labels.js";
import type { Event, Payload } from "./payload.js";
import { emptyPayload, payloadIsEmpty } from "./payload.js";
import { seriesKey } from "./series.js";

export class BatchBuilder {
  private readonly payload: Payload = emptyPayload();
  private readonly counterAggs = new Map<string, number>();
  private readonly uniqueSeen = new Set<string>();

  add(event: Event): void {
    switch (event.type) {
      case "counter":
        this.addCounter(event);
        return;
      case "value":
      case "value_sparse":
        this.payload.values.push({
          metric: event.metric,
          value: event.value,
          sparse: event.type === "value_sparse",
          labels: cloneLabels(event.labels),
          timestamp: event.timestamp,
        });
        return;
      case "unique":
        this.addUnique(event);
        return;
      case "total":
        return;
    }
  }

  build(): Payload | undefined {
    return payloadIsEmpty(this.payload) ? undefined : this.payload;
  }

  private addCounter(event: Event): void {
    const key = seriesKey(event.metric, event.labels);
    const existing = this.counterAggs.get(key);
    if (existing !== undefined) {
      const counter = this.payload.counters[existing]!;
      counter.value += event.value;
      if (event.timestamp > counter.timestamp) {
        counter.timestamp = event.timestamp;
      }
      return;
    }

    const index = this.payload.counters.length;
    this.payload.counters.push({
      metric: event.metric,
      value: event.value,
      labels: cloneLabels(event.labels),
      timestamp: event.timestamp,
    });
    if (this.counterAggs.size < DEFAULT_MAX_SERIES_PER_BATCH) {
      this.counterAggs.set(key, index);
    }
  }

  private addUnique(event: Event): void {
    if (!event.uniqueID) {
      return;
    }
    const key = `${seriesKey(event.metric, event.labels)}\x01${event.uniqueID}`;
    if (this.uniqueSeen.has(key)) {
      return;
    }
    this.uniqueSeen.add(key);
    this.payload.uniques.push({
      metric: event.metric,
      uniqueID: event.uniqueID,
      labels: cloneLabels(event.labels),
      timestamp: event.timestamp,
    });
  }
}
