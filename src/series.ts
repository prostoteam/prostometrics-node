export interface SeriesDefinition {
  metric: string;
  labels: string[];
}

export function seriesKey(metric: string, labels: readonly string[]): string {
  let out = `${metric}\0`;
  for (const label of labels) {
    out += `${label}\0`;
  }
  return out;
}
