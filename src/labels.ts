const labelPattern = /[=|\n\r]/g;

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

export function normalizeLabels(labels: readonly string[]): string[] {
  const out: string[] = [];
  for (const raw of labels) {
    const trimmed = String(raw).trim();
    if (trimmed !== "") {
      out.push(trimmed);
    }
  }
  return out;
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
