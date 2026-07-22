const MAX_UINT64 = (1n << 64n) - 1n;

export function canonicalUniqueID(id: unknown): string | undefined {
  if (typeof id === "bigint") {
    return id < 0n || id > MAX_UINT64 ? undefined : id.toString(10);
  }
  if (typeof id === "number") {
    if (!Number.isSafeInteger(id) || id < 0) {
      return undefined;
    }
    return String(id);
  }
  if (typeof id === "string") {
    return canonicalUniqueDecimalString(id);
  }
  if (Buffer.isBuffer(id) || id instanceof Uint8Array) {
    return canonicalUniqueDecimalString(Buffer.from(id).toString("utf8"));
  }
  return undefined;
}

function canonicalUniqueDecimalString(raw: string): string | undefined {
  if (raw === "" || !/^[0-9]+$/.test(raw)) {
    return undefined;
  }
  try {
    const parsed = BigInt(raw);
    if (parsed > MAX_UINT64) {
      return undefined;
    }
    return parsed.toString(10);
  } catch {
    return undefined;
  }
}
