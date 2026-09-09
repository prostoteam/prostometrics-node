import { MAX_TOP_ITEM_BYTES } from "./constants.js";

// A lone surrogate half has no UTF-8 encoding, so what reached the wire would
// not be the string the caller passed. With the u flag a well-formed pair is
// one code point outside the Surrogate category and does not match.
const loneSurrogate = /\p{Cs}/u;

// The server refuses an item carrying a byte below 0x20 or 0x7f (which also
// rules out line breaks), so the client matches that rule instead of losing the
// whole batch to a request that cannot be retried.
function hasControlCharacter(text: string): boolean {
  for (let i = 0; i < text.length; i += 1) {
    const code = text.charCodeAt(i);
    if (code < 0x20 || code === 0x7f) {
      return true;
    }
  }
  return false;
}

/**
 * Reports whether a top-list item can travel on the wire exactly as sent:
 * non-empty, at most MAX_TOP_ITEM_BYTES of UTF-8, well-formed, and free of
 * control characters. The item is never trimmed or altered, so a rejected
 * value is the caller's to fix rather than something the client rewrites.
 */
export function isValidTopItem(item: unknown): item is string {
  if (typeof item !== "string" || item === "") {
    return false;
  }
  if (Buffer.byteLength(item, "utf8") > MAX_TOP_ITEM_BYTES) {
    return false;
  }
  return !hasControlCharacter(item) && !loneSurrogate.test(item);
}
