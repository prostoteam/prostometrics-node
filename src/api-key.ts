// Client keys carry this marker; server keys never do. Both are minted from the
// same settings screen and look alike at a glance, so pasting the wrong one is
// an ordinary mistake rather than an exotic one.
export const CLIENT_KEY_MARKER = "_pk_";

/**
 * Reports whether the configured key is a client key, which this client can
 * never authenticate with. Only the marker is inspected: the key is otherwise
 * opaque, and guessing harder would risk rejecting a valid key the service
 * starts issuing later.
 */
export function apiKeyLooksLikeClientKey(apiKey: string): boolean {
  return apiKey.trim().includes(CLIENT_KEY_MARKER);
}

/**
 * Explains a refusal the key's own shape already accounts for. Empty when the
 * key looks like the server key it should be, because then the refusal says
 * nothing more specific than that the key is wrong.
 */
export function apiKeyRefusalHint(apiKey: string): string {
  if (apiKeyLooksLikeClientKey(apiKey)) {
    return ` \u2014 this is a client key (it contains "${CLIENT_KEY_MARKER}"); server-side clients need a server key`;
  }
  return "";
}
