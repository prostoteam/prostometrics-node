export const MAX_VALUE_EVENTS_PER_SECOND = 20_000;

// ValueRateLimiter keeps raw value samples from overwhelming the client pipeline.
export class ValueRateLimiter {
  private windowSecond = -1;
  private accepted = 0;

  allow(nowMs = Date.now()): boolean {
    const second = Math.floor(nowMs / 1000);
    if (second < this.windowSecond) {
      return false;
    }
    if (second > this.windowSecond) {
      this.windowSecond = second;
      this.accepted = 0;
    }
    if (this.accepted >= MAX_VALUE_EVENTS_PER_SECOND) {
      return false;
    }
    this.accepted += 1;
    return true;
  }
}
