export class RingBuffer<T> {
  private readonly items: Array<T | undefined>;
  private head = 0;
  private tail = 0;
  private count = 0;

  constructor(readonly capacity: number) {
    this.items = new Array<T | undefined>(capacity);
  }

  get length(): number {
    return this.count;
  }

  push(item: T): boolean {
    if (this.count >= this.capacity) {
      return false;
    }
    this.items[this.tail] = item;
    this.tail = (this.tail + 1) % this.capacity;
    this.count += 1;
    return true;
  }

  shift(): T | undefined {
    if (this.count === 0) {
      return undefined;
    }
    const item = this.items[this.head];
    this.items[this.head] = undefined;
    this.head = (this.head + 1) % this.capacity;
    this.count -= 1;
    return item;
  }
}
