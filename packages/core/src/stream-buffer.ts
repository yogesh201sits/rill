export interface StreamBufferOptions {
  readonly capacity?: number;
}

export class StreamBuffer<T> {
  private readonly capacity: number;

  private readonly items: T[] = [];

  private readonly waitingConsumers: Array<{
    resolve: (result: IteratorResult<T>) => void;
    reject: (error: unknown) => void;
  }> = [];

  private readonly waitingProducers: Array<{
    item: T;
    resolve: () => void;
    reject: (error: unknown) => void;
  }> = [];

  private completed = false;
  private failure: unknown = undefined;

  constructor(options: StreamBufferOptions = {}) {
    this.capacity = options.capacity ?? 100;

    if (this.capacity <= 0) {
      throw new Error("Buffer capacity must be greater than 0");
    }
  }

  get size(): number {
    return this.items.length;
  }

  get isCompleted(): boolean {
    return this.completed;
  }

  async push(item: T): Promise<void> {
    this.ensureWritable();

    const consumer = this.waitingConsumers.shift();

    if (consumer) {
      consumer.resolve({
        done: false,
        value: item,
      });

      return;
    }

    if (this.items.length < this.capacity) {
      this.items.push(item);
      return;
    }

    await new Promise<void>((resolve, reject) => {
      this.waitingProducers.push({
        item,
        resolve,
        reject,
      });
    });
  }

  async next(): Promise<IteratorResult<T>> {
    if (this.items.length > 0) {
      const item = this.items.shift()!;

      this.releaseWaitingProducer();

      return {
        done: false,
        value: item,
      };
    }

    if (this.failure !== undefined) {
      throw this.failure;
    }

    if (this.completed) {
      return {
        done: true,
        value: undefined,
      };
    }

    return new Promise<IteratorResult<T>>(
      (resolve, reject) => {
        this.waitingConsumers.push({
          resolve,
          reject,
        });
      },
    );
  }

  complete(): void {
    if (this.completed || this.failure !== undefined) {
      return;
    }

    this.completed = true;

    this.flushConsumers();
    this.rejectWaitingProducers(
      new Error("Stream buffer is completed."),
    );
  }

  fail(error: unknown): void {
    if (this.completed || this.failure !== undefined) {
      return;
    }

    this.failure = error;

    this.rejectWaitingProducers(error);

    for (const consumer of this.waitingConsumers.splice(0)) {
      consumer.reject(error);
    }
  }

  clear(): void {
    this.items.length = 0;
  }

  private releaseWaitingProducer(): void {
    const producer = this.waitingProducers.shift();

    if (!producer) {
      return;
    }

    if (this.failure !== undefined) {
      producer.reject(this.failure);
      return;
    }

    if (this.completed) {
      producer.reject(
        new Error("Stream buffer is completed."),
      );
      return;
    }

    this.items.push(producer.item);
    producer.resolve();
  }

  private flushConsumers(): void {
    if (this.items.length > 0) {
      return;
    }

    for (const consumer of this.waitingConsumers.splice(0)) {
      consumer.resolve({
        done: true,
        value: undefined,
      });
    }
  }

  private rejectWaitingProducers(error: unknown): void {
    for (const producer of this.waitingProducers.splice(0)) {
      producer.reject(error);
    }
  }

  private ensureWritable(): void {
    if (this.failure !== undefined) {
      throw this.failure;
    }

    if (this.completed) {
      throw new Error("Cannot push to a completed buffer.");
    }
  }
}