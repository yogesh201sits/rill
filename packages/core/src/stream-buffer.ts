export interface StreamBufferOptions {
  readonly capacity?: number;
}

interface WaitingConsumer<T> {
  readonly resolve: (
    result: IteratorResult<T>,
  ) => void;
  readonly reject: (error: unknown) => void;
  readonly signal?: AbortSignal;
  readonly onAbort?: () => void;
}

interface WaitingProducer<T> {
  readonly item: T;
  readonly resolve: () => void;
  readonly reject: (error: unknown) => void;
  readonly signal?: AbortSignal;
  readonly onAbort?: () => void;
}

export class StreamBuffer<T> {
  private readonly capacity: number;

  private readonly items: T[] = [];

  private readonly waitingConsumers: WaitingConsumer<T>[] =
    [];

  private readonly waitingProducers: WaitingProducer<T>[] =
    [];

  private completed = false;
  private failure: unknown = undefined;

  constructor(options: StreamBufferOptions = {}) {
    this.capacity = options.capacity ?? 100;

    if (this.capacity <= 0) {
      throw new Error(
        "Buffer capacity must be greater than 0",
      );
    }
  }

  get size(): number {
    return this.items.length;
  }

  get isCompleted(): boolean {
    return this.completed;
  }

  async push(
    item: T,
    signal?: AbortSignal,
  ): Promise<void> {
    this.ensureWritable();
    this.throwIfAborted(signal);

    const consumer = this.waitingConsumers.shift();

    if (consumer) {
      this.removeAbortListener(consumer);

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

    await this.waitForProducer(item, signal);
  }

  async next(
    signal?: AbortSignal,
  ): Promise<IteratorResult<T>> {
    this.throwIfAborted(signal);

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

    return this.waitForConsumer(signal);
  }

  complete(): void {
    if (
      this.completed ||
      this.failure !== undefined
    ) {
      return;
    }

    this.completed = true;

    this.flushConsumers();

    this.rejectWaitingProducers(
      new Error("Stream buffer is completed."),
    );
  }

  fail(error: unknown): void {
    if (
      this.completed ||
      this.failure !== undefined
    ) {
      return;
    }

    this.failure = error;

    this.rejectWaitingProducers(error);

    for (const consumer of this.waitingConsumers.splice(0)) {
      this.removeAbortListener(consumer);
      consumer.reject(error);
    }
  }

  clear(): void {
    this.items.length = 0;

    this.releaseWaitingProducer();
  }

  private waitForProducer(
    item: T,
    signal?: AbortSignal,
  ): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      let producer:
        | WaitingProducer<T>
        | undefined;

      const onAbort = () => {
        if (!producer) {
          return;
        }

        const index =
          this.waitingProducers.indexOf(producer);

        if (index !== -1) {
          this.waitingProducers.splice(index, 1);
        }

        reject(this.getAbortReason(signal!));
      };

      producer = {
        item,
        resolve,
        reject,
        ...(signal
          ? {
              signal,
              onAbort,
            }
          : {}),
      };

      if (signal) {
        signal.addEventListener("abort", onAbort, {
          once: true,
        });

        if (signal.aborted) {
          onAbort();
          return;
        }
      }

      this.waitingProducers.push(producer);
    });
  }

  private waitForConsumer(
    signal?: AbortSignal,
  ): Promise<IteratorResult<T>> {
    return new Promise<IteratorResult<T>>(
      (resolve, reject) => {
        let consumer:
          | WaitingConsumer<T>
          | undefined;

        const onAbort = () => {
          if (!consumer) {
            return;
          }

          const index =
            this.waitingConsumers.indexOf(consumer);

          if (index !== -1) {
            this.waitingConsumers.splice(index, 1);
          }

          reject(this.getAbortReason(signal!));
        };

        consumer = {
          resolve,
          reject,
          ...(signal
            ? {
                signal,
                onAbort,
              }
            : {}),
        };

        if (signal) {
          signal.addEventListener("abort", onAbort, {
            once: true,
          });

          if (signal.aborted) {
            onAbort();
            return;
          }
        }

        this.waitingConsumers.push(consumer);
      },
    );
  }

  private releaseWaitingProducer(): void {
    const producer =
      this.waitingProducers.shift();

    if (!producer) {
      return;
    }

    this.removeAbortListener(producer);

    if (this.failure !== undefined) {
      producer.reject(this.failure);
      return;
    }

    if (this.completed) {
      producer.reject(
        new Error(
          "Stream buffer is completed.",
        ),
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
      this.removeAbortListener(consumer);

      consumer.resolve({
        done: true,
        value: undefined,
      });
    }
  }

  private rejectWaitingProducers(
    error: unknown,
  ): void {
    for (const producer of this.waitingProducers.splice(0)) {
      this.removeAbortListener(producer);
      producer.reject(error);
    }
  }

  private removeAbortListener(
    entry:
      | WaitingConsumer<T>
      | WaitingProducer<T>,
  ): void {
    if (entry.signal && entry.onAbort) {
      entry.signal.removeEventListener(
        "abort",
        entry.onAbort,
      );
    }
  }

  private ensureWritable(): void {
    if (this.failure !== undefined) {
      throw this.failure;
    }

    if (this.completed) {
      throw new Error(
        "Cannot push to a completed buffer.",
      );
    }
  }

  private throwIfAborted(
    signal?: AbortSignal,
  ): void {
    if (signal?.aborted) {
      throw this.getAbortReason(signal);
    }
  }

  private getAbortReason(
    signal: AbortSignal,
  ): unknown {
    return (
      signal.reason ??
      new DOMException(
        "The operation was aborted.",
        "AbortError",
      )
    );
  }
}