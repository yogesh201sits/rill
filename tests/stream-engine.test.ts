import { describe, expect, test } from "bun:test";

import type {
  StreamChunk,
  StreamInput,
  StreamSource,
} from "../packages/core/src";

import {
  STREAM_STATES,
  StreamEngine,
  StreamSession,
} from "../packages/core/src";

class TestSource implements StreamSource {
  async *generate(
    _input: StreamInput,
    signal: AbortSignal,
  ): AsyncIterable<StreamChunk> {
    if (signal.aborted) {
      throw (
        signal.reason ??
        new DOMException("The stream was aborted.", "AbortError")
      );
    }

    yield { text: "Hello" };
    yield { text: " world" };
    yield { text: "!" };
  }
}

class FailingSource implements StreamSource {
  async *generate(): AsyncIterable<StreamChunk> {
    yield { text: "before-error" };

    throw new Error("provider failed");
  }
}

class EmptySource implements StreamSource {
  async *generate(): AsyncIterable<StreamChunk> {
    // Intentionally produces no chunks.
  }
}

describe("StreamEngine", () => {
  test("emits start, delta, and done events", async () => {
    const engine = new StreamEngine(new TestSource(), {
      now: () => 1000,
    });

    const session = new StreamSession("stream-1");

    const events = [];

    for await (const event of engine.stream(session, {
      prompt: "hello",
    })) {
      events.push(event);
    }

    expect(events).toHaveLength(5);

    expect(events[0]).toMatchObject({
      type: "stream.start",
      streamId: "stream-1",
      sequence: 0,
      timestamp: 1000,
    });

    expect(events[1]).toMatchObject({
      type: "stream.delta",
      streamId: "stream-1",
      sequence: 1,
      timestamp: 1000,
      text: "Hello",
    });

    expect(events[2]).toMatchObject({
      type: "stream.delta",
      streamId: "stream-1",
      sequence: 2,
      timestamp: 1000,
      text: " world",
    });

    expect(events[3]).toMatchObject({
      type: "stream.delta",
      streamId: "stream-1",
      sequence: 3,
      timestamp: 1000,
      text: "!",
    });

    expect(events[4]).toMatchObject({
      type: "stream.done",
      streamId: "stream-1",
      sequence: 4,
      timestamp: 1000,
    });

    expect(session.getState()).toBe(STREAM_STATES.COMPLETED);
  });

  test("maintains monotonically increasing sequence numbers", async () => {
    const engine = new StreamEngine(new TestSource());

    const session = new StreamSession("stream-2");

    const sequences: number[] = [];

    for await (const event of engine.stream(session, {
      prompt: "test",
    })) {
      sequences.push(event.sequence);
    }

    expect(sequences).toEqual([0, 1, 2, 3, 4]);
  });

  test("uses the session ID for every event", async () => {
    const engine = new StreamEngine(new TestSource());

    const session = new StreamSession("stream-consistent-id");

    const events = [];

    for await (const event of engine.stream(session, {
      prompt: "test",
    })) {
      events.push(event);
    }

    expect(
      events.every((event) => event.streamId === "stream-consistent-id"),
    ).toBe(true);
  });

  test("passes input to the source", async () => {
    let receivedInput: StreamInput | undefined;

    const source: StreamSource = {
      async *generate(input) {
        receivedInput = input;

        yield {
          text: "response",
        };
      },
    };

    const engine = new StreamEngine(source);

    const session = new StreamSession("stream-3");

    for await (const _event of engine.stream(session, {
      prompt: "test prompt",
      metadata: {
        requestId: "request-123",
      },
    })) {
      // Consume the stream.
    }

    expect(receivedInput).toEqual({
      prompt: "test prompt",
      metadata: {
        requestId: "request-123",
      },
    });
  });

  test("emits done when source produces no chunks", async () => {
    const engine = new StreamEngine(new EmptySource());

    const session = new StreamSession("empty-stream");

    const events = [];

    for await (const event of engine.stream(session, {
      prompt: "empty",
    })) {
      events.push(event);
    }

    expect(events).toHaveLength(2);

    expect(events[0]?.type).toBe("stream.start");

    expect(events[1]?.type).toBe("stream.done");

    expect(session.getState()).toBe(STREAM_STATES.COMPLETED);
  });

  test("emits error when source fails", async () => {
    const engine = new StreamEngine(new FailingSource());

    const session = new StreamSession("failed-stream");

    const events = [];

    for await (const event of engine.stream(session, {
      prompt: "test",
    })) {
      events.push(event);
    }

    expect(events).toHaveLength(3);

    expect(events[0]?.type).toBe("stream.start");

    expect(events[1]).toMatchObject({
      type: "stream.delta",
      text: "before-error",
    });

    expect(events[2]).toMatchObject({
      type: "stream.error",
      streamId: "failed-stream",
      sequence: 2,
      code: "STREAM_EXECUTION_FAILED",
      message: "provider failed",
      retryable: false,
    });

    expect(session.getState()).toBe(STREAM_STATES.FAILED);
  });

  test("does not emit done after source failure", async () => {
    const engine = new StreamEngine(new FailingSource());

    const session = new StreamSession("failed-stream");

    const types: string[] = [];

    for await (const event of engine.stream(session, {
      prompt: "test",
    })) {
      types.push(event.type);
    }

    expect(types).toEqual(["stream.start", "stream.delta", "stream.error"]);

    expect(types).not.toContain("stream.done");
  });

  test("uses the configured clock", async () => {
    let currentTime = 1000;

    const engine = new StreamEngine(new TestSource(), {
      now: () => currentTime,
    });

    const session = new StreamSession("clock-test");

    const timestamps: number[] = [];

    for await (const event of engine.stream(session, {
      prompt: "test",
    })) {
      timestamps.push(event.timestamp);
      currentTime += 100;
    }

    expect(timestamps).toEqual([1000, 1100, 1200, 1300, 1400]);
  });

  test("respects buffer capacity", async () => {
    const source: StreamSource = {
      async *generate() {
        yield { text: "A" };
        yield { text: "B" };
        yield { text: "C" };
      },
    };

    const engine = new StreamEngine(source, {
      bufferCapacity: 1,
    });

    const session = new StreamSession("buffer-test");

    const events = [];

    for await (const event of engine.stream(session, {
      prompt: "test",
    })) {
      events.push(event);
    }

    const deltas = events
      .filter(
        (event): event is Extract<typeof event, { type: "stream.delta" }> =>
          event.type === "stream.delta",
      )
      .map((event) => event.text);

    expect(deltas).toEqual(["A", "B", "C"]);

    expect(events.at(-1)?.type).toBe("stream.done");
  });

  test("rejects invalid buffer capacity", () => {
    expect(
      () =>
        new StreamEngine(new TestSource(), {
          bufferCapacity: 0,
        }),
    ).toThrow("Buffer capacity must be greater than 0");
  });
  test("emits cancelled when the session is cancelled", async () => {
    const source: StreamSource = {
      async *generate(_input, signal) {
        yield { text: "first" };

        await new Promise<void>((resolve) => {
          signal.addEventListener("abort", () => {
            resolve();
          });
        });
      },
    };

    const engine = new StreamEngine(source);

    const session = new StreamSession("cancelled-stream");

    const events = [];

    for await (const event of engine.stream(session, {
      prompt: "test",
    })) {
      events.push(event);

      if (event.type === "stream.delta") {
        session.cancel();
      }
    }

    expect(events.map((event) => event.type)).toEqual([
      "stream.start",
      "stream.delta",
      "stream.cancelled",
    ]);

    expect(session.getState()).toBe(STREAM_STATES.CANCELLED);
  });

  test("does not emit done after cancellation", async () => {
    const source: StreamSource = {
      async *generate(_input, signal) {
        yield { text: "before-cancel" };

        await new Promise<void>((resolve) => {
          signal.addEventListener("abort", () => {
            resolve();
          });
        });
      },
    };

    const engine = new StreamEngine(source);

    const session = new StreamSession("cancel-no-done");

    const types: string[] = [];

    for await (const event of engine.stream(session, {
      prompt: "test",
    })) {
      types.push(event.type);

      if (event.type === "stream.delta") {
        session.cancel();
      }
    }

    expect(types).toEqual(["stream.start", "stream.delta", "stream.cancelled"]);

    expect(types).not.toContain("stream.done");
  });

  test("cancellation while producer is blocked does not hang", async () => {
    const source: StreamSource = {
      async *generate(_input, signal) {
        yield { text: "A" };
        yield { text: "B" };

        await new Promise<void>((resolve) => {
          signal.addEventListener("abort", () => {
            resolve();
          });
        });
      },
    };

    const engine = new StreamEngine(source, {
      bufferCapacity: 1,
    });

    const session = new StreamSession("blocked-producer-cancel");

    const events = [];

    for await (const event of engine.stream(session, {
      prompt: "test",
    })) {
      events.push(event);

      if (event.type === "stream.delta") {
        session.cancel();
        break;
      }
    }

    expect(events[0]?.type).toBe("stream.start");

    expect(events[1]?.type).toBe("stream.delta");

    expect(session.getState()).toBe(STREAM_STATES.CANCELLED);
  });
  test("calls onStart when stream starts", async () => {
    let calledWith: StreamSession | undefined;

    const engine = new StreamEngine(new TestSource(), {
      hooks: {
        onStart: (session) => {
          calledWith = session;
        },
      },
    });

    const session = new StreamSession("hook-start");

    for await (const _event of engine.stream(session, {
      prompt: "test",
    })) {
      // Consume stream.
    }

    expect(calledWith).toBe(session);
  });
  test("calls onDelta for every chunk", async () => {
    const deltas: string[] = [];

    const engine = new StreamEngine(new TestSource(), {
      hooks: {
        onDelta: (_session, text) => {
          deltas.push(text);
        },
      },
    });

    const session = new StreamSession("hook-delta");

    for await (const _event of engine.stream(session, {
      prompt: "test",
    })) {
      // Consume stream.
    }

    expect(deltas).toEqual(["Hello", " world", "!"]);
  });
  test("calls onComplete when stream completes", async () => {
    let completedSession: StreamSession | undefined;

    const engine = new StreamEngine(new TestSource(), {
      hooks: {
        onComplete: (session) => {
          completedSession = session;
        },
      },
    });

    const session = new StreamSession("hook-complete");

    for await (const _event of engine.stream(session, {
      prompt: "test",
    })) {
      // Consume stream.
    }

    expect(completedSession).toBe(session);
    expect(session.getState()).toBe(STREAM_STATES.COMPLETED);
  });
  test("calls onError when source fails", async () => {
    let receivedError: unknown;
    let receivedSession: StreamSession | undefined;

    const engine = new StreamEngine(new FailingSource(), {
      hooks: {
        onError: (session, error) => {
          receivedSession = session;
          receivedError = error;
        },
      },
    });

    const session = new StreamSession("hook-error");

    for await (const _event of engine.stream(session, {
      prompt: "test",
    })) {
      // Consume stream.
    }

    expect(receivedSession).toBe(session);
    expect(receivedError).toEqual(new Error("provider failed"));

    expect(session.getState()).toBe(STREAM_STATES.FAILED);
  });
  test("calls onCancelled when stream is cancelled", async () => {
    let cancelledSession: StreamSession | undefined;

    let cancellationReason: string | undefined;

    const source: StreamSource = {
      async *generate(_input, signal) {
        yield {
          text: "first",
        };

        await new Promise<void>((resolve) => {
          signal.addEventListener("abort", () => resolve(), { once: true });
        });
      },
    };

    const engine = new StreamEngine(source, {
      hooks: {
        onCancelled: (session, reason) => {
          cancelledSession = session;
          cancellationReason = reason;
        },
      },
    });

    const session = new StreamSession("hook-cancelled");

    for await (const event of engine.stream(session, {
      prompt: "test",
    })) {
      if (event.type === "stream.delta") {
        session.cancel();
      }
    }

    expect(cancelledSession).toBe(session);
    expect(cancellationReason).toBe("Stream was cancelled.");

    expect(session.getState()).toBe(STREAM_STATES.CANCELLED);
  });
  test("calls lifecycle hooks in order", async () => {
    const calls: string[] = [];

    const source: StreamSource = {
      async *generate() {
        yield { text: "A" };
        yield { text: "B" };
      },
    };

    const engine = new StreamEngine(source, {
      hooks: {
        onStart: () => {
          calls.push("start");
        },
        onDelta: (_session, text) => {
          calls.push(`delta:${text}`);
        },
        onComplete: () => {
          calls.push("complete");
        },
      },
    });

    const session = new StreamSession("hook-order");

    for await (const _event of engine.stream(session, {
      prompt: "test",
    })) {
      // Consume stream.
    }

    expect(calls).toEqual(["start", "delta:A", "delta:B", "complete"]);
  });
  test("hook failure does not interrupt the stream", async () => {
    const engine = new StreamEngine(new TestSource(), {
      hooks: {
        onDelta: () => {
          throw new Error("telemetry failed");
        },
      },
    });

    const session = new StreamSession("hook-failure");

    const events = [];

    for await (const event of engine.stream(session, {
      prompt: "test",
    })) {
      events.push(event);
    }

    expect(events.map((event) => event.type)).toEqual([
      "stream.start",
      "stream.delta",
      "stream.delta",
      "stream.delta",
      "stream.done",
    ]);

    expect(session.getState()).toBe(STREAM_STATES.COMPLETED);
  });

  test("error hook failure does not replace the stream error", async () => {
    const engine = new StreamEngine(new FailingSource(), {
      hooks: {
        onError: () => {
          throw new Error("telemetry failed");
        },
      },
    });

    const session = new StreamSession("error-hook-failure");

    const events = [];

    for await (const event of engine.stream(session, {
      prompt: "test",
    })) {
      events.push(event);
    }

    expect(events.at(-1)).toMatchObject({
      type: "stream.error",
      code: "STREAM_EXECUTION_FAILED",
      message: "provider failed",
    });

    expect(session.getState()).toBe(STREAM_STATES.FAILED);
  });
});
