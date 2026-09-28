import {
  STREAM_EVENT_TYPES,
  type StreamEvent,
} from "@rill/shared";

import type { StreamInput } from "./stream-source";

import { StreamBuffer } from "./stream-buffer";
import type { StreamSource } from "./stream-source";
import { StreamSession } from "./stream-session";

type StreamEventPayload = {
  [Type in StreamEvent["type"]]: Omit<
    Extract<StreamEvent, { type: Type }>,
    "streamId" | "sequence" | "timestamp"
  >;
}[StreamEvent["type"]];

export interface StreamEngineHooks {
  readonly onStart?: (
    session: StreamSession,
  ) => void | Promise<void>;

  readonly onDelta?: (
    session: StreamSession,
    text: string,
  ) => void | Promise<void>;

  readonly onComplete?: (
    session: StreamSession,
  ) => void | Promise<void>;

  readonly onError?: (
    session: StreamSession,
    error: unknown,
  ) => void | Promise<void>;

  readonly onCancelled?: (
    session: StreamSession,
    reason?: string,
  ) => void | Promise<void>;
}

export interface StreamEngineOptions {
  readonly now?: () => number;
  readonly bufferCapacity?: number;
  readonly hooks?: StreamEngineHooks;
}

export class StreamEngine {
  private readonly now: () => number;
  private readonly bufferCapacity: number;
  private readonly hooks: StreamEngineHooks;

  constructor(
    private readonly source: StreamSource,
    options: StreamEngineOptions = {},
  ) {
    this.now = options.now ?? Date.now;
    this.bufferCapacity = options.bufferCapacity ?? 100;
    this.hooks = options.hooks ?? {};

    if (this.bufferCapacity <= 0) {
      throw new Error(
        "Buffer capacity must be greater than 0",
      );
    }
  }

  async *stream(
    session: StreamSession,
    input: StreamInput,
  ): AsyncIterable<StreamEvent> {
    session.start();

    await this.hooks.onStart?.(session);

    yield this.createEvent(session, {
      type: STREAM_EVENT_TYPES.START,
    });

    const buffer = new StreamBuffer<{
      type: "delta";
      text: string;
    }>({
      capacity: this.bufferCapacity,
    });

    const producer = this.produce(
      session,
      input,
      buffer,
    );

    try {
      while (true) {
        const result = await buffer.next(
          session.signal,
        );

        if (result.done) {
          break;
        }

        const event = this.createEvent(session, {
          type: STREAM_EVENT_TYPES.DELTA,
          text: result.value.text,
        });

        await this.hooks.onDelta?.(
          session,
          result.value.text,
        );

        yield event;
      }

      await producer;

      if (session.signal.aborted) {
        this.handleCancellation(session);

        const reason = "Stream was cancelled.";

        await this.hooks.onCancelled?.(
          session,
          reason,
        );

        yield this.createEvent(session, {
          type: STREAM_EVENT_TYPES.CANCELLED,
          reason,
        });

        return;
      }

      session.complete();

      await this.hooks.onComplete?.(session);

      yield this.createEvent(session, {
        type: STREAM_EVENT_TYPES.DONE,
      });
    } catch (error) {
      this.handleError(session, error);

      if (session.getState() === "cancelled") {
        const reason = "Stream was cancelled.";

        await this.hooks.onCancelled?.(
          session,
          reason,
        );

        yield this.createEvent(session, {
          type: STREAM_EVENT_TYPES.CANCELLED,
          reason,
        });

        return;
      }

      await this.hooks.onError?.(
        session,
        error,
      );

      yield this.createEvent(session, {
        type: STREAM_EVENT_TYPES.ERROR,
        code: "STREAM_EXECUTION_FAILED",
        message: this.getErrorMessage(error),
        retryable: false,
      });
    }
  }

  private async produce(
    session: StreamSession,
    input: StreamInput,
    buffer: StreamBuffer<{
      type: "delta";
      text: string;
    }>,
  ): Promise<void> {
    try {
      for await (const chunk of this.source.generate(
        input,
        session.signal,
      )) {
        if (session.signal.aborted) {
          break;
        }

        await buffer.push(
          {
            type: "delta",
            text: chunk.text,
          },
          session.signal,
        );
      }

      if (!session.signal.aborted) {
        buffer.complete();
      }
    } catch (error) {
      buffer.fail(error);
    }
  }

  private createEvent(
    session: StreamSession,
    event: StreamEventPayload,
  ): StreamEvent {
    return {
      ...event,
      streamId: session.id,
      sequence: session.nextSequence(),
      timestamp: this.now(),
    } as StreamEvent;
  }

  private handleCancellation(
    session: StreamSession,
  ): void {
    if (session.getState() !== "cancelled") {
      session.cancel();
    }
  }

  private handleError(
    session: StreamSession,
    error: unknown,
  ): void {
    if (session.signal.aborted) {
      this.handleCancellation(session);
      return;
    }

    if (session.getState() === "running") {
      session.fail();
    }
  }

  private getErrorMessage(error: unknown): string {
    if (error instanceof Error) {
      return error.message;
    }

    return "Unknown stream execution error.";
  }
}