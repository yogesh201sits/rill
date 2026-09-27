import {
  STREAM_EVENT_TYPES,
  type StreamEvent,
} from "@rill/shared";

import type { StreamInput } from "./stream-source";

import type { StreamSource } from "./stream-source";
import { StreamSession } from "./stream-session";

type StreamEventPayload = {
  [Type in StreamEvent["type"]]: Omit<
    Extract<StreamEvent, { type: Type }>,
    "streamId" | "sequence" | "timestamp"
  >;
}[StreamEvent["type"]];

export interface StreamEngineOptions {
  readonly now?: () => number;
}

export class StreamEngine {
  private readonly now: () => number;

  constructor(
    private readonly source: StreamSource,
    options: StreamEngineOptions = {},
  ) {
    this.now = options.now ?? Date.now;
  }

  async *stream(
    session: StreamSession,
    input: StreamInput,
  ): AsyncIterable<StreamEvent> {
    session.start();

    yield this.createEvent(session, {
      type: STREAM_EVENT_TYPES.START,
    });

    try {
      for await (const chunk of this.source.generate(
        input,
        session.signal,
      )) {
        if (session.signal.aborted) {
          break;
        }

        yield this.createEvent(session, {
          type: STREAM_EVENT_TYPES.DELTA,
          text: chunk.text,
        });
      }

      if (session.signal.aborted) {
        if (session.getState() !== "cancelled") {
          session.cancel();
        }

        yield this.createEvent(session, {
          type: STREAM_EVENT_TYPES.CANCELLED,
          reason: "Stream was cancelled.",
        });

        return;
      }

      session.complete();

      yield this.createEvent(session, {
        type: STREAM_EVENT_TYPES.DONE,
      });
    } catch (error) {
      if (session.signal.aborted) {
        if (session.getState() !== "cancelled") {
          session.cancel();
        }

        yield this.createEvent(session, {
          type: STREAM_EVENT_TYPES.CANCELLED,
          reason: "Stream was cancelled.",
        });

        return;
      }

      session.fail();

      yield this.createEvent(session, {
        type: STREAM_EVENT_TYPES.ERROR,
        code: "STREAM_EXECUTION_FAILED",
        message: this.getErrorMessage(error),
        retryable: false,
      });
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

  private getErrorMessage(error: unknown): string {
    if (error instanceof Error) {
      return error.message;
    }

    return "Unknown stream execution error.";
  }
}