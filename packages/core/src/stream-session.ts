import type { StreamId } from "@rill/shared";

import {
  STREAM_STATES,
  type StreamState,
} from "./stream-state";

export class StreamSession {
  readonly id: StreamId;

  private state: StreamState = STREAM_STATES.CREATED;
  private sequence = 0;

  private readonly controller = new AbortController();

  constructor(id: StreamId) {
    if (!id) {
      throw new Error("Stream ID must not be empty");
    }

    this.id = id;
  }

  getState(): StreamState {
    return this.state;
  }

  get signal(): AbortSignal {
    return this.controller.signal;
  }

  nextSequence(): number {
    const sequence = this.sequence;

    this.sequence += 1;

    return sequence;
  }

  start(): void {
    this.transition(
      STREAM_STATES.CREATED,
      STREAM_STATES.RUNNING,
    );
  }

  complete(): void {
    this.transition(
      STREAM_STATES.RUNNING,
      STREAM_STATES.COMPLETED,
    );
  }

  fail(): void {
    this.transition(
      STREAM_STATES.RUNNING,
      STREAM_STATES.FAILED,
    );
  }

  cancel(): void {
    if (
      this.state === STREAM_STATES.COMPLETED ||
      this.state === STREAM_STATES.FAILED ||
      this.state === STREAM_STATES.CANCELLED
    ) {
      return;
    }

    this.state = STREAM_STATES.CANCELLED;

    this.controller.abort(
      new DOMException(
        "The stream was cancelled.",
        "AbortError",
      ),
    );
  }

  private transition(
    expected: StreamState,
    next: StreamState,
  ): void {
    if (this.state !== expected) {
      throw new Error(
        `Invalid stream transition: ${this.state} -> ${next}`,
      );
    }

    this.state = next;
  }
}