import { describe, expect, test } from "bun:test";

import {
  STREAM_STATES,
  StreamSession,
} from "../packages/core/src";

describe("StreamSession", () => {
  test("starts in CREATED state", () => {
    const session = new StreamSession("stream-1");

    expect(session.getState()).toBe(
      STREAM_STATES.CREATED,
    );
  });

  test("transitions CREATED -> RUNNING", () => {
    const session = new StreamSession("stream-1");

    session.start();

    expect(session.getState()).toBe(
      STREAM_STATES.RUNNING,
    );
  });

  test("transitions RUNNING -> COMPLETED", () => {
    const session = new StreamSession("stream-1");

    session.start();
    session.complete();

    expect(session.getState()).toBe(
      STREAM_STATES.COMPLETED,
    );
  });

  test("transitions RUNNING -> FAILED", () => {
    const session = new StreamSession("stream-1");

    session.start();
    session.fail();

    expect(session.getState()).toBe(
      STREAM_STATES.FAILED,
    );
  });

  test("cancels a running stream", () => {
    const session = new StreamSession("stream-1");

    session.start();

    session.cancel();

    expect(session.getState()).toBe(
      STREAM_STATES.CANCELLED,
    );

    expect(session.signal.aborted).toBe(true);
  });

  test("cancellation is idempotent", () => {
    const session = new StreamSession("stream-1");

    session.start();

    session.cancel();
    session.cancel();

    expect(session.getState()).toBe(
      STREAM_STATES.CANCELLED,
    );
  });

  test("rejects invalid state transition", () => {
    const session = new StreamSession("stream-1");

    expect(() => session.complete()).toThrow(
      "Invalid stream transition",
    );
  });

  test("generates monotonically increasing sequence numbers", () => {
    const session = new StreamSession("stream-1");

    expect(session.nextSequence()).toBe(0);
    expect(session.nextSequence()).toBe(1);
    expect(session.nextSequence()).toBe(2);
  });

  test("rejects empty stream ID", () => {
    expect(() => new StreamSession("")).toThrow(
      "Stream ID must not be empty",
    );
  });
});