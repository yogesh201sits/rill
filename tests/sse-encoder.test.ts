import { describe, expect, test } from "bun:test";

import type { StreamDeltaEvent } from "../packages/shared/src";

import { SSEEncoder } from "../packages/transports/src/sse/sse-encoder";

describe("SSEEncoder", () => {
  test("encodes a stream event into SSE format", () => {
    const encoder = new SSEEncoder();

    const event: StreamDeltaEvent = {
      type: "stream.delta",
      streamId: "stream-1",
      sequence: 1,
      timestamp: 123456,
      text: "Hello",
    };

    const result = encoder.encode(event);

    expect(result).toBe(
      `event: stream.delta\ndata: ${JSON.stringify(event)}\n\n`,
    );
  });
});