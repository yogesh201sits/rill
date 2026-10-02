import {
  describe,
  expect,
  test,
} from "bun:test";

import type { StreamEvent } from "../packages/shared/src";

import { SSETransport } from "../packages/transports/src/sse";

async function* createEvents(): AsyncIterable<StreamEvent> {
  yield {
    type: "stream.start",
    streamId: "stream-1",
    sequence: 0,
    timestamp: 100,
  };

  yield {
    type: "stream.delta",
    streamId: "stream-1",
    sequence: 1,
    timestamp: 101,
    text: "Hello",
  };

  yield {
    type: "stream.done",
    streamId: "stream-1",
    sequence: 2,
    timestamp: 102,
  };
}

describe("SSETransport", () => {
  test("creates an SSE response", async () => {
    const transport = new SSETransport();

    const response = transport.createResponse({
      events: createEvents(),
    });

    expect(response.status).toBe(200);

    expect(
      response.headers.get("content-type"),
    ).toContain("text/event-stream");

    const body = await response.text();

    expect(body).toContain(
      "event: stream.start",
    );

    expect(body).toContain(
      "event: stream.delta",
    );

    expect(body).toContain(
      "event: stream.done",
    );
  });

  test("calls onDisconnect when request is aborted", async () => {
    const controller = new AbortController();

    let disconnected = false;

    const transport = new SSETransport();

    async function* events(): AsyncIterable<StreamEvent> {
      yield {
        type: "stream.start",
        streamId: "stream-1",
        sequence: 0,
        timestamp: 100,
      };

      await new Promise((resolve) => {
        setTimeout(resolve, 100);
      });
    }

    transport.createResponse({
      events: events(),
      signal: controller.signal,
      onDisconnect: () => {
        disconnected = true;
      },
    });

    controller.abort();

    expect(disconnected).toBe(true);
  });
});