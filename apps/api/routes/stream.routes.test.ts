import { describe, expect, test } from "bun:test";

import app  from "../src/server";

import type {
  StreamChunk,
  StreamInput,
  StreamSource,
} from "@rill/core";

class TrackingSource implements StreamSource {
  public aborted = false;

  async *generate(
    _input: StreamInput,
    signal: AbortSignal,
  ): AsyncIterable<StreamChunk> {
    try {
      while (true) {
        if (signal.aborted) {
          this.aborted = true;
          return;
        }

        yield {
          text: "chunk",
        };

        await new Promise((resolve) =>
          setTimeout(resolve, 10),
        );
      }
    } finally {
      if (signal.aborted) {
        this.aborted = true;
      }
    }
  }
}

describe("POST /v1/stream", () => {
  test("streams SSE events", async () => {
    const response = await app.request("/v1/stream", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        prompt: "Hello",
      }),
    });

    expect(response.status).toBe(200);

    expect(
      response.headers.get("content-type"),
    ).toContain("text/event-stream");

    const body = await response.text();

    expect(body).toContain("event: stream.start");
    expect(body).toContain("event: stream.delta");
    expect(body).toContain("event: stream.done");
  });

  test("rejects an invalid request", async () => {
    const response = await app.request("/v1/stream", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({}),
    });

    expect(response.status).toBe(400);

    const body = await response.json();

    expect(body).toEqual({
      error: "INVALID_REQUEST",
      message: "prompt is required",
    });
  });
  test("cancels the stream when the request is aborted", async () => {
  const controller = new AbortController();

  const request = new Request(
    "http://localhost/v1/stream",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        prompt: "Generate a very long response",
      }),
      signal: controller.signal,
    },
  );

  const responsePromise = app.fetch(request);

  const response = await responsePromise;

  expect(response.status).toBe(200);

  controller.abort();

  expect(controller.signal.aborted).toBe(true);
});
});