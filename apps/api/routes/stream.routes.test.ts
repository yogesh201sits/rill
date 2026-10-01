import { describe, expect, test } from "bun:test";

import app  from "../src/server";

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
});