import { describe, expect, test } from "bun:test";

import { FakeLLMSource } from "../packages/sources/src";

describe("FakeLLMSource", () => {
  test("generates chunks incrementally", async () => {
    const source = new FakeLLMSource({
      chunkDelayMs: 0,
    });

    const chunks: string[] = [];

    for await (const chunk of source.generate(
      {
        prompt: "hello",
      },
      new AbortController().signal,
    )) {
      chunks.push(chunk.text);
    }

    expect(chunks.length).toBeGreaterThan(0);
    expect(chunks.join("")).toContain(
      "This is a simulated response for: hello",
    );
  });

  test("supports cancellation", async () => {
    const source = new FakeLLMSource({
      chunkDelayMs: 100,
    });

    const controller = new AbortController();

    const stream = source.generate(
      {
        prompt: "hello world",
      },
      controller.signal,
    );

    const first = stream[Symbol.asyncIterator]();

    const pending = first.next();

    controller.abort();

    await expect(pending).rejects.toThrow();
  });

  test("rejects negative delay", () => {
    expect(
      () =>
        new FakeLLMSource({
          chunkDelayMs: -1,
        }),
    ).toThrow("chunkDelayMs must be >= 0");
  });
});
