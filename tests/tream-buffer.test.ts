import {
  describe,
  expect,
  test,
} from "bun:test";

import { StreamBuffer } from "../packages/core/src";

describe("StreamBuffer", () => {
  test("stores and retrieves items in FIFO order", async () => {
    const buffer = new StreamBuffer<number>({
      capacity: 10,
    });

    await buffer.push(1);
    await buffer.push(2);
    await buffer.push(3);

    expect(await buffer.next()).toEqual({
      done: false,
      value: 1,
    });

    expect(await buffer.next()).toEqual({
      done: false,
      value: 2,
    });

    expect(await buffer.next()).toEqual({
      done: false,
      value: 3,
    });
  });

  test("waits for a producer when empty", async () => {
    const buffer = new StreamBuffer<string>();

    let resolved = false;

    const resultPromise = buffer.next().then((result) => {
      resolved = true;
      return result;
    });

    await Bun.sleep(10);

    expect(resolved).toBe(false);

    await buffer.push("hello");

    expect(await resultPromise).toEqual({
      done: false,
      value: "hello",
    });

    expect(resolved).toBe(true);
  });

  test("completes after buffered items are consumed", async () => {
    const buffer = new StreamBuffer<number>();

    await buffer.push(1);
    await buffer.push(2);

    buffer.complete();

    expect(await buffer.next()).toEqual({
      done: false,
      value: 1,
    });

    expect(await buffer.next()).toEqual({
      done: false,
      value: 2,
    });

    expect(await buffer.next()).toEqual({
      done: true,
      value: undefined,
    });
  });

  test("waits for producer when capacity is full", async () => {
    const buffer = new StreamBuffer<number>({
      capacity: 1,
    });

    await buffer.push(1);

    let pushed = false;

    const pushPromise = buffer.push(2).then(() => {
      pushed = true;
    });

    await Bun.sleep(10);

    expect(pushed).toBe(false);
    expect(buffer.size).toBe(1);

    expect(await buffer.next()).toEqual({
      done: false,
      value: 1,
    });

    await pushPromise;

    expect(buffer.size).toBe(1);

    expect(await buffer.next()).toEqual({
      done: false,
      value: 2,
    });
  });

  test("propagates producer failure", async () => {
    const buffer = new StreamBuffer<string>();

    const error = new Error("producer failed");

    const nextPromise = buffer.next();

    buffer.fail(error);

    await expect(nextPromise).rejects.toThrow(
      "producer failed",
    );
  });

  test("rejects push after completion", async () => {
    const buffer = new StreamBuffer<number>();

    buffer.complete();

    await expect(buffer.push(1)).rejects.toThrow(
      "Cannot push to a completed buffer.",
    );
  });

  test("rejects invalid capacity", () => {
    expect(
      () =>
        new StreamBuffer({
          capacity: 0,
        }),
    ).toThrow(
      "Buffer capacity must be greater than 0",
    );
  });
  test("rejects a waiting consumer when signal is aborted", async () => {
    const buffer = new StreamBuffer<string>();
    const controller = new AbortController();

    const nextPromise = buffer.next(controller.signal);

    controller.abort();

    await expect(nextPromise).rejects.toThrow(
      "The operation was aborted.",
    );

    expect(buffer.size).toBe(0);
  });

  test("rejects a waiting producer when signal is aborted", async () => {
    const buffer = new StreamBuffer<string>({
      capacity: 1,
    });

    const controller = new AbortController();

    await buffer.push("first");

    const pushPromise = buffer.push(
      "second",
      controller.signal,
    );

    controller.abort();

    await expect(pushPromise).rejects.toThrow(
      "The operation was aborted.",
    );

    expect(buffer.size).toBe(1);

    expect(await buffer.next()).toEqual({
      done: false,
      value: "first",
    });
  });

  test("immediately rejects next when signal is already aborted", async () => {
    const buffer = new StreamBuffer<string>();
    const controller = new AbortController();

    controller.abort();

    await expect(
      buffer.next(controller.signal),
    ).rejects.toThrow(
      "The operation was aborted.",
    );
  });

  test("immediately rejects push when signal is already aborted", async () => {
    const buffer = new StreamBuffer<string>();
    const controller = new AbortController();

    controller.abort();

    await expect(
      buffer.push("value", controller.signal),
    ).rejects.toThrow(
      "The operation was aborted.",
    );

    expect(buffer.size).toBe(0);
  });
});