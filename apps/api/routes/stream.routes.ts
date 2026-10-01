import { Hono } from "hono";
import { z } from "zod";

import {
  StreamEngine,
  StreamSession,
} from "@rill/core";

import { FakeLLMSource } from "@rill/sources";

import { SSETransport } from "@rill/transports";

const streamRequestSchema = z.object({
  prompt: z.string().min(1),
  metadata: z.record(z.string(), z.unknown()).optional(),
});

const source = new FakeLLMSource({
  chunkDelayMs: 50,
});

const engine = new StreamEngine(source);

const transport = new SSETransport();

export const streamRoutes = new Hono();

streamRoutes.post("/v1/stream", async (c) => {
  const body = await c.req.json();

  const parsed = streamRequestSchema.safeParse(body);

  if (!parsed.success) {
    return c.json(
      {
        error: "INVALID_REQUEST",
        message: "prompt is required",
      },
      400,
    );
  }

  const streamId = crypto.randomUUID();

  const session = new StreamSession(streamId);

  const events = engine.stream(session, {
    prompt: parsed.data.prompt,
    ...(parsed.data.metadata
      ? {
          metadata: parsed.data.metadata,
        }
      : {}),
  });

  return transport.createResponse(events);
});