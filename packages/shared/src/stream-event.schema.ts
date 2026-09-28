import { z } from "zod";

export const streamEventBaseSchema = z.object({
  streamId: z.string().min(1),
  sequence: z.number().int().nonnegative(),
  timestamp: z.number().int().nonnegative(),
});

export const streamStartEventSchema = streamEventBaseSchema.extend({
  type: z.literal("stream.start"),
});

export const streamDeltaEventSchema = streamEventBaseSchema.extend({
  type: z.literal("stream.delta"),
  text: z.string(),
});

export const streamDoneEventSchema = streamEventBaseSchema.extend({
  type: z.literal("stream.done"),
  usage: z
    .object({
      inputTokens: z.number().int().nonnegative().optional(),
      outputTokens: z.number().int().nonnegative().optional(),
      totalTokens: z.number().int().nonnegative().optional(),
    })
    .optional(),
});

export const streamErrorEventSchema = streamEventBaseSchema.extend({
  type: z.literal("stream.error"),
  code: z.string().min(1),
  message: z.string().min(1),
  retryable: z.boolean(),
});

export const streamCancelledEventSchema = streamEventBaseSchema.extend({
  type: z.literal("stream.cancelled"),
  reason: z.string().optional(),
});

export const streamEventSchema = z.discriminatedUnion("type", [
  streamStartEventSchema,
  streamDeltaEventSchema,
  streamDoneEventSchema,
  streamErrorEventSchema,
  streamCancelledEventSchema,
]);

export type ValidatedStreamEvent = z.infer<typeof streamEventSchema>;
