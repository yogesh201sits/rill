export const STREAM_EVENT_TYPES = {
  START: "stream.start",
  DELTA: "stream.delta",
  DONE: "stream.done",
  ERROR: "stream.error",
  CANCELLED: "stream.cancelled",
} as const;

export type StreamEventType =
  (typeof STREAM_EVENT_TYPES)[keyof typeof STREAM_EVENT_TYPES];