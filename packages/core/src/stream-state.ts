export const STREAM_STATES = {
  CREATED: "created",
  RUNNING: "running",
  COMPLETED: "completed",
  FAILED: "failed",
  CANCELLED: "cancelled",
} as const;

export type StreamState = (typeof STREAM_STATES)[keyof typeof STREAM_STATES];
