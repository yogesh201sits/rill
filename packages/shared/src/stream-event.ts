export const STREAM_EVENT_TYPES = {
  START: "stream.start",
  DELTA: "stream.delta",
  DONE: "stream.done",
  ERROR: "stream.error",
  CANCELLED: "stream.cancelled",
} as const;

export type StreamEventType =
  (typeof STREAM_EVENT_TYPES)[keyof typeof STREAM_EVENT_TYPES];

export type StreamId = string;

export interface StreamEventBase {
  streamId: StreamId;
  sequence: number;
  timestamp: number;
}

export interface StreamStartEvent extends StreamEventBase {
  type: "stream.start";
}

export interface StreamDeltaEvent extends StreamEventBase {
  type: "stream.delta";
  text: string;
}

export interface StreamDoneEvent extends StreamEventBase {
  type: "stream.done";
  usage?: {
    inputTokens?: number;
    outputTokens?: number;
    totalTokens?: number;
  };
}

export interface StreamErrorEvent extends StreamEventBase {
  type: "stream.error";
  code: string;
  message: string;
  retryable: boolean;
}

export interface StreamCancelledEvent extends StreamEventBase {
  type: "stream.cancelled";
  reason?: string;
}

export type StreamEvent =
  | StreamStartEvent
  | StreamDeltaEvent
  | StreamDoneEvent
  | StreamErrorEvent
  | StreamCancelledEvent;