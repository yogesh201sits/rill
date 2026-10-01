import type { StreamEvent } from "@rill/shared";

import type { StreamEngine, StreamInput } from "@rill/core";

import { SSEEncoder } from "./sse-encoder";

export interface SSETransportOptions {
  readonly encoder?: SSEEncoder;
}

export class SSETransport {
  private readonly encoder: SSEEncoder;

  constructor(options: SSETransportOptions = {}) {
    this.encoder = options.encoder ?? new SSEEncoder();
  }

  createResponse(
    events: AsyncIterable<StreamEvent>,
  ): Response {
    const encoder = new TextEncoder();
    const sseEncoder = this.encoder;

    const stream = new ReadableStream<Uint8Array>({
      async start(controller) {
        try {
          for await (const event of events) {
            const payload = sseEncoder.encode(event);

            controller.enqueue(
              encoder.encode(payload),
            );
          }

          controller.close();
        } catch (error) {
          controller.error(error);
        }
      },
    });

    return new Response(stream, {
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      },
    });
  }
}