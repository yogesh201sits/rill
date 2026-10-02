import type { StreamEvent } from "@rill/shared";

import { SSEEncoder } from "./sse-encoder";

export interface SSETransportOptions {
  readonly encoder?: SSEEncoder;
}

export interface SSETransportRequest {
  readonly events: AsyncIterable<StreamEvent>;
  readonly signal?: AbortSignal;
  readonly onDisconnect?: () => void;
}

export class SSETransport {
  private readonly encoder: SSEEncoder;

  constructor(options: SSETransportOptions = {}) {
    this.encoder = options.encoder ?? new SSEEncoder();
  }

  createResponse({
    events,
    signal,
    onDisconnect,
  }: SSETransportRequest): Response {
    const textEncoder = new TextEncoder();
    const sseEncoder = this.encoder;

    const stream = new ReadableStream<Uint8Array>({
      async start(controller) {
        const handleAbort = () => {
          onDisconnect?.();
        };

        signal?.addEventListener("abort", handleAbort, {
          once: true,
        });

        try {
          for await (const event of events) {
            if (signal?.aborted) {
              break;
            }

            const payload = sseEncoder.encode(event);

            controller.enqueue(
              textEncoder.encode(payload),
            );
          }

          if (!signal?.aborted) {
            controller.close();
          }
        } catch (error) {
          if (!signal?.aborted) {
            controller.error(error);
          }
        } finally {
          signal?.removeEventListener(
            "abort",
            handleAbort,
          );
        }
      },

      cancel() {
        onDisconnect?.();
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