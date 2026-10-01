import type { StreamEvent } from "@rill/shared";

export class SSEEncoder {
  encode(event: StreamEvent): string {
    const data = JSON.stringify(event);

    return [
      `event: ${event.type}`,
      `data: ${data}`,
      "",
      "",
    ].join("\n");
  }
}