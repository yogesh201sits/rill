import type { StreamChunk, StreamInput, StreamSource } from "@rill/core";

export interface FakeLLMSourceOptions {
  readonly chunkDelayMs?: number;
}

const DEFAULT_CHUNK_DELAY_MS = 50;

export class FakeLLMSource implements StreamSource {
  private readonly chunkDelayMs: number;

  constructor(options: FakeLLMSourceOptions = {}) {
    this.chunkDelayMs = options.chunkDelayMs ?? DEFAULT_CHUNK_DELAY_MS;

    if (this.chunkDelayMs < 0) {
      throw new Error("chunkDelayMs must be >= 0");
    }
  }

  async *generate(
    input: StreamInput,
    signal: AbortSignal,
  ): AsyncIterable<StreamChunk> {
    const chunks = this.createChunks(input.prompt);

    for (const text of chunks) {
      this.throwIfAborted(signal);

      await this.delay(this.chunkDelayMs, signal);

      this.throwIfAborted(signal);

      yield {
        text,
      };
    }
  }

  private createChunks(prompt: string): readonly string[] {
    const response = `This is a simulated response for: ${prompt}`;

    return response.split(/(\s+)/).filter(Boolean);
  }

  private async delay(
    milliseconds: number,
    signal: AbortSignal,
  ): Promise<void> {
    if (milliseconds === 0) {
      return;
    }

    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(resolve, milliseconds);

      const onAbort = () => {
        clearTimeout(timer);
        reject(
          signal.reason ??
            new DOMException("The stream was aborted.", "AbortError"),
        );
      };

      if (signal.aborted) {
        clearTimeout(timer);
        onAbort();
        return;
      }

      signal.addEventListener("abort", onAbort, {
        once: true,
      });
    });
  }

  private throwIfAborted(signal: AbortSignal): void {
    if (signal.aborted) {
      throw (
        signal.reason ??
        new DOMException("The stream was aborted.", "AbortError")
      );
    }
  }
}
