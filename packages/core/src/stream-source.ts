export interface StreamInput {
  readonly prompt: string;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

export interface StreamChunk {
  readonly text: string;
}

export interface StreamSource {
  generate(
    input: StreamInput,
    signal: AbortSignal
  ): AsyncIterable<StreamChunk>;
}