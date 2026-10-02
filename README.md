<img width="1823" height="576" alt="Rill-logo" src="https://github.com/user-attachments/assets/f1a7f08a-3f5b-4217-9a68-ca232a23c37d" />
<h1 align="center">Rill</h1>

<p align="center">
  A production-oriented real-time streaming engine for LLM-style incremental output over SSE.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Bun-1.x-black?logo=bun" alt="Bun">
  <img src="https://img.shields.io/badge/TypeScript-5.x-blue?logo=typescript" alt="TypeScript">
  <img src="https://img.shields.io/badge/Hono-4.x-orange?logo=hono" alt="Hono">
  <img src="https://img.shields.io/badge/Transport-SSE-red" alt="SSE">
  <img src="https://img.shields.io/badge/Streams-Web%20Streams%20API-3178C6" alt="Web Streams API">
</p>

It separates stream generation, buffering, lifecycle management, and transport concerns so that the core streaming engine remains independent of HTTP and SSE.

---

## Overview

Modern LLM applications need to deliver generated output incrementally instead of waiting for the complete response.

A typical application may look like:

```text
LLM Provider
     ↓
Token / Chunk Generator
     ↓
Streaming Engine
     ↓
Buffer + Backpressure
     ↓
Transport
     ↓
Client
```

Rill focuses on the **streaming infrastructure layer**.

It does not attempt to be an LLM provider or an agent framework.

Instead, Rill provides the primitives required to take an asynchronous stream of generated chunks and reliably expose them to clients.

### Current implementation

```text
Client
  ↓
Hono API
  ↓
StreamSession
  ↓
StreamEngine
  ↓
StreamBuffer
  ↓
StreamSource
  ↓
StreamEvent
  ↓
SSETransport
  ↓
SSE Client
```

The current source implementation uses a fake LLM generator for development and testing. A real LLM provider can implement the same `StreamSource` interface later.

---

## Why Rill?

Streaming looks simple at first:

```text
generate → yield → send
```

But a production-oriented streaming system needs to handle:

* Incremental delivery
* Stream lifecycle
* Ordering
* Backpressure
* Bounded buffering
* Cancellation
* Client disconnects
* Source failures
* Consistent event contracts
* Transport separation
* Observability hooks
* Testability

Rill treats these as separate infrastructure concerns instead of placing everything inside an HTTP route.

---

## Core Design

Rill follows a **Ports and Adapters / Hexagonal Architecture** approach.

The core engine does not know about Hono, HTTP, or SSE.

```text
                    ┌──────────────────┐
                    │   Stream Source  │
                    │                  │
                    │ Fake LLM / LLM   │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │   StreamEngine   │
                    │                  │
                    │ Lifecycle        │
                    │ Cancellation     │
                    │ Error handling   │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │  StreamBuffer    │
                    │                  │
                    │ FIFO             │
                    │ Capacity         │
                    │ Backpressure     │
                    └────────┬─────────┘
                             │
                             ▼
                    StreamEvent
                             │
                             ▼
                    ┌──────────────────┐
                    │   SSETransport   │
                    │                  │
                    │ SSE encoding     │
                    │ Disconnects      │
                    └────────┬─────────┘
                             │
                             ▼
                           Client
```

This separation makes it possible to test the core streaming engine without running an HTTP server.

---

# Features

## Stream Lifecycle

Every stream follows an explicit lifecycle:

```text
CREATED
   ↓
RUNNING
   ↓
COMPLETED
```

Failure and cancellation are separate terminal states:

```text
             ┌──→ COMPLETED
CREATED → RUNNING
             ├──→ FAILED
             └──→ CANCELLED
```

Invalid state transitions are rejected by `StreamSession`.

---

## Streaming Events

Rill uses a typed event model shared between the core and transport layers.

### Start

```json
{
  "type": "stream.start",
  "streamId": "abc",
  "sequence": 0,
  "timestamp": 1790879110507
}
```

### Delta

```json
{
  "type": "stream.delta",
  "text": "Hello",
  "streamId": "abc",
  "sequence": 1,
  "timestamp": 1790879110557
}
```

### Done

```json
{
  "type": "stream.done",
  "streamId": "abc",
  "sequence": 18,
  "timestamp": 1790879111407
}
```

### Error

```json
{
  "type": "stream.error",
  "streamId": "abc",
  "sequence": 10,
  "timestamp": 1790879111007,
  "code": "STREAM_EXECUTION_FAILED",
  "message": "Source failed",
  "retryable": false
}
```

### Cancelled

```json
{
  "type": "stream.cancelled",
  "streamId": "abc",
  "sequence": 10,
  "timestamp": 1790879111007,
  "reason": "Stream was cancelled."
}
```

---

# Stream Ordering

Every event contains a monotonically increasing sequence number.

Example:

```text
stream.start   sequence=0
stream.delta   sequence=1
stream.delta   sequence=2
stream.delta   sequence=3
stream.done    sequence=4
```

This gives consumers a deterministic ordering of events within a stream.

Each stream also receives a unique `streamId`.

---

# StreamSource

The core engine depends on a source abstraction instead of a concrete LLM provider.

```ts
export interface StreamSource {
  generate(
    input: StreamInput,
    signal: AbortSignal,
  ): AsyncIterable<StreamChunk>;
}
```

A source only needs to provide an asynchronous sequence of chunks.

For example:

```ts
class MyLLMSource implements StreamSource {
  async *generate(input, signal) {
    // Call LLM provider

    yield { text: "Hello" };
    yield { text: " world" };
  }
}
```

This keeps the engine independent of the actual model provider.

---

# StreamBuffer

Rill uses a bounded FIFO buffer between the producer and consumer.

```text
Producer
   │
   │ push()
   ▼
┌───────────────┐
│ StreamBuffer  │
│               │
│ [chunk]       │
│ [chunk]       │
│ [chunk]       │
└───────┬───────┘
        │
        │ next()
        ▼
     Consumer
```

The buffer provides:

* FIFO ordering
* Configurable capacity
* Producer waiting when the buffer is full
* Consumer waiting when the buffer is empty
* Completion handling
* Failure propagation
* Cancellation-aware operations

This prevents an uncontrolled producer from continuously accumulating chunks in memory.

---

# Backpressure

Backpressure occurs when the producer generates data faster than the consumer can process it.

Without a bounded buffer:

```text
Fast Producer
     ↓
     ↓
     ↓
Unlimited Queue
     ↓
Memory Growth
```

Rill instead uses a bounded buffer:

```text
Fast Producer
     ↓
┌─────────────┐
│ Bounded     │
│ Buffer      │
└──────┬──────┘
       │
       ▼
Slower Consumer
```

When the buffer reaches capacity, `push()` waits until space becomes available.

This provides basic flow control without allowing unbounded in-memory growth.

---

# Cancellation

Streams support cancellation through `AbortSignal`.

```text
Client
  │
  │ disconnect
  ▼
HTTP Request AbortSignal
  │
  ▼
StreamSession.cancel()
  │
  ▼
AbortSignal
  │
  ├── StreamEngine
  ├── StreamBuffer
  └── StreamSource
```

Cancellation is propagated through the streaming pipeline.

The source can observe:

```ts
signal.aborted
```

and stop generating additional chunks.

The engine emits:

```text
stream.cancelled
```

instead of:

```text
stream.done
```

---

# Error Handling

Source failures are propagated through the buffer to the stream engine.

```text
StreamSource
     │
     │ error
     ▼
StreamBuffer.fail()
     │
     ▼
StreamEngine
     │
     ▼
stream.error
```

Example:

```json
{
  "type": "stream.error",
  "code": "STREAM_EXECUTION_FAILED",
  "message": "Source failed",
  "retryable": false
}
```

The stream does not emit `stream.done` after an execution failure.

---

# Lifecycle Hooks

The engine supports lifecycle hooks for future observability and integrations.

Available hooks:

```text
onStart
onDelta
onComplete
onError
onCancelled
```

Conceptually:

```ts
const engine = new StreamEngine(source, {
  hooks: {
    onStart: (session) => {},
    onDelta: (session, text) => {},
    onComplete: (session) => {},
    onError: (session, error) => {},
    onCancelled: (session, reason) => {},
  },
});
```

Hook failures are intentionally non-fatal.

A logging or metrics hook should not break the actual stream.

---

# SSE Transport

Rill currently uses **Server-Sent Events** as its HTTP transport.

The transport layer receives:

```ts
AsyncIterable<StreamEvent>
```

and converts each event into an SSE message.

Example:

```text
event: stream.start
data: {"type":"stream.start",...}

event: stream.delta
data: {"type":"stream.delta","text":"Hello",...}

event: stream.delta
data: {"type":"stream.delta","text":" world",...}

event: stream.done
data: {"type":"stream.done",...}
```

The transport does not generate or modify stream state.

Its responsibility is only to adapt stream events to SSE.

---

# API

## Start a Stream

```http
POST /v1/stream
Content-Type: application/json
```

Request:

```json
{
  "prompt": "Explain async generators"
}
```

Optional metadata can also be provided:

```json
{
  "prompt": "Explain async generators",
  "metadata": {
    "requestType": "demo"
  }
}
```

Response:

```http
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
```

The response contains a sequence of SSE events.

---

# Example Response

```text
event: stream.start
data: {"type":"stream.start","streamId":"...","sequence":0,"timestamp":...}

event: stream.delta
data: {"type":"stream.delta","text":"This","streamId":"...","sequence":1,"timestamp":...}

event: stream.delta
data: {"type":"stream.delta","text":" is","streamId":"...","sequence":2,"timestamp":...}

event: stream.delta
data: {"type":"stream.delta","text":" a","streamId":"...","sequence":3,"timestamp":...}

event: stream.done
data: {"type":"stream.done","streamId":"...","sequence":4,"timestamp":...}
```

---

# Validation

Incoming API requests are validated using Zod.

Current request contract:

```ts
const streamRequestSchema = z.object({
  prompt: z.string().min(1),
  metadata: z
    .record(z.string(), z.unknown())
    .optional(),
});
```

Invalid requests return:

```json
{
  "error": "INVALID_REQUEST",
  "message": "prompt is required"
}
```

with HTTP status:

```text
400 Bad Request
```

---

# Project Structure

```text
rill/
├── apps/
│   └── api/
│       └── src/
│           ├── server.ts
│           └── routes/
│               ├── stream.routes.ts
│               └── stream.routes.test.ts
│
├── packages/
│   ├── core/
│   │   └── src/
│   │       ├── engine/
│   │       │   ├── stream-engine.ts
│   │       │   ├── stream-session.ts
│   │       │   └── stream-state.ts
│   │       │
│   │       ├── buffer/
│   │       │   └── stream-buffer.ts
│   │       │
│   │       ├── stream-source.ts
│   │       └── index.ts
│   │
│   ├── sources/
│   │   └── src/
│   │       ├── fake-llm-source.ts
│   │       └── index.ts
│   │
│   ├── transports/
│   │   └── src/
│   │       ├── sse/
│   │       │   ├── sse-encoder.ts
│   │       ├── sse-transport.ts
│   │       ├── sse-encoder.test.ts
│   │       ├── sse-transport.test.ts
│   │       └── index.ts
│   │
│   └── shared/
│       └── src/
│           ├── stream-event.ts
│           ├── stream-event.schema.ts
│           └── index.ts
│
├── Dockerfile
├── package.json
├── tsconfig.json
└── biome.json
```

---

# Package Responsibilities

| Package            | Responsibility                                             |
| ------------------ | ---------------------------------------------------------- |
| `@rill/shared`     | Shared event types and validation schemas                  |
| `@rill/core`       | Stream lifecycle, engine, buffering and source abstraction |
| `@rill/sources`    | Stream source implementations                              |
| `@rill/transports` | Transport adapters such as SSE                             |
| `@rill/api`        | HTTP API and application wiring                            |

The important architectural rule is:

```text
Core → does not depend on Transport
Transport → consumes Core events
API → composes everything
```

---

# Design Patterns

Rill uses several software design patterns where they naturally fit the problem.

### Ports and Adapters

The `StreamSource` interface acts as a port.

Concrete LLM providers become adapters.

```text
             StreamSource
                  ▲
        ┌─────────┼─────────┐
        │         │         │
     FakeLLM    OpenAI    Other LLM
```

### State Pattern

`StreamSession` manages explicit stream states and valid transitions.

### Producer–Consumer

`StreamSource` produces chunks while the engine consumes them through `StreamBuffer`.

### Strategy / Adapter

The transport layer converts generic stream events into a specific delivery mechanism.

Currently:

```text
StreamEvent → SSETransport
```

### Observer

Lifecycle hooks provide event-driven integration points for logging, metrics and tracing.

---

# Running Locally

## Requirements

* Bun
* Git

Check Bun:

```bash
bun --version
```

---

## Install

Clone the repository:

```bash
git clone <repository-url>
cd rill
```

Install dependencies:

```bash
bun install
```

---

## Run the API

```bash
bun run dev
```

The API starts on:

```text
http://localhost:3000
```

Health check:

```http
GET /health
```

Expected response:

```json
{
  "status": "ok"
}
```

---

# Test the Stream

Send a request to:

```http
POST http://localhost:3000/v1/stream
```

with:

```json
{
  "prompt": "Explain async generators"
}
```

The response is streamed using SSE.

You can test it using an SSE-capable HTTP client such as Hoppscotch or directly from a browser/client application.

---

# Testing

Rill uses Bun's built-in test runner.

Run all tests:

```bash
bun test
```

Run type checking:

```bash
bun run typecheck
```

Package-level tests can also be run from individual workspaces.

---

# Test Coverage Areas

The current tests cover the main reliability boundaries.

### StreamSession

* Initial state
* Valid state transitions
* Invalid transitions
* Cancellation
* Idempotent cancellation
* Sequence generation
* Invalid stream IDs

### StreamBuffer

* FIFO ordering
* Completion
* Failure
* Capacity limits
* Producer backpressure
* Waiting consumers
* Consumer cancellation
* Producer cancellation
* Already-aborted signals

### StreamEngine

* Normal lifecycle
* Event ordering
* Sequence numbers
* Stream IDs
* Source input forwarding
* Empty streams
* Source failures
* Cancellation
* Backpressure
* Lifecycle hooks
* Hook failures

### SSE Transport

* SSE response creation
* Event encoding
* Event types
* Abort handling
* Client disconnect handling

### API

* Successful SSE streaming
* Request validation
* HTTP response headers
* End-to-end event delivery

---

# Example Architecture Extension

A real LLM provider can be added without changing the core engine.

Current:

```text
FakeLLMSource
      ↓
StreamEngine
```

Future:

```text
OpenAISource ─────┐
AnthropicSource ──┤
GeminiSource ─────┤
CustomSource ─────┤
                   ▼
              StreamEngine
                   ↓
              StreamBuffer
                   ↓
              StreamEvent
                   ↓
              SSETransport
```

The engine only depends on:

```ts
StreamSource
```

not on any particular provider.

---

# Reliability Model

Rill currently guarantees the following within a stream:

### Ordering

Events receive monotonically increasing sequence numbers.

### Bounded buffering

The stream buffer can be configured with a maximum capacity.

### Cancellation propagation

Cancellation is propagated through the session's `AbortSignal`.

### Terminal state

A stream reaches one terminal lifecycle state:

```text
COMPLETED
FAILED
CANCELLED
```

### Error isolation

Lifecycle hook failures do not replace or interrupt the primary stream execution.

---

# Current Scope

Rill currently focuses on the core streaming pipeline:

```text
Async Source
     ↓
Stream Engine
     ↓
Buffer
     ↓
Stream Events
     ↓
SSE
```

The project intentionally does not currently include:

* Persistent stream storage
* Stream replay
* Authentication
* Rate limiting
* Multi-region distribution
* Distributed buffering
* SSE event IDs / `Last-Event-ID`
* Multiple transport implementations
* Production LLM provider adapters

These can be introduced independently as the architecture evolves.

---

# Roadmap

Potential future work:

* Production LLM source adapters
* Structured observability
* Metrics
* Distributed stream management
* Authentication and API keys
* Rate limiting
* Stream registry
* Stream replay
* Advanced failure/retry policies
* Performance benchmarking
* Docker-based deployment
* Production deployment configuration

The roadmap is intentionally incremental so that reliability features can be added without coupling the core engine to infrastructure concerns.

---

# Performance Model

The streaming path is designed around asynchronous, non-blocking primitives:

```text
AsyncIterable
     ↓
StreamBuffer
     ↓
ReadableStream
     ↓
SSE
```

The system does not wait for the complete generated response before sending data to the client.

Instead, chunks move through the pipeline incrementally:

```text
chunk 1 → client
chunk 2 → client
chunk 3 → client
chunk 4 → client
...
```

---

# Example Source Implementation

A source can be as simple as:

```ts
import type {
  StreamChunk,
  StreamInput,
  StreamSource,
} from "@rill/core";

export class ExampleSource implements StreamSource {
  async *generate(
    input: StreamInput,
    signal: AbortSignal,
  ): AsyncIterable<StreamChunk> {
    const response = `Response for: ${input.prompt}`;

    for (const text of response.split(" ")) {
      if (signal.aborted) {
        return;
      }

      yield {
        text: `${text} `,
      };
    }
  }
}
```

Then:

```ts
const source = new ExampleSource();

const engine = new StreamEngine(source);

const session = new StreamSession(
  crypto.randomUUID(),
);

const events = engine.stream(session, {
  prompt: "Hello Rill",
});
```

The source implementation is completely separate from the transport.

---

# Engineering Principles

Rill follows a few core principles:

**Separation of concerns**

The stream engine should not know how events are transported.

**Dependency inversion**

The engine depends on abstractions such as `StreamSource`.

**Bounded resources**

Buffers should have explicit capacity rather than growing indefinitely.

**Explicit lifecycle**

Stream states and terminal conditions should be deterministic.

**Cancellation-aware design**

Long-running asynchronous work should respond to cancellation.

**Testability**

Core behavior should be testable without requiring an HTTP server.

**Incremental complexity**

Features are added only when they solve an actual infrastructure problem.

---
