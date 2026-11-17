<div align="center">

# grpcarmor

**gRPC and HTTP/2 stream starvation sentry, subchannel multiplexer, and flow-control autotuner.**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go&logoColor=white)](go.mod)
[![Zero Dependencies](https://img.shields.io/badge/Dependencies-Zero%20(Pure%20Go)-green)](#)

<p align="center">
  <a href="#the-problem">The problem</a> •
  <a href="#how-it-works">How it works</a> •
  <a href="#architecture">Architecture</a> •
  <a href="#comparison">Comparison</a> •
  <a href="#installation">Installation</a> •
  <a href="#usage">Usage</a> •
  <a href="#license">License</a>
</p>

</div>

## The problem

gRPC relies on HTTP/2 multiplexing, allowing dozens of concurrent logical RPC streams to share a single physical TCP connection. While multiplexing avoids the connection sprawl of HTTP/1.1, it introduces head-of-line blocking inside the HTTP/2 flow-control layer.

RFC 7540 specifies two independent flow-control window tiers: stream-level windows and a shared connection-level window (defaulting to 65,535 bytes = 64 KB). When an application transmits a bulk streaming RPC (such as a database export, telemetry batch, or file chunk), it consumes the entire 64 KB connection window in milliseconds:

```
  Client Process                   Single HTTP/2 Connection                   Remote gRPC Server
        |                                     |                                       |
        |  Stream 1: UploadBatch(5MB)         |                                       |
        |------------------------------------>|  Sends 65,535 bytes of DATA           |
        |                                     |-------------------------------------->|
        |                                     |  [Connection window hits 0 credits!]  |
        |                                     |                                       |
        |  Stream 3: CheckAuth() [Unary]      |                                       |
        |  (Needs 40 bytes)                   |                                       |
        |------------------------------------>|  CANNOT TRANSMIT: Window exhausted!   |
        |                                     |  [CheckAuth stalls in client memory]  |
        |                                     |                                       |
        |                                     |  (Remote server processing bulk...)   |
        |                                     |  <------------------------------------| WINDOW_UPDATE (64KB)
        |                                     |  Transmits CheckAuth bytes            |
        |                                     |-------------------------------------->|
        |  CheckAuth response received        |                                       |
        |<------------------------------------|  P99 latency: 650ms (stalled!)        |
```

This causes severe tail-latency spikes:
1. **Connection-level starvation**: Even though Stream 3 (the Unary authentication call) has its own stream-level window credits, it cannot send a single byte because the shared connection-level window is empty. It stalls in client memory until the remote server finishes reading the bulk payload and returns a `WINDOW_UPDATE` frame.
2. **The APM blind spot**: Distributed tracing (OpenTelemetry, Datadog) records an overall client latency of 650ms, while the server-side trace span shows execution completed in 2ms. The missing 648ms was invisible client-side queuing waiting for flow-control credit replenishment. SREs misdiagnose the issue as server CPU saturation or database contention.
3. **Single TCP socket contention**: High-throughput microservices saturate the single TCP socket lock and event loop at high QPS while neighboring CPU cores sit idle.

### Why existing approaches fail

- **Service mesh sidecars (Envoy/Istio)**: Envoy multiplexes upstream gRPC streams onto shared HTTP/2 sockets as well, suffering from identical connection-level flow control starvation while adding memory overhead per pod.
- **Manual client connection pooling**: Writing custom connection pools (`*grpc.ClientConn` slices with round-robin balancers) inside application code requires hundreds of lines of fragile boilerplate, breaks native name resolvers, and fails to segregate heavy streams from light unary calls.

## How it works

`grpcarmor` eliminates HTTP/2 stream starvation and tail-latency regressions through traffic segregation and proactive window management:

```
  Client Application                grpcarmor proxy                     Remote gRPC Target
         |                                 |                                    |
         |  Stream 1: UploadBatch (Bulk)   |                                    |
         |-------------------------------->|  Routes to Subchannel #2 (Bulk)    |
         |                                 |----------------------------------->|
         |                                 |  [Subchannel #2 window drains]     |
         |                                 |                                    |
         |  Stream 3: CheckAuth (Unary)    |                                    |
         |-------------------------------->|  Routes to Subchannel #1 (Unary)   |
         |                                 |----------------------------------->|
         |                                 |  [Subchannel #1 window is clean!]  |
         |                                 |  Transmits immediately (<2ms)      |
         |  CheckAuth response (1.8ms)     |                                    |
         |<--------------------------------|                                    |
```

1. **Priority-aware subchannel multiplexing**: Maintains an adaptive pool of independent TCP subchannels. Unary RPCs are routed to dedicated, clean subchannels that never experience bulk streaming drain. Heavy streaming RPCs are isolated onto separate bulk subchannels. Cross-stream head-of-line blocking is eliminated by design.
2. **Preemptive window autotuning**: Monitors the connection-level flow-control credit balance on each subchannel. When credits drop below 50% capacity, `grpcarmor` automatically injects speculative `WINDOW_UPDATE` frames on stream 0, keeping the pipe primed and preventing zero-window stalls before they occur.
3. **Starvation sentry & diagnostic prober**: Decodes HTTP/2 frames, measures exact client queuing delay, and attributes blame to the stream draining the connection window.

## Architecture

`grpcarmor` is written in pure Go with zero external dependencies.

```
grpcarmor/
├── cmd/
│   └── grpcarmor/
│       ├── main.go             # CLI interface: proxy and doctor commands
│       └── main_test.go        # Command line integration tests
├── pkg/
│   ├── h2frame/
│   │   ├── frame.go            # HTTP/2 9-byte binary frame header codec
│   │   ├── payload.go          # Frame builders: DATA, SETTINGS, WINDOW_UPDATE, PING
│   │   └── frame_test.go       # Frame encoding and decoding tests
│   ├── sentry/
│   │   ├── window.go           # Flow-control credit accounting (connection and stream)
│   │   ├── starvation.go       # Starvation detector and stream attribution engine
│   │   └── sentry_test.go      # Window deduction and starvation tests
│   ├── pool/
│   │   ├── multiplexer.go      # Multi-subchannel pool with Unary vs Bulk segregation
│   │   ├── autotuner.go        # Speculative WINDOW_UPDATE injection engine
│   │   └── pool_test.go        # Subchannel routing and autotuning tests
│   └── doctor/
│       ├── doctor.go           # Diagnostic prober testing server flow control
│       └── doctor_test.go      # Prober unit tests
├── go.mod
└── README.md
```

## Installation

Building from source:

```bash
git clone https://github.com/OGAbhinavMaurya/grpcarmor.git
cd grpcarmor
go build -o grpcarmor ./cmd/grpcarmor
sudo install -m 0755 grpcarmor /usr/local/bin/
```

## Usage

### 1. Proxy (subchannel multiplexer and autotuner)

Run `grpcarmor proxy` between your gRPC client and backend service:

```bash
# Listen on 50052 and proxy traffic across 4 subchannels to 50051
grpcarmor proxy --listen :50052 --target 127.0.0.1:50051 --subchannels 4

# Output operational metrics in JSON upon exit
grpcarmor proxy --listen :50052 --target 127.0.0.1:50051 --json
```

Point your gRPC client application to `localhost:50052`. Latency-critical Unary calls are automatically segregated from bulk streaming RPCs, while connection windows are kept primed with autotuned updates.

Example shutdown summary:
```text
--------------------------------------------------------------------------------
Proxy Metrics:
  Unary Streams Routed:    1420
  Bulk Streams Routed:     85
  Starvations Averted:     85
  Active Subchannels:      4
--------------------------------------------------------------------------------
```

### 2. Doctor (flow-control starvation diagnostic)

Diagnose whether an upstream gRPC server is vulnerable to connection-level flow control starvation:

```bash
# Run doctor diagnostic against gRPC endpoint
grpcarmor doctor --target 127.0.0.1:50051

# Output diagnostic report in JSON
grpcarmor doctor --target 127.0.0.1:50051 --json
```

Example doctor report:
```text
================================================================================
                            GRPCARMOR DOCTOR REPORT                             
================================================================================
 Target Address:           127.0.0.1:50051
 Status:                   [VULNERABLE]
 Initial Window Size:      65535 bytes (64.0 KB)
 Flow-Control Depletion:   true
 Max Burst Without Stall:  65535 bytes
 Summary:                  Target uses standard HTTP/2 connection window (65535 bytes). Bulk streams over 64 KB will drain credits to 0 and starve concurrent Unary RPCs.
 Recommendation:           Deploy 'grpcarmor proxy' with subchannel segregation and window autotuning to eliminate tail-latency spikes.
--------------------------------------------------------------------------------
 Diagnostic Trace:
   * Sent HTTP/2 connection preface (PRI * HTTP/2.0...)
   * Sent client SETTINGS frame (initial_window_size=65535)
   * Received server SETTINGS frame
   * Server configured initial stream window: 65535 bytes
================================================================================
```

## Comparison

| Feature | `grpcarmor` | Single gRPC Connection | Envoy Sidecar | Manual Conn Pool |
| :--- | :--- | :--- | :--- | :--- |
| **Unary vs Bulk Segregation** | Yes (subchannel pool) | No (shared socket) | No (shared socket) | Partial (manual code) |
| **Preemptive Window Autotuning** | Yes (stream 0 updates) | No | No | No |
| **Eliminates Client Queuing Stall** | Yes | No (stalls on 0 credits) | No (stalls on 0 credits) | Partial |
| **Zero Code Changes Required** | Yes (transparent proxy) | N/A | Yes | No (requires rewrites) |
| **Memory Overhead** | Minimal (~8MB binary) | Baseline | High (100MB+ per pod) | Baseline |
| **Tracing Attribution** | Yes (detects stall blame) | No (blind spot) | No | No |

## License

MIT
