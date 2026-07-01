# Distributed Task Execution Engine

A high-performance distributed task execution engine built with Java and Python.

## Architecture

- **MasterApp.java** — gRPC orchestration master with WebSocket telemetry and a priority job queue (1,000 tasks).
- **WorkerApp.java** — Multi-threaded compute worker; one thread per CPU core, each managing a persistent Python TCP daemon.
- **compute_daemon.py** — Headless Python TCP server handling CPU-bound workloads: `HASH_CRACKING`, `LOG_PARSING`, `SORT_BENCHMARK`, `PRIME_SIEVE`.
- **dashboard/index.html** — Real-time monitoring dashboard connected via WebSocket.

## Tech Stack

- Java 18, gRPC, Protocol Buffers
- Python 3, TCP sockets
- Gradle build system

## Running

```bash
.\run_cluster.bat
```

Then open `dashboard/index.html` in a browser.
