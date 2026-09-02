# saasy-orchestrator

## Commands
- `make run` — run dev server (via GCP credential helper)
- `make build` / `make release` — debug / release build
- `make check` — fast compilation check (no codegen)
- `make clippy` / `make clippy-strict` — lint / lint with `-D warnings`
- `cargo lint` — strict clippy alias (pedantic + nursery + unwrap/expect warnings)
- `make fmt` — format code
- No test suite exists yet

## Conventions
- **Error types**: per-module enums with `thiserror::Error` (e.g., `LlmClientError`, `EngineClientError`, `SttClientError`). No `anyhow`.
- **Logging**: `tracing` crate (`info!`, `warn!`, `error!`, `debug!`). Not `log` or `println!`.
- **Entry point**: `#[tokio::main]`, not `#[actix_web::main]` — actix-web is only used for health endpoints.
- **Strict clippy**: pedantic + nursery + `unwrap_used`/`expect_used` enabled via `.cargo/config.toml` build rustflags.
- **Provider pattern**: trait (`LlmClient`/`SttClient`/`TtsClient`) → factory function (`create_*_config` + `create_*_client`) → per-provider module with `mod.rs`, `client.rs`, `types.rs`, `inbound_handler.rs`.
- **Shared state**: `Arc<RwLock<_>>` for read-heavy state (e.g., `SessionManager.sessions`), `CancellationToken` for graceful shutdown propagation.
- **Event passing**: `mpsc` channels between components (system events, VAD events, transcripts, audio chunks).
- **Config layering**: `config/default.toml` → env var overrides (via `config` crate + `dotenvy`). API keys come from the environment (`.env`); never committed.
- **Proto types**: from `saasy-proto-rust` (git dep): `saasy_proto_rust::{signal, sfu, shared}`.
- **All API keys required at startup** even if that provider isn't used in a session.

## Service Boundaries
- **Calls saasy-signal** (WebSocket): receives session lifecycle events (`SessionCreated`, `FarewellRequested`, `SessionEnded`), WebRTC signaling.
- **Calls saasy-edge** (gRPC): provider model catalog validation and caching.
- **Calls ListeningEngine / SpeakingEngine** (gRPC over UDS): audio pipeline — VAD events inbound, audio frames outbound. Co-located on AI Host.
- **Calls LLM providers** (SSE/HTTP): OpenAI, Anthropic, Groq, xAI, GCP Vertex, AWS Bedrock.
- **Calls STT providers** (WebSocket): Deepgram, Speechmatics.
- **Calls TTS providers** (WebSocket): Cartesia, ElevenLabs.
- **Proto types from saasy-proto-rust** (git dep): do not define proto types locally.
- **Does not own**: session lifecycle (saasy-signal), media forwarding (saasy-sfu), provider model registry (saasy-edge), proto schema (saasy-proto-rust).
