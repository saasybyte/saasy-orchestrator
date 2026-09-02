# saasy-orchestrator

Intelligence-plane orchestrator for [SaasyByte](https://github.com/saasybyte/saasybyte), an open-source real-time AI voice platform.

The Orchestrator is the brain of the AI participant. It listens for session lifecycle events from the signaling server, autonomously joins new sessions, and runs the voice pipeline: transcripts in from the Listening Engine, LLM inference, streaming TTS out through the Speaking Engine. It implements a multi-provider architecture where LLM, STT, and TTS providers are swappable per modality at runtime, with the model catalog fetched from the API service.

Providers: OpenAI, Anthropic, xAI, Groq, GCP Vertex, AWS Bedrock (LLM); Deepgram, Speechmatics (STT); Cartesia, ElevenLabs (TTS).

## How It Fits

- **Calls saasy-signal** (WebSocket): receives session lifecycle events and performs WebRTC signaling as a participant.
- **Calls saasy-edge** (gRPC): provider model catalog validation and caching.
- **Calls the media engines** (gRPC over Unix Domain Sockets): VAD events and transcript audio inbound from the Listening Engine, synthesized audio outbound to the Speaking Engine. Co-located on the same host.
- **Proto types** come from [saasy-proto-rust](https://github.com/saasybyte/saasy-proto-rust) (git dependency).

See the [platform overview](https://github.com/saasybyte/saasybyte) for the full architecture.

## The Assistant Persona

The system prompt lives at `src/prompts/system.md` and is compiled into the binary (`include_str!`). Anysia is the shipped persona; to use your own, edit that file and rebuild.

## Build & Run

Requirements: stable Rust toolchain, `protoc` (protobuf compiler).

```bash
make run            # run dev server (wraps cargo run with the GCP credential helper)
make build          # debug build
make release        # release build
make clippy-strict  # lint, fail on warnings
```

Configuration is layered: `config/default.toml` provides defaults, overridden by environment variables (loaded from `.env` via dotenvy). All provider API keys are required at startup, even for providers unused in a given session. See `.env.example` for the full list. GCP credentials can be passed as `GCP_SA_JSON`; the `run-with-gcp.sh` entrypoint writes them to a temp file and sets `GOOGLE_APPLICATION_CREDENTIALS`.

A `Dockerfile` is included; `docker build .` needs no credentials.

## License

Apache-2.0, see [LICENSE](LICENSE).
