use base64::{engine::general_purpose, Engine as _};
use tracing::{debug, error, info};

use super::types::CartesiaResponse;
use crate::websocket::tts::types::TtsAudioChunk;

pub struct CartesiaInboundHandler;

impl CartesiaInboundHandler {
    pub fn handle_response(response: CartesiaResponse) -> Option<TtsAudioChunk> {
        match response {
            CartesiaResponse::Chunk(msg) => {
                debug!(
                    "Received audio chunk: {} bytes (base64), done={}, status={}, context_id={:?}",
                    msg.data.len(),
                    msg.done,
                    msg.status_code,
                    msg.context_id
                );

                // Decode Base64 audio data
                match general_purpose::STANDARD.decode(&msg.data) {
                    Ok(decoded_audio) => {
                        debug!("Decoded {} bytes of raw PCM audio", decoded_audio.len());

                        Some(TtsAudioChunk {
                            data: decoded_audio,
                            done: msg.done,
                            context_id: msg.context_id,
                        })
                    }
                    Err(e) => {
                        error!("Failed to decode Base64 audio: {}", e);
                        None
                    }
                }
            }
            CartesiaResponse::Done(msg) => {
                info!(
                    "TTS generation complete: done={}, status={}, context_id={:?}",
                    msg.done, msg.status_code, msg.context_id
                );

                // Send final marker
                Some(TtsAudioChunk {
                    data: vec![],
                    done: true,
                    context_id: msg.context_id,
                })
            }
            CartesiaResponse::Error(msg) => {
                if msg.error.contains("context not found") {
                    debug!(
                        "Cartesia TTS cancel: context not found (likely interrupted before TTS started), done={}, status={}, context_id={:?}",
                        msg.done,
                        msg.status_code,
                        msg.context_id
                    );
                } else {
                    error!(
                        "Cartesia TTS error: {}, done={}, status={}, context_id={:?}",
                        msg.error, msg.done, msg.status_code, msg.context_id
                    );
                }
                None
            }
        }
    }
}
