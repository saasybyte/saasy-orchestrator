use base64::{engine::general_purpose, Engine as _};
use tracing::{debug, error, info};

use super::types::ElevenLabsResponse;
use crate::websocket::tts::types::TtsAudioChunk;

pub struct ElevenLabsInboundHandler;

impl ElevenLabsInboundHandler {
    pub fn handle_response(response: ElevenLabsResponse) -> Option<TtsAudioChunk> {
        match response {
            ElevenLabsResponse::Audio(msg) => {
                debug!(
                    "Received audio chunk: {} bytes (base64), context_id={:?}",
                    msg.audio.len(),
                    msg.context_id
                );

                match general_purpose::STANDARD.decode(&msg.audio) {
                    Ok(decoded_audio) => {
                        debug!("Decoded {} bytes of 24kHz PCM audio", decoded_audio.len());

                        // Upsample 24kHz -> 48kHz (2x linear interpolation)
                        let upsampled = upsample_2x(&decoded_audio);
                        debug!("Upsampled to {} bytes of 48kHz PCM audio", upsampled.len());

                        Some(TtsAudioChunk {
                            data: upsampled,
                            done: false,
                            context_id: msg.context_id,
                        })
                    }
                    Err(e) => {
                        error!("Failed to decode Base64 audio: {}", e);
                        None
                    }
                }
            }
            ElevenLabsResponse::Final(msg) => {
                if msg.is_final {
                    info!("TTS generation complete for context_id={:?}", msg.context_id);

                    Some(TtsAudioChunk {
                        data: vec![],
                        done: true,
                        context_id: msg.context_id,
                    })
                } else {
                    None
                }
            }
        }
    }
}

/// Upsample PCM S16LE audio from 24kHz to 48kHz using linear interpolation.
/// For each pair of samples, inserts an interpolated sample between them.
fn upsample_2x(input: &[u8]) -> Vec<u8> {
    // PCM S16LE: 2 bytes per sample
    let sample_count = input.len() / 2;
    if sample_count < 2 {
        return input.to_vec();
    }

    // Output will have (2 * sample_count - 1) samples, but we approximate as 2x
    // Actually: for N input samples, we get 2N-1 output samples
    // We'll output 2N samples by duplicating the last sample
    let mut output = Vec::with_capacity(input.len() * 2);

    for i in 0..sample_count {
        // Read current sample as i16 (little-endian)
        let s0 = i16::from_le_bytes([input[i * 2], input[i * 2 + 1]]);

        // Output current sample
        output.extend_from_slice(&s0.to_le_bytes());

        // If not the last sample, output interpolated sample
        if i < sample_count - 1 {
            let s1 = i16::from_le_bytes([input[(i + 1) * 2], input[(i + 1) * 2 + 1]]);
            // Linear interpolation: (s0 + s1) / 2
            let interpolated = s0.midpoint(s1);
            output.extend_from_slice(&interpolated.to_le_bytes());
        } else {
            // Duplicate last sample to maintain exact 2x ratio
            output.extend_from_slice(&s0.to_le_bytes());
        }
    }

    output
}
