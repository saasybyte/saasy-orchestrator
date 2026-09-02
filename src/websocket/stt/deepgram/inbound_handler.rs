use tracing::{debug, info, warn};

use super::types::{DeepgramResponse, DeepgramResultsMessage};
use crate::websocket::stt::types::SttTranscript;

pub struct DeepgramInboundHandler;

impl DeepgramInboundHandler {
    pub fn handle_response(response: DeepgramResponse) -> Option<SttTranscript> {
        match response {
            DeepgramResponse::Results(msg) => Self::handle_results(&msg),
            DeepgramResponse::Metadata(msg) => {
                info!(
                    "Deepgram Metadata: request_id={}, created={}",
                    msg.request_id, msg.created
                );
                None
            }
            DeepgramResponse::UtteranceEnd(msg) => {
                debug!("Utterance ended at {:.2}s", msg.last_word_end);
                None
            }
            DeepgramResponse::SpeechStarted(msg) => {
                debug!("Speech started at {:.2}s", msg.timestamp);
                None
            }
        }
    }

    #[allow(clippy::option_if_let_else)]
    fn handle_results(results_message: &DeepgramResultsMessage) -> Option<SttTranscript> {
        if let Some(alt) = results_message.channel.alternatives.first() {
            let speech_final = results_message.speech_final.unwrap_or(false);
            let from_finalize = results_message.from_finalize.unwrap_or(false);

            info!(
                "Transcript [speech_final={}, from_finalize={}]: \"{}\" (confidence: {:.2})",
                speech_final, from_finalize, alt.transcript, alt.confidence
            );

            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            Some(SttTranscript {
                text: alt.transcript.clone(),
                confidence: Some(alt.confidence as f32),
                timestamp: Some((results_message.start * 1000.0).round() as u64),
                speech_final,
                from_finalize,
            })
        } else {
            warn!("Results message had no alternatives");
            None
        }
    }
}
