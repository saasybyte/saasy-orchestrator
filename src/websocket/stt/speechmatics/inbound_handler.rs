use tracing::{debug, info, warn};

use super::types::{SpeechmaticsResponse, TranscriptMessage};
use crate::websocket::stt::types::SttTranscript;

pub struct SpeechmaticsInboundHandler;

impl SpeechmaticsInboundHandler {
    pub fn handle_response(
        response: SpeechmaticsResponse,
        pending_force_end: bool,
    ) -> Option<SttTranscript> {
        match response {
            SpeechmaticsResponse::RecognitionStarted(msg) => {
                info!("Speechmatics RecognitionStarted: id={}", msg.id);
                None
            }
            SpeechmaticsResponse::AudioAdded(msg) => {
                debug!("Speechmatics AudioAdded: seq_no={}", msg.seq_no);
                None
            }
            SpeechmaticsResponse::AddPartialTranscript(msg) => {
                Self::handle_transcript(&msg, false, pending_force_end)
            }
            SpeechmaticsResponse::AddTranscript(msg) => {
                Self::handle_transcript(&msg, true, pending_force_end)
            }
            SpeechmaticsResponse::EndOfUtterance(msg) => {
                debug!("Speechmatics EndOfUtterance at {:.2}s", msg.metadata.end_time);
                None
            }
            SpeechmaticsResponse::EndOfTranscript => {
                info!("Speechmatics EndOfTranscript received");
                None
            }
            SpeechmaticsResponse::Info(msg) => {
                info!("Speechmatics Info: type={}, reason={}", msg.r#type, msg.reason);
                None
            }
            SpeechmaticsResponse::Warning(msg) => {
                warn!(
                    "Speechmatics Warning: type={}, reason={}",
                    msg.r#type, msg.reason
                );
                None
            }
            SpeechmaticsResponse::Error(msg) => {
                warn!(
                    "Speechmatics Error: type={}, reason={}",
                    msg.r#type, msg.reason
                );
                None
            }
            SpeechmaticsResponse::Unknown => {
                debug!("Speechmatics: Received unknown message type");
                None
            }
        }
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn handle_transcript(
        msg: &TranscriptMessage,
        is_final: bool,
        from_finalize: bool,
    ) -> Option<SttTranscript> {
        let text = &msg.metadata.transcript;

        // Get confidence from first word's first alternative, if available
        let confidence = msg
            .results
            .first()
            .and_then(|r| r.alternatives.first())
            .map(|a| a.confidence as f32);

        // Don't mark empty transcripts as speech_final - keep waiting for real content
        // Speechmatics sometimes sends AddTranscript with empty text before actual text is ready
        let has_content = !text.trim().is_empty();
        let effective_speech_final = is_final && has_content;

        info!(
            "Transcript [speech_final={}, from_finalize={}]: \"{}\" (confidence: {:?})",
            effective_speech_final, from_finalize && effective_speech_final, text, confidence
        );

        Some(SttTranscript {
            text: text.clone(),
            speech_final: effective_speech_final,
            from_finalize: effective_speech_final && from_finalize,
            confidence,
            timestamp: Some((msg.metadata.start_time * 1000.0).round() as u64),
        })
    }
}
