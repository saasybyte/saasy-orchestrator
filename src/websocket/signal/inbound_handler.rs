use std::sync::Arc;

use prost::Message;
use prost::bytes::Bytes;
use saasy_proto_rust::sfu::{sfu_event, SfuEvent};
use saasy_proto_rust::shared::ProducerId;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info};

use crate::orchestrator::SessionState;

#[derive(Clone)]
pub struct SignalInboundHandler;

impl SignalInboundHandler {
    pub async fn handle_system_event(
        signal_event_tx: mpsc::Sender<SfuEvent>,
        data: Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!("Received system event: {} bytes", data.len());

        let sfu_event = SfuEvent::decode(&data[..])?;

        match &sfu_event.event {
            Some(
                sfu_event::Event::SessionCreated(_)
                | sfu_event::Event::SessionEnded(_)
                | sfu_event::Event::FarewellRequested(_)
            ) => {
                signal_event_tx.send(sfu_event).await?;
            }
            _ => {
                debug!("Ignoring event type");
            }
        }

        Ok(())
    }

    pub async fn handle_session_event(
        session_state: Arc<RwLock<SessionState>>,
        data: Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!("Received session event: {} bytes", data.len());

        let sfu_event = SfuEvent::decode(&data[..])?;

        match &sfu_event.event {
            Some(sfu_event::Event::NewProducer(event)) => {
                info!("Remote producer detected");
                
                let mut state = session_state.write().await;
                state.has_remote_producer = true;
                state.router_producer_id = Some(ProducerId {
                    id: event.producer_id.clone(),
                });
            }
            _ => {
                debug!("Ignoring event type");
            }
        }

        Ok(())
    }
}
