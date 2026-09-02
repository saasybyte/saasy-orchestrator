use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::SessionState;

pub struct SessionManager {
    sessions: RwLock<HashMap<String, SessionHandle>>,
}

pub struct SessionHandle {
    pub task: JoinHandle<()>,
    pub state: Arc<RwLock<SessionState>>,
    pub session_token: CancellationToken,
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_session_state(&self, session_id: &str) -> Option<Arc<RwLock<SessionState>>> {
        let sessions = self.sessions.read().await;
        sessions.get(session_id).map(|handle| handle.state.clone())
    }

    pub async fn get_all_session_ids(&self) -> Vec<String> {
        self.sessions.read().await.keys().cloned().collect()
    }

    pub async fn add_session(
        &self,
        session_id: &str,
        task: JoinHandle<()>,
        session_token: CancellationToken
    ) {
        let mut sessions = self.sessions.write().await;
        if sessions.contains_key(session_id) {
            warn!("Session {session_id} already exists");
            task.abort();
            return;
        }

        let state = Arc::new(RwLock::new(SessionState::new(session_id.to_string())));
        
        sessions.insert(session_id.to_string(), SessionHandle {
            task,
            state,
            session_token,
        });
    }

    pub async fn remove_session(&self, session_id: &str) -> Option<SessionHandle> {
        self.sessions.write().await.remove(session_id)
    }

    pub async fn shutdown_all_sessions(&self) -> Vec<JoinHandle<()>> {
        let mut sessions = self.sessions.write().await;
        sessions
            .drain()
            .map(|(_, handle)| {
                handle.session_token.cancel();
                handle.task
            })
            .collect()
    }
}
