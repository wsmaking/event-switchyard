//! SSE (Server-Sent Events) Hub
//!
//! 注文・アカウント単位でクライアントにイベントを配信する。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;

/// SSE イベント
#[derive(Debug, Clone)]
pub struct SseEvent {
    pub id: u64,
    pub event_type: String,
    pub data: String,
}

/// SSE Hub
pub struct SseHub {
    next_event_id: AtomicU64,
    order_channels: RwLock<HashMap<String, broadcast::Sender<SseEvent>>>,
    account_channels: RwLock<HashMap<String, broadcast::Sender<SseEvent>>>,
    channel_capacity: usize,
}

impl SseHub {
    pub fn new() -> Self {
        Self::with_capacity(1000)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            next_event_id: AtomicU64::new(1),
            order_channels: RwLock::new(HashMap::new()),
            account_channels: RwLock::new(HashMap::new()),
            channel_capacity: capacity,
        }
    }

    /// 注文のSSEチャンネルを購読
    pub fn subscribe_order(&self, order_id: &str) -> broadcast::Receiver<SseEvent> {
        let mut channels = self.order_channels.write().unwrap();
        let sender = channels
            .entry(order_id.to_string())
            .or_insert_with(|| broadcast::channel(self.channel_capacity).0);
        sender.subscribe()
    }

    /// アカウントのSSEチャンネルを購読
    pub fn subscribe_account(&self, account_id: &str) -> broadcast::Receiver<SseEvent> {
        let mut channels = self.account_channels.write().unwrap();
        let sender = channels
            .entry(account_id.to_string())
            .or_insert_with(|| broadcast::channel(self.channel_capacity).0);
        sender.subscribe()
    }

    /// 注文にイベントを配信
    pub fn publish_order(&self, order_id: &str, event_type: &str, data: &str) {
        let event = SseEvent {
            id: self.next_event_id.fetch_add(1, Ordering::Relaxed),
            event_type: event_type.to_string(),
            data: data.to_string(),
        };

        let channels = self.order_channels.read().unwrap();
        if let Some(sender) = channels.get(order_id) {
            let _ = sender.send(event);
        }
    }

    /// アカウントにイベントを配信
    pub fn publish_account(&self, account_id: &str, event_type: &str, data: &str) {
        let event = SseEvent {
            id: self.next_event_id.fetch_add(1, Ordering::Relaxed),
            event_type: event_type.to_string(),
            data: data.to_string(),
        };

        let channels = self.account_channels.read().unwrap();
        if let Some(sender) = channels.get(account_id) {
            let _ = sender.send(event);
        }
    }

    /// 注文チャンネルの購読者数
    pub fn order_subscriber_count(&self, order_id: &str) -> usize {
        let channels = self.order_channels.read().unwrap();
        channels
            .get(order_id)
            .map(|s| s.receiver_count())
            .unwrap_or(0)
    }

    /// アカウントチャンネルの購読者数
    pub fn account_subscriber_count(&self, account_id: &str) -> usize {
        let channels = self.account_channels.read().unwrap();
        channels
            .get(account_id)
            .map(|s| s.receiver_count())
            .unwrap_or(0)
    }
}

impl Default for SseHub {
    fn default() -> Self {
        Self::new()
    }
}

/// SSE Hub を Arc でラップしたもの
pub type SharedSseHub = Arc<SseHub>;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_publish_subscribe() {
        let hub = SseHub::new();

        let mut rx = hub.subscribe_order("ord_1");

        hub.publish_order("ord_1", "order_update", r#"{"status":"FILLED"}"#);

        let event = rx.recv().await.unwrap();
        assert_eq!(event.event_type, "order_update");
        assert!(event.data.contains("FILLED"));
    }

    #[tokio::test]
    async fn test_no_subscriber() {
        let hub = SseHub::new();

        // 購読者がいなくてもパニックしない
        hub.publish_order("ord_1", "order_update", r#"{"status":"FILLED"}"#);
    }
}
