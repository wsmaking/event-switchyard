//! SSE (Server-Sent Events) Hub
//!
//! 注文・アカウント単位でクライアントにイベントを配信する。

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
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
    order_buffers: RwLock<HashMap<String, VecDeque<SseEvent>>>,
    account_buffers: RwLock<HashMap<String, VecDeque<SseEvent>>>,
    channel_capacity: usize,
    buffer_capacity: usize,
}

impl SseHub {
    pub fn new() -> Self {
        Self::with_capacity(1000, 1000)
    }

    pub fn with_capacity(channel_capacity: usize, buffer_capacity: usize) -> Self {
        Self {
            next_event_id: AtomicU64::new(1),
            order_channels: RwLock::new(HashMap::new()),
            account_channels: RwLock::new(HashMap::new()),
            order_buffers: RwLock::new(HashMap::new()),
            account_buffers: RwLock::new(HashMap::new()),
            channel_capacity,
            buffer_capacity,
        }
    }

    pub fn from_env() -> Self {
        let buffer_capacity = std::env::var("SSE_BUFFER_SIZE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1000);
        Self::with_capacity(buffer_capacity, buffer_capacity)
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

        self.push_buffer(&self.order_buffers, order_id, event.clone());
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

        self.push_buffer(&self.account_buffers, account_id, event.clone());
        let channels = self.account_channels.read().unwrap();
        if let Some(sender) = channels.get(account_id) {
            let _ = sender.send(event);
        }
    }

    pub fn replay_order(&self, order_id: &str, last_event_id: Option<u64>) -> ReplayResult {
        self.replay_from(&self.order_buffers, order_id, last_event_id)
    }

    pub fn replay_account(&self, account_id: &str, last_event_id: Option<u64>) -> ReplayResult {
        self.replay_from(&self.account_buffers, account_id, last_event_id)
    }

    /// 注文チャンネルの購読者数
    #[allow(dead_code)]
    /// 運用メトリクス向けに保持
    pub fn order_subscriber_count(&self, order_id: &str) -> usize {
        let channels = self.order_channels.read().unwrap();
        channels
            .get(order_id)
            .map(|s| s.receiver_count())
            .unwrap_or(0)
    }

    /// アカウントチャンネルの購読者数
    #[allow(dead_code)]
    /// 運用メトリクス向けに保持
    pub fn account_subscriber_count(&self, account_id: &str) -> usize {
        let channels = self.account_channels.read().unwrap();
        channels
            .get(account_id)
            .map(|s| s.receiver_count())
            .unwrap_or(0)
    }

    fn push_buffer(&self, buffers: &RwLock<HashMap<String, VecDeque<SseEvent>>>, key: &str, event: SseEvent) {
        let mut map = buffers.write().unwrap();
        let buf = map.entry(key.to_string()).or_insert_with(|| VecDeque::with_capacity(self.buffer_capacity));
        buf.push_back(event);
        while buf.len() > self.buffer_capacity {
            buf.pop_front();
        }
    }

    fn replay_from(
        &self,
        buffers: &RwLock<HashMap<String, VecDeque<SseEvent>>>,
        key: &str,
        last_event_id: Option<u64>,
    ) -> ReplayResult {
        let from_id = last_event_id.unwrap_or(0);
        if from_id == 0 {
            return ReplayResult {
                events: Vec::new(),
                resync_required: None,
            };
        }
        let map = buffers.read().unwrap();
        let buf = match map.get(key) {
            Some(b) => b,
            None => {
                return ReplayResult {
                    events: Vec::new(),
                    resync_required: None,
                }
            }
        };
        let oldest = buf.front().map(|e| e.id);
        if let Some(oldest_id) = oldest {
            if from_id < oldest_id {
                return ReplayResult {
                    events: Vec::new(),
                    resync_required: Some(ResyncRequired {
                        last_event_id: from_id,
                        oldest_available_id: oldest_id,
                    }),
                };
            }
        }
        let events = buf
            .iter()
            .filter(|e| e.id > from_id)
            .cloned()
            .collect();
        ReplayResult {
            events,
            resync_required: None,
        }
    }
}

impl Default for SseHub {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct ResyncRequired {
    pub last_event_id: u64,
    pub oldest_available_id: u64,
}

#[derive(Debug, Clone)]
pub struct ReplayResult {
    pub events: Vec<SseEvent>,
    pub resync_required: Option<ResyncRequired>,
}

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
