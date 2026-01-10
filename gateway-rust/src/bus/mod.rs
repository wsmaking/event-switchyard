//! Bus event publisher (Kafka).
//!
//! Best-effort publish for downstream consumers.

use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::sync::mpsc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::warn;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BusEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub at: u64,
    pub account_id: String,
    pub order_id: Option<String>,
    pub data: Value,
}

pub struct BusPublisher {
    enabled: bool,
    topic: String,
    producer: Mutex<Option<FutureProducer>>,
    stats: Arc<BusStats>,
    delivery_tx: Option<mpsc::Sender<DeliveryFuture>>,
    warned_missing_producer: AtomicBool,
}

impl BusPublisher {
    pub fn from_env() -> anyhow::Result<Self> {
        let enabled = std::env::var("KAFKA_ENABLE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let bootstrap = std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".into());
        let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "events".into());
        let client_id = std::env::var("KAFKA_CLIENT_ID").unwrap_or_else(|_| "gateway-rust".into());

        let producer = if enabled {
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", &bootstrap)
                .set("client.id", &client_id)
                .set("acks", "all")
                .set("enable.idempotence", "true")
                .set("linger.ms", "5")
                .set("batch.size", "65536")
                .set("compression.type", "lz4")
                .create()?;
            Some(producer)
        } else {
            None
        };

        let stats = Arc::new(BusStats::new());
        let delivery_tx = if enabled {
            let (tx, rx) = mpsc::channel::<DeliveryFuture>();
            let stats_thread = Arc::clone(&stats);
            std::thread::spawn(move || delivery_loop(rx, stats_thread));
            Some(tx)
        } else {
            None
        };

        Ok(Self {
            enabled,
            topic,
            producer: Mutex::new(producer),
            stats,
            delivery_tx,
            warned_missing_producer: AtomicBool::new(false),
        })
    }

    pub fn publish(&self, event: BusEvent) {
        if !self.enabled {
            self.stats.publish_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let mut guard = match self.producer.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let producer = match guard.as_mut() {
            Some(p) => p,
            None => {
                self.stats.publish_dropped.fetch_add(1, Ordering::Relaxed);
                if !self.warned_missing_producer.swap(true, Ordering::Relaxed) {
                    warn!("kafka publisher missing producer instance");
                }
                return;
            }
        };
        let payload = match serde_json::to_vec(&event) {
            Ok(p) => p,
            Err(err) => {
                self.stats.publish_dropped.fetch_add(1, Ordering::Relaxed);
                warn!("kafka publish serialization failed: {}", err);
                return;
            }
        };
        let key = event.order_id.as_deref().unwrap_or(event.account_id.as_str());
        match producer.send_result(
            FutureRecord::to(&self.topic)
                .payload(&payload)
                .key(key),
        ) {
            Ok(delivery) => {
                self.stats.publish_queued.fetch_add(1, Ordering::Relaxed);
                if let Some(tx) = &self.delivery_tx {
                    if tx.send(delivery).is_err() {
                        self.stats.publish_dropped.fetch_add(1, Ordering::Relaxed);
                        warn!("kafka delivery worker unavailable");
                    }
                } else {
                    self.stats.publish_dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err((err, _)) => {
                self.stats.publish_dropped.fetch_add(1, Ordering::Relaxed);
                warn!("kafka publish enqueue failed: {}", err);
            }
        }
    }

    pub fn publish_blocking(&self, event: BusEvent) -> bool {
        if !self.enabled {
            self.stats.publish_dropped.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        let mut guard = match self.producer.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let producer = match guard.as_mut() {
            Some(p) => p,
            None => {
                self.stats.publish_dropped.fetch_add(1, Ordering::Relaxed);
                if !self.warned_missing_producer.swap(true, Ordering::Relaxed) {
                    warn!("kafka publisher missing producer instance");
                }
                return false;
            }
        };
        let payload = match serde_json::to_vec(&event) {
            Ok(p) => p,
            Err(err) => {
                self.stats.publish_dropped.fetch_add(1, Ordering::Relaxed);
                warn!("kafka publish serialization failed: {}", err);
                return false;
            }
        };
        let key = event.order_id.as_deref().unwrap_or(event.account_id.as_str());
        match producer.send_result(
            FutureRecord::to(&self.topic)
                .payload(&payload)
                .key(key),
        ) {
            Ok(delivery) => {
                self.stats.publish_queued.fetch_add(1, Ordering::Relaxed);
                match futures::executor::block_on(delivery) {
                    Ok(Ok(_)) => {
                        self.stats.publish_delivery_ok.fetch_add(1, Ordering::Relaxed);
                        true
                    }
                    Ok(Err((err, _))) => {
                        self.stats.publish_delivery_err.fetch_add(1, Ordering::Relaxed);
                        warn!("kafka delivery failed: {}", err);
                        false
                    }
                    Err(err) => {
                        self.stats.publish_delivery_err.fetch_add(1, Ordering::Relaxed);
                        warn!("kafka delivery canceled: {}", err);
                        false
                    }
                }
            }
            Err((err, _)) => {
                self.stats.publish_dropped.fetch_add(1, Ordering::Relaxed);
                warn!("kafka publish enqueue failed: {}", err);
                false
            }
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn metrics(&self) -> BusMetrics {
        BusMetrics {
            enabled: self.enabled,
            publish_queued: self.stats.publish_queued.load(Ordering::Relaxed),
            publish_delivery_ok: self.stats.publish_delivery_ok.load(Ordering::Relaxed),
            publish_delivery_err: self.stats.publish_delivery_err.load(Ordering::Relaxed),
            publish_dropped: self.stats.publish_dropped.load(Ordering::Relaxed),
        }
    }
}

pub struct BusMetrics {
    pub enabled: bool,
    pub publish_queued: u64,
    pub publish_delivery_ok: u64,
    pub publish_delivery_err: u64,
    pub publish_dropped: u64,
}

struct BusStats {
    publish_queued: AtomicU64,
    publish_delivery_ok: AtomicU64,
    publish_delivery_err: AtomicU64,
    publish_dropped: AtomicU64,
}

impl BusStats {
    fn new() -> Self {
        Self {
            publish_queued: AtomicU64::new(0),
            publish_delivery_ok: AtomicU64::new(0),
            publish_delivery_err: AtomicU64::new(0),
            publish_dropped: AtomicU64::new(0),
        }
    }
}

fn delivery_loop(rx: mpsc::Receiver<DeliveryFuture>, stats: Arc<BusStats>) {
    for delivery in rx {
        match futures::executor::block_on(delivery) {
            Ok(Ok(_)) => {
                stats.publish_delivery_ok.fetch_add(1, Ordering::Relaxed);
            }
            Ok(Err((err, _))) => {
                stats.publish_delivery_err.fetch_add(1, Ordering::Relaxed);
                warn!("kafka delivery failed: {}", err);
            }
            Err(err) => {
                stats.publish_delivery_err.fetch_add(1, Ordering::Relaxed);
                warn!("kafka delivery canceled: {}", err);
            }
        }
    }
}
