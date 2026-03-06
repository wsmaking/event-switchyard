//! Bus event publisher.
//!
//! - `kafka-bus` feature ON: Kafka best-effort publish.
//! - feature OFF (default): no-op publisher for hot-path focused builds.

use chrono::{SecondsFormat, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::warn;

#[cfg(feature = "kafka-bus")]
use rdkafka::ClientConfig;
#[cfg(feature = "kafka-bus")]
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
#[cfg(feature = "kafka-bus")]
use std::sync::Mutex;
#[cfg(feature = "kafka-bus")]
use std::sync::atomic::AtomicBool;
#[cfg(feature = "kafka-bus")]
use std::sync::mpsc;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BusEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub at: String,
    pub account_id: String,
    pub order_id: Option<String>,
    pub data: Value,
}

pub fn format_event_time(epoch_ms: u64) -> String {
    let secs = (epoch_ms / 1000) as i64;
    let nsec = ((epoch_ms % 1000) * 1_000_000) as u32;
    let dt = Utc
        .timestamp_opt(secs, nsec)
        .single()
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).unwrap());
    dt.to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::BusEvent;
    use std::fs;
    use std::path::PathBuf;

    fn find_fixture(rel: &str) -> PathBuf {
        let mut dir = std::env::current_dir().expect("cwd");
        for _ in 0..6 {
            let candidate = dir.join(rel);
            if candidate.exists() {
                return candidate;
            }
            if !dir.pop() {
                break;
            }
        }
        panic!("fixture not found: {rel}");
    }

    #[test]
    fn bus_event_fixture_deserializes() {
        let path = find_fixture("contracts/fixtures/bus_event_v1.json");
        let raw = fs::read_to_string(path).expect("read fixture");
        let parsed: BusEvent = serde_json::from_str(&raw).expect("deserialize");
        assert_eq!(parsed.event_type, "OrderAccepted");
        assert_eq!(parsed.account_id, "acct-1");
        assert_eq!(parsed.at, "2025-01-01T00:00:00Z");
    }
}

pub struct BusPublisher {
    enabled: bool,
    stats: Arc<BusStats>,
    #[cfg(feature = "kafka-bus")]
    kafka: KafkaRuntime,
}

#[cfg(feature = "kafka-bus")]
struct KafkaRuntime {
    topic: String,
    producer: Mutex<Option<FutureProducer>>,
    delivery_tx: Option<mpsc::Sender<DeliveryFuture>>,
    warned_missing_producer: AtomicBool,
}

impl BusPublisher {
    pub fn from_env() -> anyhow::Result<Self> {
        let requested_enabled = std::env::var("KAFKA_ENABLE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let stats = Arc::new(BusStats::new());

        #[cfg(feature = "kafka-bus")]
        {
            let bootstrap = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
                .unwrap_or_else(|_| "localhost:9092".into());
            let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "events".into());
            let client_id =
                std::env::var("KAFKA_CLIENT_ID").unwrap_or_else(|_| "gateway-rust".into());

            let producer = if requested_enabled {
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

            let delivery_tx = if requested_enabled {
                let (tx, rx) = mpsc::channel::<DeliveryFuture>();
                let stats_thread = Arc::clone(&stats);
                std::thread::spawn(move || delivery_loop(rx, stats_thread));
                Some(tx)
            } else {
                None
            };

            return Ok(Self {
                enabled: requested_enabled,
                stats,
                kafka: KafkaRuntime {
                    topic,
                    producer: Mutex::new(producer),
                    delivery_tx,
                    warned_missing_producer: AtomicBool::new(false),
                },
            });
        }

        #[cfg(not(feature = "kafka-bus"))]
        {
            if requested_enabled {
                warn!(
                    "KAFKA_ENABLE=true but binary was built without 'kafka-bus' feature; bus disabled"
                );
            }
            Ok(Self {
                enabled: false,
                stats,
            })
        }
    }

    #[cfg(test)]
    pub(crate) fn disabled_for_test() -> Self {
        let stats = Arc::new(BusStats::new());

        #[cfg(feature = "kafka-bus")]
        {
            return Self {
                enabled: false,
                stats,
                kafka: KafkaRuntime {
                    topic: "events".into(),
                    producer: Mutex::new(None),
                    delivery_tx: None,
                    warned_missing_producer: AtomicBool::new(false),
                },
            };
        }

        #[cfg(not(feature = "kafka-bus"))]
        {
            Self {
                enabled: false,
                stats,
            }
        }
    }

    pub fn publish(&self, event: BusEvent) {
        if !self.enabled {
            self.stats.publish_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }

        #[cfg(feature = "kafka-bus")]
        {
            self.publish_kafka(event);
            return;
        }

        #[cfg(not(feature = "kafka-bus"))]
        {
            let _ = event;
            self.stats.publish_dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn publish_blocking(&self, event: BusEvent) -> bool {
        if !self.enabled {
            self.stats.publish_dropped.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        #[cfg(feature = "kafka-bus")]
        {
            return self.publish_blocking_kafka(event);
        }

        #[cfg(not(feature = "kafka-bus"))]
        {
            let _ = event;
            self.stats.publish_dropped.fetch_add(1, Ordering::Relaxed);
            false
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

    #[cfg(feature = "kafka-bus")]
    fn publish_kafka(&self, event: BusEvent) {
        let mut guard = match self.kafka.producer.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let producer = match guard.as_mut() {
            Some(p) => p,
            None => {
                self.stats.publish_dropped.fetch_add(1, Ordering::Relaxed);
                if !self
                    .kafka
                    .warned_missing_producer
                    .swap(true, Ordering::Relaxed)
                {
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
        let key = event
            .order_id
            .as_deref()
            .unwrap_or(event.account_id.as_str());

        match producer.send_result(
            FutureRecord::to(&self.kafka.topic)
                .payload(&payload)
                .key(key),
        ) {
            Ok(delivery) => {
                self.stats.publish_queued.fetch_add(1, Ordering::Relaxed);
                if let Some(tx) = &self.kafka.delivery_tx {
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

    #[cfg(feature = "kafka-bus")]
    fn publish_blocking_kafka(&self, event: BusEvent) -> bool {
        let mut guard = match self.kafka.producer.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let producer = match guard.as_mut() {
            Some(p) => p,
            None => {
                self.stats.publish_dropped.fetch_add(1, Ordering::Relaxed);
                if !self
                    .kafka
                    .warned_missing_producer
                    .swap(true, Ordering::Relaxed)
                {
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
        let key = event
            .order_id
            .as_deref()
            .unwrap_or(event.account_id.as_str());

        match producer.send_result(
            FutureRecord::to(&self.kafka.topic)
                .payload(&payload)
                .key(key),
        ) {
            Ok(delivery) => {
                self.stats.publish_queued.fetch_add(1, Ordering::Relaxed);
                match futures::executor::block_on(delivery) {
                    Ok(Ok(_)) => {
                        self.stats
                            .publish_delivery_ok
                            .fetch_add(1, Ordering::Relaxed);
                        true
                    }
                    Ok(Err((err, _))) => {
                        self.stats
                            .publish_delivery_err
                            .fetch_add(1, Ordering::Relaxed);
                        warn!("kafka delivery failed: {}", err);
                        false
                    }
                    Err(err) => {
                        self.stats
                            .publish_delivery_err
                            .fetch_add(1, Ordering::Relaxed);
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

#[cfg(feature = "kafka-bus")]
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
