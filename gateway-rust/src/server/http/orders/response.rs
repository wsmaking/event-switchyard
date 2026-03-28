use gateway_core::now_nanos;
use std::{cell::RefCell, ops::Deref};

use super::V3HotPathOutcome;

/// v3 入口の即時応答（VOLATILE_ACCEPT）
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::server::http) struct VolatileOrderResponse {
    pub(in crate::server::http) session_id: PooledJsonString,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(in crate::server::http) session_seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(in crate::server::http) attempt_id: Option<PooledJsonString>,
    pub(in crate::server::http) received_at_ns: u64,
    pub(in crate::server::http) status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(in crate::server::http) reason: Option<PooledJsonString>,
}

const V3_JSON_STRING_POOL_MAX_ITEMS: usize = 4096;
const V3_JSON_STRING_POOL_MAX_CAPACITY: usize = 256;

thread_local! {
    static V3_JSON_STRING_POOL: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

fn v3_pool_take_string(min_capacity: usize) -> String {
    V3_JSON_STRING_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        if let Some(mut cached) = pool.pop() {
            if cached.capacity() < min_capacity {
                cached.reserve(min_capacity - cached.capacity());
            }
            cached
        } else {
            String::with_capacity(min_capacity.max(32))
        }
    })
}

fn v3_pool_put_string(mut value: String) {
    if value.capacity() > V3_JSON_STRING_POOL_MAX_CAPACITY {
        return;
    }
    value.clear();
    V3_JSON_STRING_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        if pool.len() < V3_JSON_STRING_POOL_MAX_ITEMS {
            pool.push(value);
        }
    });
}

#[derive(Debug)]
pub(in crate::server::http) struct PooledJsonString {
    inner: Option<String>,
}

impl PooledJsonString {
    pub(in crate::server::http) fn from_str(value: &str) -> Self {
        let mut inner = v3_pool_take_string(value.len());
        inner.push_str(value);
        Self { inner: Some(inner) }
    }

    fn with_prefix_u64(prefix: &str, value: u64) -> Self {
        let mut inner = v3_pool_take_string(prefix.len() + 20);
        inner.push_str(prefix);
        append_u64_decimal(&mut inner, value);
        Self { inner: Some(inner) }
    }

    pub(in crate::server::http) fn as_str(&self) -> &str {
        self.inner.as_deref().unwrap_or("")
    }
}

impl Clone for PooledJsonString {
    fn clone(&self) -> Self {
        Self::from_str(self.as_str())
    }
}

impl Deref for PooledJsonString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl serde::Serialize for PooledJsonString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl Drop for PooledJsonString {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            v3_pool_put_string(inner);
        }
    }
}

fn build_attempt_id(session_seq: u64) -> PooledJsonString {
    PooledJsonString::with_prefix_u64("att_", session_seq)
}

fn append_u64_decimal(out: &mut String, mut value: u64) {
    if value == 0 {
        out.push('0');
        return;
    }
    let mut buf = [0u8; 20];
    let mut pos = buf.len();
    while value > 0 {
        pos -= 1;
        buf[pos] = b'0' + (value % 10) as u8;
        value /= 10;
    }
    for b in &buf[pos..] {
        out.push(*b as char);
    }
}

impl VolatileOrderResponse {
    pub(in crate::server::http) fn from_hotpath(
        session_id: &str,
        outcome: V3HotPathOutcome,
    ) -> Self {
        match outcome.session_seq {
            Some(session_seq) => Self {
                session_id: PooledJsonString::from_str(session_id),
                session_seq: Some(session_seq),
                attempt_id: Some(build_attempt_id(session_seq)),
                received_at_ns: outcome.received_at_ns,
                status: outcome.status_text,
                reason: None,
            },
            None => Self {
                session_id: PooledJsonString::from_str(session_id),
                session_seq: None,
                attempt_id: None,
                received_at_ns: outcome.received_at_ns,
                status: outcome.status_text,
                reason: outcome.reason_text.map(PooledJsonString::from_str),
            },
        }
    }

    pub(in crate::server::http) fn algo_runtime_scheduled(
        session_id: &str,
        received_at_ns: u64,
    ) -> Self {
        Self {
            session_id: PooledJsonString::from_str(session_id),
            session_seq: None,
            attempt_id: None,
            received_at_ns,
            status: "ALGO_RUNTIME_SCHEDULED",
            reason: None,
        }
    }

    pub(in crate::server::http) fn accepted(
        session_id: String,
        session_seq: u64,
        received_at_ns: u64,
    ) -> Self {
        Self {
            session_id: PooledJsonString::from_str(&session_id),
            session_seq: Some(session_seq),
            attempt_id: Some(build_attempt_id(session_seq)),
            received_at_ns,
            status: "VOLATILE_ACCEPT",
            reason: None,
        }
    }

    pub(in crate::server::http) fn rejected(
        session_id: &str,
        status: &'static str,
        reason: &str,
    ) -> Self {
        Self {
            session_id: PooledJsonString::from_str(session_id),
            session_seq: None,
            attempt_id: None,
            received_at_ns: now_nanos(),
            status,
            reason: Some(PooledJsonString::from_str(reason)),
        }
    }

    pub(in crate::server::http) fn session_seq(&self) -> Option<u64> {
        self.session_seq
    }

    pub(in crate::server::http) fn status_text(&self) -> &'static str {
        self.status
    }

    pub(in crate::server::http) fn reason_text(&self) -> Option<&str> {
        self.reason.as_deref()
    }

    pub(in crate::server::http) fn received_at_ns(&self) -> u64 {
        self.received_at_ns
    }
}

/// v3 durable confirm 照会レスポンス。
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::server::http) struct DurableOrderStatusResponse {
    pub(super) session_id: String,
    pub(super) session_seq: u64,
    pub(super) status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) attempt_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) received_at_ns: Option<u64>,
    pub(super) updated_at_ns: u64,
    pub(super) shard_id: u64,
}
