//! Lock-free SPSC/MPSC ring buffer for order events
//! Based on crossbeam ArrayQueue for cache-line optimized operations

use crossbeam_queue::ArrayQueue;
use std::sync::Arc;

/// Order structure sized to 64 bytes for cache efficiency
/// Fields are ordered to minimize padding
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct Order {
    pub order_id: u64,      // 8 bytes
    pub account_id: u64,    // 8 bytes
    pub price: u64,         // 8 bytes
    pub timestamp_ns: u64,  // 8 bytes
    pub symbol: [u8; 8],    // 8 bytes
    pub qty: u32,           // 4 bytes
    pub side: u8,           // 1 byte
    _padding: [u8; 19],     // 19 bytes -> total 64 bytes
}

impl Order {
    #[inline]
    pub fn new(
        order_id: u64,
        account_id: u64,
        symbol: [u8; 8],
        side: u8,
        qty: u32,
        price: u64,
        timestamp_ns: u64,
    ) -> Self {
        Self {
            order_id,
            account_id,
            price,
            timestamp_ns,
            symbol,
            qty,
            side,
            _padding: [0u8; 19],
        }
    }

    #[inline]
    pub fn is_buy(&self) -> bool {
        self.side == 1
    }
}

/// High-performance order queue using lock-free ring buffer
pub struct FastPathQueue {
    inner: Arc<ArrayQueue<Order>>,
    capacity: usize,
}

impl FastPathQueue {
    /// Create a new queue with specified capacity
    /// Capacity should be power of 2 for optimal performance
    pub fn new(capacity: usize) -> Self {
        // Ensure capacity is power of 2
        let capacity = capacity.next_power_of_two();
        Self {
            inner: Arc::new(ArrayQueue::new(capacity)),
            capacity,
        }
    }

    /// Push an order to the queue
    /// Returns Err if queue is full (back-pressure)
    #[inline]
    pub fn push(&self, order: Order) -> Result<(), Order> {
        self.inner.push(order)
    }

    /// Pop an order from the queue
    /// Returns None if queue is empty
    #[inline]
    pub fn pop(&self) -> Option<Order> {
        self.inner.pop()
    }

    /// Check if queue is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Get current queue length
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Get queue capacity
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Clone the Arc handle for sharing across threads
    pub fn clone_handle(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            capacity: self.capacity,
        }
    }
}

// Thread-safe by design (uses atomic operations internally)
unsafe impl Send for FastPathQueue {}
unsafe impl Sync for FastPathQueue {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_size() {
        assert_eq!(std::mem::size_of::<Order>(), 64);
    }

    #[test]
    fn test_queue_push_pop() {
        let queue = FastPathQueue::new(1024);
        let order = Order::new(1, 100, *b"AAPL\0\0\0\0", 1, 100, 15000, 0);

        assert!(queue.push(order).is_ok());
        assert_eq!(queue.len(), 1);

        let popped = queue.pop().unwrap();
        assert_eq!(popped.order_id, 1);
        assert_eq!(popped.account_id, 100);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_queue_full() {
        let queue = FastPathQueue::new(2);
        let order = Order::default();

        assert!(queue.push(order).is_ok());
        assert!(queue.push(order).is_ok());
        assert!(queue.push(order).is_err()); // Should fail - queue full
    }

    #[test]
    fn test_power_of_two_capacity() {
        let queue = FastPathQueue::new(100);
        assert_eq!(queue.capacity(), 128); // Rounds up to nearest power of 2
    }
}
