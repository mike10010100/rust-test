//! Thread-safe async rate limiter implementing the token bucket algorithm.

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::sleep;

#[derive(Debug)]
struct TokenBucket {
    capacity: f64,
    refill_rate: f64, // tokens per second
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.last_refill).as_secs_f64();

        self.last_refill = now;
        let new_tokens = elapsed * self.refill_rate;
        self.tokens = (self.tokens + new_tokens).min(self.capacity);
    }
}


/// A thread-safe, asynchronous rate limiter.
/// It uses a token bucket algorithm to control the rate of job dispatches.
#[derive(Debug, Clone)]
pub struct RateLimiter {
    bucket: Arc<Mutex<TokenBucket>>,
}

impl RateLimiter {
    /// Creates a new `RateLimiter` with the given capacity and refill rate.
    ///
    /// # Panics
    /// Panics if `capacity` is 0 or `refill_rate` is <= 0.0.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn new(capacity: usize, refill_rate: f64) -> Self {
        assert!(capacity > 0, "Capacity must be greater than zero");

        assert!(refill_rate > 0.0, "Refill rate must be positive");

        Self {
            bucket: Arc::new(Mutex::new(TokenBucket {
                capacity: capacity as f64,
                refill_rate,
                tokens: capacity as f64,
                last_refill: Instant::now(),
            })),
        }
    }

    /// Asynchronously acquires a token.
    /// If no tokens are available, it sleeps until a token is refilled.
    pub async fn acquire(&self) {
        loop {
            let mut bucket = self.bucket.lock().await;
            bucket.refill();

            if bucket.tokens >= 1.0 {
                bucket.tokens -= 1.0;
                return;
            }

            // Calculate duration to wait for at least one token
            let needed = 1.0 - bucket.tokens;
            let wait_secs = needed / bucket.refill_rate;
            // Floor at a minimum sleep of 1 millisecond to prevent tight-looping
            let wait_duration = Duration::from_secs_f64(wait_secs).max(Duration::from_millis(1));

            // Drop the mutex guard before sleeping so other tasks are not blocked from checking/acquiring.
            drop(bucket);
            sleep(wait_duration).await;
        }
    }

    /// Checks if a token is available immediately without blocking.
    /// Returns `true` and consumes a token if available, `false` otherwise.
    pub async fn try_acquire(&self) -> bool {
        let mut bucket = self.bucket.lock().await;
        bucket.refill();

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rate_limiter_acquire() {
        let limiter = RateLimiter::new(1, 10.0);

        let start = Instant::now();
        limiter.acquire().await;
        // First acquire should be immediate
        assert!(start.elapsed() < Duration::from_millis(50));

        // Second acquire must wait for refill (approx 100ms)
        limiter.acquire().await;
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(90));
        assert!(elapsed < Duration::from_millis(150));
    }

    #[tokio::test]
    async fn test_rate_limiter_fractional_tokens() {
        let limiter = RateLimiter::new(2, 10.0);

        // 1. Consume 1 token immediately (1 left)
        assert!(limiter.try_acquire().await);

        // 2. Wait for 50ms (refills 0.5 tokens -> 1.5 left)
        sleep(Duration::from_millis(50)).await;

        // 3. Consume 1 token (0.5 left)
        assert!(limiter.try_acquire().await);

        // 4. Block to acquire another token. Since we have 0.5 tokens left, we need 0.5 more.
        // Wait duration should be 0.5 / 10.0 = 50ms.
        // If mutated to (+), needed = 1.0 + 0.5 = 1.5 tokens (150ms wait).
        let start = Instant::now();
        limiter.acquire().await;
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_millis(40));
        assert!(elapsed < Duration::from_millis(100));
    }
}


