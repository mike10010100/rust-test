//! Thread-safe async rate limiter implementing the token bucket algorithm.

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::sleep;

use crate::error::{Result, SchedulerError};

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
        let elapsed = now
            .saturating_duration_since(self.last_refill)
            .as_secs_f64();

        self.last_refill = now;
        let new_tokens = elapsed * self.refill_rate;
        self.tokens = (self.tokens + new_tokens).min(self.capacity);
    }
}

/// A thread-safe, asynchronous rate limiter.
/// It uses a token bucket algorithm to control the rate of job dispatches.
///
/// # Examples
///
/// ```rust
/// use rust_best_practices::limiter::RateLimiter;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let limiter = RateLimiter::try_new(5, 10.0)?;
/// assert!(limiter.try_acquire().await);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct RateLimiter {
    bucket: Arc<Mutex<TokenBucket>>,
}

impl RateLimiter {
    /// Attempts to create a new `RateLimiter` with the given capacity and refill rate.
    ///
    /// # Errors
    /// Returns `SchedulerError::InvalidConfig` if `capacity == 0`, `refill_rate <= 0.0`, or `refill_rate` is NaN.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rust_best_practices::limiter::RateLimiter;
    ///
    /// let limiter = RateLimiter::try_new(10, 2.5);
    /// assert!(limiter.is_ok());
    ///
    /// let invalid = RateLimiter::try_new(0, 1.0);
    /// assert!(invalid.is_err());
    /// ```
    #[allow(clippy::cast_precision_loss)]
    pub fn try_new(capacity: usize, refill_rate: f64) -> Result<Self> {
        if capacity == 0 {
            return Err(SchedulerError::InvalidConfig(
                "Capacity must be greater than zero".to_string(),
            ));
        }

        if refill_rate <= 0.0 || refill_rate.is_nan() {
            return Err(SchedulerError::InvalidConfig(
                "Refill rate must be positive and non-NaN".to_string(),
            ));
        }

        Ok(Self {
            bucket: Arc::new(Mutex::new(TokenBucket {
                capacity: capacity as f64,
                refill_rate,
                tokens: capacity as f64,
                last_refill: Instant::now(),
            })),
        })
    }

    /// Creates a new `RateLimiter` with the given capacity and refill rate.
    ///
    /// # Panics
    /// Panics if `capacity` is 0 or `refill_rate` is <= 0.0. For a non-panicking constructor,
    /// prefer [`RateLimiter::try_new`].
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
    ///
    /// # Cancellation Safety
    /// This method is **cancellation safe**. If cancelled while waiting in `sleep()`,
    /// no tokens are deducted from the bucket and the lock guard is already dropped.
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

            log::trace!("Rate limit reached, sleeping {wait_secs}s before retry");

            // Drop the mutex guard before sleeping so other tasks are not blocked from checking/acquiring.
            drop(bucket);
            sleep(wait_duration).await;
        }
    }

    /// Checks if a token is available immediately without blocking.
    /// Returns `true` and consumes a token if available, `false` otherwise.
    ///
    /// # Cancellation Safety
    /// This method is **cancellation safe**.
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
        assert!(
            tokio::time::timeout(Duration::from_millis(100), limiter.acquire())
                .await
                .is_ok()
        );
        // First acquire should be immediate
        assert!(start.elapsed() < Duration::from_millis(50));

        // Second acquire must wait for refill (approx 100ms)
        assert!(
            tokio::time::timeout(Duration::from_millis(250), limiter.acquire())
                .await
                .is_ok()
        );
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
        assert!(
            tokio::time::timeout(Duration::from_millis(250), limiter.acquire())
                .await
                .is_ok()
        );
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_millis(40));
        assert!(elapsed < Duration::from_millis(100));
    }
}
