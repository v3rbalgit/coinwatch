//! A weight-aware sliding-window rate limiter for Hyperliquid REST calls.
//!
//! Hyperliquid caps REST usage at a fixed request *weight* per rolling minute per
//! IP (1200/min). Each call declares its weight (`candleSnapshot` ≈ 20 + items/60,
//! most other info requests 20); [`WeightLimiter::acquire`] blocks until admitting
//! the call keeps the trailing-window total within budget. All REST callers share
//! one limiter, so the budget is honored globally rather than eyeballed per request.

use std::collections::VecDeque;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

/// Sliding-window weight limiter shared across REST callers.
#[derive(Debug)]
pub(crate) struct WeightLimiter {
    /// Maximum weight admitted per `window`.
    budget: u32,
    /// Rolling window Hyperliquid measures weight over.
    window: Duration,
    /// `(admitted_at, weight)` for calls still inside the window, oldest first.
    events: Mutex<VecDeque<(Instant, u32)>>,
}

impl WeightLimiter {
    /// A limiter admitting up to `budget` weight per rolling minute.
    pub(crate) const fn new(budget: u32) -> Self {
        Self::with_window(budget, Duration::from_secs(60))
    }

    /// A limiter with an explicit window (used by tests).
    pub(crate) const fn with_window(budget: u32, window: Duration) -> Self {
        Self { budget, window, events: Mutex::new(VecDeque::new()) }
    }

    /// Block until admitting `weight` keeps the trailing-window total within budget,
    /// then record it. A single call heavier than the whole budget is admitted only
    /// once the window is otherwise clear (it can never fit alongside anything).
    pub(crate) async fn acquire(&self, weight: u32) {
        loop {
            let wait = {
                let mut events = self.events.lock().unwrap_or_else(PoisonError::into_inner);
                let now = Instant::now();
                prune(&mut events, now, self.window);
                let used: u32 = events.iter().map(|&(_, w)| w).sum();
                if used == 0 || used + weight <= self.budget {
                    events.push_back((now, weight));
                    return;
                }
                wait_until_room(&events, used, weight, self.budget, self.window, now)
            };
            tokio::time::sleep(wait).await;
        }
    }
}

/// Drop events that have aged out of the window.
fn prune(events: &mut VecDeque<(Instant, u32)>, now: Instant, window: Duration) {
    while let Some(&(at, _)) = events.front() {
        if now.duration_since(at) >= window {
            events.pop_front();
        } else {
            break;
        }
    }
}

/// How long until enough of the oldest events expire to admit `weight` within budget.
fn wait_until_room(
    events: &VecDeque<(Instant, u32)>,
    used: u32,
    weight: u32,
    budget: u32,
    window: Duration,
    now: Instant,
) -> Duration {
    let need = (used + weight).saturating_sub(budget);
    let mut freed = 0u32;
    for &(at, w) in events {
        freed += w;
        if freed >= need {
            return window.saturating_sub(now.duration_since(at)).max(Duration::from_millis(1));
        }
    }
    window
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::WeightLimiter;

    #[tokio::test]
    async fn admits_within_budget_without_waiting() {
        let limiter = WeightLimiter::with_window(100, Duration::from_millis(200));
        let start = Instant::now();
        for _ in 0..5 {
            limiter.acquire(20).await; // 5 x 20 == budget
        }
        assert!(start.elapsed() < Duration::from_millis(50), "in-budget calls must not block");
    }

    #[tokio::test]
    async fn paces_once_the_budget_is_spent() {
        let limiter = WeightLimiter::with_window(100, Duration::from_millis(200));
        limiter.acquire(100).await; // fills the window
        let start = Instant::now();
        limiter.acquire(1).await; // must wait for the window to clear
        assert!(start.elapsed() >= Duration::from_millis(150), "over-budget call must wait a window");
    }
}
