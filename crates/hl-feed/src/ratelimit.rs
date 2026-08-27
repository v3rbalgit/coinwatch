//! Weight-aware sliding-window limiter for Hyperliquid's per-IP REST budget.

use std::collections::VecDeque;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use tokio::time::Instant;

const WINDOW: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub(crate) struct WeightLimiter {
    budget: u32,
    events: Mutex<VecDeque<(Instant, u32)>>,
}

impl WeightLimiter {
    pub(crate) const fn new(budget: u32) -> Self {
        Self {
            budget,
            events: Mutex::new(VecDeque::new()),
        }
    }

    /// Block until admitting `weight` keeps the trailing-window total within budget,
    /// then record it. A single call heavier than the whole budget is admitted once
    /// the window is otherwise clear.
    pub(crate) async fn acquire(&self, weight: u32) {
        loop {
            let wait = {
                let mut events = self.events.lock().unwrap_or_else(PoisonError::into_inner);
                let now = Instant::now();
                prune(&mut events, now);
                let used: u32 = events.iter().map(|&(_, w)| w).sum();
                if used == 0 || used.saturating_add(weight) <= self.budget {
                    events.push_back((now, weight));
                    return;
                }
                wait_until_room(&events, used, weight, self.budget, now)
            };
            tokio::time::sleep(wait).await;
        }
    }
}

fn prune(events: &mut VecDeque<(Instant, u32)>, now: Instant) {
    while let Some(&(at, _)) = events.front() {
        if now.duration_since(at) >= WINDOW {
            events.pop_front();
        } else {
            break;
        }
    }
}

fn wait_until_room(events: &VecDeque<(Instant, u32)>, used: u32, weight: u32, budget: u32, now: Instant) -> Duration {
    let need = used.saturating_add(weight).saturating_sub(budget);
    let mut freed = 0u32;
    for &(at, w) in events {
        freed = freed.saturating_add(w);
        if freed >= need {
            return WINDOW.saturating_sub(now.duration_since(at)).max(Duration::from_millis(1));
        }
    }
    WINDOW
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::WeightLimiter;

    #[tokio::test(start_paused = true)]
    async fn admits_within_budget_without_waiting() {
        let limiter = WeightLimiter::new(100);
        let start = tokio::time::Instant::now();
        for _ in 0..5 {
            limiter.acquire(20).await;
        }
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn paces_once_the_budget_is_spent() {
        let limiter = WeightLimiter::new(100);
        limiter.acquire(100).await;
        let start = tokio::time::Instant::now();
        limiter.acquire(1).await;
        assert!(start.elapsed() >= Duration::from_secs(59));
    }
}
