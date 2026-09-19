//! Connection load balancing across shards.
//!
//! Each shard binds its own listener with `SO_REUSEPORT`, which means the
//! *kernel* chooses the owning shard by hashing the connection 4-tuple. For a
//! loopback client the source IP, destination IP and destination port are all
//! constant, so the only varying input is the client's ephemeral source port.
//! Shard assignment is therefore a random ball-into-bins draw, re-rolled on
//! every reconnect.
//!
//! With 64 connections over 16 shards the busiest shard receives ~8 while
//! others receive 1-2, and since throughput is gated by the most loaded shard
//! the server runs at roughly half its ideal capacity. Measured: 2.74M ops/s at
//! 64 connections versus 5.44M at 256 connections on an otherwise identical
//! configuration, matching a Monte-Carlo prediction of 51.9% of ideal.
//!
//! Deferring `accept()` on an overloaded shard does **not** help: `SO_REUSEPORT`
//! commits the connection to a specific listener's accept queue at SYN time, so
//! an unaccepted connection simply waits there rather than being redistributed.
//!
//! This module instead balances explicitly. After accepting, a shard consults
//! the global per-shard connection census and, if it is carrying more than the
//! least loaded shard, hands the raw fd over to that shard. All shards are
//! threads in a single process and share one fd table, so the transfer is just
//! an integer -- no `SCM_RIGHTS` required.
//!
//! # Thread-per-core compliance
//!
//! The atomics here are touched exactly twice per connection: once on accept
//! and once on close. They are **not** on the per-command hot data path, which
//! remains free of shared state.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Upper bound on shards we track. Shards beyond this are simply never chosen
/// as handoff targets; correctness is unaffected.
pub const MAX_TRACKED_SHARDS: usize = 256;

static CONN_COUNTS: [AtomicUsize; MAX_TRACKED_SHARDS] =
    [const { AtomicUsize::new(0) }; MAX_TRACKED_SHARDS];

/// Record that `shard_id` has taken ownership of a connection.
#[inline]
pub fn register_conn(shard_id: usize) {
    if shard_id < MAX_TRACKED_SHARDS {
        CONN_COUNTS[shard_id].fetch_add(1, Ordering::Relaxed);
    }
}

/// Record that a connection owned by `shard_id` has closed.
#[inline]
pub fn unregister_conn(shard_id: usize) {
    if shard_id < MAX_TRACKED_SHARDS {
        // Saturating: a double-unregister must not wrap to usize::MAX and make
        // this shard look permanently idle.
        let _ = CONN_COUNTS[shard_id].fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
            Some(c.saturating_sub(1))
        });
    }
}

/// Current number of connections owned by `shard_id`.
#[inline]
pub fn conn_count(shard_id: usize) -> usize {
    if shard_id < MAX_TRACKED_SHARDS {
        CONN_COUNTS[shard_id].load(Ordering::Relaxed)
    } else {
        0
    }
}

/// Reset the census. Test-only.
#[doc(hidden)]
pub fn reset_counts(num_shards: usize) {
    for c in CONN_COUNTS.iter().take(num_shards.min(MAX_TRACKED_SHARDS)) {
        c.store(0, Ordering::Relaxed);
    }
}

/// Decide whether a freshly accepted connection should be handed to another
/// shard, and if so which one.
///
/// Returns `Some(target)` when `accepting_shard` already owns strictly more
/// connections than the least loaded shard. Ties yield `None` so that a
/// balanced server never moves connections needlessly.
pub fn plan_handoff(accepting_shard: usize, num_shards: usize) -> Option<usize> {
    let tracked = num_shards.min(MAX_TRACKED_SHARDS);
    if tracked < 2 || accepting_shard >= tracked {
        return None;
    }

    let mine = conn_count(accepting_shard);

    let mut best = accepting_shard;
    let mut best_count = mine;
    for shard in 0..tracked {
        let count = conn_count(shard);
        if count < best_count {
            best = shard;
            best_count = count;
        }
    }

    if best != accepting_shard && mine > best_count {
        Some(best)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conn_census_register_and_unregister() {
        reset_counts(8);
        assert_eq!(conn_count(3), 0);

        register_conn(3);
        register_conn(3);
        assert_eq!(conn_count(3), 2);

        unregister_conn(3);
        assert_eq!(conn_count(3), 1);

        // Saturating: must not wrap past zero and look permanently idle.
        unregister_conn(3);
        unregister_conn(3);
        assert_eq!(conn_count(3), 0);

        // Out-of-range shards are ignored rather than panicking.
        register_conn(MAX_TRACKED_SHARDS + 5);
        assert_eq!(conn_count(MAX_TRACKED_SHARDS + 5), 0);
        reset_counts(8);
    }

    #[test]
    fn test_plan_handoff_moves_from_loaded_to_idle_shard() {
        reset_counts(4);

        // Balanced (all zero) -> no handoff, avoids pointless churn.
        assert_eq!(plan_handoff(0, 4), None);

        // Shard 0 loaded, shard 2 idle -> hand off to the least loaded.
        register_conn(0);
        register_conn(0);
        register_conn(1);
        assert_eq!(plan_handoff(0, 4), Some(2));

        // The idle shard itself should keep what it accepts.
        assert_eq!(plan_handoff(2, 4), None);

        // A tie must not trigger a move.
        reset_counts(4);
        register_conn(0);
        register_conn(1);
        register_conn(2);
        register_conn(3);
        assert_eq!(plan_handoff(0, 4), None);

        // Single-shard servers never hand off.
        reset_counts(4);
        register_conn(0);
        assert_eq!(plan_handoff(0, 1), None);

        reset_counts(4);
    }

    #[test]
    fn test_plan_handoff_converges_to_even_distribution() {
        // Simulate the pathological SO_REUSEPORT draw that motivated this
        // module: every connection lands on shard 0. With balancing applied the
        // census must end up even rather than 64-on-one-shard.
        const SHARDS: usize = 16;
        const CONNS: usize = 64;
        reset_counts(SHARDS);

        for _ in 0..CONNS {
            let owner = plan_handoff(0, SHARDS).unwrap_or(0);
            register_conn(owner);
        }

        let counts: Vec<usize> = (0..SHARDS).map(conn_count).collect();
        let total: usize = counts.iter().sum();
        assert_eq!(total, CONNS, "every connection must be accounted for");

        let max = *counts.iter().max().unwrap();
        let min = *counts.iter().min().unwrap();
        assert!(
            max - min <= 1,
            "distribution must be even within 1, got {:?}",
            counts
        );
        assert_eq!(max, CONNS / SHARDS, "expected {} per shard", CONNS / SHARDS);

        reset_counts(SHARDS);
    }
}
