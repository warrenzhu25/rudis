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

/// Scan for the least loaded shard, starting from the accepting shard's own
/// count. Returns `(shard, count)` as observed; the values may be stale the
/// instant they are returned.
#[inline]
fn least_loaded(accepting_shard: usize, tracked: usize) -> (usize, usize) {
    let mut best = accepting_shard;
    let mut best_count = conn_count(accepting_shard);
    for shard in 0..tracked {
        let count = conn_count(shard);
        if count < best_count {
            best = shard;
            best_count = count;
        }
    }
    (best, best_count)
}

/// Decide whether a freshly accepted connection should be handed to another
/// shard, and if so which one.
///
/// Returns `Some(target)` when `accepting_shard` already owns strictly more
/// connections than the least loaded shard. Ties yield `None` so that a
/// balanced server never moves connections needlessly.
///
/// This is the pure decision function. Callers that are actually placing a
/// connection should use [`claim_owner`], which performs the same decision but
/// reserves the slot atomically.
pub fn plan_handoff(accepting_shard: usize, num_shards: usize) -> Option<usize> {
    let tracked = num_shards.min(MAX_TRACKED_SHARDS);
    if tracked < 2 || accepting_shard >= tracked {
        return None;
    }
    let (best, best_count) = least_loaded(accepting_shard, tracked);
    if best != accepting_shard && conn_count(accepting_shard) > best_count {
        Some(best)
    } else {
        None
    }
}

/// Number of times [`claim_owner`] re-scans before giving up and keeping the
/// connection locally. Bounds the retry loop so a hot census cannot livelock
/// the accept path; keeping one connection on a slightly busier shard is a far
/// cheaper outcome than stalling accepts.
const MAX_CLAIM_RETRIES: usize = 4;

/// Choose the shard that will own a freshly accepted connection **and reserve
/// its census slot**, returning that shard.
///
/// The reservation is the whole point. During a client's connection burst every
/// shard is accepting in parallel, so if the count only rose once the target
/// adopted the connection, the target would appear idle for the entire in-flight
/// window and every other shard would independently choose it, reproducing the
/// imbalance this module exists to remove.
///
/// The claim is a single `compare_exchange` against the count that was observed
/// during the scan, so two shards racing for the same target cannot both win:
/// the loser sees a stale value, re-scans, and picks the next least loaded
/// shard.
///
/// The caller owns the returned reservation and must eventually call
/// [`unregister_conn`] for that shard, whether the connection runs to completion
/// or is abandoned.
pub fn claim_owner(accepting_shard: usize, num_shards: usize) -> usize {
    let tracked = num_shards.min(MAX_TRACKED_SHARDS);
    if tracked < 2 || accepting_shard >= tracked {
        register_conn(accepting_shard);
        return accepting_shard;
    }

    for _ in 0..MAX_CLAIM_RETRIES {
        let (best, best_count) = least_loaded(accepting_shard, tracked);
        if best == accepting_shard {
            break;
        }
        if CONN_COUNTS[best]
            .compare_exchange(
                best_count,
                best_count + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            return best;
        }
    }

    register_conn(accepting_shard);
    accepting_shard
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The census is process-global state, but cargo runs tests in parallel by
    /// default. Every test here mutates it, so they must be serialized against
    /// each other or they will observe one another's registrations.
    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Takes the serialization lock, ignoring poisoning: an earlier test
    /// panicking on an assertion must not cascade into spurious failures here.
    fn lock_census() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn test_conn_census_register_and_unregister() {
        let _guard = lock_census();
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
        let _guard = lock_census();
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
        let _guard = lock_census();
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

    #[test]
    fn test_claim_owner_reserves_slot_so_serial_placement_is_even() {
        // claim_owner both decides and registers, so the caller never has to
        // add to the census itself.
        let _guard = lock_census();
        const SHARDS: usize = 8;
        reset_counts(SHARDS);

        for _ in 0..32 {
            claim_owner(0, SHARDS);
        }

        let counts: Vec<usize> = (0..SHARDS).map(conn_count).collect();
        assert_eq!(counts.iter().sum::<usize>(), 32);
        assert!(
            counts.iter().all(|&c| c == 4),
            "claim_owner must place evenly, got {:?}",
            counts
        );

        reset_counts(SHARDS);
    }

    #[test]
    fn test_claim_owner_reservation_is_visible_immediately() {
        // This is the invariant whose absence crippled the first version of
        // this module. There, the accept loop only decided on a target and the
        // target incremented its own count when it later dequeued the handoff
        // message. Between those two points the connection was invisible, so
        // during a burst every shard saw the same idle target and piled onto
        // it.
        //
        // The fix is that the census must account for the connection the
        // instant placement is decided. Asserting that here means the deferred
        // scheme cannot be reintroduced without this test failing, which a
        // throughput-style test cannot guarantee.
        let _guard = lock_census();
        const SHARDS: usize = 8;
        reset_counts(SHARDS);

        // Make shard 0 the busiest so the claim is genuinely a handoff to a
        // different shard, which is the case that used to defer registration.
        register_conn(0);
        let total_before: usize = (0..SHARDS).map(conn_count).sum();

        let owner = claim_owner(0, SHARDS);
        assert_ne!(owner, 0, "loaded shard should hand off");

        let total_after: usize = (0..SHARDS).map(conn_count).sum();
        assert_eq!(
            total_after,
            total_before + 1,
            "claim_owner must reserve before returning, not on adoption"
        );
        assert_eq!(
            conn_count(owner),
            1,
            "the reservation must land on the returned shard"
        );

        // A second claim must therefore see the first one and pick elsewhere.
        let owner2 = claim_owner(0, SHARDS);
        assert_ne!(
            owner2, owner,
            "a pending claim must not be invisible to the next placement"
        );

        reset_counts(SHARDS);
    }

    #[test]
    fn test_claim_owner_is_race_free_under_concurrent_accepts() {
        // Concurrency stress: all shards accept in parallel during a client's
        // connection burst, so claims race. This checks that the
        // compare_exchange placement never loses or double counts a claim and
        // still lands evenly under contention.
        //
        // Note on scope: this test was checked against the buggy split
        // (plan_handoff then register_conn) and still passed, because in a
        // tight loop the gap between the two is nanoseconds. The window that
        // actually mattered in production was cross-thread and far wider. The
        // guard against that is
        // test_claim_owner_reservation_is_visible_immediately, which asserts
        // the ordering property directly rather than trying to lose a race.
        let _guard = lock_census();
        const SHARDS: usize = 16;
        const PER_THREAD: usize = 16;
        reset_counts(SHARDS);

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(SHARDS));
        let handles: Vec<_> = (0..SHARDS)
            .map(|accepting| {
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    // Release all threads at once to maximise the overlap.
                    barrier.wait();
                    for _ in 0..PER_THREAD {
                        claim_owner(accepting, SHARDS);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let counts: Vec<usize> = (0..SHARDS).map(conn_count).collect();
        let total: usize = counts.iter().sum();
        assert_eq!(
            total,
            SHARDS * PER_THREAD,
            "no claim may be lost or double counted, got {:?}",
            counts
        );

        // Every claim is a reservation, so nothing can pile onto one shard even
        // though the placements are concurrent.
        let max = *counts.iter().max().unwrap();
        let min = *counts.iter().min().unwrap();
        assert!(
            max - min <= 1,
            "concurrent claims must stay balanced, got {:?} (max {}, min {})",
            counts,
            max,
            min
        );

        reset_counts(SHARDS);
    }
}
