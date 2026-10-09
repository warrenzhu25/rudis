# Rudis patches to monoio 0.2.4

Vendored from crates.io `monoio-0.2.4` (MIT OR Apache-2.0, license files kept).
Every change from upstream is listed here, newest last. Each is marked
`Rudis patch` in the source.

1. **Drain foreign wakers on submit** (`driver/uring/mod.rs`, `Driver::submit`).
   Upstream runs wakes queued by other threads only when the thread parks, so
   a thread that always has local work delayed every cross-thread wake (for
   Rudis: every cross-shard reply) until its run queue drained. The legacy
   driver already did this (its `submit` is a zero-timeout park).
2. **Idle poll** (`set_idle_poll_us` / `idle_poll_us` in `lib.rs`,
   `IDLE_POLL_US` in `driver/mod.rs`, `UringDriver::idle_poll`). Before
   sleeping, an io_uring thread busy-polls for up to the configured window
   for a foreign waker or a completion. It stays "awake" meanwhile, so other
   threads' wakes skip the eventfd write and the work starts without a kernel
   wake-up. Pending task_work (`IORING_SQ_TASKRUN`, with `TASKRUN_FLAG`) and
   a 20 µs fallback tick enter the kernel without waiting so socket
   completions are never starved. Work the tasks queued (e.g. a reply's write)
   is submitted before polling starts. Default 0 = upstream behaviour.
