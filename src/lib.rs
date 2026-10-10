#![allow(clippy::type_complexity, clippy::too_many_arguments)]
#![deny(unsafe_op_in_unsafe_fn)]
#![deny(clippy::undocumented_unsafe_blocks)]

pub mod acl;
#[rustfmt::skip]
pub mod acl_categories;
pub mod agent;
pub mod allocator;
pub mod aof;

pub mod block;
pub mod cluster;
#[rustfmt::skip]
pub mod command_docs;
pub mod command_info;
pub mod compact;
pub mod config;
pub mod conn_balance;
pub mod connection;
pub mod crdt;
pub mod geo;
pub mod hll;
pub mod json;
pub mod log;
pub mod mailbox;
pub mod mcp;
pub mod netsec;
pub mod probabilistic;
pub mod pubsub;
pub mod redis_rdb;
pub mod replication;
pub mod resp;
pub mod router;
pub mod scripting;
pub mod search;
pub mod server;
pub mod server_stats;
pub mod shard;
pub mod shutdown;
pub mod slowlog;
pub mod snapshot;
pub mod syscheck;
pub mod table;
pub mod telemetry;
pub mod tiering;
pub mod tls;
pub mod transport;
pub mod vector;
pub mod xdp;
pub mod zerocopy;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
