#![allow(clippy::type_complexity, clippy::too_many_arguments)]

pub mod acl;
pub mod allocator;
pub mod aof;

pub mod block;
pub mod cluster;
pub mod connection;
pub mod crdt;
pub mod geo;
pub mod json;
pub mod mailbox;
pub mod probabilistic;
pub mod pubsub;
pub mod replication;
pub mod resp;
pub mod router;
pub mod scripting;
pub mod search;
pub mod server;
pub mod shard;
pub mod shutdown;
pub mod table;
pub mod tiering;
pub mod tls;
pub mod vector;
pub mod xdp;
pub mod zerocopy;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
