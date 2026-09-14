pub mod acl;
pub mod aof;
pub mod block;
pub mod cluster;
pub mod connection;
pub mod pubsub;
pub mod replication;
pub mod resp;
pub mod router;
pub mod scripting;
pub mod server;
pub mod shard;
pub mod table;
pub mod tiering;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
