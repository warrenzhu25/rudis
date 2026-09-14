pub mod aof;
pub mod connection;
pub mod pubsub;
pub mod resp;
pub mod router;
pub mod server;
pub mod shard;
pub mod table;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
