pub mod connection;
pub mod resp;
pub mod router;
pub mod server;
pub mod shard;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
