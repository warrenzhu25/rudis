//! Cluster-bus lines from a peer (MEET, PING/PONG gossip, FAIL, votes,
//! manual failover, ...). Bus peers are unauthenticated, so any bytes may
//! arrive; applying them must never panic.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let hub = rudis::cluster::ClusterHub::new(17000);
    for line in String::from_utf8_lossy(data).lines() {
        let _ = rudis::cluster::handle_cluster_bus_line(line, &hub);
    }
    // The node table it built must still render.
    let _ = hub.cluster_nodes();
});
