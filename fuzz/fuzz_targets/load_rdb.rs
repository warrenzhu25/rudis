//! RDB files (copied between hosts, or streamed from a master). The fuzzer
//! supplies the record stream; the harness adds the magic header and a valid
//! CRC64 trailer so inputs get past the checksum into the record parser.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|body: &[u8]| {
    let mut file = b"REDIS0011".to_vec();
    file.extend_from_slice(body);
    let crc = rudis::table::crc64(&file);
    file.extend_from_slice(&crc.to_le_bytes());
    let mut db = rudis::shard::ShardDb::new(0);
    let _ = rudis::table::load_rdb_bytes(&file, &mut db, 0, 1);
});
