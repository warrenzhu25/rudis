//! RESTORE payload bodies (client controlled). Must never panic, and
//! whatever is accepted must re-serialize.
#![no_main]

use libfuzzer_sys::fuzz_target;
use rudis::table::RudisTable;

fuzz_target!(|data: &[u8]| {
    // Redis/Valkey encoding (what DUMP writes and RESTORE tries first).
    if let Ok(decoded) = rudis::redis_rdb::decode_dump_body(data, 0) {
        let dump = rudis::redis_rdb::dump_value(&decoded.value).expect("in-memory values dump");
        assert!(dump.len() >= 10);
    }
    let mut r = rudis::redis_rdb::Reader::new(data);
    if let Ok(t) = r.u8() {
        let _ = rudis::redis_rdb::skip_value(t, &mut r);
    }
    // Legacy Rudis encoding (RESTORE's fallback).
    if let Ok((val, used)) = RudisTable::deserialize_val_payload(data) {
        assert!(used <= data.len());
        let mut out = Vec::new();
        RudisTable::serialize_val_payload(&val, &mut out);
    }
});
