//! RESTORE payload bodies (client controlled). Must never panic, and
//! whatever is accepted must re-serialize.
#![no_main]

use libfuzzer_sys::fuzz_target;
use rudis::table::RudisTable;

fuzz_target!(|data: &[u8]| {
    if let Ok((val, used)) = RudisTable::deserialize_val_payload(data) {
        assert!(used <= data.len());
        let mut out = Vec::new();
        RudisTable::serialize_val_payload(&val, &mut out);
    }
});
