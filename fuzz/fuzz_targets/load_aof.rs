//! AOF files at startup (a damaged disk, a truncated write, a file copied
//! between hosts). Replaying any bytes, with `aof-load-truncated` on or
//! off, must never panic: it loads or refuses the file.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let path = std::env::temp_dir().join(format!("rudis-fuzz-{}.aof", std::process::id()));
    std::fs::write(&path, data).unwrap();
    for load_truncated in [true, false] {
        let mut db = rudis::shard::ShardDb::new(0);
        let _ = rudis::aof::replay_aof_with(&path, &mut db, load_truncated);
    }
});
