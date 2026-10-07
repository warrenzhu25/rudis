//! Client bytes through the connection framer (RESP arrays, inline commands
//! and memcached text). Must never panic, and every parsed command must
//! consume input (otherwise the connection loop would spin).
#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut buf = BytesMut::from(data);
    loop {
        let before = buf.len();
        match rudis::resp::parse_command(&mut buf) {
            Ok(Some(_)) => assert!(buf.len() < before, "parsed without consuming input"),
            Ok(None) | Err(_) => break,
        }
    }
});
