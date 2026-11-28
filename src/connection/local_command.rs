use super::*;

pub fn execute_local_command(
    cmd: &Command,
    db: &mut ShardDb,
    out: &mut Vec<u8>,
    aof: Option<&RefCell<crate::aof::AofWriter>>,
) -> bool {
    db.select_db(CURRENT_DB.get());
    db.hydrate_cmd_keys(cmd);
    if crate::shard::aof_image_active() {
        aof_image_before_command(db, cmd, |_| true);
    }
    // The client this command runs for, 0 for internal work (replication,
    // expiry, eviction); the connection path and shard batches set it.
    let cid = CURRENT_CLIENT_ID.get();
    macro_rules! record_change {
        // `$warn`: whether a change `command_to_resp` cannot encode (and so
        // never reaches the AOF or the replicas) is reported.
        (@log $cmd_expr:expr, $warn:expr) => {{
            let changed: &Command = $cmd_expr;
            if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed)
                || HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed)
            {
                for_each_cmd_key(changed, |k| {
                    notify_key_invalidation(db.port, k, cid);
                });
            }
            log_change(changed, aof, db.port, db.shard_id, $warn);
        }};
        ($cmd_expr:expr) => {
            record_change!(@log $cmd_expr, true)
        };
    }
    // CRDT writes are logged as their effect, a CRDT.MERGE of the changed
    // state: their HLC timestamps (and the counter's node id) would differ
    // if the command itself were replayed. `record_change!` still runs for
    // key invalidation; it logs nothing for these commands.
    macro_rules! record_crdt_effect {
        ($payload:expr) => {
            record_change!(@log cmd, false);
            crate::replication::log_shard_mutation(db.port, db.shard_id, aof, || {
                Command::CrdtMerge(bytes::Bytes::from($payload))
            });
        };
    }
    struct ReadTrackGuard<'a> {
        port: u16,
        cid: u64,
        cmd: &'a Command,
    }
    impl<'a> Drop for ReadTrackGuard<'a> {
        fn drop(&mut self) {
            if self.cid != 0 && HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                for_each_read_key(self.cmd, |k| record_client_read(self.port, self.cid, k));
            }
        }
    }
    let _read_track_guard = ReadTrackGuard {
        port: db.port,
        cid,
        cmd,
    };
    // Tiered values dropped by the command are reclaimed after it, on every
    // exit path; the body is a closure so its early `return`s land here
    // (this used to be a drop guard holding a raw pointer to `db`).
    #[allow(clippy::redundant_closure_call)]
    let ret = (|| -> bool {
        // GET and MGET read through counting table accessors; probe the keys of
        // the other read commands for INFO keyspace_hits / keyspace_misses.
        if !matches!(cmd, Command::Get(_) | Command::Mget(_)) {
            for_each_read_key(cmd, |k| {
                crate::server_stats::note_key_lookup(db.table.key_is_live(k))
            });
        }
        match cmd {
            Command::Get(key) => {
                match db.get_checked(key) {
                    Ok(Some(v)) => {
                        write_resp_bulk(out, &v);
                    }
                    Ok(None) => {
                        write_resp_null(out);
                    }
                    Err(err) => write_resp_err(out, err),
                }
                false
            }
            Command::Getex {
                key,
                expire_in,
                persist,
            } => {
                let val = match db.get_checked(key.as_ref()) {
                    Ok(val) => val,
                    Err(err) => {
                        write_resp_err(out, err);
                        return false;
                    }
                };
                match val {
                    Some(v) => {
                        let ttl_change = if *persist {
                            db.persist(key.as_ref())
                                .then(|| ("persist", Command::Persist(key.clone())))
                        } else if let Some(exp) = expire_in {
                            let opts = crate::resp::ExpireOptions::default();
                            db.expire(key.as_ref(), *exp, opts).then(|| {
                                let cmd = Command::Expire {
                                    key: key.clone(),
                                    duration: *exp,
                                    opts,
                                };
                                ("expire", cmd)
                            })
                        } else {
                            None
                        };
                        if let Some((event, change)) = ttl_change {
                            record_change!(&change);
                            if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                touch_watched_key(db.port, key.as_ref());
                            }
                            if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                                notify_key_invalidation(db.port, key.as_ref(), 0);
                            }
                            notify_keyspace_event(NOTIFY_GENERIC, event, key);
                        }
                        write_resp_bulk(out, &v);
                    }
                    None => {
                        write_resp_null(out);
                    }
                }
                false
            }
            Command::Mget(keys) => {
                write_resp_array_header(out, keys.len());
                for k in keys {
                    match db.get(k) {
                        Some(v) => write_resp_bulk(out, &v),
                        None => write_resp_null(out),
                    }
                }
                false
            }
            Command::Set {
                key,
                value,
                expire_in,
                condition,
                get,
                keepttl,
                past_expired,
            } => {
                // While re-applying writes (AOF load, from the master) nothing
                // expires, like Redis: a past EXAT/PXAT (as a rewritten AOF
                // stores TTLs) keeps the key with its passed deadline instead
                // of deleting it, so later writes to it (PERSIST, APPEND...)
                // apply as they did originally.
                let replay_past = *past_expired && access_expired_allowed();
                let past_expired = &(*past_expired && !replay_past);
                let passed_deadline = Some(std::time::Duration::ZERO);
                let expire_in = if replay_past {
                    &passed_deadline
                } else {
                    expire_in
                };
                // The old value only for GET and the IF* conditions: a plain
                // SET read it too (unpacking it, and counting a keyspace hit
                // or miss as GET does).
                let needs_current = *get
                    || matches!(
                        condition,
                        crate::resp::SetCondition::Ifeq(_)
                            | crate::resp::SetCondition::Ifne(_)
                            | crate::resp::SetCondition::Ifdeq(_)
                            | crate::resp::SetCondition::Ifdne(_)
                    );
                let current_val = if needs_current {
                    match db.table.get(key) {
                        Ok(val) => val,
                        Err(err) => {
                            write_resp_err(out, err);
                            return false;
                        }
                    }
                } else {
                    None
                };

                let exists = db.exists(key);
                let condition_met = match condition {
                    crate::resp::SetCondition::None => true,
                    crate::resp::SetCondition::Nx => !exists,
                    crate::resp::SetCondition::Xx => exists,
                    crate::resp::SetCondition::Ifeq(expected) => {
                        current_val.as_ref() == Some(expected)
                    }
                    crate::resp::SetCondition::Ifne(expected) => match &current_val {
                        None => true,
                        Some(val) => val != expected,
                    },
                    crate::resp::SetCondition::Ifdeq(expected_digest) => match &current_val {
                        None => false,
                        Some(val) => {
                            if expected_digest.len() != 16
                                || !expected_digest.iter().all(|b| b.is_ascii_hexdigit())
                            {
                                write_resp_err(
                                    out,
                                    "ERR digest must be exactly 16 hexadecimal characters",
                                );
                                return false;
                            }
                            let d = crate::table::compute_digest(val);
                            d.eq_ignore_ascii_case(&String::from_utf8_lossy(expected_digest))
                        }
                    },
                    crate::resp::SetCondition::Ifdne(expected_digest) => match &current_val {
                        None => true,
                        Some(val) => {
                            if expected_digest.len() != 16
                                || !expected_digest.iter().all(|b| b.is_ascii_hexdigit())
                            {
                                write_resp_err(
                                    out,
                                    "ERR digest must be exactly 16 hexadecimal characters",
                                );
                                return false;
                            }
                            let d = crate::table::compute_digest(val);
                            !d.eq_ignore_ascii_case(&String::from_utf8_lossy(expected_digest))
                        }
                    },
                };

                if !condition_met {
                    if *get {
                        if let Some(v) = current_val {
                            out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            out.extend_from_slice(&v);
                            out.extend_from_slice(b"\r\n");
                        } else {
                            write_resp_null(out);
                        }
                    } else {
                        write_resp_null(out);
                    }
                    return false;
                }

                if *past_expired {
                    crate::table::inc_expired_keys();
                    if exists {
                        db.del(key);
                        // Logged as the deletion: the SET itself would be
                        // encoded without its (past) expiry, and replicas
                        // and AOF reloads would keep the key for good.
                        record_change!(&server_del_cmd(key));
                    }
                    if *get {
                        if let Some(v) = current_val {
                            out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            out.extend_from_slice(&v);
                            out.extend_from_slice(b"\r\n");
                        } else {
                            write_resp_null(out);
                        }
                    } else {
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    return false;
                }

                let prev_kind = db.type_of(key.as_ref());
                db.set_extended(key.clone(), value.clone(), *expire_in, *keepttl);
                if replay_past {
                    // `now + 0` rounds to the nearest stored millisecond, so
                    // it can sit just in the future; pin it clearly passed.
                    db.table
                        .expire(key.as_ref(), std::time::Duration::ZERO, Default::default());
                }
                record_change!(cmd);
                notify_set_key_events_local(prev_kind, "string", key);
                notify_keyspace_event(NOTIFY_STRING, "set", key);
                notify_stream_or_defer(db, key);
                if expire_in.is_some() {
                    notify_keyspace_event(NOTIFY_GENERIC, "expire", key);
                }

                if *get {
                    if let Some(v) = current_val {
                        out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                        out.extend_from_slice(&v);
                        out.extend_from_slice(b"\r\n");
                    } else {
                        write_resp_null(out);
                    }
                } else {
                    out.extend_from_slice(b"+OK\r\n");
                }
                false
            }
            Command::Mset(pairs) => {
                for (k, v) in pairs {
                    db.set(k.clone(), v.clone(), None);
                }
                record_change!(cmd);
                for (k, _) in pairs {
                    notify_keyspace_event(NOTIFY_STRING, "set", k);
                }
                out.extend_from_slice(b"+OK\r\n");
                false
            }
            Command::Msetex {
                pairs,
                condition,
                expiry,
            } => {
                let pass = match condition {
                    MsetexCondition::None => true,
                    MsetexCondition::Nx => pairs.iter().all(|(k, _)| !db.exists(k)),
                    MsetexCondition::Xx => pairs.iter().all(|(k, _)| db.exists(k)),
                };

                if !pass {
                    out.extend_from_slice(b":0\r\n");
                    return false;
                }

                for (k, v) in pairs {
                    let ttl = match expiry {
                        MsetexExpiry::None => None,
                        MsetexExpiry::KeepTtl => db.get_entry(k).and_then(|(_, exp)| exp),
                        MsetexExpiry::ExpireIn(d) => Some(*d),
                    };
                    db.set(k.clone(), v.clone(), ttl);
                }
                record_change!(cmd);
                match condition {
                    MsetexCondition::None => {
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    MsetexCondition::Nx | MsetexCondition::Xx => {
                        out.extend_from_slice(b":1\r\n");
                    }
                }
                false
            }
            Command::Lcs {
                key1,
                key2,
                len_only,
                idx,
                min_match_len,
                with_match_len,
            } => {
                let val1 = match db.table.get(key1) {
                    Ok(v) => v.unwrap_or_default(),
                    Err(err) => {
                        write_resp_err(out, err);
                        return false;
                    }
                };
                let val2 = match db.table.get(key2) {
                    Ok(v) => v.unwrap_or_default(),
                    Err(err) => {
                        write_resp_err(out, err);
                        return false;
                    }
                };

                let s1 = val1.as_ref();
                let s2 = val2.as_ref();
                let m = s1.len();
                let n = s2.len();
                if let Some(err) = lcs_table_too_big(m, n) {
                    write_resp_err(out, err);
                    return false;
                }

                let mut dp = vec![vec![0u32; n + 1]; m + 1];
                for i in 1..=m {
                    for j in 1..=n {
                        if s1[i - 1] == s2[j - 1] {
                            dp[i][j] = dp[i - 1][j - 1] + 1;
                        } else {
                            dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
                        }
                    }
                }

                let lcs_len = dp[m][n] as usize;

                if *len_only {
                    out.extend_from_slice(format!(":{}\r\n", lcs_len).as_bytes());
                    return false;
                }

                if *idx {
                    let mut matches = Vec::new();
                    let mut i = m;
                    let mut j = n;
                    while i > 0 && j > 0 {
                        if s1[i - 1] == s2[j - 1] {
                            let end1 = i - 1;
                            let end2 = j - 1;
                            while i > 0 && j > 0 && s1[i - 1] == s2[j - 1] {
                                i -= 1;
                                j -= 1;
                            }
                            let start1 = i;
                            let start2 = j;
                            let match_len = end1 - start1 + 1;
                            if match_len >= *min_match_len {
                                matches.push(((start1, end1), (start2, end2), match_len));
                            }
                        } else if dp[i - 1][j] >= dp[i][j - 1] {
                            i -= 1;
                        } else {
                            j -= 1;
                        }
                    }

                    out.extend_from_slice(b"*4\r\n$7\r\nmatches\r\n");
                    out.extend_from_slice(format!("*{}\r\n", matches.len()).as_bytes());
                    for ((s1_idx, e1_idx), (s2_idx, e2_idx), match_len) in matches {
                        if *with_match_len {
                            out.extend_from_slice(
                                format!(
                                    "*3\r\n*2\r\n:{}\r\n:{}\r\n*2\r\n:{}\r\n:{}\r\n:{}\r\n",
                                    s1_idx, e1_idx, s2_idx, e2_idx, match_len
                                )
                                .as_bytes(),
                            );
                        } else {
                            out.extend_from_slice(
                                format!(
                                    "*2\r\n*2\r\n:{}\r\n:{}\r\n*2\r\n:{}\r\n:{}\r\n",
                                    s1_idx, e1_idx, s2_idx, e2_idx
                                )
                                .as_bytes(),
                            );
                        }
                    }
                    out.extend_from_slice(format!("$3\r\nlen\r\n:{}\r\n", lcs_len).as_bytes());
                    return false;
                }

                let mut lcs_bytes = Vec::with_capacity(lcs_len);
                let mut i = m;
                let mut j = n;
                while i > 0 && j > 0 {
                    if s1[i - 1] == s2[j - 1] {
                        lcs_bytes.push(s1[i - 1]);
                        i -= 1;
                        j -= 1;
                    } else if dp[i - 1][j] >= dp[i][j - 1] {
                        i -= 1;
                    } else {
                        j -= 1;
                    }
                }
                lcs_bytes.reverse();
                out.extend_from_slice(format!("${}\r\n", lcs_bytes.len()).as_bytes());
                out.extend_from_slice(&lcs_bytes);
                out.extend_from_slice(b"\r\n");
                false
            }
            Command::Del(keys) | Command::Unlink(keys) => {
                let is_unlink = matches!(cmd, Command::Unlink(_));
                if keys.len() == 1 {
                    let lazy = is_unlink && db.table.is_lazyfree_worthy(&keys[0]);
                    let deleted = db.del(&keys[0]);
                    if deleted {
                        if lazy {
                            crate::table::add_lazyfreed_objects(1);
                        }
                        db.delete_document_local(&String::from_utf8_lossy(&keys[0]));
                        crate::search::delete_document_hook(&String::from_utf8_lossy(&keys[0]));
                        record_change!(cmd);
                        notify_keyspace_event(NOTIFY_GENERIC, "del", &keys[0]);
                        notify_stream_or_defer(db, &keys[0]);
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                    return false;
                }
                let mut count = 0usize;
                for k in keys {
                    let lazy = is_unlink && db.table.is_lazyfree_worthy(k);
                    if db.del(k) {
                        count += 1;
                        if lazy {
                            crate::table::add_lazyfreed_objects(1);
                        }
                        db.delete_document_local(&String::from_utf8_lossy(k));
                        crate::search::delete_document_hook(&String::from_utf8_lossy(k));
                        notify_keyspace_event(NOTIFY_GENERIC, "del", k);
                        notify_stream_or_defer(db, k);
                    }
                }
                if count > 0 {
                    record_change!(cmd);
                }
                write_resp_integer(out, count as i64);
                false
            }
            Command::Exists(keys) => {
                if keys.len() == 1 {
                    let exists = db.exists(&keys[0]);
                    out.extend_from_slice(if exists { b":1\r\n" } else { b":0\r\n" });
                    return false;
                }
                let mut count = 0usize;
                for k in keys {
                    if db.exists(k) {
                        count += 1;
                    }
                }
                write_resp_integer(out, count as i64);
                false
            }
            Command::IncrBy(key, delta, _) => {
                match db.incr_by_slice(key.as_ref(), *delta) {
                    Ok(val) => {
                        record_change!(cmd);
                        notify_keyspace_event(NOTIFY_STRING, "incrby", key);
                        write_resp_integer(out, val);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Expire {
                key,
                duration,
                opts,
            } => {
                let res = db.expire(key, *duration, *opts);
                if res {
                    record_change!(cmd);
                    if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                        touch_watched_key(db.port, key.as_ref());
                    }
                    if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                        notify_key_invalidation(db.port, key.as_ref(), 0);
                    }
                    notify_keyspace_event(NOTIFY_GENERIC, "expire", key);
                    out.extend_from_slice(b":1\r\n");
                } else {
                    out.extend_from_slice(b":0\r\n");
                }
                false
            }
            Command::Persist(key) => {
                let res = db.persist(key);
                if res {
                    record_change!(cmd);
                    if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                        touch_watched_key(db.port, key.as_ref());
                    }
                    if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                        notify_key_invalidation(db.port, key.as_ref(), 0);
                    }
                    notify_keyspace_event(NOTIFY_GENERIC, "persist", key);
                    out.extend_from_slice(b":1\r\n");
                } else {
                    out.extend_from_slice(b":0\r\n");
                }
                false
            }
            Command::Ttl(key, in_millis) => {
                let res = db.ttl(key, *in_millis);
                write_resp_integer(out, res);
                false
            }
            Command::Hset { key, fields } => {
                match db.hset_slice_fast(key, fields) {
                    Ok(count) => {
                        record_change!(cmd);
                        notify_hset_fields(key, fields);
                        if db.has_search_indices() || crate::search::has_active_search_indices() {
                            reindex_hash_for_search(db, key);
                        }
                        write_resp_integer(out, count as i64);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Himport(sub) => {
                // From a script (connections resolve fieldsets first).
                let set = SCRIPT_FIELDSETS.with(|f| himport_session(&mut f.borrow_mut(), sub, out));
                if let Some(set) = set {
                    execute_local_command(&set, db, out, aof);
                }
                false
            }
            Command::HimportSet { key, fields } => {
                let ty = db.type_of(key);
                if ty != "none" && ty != "hash" {
                    out.extend_from_slice(
                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                    return false;
                }
                // Replaces the key, so it goes to the AOF and replicas as
                // DEL + HSET (the fieldset is the client's, not theirs).
                if db.del(key) {
                    record_change!(&Command::Del(smallvec::smallvec![key.clone()]));
                }
                match db.hset_slice_fast(key, fields) {
                    Ok(_) => {
                        let as_hset = Command::Hset {
                            key: key.clone(),
                            fields: fields.clone(),
                        };
                        record_change!(&as_hset);
                        notify_hset_fields(key, fields);
                        if db.has_search_indices() || crate::search::has_active_search_indices() {
                            reindex_hash_for_search(db, key);
                        }
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    Err(err) => write_resp_err(out, err),
                }
                false
            }
            Command::Hsetnx { key, field, value } => {
                match db.hsetnx(key.clone(), field.clone(), value.clone()) {
                    Ok(count) => {
                        if count > 0 {
                            record_change!(cmd);
                            notify_hash_fields_event("hset", key, std::slice::from_ref(field));
                        }
                        write_resp_integer(out, count as i64);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Hmset { key, fields } => {
                match db.hset_slice_fast(key, fields) {
                    Ok(_) => {
                        record_change!(cmd);
                        notify_hset_fields(key, fields);
                        if db.has_search_indices() || crate::search::has_active_search_indices() {
                            reindex_hash_for_search(db, key);
                        }
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Hget { key, field } => {
                match db.hget(key, field) {
                    Ok(Some(v)) => {
                        out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                        out.extend_from_slice(&v);
                        out.extend_from_slice(b"\r\n");
                    }
                    Ok(None) => {
                        write_resp_null(out);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Hmget { key, fields } => {
                match db.hmget(key, fields) {
                    Ok(vals) => {
                        out.extend_from_slice(format!("*{}\r\n", vals.len()).as_bytes());
                        for v in vals {
                            match v {
                                Some(val) => {
                                    out.extend_from_slice(format!("${}\r\n", val.len()).as_bytes());
                                    out.extend_from_slice(&val);
                                    out.extend_from_slice(b"\r\n");
                                }
                                None => {
                                    write_resp_null(out);
                                }
                            }
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Hdel { key, fields } => {
                let (event, deleted) = hdel_event_and_fields(db, key, fields);
                match db.hdel(key, fields) {
                    Ok(count) => {
                        if count > 0 {
                            record_change!(cmd);
                            notify_hash_event_then_del(db, key, event, &deleted);
                        }
                        out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Hexists { key, field } => {
                match db.hexists(key, field) {
                    Ok(exists) => {
                        if exists {
                            out.extend_from_slice(b":1\r\n");
                        } else {
                            out.extend_from_slice(b":0\r\n");
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Hlen(key) => {
                match db.hlen(key) {
                    Ok(len) => {
                        write_resp_integer(out, len as i64);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Hgetall(key) => {
                match db.hgetall(key) {
                    Ok(pairs) => {
                        write_resp_map_len(out, pairs.len());
                        for (k, v) in pairs {
                            out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                            out.extend_from_slice(&k);
                            out.extend_from_slice(b"\r\n");
                            out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            out.extend_from_slice(&v);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Hkeys(key) => {
                match db.hkeys(key) {
                    Ok(keys) => {
                        out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
                        for k in keys {
                            out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                            out.extend_from_slice(&k);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Hvals(key) => {
                match db.hvals(key) {
                    Ok(vals) => {
                        out.extend_from_slice(format!("*{}\r\n", vals.len()).as_bytes());
                        for v in vals {
                            out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            out.extend_from_slice(&v);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Hstrlen { key, field } => {
                match db.hstrlen(key, field) {
                    Ok(len) => {
                        write_resp_integer(out, len as i64);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Hgetdel { key, fields } => {
                match db.hgetdel(key, fields) {
                    Ok((vals, deleted_fields)) => {
                        if !deleted_fields.is_empty() {
                            record_change!(cmd);
                            notify_hash_event_then_del(db, key, "hdel", &deleted_fields);
                        }
                        out.extend_from_slice(format!("*{}\r\n", vals.len()).as_bytes());
                        for v in vals {
                            match v {
                                Some(val) => {
                                    out.extend_from_slice(format!("${}\r\n", val.len()).as_bytes());
                                    out.extend_from_slice(&val);
                                    out.extend_from_slice(b"\r\n");
                                }
                                None => {
                                    write_resp_null(out);
                                }
                            }
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            // LIST COMMANDS
            Command::Lpush { key, values } => {
                match db.lpush_slice(key.as_ref(), values) {
                    Ok(len) => {
                        record_change!(cmd);
                        notify_keyspace_event(NOTIFY_LIST, "lpush", key);
                        notify_list_or_defer(db, key);
                        write_resp_integer(out, len as i64);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Rpush { key, values } => {
                match db.rpush_slice_fast(key, values) {
                    Ok(len) => {
                        record_change!(cmd);
                        notify_keyspace_event(NOTIFY_LIST, "rpush", key);
                        notify_list_or_defer(db, key);
                        write_resp_integer(out, len as i64);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Lpushx { key, values } => {
                match db.lpushx_slice(key.as_ref(), values) {
                    Ok(len) => {
                        if len > 0 {
                            record_change!(cmd);
                            notify_list_or_defer(db, key);
                        }
                        write_resp_integer(out, len as i64);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Rpushx { key, values } => {
                match db.rpushx_slice(key.as_ref(), values) {
                    Ok(len) => {
                        if len > 0 {
                            record_change!(cmd);
                            notify_list_or_defer(db, key);
                        }
                        write_resp_integer(out, len as i64);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Lpop { key, count } => {
                if count.is_none() {
                    match db.lpop_one(key.as_ref()) {
                        Ok(Some(v)) => {
                            record_change!(cmd);
                            notify_keyspace_event(NOTIFY_LIST, "lpop", key);
                            if !db.exists(key) {
                                notify_keyspace_event(NOTIFY_GENERIC, "del", key);
                            }
                            write_resp_bulk(out, &v);
                        }
                        Ok(None) => {
                            write_resp_null(out);
                        }
                        Err(err) => {
                            write_resp_err(out, err);
                        }
                    }
                    return false;
                }
                let n = count.unwrap_or(1);
                match db.lpop(key, n) {
                    Ok(popped) => {
                        if !popped.is_empty() {
                            record_change!(cmd);
                            notify_keyspace_event(NOTIFY_LIST, "lpop", key);
                            if !db.exists(key) {
                                notify_keyspace_event(NOTIFY_GENERIC, "del", key);
                            }
                        }
                        if !popped.is_empty() {
                            out.extend_from_slice(format!("*{}\r\n", popped.len()).as_bytes());
                            for v in popped {
                                write_resp_bulk(out, &v);
                            }
                        } else if *count == Some(0) && db.exists(key) {
                            out.extend_from_slice(b"*0\r\n");
                        } else {
                            write_resp_null_array(out);
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Rpop { key, count } => {
                if count.is_none() {
                    match db.rpop_one(key.as_ref()) {
                        Ok(Some(v)) => {
                            record_change!(cmd);
                            notify_keyspace_event(NOTIFY_LIST, "rpop", key);
                            if !db.exists(key) {
                                notify_keyspace_event(NOTIFY_GENERIC, "del", key);
                            }
                            write_resp_bulk(out, &v);
                        }
                        Ok(None) => {
                            write_resp_null(out);
                        }
                        Err(err) => {
                            write_resp_err(out, err);
                        }
                    }
                    return false;
                }
                let n = count.unwrap_or(1);
                match db.rpop(key, n) {
                    Ok(popped) => {
                        if !popped.is_empty() {
                            record_change!(cmd);
                            notify_keyspace_event(NOTIFY_LIST, "rpop", key);
                            if !db.exists(key) {
                                notify_keyspace_event(NOTIFY_GENERIC, "del", key);
                            }
                        }
                        if !popped.is_empty() {
                            out.extend_from_slice(format!("*{}\r\n", popped.len()).as_bytes());
                            for v in popped {
                                write_resp_bulk(out, &v);
                            }
                        } else if *count == Some(0) && db.exists(key) {
                            out.extend_from_slice(b"*0\r\n");
                        } else {
                            write_resp_null_array(out);
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Lmpop {
                keys,
                where_from,
                count,
            } => {
                let mut popped: Option<(Bytes, Vec<Bytes>)> = None;
                for k in keys {
                    if !db.exists(k) {
                        continue;
                    }
                    let res = match where_from {
                        crate::table::ListDirection::Left => db.lpop(k, *count),
                        crate::table::ListDirection::Right => db.rpop(k, *count),
                    };
                    match res {
                        Ok(vals) => {
                            if !vals.is_empty() {
                                record_change!(cmd);
                                popped = Some((k.clone(), vals));
                                break;
                            }
                        }
                        Err(err) => {
                            write_resp_err(out, err);
                            return false;
                        }
                    }
                }
                if let Some((k, vals)) = popped {
                    out.extend_from_slice(b"*2\r\n$");
                    out.extend_from_slice(k.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    out.extend_from_slice(&k);
                    out.extend_from_slice(b"\r\n*");
                    out.extend_from_slice(vals.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    for v in &vals {
                        out.extend_from_slice(b"$");
                        out.extend_from_slice(v.len().to_string().as_bytes());
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(v);
                        out.extend_from_slice(b"\r\n");
                    }
                } else {
                    write_resp_null_array(out);
                }
                false
            }
            Command::Blmpop { .. } => false,
            Command::Llen(key) => {
                match db.llen(key) {
                    Ok(len) => {
                        out.extend_from_slice(format!(":{}\r\n", len).as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Lindex { key, index } => {
                match db.lindex(key, *index) {
                    Ok(Some(v)) => {
                        out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                        out.extend_from_slice(&v);
                        out.extend_from_slice(b"\r\n");
                    }
                    Ok(None) => {
                        write_resp_null(out);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Lrange { key, start, stop } => {
                if let Err(err) = db.write_lrange_resp(key.as_ref(), *start, *stop, out) {
                    write_resp_err(out, err);
                }
                false
            }
            // SET COMMANDS
            Command::Sadd { key, members } => {
                match db.sadd_slice(key.as_ref(), members) {
                    Ok(added) => {
                        if added > 0 {
                            record_change!(cmd);
                            notify_keyspace_event(NOTIFY_SET, "sadd", key);
                        }
                        out.extend_from_slice(format!(":{}\r\n", added).as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Srem { key, members } => {
                match db.srem(key, members) {
                    Ok(count) => {
                        if count > 0 {
                            record_change!(cmd);
                            notify_keyspace_event(NOTIFY_SET, "srem", key);
                            if !db.exists(key) {
                                notify_keyspace_event(NOTIFY_GENERIC, "del", key);
                            }
                        }
                        out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Smembers(key) => {
                match db.smembers(key) {
                    Ok(members) => {
                        write_resp_set_len(out, members.len());
                        for m in members {
                            out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            out.extend_from_slice(&m);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Sismember { key, member } => {
                match db.sismember(key, member) {
                    Ok(is_mem) => {
                        if is_mem {
                            out.extend_from_slice(b":1\r\n");
                        } else {
                            out.extend_from_slice(b":0\r\n");
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Scard(key) => {
                match db.scard(key) {
                    Ok(card) => {
                        out.extend_from_slice(format!(":{}\r\n", card).as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Spop { key, count } => {
                let n = count.unwrap_or(1);
                match db.spop(key, n) {
                    Ok(popped) => {
                        if !popped.is_empty() {
                            if count.is_some() && !db.exists(key) {
                                // A count covering the whole set: like Redis,
                                // replicate the set's deletion, DEL (UNLINK
                                // under lazyfree-lazy-server-del), not every
                                // member.
                                record_change!(&server_del_cmd(key));
                            } else {
                                // Like Redis, replicate as SREMs of at most
                                // 1024 members each, so no single propagated
                                // command grows with the count.
                                for chunk in popped.chunks(1024) {
                                    let srem_cmd = Command::Srem {
                                        key: key.clone(),
                                        members: chunk.iter().cloned().collect(),
                                    };
                                    record_change!(&srem_cmd);
                                }
                            }
                            notify_keyspace_event(NOTIFY_SET, "spop", key);
                            notify_del_if_emptied(db, key);
                        }
                        if count.is_some() {
                            write_resp_set_len(out, popped.len());
                            for m in popped {
                                out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                                out.extend_from_slice(&m);
                                out.extend_from_slice(b"\r\n");
                            }
                        } else if let Some(first) = popped.into_iter().next() {
                            out.extend_from_slice(format!("${}\r\n", first.len()).as_bytes());
                            out.extend_from_slice(&first);
                            out.extend_from_slice(b"\r\n");
                        } else {
                            write_resp_null(out);
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Sinter(keys) => {
                match db.sinter(keys) {
                    Ok(members) => {
                        write_resp_set_len(out, members.len());
                        for m in members {
                            out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            out.extend_from_slice(&m);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Sunion(keys) => {
                match db.sunion(keys) {
                    Ok(members) => {
                        write_resp_set_len(out, members.len());
                        for m in members {
                            out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            out.extend_from_slice(&m);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Sdiff(keys) => {
                match db.sdiff(keys) {
                    Ok(members) => {
                        write_resp_set_len(out, members.len());
                        for m in members {
                            out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            out.extend_from_slice(&m);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Sinterstore { destination, keys } => {
                let prev_kind = db.type_of(destination.as_ref());
                match db.sinterstore(destination.clone(), keys) {
                    Ok(count) => {
                        record_change!(cmd);
                        if count > 0 {
                            notify_set_key_events_local(prev_kind, "set", destination.as_ref());
                        } else if prev_kind != "none" {
                            notify_keyspace_event(NOTIFY_GENERIC, "del", destination.as_ref());
                        }
                        write_resp_integer(out, count as i64);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Sunionstore { destination, keys } => {
                let prev_kind = db.type_of(destination.as_ref());
                match db.sunionstore(destination.clone(), keys) {
                    Ok(count) => {
                        record_change!(cmd);
                        if count > 0 {
                            notify_set_key_events_local(prev_kind, "set", destination.as_ref());
                        } else if prev_kind != "none" {
                            notify_keyspace_event(NOTIFY_GENERIC, "del", destination.as_ref());
                        }
                        write_resp_integer(out, count as i64);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Sdiffstore { destination, keys } => {
                let prev_kind = db.type_of(destination.as_ref());
                match db.sdiffstore(destination.clone(), keys) {
                    Ok(count) => {
                        record_change!(cmd);
                        if count > 0 {
                            notify_set_key_events_local(prev_kind, "set", destination.as_ref());
                        } else if prev_kind != "none" {
                            notify_keyspace_event(NOTIFY_GENERIC, "del", destination.as_ref());
                        }
                        write_resp_integer(out, count as i64);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Sintercard { keys, limit } => {
                match db.sintercard(keys, *limit) {
                    Ok(card) => write_resp_integer(out, card as i64),
                    Err(err) => write_resp_err(out, err),
                }
                false
            }
            Command::Sunioncard { keys, limit } => {
                match db.sunioncard(keys, *limit) {
                    Ok(card) => write_resp_integer(out, card as i64),
                    Err(err) => write_resp_err(out, err),
                }
                false
            }
            Command::Sdiffcard { keys, limit } => {
                match db.sdiffcard(keys, *limit) {
                    Ok(card) => write_resp_integer(out, card as i64),
                    Err(err) => write_resp_err(out, err),
                }
                false
            }
            // ZSET COMMANDS
            Command::Zadd {
                key,
                elements,
                flags,
            } => {
                match db.zadd_slice_changes(key.as_ref(), elements, *flags) {
                    Ok((count, changed, incr_score)) => {
                        if flags.incr {
                            if incr_score.is_some() {
                                record_change!(cmd);
                                notify_zset_or_defer(db, key);
                                notify_keyspace_event(NOTIFY_ZSET, "zadd", key);
                            }
                        } else if changed > 0 {
                            // Not `count`: without CH it only counts new
                            // members, and a score update must replicate too.
                            record_change!(cmd);
                            notify_zset_or_defer(db, key);
                            notify_keyspace_event(NOTIFY_ZSET, "zadd", key);
                        }
                        if flags.incr {
                            if let Some(score) = incr_score {
                                write_resp_score(out, score);
                            } else {
                                write_resp_null(out);
                            }
                        } else {
                            write_resp_integer(out, count as i64);
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Zrem { key, members } => {
                match db.zrem(key, members) {
                    Ok(count) => {
                        if count > 0 {
                            record_change!(cmd);
                            notify_keyspace_event(NOTIFY_ZSET, "zrem", key);
                            if !db.exists(key) {
                                notify_keyspace_event(NOTIFY_GENERIC, "del", key);
                            }
                        }
                        out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Zscore { key, member } => {
                match db.zscore(key, member) {
                    Ok(Some(score)) => {
                        write_resp_score(out, score);
                    }
                    Ok(None) => {
                        write_resp_null(out);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Zcard(key) => {
                match db.zcard(key) {
                    Ok(card) => {
                        out.extend_from_slice(format!(":{}\r\n", card).as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Zrank {
                key,
                member,
                with_score,
            } => {
                match db.zrank(key, member, false, *with_score) {
                    Ok(Some((rank, score))) => {
                        if *with_score {
                            out.extend_from_slice(format!("*2\r\n:{}\r\n", rank).as_bytes());
                            write_resp_score(out, score.unwrap_or(0.0));
                        } else {
                            out.extend_from_slice(format!(":{}\r\n", rank).as_bytes());
                        }
                    }
                    Ok(None) => {
                        if *with_score {
                            write_resp_null_array(out);
                        } else {
                            write_resp_null(out);
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Zrevrank {
                key,
                member,
                with_score,
            } => {
                match db.zrank(key, member, true, *with_score) {
                    Ok(Some((rank, score))) => {
                        if *with_score {
                            out.extend_from_slice(format!("*2\r\n:{}\r\n", rank).as_bytes());
                            write_resp_score(out, score.unwrap_or(0.0));
                        } else {
                            out.extend_from_slice(format!(":{}\r\n", rank).as_bytes());
                        }
                    }
                    Ok(None) => {
                        if *with_score {
                            write_resp_null_array(out);
                        } else {
                            write_resp_null(out);
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Zcount {
                key,
                min,
                min_inc,
                max,
                max_inc,
            } => {
                match db.zcount(key, *min, *min_inc, *max, *max_inc) {
                    Ok(count) => {
                        out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Zincrby { key, delta, member } => {
                match db.zincrby(key.clone(), *delta, member.clone()) {
                    Ok(score) => {
                        record_change!(cmd);
                        notify_zset_or_defer(db, key);
                        write_resp_score(out, score);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Zrange { key, opts } => {
                let is_resp3 = CURRENT_CLIENT_RESP3.get();
                if let Err(err) = db.write_zrange_resp(key.as_ref(), opts, is_resp3, out) {
                    write_resp_err(out, err);
                }
                false
            }
            Command::Zrangestore { dst, src, opts } => {
                let prev_kind = db.type_of(dst.as_ref());
                match db.zrangestore(dst, src, opts) {
                    Ok(count) => {
                        record_change!(cmd);
                        if count > 0 {
                            notify_set_key_events_local(prev_kind, "zset", dst.as_ref());
                        } else if prev_kind != "none" {
                            notify_keyspace_event(NOTIFY_GENERIC, "del", dst.as_ref());
                        }
                        write_resp_integer(out, count as i64);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Zpopmin { key, count } => {
                let n = count.unwrap_or(1);
                match db.zpopmin(key, n) {
                    Ok(popped) => {
                        if !popped.is_empty() {
                            let zrem_cmd = Command::Zrem {
                                key: key.clone(),
                                members: popped.iter().map(|(m, _)| m.clone()).collect(),
                            };
                            record_change!(&zrem_cmd);
                            notify_keyspace_event(NOTIFY_ZSET, "zpopmin", key);
                            notify_del_if_emptied(db, key);
                        }
                        if count.is_none() {
                            if popped.is_empty() {
                                out.extend_from_slice(b"*0\r\n");
                            } else {
                                out.extend_from_slice(b"*2\r\n");
                                let (m, s) = &popped[0];
                                write_resp_bulk(out, m);
                                write_resp_score(out, *s);
                            }
                        } else if CURRENT_CLIENT_RESP3.get() {
                            out.extend_from_slice(format!("*{}\r\n", popped.len()).as_bytes());
                            for (m, s) in &popped {
                                out.extend_from_slice(b"*2\r\n");
                                write_resp_bulk(out, m);
                                write_resp_score(out, *s);
                            }
                        } else {
                            out.extend_from_slice(format!("*{}\r\n", popped.len() * 2).as_bytes());
                            for (m, s) in &popped {
                                write_resp_bulk(out, m);
                                write_resp_score(out, *s);
                            }
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Zpopmax { key, count } => {
                let n = count.unwrap_or(1);
                match db.zpopmax(key, n) {
                    Ok(popped) => {
                        if !popped.is_empty() {
                            let zrem_cmd = Command::Zrem {
                                key: key.clone(),
                                members: popped.iter().map(|(m, _)| m.clone()).collect(),
                            };
                            record_change!(&zrem_cmd);
                            notify_keyspace_event(NOTIFY_ZSET, "zpopmax", key);
                            notify_del_if_emptied(db, key);
                        }
                        if count.is_none() {
                            if popped.is_empty() {
                                out.extend_from_slice(b"*0\r\n");
                            } else {
                                out.extend_from_slice(b"*2\r\n");
                                let (m, s) = &popped[0];
                                write_resp_bulk(out, m);
                                write_resp_score(out, *s);
                            }
                        } else if CURRENT_CLIENT_RESP3.get() {
                            out.extend_from_slice(format!("*{}\r\n", popped.len()).as_bytes());
                            for (m, s) in &popped {
                                out.extend_from_slice(b"*2\r\n");
                                write_resp_bulk(out, m);
                                write_resp_score(out, *s);
                            }
                        } else {
                            out.extend_from_slice(format!("*{}\r\n", popped.len() * 2).as_bytes());
                            for (m, s) in &popped {
                                write_resp_bulk(out, m);
                                write_resp_score(out, *s);
                            }
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Zmpop {
                keys,
                is_min,
                count,
            } => {
                let mut popped: Option<(Bytes, Vec<(Bytes, f64)>)> = None;
                for k in keys {
                    if !db.exists(k) {
                        continue;
                    }
                    let res = if *is_min {
                        db.zpopmin(k, *count)
                    } else {
                        db.zpopmax(k, *count)
                    };
                    match res {
                        Ok(items) => {
                            if !items.is_empty() {
                                let rep_cmd = if *is_min {
                                    Command::Zpopmin {
                                        key: k.clone(),
                                        count: Some(items.len()),
                                    }
                                } else {
                                    Command::Zpopmax {
                                        key: k.clone(),
                                        count: Some(items.len()),
                                    }
                                };
                                record_change!(&rep_cmd);
                                popped = Some((k.clone(), items));
                                break;
                            }
                        }
                        Err(err) => {
                            write_resp_err(out, err);
                            return false;
                        }
                    }
                }
                if let Some((k, items)) = popped {
                    format_zmpop_response(out, &k, &items);
                } else {
                    write_resp_null_array(out);
                }
                false
            }
            Command::Zunionstore {
                destination,
                keys,
                weights,
                aggregate,
            } => {
                let prev_kind = db.type_of(destination.as_ref());
                match db.zunionstore(destination.clone(), keys, weights, *aggregate) {
                    Ok(count) => {
                        record_change!(cmd);
                        if count > 0 {
                            notify_set_key_events_local(prev_kind, "zset", destination.as_ref());
                        } else if prev_kind != "none" {
                            notify_keyspace_event(NOTIFY_GENERIC, "del", destination.as_ref());
                        }
                        write_resp_integer(out, count as i64);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Zinterstore {
                destination,
                keys,
                weights,
                aggregate,
            } => {
                let prev_kind = db.type_of(destination.as_ref());
                match db.zinterstore(destination.clone(), keys, weights, *aggregate) {
                    Ok(count) => {
                        record_change!(cmd);
                        if count > 0 {
                            notify_set_key_events_local(prev_kind, "zset", destination.as_ref());
                        } else if prev_kind != "none" {
                            notify_keyspace_event(NOTIFY_GENERIC, "del", destination.as_ref());
                        }
                        write_resp_integer(out, count as i64);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Zdiffstore { destination, keys } => {
                let prev_kind = db.type_of(destination.as_ref());
                match db.zdiffstore(destination.clone(), keys) {
                    Ok(count) => {
                        record_change!(cmd);
                        if count > 0 {
                            notify_set_key_events_local(prev_kind, "zset", destination.as_ref());
                        } else if prev_kind != "none" {
                            notify_keyspace_event(NOTIFY_GENERIC, "del", destination.as_ref());
                        }
                        write_resp_integer(out, count as i64);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Zdiff { keys, with_scores } => {
                match db.zdiff(keys, *with_scores) {
                    Ok(items) => {
                        if *with_scores {
                            if CURRENT_CLIENT_RESP3.get() {
                                out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                                for (m, s) in items {
                                    out.extend_from_slice(b"*2\r\n");
                                    write_resp_bulk(out, &m);
                                    write_resp_score(out, s);
                                }
                            } else {
                                out.extend_from_slice(
                                    format!("*{}\r\n", items.len() * 2).as_bytes(),
                                );
                                for (m, s) in items {
                                    write_resp_bulk(out, &m);
                                    write_resp_score(out, s);
                                }
                            }
                        } else {
                            out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                            for (m, _) in items {
                                out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                                out.extend_from_slice(&m);
                                out.extend_from_slice(b"\r\n");
                            }
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Zinter {
                keys,
                weights,
                aggregate,
                with_scores,
            } => {
                match db.zinter(keys, weights, *aggregate, *with_scores) {
                    Ok(items) => {
                        if *with_scores {
                            if CURRENT_CLIENT_RESP3.get() {
                                out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                                for (m, s) in items {
                                    out.extend_from_slice(b"*2\r\n");
                                    write_resp_bulk(out, &m);
                                    write_resp_score(out, s);
                                }
                            } else {
                                out.extend_from_slice(
                                    format!("*{}\r\n", items.len() * 2).as_bytes(),
                                );
                                for (m, s) in items {
                                    write_resp_bulk(out, &m);
                                    write_resp_score(out, s);
                                }
                            }
                        } else {
                            out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                            for (m, _) in items {
                                out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                                out.extend_from_slice(&m);
                                out.extend_from_slice(b"\r\n");
                            }
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Zunion {
                keys,
                weights,
                aggregate,
                with_scores,
            } => {
                match db.zunion(keys, weights, *aggregate, *with_scores) {
                    Ok(items) => {
                        if *with_scores {
                            if CURRENT_CLIENT_RESP3.get() {
                                out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                                for (m, s) in items {
                                    out.extend_from_slice(b"*2\r\n");
                                    write_resp_bulk(out, &m);
                                    write_resp_score(out, s);
                                }
                            } else {
                                out.extend_from_slice(
                                    format!("*{}\r\n", items.len() * 2).as_bytes(),
                                );
                                for (m, s) in items {
                                    write_resp_bulk(out, &m);
                                    write_resp_score(out, s);
                                }
                            }
                        } else {
                            out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                            for (m, _) in items {
                                out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                                out.extend_from_slice(&m);
                                out.extend_from_slice(b"\r\n");
                            }
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Zintercard { keys, limit } => {
                match db.zintercard(keys, *limit) {
                    Ok(card) => write_resp_integer(out, card as i64),
                    Err(err) => write_resp_err(out, err),
                }
                false
            }
            // GENERIC & DATABASE COMMANDS
            Command::Type(key) => {
                let t = db.type_of(key);
                out.extend_from_slice(format!("+{}\r\n", t).as_bytes());
                false
            }
            Command::Object(sub) => {
                match sub {
                    crate::resp::ObjectSubcommand::Encoding(key) => {
                        if let Some(enc) = db.object_encoding(key) {
                            write_resp_bulk(out, enc.as_bytes());
                        } else {
                            write_resp_null(out);
                        }
                    }
                    crate::resp::ObjectSubcommand::Freq(key) => {
                        if !crate::table::LFU_ACTIVE.load(std::sync::atomic::Ordering::Relaxed) {
                            out.extend_from_slice(b"-ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.\r\n");
                        } else if let Some(freq) = db.lfu_freq(key) {
                            write_resp_integer(out, freq as i64);
                        } else {
                            write_resp_null(out);
                        }
                    }
                    crate::resp::ObjectSubcommand::Idletime(_)
                        if crate::table::LFU_ACTIVE.load(std::sync::atomic::Ordering::Relaxed) =>
                    {
                        out.extend_from_slice(b"-ERR An LFU maxmemory policy is selected, idle time not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.\r\n");
                    }
                    crate::resp::ObjectSubcommand::Idletime(key) => {
                        if let Some(idle) = db.idletime(key) {
                            write_resp_integer(out, idle as i64);
                        } else {
                            write_resp_null(out);
                        }
                    }
                    crate::resp::ObjectSubcommand::Refcount(key) => {
                        if db.exists(key) {
                            out.extend_from_slice(b":1\r\n");
                        } else {
                            write_resp_null(out);
                        }
                    }
                    crate::resp::ObjectSubcommand::Help => {
                        let help_items = [
                            "OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                            "ENCODING <key> -- Return the kind of internal representation used in the object stored at <key>.",
                            "FREQ <key> -- Return the logarithmic access frequency counter of the object stored at <key>.",
                            "IDLETIME <key> -- Return the idle time of the object stored at <key>, in seconds.",
                            "REFCOUNT <key> -- Return the number of references of the value at <key>.",
                            "HELP -- Print this help.",
                        ];
                        write_resp_array_header(out, help_items.len());
                        for item in help_items {
                            write_resp_status(out, item);
                        }
                    }
                }
                false
            }
            Command::Readonly | Command::Readwrite => {
                out.extend_from_slice(b"+OK\r\n");
                false
            }
            Command::Xinfo(sub) => {
                match sub {
                    crate::resp::XinfoSubcommand::Stream(key) => match db.xinfo_stream(key) {
                        Ok(info) => {
                            write_resp_array_header(out, 32);
                            write_resp_bulk(out, b"length");
                            write_resp_integer(out, info.length as i64);
                            write_resp_bulk(out, b"radix-tree-keys");
                            write_resp_integer(out, info.radix_tree_keys as i64);
                            write_resp_bulk(out, b"radix-tree-nodes");
                            write_resp_integer(out, info.radix_tree_nodes as i64);
                            write_resp_bulk(out, b"last-generated-id");
                            let last_id_str = info.last_generated_id.to_string();
                            write_resp_bulk(out, last_id_str.as_bytes());
                            write_resp_bulk(out, b"max-deleted-entry-id");
                            let max_del_str = info.max_deleted_entry_id.to_string();
                            write_resp_bulk(out, max_del_str.as_bytes());
                            write_resp_bulk(out, b"entries-added");
                            write_resp_integer(out, info.entries_added as i64);
                            write_resp_bulk(out, b"recorded-first-entry-id");
                            let rec_first_str =
                                info.recorded_first_entry_id.unwrap_or_default().to_string();
                            write_resp_bulk(out, rec_first_str.as_bytes());
                            write_resp_bulk(out, b"groups");
                            write_resp_integer(out, info.groups as i64);
                            write_resp_bulk(out, b"first-entry");
                            if let Some((id, fields)) = &info.first_entry {
                                write_resp_array_header(out, 2);
                                let id_s = id.to_string();
                                write_resp_bulk(out, id_s.as_bytes());
                                write_resp_array_header(out, fields.len() * 2);
                                for (f, v) in fields {
                                    write_resp_bulk(out, f);
                                    write_resp_bulk(out, v);
                                }
                            } else {
                                write_resp_null_array(out);
                            }
                            write_resp_bulk(out, b"last-entry");
                            if let Some((id, fields)) = &info.last_entry {
                                write_resp_array_header(out, 2);
                                let id_s = id.to_string();
                                write_resp_bulk(out, id_s.as_bytes());
                                write_resp_array_header(out, fields.len() * 2);
                                for (f, v) in fields {
                                    write_resp_bulk(out, f);
                                    write_resp_bulk(out, v);
                                }
                            } else {
                                write_resp_null_array(out);
                            }
                            write_resp_bulk(out, b"pids-tracked");
                            write_resp_integer(out, info.pids_tracked as i64);
                            write_resp_bulk(out, b"iids-tracked");
                            write_resp_integer(out, info.iids_tracked as i64);
                            write_resp_bulk(out, b"iids-added");
                            write_resp_integer(out, info.iids_added as i64);
                            write_resp_bulk(out, b"iids-duplicates");
                            write_resp_integer(out, info.iids_duplicates as i64);
                            write_resp_bulk(out, b"idmp-duration");
                            write_resp_integer(out, info.idmp_duration as i64);
                            write_resp_bulk(out, b"idmp-maxsize");
                            write_resp_integer(out, info.idmp_maxsize as i64);
                        }
                        Err(err) => write_resp_err(out, err),
                    },
                    crate::resp::XinfoSubcommand::StreamFull { key, count } => {
                        match db.xinfo_stream_full(key, *count) {
                            Ok(info) => {
                                write_resp_array_header(out, 30);
                                write_resp_bulk(out, b"length");
                                write_resp_integer(out, info.length as i64);
                                write_resp_bulk(out, b"radix-tree-keys");
                                write_resp_integer(out, info.radix_tree_keys as i64);
                                write_resp_bulk(out, b"radix-tree-nodes");
                                write_resp_integer(out, info.radix_tree_nodes as i64);
                                write_resp_bulk(out, b"last-generated-id");
                                let last_id_str = info.last_generated_id.to_string();
                                write_resp_bulk(out, last_id_str.as_bytes());
                                write_resp_bulk(out, b"max-deleted-entry-id");
                                let max_del_str = info.max_deleted_entry_id.to_string();
                                write_resp_bulk(out, max_del_str.as_bytes());
                                write_resp_bulk(out, b"entries-added");
                                write_resp_integer(out, info.entries_added as i64);
                                write_resp_bulk(out, b"recorded-first-entry-id");
                                let rec_first_str =
                                    info.recorded_first_entry_id.unwrap_or_default().to_string();
                                write_resp_bulk(out, rec_first_str.as_bytes());

                                write_resp_bulk(out, b"entries");
                                write_resp_array_header(out, info.entries.len());
                                for (id, fields) in &info.entries {
                                    write_resp_array_header(out, 2);
                                    let id_s = id.to_string();
                                    write_resp_bulk(out, id_s.as_bytes());
                                    write_resp_array_header(out, fields.len() * 2);
                                    for (f, v) in fields {
                                        write_resp_bulk(out, f);
                                        write_resp_bulk(out, v);
                                    }
                                }

                                write_resp_bulk(out, b"groups");
                                write_resp_array_header(out, info.groups.len());
                                for g in &info.groups {
                                    write_resp_array_header(out, 16);
                                    write_resp_bulk(out, b"name");
                                    write_resp_bulk(out, &g.name);
                                    write_resp_bulk(out, b"last-delivered-id");
                                    let id_str = g.last_delivered_id.to_string();
                                    write_resp_bulk(out, id_str.as_bytes());
                                    write_resp_bulk(out, b"entries-read");
                                    if let Some(er) = g.entries_read {
                                        write_resp_integer(out, er as i64);
                                    } else {
                                        write_resp_null(out);
                                    }
                                    write_resp_bulk(out, b"lag");
                                    if let Some(lag) = g.lag {
                                        write_resp_integer(out, lag as i64);
                                    } else {
                                        write_resp_null(out);
                                    }
                                    write_resp_bulk(out, b"pel-count");
                                    write_resp_integer(out, g.pel_count as i64);
                                    write_resp_bulk(out, b"nacked-count");
                                    write_resp_integer(out, g.nacked_count as i64);
                                    write_resp_bulk(out, b"pending");
                                    write_resp_array_header(out, g.pending.len());
                                    for (id, consumer, time, count) in &g.pending {
                                        write_resp_array_header(out, 4);
                                        let id_s = id.to_string();
                                        write_resp_bulk(out, id_s.as_bytes());
                                        write_resp_bulk(out, consumer);
                                        write_resp_integer(out, *time as i64);
                                        write_resp_integer(out, *count as i64);
                                    }
                                    write_resp_bulk(out, b"consumers");
                                    write_resp_array_header(out, g.consumers.len());
                                    for c in &g.consumers {
                                        write_resp_array_header(out, 10);
                                        write_resp_bulk(out, b"name");
                                        write_resp_bulk(out, &c.name);
                                        write_resp_bulk(out, b"seen-time");
                                        write_resp_integer(out, c.seen_time_ms as i64);
                                        write_resp_bulk(out, b"active-time");
                                        write_resp_integer(
                                            out,
                                            c.active_time_ms.map(|t| t as i64).unwrap_or(-1),
                                        );
                                        write_resp_bulk(out, b"pel-count");
                                        write_resp_integer(out, c.pel_count as i64);
                                        write_resp_bulk(out, b"pending");
                                        write_resp_array_header(out, c.pending.len());
                                        for (id, time, count) in &c.pending {
                                            write_resp_array_header(out, 3);
                                            let id_s = id.to_string();
                                            write_resp_bulk(out, id_s.as_bytes());
                                            write_resp_integer(out, *time as i64);
                                            write_resp_integer(out, *count as i64);
                                        }
                                    }
                                }

                                write_resp_bulk(out, b"pids-tracked");
                                write_resp_integer(out, info.pids_tracked as i64);
                                write_resp_bulk(out, b"iids-tracked");
                                write_resp_integer(out, info.iids_tracked as i64);
                                write_resp_bulk(out, b"iids-added");
                                write_resp_integer(out, info.iids_added as i64);
                                write_resp_bulk(out, b"iids-duplicates");
                                write_resp_integer(out, info.iids_duplicates as i64);
                                write_resp_bulk(out, b"idmp-duration");
                                write_resp_integer(out, info.idmp_duration as i64);
                                write_resp_bulk(out, b"idmp-maxsize");
                                write_resp_integer(out, info.idmp_maxsize as i64);
                            }
                            Err(err) => write_resp_err(out, err),
                        }
                    }
                    crate::resp::XinfoSubcommand::Groups(key) => match db.xinfo_groups(key) {
                        Ok(mut groups) => {
                            groups.sort_by(|a, b| a.name.cmp(&b.name));
                            write_resp_array_header(out, groups.len());
                            for g in groups {
                                write_resp_array_header(out, 12);
                                write_resp_bulk(out, b"name");
                                write_resp_bulk(out, &g.name);
                                write_resp_bulk(out, b"consumers");
                                write_resp_integer(out, g.consumers as i64);
                                write_resp_bulk(out, b"pending");
                                write_resp_integer(out, g.pending as i64);
                                write_resp_bulk(out, b"last-delivered-id");
                                let id_str = g.last_delivered_id.to_string();
                                write_resp_bulk(out, id_str.as_bytes());
                                write_resp_bulk(out, b"entries-read");
                                if let Some(er) = g.entries_read {
                                    write_resp_integer(out, er as i64);
                                } else {
                                    write_resp_null(out);
                                }
                                write_resp_bulk(out, b"lag");
                                if let Some(lag) = g.lag {
                                    write_resp_integer(out, lag as i64);
                                } else {
                                    write_resp_null(out);
                                }
                            }
                        }
                        Err(err) => write_resp_err(out, err),
                    },
                    crate::resp::XinfoSubcommand::Consumers { key, group } => {
                        match db.xinfo_consumers(key, group) {
                            Ok(mut consumers) => {
                                consumers.sort_by(|a, b| a.name.cmp(&b.name));
                                write_resp_array_header(out, consumers.len());
                                for c in consumers {
                                    write_resp_array_header(out, 8);
                                    write_resp_bulk(out, b"name");
                                    write_resp_bulk(out, &c.name);
                                    write_resp_bulk(out, b"pending");
                                    write_resp_integer(out, c.pending as i64);
                                    write_resp_bulk(out, b"idle");
                                    write_resp_integer(out, c.idle_ms as i64);
                                    write_resp_bulk(out, b"inactive");
                                    write_resp_integer(out, c.inactive_ms);
                                }
                            }
                            Err(err) => write_resp_err(out, err),
                        }
                    }
                    crate::resp::XinfoSubcommand::Help => {
                        let help_items = [
                            "CONSUMERS <key> <group> -- Show consumers of <group>.",
                            "GROUPS <key> -- Show consumer groups of <key>.",
                            "STREAM <key> -- Show information about stream <key>.",
                            "HELP -- Print this help.",
                        ];
                        write_resp_array_header(out, help_items.len());
                        for item in help_items {
                            write_resp_status(out, item);
                        }
                    }
                }
                false
            }
            Command::XgroupHelp => {
                let help_items = [
                    "CREATE <key> <groupname> <id|$> [option]",
                    "    Create a new consumer group. Options are:",
                    "    * MKSTREAM",
                    "      Create the empty stream if it does not exist.",
                    "    * ENTRIESREAD entries_read",
                    "      Set the group's entries_read counter (internal use).",
                    "CREATECONSUMER <key> <groupname> <consumer>",
                    "    Create a new consumer in the specified group.",
                    "DELCONSUMER <key> <groupname> <consumer>",
                    "    Remove the specified consumer.",
                    "DESTROY <key> <groupname>",
                    "    Destroy the specified consumer group.",
                    "SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]",
                    "    Set the group last delivered ID to <id|$>.",
                    "HELP",
                    "    Print this help.",
                ];
                write_resp_array_header(out, help_items.len());
                for item in help_items {
                    write_resp_status(out, item);
                }
                false
            }
            Command::CommandCount => {
                crate::command_info::write_count(out);
                false
            }
            Command::CommandList => {
                crate::command_info::write_list(out, None);
                false
            }
            Command::CommandListFiltered {
                filter_type,
                filter_val,
            } => {
                crate::command_info::write_list(
                    out,
                    Some((filter_type.as_str(), filter_val.as_str())),
                );
                false
            }
            Command::CommandGetkeys(cmd_args) => {
                write_command_getkeys(out, cmd_args);
                false
            }
            Command::CommandGetkeysAndFlags(cmd_args) => {
                write_command_getkeys_and_flags(out, cmd_args);
                false
            }
            Command::CommandInfo(cmds) => {
                crate::command_info::write_info(out, cmds, CURRENT_CLIENT_RESP3.get());
                false
            }
            Command::Dbsize => {
                let n = db.visible_dbsize();
                out.extend_from_slice(format!(":{}\r\n", n).as_bytes());
                false
            }
            Command::KeyspaceStats => {
                let keys = db.dbsize();
                let expires = db.table.num_expires.min(keys);
                out.extend_from_slice(format!("*2\r\n:{}\r\n:{}\r\n", keys, expires).as_bytes());
                false
            }
            Command::Select(idx) => {
                if *idx as usize >= crate::config::databases_count() {
                    out.extend_from_slice(b"-ERR DB index is out of range\r\n");
                } else {
                    let d = *idx as usize;
                    CURRENT_DB.set(d);
                    db.select_db(d);
                    out.extend_from_slice(b"+OK\r\n");
                }
                false
            }
            Command::Slowlog(args) => {
                let sub = if !args.is_empty() {
                    args[0].to_ascii_lowercase()
                } else {
                    b"get".to_vec()
                };
                if sub == b"reset" {
                    crate::slowlog::slowlog_reset();
                    out.extend_from_slice(b"+OK\r\n");
                } else if sub == b"len" {
                    let len = crate::slowlog::slowlog_len();
                    out.extend_from_slice(format!(":{}\r\n", len).as_bytes());
                } else if sub == b"help" {
                    let help_lines = [
                        "SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                        "GET [<count>]",
                        "    Return top <count> entries from the slowlog (default: 10, -1 for all).",
                        "LEN",
                        "    Return the length of the slowlog.",
                        "RESET",
                        "    Reset the slowlog, clearing all entries.",
                        "HELP",
                        "    Return top-level help about SLOWLOG subcommands.",
                    ];
                    out.extend_from_slice(format!("*{}\r\n", help_lines.len()).as_bytes());
                    for line in help_lines {
                        out.extend_from_slice(
                            format!("${}\r\n{}\r\n", line.len(), line).as_bytes(),
                        );
                    }
                } else {
                    let count = if args.len() > 1 {
                        match std::str::from_utf8(&args[1])
                            .ok()
                            .and_then(|s| s.parse::<i64>().ok())
                        {
                            Some(c) if c < -1 => {
                                out.extend_from_slice(
                                    b"-ERR count should be greater than or equal to -1\r\n",
                                );
                                return false;
                            }
                            Some(c) => Some(c),
                            None => None,
                        }
                    } else {
                        None
                    };
                    let entries = crate::slowlog::slowlog_get(count);
                    crate::slowlog::write_slowlog_entries_resp(&entries, out);
                }
                false
            }
            Command::Commandlog(args) => {
                crate::slowlog::handle_commandlog(args, out);
                false
            }
            Command::Lolwut(args) => {
                crate::slowlog::handle_lolwut(args, out);
                false
            }
            Command::Sentinel(args) => {
                crate::slowlog::handle_sentinel(args, out);
                false
            }
            Command::Flushdb | Command::Flushall => {
                let is_all = matches!(cmd, Command::Flushall);
                let cur_db = db.current_db;
                if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                    let watched_keys_for_port: Vec<(usize, Bytes)> = {
                        let map = WATCHED_KEYS.read();
                        map.get(&db.port)
                            .map(|m| m.keys().cloned().collect())
                            .unwrap_or_default()
                    };
                    for (w_db, k) in watched_keys_for_port {
                        if (is_all || w_db == cur_db)
                            && db.table_for_db(w_db).is_some()
                            && db.table_for_db_mut(w_db).exists(k.as_ref())
                        {
                            touch_watched_key_in_db(db.port, w_db, k.as_ref());
                        }
                    }
                }
                if is_all {
                    db.flushall();
                } else {
                    db.flushdb();
                }
                record_change!(cmd);
                crate::block::get_block_hub_for_port(db.port)
                    .lock()
                    .notify_all_streams(db);
                out.extend_from_slice(b"+OK\r\n");
                false
            }
            Command::Swapdb { db1, db2 } => {
                let num_dbs = crate::config::databases_count();
                if *db1 < 0 || *db2 < 0 || (*db1 as usize) >= num_dbs || (*db2 as usize) >= num_dbs
                {
                    out.extend_from_slice(b"-ERR DB index is out of range\r\n");
                    return false;
                }
                let (d1, d2) = (*db1 as usize, *db2 as usize);
                if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                    // As `touch_live_watched_keys_on_swapdb`.
                    let keys: Vec<(usize, Bytes)> = WATCHED_KEYS
                        .read()
                        .get(&node_port(db.port))
                        .map(|m| {
                            m.keys()
                                .filter(|(w, _)| *w == d1 || *w == d2)
                                .cloned()
                                .collect()
                        })
                        .unwrap_or_default();
                    for (w_db, k) in keys {
                        let live = [d1, d2].iter().any(|&d| {
                            db.table_for_db(d).is_some() && db.table_for_db_mut(d).exists(&k)
                        });
                        if live {
                            touch_watched_key_in_db(db.port, w_db, &k);
                        }
                    }
                }
                db.swap_dbs(d1, d2);
                record_change!(cmd);
                out.extend_from_slice(b"+OK\r\n");
                false
            }
            Command::Move { key, db: dst_db } => {
                let num_dbs = crate::config::databases_count();
                if *dst_db < 0 || (*dst_db as usize) >= num_dbs {
                    out.extend_from_slice(b"-ERR DB index is out of range\r\n");
                    return false;
                }
                let src_db = db.current_db;
                let dst = *dst_db as usize;
                if db.move_key(src_db, dst, key) {
                    record_change!(cmd);
                    touch_watched_key_in_db(db.port, src_db, key.as_ref());
                    db.select_db(dst);
                    CURRENT_DB.set(dst);
                    match db.type_of(key) {
                        "list" => notify_list_or_defer(db, key),
                        "zset" => notify_zset_or_defer(db, key),
                        "stream" => notify_stream_or_defer(db, key),
                        _ => touch_watched_key_in_db(db.port, dst, key.as_ref()),
                    }
                    CURRENT_DB.set(src_db);
                    db.select_db(src_db);
                    notify_key_invalidation_in_db(db.port, src_db, key.as_ref(), cid);
                    notify_key_invalidation_in_db(db.port, dst, key.as_ref(), cid);
                    notify_keyspace_event_in_db(src_db, NOTIFY_GENERIC, "move_from", key.as_ref());
                    notify_keyspace_event_in_db(dst, NOTIFY_GENERIC, "move_to", key.as_ref());
                    out.extend_from_slice(b":1\r\n");
                } else {
                    out.extend_from_slice(b":0\r\n");
                }
                false
            }
            Command::Touch(keys) => {
                let n = db.touch(keys);
                out.extend_from_slice(format!(":{}\r\n", n).as_bytes());
                false
            }
            Command::Rename { key, newkey, nx } => {
                let prev_kind = db.type_of(newkey.as_ref());
                let src_kind = db.type_of(key.as_ref());
                match db.rename(key, newkey.clone(), *nx) {
                    Ok(success) => {
                        if success {
                            record_change!(cmd);
                            notify_stream_or_defer(db, key);
                            notify_stream_or_defer(db, newkey);
                            if db.type_of(newkey) == "list" {
                                notify_list_or_defer(db, newkey);
                            }
                            if prev_kind != "none" {
                                notify_set_key_events_local(prev_kind, src_kind, newkey.as_ref());
                            }
                            notify_keyspace_event(NOTIFY_GENERIC, "rename_from", key.as_ref());
                            notify_keyspace_event(NOTIFY_GENERIC, "rename_to", newkey.as_ref());
                            if *nx {
                                out.extend_from_slice(b":1\r\n");
                            } else {
                                out.extend_from_slice(b"+OK\r\n");
                            }
                        } else {
                            out.extend_from_slice(b":0\r\n");
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Copy {
                source,
                destination,
                destination_db,
                replace,
            } => {
                let src_db = db.current_db;
                let dst_db = destination_db.map(|d| d as usize).unwrap_or(src_db);
                if dst_db >= crate::config::databases_count() {
                    out.extend_from_slice(b"-ERR DB index is out of range\r\n");
                    return false;
                }
                let src_kind = db.type_of(source.as_ref());
                db.select_db(dst_db);
                let prev_kind = db.type_of(destination.as_ref());
                db.select_db(src_db);
                match db.copy_to_db(src_db, dst_db, source, destination.clone(), *replace) {
                    Ok(success) => {
                        if success {
                            record_change!(cmd);
                            db.select_db(dst_db);
                            CURRENT_DB.set(dst_db);
                            match db.type_of(destination) {
                                "list" => notify_list_or_defer(db, destination),
                                "zset" => notify_zset_or_defer(db, destination),
                                "stream" => notify_stream_or_defer(db, destination),
                                _ => touch_watched_key_in_db(db.port, dst_db, destination.as_ref()),
                            }
                            notify_key_invalidation_in_db(
                                db.port,
                                dst_db,
                                destination.as_ref(),
                                cid,
                            );
                            if prev_kind != "none" {
                                notify_set_key_events_local(
                                    prev_kind,
                                    src_kind,
                                    destination.as_ref(),
                                );
                            }
                            notify_keyspace_event_in_db(
                                dst_db,
                                NOTIFY_GENERIC,
                                "copy_to",
                                destination.as_ref(),
                            );
                            CURRENT_DB.set(src_db);
                            db.select_db(src_db);
                            out.extend_from_slice(b":1\r\n");
                        } else {
                            out.extend_from_slice(b":0\r\n");
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            // EXTENDED STRING COMMANDS
            Command::Setnx { key, value } => {
                let set = db.setnx(key.clone(), value.clone());
                if set {
                    record_change!(cmd);
                    notify_keyspace_event(NOTIFY_STRING, "set", key);
                    out.extend_from_slice(b":1\r\n");
                } else {
                    out.extend_from_slice(b":0\r\n");
                }
                false
            }
            Command::Getset { key, value } => {
                match db.getset(key.clone(), value.clone()) {
                    Ok(old) => {
                        record_change!(cmd);
                        notify_keyspace_event(NOTIFY_STRING, "set", key);
                        match old {
                            Some(v) => {
                                out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                                out.extend_from_slice(&v);
                                out.extend_from_slice(b"\r\n");
                            }
                            None => write_resp_null(out),
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Getdel(key) => {
                match db.getdel(key) {
                    Ok(old) => {
                        if old.is_some() {
                            record_change!(cmd);
                            notify_keyspace_event(NOTIFY_GENERIC, "del", key);
                        }
                        match old {
                            Some(v) => {
                                out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                                out.extend_from_slice(&v);
                                out.extend_from_slice(b"\r\n");
                            }
                            None => write_resp_null(out),
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Append { key, value } => {
                match db.append(key.clone(), value) {
                    Ok(new_len) => {
                        record_change!(cmd);
                        notify_keyspace_event(NOTIFY_STRING, "append", key);
                        out.extend_from_slice(format!(":{}\r\n", new_len).as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Strlen(key) => {
                match db.strlen(key) {
                    Ok(len) => {
                        out.extend_from_slice(format!(":{}\r\n", len).as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Msetnx(pairs) => {
                let any_exists = pairs.iter().any(|(k, _)| db.exists(k));
                if any_exists {
                    out.extend_from_slice(b":0\r\n");
                } else {
                    for (k, v) in pairs {
                        db.set(k.clone(), v.clone(), None);
                    }
                    record_change!(cmd);
                    for (k, _) in pairs {
                        notify_keyspace_event(NOTIFY_STRING, "set", k);
                    }
                    out.extend_from_slice(b":1\r\n");
                }
                false
            }
            Command::Ping(msg) => {
                match msg {
                    Some(m) => {
                        out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                        out.extend_from_slice(m);
                        out.extend_from_slice(b"\r\n");
                    }
                    None => {
                        out.extend_from_slice(b"+PONG\r\n");
                    }
                }
                false
            }
            Command::Monitor => {
                out.extend_from_slice(b"+OK\r\n");
                false
            }
            Command::CommandDocs(cmds) => {
                crate::command_info::write_docs(out, cmds, CURRENT_CLIENT_RESP3.get());
                false
            }
            Command::Keys(pattern) => {
                let keys = db.keys(pattern);
                out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
                for k in keys {
                    out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                    out.extend_from_slice(&k);
                    out.extend_from_slice(b"\r\n");
                }
                false
            }
            Command::Scan {
                cursor,
                pattern,
                count,
                key_type,
            } => {
                let cnt = count.unwrap_or(10);
                let (next_cursor, keys) = db.scan(
                    *cursor as usize,
                    pattern.as_deref(),
                    cnt,
                    key_type.as_deref(),
                );
                let cursor_str = next_cursor.to_string();
                out.extend_from_slice(b"*2\r\n$");
                out.extend_from_slice(cursor_str.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(cursor_str.as_bytes());
                out.extend_from_slice(b"\r\n*");
                out.extend_from_slice(keys.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for k in keys {
                    out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                    out.extend_from_slice(&k);
                    out.extend_from_slice(b"\r\n");
                }
                false
            }
            Command::Randomkey => {
                match db.random_key() {
                    Some(k) => {
                        out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                        out.extend_from_slice(&k);
                        out.extend_from_slice(b"\r\n");
                    }
                    None => {
                        write_resp_null(out);
                    }
                }
                false
            }
            Command::Expiretime(key, in_millis) => {
                let ts = db.expiretime(key, *in_millis);
                out.extend_from_slice(format!(":{}\r\n", ts).as_bytes());
                false
            }
            Command::Multi => {
                out.extend_from_slice(b"+OK\r\n");
                false
            }
            Command::Exec => {
                out.extend_from_slice(b"-ERR EXEC without MULTI\r\n");
                false
            }
            Command::Discard => {
                out.extend_from_slice(b"-ERR DISCARD without MULTI\r\n");
                false
            }
            Command::Watch(_) | Command::Unwatch => {
                out.extend_from_slice(b"+OK\r\n");
                false
            }
            Command::Setbit { key, offset, value } => {
                match db.setbit(key.clone(), *offset, *value) {
                    Ok((old, changed)) => {
                        if changed {
                            crate::snapshot::note_changes(1);
                            record_change!(cmd);
                        }
                        notify_keyspace_event(NOTIFY_STRING, "setbit", key);
                        out.extend_from_slice(format!(":{}\r\n", old).as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Getbit { key, offset } => {
                match db.getbit(key, *offset) {
                    Ok(bit) => {
                        out.extend_from_slice(format!(":{}\r\n", bit).as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Bitcount {
                key,
                start,
                end,
                is_bit,
            } => {
                match db.bitcount(key, *start, *end, *is_bit) {
                    Ok(count) => {
                        out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Bitpos {
                key,
                bit,
                start,
                end,
                is_bit,
            } => {
                match db.bitpos(key, *bit, *start, *end, *is_bit) {
                    Ok(pos) => {
                        out.extend_from_slice(format!(":{}\r\n", pos).as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Bitfield { key, ops, readonly } => {
                match db.bitfield(key.clone(), ops) {
                    Ok((results, changes)) => {
                        if changes > 0 && !*readonly {
                            crate::snapshot::note_changes(changes as u64);
                            if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                touch_watched_key(db.port, key.as_ref());
                            }
                            let need_aof = aof.is_some();
                            let need_rep = crate::replication::has_connected_replicas(db.port);
                            if (need_aof || need_rep)
                                && let Some(bytes) = crate::aof::command_to_resp(cmd)
                            {
                                if let Some(aof_w) = aof {
                                    aof_w.borrow_mut().append(&bytes);
                                }
                                if need_rep {
                                    crate::replication::propagate_shard_bytes(
                                        db.port,
                                        db.shard_id,
                                        &bytes,
                                    );
                                }
                            }
                        }
                        write_resp_array_header(out, results.len());
                        for res in results {
                            match res {
                                Some(val) => {
                                    write_resp_integer(out, val);
                                }
                                None => {
                                    write_resp_null(out);
                                }
                            }
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Bitop {
                op,
                destkey,
                srckeys,
            } => {
                let prev_kind = db.type_of(destkey.as_ref());
                match db.bitop(op, destkey.clone(), srckeys) {
                    Ok(len) => {
                        record_change!(cmd);
                        if len > 0 {
                            notify_set_key_events_local(prev_kind, "string", destkey.as_ref());
                            notify_keyspace_event(NOTIFY_STRING, "set", destkey);
                        } else if prev_kind != "none" {
                            notify_keyspace_event(NOTIFY_GENERIC, "del", destkey.as_ref());
                        }
                        out.extend_from_slice(format!(":{}\r\n", len).as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Pfadd { key, elements } => {
                match db.pfadd(key.clone(), elements) {
                    Ok(updated) => {
                        if updated {
                            record_change!(cmd);
                            notify_keyspace_event(NOTIFY_STRING, "pfadd", key);
                            out.extend_from_slice(b":1\r\n");
                        } else {
                            out.extend_from_slice(b":0\r\n");
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Pfcount { keys } => {
                match db.pfcount(keys) {
                    Ok(count) => {
                        out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Pfmerge { destkey, srckeys } => {
                match db.pfmerge(destkey.clone(), srckeys) {
                    Ok(()) => {
                        record_change!(cmd);
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::PfdebugGetreg(key) => {
                match db.pfdebug_getreg(key) {
                    Ok(regs) => {
                        out.extend_from_slice(b"*16384\r\n");
                        for r in regs {
                            out.extend_from_slice(format!(":{}\r\n", r).as_bytes());
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::PfdebugEncoding(key) => {
                match db.pfdebug_encoding(key) {
                    Ok(enc) => {
                        out.extend_from_slice(format!("+{}\r\n", enc).as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::PfdebugTodense(key) => {
                match db.pfdebug_todense(key) {
                    Ok(changed) => {
                        if changed {
                            record_change!(cmd);
                            out.extend_from_slice(b":1\r\n");
                        } else {
                            out.extend_from_slice(b":0\r\n");
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::PfdebugSimd(_) => {
                out.extend_from_slice(b"+enabled\r\n");
                false
            }
            Command::Pfselftest => {
                out.extend_from_slice(b"+OK\r\n");
                false
            }
            Command::Dump(key) => {
                match db.dump(key) {
                    Some(bytes) => {
                        out.extend_from_slice(format!("${}\r\n", bytes.len()).as_bytes());
                        out.extend_from_slice(&bytes);
                        out.extend_from_slice(b"\r\n");
                    }
                    None => {
                        write_resp_null(out);
                    }
                }
                false
            }
            Command::Restore {
                key,
                ttl_ms,
                serialized,
                replace,
                absttl,
                idletime,
                freq,
                ..
            } => {
                let prev_type = if *replace {
                    Some(db.type_of(key.as_ref()))
                } else {
                    None
                };
                match db.restore(key.clone(), *ttl_ms, serialized, *replace, *absttl) {
                    Ok(crate::table::RestoreOutcome::Expired { replaced }) => {
                        // Already expired, so not stored: like Redis, the
                        // replicas and the AOF see the deletion of the key it
                        // replaced (if any), never the RESTORE.
                        if replaced {
                            record_change!(&server_del_cmd(key));
                            notify_keyspace_event(NOTIFY_GENERIC, "del", key);
                        }
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    Ok(crate::table::RestoreOutcome::Stored) => {
                        db.table.set_lru_or_lfu(key, *freq, *idletime);
                        record_change!(cmd);
                        notify_keyspace_event(NOTIFY_GENERIC, "restore", key);
                        if let Some(prev) = prev_type
                            && prev != "none"
                        {
                            notify_keyspace_event(NOTIFY_OVERWRITTEN, "overwritten", key);
                            let new_type = db.type_of(key.as_ref());
                            if prev != new_type {
                                notify_keyspace_event(NOTIFY_TYPE_CHANGED, "type_changed", key);
                            }
                        }
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    Err(err) => {
                        if err.starts_with("BUSYKEY") || err.starts_with("ERR") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Xadd {
                key,
                nomkstream,
                maxlen,
                minid,
                approx,
                trim_strategy,
                idmp,
                id,
                fields,
                limit,
            } => {
                match db.xadd(
                    key.clone(),
                    id.clone(),
                    fields.clone(),
                    *nomkstream,
                    *maxlen,
                    *minid,
                    *approx,
                    *trim_strategy,
                    idmp.as_deref().cloned(),
                    *limit,
                ) {
                    Ok(crate::table::StreamAddResult::Added(generated_id)) => {
                        // An approximate (or LIMITed) trim depends on how the
                        // stream is split into nodes, so like Redis it is
                        // propagated as the exact trim it amounted to.
                        let trims = maxlen.is_some() || minid.is_some();
                        let (p_maxlen, p_minid, p_approx, p_limit) =
                            if trims && (*approx || limit.is_some()) {
                                let (m, n) = db.table.stream_exact_trim_args(
                                    key,
                                    *maxlen,
                                    *minid,
                                    *trim_strategy,
                                );
                                (m, n, false, None)
                            } else {
                                (*maxlen, *minid, *approx, *limit)
                            };
                        let explicit_id = crate::table::StreamAddId::Explicit(generated_id);
                        if let Some(idmp) = idmp {
                            // IDMP needs an auto ID, and the replica must not
                            // judge duplicates with its own clock: log the
                            // entry, its IID, then the trim.
                            let (pid, iid) = match &**idmp {
                                crate::table::StreamIdmpOption::Manual { producer, iid } => {
                                    (producer.clone(), iid.clone())
                                }
                                crate::table::StreamIdmpOption::Auto { producer } => (
                                    producer.clone(),
                                    crate::table::compute_stream_auto_iid(fields),
                                ),
                            };
                            record_change!(&Command::Xadd {
                                key: key.clone(),
                                nomkstream: *nomkstream,
                                maxlen: None,
                                minid: None,
                                approx: false,
                                trim_strategy: crate::table::StreamTrimStrategy::KeepRef,
                                idmp: None,
                                id: explicit_id,
                                fields: fields.clone(),
                                limit: None,
                            });
                            record_change!(&Command::Xidmprecord {
                                key: key.clone(),
                                pid,
                                iid,
                                id_raw: Bytes::from(generated_id.to_string()),
                            });
                            if trims {
                                record_change!(&Command::Xtrim {
                                    key: key.clone(),
                                    maxlen: p_maxlen,
                                    minid: p_minid,
                                    approx: p_approx,
                                    trim_strategy: *trim_strategy,
                                    limit: p_limit,
                                });
                            }
                        } else {
                            record_change!(&Command::Xadd {
                                key: key.clone(),
                                nomkstream: *nomkstream,
                                maxlen: p_maxlen,
                                minid: p_minid,
                                approx: p_approx,
                                trim_strategy: *trim_strategy,
                                idmp: None,
                                id: explicit_id,
                                fields: fields.clone(),
                                limit: p_limit,
                            });
                        }
                        let s = generated_id.to_string();
                        notify_stream_or_defer(db, key);
                        notify_keyspace_event(NOTIFY_STREAM, "xadd", key);
                        out.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                    }
                    Ok(crate::table::StreamAddResult::Duplicate(existing_id)) => {
                        let s = existing_id.to_string();
                        out.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                    }
                    Ok(crate::table::StreamAddResult::NoMkStream) => {
                        write_resp_null(out);
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Xlen(key) => {
                match db.xlen(key) {
                    Ok(len) => {
                        out.extend_from_slice(format!(":{}\r\n", len).as_bytes());
                    }
                    Err(err) => {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    }
                }
                false
            }
            Command::Xrange {
                key,
                start,
                end,
                count,
            } => {
                match db.xrange(key, start, end, *count) {
                    Ok(items) => {
                        out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                        for (id, fields) in items {
                            let id_str = id.to_string();
                            out.extend_from_slice(
                                format!(
                                    "*2\r\n${}\r\n{}\r\n*{}\r\n",
                                    id_str.len(),
                                    id_str,
                                    fields.len() * 2
                                )
                                .as_bytes(),
                            );
                            for (f, v) in fields {
                                out.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                                out.extend_from_slice(&f);
                                out.extend_from_slice(b"\r\n");
                                out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                                out.extend_from_slice(&v);
                                out.extend_from_slice(b"\r\n");
                            }
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Xrevrange {
                key,
                end,
                start,
                count,
            } => {
                match db.xrevrange(key, end, start, *count) {
                    Ok(items) => {
                        out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                        for (id, fields) in items {
                            let id_str = id.to_string();
                            out.extend_from_slice(
                                format!(
                                    "*2\r\n${}\r\n{}\r\n*{}\r\n",
                                    id_str.len(),
                                    id_str,
                                    fields.len() * 2
                                )
                                .as_bytes(),
                            );
                            for (f, v) in fields {
                                out.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                                out.extend_from_slice(&f);
                                out.extend_from_slice(b"\r\n");
                                out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                                out.extend_from_slice(&v);
                                out.extend_from_slice(b"\r\n");
                            }
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Xread {
                count,
                maxcount,
                maxsize,
                keys,
                ids,
                ..
            } => {
                match db.xread(keys, ids, *count, *maxcount, *maxsize) {
                    Ok(streams) => {
                        if streams.is_empty() {
                            write_resp_null(out);
                        } else {
                            out.extend_from_slice(format!("*{}\r\n", streams.len()).as_bytes());
                            for (stream_key, entries) in streams {
                                out.extend_from_slice(
                                    format!("*2\r\n${}\r\n", stream_key.len()).as_bytes(),
                                );
                                out.extend_from_slice(&stream_key);
                                out.extend_from_slice(
                                    format!("\r\n*{}\r\n", entries.len()).as_bytes(),
                                );
                                for (id, fields) in entries {
                                    let id_str = id.to_string();
                                    out.extend_from_slice(
                                        format!(
                                            "*2\r\n${}\r\n{}\r\n*{}\r\n",
                                            id_str.len(),
                                            id_str,
                                            fields.len() * 2
                                        )
                                        .as_bytes(),
                                    );
                                    for (f, v) in fields {
                                        out.extend_from_slice(
                                            format!("${}\r\n", f.len()).as_bytes(),
                                        );
                                        out.extend_from_slice(&f);
                                        out.extend_from_slice(b"\r\n");
                                        out.extend_from_slice(
                                            format!("${}\r\n", v.len()).as_bytes(),
                                        );
                                        out.extend_from_slice(&v);
                                        out.extend_from_slice(b"\r\n");
                                    }
                                }
                            }
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Xdel { key, ids } => {
                match db.xdel(key, ids) {
                    Ok(count) => {
                        if count > 0 {
                            crate::snapshot::note_changes(count as u64);
                            record_change!(cmd);
                            notify_keyspace_event(NOTIFY_STREAM, "xdel", key);
                        }
                        out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Xtrim {
                key,
                maxlen,
                minid,
                approx,
                trim_strategy,
                limit,
            } => {
                match db.xtrim(key, *maxlen, *minid, *approx, *trim_strategy, *limit) {
                    Ok(count) => {
                        if count > 0 {
                            crate::snapshot::note_changes(count as u64);
                            if *approx || limit.is_some() {
                                // Propagated as the exact trim it amounted to,
                                // as Redis does.
                                let (m, n) = db.table.stream_exact_trim_args(
                                    key,
                                    *maxlen,
                                    *minid,
                                    *trim_strategy,
                                );
                                record_change!(&Command::Xtrim {
                                    key: key.clone(),
                                    maxlen: m,
                                    minid: n,
                                    approx: false,
                                    trim_strategy: *trim_strategy,
                                    limit: None,
                                });
                            } else {
                                record_change!(cmd);
                            }
                            notify_keyspace_event(NOTIFY_STREAM, "xtrim", key);
                        }
                        out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Xcfgset {
                key,
                duration,
                maxsize,
            } => {
                match db.xcfgset(key, *duration, *maxsize) {
                    Ok(()) => {
                        record_change!(cmd);
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Xsetid {
                key,
                last_id,
                entries_added,
                max_deleted_id,
            } => {
                match db.xsetid(key, *last_id, *entries_added, *max_deleted_id) {
                    Ok(()) => {
                        record_change!(cmd);
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Xdelex { key, strategy, ids } => {
                let (res, dirty_count) = db.xdelex(key, *strategy, ids);
                if dirty_count > 0 {
                    crate::snapshot::note_changes(dirty_count as u64);
                    record_change!(cmd);
                }
                write_resp_array_header(out, res.len());
                for code in res {
                    write_resp_integer(out, code);
                }
                false
            }
            Command::Xidmprecord {
                key,
                pid,
                iid,
                id_raw,
            } => {
                match db.xidmprecord(key, pid.clone(), iid.clone(), id_raw) {
                    Ok(()) => {
                        record_change!(cmd);
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::XgroupCreate {
                key,
                group,
                id,
                mkstream,
                entries_read,
            } => {
                match db.xgroup_create(key.clone(), group.clone(), id, *mkstream, *entries_read) {
                    Ok(()) => {
                        record_change!(cmd);
                        notify_keyspace_event(NOTIFY_STREAM, "xgroup-create", key);
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    Err(err) => {
                        if err.starts_with("BUSYGROUP") || err.starts_with("ERR") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::XgroupDestroy { key, group } => {
                match db.xgroup_destroy(key, group) {
                    Ok(destroyed) => {
                        if destroyed {
                            record_change!(cmd);
                            notify_keyspace_event(NOTIFY_STREAM, "xgroup-destroy", key);
                            notify_stream_or_defer(db, key);
                            out.extend_from_slice(b":1\r\n");
                        } else {
                            out.extend_from_slice(b":0\r\n");
                        }
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::XgroupCreateConsumer {
                key,
                group,
                consumer,
            } => {
                match db.xgroup_createconsumer(key, group, consumer.clone()) {
                    Ok(created) => {
                        if created {
                            crate::snapshot::note_changes(1);
                            record_change!(cmd);
                            notify_keyspace_event(NOTIFY_STREAM, "xgroup-createconsumer", key);
                            out.extend_from_slice(b":1\r\n");
                        } else {
                            out.extend_from_slice(b":0\r\n");
                        }
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::XgroupDelConsumer {
                key,
                group,
                consumer,
            } => {
                match db.xgroup_delconsumer(key, group, consumer) {
                    Ok(Some(pending_count)) => {
                        record_change!(cmd);
                        notify_keyspace_event(NOTIFY_STREAM, "xgroup-delconsumer", key);
                        out.extend_from_slice(format!(":{}\r\n", pending_count).as_bytes());
                    }
                    Ok(None) => {
                        out.extend_from_slice(b":0\r\n");
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::XgroupSetId {
                key,
                group,
                id,
                entries_read,
            } => {
                match db.xgroup_setid(key, group, id, *entries_read) {
                    Ok(()) => {
                        record_change!(cmd);
                        notify_keyspace_event(NOTIFY_STREAM, "xgroup-setid", key);
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Xreadgroup {
                group,
                consumer,
                count,
                maxcount,
                maxsize,
                noack,
                claim,
                keys,
                ids,
                ..
            } => {
                let mut all_results = Vec::new();
                let mut err = None;
                let mut props: Vec<Command> = Vec::new();
                let per_stream_limit = count.unwrap_or(usize::MAX);
                let max_total = maxcount.unwrap_or(usize::MAX);
                let max_bytes = maxsize.as_deref().copied().unwrap_or(usize::MAX);
                let mut total_entries = 0;
                let mut total_bytes = 0;

                for (k, id_str) in keys.iter().zip(ids.iter()) {
                    if total_entries >= max_total || (total_entries > 0 && total_bytes >= max_bytes)
                    {
                        break;
                    }
                    let remaining_count = per_stream_limit.min(max_total - total_entries);
                    let prev_last_id = db
                        .table
                        .stream_group_snapshot(k, group, &[])
                        .map(|s| s.last_delivered_id);
                    let had_consumer = db.table.stream_has_consumer(k, group, consumer);
                    match db.xreadgroup(
                        k,
                        group,
                        consumer.clone(),
                        id_str,
                        Some(remaining_count),
                        *noack,
                        claim.as_deref().copied(),
                        max_bytes,
                        &mut total_entries,
                        &mut total_bytes,
                    ) {
                        Ok((entries, modified)) => {
                            if modified {
                                // Reading the pending list only updates delivery
                                // metadata, which (like Redis) is no change.
                                crate::snapshot::note_changes(1);
                                notify_keyspace_event(NOTIFY_STREAM, "xgroup-createconsumer", k);
                            }
                            if let Some(prev_last_id) = prev_last_id {
                                let delivered: Vec<crate::table::StreamId> =
                                    entries.iter().map(|(id, _, _)| *id).collect();
                                props.extend(stream_read_propagation(
                                    db,
                                    k,
                                    group,
                                    consumer,
                                    !had_consumer,
                                    &delivered,
                                    prev_last_id,
                                ));
                            }
                            if !entries.is_empty() {
                                all_results.push((k.clone(), entries));
                            }
                        }
                        Err(e) => {
                            err = Some(e);
                            break;
                        }
                    }
                }
                if let Some(err) = err {
                    if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                } else if all_results.is_empty() {
                    if ids.iter().any(|id| id != ">") {
                        out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
                        for k in keys {
                            out.extend_from_slice(format!("*2\r\n${}\r\n", k.len()).as_bytes());
                            out.extend_from_slice(k);
                            out.extend_from_slice(b"\r\n*0\r\n");
                        }
                    } else {
                        write_resp_null(out);
                    }
                } else {
                    out.extend_from_slice(format!("*{}\r\n", all_results.len()).as_bytes());
                    for (stream_key, entries) in all_results {
                        out.extend_from_slice(
                            format!("*2\r\n${}\r\n", stream_key.len()).as_bytes(),
                        );
                        out.extend_from_slice(&stream_key);
                        out.extend_from_slice(format!("\r\n*{}\r\n", entries.len()).as_bytes());
                        for (id, fields, claim_info) in entries {
                            let id_str = id.to_string();
                            if let Some((idle_ms, delivery_count)) = claim_info {
                                write_resp_array_header(out, 4);
                                write_resp_bulk(out, id_str.as_bytes());
                                write_resp_array_header(out, fields.len() * 2);
                                for (f, v) in fields {
                                    write_resp_bulk(out, &f);
                                    write_resp_bulk(out, &v);
                                }
                                write_resp_integer(out, idle_ms as i64);
                                write_resp_integer(out, delivery_count as i64);
                            } else {
                                write_resp_array_header(out, 2);
                                write_resp_bulk(out, id_str.as_bytes());
                                write_resp_array_header(out, fields.len() * 2);
                                for (f, v) in fields {
                                    write_resp_bulk(out, &f);
                                    write_resp_bulk(out, &v);
                                }
                            }
                        }
                    }
                }
                // Never XREADGROUP itself: with BLOCK it would hang the
                // replica's link, and it would deliver differently there.
                for p in &props {
                    record_change!(p);
                }
                false
            }
            Command::Xack { key, group, ids } => {
                match db.xack(key, group, ids) {
                    Ok(count) => {
                        if count > 0 {
                            record_change!(cmd);
                        }
                        out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Xackdel {
                key,
                group,
                strategy,
                ids,
            } => {
                let (res, dirty_count) = db.xackdel(key, group, *strategy, ids);
                if dirty_count > 0 {
                    record_change!(cmd);
                }
                write_resp_array_header(out, res.len());
                for code in res {
                    write_resp_integer(out, code);
                }
                false
            }
            Command::Xnack {
                key,
                group,
                mode,
                ids,
                retrycount,
                force,
            } => {
                match db.xnack(key, group, *mode, ids, *retrycount, *force) {
                    Ok(count) => {
                        if count > 0 {
                            record_change!(cmd);
                            notify_stream_or_defer(db, key);
                        }
                        write_resp_integer(out, count as i64);
                    }
                    Err(err) => {
                        if err.starts_with("ERR")
                            || err.starts_with("NOGROUP")
                            || err.starts_with("WRONGTYPE")
                        {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Xpending { key, group, range } => {
                match range {
                    None => match db.xpending_summary(key, group) {
                        Ok((count, min_id, max_id, consumers)) => {
                            out.extend_from_slice(b"*4\r\n");
                            out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                            if let Some(min) = min_id {
                                let s = min.to_string();
                                out.extend_from_slice(
                                    format!("${}\r\n{}\r\n", s.len(), s).as_bytes(),
                                );
                            } else {
                                write_resp_null(out);
                            }
                            if let Some(max) = max_id {
                                let s = max.to_string();
                                out.extend_from_slice(
                                    format!("${}\r\n{}\r\n", s.len(), s).as_bytes(),
                                );
                            } else {
                                write_resp_null(out);
                            }
                            out.extend_from_slice(format!("*{}\r\n", consumers.len()).as_bytes());
                            for (c_name, c_cnt) in consumers {
                                out.extend_from_slice(
                                    format!("*2\r\n${}\r\n", c_name.len()).as_bytes(),
                                );
                                out.extend_from_slice(&c_name);
                                // A bulk string, as Redis sends it.
                                out.extend_from_slice(b"\r\n");
                                write_resp_bulk(out, c_cnt.to_string().as_bytes());
                            }
                        }
                        Err(err) => {
                            if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                                out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                            } else {
                                write_resp_err(out, err);
                            }
                        }
                    },
                    Some((start, end, count, consumer, min_idle)) => {
                        match db.xpending_range(
                            key,
                            group,
                            *start,
                            *end,
                            *count,
                            consumer.as_deref(),
                            *min_idle,
                        ) {
                            Ok(entries) => {
                                out.extend_from_slice(format!("*{}\r\n", entries.len()).as_bytes());
                                for (id, c_name, idle, delivery_cnt) in entries {
                                    let id_str = id.to_string();
                                    out.extend_from_slice(
                                        format!(
                                            "*4\r\n${}\r\n{}\r\n${}\r\n",
                                            id_str.len(),
                                            id_str,
                                            c_name.len(),
                                        )
                                        .as_bytes(),
                                    );
                                    out.extend_from_slice(&c_name);
                                    out.extend_from_slice(
                                        format!("\r\n:{}\r\n:{}\r\n", idle, delivery_cnt)
                                            .as_bytes(),
                                    );
                                }
                            }
                            Err(err) => {
                                if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                                    out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                                } else {
                                    write_resp_err(out, err);
                                }
                            }
                        }
                    }
                }
                false
            }
            Command::Hincrby {
                key,
                field,
                increment,
            } => {
                match db.hincrby(key.clone(), field.clone(), *increment) {
                    Ok(val) => {
                        record_change!(cmd);
                        notify_hash_fields_event("hincrby", key, std::slice::from_ref(field));
                        write_resp_integer(out, val);
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Hincrbyfloat {
                key,
                field,
                increment,
            } => {
                match db.hincrbyfloat(key.clone(), field.clone(), *increment) {
                    Ok(val) => {
                        // Replicated as HSETEX ... KEEPTTL of the result, as
                        // Redis does: the float can't round differently on a
                        // replica, and the field keeps its TTL there.
                        let value = Bytes::from(val.to_string());
                        record_change!(&Command::Hsetex {
                            key: key.clone(),
                            condition: crate::resp::HsetexCondition::None,
                            expire: crate::resp::HFieldExpireOpt::KeepTtl,
                            pairs: vec![(field.clone(), value.clone())],
                        });
                        notify_hash_fields_event("hincrbyfloat", key, std::slice::from_ref(field));
                        write_resp_bulk(out, &value);
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Hrandfield {
                key,
                count,
                with_values,
            } => {
                match db.hrandfield(key, *count, *with_values) {
                    Ok(items) => {
                        if count.is_none() {
                            if let Some(f) = items.first() {
                                write_resp_bulk(out, f);
                            } else {
                                write_resp_null(out);
                            }
                        } else if *with_values && CURRENT_CLIENT_RESP3.get() {
                            let num_pairs = items.len() / 2;
                            out.extend_from_slice(format!("*{}\r\n", num_pairs).as_bytes());
                            for chunk in items.chunks(2) {
                                out.extend_from_slice(b"*2\r\n");
                                write_resp_bulk(out, &chunk[0]);
                                write_resp_bulk(out, &chunk[1]);
                            }
                        } else {
                            out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                            for item in items {
                                write_resp_bulk(out, &item);
                            }
                        }
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Hscan {
                key,
                cursor,
                pattern,
                count,
                no_values,
            } => {
                match db.hscan(
                    key,
                    *cursor,
                    pattern.as_deref(),
                    count.unwrap_or(10),
                    *no_values,
                ) {
                    Ok((next_cursor, entries)) => {
                        out.extend_from_slice(b"*2\r\n");
                        let cur_str = next_cursor.to_string();
                        write_resp_bulk(out, cur_str.as_bytes());
                        out.extend_from_slice(format!("*{}\r\n", entries.len()).as_bytes());
                        for item in entries {
                            write_resp_bulk(out, &item);
                        }
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Smismember { key, members } => {
                match db.smismember(key, members) {
                    Ok(bools) => {
                        out.extend_from_slice(format!("*{}\r\n", bools.len()).as_bytes());
                        for b in bools {
                            write_resp_integer(out, if b { 1 } else { 0 });
                        }
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Srandmember { key, count } => {
                match db.srandmember(key, *count) {
                    Ok(items) => {
                        if count.is_none() {
                            if let Some(item) = items.first() {
                                write_resp_bulk(out, item);
                            } else {
                                write_resp_null(out);
                            }
                        } else {
                            out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                            for item in items {
                                write_resp_bulk(out, &item);
                            }
                        }
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Smove {
                source,
                destination,
                member,
            } => {
                match db.smove(source, destination.clone(), member.clone()) {
                    Ok(res) => {
                        if res.moved {
                            touch_watched_key(db.port, source.as_ref());
                            if res.dst_added {
                                touch_watched_key(db.port, destination.as_ref());
                            }
                            let need_aof = aof.is_some();
                            let need_rep = crate::replication::has_connected_replicas(db.port);
                            if (need_aof || need_rep)
                                && let Some(bytes) = crate::aof::command_to_resp(cmd)
                            {
                                if let Some(aof_w) = aof {
                                    aof_w.borrow_mut().append(&bytes);
                                }
                                if need_rep {
                                    crate::replication::propagate_shard_bytes(
                                        db.port,
                                        db.shard_id,
                                        &bytes,
                                    );
                                }
                            }
                        }
                        write_resp_integer(out, if res.moved { 1 } else { 0 });
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Sscan {
                key,
                cursor,
                pattern,
                count,
            } => {
                match db.sscan(key, *cursor, pattern.as_deref(), count.unwrap_or(10)) {
                    Ok((next_cursor, entries)) => {
                        out.extend_from_slice(b"*2\r\n");
                        let cur_str = next_cursor.to_string();
                        write_resp_bulk(out, cur_str.as_bytes());
                        out.extend_from_slice(format!("*{}\r\n", entries.len()).as_bytes());
                        for item in entries {
                            write_resp_bulk(out, &item);
                        }
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Zmscore { key, members } => {
                match db.zmscore(key, members) {
                    Ok(scores) => {
                        out.extend_from_slice(format!("*{}\r\n", scores.len()).as_bytes());
                        for s in scores {
                            match s {
                                Some(val) => write_resp_score(out, val),
                                None => write_resp_null(out),
                            }
                        }
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Zrandmember {
                key,
                count,
                with_scores,
            } => {
                match db.zrandmember(key, *count, *with_scores) {
                    Ok(items) => {
                        if count.is_none() {
                            if let Some((m, _)) = items.first() {
                                write_resp_bulk(out, m);
                            } else {
                                write_resp_null(out);
                            }
                        } else if *with_scores {
                            if CURRENT_CLIENT_RESP3.get() {
                                out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                                for (m, s) in items {
                                    out.extend_from_slice(b"*2\r\n");
                                    write_resp_bulk(out, &m);
                                    write_resp_score(out, s);
                                }
                            } else {
                                out.extend_from_slice(
                                    format!("*{}\r\n", items.len() * 2).as_bytes(),
                                );
                                for (m, s) in items {
                                    write_resp_bulk(out, &m);
                                    let formatted = format_score(s);
                                    write_resp_bulk(out, formatted.as_bytes());
                                }
                            }
                        } else {
                            out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                            for (m, _) in items {
                                write_resp_bulk(out, &m);
                            }
                        }
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Zremrangebyrank { key, start, stop } => {
                match db.zremrangebyrank(key, *start, *stop) {
                    Ok(removed) => {
                        if removed > 0 {
                            record_change!(cmd);
                        }
                        write_resp_integer(out, removed as i64);
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Zremrangebyscore {
                key,
                min_score,
                min_inc,
                max_score,
                max_inc,
            } => {
                match db.zremrangebyscore(key, *min_score, *min_inc, *max_score, *max_inc) {
                    Ok(removed) => {
                        if removed > 0 {
                            record_change!(cmd);
                        }
                        write_resp_integer(out, removed as i64);
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Zremrangebylex { key, min, max } => {
                match db.zremrangebylex(key, min, max) {
                    Ok(removed) => {
                        if removed > 0 {
                            record_change!(cmd);
                        }
                        write_resp_integer(out, removed as i64);
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Zlexcount { key, min, max } => {
                match db.zlexcount(key, min, max) {
                    Ok(count) => {
                        write_resp_integer(out, count as i64);
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Zscan {
                key,
                cursor,
                pattern,
                count,
            } => {
                match db.zscan(key, *cursor, pattern.as_deref(), count.unwrap_or(10)) {
                    Ok((next_cursor, entries)) => {
                        out.extend_from_slice(b"*2\r\n");
                        let cur_str = next_cursor.to_string();
                        write_resp_bulk(out, cur_str.as_bytes());
                        out.extend_from_slice(format!("*{}\r\n", entries.len() * 2).as_bytes());
                        for (m, s) in entries {
                            write_resp_bulk(out, &m);
                            let formatted = format_score(s);
                            write_resp_bulk(out, formatted.as_bytes());
                        }
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Ltrim { key, start, stop } => {
                let existed = NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed) != 0
                    && db.exists(key);
                match db.ltrim(key, *start, *stop) {
                    Ok(()) => {
                        record_change!(cmd);
                        if existed {
                            notify_keyspace_event(NOTIFY_LIST, "ltrim", key);
                            notify_del_if_emptied(db, key);
                        }
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Lset {
                key,
                index,
                element,
            } => {
                match db.lset(key, *index, element.clone()) {
                    Ok(()) => {
                        record_change!(cmd);
                        notify_keyspace_event(NOTIFY_LIST, "lset", key);
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Lrem {
                key,
                count,
                element,
            } => {
                match db.lrem(key, *count, element) {
                    Ok(removed) => {
                        if removed > 0 {
                            record_change!(cmd);
                            notify_keyspace_event(NOTIFY_LIST, "lrem", key);
                            notify_del_if_emptied(db, key);
                        }
                        write_resp_integer(out, removed as i64);
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Lpos {
                key,
                element,
                rank,
                count,
                maxlen,
            } => {
                match db.lpos(key, element, rank.unwrap_or(1), *count, *maxlen) {
                    Ok(indices) => {
                        if count.is_none() {
                            if let Some(idx) = indices.first() {
                                write_resp_integer(out, *idx as i64);
                            } else {
                                write_resp_null(out);
                            }
                        } else {
                            out.extend_from_slice(format!("*{}\r\n", indices.len()).as_bytes());
                            for idx in indices {
                                write_resp_integer(out, idx as i64);
                            }
                        }
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Linsert {
                key,
                before,
                pivot,
                element,
            } => {
                match db.linsert(key.clone(), *before, pivot, element.clone()) {
                    Ok(len) => {
                        if len > 0 {
                            record_change!(cmd);
                            notify_list_or_defer(db, key);
                            notify_keyspace_event(NOTIFY_LIST, "linsert", key);
                        }
                        write_resp_integer(out, len);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Sort {
                key,
                desc,
                alpha,
                store,
                limit,
                by,
                get,
                readonly: _,
            } => {
                let t = db.type_of(key);
                let mut items: Vec<Bytes> = match t {
                    "none" => Vec::new(),
                    "list" => db.lrange(key, 0, -1).unwrap_or_default(),
                    "set" => db.smembers(key).unwrap_or_default(),
                    "zset" => db
                        .zrange(
                            key,
                            &crate::table::ZRangeOpts {
                                start: 0,
                                stop: -1,
                                ..Default::default()
                            },
                        )
                        .map(|pairs| pairs.into_iter().map(|(m, _)| m).collect())
                        .unwrap_or_default(),
                    _ => {
                        write_resp_err(
                            out,
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                        return false;
                    }
                };

                let mut by_pattern = by.clone();
                let mut sort_alpha = *alpha;
                let mut dontsort = match &by_pattern {
                    Some(p) => !p.contains(&b'*'),
                    None => false,
                };

                if dontsort && t == "set" {
                    dontsort = false;
                    sort_alpha = true;
                    by_pattern = None;
                }

                if !dontsort {
                    let mut sort_items = Vec::with_capacity(items.len());
                    let mut int_conversion_error = false;

                    for item in items {
                        let by_val: Option<Bytes> = if let Some(ref pat) = by_pattern {
                            let (lookup_key, field_opt) = pattern_subst_and_field(pat, &item);
                            if let Some(field) = field_opt {
                                db.hget(&lookup_key, &field).ok().flatten()
                            } else {
                                db.get(&lookup_key)
                            }
                        } else {
                            Some(item.clone())
                        };

                        if sort_alpha {
                            sort_items.push((0.0f64, by_val, item));
                        } else {
                            let score = match by_val {
                                Some(ref s_bytes) => {
                                    match std::str::from_utf8(s_bytes.as_ref())
                                        .ok()
                                        .and_then(|s| s.parse::<f64>().ok())
                                    {
                                        Some(val) if !val.is_nan() => val,
                                        _ => {
                                            int_conversion_error = true;
                                            0.0
                                        }
                                    }
                                }
                                None => 0.0,
                            };
                            sort_items.push((score, None, item));
                        }
                    }

                    if int_conversion_error {
                        write_resp_err(out, "One or more scores can't be converted into double");
                        return false;
                    }

                    if sort_alpha {
                        sort_items.sort_by(|a, b| {
                            let cmp = match (&a.1, &b.1) {
                                (Some(v1), Some(v2)) => v1.cmp(v2),
                                (None, Some(_)) => std::cmp::Ordering::Less,
                                (Some(_), None) => std::cmp::Ordering::Greater,
                                (None, None) => std::cmp::Ordering::Equal,
                            };
                            let cmp = if cmp == std::cmp::Ordering::Equal {
                                a.2.cmp(&b.2)
                            } else {
                                cmp
                            };
                            if *desc { cmp.reverse() } else { cmp }
                        });
                    } else {
                        sort_items.sort_by(|a, b| {
                            let cmp = a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal);
                            let cmp = if cmp == std::cmp::Ordering::Equal {
                                a.2.cmp(&b.2)
                            } else {
                                cmp
                            };
                            if *desc { cmp.reverse() } else { cmp }
                        });
                    }

                    items = sort_items.into_iter().map(|(_, _, item)| item).collect();
                } else if *desc {
                    items.reverse();
                }

                let (start, count) = match limit {
                    Some((offset, count)) => {
                        let start = (*offset).max(0) as usize;
                        let count = if *count < 0 {
                            items.len()
                        } else {
                            *count as usize
                        };
                        (start, count)
                    }
                    None => (0, items.len()),
                };

                let sliced_items: Vec<Bytes> = if start >= items.len() {
                    Vec::new()
                } else {
                    items.into_iter().skip(start).take(count).collect()
                };

                let mut results: Vec<Option<Bytes>> = Vec::new();
                if get.is_empty() {
                    for item in sliced_items {
                        results.push(Some(item));
                    }
                } else {
                    for item in sliced_items {
                        for get_pat in get {
                            if get_pat.as_ref() == b"#" {
                                results.push(Some(item.clone()));
                            } else {
                                let (lookup_key, field_opt) =
                                    pattern_subst_and_field(get_pat, &item);
                                let val = if let Some(field) = field_opt {
                                    db.hget(&lookup_key, &field).ok().flatten()
                                } else {
                                    db.get(&lookup_key)
                                };
                                results.push(val);
                            }
                        }
                    }
                }

                if let Some(dest) = store {
                    // Propagated as its effect (DEL, then RPUSH of the
                    // result), which replays the same on a replica or from
                    // the AOF whatever the BY/GET keys hold there.
                    let prev_kind = db.type_of(dest);
                    if results.is_empty() {
                        if db.del(dest) {
                            record_change!(&Command::Del(smallvec::smallvec![dest.clone()]));
                        }
                        if prev_kind != "none" {
                            notify_keyspace_event(NOTIFY_GENERIC, "del", dest.as_ref());
                        }
                        out.extend_from_slice(b":0\r\n");
                    } else {
                        let count = results.len();
                        let list_items: Vec<Bytes> = results
                            .into_iter()
                            .map(|opt| opt.unwrap_or_else(|| Bytes::from_static(b"")))
                            .collect();
                        if db.del(dest) {
                            record_change!(&Command::Del(smallvec::smallvec![dest.clone()]));
                        }
                        let rpush = Command::Rpush {
                            key: dest.clone(),
                            values: list_items.clone().into(),
                        };
                        let _ = db.rpush(dest.clone(), list_items);
                        record_change!(&rpush);
                        notify_list_or_defer(db, dest);
                        notify_set_key_events_local(prev_kind, "list", dest.as_ref());
                        out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                    }
                } else {
                    out.extend_from_slice(format!("*{}\r\n", results.len()).as_bytes());
                    for res in results {
                        match res {
                            Some(b) => write_resp_bulk(out, &b),
                            None => write_resp_null(out),
                        }
                    }
                }
                false
            }
            Command::Lmove {
                source,
                destination,
                where_from,
                where_to,
                ..
            } => {
                match db.lmove(source, destination.clone(), *where_from, *where_to) {
                    Ok(Some(val)) => {
                        record_change!(cmd);
                        notify_list_or_defer(db, destination);
                        write_resp_bulk(out, &val);
                    }
                    Ok(None) => {
                        write_resp_null(out);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Lmovem {
                source,
                destination,
                where_from,
                where_to,
                mode,
                count,
                ordering,
                ..
            } => {
                match db.lmovem(
                    source,
                    destination.clone(),
                    *where_from,
                    *where_to,
                    *mode,
                    *count,
                    *ordering,
                ) {
                    Ok(Some(vals)) => {
                        record_change!(cmd);
                        notify_list_or_defer(db, destination);
                        write_resp_array_header(out, vals.len());
                        for v in &vals {
                            write_resp_bulk(out, v);
                        }
                    }
                    Ok(None) => {
                        write_resp_null_array(out);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Incrbyfloat { key, increment } => {
                match db.incrbyfloat(key.clone(), *increment) {
                    Ok(val) => {
                        // Replicated as SET of the result, as Redis does: a
                        // replica re-adding the float could round differently.
                        let value = Bytes::from(val.to_string());
                        record_change!(&Command::Set {
                            key: key.clone(),
                            value: value.clone(),
                            expire_in: None,
                            condition: crate::resp::SetCondition::None,
                            get: false,
                            keepttl: true,
                            past_expired: false,
                        });
                        notify_keyspace_event(NOTIFY_STRING, "incrbyfloat", key);
                        write_resp_bulk(out, &value);
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Increx {
                key,
                increment,
                lbound,
                ubound,
                saturate,
                expire,
                enx,
            } => {
                match db.increx(
                    key.clone(),
                    *increment,
                    *lbound,
                    *ubound,
                    *saturate,
                    *expire,
                    *enx,
                ) {
                    Ok(out_res) => {
                        if let Some(rep) = out_res.rep_cmd {
                            record_change!(&rep);
                        }
                        for ev in out_res.events {
                            let ev_type = match ev {
                                "incrby" | "incrbyfloat" => NOTIFY_STRING,
                                _ => NOTIFY_GENERIC,
                            };
                            notify_keyspace_event(ev_type, ev, key);
                        }
                        out.extend_from_slice(b"*2\r\n");
                        if out_res.is_float {
                            let s_val = out_res.val_float.to_string();
                            out.extend_from_slice(
                                format!("${}\r\n{}\r\n", s_val.len(), s_val).as_bytes(),
                            );
                            let s_delta = out_res.delta_float.to_string();
                            out.extend_from_slice(
                                format!("${}\r\n{}\r\n", s_delta.len(), s_delta).as_bytes(),
                            );
                        } else {
                            let s_val = out_res.val_int.to_string();
                            out.extend_from_slice(format!(":{}\r\n", s_val).as_bytes());
                            let s_delta = out_res.delta_int.to_string();
                            out.extend_from_slice(format!(":{}\r\n", s_delta).as_bytes());
                        }
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Setrange { key, offset, value } => {
                match db.setrange(key.clone(), *offset, value) {
                    Ok(len) => {
                        record_change!(cmd);
                        if !value.is_empty() {
                            notify_keyspace_event(NOTIFY_STRING, "setrange", key);
                        }
                        write_resp_integer(out, len as i64);
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Getrange { key, start, end } => {
                match db.getrange(key, *start, *end) {
                    Ok(slice) => {
                        write_resp_bulk(out, &slice);
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
                false
            }
            Command::Time => {
                let now = crate::clock::system_now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                let secs = now.as_secs();
                let micros = now.subsec_micros();
                out.extend_from_slice(
                    format!(
                        "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        secs.to_string().len(),
                        secs,
                        micros.to_string().len(),
                        micros
                    )
                    .as_bytes(),
                );
                false
            }
            Command::Echo(msg) => {
                write_resp_bulk(out, msg);
                false
            }
            // REDISJSON COMMANDS
            Command::JsonSet {
                key,
                path,
                json_val,
                nx,
                xx,
            } => {
                match db.json_store.json_set(key, path, json_val, *nx, *xx) {
                    Ok(true) => {
                        record_change!(cmd);
                        if let Some(doc) = db.json_store.get(key).cloned() {
                            let k_str = String::from_utf8_lossy(key);
                            db.index_json_document_local(&k_str, &doc);
                            crate::search::index_json_document_hook(&k_str, &doc);
                        }
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    Ok(false) => {
                        write_resp_null(out);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::JsonGet { key, paths } => {
                let path_refs: Vec<&str> = paths.iter().map(|p| p.as_str()).collect();
                match db.json_store.json_get(key, &path_refs) {
                    Some(res) => {
                        write_resp_bulk(out, res.as_bytes());
                    }
                    None => {
                        write_resp_null(out);
                    }
                }
                false
            }
            Command::JsonDel { key, path } => {
                let count = db.json_store.json_del(key, path.as_deref());
                if count > 0 {
                    record_change!(cmd);
                    let k_str = String::from_utf8_lossy(key);
                    if let Some(doc) = db.json_store.get(key).cloned() {
                        db.index_json_document_local(&k_str, &doc);
                        crate::search::index_json_document_hook(&k_str, &doc);
                    } else {
                        db.delete_document_local(&k_str);
                        crate::search::delete_document_hook(&k_str);
                    }
                }
                write_resp_integer(out, count as i64);
                false
            }
            Command::JsonType { key, path } => {
                match db.json_store.json_type(key, path.as_deref()) {
                    Some(t) => {
                        out.extend_from_slice(format!("+{}\r\n", t).as_bytes());
                    }
                    None => {
                        write_resp_null(out);
                    }
                }
                false
            }
            Command::JsonNumIncrBy { key, path, delta } => {
                match db.json_store.json_numincrby(key, path, *delta) {
                    Ok(new_val) => {
                        record_change!(cmd);
                        if let Some(doc) = db.json_store.get(key) {
                            crate::search::index_json_document_hook(
                                &String::from_utf8_lossy(key),
                                doc,
                            );
                        }
                        write_resp_bulk(out, new_val.as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::JsonNumMultBy { key, path, factor } => {
                match db.json_store.json_nummultby(key, path, *factor) {
                    Ok(res) => {
                        record_change!(cmd);
                        if let Some(doc) = db.json_store.get(key) {
                            crate::search::index_json_document_hook(
                                &String::from_utf8_lossy(key),
                                doc,
                            );
                        }
                        write_resp_bulk(out, res.as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::JsonStrAppend { key, path, value } => {
                match db.json_store.json_strappend(key, path.as_deref(), value) {
                    Ok(new_len) => {
                        record_change!(cmd);
                        if let Some(doc) = db.json_store.get(key) {
                            crate::search::index_json_document_hook(
                                &String::from_utf8_lossy(key),
                                doc,
                            );
                        }
                        write_resp_integer(out, new_len as i64);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::JsonStrLen { key, path } => {
                match db.json_store.json_strlen(key, path.as_deref()) {
                    Some(len) => {
                        write_resp_integer(out, len as i64);
                    }
                    None => {
                        write_resp_null(out);
                    }
                }
                false
            }
            Command::JsonArrAppend { key, path, values } => {
                let val_refs: Vec<&str> = values.iter().map(|v| v.as_str()).collect();
                match db.json_store.json_arrappend(key, path, &val_refs) {
                    Ok(new_len) => {
                        record_change!(cmd);
                        if let Some(doc) = db.json_store.get(key) {
                            crate::search::index_json_document_hook(
                                &String::from_utf8_lossy(key),
                                doc,
                            );
                        }
                        write_resp_integer(out, new_len as i64);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::JsonArrLen { key, path } => {
                match db.json_store.json_arrlen(key, path.as_deref()) {
                    Some(len) => {
                        write_resp_integer(out, len as i64);
                    }
                    None => {
                        write_resp_null(out);
                    }
                }
                false
            }
            Command::JsonArrPop { key, path, index } => {
                match db.json_store.json_arrpop(key, path.as_deref(), *index) {
                    Some(popped) => {
                        record_change!(cmd);
                        if let Some(doc) = db.json_store.get(key) {
                            crate::search::index_json_document_hook(
                                &String::from_utf8_lossy(key),
                                doc,
                            );
                        }
                        write_resp_bulk(out, popped.as_bytes());
                    }
                    None => {
                        write_resp_null(out);
                    }
                }
                false
            }
            Command::JsonObjKeys { key, path } => {
                match db.json_store.json_objkeys(key, path.as_deref()) {
                    Some(keys) => {
                        out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
                        for k in keys {
                            write_resp_bulk(out, k.as_bytes());
                        }
                    }
                    None => {
                        write_resp_null(out);
                    }
                }
                false
            }
            Command::JsonObjLen { key, path } => {
                match db.json_store.json_objlen(key, path.as_deref()) {
                    Some(len) => {
                        write_resp_integer(out, len as i64);
                    }
                    None => {
                        write_resp_null(out);
                    }
                }
                false
            }
            Command::JsonToggle { key, path } => {
                match db.json_store.json_toggle(key, path) {
                    Ok(b) => {
                        record_change!(cmd);
                        if let Some(doc) = db.json_store.get(key) {
                            crate::search::index_json_document_hook(
                                &String::from_utf8_lossy(key),
                                doc,
                            );
                        }
                        write_resp_bulk(out, b.as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::JsonClear { key, path } => {
                let cleared = db.json_store.json_clear(key, path.as_deref());
                if cleared > 0 {
                    record_change!(cmd);
                    if let Some(doc) = db.json_store.get(key) {
                        crate::search::index_json_document_hook(&String::from_utf8_lossy(key), doc);
                    }
                }
                write_resp_integer(out, cleared as i64);
                false
            }
            // GEOSPATIAL COMMANDS
            Command::Geoadd {
                key,
                items,
                nx,
                xx,
                ch,
            } => {
                let mut elements = Vec::with_capacity(items.len());
                for (lon, lat, member) in items {
                    match crate::geo::encode_geohash(*lon, *lat) {
                        Ok(hash) => {
                            elements.push((hash as f64, member.clone()));
                        }
                        Err(e) => {
                            out.extend_from_slice(format!("-{}\r\n", e).as_bytes());
                            return false;
                        }
                    }
                }
                let flags = crate::table::ZAddFlags {
                    nx: *nx,
                    xx: *xx,
                    ch: *ch,
                    gt: false,
                    lt: false,
                    incr: false,
                };
                match db.zadd(key.clone(), elements, flags) {
                    Ok((added, _)) => {
                        record_change!(cmd);
                        write_resp_integer(out, added as i64);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Geodist { key, m1, m2, unit } => {
                let t = db.type_of(key);
                if t != "none" && t != "zset" {
                    out.extend_from_slice(
                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                    return false;
                }
                let s1 = db.zscore(key, m1);
                let s2 = db.zscore(key, m2);
                match (s1, s2) {
                    (Ok(Some(sc1)), Ok(Some(sc2))) => {
                        let (lon1, lat1) = crate::geo::decode_geohash(sc1 as u64);
                        let (lon2, lat2) = crate::geo::decode_geohash(sc2 as u64);
                        let mut dist = crate::geo::haversine_distance(lon1, lat1, lon2, lat2);
                        if let Some(u) = unit {
                            dist = u.from_meters(dist);
                        }
                        let s = format!("{:.4}", dist);
                        write_resp_bulk(out, s.as_bytes());
                    }
                    _ => {
                        write_resp_null(out);
                    }
                }
                false
            }
            Command::Geopos { key, members } => {
                let t = db.type_of(key);
                if t != "none" && t != "zset" {
                    out.extend_from_slice(
                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                    return false;
                }
                out.extend_from_slice(format!("*{}\r\n", members.len()).as_bytes());
                for m in members {
                    match db.zscore(key, m) {
                        Ok(Some(score)) => {
                            let (lon, lat) = crate::geo::decode_geohash(score as u64);
                            out.extend_from_slice(b"*2\r\n");
                            write_resp_double_text(out, &format!("{}", lon));
                            write_resp_double_text(out, &format!("{}", lat));
                        }
                        _ => {
                            write_resp_null_array(out);
                        }
                    }
                }
                false
            }
            Command::Geohash { key, members } => {
                let t = db.type_of(key);
                if t != "none" && t != "zset" {
                    out.extend_from_slice(
                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                    return false;
                }
                out.extend_from_slice(format!("*{}\r\n", members.len()).as_bytes());
                for m in members {
                    match db.zscore(key, m) {
                        Ok(Some(score)) => {
                            let b32 = crate::geo::geohash_to_base32(score as u64);
                            write_resp_bulk(out, b32.as_bytes());
                        }
                        _ => {
                            write_resp_null(out);
                        }
                    }
                }
                false
            }
            Command::Georadius {
                key,
                lon,
                lat,
                radius,
                unit,
                withcoord,
                withdist,
                withhash,
                count,
                any,
                asc,
                store,
                storedist,
            } => {
                let key_type = db.type_of(key);
                if key_type != "none" && key_type != "zset" {
                    out.extend_from_slice(
                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                    return false;
                }
                let storekey = store.as_ref().or(storedist.as_ref());
                let is_storedist = storedist.is_some();
                if key_type == "none" {
                    if let Some(store_dest) = storekey {
                        if db.del(store_dest) {
                            record_change!(&Command::Del(smallvec::smallvec![store_dest.clone()]));
                        }
                        write_resp_integer(out, 0);
                        return true;
                    } else {
                        out.extend_from_slice(b"*0\r\n");
                        return false;
                    }
                }
                let radius_m = unit.to_meters(*radius);
                let shape = crate::geo::GeoSearchShape::Radius { radius_m };
                let results = crate::geo::execute_geo_query(
                    db, key, *lon, *lat, shape, *unit, *withdist, *withhash, *withcoord, *count,
                    *any, *asc,
                );
                let has_options = *withcoord || *withdist || *withhash;
                if let Some(store_dest) = storekey {
                    let prev_kind = db.type_of(store_dest.as_ref());
                    if results.is_empty() {
                        if db.del(store_dest) {
                            record_change!(&Command::Del(smallvec::smallvec![store_dest.clone()]));
                        }
                        if prev_kind != "none" {
                            notify_keyspace_event(NOTIFY_GENERIC, "del", store_dest.as_ref());
                        }
                        write_resp_integer(out, 0);
                    } else {
                        if db.del(store_dest) {
                            record_change!(&Command::Del(smallvec::smallvec![store_dest.clone()]));
                        }
                        let mut zset_elements = Vec::with_capacity(results.len());
                        for item in &results {
                            let sc = if is_storedist {
                                unit.from_meters(item.dist_m)
                            } else {
                                item.score as f64
                            };
                            zset_elements.push((sc, item.member.clone()));
                        }
                        let zadd = Command::Zadd {
                            key: store_dest.clone(),
                            elements: zset_elements.iter().cloned().collect(),
                            flags: crate::table::ZAddFlags::default(),
                        };
                        let _ = db.zadd(
                            store_dest.clone(),
                            zset_elements,
                            crate::table::ZAddFlags::default(),
                        );
                        record_change!(&zadd);
                        notify_set_key_events_local(prev_kind, "zset", store_dest.as_ref());
                        write_resp_integer(out, results.len() as i64);
                    }
                    // Propagated above as its effect (DEL, ZADD): GEORADIUS
                    // itself has no serializer and is read-only in Redis 6.2+.
                } else {
                    crate::geo::format_geo_results(out, &results, has_options);
                }
                false
            }
            Command::Georadiusbymember {
                key,
                member,
                radius,
                unit,
                withcoord,
                withdist,
                withhash,
                count,
                any,
                asc,
                store,
                storedist,
            } => {
                let key_type = db.type_of(key);
                if key_type != "none" && key_type != "zset" {
                    out.extend_from_slice(
                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                    return false;
                }
                let storekey = store.as_ref().or(storedist.as_ref());
                let is_storedist = storedist.is_some();
                if key_type == "none" {
                    if let Some(store_dest) = storekey {
                        if db.del(store_dest) {
                            record_change!(&Command::Del(smallvec::smallvec![store_dest.clone()]));
                        }
                        write_resp_integer(out, 0);
                        return true;
                    } else {
                        out.extend_from_slice(b"*0\r\n");
                        return false;
                    }
                }
                let center = match db.zscore(key, member) {
                    Ok(Some(score)) => crate::geo::decode_geohash(score as u64),
                    _ => {
                        out.extend_from_slice(b"-ERR could not decode requested zset member\r\n");
                        return false;
                    }
                };
                let radius_m = unit.to_meters(*radius);
                let shape = crate::geo::GeoSearchShape::Radius { radius_m };
                let results = crate::geo::execute_geo_query(
                    db, key, center.0, center.1, shape, *unit, *withdist, *withhash, *withcoord,
                    *count, *any, *asc,
                );
                let has_options = *withcoord || *withdist || *withhash;
                if let Some(store_dest) = storekey {
                    let prev_kind = db.type_of(store_dest.as_ref());
                    if results.is_empty() {
                        if db.del(store_dest) {
                            record_change!(&Command::Del(smallvec::smallvec![store_dest.clone()]));
                        }
                        if prev_kind != "none" {
                            notify_keyspace_event(NOTIFY_GENERIC, "del", store_dest.as_ref());
                        }
                        write_resp_integer(out, 0);
                    } else {
                        if db.del(store_dest) {
                            record_change!(&Command::Del(smallvec::smallvec![store_dest.clone()]));
                        }
                        let mut zset_elements = Vec::with_capacity(results.len());
                        for item in &results {
                            let sc = if is_storedist {
                                unit.from_meters(item.dist_m)
                            } else {
                                item.score as f64
                            };
                            zset_elements.push((sc, item.member.clone()));
                        }
                        let zadd = Command::Zadd {
                            key: store_dest.clone(),
                            elements: zset_elements.iter().cloned().collect(),
                            flags: crate::table::ZAddFlags::default(),
                        };
                        let _ = db.zadd(
                            store_dest.clone(),
                            zset_elements,
                            crate::table::ZAddFlags::default(),
                        );
                        record_change!(&zadd);
                        notify_set_key_events_local(prev_kind, "zset", store_dest.as_ref());
                        write_resp_integer(out, results.len() as i64);
                    }
                    // Propagated above as its effect (DEL, ZADD): GEORADIUS
                    // itself has no serializer and is read-only in Redis 6.2+.
                } else {
                    crate::geo::format_geo_results(out, &results, has_options);
                }
                false
            }
            Command::Geosearch {
                key,
                from_member,
                from_lonlat,
                by_radius,
                by_box,
                asc,
                count,
                any,
                withcoord,
                withdist,
                withhash,
            } => {
                let key_type = db.type_of(key);
                if key_type != "none" && key_type != "zset" {
                    out.extend_from_slice(
                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                    return false;
                }
                if key_type == "none" {
                    out.extend_from_slice(b"*0\r\n");
                    return false;
                }
                let center_opt = if let Some((lon, lat)) = from_lonlat {
                    Some((*lon, *lat))
                } else if let Some(m) = from_member {
                    match db.zscore(key, m) {
                        Ok(Some(score)) => Some(crate::geo::decode_geohash(score as u64)),
                        _ => None,
                    }
                } else {
                    None
                };
                let (center_lon, center_lat) = match center_opt {
                    Some(c) => c,
                    None => {
                        out.extend_from_slice(b"-ERR could not decode requested zset member\r\n");
                        return false;
                    }
                };
                let (shape, unit) = if let Some((rad, u)) = by_radius {
                    (
                        crate::geo::GeoSearchShape::Radius {
                            radius_m: u.to_meters(*rad),
                        },
                        *u,
                    )
                } else if let Some((w, h, u)) = by_box {
                    (
                        crate::geo::GeoSearchShape::Box {
                            width_m: u.to_meters(*w),
                            height_m: u.to_meters(*h),
                        },
                        *u,
                    )
                } else {
                    (
                        crate::geo::GeoSearchShape::Radius { radius_m: 0.0 },
                        crate::geo::GeoUnit::Meters,
                    )
                };
                let results = crate::geo::execute_geo_query(
                    db, key, center_lon, center_lat, shape, unit, *withdist, *withhash, *withcoord,
                    *count, *any, *asc,
                );
                let has_options = *withcoord || *withdist || *withhash;
                crate::geo::format_geo_results(out, &results, has_options);
                false
            }
            Command::Geosearchstore {
                dest,
                key,
                from_member,
                from_lonlat,
                by_radius,
                by_box,
                asc,
                count,
                any,
                storedist,
            } => {
                let key_type = db.type_of(key);
                if key_type != "none" && key_type != "zset" {
                    out.extend_from_slice(
                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                    return false;
                }
                if key_type == "none" {
                    if db.del(dest) {
                        record_change!(&Command::Del(smallvec::smallvec![dest.clone()]));
                    }
                    write_resp_integer(out, 0);
                    return true;
                }
                let center_opt = if let Some((lon, lat)) = from_lonlat {
                    Some((*lon, *lat))
                } else if let Some(m) = from_member {
                    match db.zscore(key, m) {
                        Ok(Some(score)) => Some(crate::geo::decode_geohash(score as u64)),
                        _ => None,
                    }
                } else {
                    None
                };
                let (center_lon, center_lat) = match center_opt {
                    Some(c) => c,
                    None => {
                        out.extend_from_slice(b"-ERR could not decode requested zset member\r\n");
                        return false;
                    }
                };
                let (shape, unit) = if let Some((rad, u)) = by_radius {
                    (
                        crate::geo::GeoSearchShape::Radius {
                            radius_m: u.to_meters(*rad),
                        },
                        *u,
                    )
                } else if let Some((w, h, u)) = by_box {
                    (
                        crate::geo::GeoSearchShape::Box {
                            width_m: u.to_meters(*w),
                            height_m: u.to_meters(*h),
                        },
                        *u,
                    )
                } else {
                    (
                        crate::geo::GeoSearchShape::Radius { radius_m: 0.0 },
                        crate::geo::GeoUnit::Meters,
                    )
                };
                let results = crate::geo::execute_geo_query(
                    db, key, center_lon, center_lat, shape, unit, false, false, false, *count,
                    *any, *asc,
                );
                let prev_kind = db.type_of(dest.as_ref());
                if results.is_empty() {
                    db.del(dest);
                    if prev_kind != "none" {
                        notify_keyspace_event(NOTIFY_GENERIC, "del", dest.as_ref());
                    }
                    write_resp_integer(out, 0);
                } else {
                    db.del(dest);
                    let mut zset_elements = Vec::with_capacity(results.len());
                    for item in &results {
                        let sc = if *storedist {
                            unit.from_meters(item.dist_m)
                        } else {
                            item.score as f64
                        };
                        zset_elements.push((sc, item.member.clone()));
                    }
                    let _ = db.zadd(
                        dest.clone(),
                        zset_elements,
                        crate::table::ZAddFlags::default(),
                    );
                    notify_set_key_events_local(prev_kind, "zset", dest.as_ref());
                    write_resp_integer(out, results.len() as i64);
                }
                record_change!(cmd);
                false
            }
            // PROBABILISTIC COMMANDS
            Command::BfReserve {
                key,
                error_rate,
                capacity,
            } => {
                if db.probabilistic_store.bloom_filters.contains_key(key) {
                    out.extend_from_slice(b"-ERR item exists\r\n");
                } else {
                    match crate::probabilistic::BloomFilter::try_new(*capacity, *error_rate) {
                        Ok(bf) => {
                            db.probabilistic_store.bloom_filters.insert(key.clone(), bf);
                            record_change!(cmd);
                            out.extend_from_slice(b"+OK\r\n");
                        }
                        Err(e) => out.extend_from_slice(format!("-ERR {e}\r\n").as_bytes()),
                    }
                }
                false
            }
            Command::BfAdd { key, item } => {
                let bf = db
                    .probabilistic_store
                    .bloom_filters
                    .entry(key.clone())
                    .or_insert_with(|| crate::probabilistic::BloomFilter::new(1000, 0.01));
                let added = bf.add(item);
                if added {
                    record_change!(cmd);
                    out.extend_from_slice(b":1\r\n");
                } else {
                    out.extend_from_slice(b":0\r\n");
                }
                false
            }
            Command::BfMadd { key, items } => {
                let bf = db
                    .probabilistic_store
                    .bloom_filters
                    .entry(key.clone())
                    .or_insert_with(|| crate::probabilistic::BloomFilter::new(1000, 0.01));
                out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                let mut any_added = false;
                for it in items {
                    let added = bf.add(it);
                    if added {
                        any_added = true;
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                }
                if any_added {
                    record_change!(cmd);
                }
                false
            }
            Command::BfExists { key, item } => {
                if let Some(bf) = db.probabilistic_store.bloom_filters.get(key) {
                    if bf.contains(item) {
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                } else {
                    out.extend_from_slice(b":0\r\n");
                }
                false
            }
            Command::BfMexists { key, items } => {
                out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                if let Some(bf) = db.probabilistic_store.bloom_filters.get(key) {
                    for it in items {
                        if bf.contains(it) {
                            out.extend_from_slice(b":1\r\n");
                        } else {
                            out.extend_from_slice(b":0\r\n");
                        }
                    }
                } else {
                    for _ in items {
                        out.extend_from_slice(b":0\r\n");
                    }
                }
                false
            }
            Command::BfInfo(key) => {
                if let Some(bf) = db.probabilistic_store.bloom_filters.get(key) {
                    out.extend_from_slice(b"*8\r\n");
                    write_resp_bulk(out, b"Capacity");
                    write_resp_integer(out, bf.capacity as i64);
                    write_resp_bulk(out, b"Size");
                    write_resp_integer(out, (bf.bits.len() * 8) as i64);
                    write_resp_bulk(out, b"Number of filters");
                    write_resp_integer(out, 1);
                    write_resp_bulk(out, b"Number of items inserted");
                    write_resp_integer(out, bf.count as i64);
                } else {
                    out.extend_from_slice(b"-ERR not found\r\n");
                }
                false
            }
            Command::CfReserve { key, capacity } => {
                if db.probabilistic_store.cuckoo_filters.contains_key(key) {
                    out.extend_from_slice(b"-ERR item exists\r\n");
                } else {
                    match crate::probabilistic::CuckooFilter::try_new(*capacity) {
                        Ok(cf) => {
                            db.probabilistic_store
                                .cuckoo_filters
                                .insert(key.clone(), cf);
                            record_change!(cmd);
                            out.extend_from_slice(b"+OK\r\n");
                        }
                        Err(e) => out.extend_from_slice(format!("-ERR {e}\r\n").as_bytes()),
                    }
                }
                false
            }
            Command::CfAdd { key, item } => {
                let cf = db
                    .probabilistic_store
                    .cuckoo_filters
                    .entry(key.clone())
                    .or_insert_with(|| crate::probabilistic::CuckooFilter::new(1000));
                match cf.add(item) {
                    Ok(_) => {
                        record_change!(cmd);
                        out.extend_from_slice(b":1\r\n");
                    }
                    Err(e) => {
                        // A failed insert has already kicked fingerprints
                        // around (and dropped one); replaying it does the same.
                        record_change!(cmd);
                        out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes());
                    }
                }
                false
            }
            Command::CfAddnx { key, item } => {
                let cf = db
                    .probabilistic_store
                    .cuckoo_filters
                    .entry(key.clone())
                    .or_insert_with(|| crate::probabilistic::CuckooFilter::new(1000));
                if cf.contains(item) {
                    out.extend_from_slice(b":0\r\n");
                } else {
                    match cf.add(item) {
                        Ok(_) => {
                            record_change!(cmd);
                            out.extend_from_slice(b":1\r\n");
                        }
                        Err(e) => {
                            // As in CF.ADD, the failed insert changed the filter.
                            record_change!(cmd);
                            out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes());
                        }
                    }
                }
                false
            }
            Command::CfExists { key, item } => {
                if let Some(cf) = db.probabilistic_store.cuckoo_filters.get(key) {
                    if cf.contains(item) {
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                } else {
                    out.extend_from_slice(b":0\r\n");
                }
                false
            }
            Command::CfDel { key, item } => {
                if let Some(cf) = db.probabilistic_store.cuckoo_filters.get_mut(key) {
                    if cf.delete(item) {
                        record_change!(cmd);
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                } else {
                    out.extend_from_slice(b":0\r\n");
                }
                false
            }
            Command::CfInfo(key) => {
                if let Some(cf) = db.probabilistic_store.cuckoo_filters.get(key) {
                    out.extend_from_slice(b"*6\r\n");
                    write_resp_bulk(out, b"Size");
                    write_resp_integer(out, (cf.num_buckets * 4 * 2) as i64);
                    write_resp_bulk(out, b"Number of buckets");
                    write_resp_integer(out, cf.num_buckets as i64);
                    write_resp_bulk(out, b"Number of items inserted");
                    write_resp_integer(out, cf.count as i64);
                } else {
                    out.extend_from_slice(b"-ERR not found\r\n");
                }
                false
            }
            Command::CmsInitbydim { key, width, depth } => {
                match crate::probabilistic::CountMinSketch::try_new(*width, *depth) {
                    Ok(cms) => {
                        db.probabilistic_store.cms_sketches.insert(key.clone(), cms);
                        record_change!(cmd);
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    Err(e) => out.extend_from_slice(format!("-ERR {e}\r\n").as_bytes()),
                }
                false
            }
            Command::CmsInitbyprob {
                key,
                error,
                probability,
            } => {
                db.probabilistic_store.cms_sketches.insert(
                    key.clone(),
                    crate::probabilistic::CountMinSketch::from_prob(*error, *probability),
                );
                record_change!(cmd);
                out.extend_from_slice(b"+OK\r\n");
                false
            }
            Command::CmsIncrby { key, pairs } => {
                let cms = db
                    .probabilistic_store
                    .cms_sketches
                    .entry(key.clone())
                    .or_insert_with(|| crate::probabilistic::CountMinSketch::new(2000, 5));
                out.extend_from_slice(format!("*{}\r\n", pairs.len()).as_bytes());
                for (item, delta) in pairs {
                    let count = cms.incr_by(item, *delta);
                    write_resp_integer(out, count as i64);
                }
                record_change!(cmd);
                false
            }
            Command::CmsQuery { key, items } => {
                out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                if let Some(cms) = db.probabilistic_store.cms_sketches.get(key) {
                    for it in items {
                        let count = cms.query(it);
                        write_resp_integer(out, count as i64);
                    }
                } else {
                    for _ in items {
                        write_resp_integer(out, 0);
                    }
                }
                false
            }
            Command::CmsInfo(key) => {
                if let Some(cms) = db.probabilistic_store.cms_sketches.get(key) {
                    out.extend_from_slice(b"*6\r\n");
                    write_resp_bulk(out, b"width");
                    write_resp_integer(out, cms.width as i64);
                    write_resp_bulk(out, b"depth");
                    write_resp_integer(out, cms.depth as i64);
                    write_resp_bulk(out, b"count");
                    write_resp_integer(out, cms.total_count as i64);
                } else {
                    out.extend_from_slice(b"-ERR not found\r\n");
                }
                false
            }
            Command::TopkReserve { key, topk } => {
                db.probabilistic_store
                    .topk_trackers
                    .insert(key.clone(), crate::probabilistic::TopK::new(*topk));
                record_change!(cmd);
                out.extend_from_slice(b"+OK\r\n");
                false
            }
            Command::TopkAdd { key, items } => {
                let tk = db
                    .probabilistic_store
                    .topk_trackers
                    .entry(key.clone())
                    .or_insert_with(|| crate::probabilistic::TopK::new(50));
                out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                for it in items {
                    if let Some(evicted) = tk.add(it.clone(), 1) {
                        write_resp_bulk(out, &evicted);
                    } else {
                        write_resp_null(out);
                    }
                }
                record_change!(cmd);
                false
            }
            Command::TopkQuery { key, items } => {
                out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                if let Some(tk) = db.probabilistic_store.topk_trackers.get(key) {
                    for it in items {
                        if tk.query(it) {
                            out.extend_from_slice(b":1\r\n");
                        } else {
                            out.extend_from_slice(b":0\r\n");
                        }
                    }
                } else {
                    for _ in items {
                        out.extend_from_slice(b":0\r\n");
                    }
                }
                false
            }
            Command::TopkList(key) => {
                if let Some(tk) = db.probabilistic_store.topk_trackers.get(key) {
                    let items = tk.list();
                    out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                    for (item, _) in items {
                        write_resp_bulk(out, &item);
                    }
                } else {
                    out.extend_from_slice(b"*0\r\n");
                }
                false
            }
            Command::TopkInfo(key) => {
                if let Some(tk) = db.probabilistic_store.topk_trackers.get(key) {
                    out.extend_from_slice(b"*4\r\n");
                    write_resp_bulk(out, b"k");
                    write_resp_integer(out, tk.k as i64);
                    write_resp_bulk(out, b"width");
                    write_resp_integer(out, tk.items.len() as i64);
                } else {
                    out.extend_from_slice(b"-ERR not found\r\n");
                }
                false
            }
            Command::ProbRestore { kind, key, payload } => {
                match db.probabilistic_store.restore(*kind, key.clone(), payload) {
                    Ok(()) => {
                        record_change!(cmd);
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    Err(e) => {
                        out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes());
                    }
                }
                false
            }
            Command::Memory(sub) => {
                match sub {
                    MemorySubcommand::Usage { key } => {
                        if let Some((val, _)) = db.get_entry(key) {
                            let size = 24 + key.len() + val.approx_bytes();
                            write_resp_integer(out, size as i64);
                        } else {
                            write_resp_null(out);
                        }
                    }
                    MemorySubcommand::Stats => {
                        // Inside a script or another shard's batch: this
                        // shard's figures only.
                        let report = crate::allocator::MemoryReport {
                            breakdown: db.memory_breakdown(),
                            used: db.total_used_memory(),
                            maxmemory: 0,
                            alloc: crate::allocator::get_allocator_stats(),
                        };
                        crate::allocator::format_memory_stats(&report, out);
                    }
                    _ => {
                        out.extend_from_slice(b"+OK\r\n");
                    }
                }
                false
            }
            Command::Debug(args) => {
                if let Some(sub) = args.first() {
                    if sub.eq_ignore_ascii_case(b"object") {
                        if let Some(key) = args.get(1) {
                            // As stored, expired keys included (see above).
                            let _raw = ReplayingWrites::new();
                            if let Some(enc) = db.object_encoding(key) {
                                let (lru, idle) = db.lru_and_idletime(key).unwrap_or((0, 0));
                                out.extend_from_slice(format!("+Value at:0x12345678 refcount:1 encoding:{} serializedlength:10 lru:{} lru_seconds_idle:{}\r\n", enc, lru, idle).as_bytes());
                            } else {
                                out.extend_from_slice(b"-ERR no such key\r\n");
                            }
                            return false;
                        }
                    } else if sub.eq_ignore_ascii_case(b"pause-cron") {
                        let flag = args.get(1).map(|v| v.as_ref() == b"1").unwrap_or(false);
                        PAUSE_CRON.store(flag, std::sync::atomic::Ordering::Relaxed);
                        out.extend_from_slice(b"+OK\r\n");
                        return false;
                    } else if sub.eq_ignore_ascii_case(b"set-allow-access-expired") {
                        let flag = args.get(1).map(|v| v.as_ref() == b"1").unwrap_or(false);
                        ALLOW_ACCESS_EXPIRED.store(flag, std::sync::atomic::Ordering::Relaxed);
                        out.extend_from_slice(b"+OK\r\n");
                        return false;
                    } else if sub.eq_ignore_ascii_case(b"set-active-expire") {
                        out.extend_from_slice(b"+OK\r\n");
                        return false;
                    } else if sub.eq_ignore_ascii_case(b"sleep") {
                        if let Some(arg) = args.get(1)
                            && let Ok(s) = std::str::from_utf8(arg)
                            && let Ok(secs) = s.parse::<f64>()
                        {
                            std::thread::sleep(std::time::Duration::from_secs_f64(secs));
                        }
                        out.extend_from_slice(b"+OK\r\n");
                        return false;
                    } else if sub.eq_ignore_ascii_case(b"panic") {
                        panic!("DEBUG PANIC requested by client");
                    }
                }
                crate::debug_cmd::execute_shard_debug(args, db, out, aof);
                false
            }
            Command::Digest(key) => {
                match db.table.get(key) {
                    Ok(Some(bytes)) => {
                        let digest = crate::table::compute_digest(&bytes);
                        write_resp_bulk(out, digest.as_bytes());
                    }
                    Ok(None) => {
                        write_resp_null(out);
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Delex { key, condition } => {
                let should_del = match condition {
                    None => db.exists(key),
                    Some((op, expected)) => match db.table.get(key) {
                        Ok(Some(val)) => match op.to_uppercase().as_str() {
                            "IFEQ" => val == *expected,
                            "IFNE" => val != *expected,
                            "IFDEQ" => {
                                if expected.len() != 16
                                    || !expected.iter().all(|b| b.is_ascii_hexdigit())
                                {
                                    write_resp_err(
                                        out,
                                        "ERR digest must be exactly 16 hexadecimal characters",
                                    );
                                    return false;
                                }
                                let d = crate::table::compute_digest(&val);
                                d.eq_ignore_ascii_case(&String::from_utf8_lossy(expected))
                            }
                            "IFDNE" => {
                                if expected.len() != 16
                                    || !expected.iter().all(|b| b.is_ascii_hexdigit())
                                {
                                    write_resp_err(
                                        out,
                                        "ERR digest must be exactly 16 hexadecimal characters",
                                    );
                                    return false;
                                }
                                let d = crate::table::compute_digest(&val);
                                !d.eq_ignore_ascii_case(&String::from_utf8_lossy(expected))
                            }
                            "IFGT" => val > *expected,
                            "IFLT" => val < *expected,
                            _ => false,
                        },
                        Ok(None) => false,
                        Err(_) => {
                            write_resp_err(
                                out,
                                "ERR WRONGTYPE Operation against a key holding the wrong kind of value",
                            );
                            return false;
                        }
                    },
                };
                if should_del {
                    if db.del(key) {
                        record_change!(cmd);
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                } else {
                    out.extend_from_slice(b":0\r\n");
                }
                false
            }
            Command::CrdtSet { key, val } => {
                let ts = db.crdt_set(key.clone(), val.clone());
                record_crdt_effect!(db.crdt_store.register_payload(key));
                let s = format!("+OK {}:{}:{}\r\n", ts.physical_ms, ts.logical, ts.node_id);
                out.extend_from_slice(s.as_bytes());
                false
            }
            Command::CrdtGet(key) => {
                if let Some(v) = db.crdt_get(key) {
                    write_resp_bulk(out, &v);
                } else {
                    write_resp_null(out);
                }
                false
            }
            Command::CrdtDel(key) => {
                let removed = db.crdt_del(key);
                if removed {
                    record_crdt_effect!(db.crdt_store.register_payload(key));
                    write_resp_integer(out, 1);
                } else {
                    write_resp_integer(out, 0);
                }
                false
            }
            Command::CrdtIncrby { key, delta } => {
                match db.crdt_incrby(key.clone(), *delta) {
                    Ok(val) => {
                        record_crdt_effect!(db.crdt_store.counter_payload(key));
                        write_resp_integer(out, val);
                    }
                    // Refused: the counter is unchanged, so nothing is logged.
                    Err(e) => out.extend_from_slice(format!("-ERR {e}\r\n").as_bytes()),
                }
                false
            }
            Command::CrdtSadd { key, member } => {
                // Even a re-add changes the set: it adds another tag.
                let added = db.crdt_sadd(key.clone(), member.clone());
                record_crdt_effect!(db.crdt_store.set_member_payload(key, member));
                write_resp_integer(out, if added { 1 } else { 0 });
                false
            }
            Command::CrdtSmembers(key) => {
                let members = db.crdt_smembers(key);
                out.extend_from_slice(format!("*{}\r\n", members.len()).as_bytes());
                for m in members {
                    write_resp_bulk(out, &m);
                }
                false
            }
            Command::CrdtSrem { key, member } => {
                let removed = db.crdt_srem(key, member);
                if !removed.is_empty() {
                    record_crdt_effect!(crate::crdt::CrdtStore::set_removal_payload(key, &removed));
                    write_resp_integer(out, 1);
                } else {
                    write_resp_integer(out, 0);
                }
                false
            }
            Command::CrdtDump => {
                let payload = db.crdt_dump();
                write_resp_bulk(out, &payload);
                false
            }
            Command::CrdtMerge(payload) => {
                match db.crdt_merge(payload) {
                    Ok(count) => {
                        // Merging is a pure function of the payload and the
                        // current state, so it is logged as issued.
                        record_change!(cmd);
                        write_resp_integer(out, count as i64);
                    }
                    Err(e) => {
                        let err_resp = format!("-ERR {}\r\n", e);
                        out.extend_from_slice(err_resp.as_bytes());
                    }
                }
                false
            }
            Command::CrdtGc(horizon) => {
                // Logged with its absolute cutoff: replaying a TTL later would
                // prune more than this did.
                let cutoff = horizon.cutoff_ms();
                let (regs, set_tombstones) = db.crdt_gc(cutoff);
                if regs + set_tombstones > 0 {
                    crate::replication::log_shard_mutation(db.port, db.shard_id, aof, || {
                        Command::CrdtGc(crate::crdt::GcHorizon::Before(cutoff))
                    });
                }
                out.extend_from_slice(b"*4\r\n");
                write_resp_bulk(out, b"registers_pruned");
                write_resp_integer(out, regs as i64);
                write_resp_bulk(out, b"set_tombstones_pruned");
                write_resp_integer(out, set_tombstones as i64);
                false
            }
            Command::SemanticSet {
                namespace,
                id,
                prompt,
                response,
                vector,
                ttl,
                scope,
                quantize,
                tokens,
            } => {
                match db.semantic_set(
                    namespace.clone(),
                    id.clone(),
                    prompt.clone(),
                    response.clone(),
                    (**vector).clone(),
                    *ttl,
                    scope.as_deref().cloned(),
                    *quantize,
                    *tokens,
                ) {
                    Ok(()) => {
                        record_change!(cmd);
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    Err(e) => {
                        write_resp_err(out, &e);
                    }
                }
                false
            }
            Command::SemanticGet {
                namespace,
                query,
                threshold,
                scope,
                with_score,
                with_prompt,
                with_id,
            } => {
                match db.semantic_get(namespace, query, *threshold, scope.as_deref()) {
                    Ok(Some(hit)) => {
                        if !*with_score && !*with_prompt && !*with_id {
                            write_resp_bulk(out, &hit.response);
                        } else {
                            let count = 1
                                + usize::from(*with_score)
                                + usize::from(*with_prompt)
                                + usize::from(*with_id);
                            write_resp_array_header(out, count);
                            write_resp_bulk(out, &hit.response);
                            if *with_score {
                                let s = format!("{:.6}", hit.score);
                                write_resp_bulk(out, s.as_bytes());
                            }
                            if *with_prompt {
                                write_resp_bulk(out, &hit.prompt);
                            }
                            if *with_id {
                                write_resp_bulk(out, &hit.id);
                            }
                        }
                    }
                    Ok(None) => {
                        write_resp_null(out);
                    }
                    Err(e) => {
                        write_resp_err(out, &e);
                    }
                }
                false
            }
            Command::SemanticDel { namespace, ids } => {
                let removed = db.semantic_del(namespace, ids);
                if removed > 0 {
                    record_change!(cmd);
                }
                write_resp_integer(out, removed as i64);
                false
            }
            Command::SemanticFlush(namespace) => {
                db.semantic_flush(namespace);
                record_change!(cmd);
                out.extend_from_slice(b"+OK\r\n");
                false
            }
            Command::SemanticInfo(namespace) => {
                let (entries, dim, hits, misses, tokens_saved, evicted_expired) =
                    db.semantic_info(namespace);
                let total_lookups = hits + misses;
                let hit_rate = if total_lookups > 0 {
                    format!("{:.4}", (hits as f64) / (total_lookups as f64))
                } else {
                    "0.0000".to_string()
                };
                out.extend_from_slice(b"*18\r\n");
                write_resp_bulk(out, b"namespace");
                write_resp_bulk(out, namespace);
                write_resp_bulk(out, b"entries");
                write_resp_integer(out, entries as i64);
                write_resp_bulk(out, b"dimension");
                write_resp_integer(out, dim as i64);
                write_resp_bulk(out, b"metric");
                write_resp_bulk(out, b"COSINE");
                write_resp_bulk(out, b"hits");
                write_resp_integer(out, hits as i64);
                write_resp_bulk(out, b"misses");
                write_resp_integer(out, misses as i64);
                write_resp_bulk(out, b"hit_rate");
                write_resp_bulk(out, hit_rate.as_bytes());
                write_resp_bulk(out, b"tokens_saved");
                write_resp_integer(out, tokens_saved as i64);
                write_resp_bulk(out, b"evicted_expired");
                write_resp_integer(out, evicted_expired as i64);
                false
            }
            Command::AgentMemAdd {
                session,
                role,
                content,
                tokens,
                vector,
                meta,
            } => {
                match db.agent_mem_add(
                    session.clone(),
                    role.clone(),
                    content.clone(),
                    *tokens,
                    vector.clone(),
                    meta.clone(),
                ) {
                    Ok(id) => {
                        record_change!(cmd);
                        write_resp_integer(out, id as i64);
                    }
                    Err(e) => write_resp_err(out, &e),
                }
                false
            }
            Command::AgentMemContext {
                session,
                max_tokens,
                query,
                recall_k,
            } => {
                match db.agent_mem_context(session, *max_tokens, query.as_deref(), *recall_k) {
                    Ok(ctx) => {
                        write_resp_array_header(out, 2);
                        write_resp_array_header(out, ctx.recent_turns.len());
                        for turn in ctx.recent_turns {
                            write_resp_array_header(out, 5);
                            write_resp_integer(out, turn.id as i64);
                            write_resp_bulk(out, &turn.role);
                            write_resp_bulk(out, &turn.content);
                            write_resp_integer(out, turn.tokens as i64);
                            match turn.meta {
                                Some(m) => write_resp_bulk(out, &m),
                                None => write_resp_null(out),
                            }
                        }
                        write_resp_array_header(out, ctx.recalled_episodes.len());
                        for ep in ctx.recalled_episodes {
                            write_resp_array_header(out, 5);
                            write_resp_integer(out, ep.id as i64);
                            write_resp_bulk(out, &ep.role);
                            write_resp_bulk(out, &ep.content);
                            let s = format!("{:.6}", ep.score);
                            write_resp_bulk(out, s.as_bytes());
                            match ep.meta {
                                Some(m) => write_resp_bulk(out, &m),
                                None => write_resp_null(out),
                            }
                        }
                    }
                    Err(e) => write_resp_err(out, &e),
                }
                false
            }
            Command::AgentMemCompact {
                session,
                keep_recent,
                summary,
                tokens,
                vector,
            } => {
                match db.agent_mem_compact(
                    session,
                    *keep_recent,
                    summary.clone(),
                    *tokens,
                    vector.clone(),
                ) {
                    Ok(compacted) => {
                        if compacted > 0 {
                            record_change!(cmd);
                        }
                        write_resp_integer(out, compacted as i64);
                    }
                    Err(e) => write_resp_err(out, &e),
                }
                false
            }
            Command::AgentMemInfo(session) => {
                let (active_turns, total_turns, active_tokens, dim, vecs, compactions) =
                    db.agent_mem_info(session);
                out.extend_from_slice(b"*14\r\n");
                write_resp_bulk(out, b"session");
                write_resp_bulk(out, session);
                write_resp_bulk(out, b"active_turns");
                write_resp_integer(out, active_turns as i64);
                write_resp_bulk(out, b"total_turns");
                write_resp_integer(out, total_turns as i64);
                write_resp_bulk(out, b"active_tokens");
                write_resp_integer(out, active_tokens as i64);
                write_resp_bulk(out, b"vector_dim");
                write_resp_integer(out, dim as i64);
                write_resp_bulk(out, b"episodic_vectors");
                write_resp_integer(out, vecs as i64);
                write_resp_bulk(out, b"compactions");
                write_resp_integer(out, compactions as i64);
                false
            }
            Command::AgentMemClear(session) => {
                let cleared = db.agent_mem_clear(session);
                if cleared {
                    record_change!(cmd);
                }
                write_resp_integer(out, i64::from(cleared));
                false
            }
            Command::LlmQuotaReserve {
                key,
                rpm,
                tpm,
                est_tokens,
                window_ms,
            } => {
                let res = db.llm_quota_reserve(key.clone(), *rpm, *tpm, *est_tokens, *window_ms);
                write_resp_array_header(out, 4);
                write_resp_integer(out, i64::from(res.allowed));
                match res.reservation_id {
                    Some(id) => write_resp_integer(out, id as i64),
                    None => write_resp_null(out),
                }
                write_resp_integer(out, res.remaining_tokens as i64);
                write_resp_integer(out, res.retry_after_ms as i64);
                false
            }
            Command::LlmQuotaSettle {
                key,
                reservation_id,
                actual_tokens,
            } => {
                let (found, delta) = db.llm_quota_settle(key, *reservation_id, *actual_tokens);
                write_resp_array_header(out, 2);
                write_resp_integer(out, i64::from(found));
                write_resp_integer(out, delta);
                false
            }
            Command::LlmQuotaInfo(key) => {
                let (active_reqs, used_tokens, reserved_tokens, active_res, window_ms) =
                    db.llm_quota_info(key);
                out.extend_from_slice(b"*12\r\n");
                write_resp_bulk(out, b"key");
                write_resp_bulk(out, key);
                write_resp_bulk(out, b"active_requests");
                write_resp_integer(out, active_reqs as i64);
                write_resp_bulk(out, b"used_tokens");
                write_resp_integer(out, used_tokens as i64);
                write_resp_bulk(out, b"reserved_tokens");
                write_resp_integer(out, reserved_tokens as i64);
                write_resp_bulk(out, b"active_reservations");
                write_resp_integer(out, active_res as i64);
                write_resp_bulk(out, b"window_ms");
                write_resp_integer(out, window_ms as i64);
                false
            }
            Command::AgentCheckpointPut {
                key,
                step_id,
                parent_id,
                state,
                meta,
            } => {
                let seq = db.agent_checkpoint_put(
                    key.clone(),
                    step_id.clone(),
                    parent_id.clone(),
                    state.clone(),
                    meta.clone(),
                );
                record_change!(cmd);
                write_resp_integer(out, seq as i64);
                false
            }
            Command::AgentCheckpointGet { key, step_id } => {
                match db.agent_checkpoint_get(key, step_id.as_ref()) {
                    Some(node) => {
                        write_resp_array_header(out, 10);
                        write_resp_bulk(out, b"step_id");
                        write_resp_bulk(out, &node.step_id);
                        write_resp_bulk(out, b"parent_id");
                        match node.parent_id {
                            Some(p) => write_resp_bulk(out, &p),
                            None => write_resp_null(out),
                        }
                        write_resp_bulk(out, b"seq");
                        write_resp_integer(out, node.seq as i64);
                        write_resp_bulk(out, b"state");
                        write_resp_bulk(out, &node.state);
                        write_resp_bulk(out, b"meta");
                        match node.metadata {
                            Some(m) => write_resp_bulk(out, &m),
                            None => write_resp_null(out),
                        }
                    }
                    None => write_resp_null(out),
                }
                false
            }
            Command::AgentCheckpointHistory {
                key,
                from_step,
                limit,
            } => {
                let history = db.agent_checkpoint_history(key, from_step.as_ref(), *limit);
                write_resp_array_header(out, history.len());
                for node in history {
                    write_resp_array_header(out, 10);
                    write_resp_bulk(out, b"step_id");
                    write_resp_bulk(out, &node.step_id);
                    write_resp_bulk(out, b"parent_id");
                    match node.parent_id {
                        Some(p) => write_resp_bulk(out, &p),
                        None => write_resp_null(out),
                    }
                    write_resp_bulk(out, b"seq");
                    write_resp_integer(out, node.seq as i64);
                    write_resp_bulk(out, b"state");
                    write_resp_bulk(out, &node.state);
                    write_resp_bulk(out, b"meta");
                    match node.metadata {
                        Some(m) => write_resp_bulk(out, &m),
                        None => write_resp_null(out),
                    }
                }
                false
            }
            Command::AgentToolClaim {
                key,
                call_id,
                ttl_ms,
                input,
            } => {
                let res = db.agent_tool_claim(key.clone(), call_id.clone(), *ttl_ms, input.clone());
                if res.state == crate::agent::ToolClaimState::Claimed {
                    record_change!(cmd);
                }
                write_resp_array_header(out, 3);
                let status_str = match res.state {
                    crate::agent::ToolClaimState::Claimed => b"CLAIMED" as &[u8],
                    crate::agent::ToolClaimState::InProgress => b"IN_PROGRESS" as &[u8],
                    crate::agent::ToolClaimState::Completed => b"COMPLETED" as &[u8],
                };
                write_resp_bulk(out, status_str);
                match res.output {
                    Some(o) => write_resp_bulk(out, &o),
                    None => write_resp_null(out),
                }
                write_resp_integer(out, res.meta_int as i64);
                false
            }
            Command::AgentToolComplete {
                key,
                call_id,
                output,
                ttl_ms,
            } => {
                let created =
                    db.agent_tool_complete(key.clone(), call_id.clone(), output.clone(), *ttl_ms);
                record_change!(cmd);
                write_resp_integer(out, i64::from(created));
                false
            }
            Command::Vadd {
                key,
                element,
                vector,
                metric,
                quantize,
                pq,
                tiered,
                reduce,
                quant,
                ef,
                setattr,
                m,
                cas: _,
                is_redis_vset,
            } => {
                let index = String::from_utf8_lossy(key);
                match db.vadd_ext(
                    &index,
                    element.clone(),
                    vector.clone(),
                    *metric,
                    *quantize,
                    *pq,
                    *tiered,
                    *reduce,
                    *quant,
                    *ef,
                    setattr.clone(),
                    *m,
                    *is_redis_vset,
                ) {
                    Ok(added) => {
                        record_change!(cmd);
                        notify_key_invalidation(db.port, key.as_ref(), 0);
                        notify_key_invalidation(db.port, element.as_ref(), 0);
                        if *is_redis_vset {
                            write_resp_integer(out, i64::from(added));
                        } else {
                            out.extend_from_slice(b"+OK\r\n");
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, &err);
                    }
                }
                false
            }
            Command::Vquery {
                key,
                k,
                query,
                rerank,
            } => {
                let index = String::from_utf8_lossy(key);
                let results = db.vquery(&index, query, *k, *rerank);
                out.extend_from_slice(format!("*{}\r\n", results.len() * 2).as_bytes());
                for (elem, dist) in results {
                    write_resp_bulk(out, &elem);
                    let s = format!("{:.6}", dist);
                    write_resp_bulk(out, s.as_bytes());
                }
                false
            }
            Command::Vdist {
                key,
                k1,
                k2,
                metric,
            } => {
                let index = String::from_utf8_lossy(key);
                match db.vsim(&index, k1, k2, *metric) {
                    Ok(dist) => {
                        let s = format!("{:.6}", dist);
                        write_resp_bulk(out, s.as_bytes());
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                    }
                }
                false
            }
            Command::Vsim {
                key,
                target,
                with_scores,
                with_attribs,
                count,
                epsilon,
                ef,
                filter,
                filter_ef,
                truth,
                no_thread: _,
            } => {
                let index = String::from_utf8_lossy(key);
                match db.vsim_ext(
                    &index,
                    target,
                    *with_attribs,
                    *count,
                    *epsilon,
                    *ef,
                    filter.as_deref(),
                    *filter_ef,
                    *truth,
                ) {
                    Ok(hits) => {
                        let is_resp3 = CURRENT_CLIENT_RESP3.get();
                        if !*with_scores && !*with_attribs {
                            write_resp_array_header(out, hits.len());
                            for (elem, _, _) in hits {
                                write_resp_bulk(out, &elem);
                            }
                        } else if *with_scores && !*with_attribs {
                            if is_resp3 {
                                out.extend_from_slice(format!("%{}\r\n", hits.len()).as_bytes());
                                for (elem, score, _) in hits {
                                    write_resp_bulk(out, &elem);
                                    write_resp_score(out, score as f64);
                                }
                            } else {
                                write_resp_array_header(out, hits.len() * 2);
                                for (elem, score, _) in hits {
                                    write_resp_bulk(out, &elem);
                                    write_resp_score(out, score as f64);
                                }
                            }
                        } else if !*with_scores && *with_attribs {
                            if is_resp3 {
                                out.extend_from_slice(format!("%{}\r\n", hits.len()).as_bytes());
                            } else {
                                write_resp_array_header(out, hits.len() * 2);
                            }
                            for (elem, _, attr) in hits {
                                write_resp_bulk(out, &elem);
                                match attr {
                                    Some(a) => write_resp_bulk(out, a.as_bytes()),
                                    None => write_resp_null(out),
                                }
                            }
                        } else if is_resp3 {
                            out.extend_from_slice(format!("%{}\r\n", hits.len()).as_bytes());
                            for (elem, score, attr) in hits {
                                write_resp_bulk(out, &elem);
                                out.extend_from_slice(b"*2\r\n");
                                write_resp_score(out, score as f64);
                                match attr {
                                    Some(a) => write_resp_bulk(out, a.as_bytes()),
                                    None => write_resp_null(out),
                                }
                            }
                        } else {
                            write_resp_array_header(out, hits.len() * 3);
                            for (elem, score, attr) in hits {
                                write_resp_bulk(out, &elem);
                                write_resp_score(out, score as f64);
                                match attr {
                                    Some(a) => write_resp_bulk(out, a.as_bytes()),
                                    None => write_resp_null(out),
                                }
                            }
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, &err);
                    }
                }
                false
            }
            Command::Vdel { key, element } => {
                if db.table.exists(key.as_ref()) {
                    write_resp_err(
                        out,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                    return false;
                }
                let index = String::from_utf8_lossy(key);
                let removed = db.vdel(&index, element);
                if removed {
                    record_change!(cmd);
                    notify_key_invalidation(db.port, key.as_ref(), 0);
                    write_resp_integer(out, 1);
                } else {
                    write_resp_integer(out, 0);
                }
                false
            }
            Command::Vinfo(key) => {
                if db.table.exists(key.as_ref()) {
                    write_resp_err(
                        out,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                    return false;
                }
                let index = String::from_utf8_lossy(key);
                if let Some(idx) = db.vector_indexes.get(index.as_ref()) {
                    if idx.is_redis_vset {
                        if CURRENT_CLIENT_RESP3.get() {
                            out.extend_from_slice(b"%9\r\n");
                        } else {
                            out.extend_from_slice(b"*18\r\n");
                        }
                        write_resp_bulk(out, b"quant-type");
                        write_resp_bulk(out, idx.quant.as_str().as_bytes());
                        write_resp_bulk(out, b"hnsw-m");
                        write_resp_integer(out, idx.m as i64);
                        write_resp_bulk(out, b"vector-dim");
                        write_resp_integer(out, idx.dim as i64);
                        write_resp_bulk(out, b"projection-input-dim");
                        write_resp_integer(out, idx.input_dim as i64);
                        write_resp_bulk(out, b"size");
                        write_resp_integer(out, idx.len() as i64);
                        write_resp_bulk(out, b"max-level");
                        write_resp_integer(out, idx.max_layer as i64);
                        write_resp_bulk(out, b"attributes-count");
                        write_resp_integer(out, idx.attributes.len() as i64);
                        write_resp_bulk(out, b"vset-uid");
                        write_resp_integer(out, idx.uid as i64);
                        write_resp_bulk(out, b"hnsw-max-node-uid");
                        write_resp_integer(out, idx.max_node_uid() as i64);
                    } else {
                        out.extend_from_slice(b"*8\r\n");
                        write_resp_bulk(out, b"num_elements");
                        write_resp_integer(out, idx.len() as i64);
                        write_resp_bulk(out, b"dimension");
                        write_resp_integer(out, idx.dim as i64);
                        write_resp_bulk(out, b"metric");
                        write_resp_bulk(out, idx.metric.as_str().as_bytes());
                        write_resp_bulk(out, b"max_layer");
                        write_resp_integer(out, idx.max_layer as i64);
                    }
                } else {
                    write_resp_null(out);
                }
                false
            }
            Command::Vcard(key) => {
                if db.table.exists(key.as_ref()) {
                    write_resp_err(
                        out,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                    return false;
                }
                let index = String::from_utf8_lossy(key);
                let card = db
                    .vector_indexes
                    .get(index.as_ref())
                    .map(|idx| idx.len())
                    .unwrap_or(0);
                write_resp_integer(out, card as i64);
                false
            }
            Command::Vdim(key) => {
                if db.table.exists(key.as_ref()) {
                    write_resp_err(
                        out,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                    return false;
                }
                let index = String::from_utf8_lossy(key);
                if let Some(idx) = db.vector_indexes.get(index.as_ref()) {
                    write_resp_integer(out, idx.client_dim() as i64);
                } else {
                    write_resp_err(out, "key does not exist");
                }
                false
            }
            Command::Vemb { key, element, raw } => {
                if db.table.exists(key.as_ref()) {
                    write_resp_err(
                        out,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                    return false;
                }
                let index = String::from_utf8_lossy(key);
                let Some(idx) = db.vector_indexes.get(index.as_ref()) else {
                    write_resp_null(out);
                    return false;
                };
                if *raw {
                    if let Some((qtype, blob, norm, q8_range)) = idx.raw_embedding(element) {
                        let count = if q8_range.is_some() { 4 } else { 3 };
                        write_resp_array_header(out, count);
                        write_resp_bulk(out, qtype.as_bytes());
                        write_resp_bulk(out, &blob);
                        write_resp_score(out, norm as f64);
                        if let Some(r) = q8_range {
                            write_resp_score(out, r as f64);
                        }
                    } else {
                        write_resp_null(out);
                    }
                } else if let Some(vec) = idx.stored_vector(element) {
                    write_resp_array_header(out, vec.len());
                    for v in vec {
                        write_resp_score(out, v as f64);
                    }
                } else {
                    write_resp_null(out);
                }
                false
            }
            Command::Vlinks {
                key,
                element,
                with_scores,
            } => {
                if db.table.exists(key.as_ref()) {
                    write_resp_err(
                        out,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                    return false;
                }
                let index = String::from_utf8_lossy(key);
                let Some(idx) = db.vector_indexes.get(index.as_ref()) else {
                    write_resp_null(out);
                    return false;
                };
                if let Some(layers) = idx.links(element) {
                    let is_resp3 = CURRENT_CLIENT_RESP3.get();
                    write_resp_array_header(out, layers.len());
                    for layer in layers {
                        if *with_scores {
                            if is_resp3 {
                                out.extend_from_slice(format!("%{}\r\n", layer.len()).as_bytes());
                            } else {
                                write_resp_array_header(out, layer.len() * 2);
                            }
                            for (nbr, score) in layer {
                                write_resp_bulk(out, &nbr);
                                write_resp_score(out, score as f64);
                            }
                        } else {
                            write_resp_array_header(out, layer.len());
                            for (nbr, _) in layer {
                                write_resp_bulk(out, &nbr);
                            }
                        }
                    }
                } else {
                    write_resp_null(out);
                }
                false
            }
            Command::Vrandmember { key, count } => {
                if db.table.exists(key.as_ref()) {
                    write_resp_err(
                        out,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                    return false;
                }
                let index = String::from_utf8_lossy(key);
                match count {
                    None => {
                        if let Some(idx) = db.vector_indexes.get_mut(index.as_ref())
                            && let Some(first) = idx.random_members(1).into_iter().next()
                        {
                            write_resp_bulk(out, &first);
                        } else {
                            write_resp_null(out);
                        }
                    }
                    Some(c) => {
                        if let Some(idx) = db.vector_indexes.get_mut(index.as_ref()) {
                            let members = idx.random_members(*c);
                            write_resp_array_header(out, members.len());
                            for m in members {
                                write_resp_bulk(out, &m);
                            }
                        } else {
                            out.extend_from_slice(b"*0\r\n");
                        }
                    }
                }
                false
            }
            Command::Vsetattr { key, element, attr } => {
                let index = String::from_utf8_lossy(key);
                match db.vsetattr(&index, element, attr.clone()) {
                    Ok(updated) => {
                        if updated {
                            record_change!(cmd);
                            notify_key_invalidation(db.port, key.as_ref(), 0);
                        }
                        write_resp_integer(out, i64::from(updated));
                    }
                    Err(err) => {
                        write_resp_err(out, &err);
                    }
                }
                false
            }
            Command::Vgetattr { key, element } => {
                let index = String::from_utf8_lossy(key);
                match db.vgetattr(&index, element) {
                    Ok(Some(attr)) => {
                        write_resp_bulk(out, attr.as_bytes());
                    }
                    Ok(None) => {
                        write_resp_null(out);
                    }
                    Err(err) => {
                        write_resp_err(out, &err);
                    }
                }
                false
            }
            Command::Vismember { key, element } => {
                if db.table.exists(key.as_ref()) {
                    write_resp_err(
                        out,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                    return false;
                }
                let index = String::from_utf8_lossy(key);
                let is_mem = db
                    .vector_indexes
                    .get(index.as_ref())
                    .is_some_and(|idx| idx.key_to_id.contains_key(element));
                write_resp_integer(out, i64::from(is_mem));
                false
            }
            Command::Latency(sub) => {
                write_latency_response(sub, out);
                false
            }
            Command::PubsubHelp => {
                let help_lines = [
                    "PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "CHANNELS [<pattern>]",
                    "    Return the currently active channels matching a <pattern> (default: '*').",
                    "NUMPAT",
                    "    Return number of subscriptions to patterns (that are performed using",
                    "    the PSUBSCRIBE command).",
                    "NUMSUB [<channel> ...]",
                    "    Return the number of subscribers for the specified channels, excluding",
                    "    pattern subscriptions(default: no channels).",
                    "SHARDCHANNELS [<pattern>]",
                    "    Return the currently active shard level channels matching a <pattern> (default: '*').",
                    "SHARDNUMSUB [<shardchannel> ...]",
                    "    Return the number of subscribers for the specified shard level channels.",
                    "HELP",
                    "    Print this help.",
                ];
                out.extend_from_slice(format!("*{}\r\n", help_lines.len()).as_bytes());
                for line in help_lines {
                    write_resp_status(out, line);
                }
                false
            }
            Command::FunctionStats => {
                let libs = crate::scripting::list_functions();
                let lib_count = libs.len();
                let func_count: usize = libs.iter().map(|l| l.functions.len()).sum();
                if CURRENT_CLIENT_RESP3.get() {
                    out.extend_from_slice(b"%2\r\n");
                } else {
                    out.extend_from_slice(b"*4\r\n");
                }
                write_resp_bulk(out, b"running_script");
                write_function_stats_running(out, db.port);
                write_resp_bulk(out, b"engines");
                if CURRENT_CLIENT_RESP3.get() {
                    out.extend_from_slice(b"%1\r\n");
                } else {
                    out.extend_from_slice(b"*2\r\n");
                }
                write_resp_bulk(out, b"LUA");
                if CURRENT_CLIENT_RESP3.get() {
                    out.extend_from_slice(b"%2\r\n");
                } else {
                    out.extend_from_slice(b"*4\r\n");
                }
                write_resp_bulk(out, b"libraries_count");
                write_resp_integer(out, lib_count as i64);
                write_resp_bulk(out, b"functions_count");
                write_resp_integer(out, func_count as i64);
                false
            }
            Command::Hexpire {
                key,
                expire_ms,
                is_at,
                condition,
                fields,
            } => {
                match db
                    .table
                    .hexpire(key, *expire_ms, *is_at, *condition, fields)
                {
                    Ok(res) => {
                        record_change!(cmd);
                        notify_hexpire_result(db, key, fields, &res);
                        write_resp_array_header(out, res.len());
                        for code in res {
                            write_resp_integer(out, code);
                        }
                    }
                    Err(e) => write_resp_err(out, e),
                }
                false
            }
            Command::Httl {
                key,
                is_ms,
                is_expiretime,
                fields,
            } => {
                match db.table.httl(key, *is_ms, *is_expiretime, fields) {
                    Ok(res) => {
                        write_resp_array_header(out, res.len());
                        for code in res {
                            write_resp_integer(out, code);
                        }
                    }
                    Err(e) => write_resp_err(out, e),
                }
                false
            }
            Command::Hpersist { key, fields } => {
                match db.table.hpersist(key, fields) {
                    Ok(res) => {
                        record_change!(cmd);
                        if res.contains(&1) {
                            notify_hpersist_result(key, fields, &res);
                        }
                        write_resp_array_header(out, res.len());
                        for code in res {
                            write_resp_integer(out, code);
                        }
                    }
                    Err(e) => write_resp_err(out, e),
                }
                false
            }
            Command::Hgetex {
                key,
                expire,
                fields,
            } => {
                match db.table.hgetex(key, *expire, fields) {
                    Ok((vals, event)) => {
                        if let Some((event, changed)) = event {
                            record_change!(cmd);
                            notify_hash_event_then_del(db, key, event, &changed);
                        }
                        write_resp_array_header(out, vals.len());
                        for v in vals {
                            match v {
                                Some(val) => write_resp_bulk(out, &val),
                                None => write_resp_null(out),
                            }
                        }
                    }
                    Err(e) => write_resp_err(out, e),
                }
                false
            }
            Command::Hsetex {
                key,
                condition,
                expire,
                pairs,
            } => {
                let existed = db.exists(key);
                match db
                    .table
                    .hsetex(key.clone(), *condition, *expire, pairs.clone())
                {
                    Ok(applied) => {
                        if applied {
                            record_change!(cmd);
                            notify_hsetex_applied(db, key, expire, pairs);
                            write_resp_integer(out, 1);
                        } else {
                            // Nothing set, but expired fields may have been
                            // the last ones: the hash is gone.
                            if existed {
                                notify_del_if_gone(db, key);
                            }
                            write_resp_integer(out, 0);
                        }
                    }
                    Err(e) => write_resp_err(out, e),
                }
                false
            }
            Command::Xclaim {
                key,
                group,
                consumer,
                min_idle_time,
                ids,
                idle,
                time,
                retrycount,
                force,
                justid,
                lastid,
            } => {
                match db.table.xclaim(
                    key,
                    group,
                    consumer.clone(),
                    *min_idle_time,
                    ids,
                    *idle,
                    *time,
                    *retrycount,
                    *force,
                    *justid,
                    lastid.as_deref().copied(),
                ) {
                    Ok(outcome) => {
                        if outcome.consumer_created {
                            notify_keyspace_event(NOTIFY_STREAM, "xgroup-createconsumer", key);
                        }
                        // Like Redis, XCLAIM is never propagated itself, but
                        // as one idempotent XCLAIM per changed PEL entry.
                        let props = stream_claim_propagation(db, key, group, consumer, &outcome);
                        if !props.is_empty() {
                            crate::snapshot::note_changes(props.len() as u64);
                        }
                        for p in &props {
                            record_change!(p);
                        }
                        write_resp_array_header(out, outcome.claimed.len());
                        for (sid, fields) in outcome.claimed {
                            let sid_str = sid.to_string();
                            if *justid {
                                write_resp_bulk(out, sid_str.as_bytes());
                            } else {
                                write_resp_array_header(out, 2);
                                write_resp_bulk(out, sid_str.as_bytes());
                                write_resp_array_header(out, fields.len() * 2);
                                for (f, v) in fields {
                                    write_resp_bulk(out, &f);
                                    write_resp_bulk(out, &v);
                                }
                            }
                        }
                    }
                    Err(e) => write_resp_err(out, e),
                }
                false
            }
            Command::Xautoclaim {
                key,
                group,
                consumer,
                min_idle_time,
                start,
                count,
                justid,
            } => {
                match db.table.xautoclaim(
                    key,
                    group,
                    consumer.clone(),
                    *min_idle_time,
                    start,
                    *count,
                    *justid,
                ) {
                    Ok((next_cursor, outcome)) => {
                        if outcome.consumer_created {
                            notify_keyspace_event(NOTIFY_STREAM, "xgroup-createconsumer", key);
                        }
                        let props = stream_claim_propagation(db, key, group, consumer, &outcome);
                        if !props.is_empty() {
                            crate::snapshot::note_changes(props.len() as u64);
                        }
                        for p in &props {
                            record_change!(p);
                        }
                        write_resp_array_header(out, 3);
                        write_resp_bulk(out, next_cursor.as_bytes());
                        write_resp_array_header(out, outcome.claimed.len());
                        for (sid, fields) in outcome.claimed {
                            let sid_str = sid.to_string();
                            if *justid {
                                write_resp_bulk(out, sid_str.as_bytes());
                            } else {
                                write_resp_array_header(out, 2);
                                write_resp_bulk(out, sid_str.as_bytes());
                                write_resp_array_header(out, fields.len() * 2);
                                for (f, v) in fields {
                                    write_resp_bulk(out, &f);
                                    write_resp_bulk(out, &v);
                                }
                            }
                        }
                        write_resp_array_header(out, outcome.deleted.len());
                        for (del_sid, _) in outcome.deleted {
                            let s = del_sid.to_string();
                            write_resp_bulk(out, s.as_bytes());
                        }
                    }
                    Err(e) => write_resp_err(out, e),
                }
                false
            }
            // From a master or the AOF: function libraries are server-wide.
            Command::FunctionLoad { replace, code } => {
                match crate::scripting::load_function(&String::from_utf8_lossy(code), *replace) {
                    Ok(lib_name) => {
                        record_change!(cmd);
                        write_resp_bulk(out, lib_name.as_bytes());
                    }
                    Err(err) => write_resp_err(out, err),
                }
                false
            }
            Command::FunctionDelete(lib) => {
                if crate::scripting::delete_function(lib) {
                    record_change!(cmd);
                    out.extend_from_slice(b"+OK\r\n");
                } else {
                    write_resp_err(out, "ERR Library not found");
                }
                false
            }
            Command::FunctionFlush => {
                crate::scripting::flush_functions();
                record_change!(cmd);
                out.extend_from_slice(b"+OK\r\n");
                false
            }
            Command::FunctionRestore { payload, policy } => {
                match crate::scripting::restore_functions(payload, policy) {
                    Ok(()) => {
                        record_change!(cmd);
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    Err(e) => write_resp_err(out, e),
                }
                false
            }
            // From this shard's AOF: every shard logs the index definitions
            // (see `Router::create_search_index`), so replay (re)defines the
            // index on this shard alone and re-indexes its keys.
            Command::FtCreate {
                index,
                on_type,
                prefixes,
                fields,
                schema_fields,
            } => {
                db.define_search_index(crate::search::IndexSchema {
                    name: index.clone(),
                    on_type: on_type.clone(),
                    prefixes: prefixes.clone(),
                    fields: fields.clone(),
                    schema_fields: schema_fields.clone(),
                });
                record_change!(cmd);
                out.extend_from_slice(b"+OK\r\n");
                false
            }
            Command::FtAlter {
                index,
                fields,
                schema_fields,
            } => {
                match db.alter_search_index(index, fields.clone(), schema_fields.clone()) {
                    Ok(()) => {
                        record_change!(cmd);
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    Err(e) => write_resp_err(out, format!("ERR {}", e)),
                }
                false
            }
            Command::FtDropIndex { index, .. } => {
                let dropped = db.drop_search_index(index);
                if crate::search::drop_search_index(index).is_ok() || dropped {
                    record_change!(cmd);
                    out.extend_from_slice(b"+OK\r\n");
                } else {
                    write_resp_err(out, format!("ERR Unknown Index name: {}", index));
                }
                false
            }
            Command::FunctionKill => {
                out.extend_from_slice(b"+OK\r\n");
                false
            }
            // From a script, or a replica applying its master's stream.
            Command::Publish { channel, message } => {
                propagate_publish(b"PUBLISH", channel, message);
                let count = publish_sync(channel.as_ref(), message.as_ref());
                write_resp_integer(out, count as i64);
                false
            }
            Command::Spublish { channel, message } => {
                propagate_publish(b"SPUBLISH", channel, message);
                let count = spublish_sync(channel.as_ref(), message.as_ref());
                write_resp_integer(out, count as i64);
                false
            }
            Command::Unknown(cmd_name) => {
                if let Some(cmd_prefix) = cmd_name.strip_suffix(" HELP") {
                    let first_line = format!(
                        "{} <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                        cmd_prefix
                    );
                    let help_lines = [first_line.as_str(), "HELP -- Print this help."];
                    write_resp_array_header(out, help_lines.len());
                    for line in help_lines {
                        write_resp_status(out, line);
                    }
                    return false;
                }
                let resp = format!("-ERR unknown command '{}'\r\n", err_arg(cmd_name));
                out.extend_from_slice(resp.as_bytes());
                false
            }
            Command::Wait { .. } => {
                out.extend_from_slice(b":0\r\n");
                false
            }
            Command::WaitAof { .. } => {
                out.extend_from_slice(b"*2\r\n:0\r\n:0\r\n");
                false
            }
            Command::Quit => {
                out.extend_from_slice(b"+OK\r\n");
                true
            }
            _ => false,
        }
    })();
    db.drain_dropped_tier();
    ret
}
