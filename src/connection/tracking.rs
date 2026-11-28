use super::*;

#[allow(clippy::too_many_arguments)]
pub fn register_client_tracking(
    port: u16,
    client_id: u64,
    redirect: Option<i64>,
    bcast: bool,
    mut prefixes: Vec<Bytes>,
    optin: bool,
    optout: bool,
    noloop: bool,
) -> Result<(), String> {
    let redirect_id = match redirect {
        Some(id) if id < 0 => {
            return Err("REDIRECT client ID must be a positive integer".to_string());
        }
        Some(id) => id as u64,
        None => 0,
    };
    if redirect_id > 0 {
        let clients = GLOBAL_CLIENTS.read();
        if !clients.contains_key(&(port, redirect_id)) {
            return Err("The client ID you want to redirect to does not exist".to_string());
        }
    }

    if bcast && prefixes.is_empty() {
        prefixes.push(Bytes::new());
    }

    // Check overlap among provided prefixes
    if bcast {
        for i in 0..prefixes.len() {
            for j in (i + 1)..prefixes.len() {
                if prefixes[i].starts_with(&prefixes[j]) || prefixes[j].starts_with(&prefixes[i]) {
                    return Err(format!(
                        "Prefix '{}' overlaps with another provided prefix '{}'",
                        String::from_utf8_lossy(&prefixes[i]),
                        String::from_utf8_lossy(&prefixes[j])
                    ));
                }
            }
        }
    }

    let mut map = TRACKING_CLIENTS.write();
    if let Some(existing) = map.get_mut(&(port, client_id))
        && existing.bcast
        && bcast
    {
        for new_p in &prefixes {
            for old_p in &existing.prefixes {
                if new_p != old_p
                    && (new_p.starts_with(old_p.as_ref()) || old_p.starts_with(new_p.as_ref()))
                {
                    return Err(format!(
                        "Prefix '{}' overlaps with an existing prefix '{}'",
                        String::from_utf8_lossy(new_p),
                        String::from_utf8_lossy(old_p)
                    ));
                }
            }
        }
        for new_p in prefixes {
            if !existing.prefixes.contains(&new_p) {
                existing.prefixes.push(new_p);
            }
        }
        existing.redirect_id = redirect_id;
        existing.broken_redirect = false;
        existing.noloop = noloop;
        HAS_TRACKING_CLIENTS.store(true, std::sync::atomic::Ordering::Release);
        return Ok(());
    }

    map.insert(
        (port, client_id),
        ClientTracker {
            port,
            client_id,
            redirect_id,
            broken_redirect: false,
            bcast,
            prefixes,
            optin,
            optout,
            noloop,
            caching: None,
            tracked_keys: hashbrown::HashSet::new(),
            pending_bcast: Vec::new(),
        },
    );
    HAS_TRACKING_CLIENTS.store(!map.is_empty(), std::sync::atomic::Ordering::Release);
    Ok(())
}

pub fn unregister_client_tracking(port: u16, client_id: u64) {
    if !HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let mut map = TRACKING_CLIENTS.write();
    if map.remove(&(port, client_id)).is_some() {
        HAS_TRACKING_CLIENTS.store(!map.is_empty(), std::sync::atomic::Ordering::Release);
    }
}

pub fn set_client_caching(
    port: u16,
    client_id: u64,
    yes_opt: Option<bool>,
) -> Result<(), &'static str> {
    if !HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
        return Err(
            "CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled",
        );
    }
    let mut map = TRACKING_CLIENTS.write();
    let Some(tracker) = map.get_mut(&(port, client_id)) else {
        return Err(
            "CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled",
        );
    };
    if (!tracker.optin && !tracker.optout) || tracker.bcast {
        return Err(
            "CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled",
        );
    }
    let Some(yes) = yes_opt else {
        return Err("syntax error");
    };
    if yes && !tracker.optin {
        return Err("CLIENT CACHING YES is only valid when OPTIN is enabled");
    }
    if !yes && !tracker.optout {
        return Err("CLIENT CACHING NO is only valid when OPTOUT is enabled");
    }
    tracker.caching = Some(yes);
    Ok(())
}

#[inline(always)]
pub fn reset_client_caching(port: u16, client_id: u64) {
    if !HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let mut map = TRACKING_CLIENTS.write();
    if let Some(tracker) = map.get_mut(&(port, client_id)) {
        tracker.caching = None;
    }
}

pub fn get_client_redir(port: u16, client_id: u64) -> i64 {
    if !HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
        return -1;
    }
    let map = TRACKING_CLIENTS.read();
    if let Some(tracker) = map.get(&(port, client_id)) {
        tracker.redirect_id as i64
    } else {
        -1
    }
}

pub fn write_client_tracking_info(port: u16, client_id: u64, is_resp3: bool, out: &mut Vec<u8>) {
    let map = TRACKING_CLIENTS.read();
    let tracker_opt = map.get(&(port, client_id));

    let mut flags: Vec<&'static str> = Vec::new();
    let redirect: i64;
    let mut prefixes: Vec<Bytes> = Vec::new();

    if let Some(t) = tracker_opt {
        flags.push("on");
        if t.bcast {
            flags.push("bcast");
        }
        if t.optin {
            flags.push("optin");
        }
        if t.optout {
            flags.push("optout");
        }
        if t.caching == Some(true) {
            flags.push("caching-yes");
        } else if t.caching == Some(false) {
            flags.push("caching-no");
        }
        if t.noloop {
            flags.push("noloop");
        }
        if t.broken_redirect {
            flags.push("broken_redirect");
        }
        redirect = t.redirect_id as i64;
        if t.bcast {
            prefixes = t.prefixes.clone();
        }
    } else {
        flags.push("off");
        redirect = -1;
    }

    if is_resp3 {
        out.extend_from_slice(b"%3\r\n");
    } else {
        out.extend_from_slice(b"*6\r\n");
    }
    write_resp_bulk(out, b"flags");
    if is_resp3 {
        out.extend_from_slice(format!("~{}\r\n", flags.len()).as_bytes());
    } else {
        out.extend_from_slice(format!("*{}\r\n", flags.len()).as_bytes());
    }
    for f in flags {
        write_resp_bulk(out, f.as_bytes());
    }
    write_resp_bulk(out, b"redirect");
    write_resp_integer(out, redirect);
    write_resp_bulk(out, b"prefixes");
    out.extend_from_slice(format!("*{}\r\n", prefixes.len()).as_bytes());
    for p in prefixes {
        write_resp_bulk(out, &p);
    }
}

pub fn get_tracking_info_stats(port: u16) -> (usize, usize, usize, usize) {
    if !HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
        return (0, 0, 0, 0);
    }
    let map = TRACKING_CLIENTS.read();
    let mut tracking_clients = 0usize;
    let mut tracking_total_items = 0usize;
    let mut unique_keys: hashbrown::HashSet<&[u8]> = hashbrown::HashSet::new();
    let mut tracking_total_prefixes = 0usize;

    for tracker in map.values() {
        if tracker.port != port {
            continue;
        }
        tracking_clients += 1;
        if tracker.bcast {
            tracking_total_prefixes += tracker.prefixes.len();
        } else {
            tracking_total_items += tracker.tracked_keys.len();
            for k in &tracker.tracked_keys {
                unique_keys.insert(k.as_slice());
            }
        }
    }
    (
        tracking_clients,
        tracking_total_items,
        unique_keys.len(),
        tracking_total_prefixes,
    )
}

pub(super) fn send_raw_to_client_entry(target_id: u64, entry: &GlobalClientEntry, msg: Vec<u8>) {
    let captured = EVICT_CAPTURE.with(|c| match c.borrow_mut().as_mut() {
        Some((id, buf)) if *id == target_id => {
            buf.extend_from_slice(&msg);
            true
        }
        _ => false,
    });
    if captured {
        return;
    }
    let exec_cid = {
        let local = EXECUTING_CLIENT_ID.get();
        if local != 0 {
            local
        } else {
            // A shard running another shard's batch for this client.
            CURRENT_CLIENT_ID.get()
        }
    };
    if target_id == exec_cid {
        let _ = entry.track_tx.send(msg);
    } else if !entry.push.push(&msg) {
        // Never dropped: a client that missed an invalidation would keep
        // serving the key from its cache. As Redis disconnects a client
        // whose output it can't hold, close it; a client drops its cache
        // when it loses its connection.
        tracking_client_overflowed(target_id, entry);
    }
}

/// Closes tracking client `id`: an invalidation for it couldn't be
/// delivered (its socket full, or a partial write).
pub(super) fn tracking_client_overflowed(id: u64, entry: &GlobalClientEntry) {
    use std::sync::atomic::Ordering::Relaxed;
    if entry
        .stats
        .killed
        .compare_exchange(false, true, Relaxed, Relaxed)
        .is_err()
    {
        return;
    }
    ACTIVE_CLIENTS.fetch_sub(1, Relaxed);
    crate::server_stats::add(
        crate::server_stats::Stat::ClientOutputBufferLimitDisconnections,
        1,
    );
    crate::log_warning!(
        "Client id={} addr={} closed: a tracking invalidation for it could not be delivered (its output is full)",
        id,
        entry.stats.addr
    );
    // SAFETY: shutdown(2) takes no pointers; the entry is read under the
    // GLOBAL_CLIENTS lock (see close_client_over_output_limit).
    unsafe {
        libc::shutdown(entry.raw_fd, libc::SHUT_RDWR);
    }
}

pub(super) fn format_invalidation_msg(
    is_resp3: bool,
    is_redir_or_pubsub: bool,
    keys: Option<&[&[u8]]>,
) -> Option<Vec<u8>> {
    if is_resp3 {
        let mut msg = Vec::new();
        match keys {
            Some(ks) => {
                msg.extend_from_slice(
                    format!(">2\r\n$10\r\ninvalidate\r\n*{}\r\n", ks.len()).as_bytes(),
                );
                for k in ks {
                    write_resp_bulk(&mut msg, k);
                }
            }
            None => {
                msg.extend_from_slice(b">2\r\n$10\r\ninvalidate\r\n_\r\n");
            }
        }
        Some(msg)
    } else if is_redir_or_pubsub {
        let mut msg = Vec::new();
        match keys {
            Some(ks) => {
                msg.extend_from_slice(
                    format!(
                        "*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*{}\r\n",
                        ks.len()
                    )
                    .as_bytes(),
                );
                for k in ks {
                    write_resp_bulk(&mut msg, k);
                }
            }
            None => {
                msg.extend_from_slice(
                    b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n$-1\r\n",
                );
            }
        }
        Some(msg)
    } else {
        None
    }
}

pub(super) fn deliver_invalidation_to_tracker(
    tracker: &mut ClientTracker,
    clients: &hashbrown::HashMap<(u16, u64), GlobalClientEntry>,
    keys: Option<&[&[u8]]>,
) {
    let target_id = if tracker.redirect_id > 0 {
        tracker.redirect_id
    } else {
        tracker.client_id
    };
    if let Some(target_entry) = clients.get(&(tracker.port, target_id)) {
        let is_redir_or_pubsub = tracker.redirect_id > 0 || target_entry.is_pubsub;
        if let Some(msg) = format_invalidation_msg(target_entry.is_resp3, is_redir_or_pubsub, keys)
        {
            send_raw_to_client_entry(target_id, target_entry, msg);
        }
    } else if tracker.redirect_id > 0 {
        tracker.broken_redirect = true;
        if let Some(owner_entry) = clients.get(&(tracker.port, tracker.client_id))
            && owner_entry.is_resp3
        {
            let msg = format!(
                ">2\r\n$21\r\ntracking-redir-broken\r\n:{}\r\n",
                tracker.redirect_id
            )
            .into_bytes();
            send_raw_to_client_entry(tracker.client_id, owner_entry, msg);
        }
    }
}

pub(super) fn can_tracker_read_key(
    port: u16,
    client_id: u64,
    clients: &hashbrown::HashMap<(u16, u64), GlobalClientEntry>,
    key: &[u8],
) -> bool {
    let user_name = clients
        .get(&(port, client_id))
        .map(|c| c.auth_user.as_str())
        .unwrap_or("default");
    if !crate::acl::HAS_CUSTOM_ACL.load(std::sync::atomic::Ordering::Relaxed)
        && user_name == "default"
    {
        return true;
    }
    let acl = crate::acl::get_acl_for_port(port);
    let guard = acl.read();
    guard
        .users
        .get(user_name)
        .map(|u| u.can_access_key(key, crate::acl::KEY_READ))
        .unwrap_or(true)
}

pub fn enforce_tracking_max_keys(port: u16) {
    if !HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let max_keys = TRACKING_TABLE_MAX_KEYS.load(std::sync::atomic::Ordering::Relaxed);
    let mut map = TRACKING_CLIENTS.write();
    let clients = GLOBAL_CLIENTS.read();

    loop {
        let total_keys: usize = map
            .values()
            .filter(|t| t.port == port && !t.bcast)
            .map(|t| t.tracked_keys.len())
            .sum();
        if total_keys <= max_keys {
            break;
        }
        let mut evicted_any = false;
        for tracker in map.values_mut() {
            if tracker.port != port || tracker.bcast || tracker.tracked_keys.is_empty() {
                continue;
            }
            if let Some(k) = tracker.tracked_keys.iter().next().cloned() {
                tracker.tracked_keys.remove(&k);
                deliver_invalidation_to_tracker(tracker, &clients, Some(&[k.as_slice()]));
                evicted_any = true;
                break;
            }
        }
        if !evicted_any {
            break;
        }
    }
}

#[inline(always)]
pub fn record_client_read(port: u16, client_id: u64, key: &[u8]) {
    if !HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) || client_id == 0 {
        return;
    }
    let needs_evict = {
        let mut map = TRACKING_CLIENTS.write();
        if let Some(tracker) = map.get_mut(&(port, client_id))
            && !tracker.bcast
        {
            if tracker.optin && tracker.caching != Some(true) {
                return;
            }
            if tracker.optout && tracker.caching == Some(false) {
                return;
            }
            tracker.tracked_keys.insert(key.to_vec());
            let max_keys = TRACKING_TABLE_MAX_KEYS.load(std::sync::atomic::Ordering::Relaxed);
            tracker.tracked_keys.len() > max_keys
        } else {
            false
        }
    };
    if needs_evict {
        enforce_tracking_max_keys(port);
    }
}

pub fn notify_key_invalidation(port: u16, key: &[u8], sender_client_id: u64) {
    notify_key_invalidation_in_db(port, CURRENT_DB.get(), key, sender_client_id);
}

pub fn notify_key_invalidation_in_db(port: u16, db: usize, key: &[u8], sender_client_id: u64) {
    touch_watched_key_in_db(port, db, key);
    if !HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let defer_bcast = DEFER_BCAST_FLUSH.get();
    let mut map = TRACKING_CLIENTS.write();
    let clients = GLOBAL_CLIENTS.read();
    for tracker in map.values_mut() {
        if tracker.port != port {
            continue;
        }
        if tracker.noloop && sender_client_id != 0 && tracker.client_id == sender_client_id {
            if !tracker.bcast {
                tracker.tracked_keys.remove(key);
            }
            continue;
        }
        if tracker.bcast {
            if !can_tracker_read_key(port, tracker.client_id, &clients, key) {
                continue;
            }
            for pfx_idx in 0..tracker.prefixes.len() {
                if key.starts_with(&tracker.prefixes[pfx_idx]) {
                    if defer_bcast {
                        let pfx = tracker.prefixes[pfx_idx].clone();
                        if let Some((_, list)) =
                            tracker.pending_bcast.iter_mut().find(|(p, _)| *p == pfx)
                        {
                            if !list.iter().any(|k| k.as_slice() == key) {
                                list.push(key.to_vec());
                            }
                        } else {
                            tracker.pending_bcast.push((pfx, vec![key.to_vec()]));
                        }
                    } else {
                        deliver_invalidation_to_tracker(tracker, &clients, Some(&[key]));
                    }
                }
            }
        } else if tracker.tracked_keys.remove(key)
            && can_tracker_read_key(port, tracker.client_id, &clients, key)
        {
            deliver_invalidation_to_tracker(tracker, &clients, Some(&[key]));
        }
    }
}

pub fn flush_pending_bcast(port: u16) {
    if !HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let mut map = TRACKING_CLIENTS.write();
    let clients = GLOBAL_CLIENTS.read();
    for tracker in map.values_mut() {
        if tracker.port == port && !tracker.pending_bcast.is_empty() {
            let pending = std::mem::take(&mut tracker.pending_bcast);
            for (_pfx, keys) in pending {
                let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
                deliver_invalidation_to_tracker(tracker, &clients, Some(&refs));
            }
        }
    }
}

pub fn notify_key_invalidation_any_port(key: &[u8], sender_client_id: u64) {
    if !HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let ports: smallvec::SmallVec<[u16; 4]> = {
        let map = TRACKING_CLIENTS.read();
        let mut p = smallvec::SmallVec::new();
        for &(port, _) in map.keys() {
            if !p.contains(&port) {
                p.push(port);
            }
        }
        p
    };
    for port in ports {
        notify_key_invalidation(port, key, sender_client_id);
    }
}

pub fn notify_flush_invalidation(port: u16, sender_client_id: u64) {
    if !HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let mut map = TRACKING_CLIENTS.write();
    let clients = GLOBAL_CLIENTS.read();
    for tracker in map.values_mut() {
        if tracker.port != port {
            continue;
        }
        let had_keys = !tracker.tracked_keys.is_empty();
        tracker.tracked_keys.clear();
        tracker.pending_bcast.clear();
        if !tracker.bcast && !had_keys {
            continue;
        }
        if tracker.noloop && sender_client_id != 0 && tracker.client_id == sender_client_id {
            continue;
        }
        deliver_invalidation_to_tracker(tracker, &clients, None);
    }
}
