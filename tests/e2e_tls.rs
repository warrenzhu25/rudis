//! End-to-end tests: tls (see `tests/common` for the helpers).

#[macro_use]
mod common;
#[allow(unused_imports)]
use common::*;
#[allow(unused_imports)]
use rudis::router::target_shard;
#[allow(unused_imports)]
use rudis::server::run_shard_worker;
#[allow(unused_imports)]
use std::io::{Read, Write};
#[allow(unused_imports)]
use std::net::TcpStream;
#[allow(unused_imports)]
use std::thread;
#[allow(unused_imports)]
use std::time::Duration;

#[test]
fn test_auth_and_acl_e2e() {
    let port = 16407;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    // 1. Check default user info
    assert_eq!(
        send_and_read(&mut client, b"ACL WHOAMI\r\n"),
        "$7\r\ndefault\r\n"
    );
    let users = send_and_read(&mut client, b"ACL USERS\r\n");
    assert!(users.contains("$7\r\ndefault\r\n"));

    let list = send_and_read(&mut client, b"ACL LIST\r\n");
    assert!(list.contains("default on nopass"));

    // 2. Create user alice
    assert_eq!(
        send_and_read(&mut client, b"ACL SETUSER alice on >secret123 +@all ~*\r\n"),
        "+OK\r\n"
    );

    let getuser = send_and_read(&mut client, b"ACL GETUSER alice\r\n");
    let hex = rudis::acl::hash_password_sha256("secret123")[1..].to_string();
    assert!(
        getuser.contains(&format!("$64\r\n{}\r\n", hex)),
        "{}",
        getuser
    );
    assert!(!getuser.contains("secret123"), "{}", getuser);

    // 3. Authenticate as alice
    assert_eq!(
        send_and_read(&mut client, b"AUTH alice secret123\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"ACL WHOAMI\r\n"),
        "$5\r\nalice\r\n"
    );

    // Wrong password check
    assert!(send_and_read(&mut client, b"AUTH alice wrongpass\r\n").starts_with("-WRONGPASS"));

    // 4. Switch back to default before deleting alice (Redis closes connections using a deleted user)
    assert_eq!(
        send_and_read(&mut client, b"AUTH default \"\"\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"ACL DELUSER alice\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"ACL GETUSER alice\r\n"),
        "$-1\r\n"
    );

    // 5. Enforce password on default user and verify NOAUTH
    assert_eq!(
        send_and_read(&mut client, b"ACL SETUSER default >defpass\r\n"),
        "+OK\r\n"
    );

    // Open new connection - must be unauthenticated now
    let mut new_client = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let noauth_resp = send_and_read(&mut new_client, b"PING\r\n");
    assert_eq!(noauth_resp, "-NOAUTH Authentication required.\r\n");

    // Authenticate
    assert_eq!(
        send_and_read(&mut new_client, b"AUTH default defpass\r\n"),
        "+OK\r\n"
    );
    assert_eq!(send_and_read(&mut new_client, b"PING\r\n"), "+PONG\r\n");

    // Restore default user to nopass for subsequent tests
    assert_eq!(
        send_and_read(&mut new_client, b"ACL SETUSER default nopass\r\n"),
        "+OK\r\n"
    );
}

#[test]
fn test_acl_permissions_and_hashed_passwords_e2e() {
    let port = 16670;
    start_test_server(port, 2);

    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect admin client");

    // 1. Create user 'carol' with restricted commands (-@all +get +ping) and restricted keys (~user:*)
    let pass = "carol_secure_pass";
    let hash = rudis::acl::hash_password_sha256(pass);
    let setuser_cmd = format!("ACL SETUSER carol on {} -@all +get +acl ~user:*\r\n", hash);
    assert_eq!(
        send_and_read(&mut client, setuser_cmd.as_bytes()),
        "+OK\r\n"
    );

    // Set a key as admin (default user)
    assert_eq!(
        send_and_read(&mut client, b"SET user:profile alice_data\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"SET secret:token secret_val\r\n"),
        "+OK\r\n"
    );

    // 2. Connect as Carol and authenticate using plaintext password matching stored hash
    let mut carol_client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect carol client");
    assert_eq!(
        send_and_read(
            &mut carol_client,
            format!("AUTH carol {}\r\n", pass).as_bytes()
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut carol_client, b"ACL WHOAMI\r\n"),
        "$5\r\ncarol\r\n"
    );

    // 3. Test allowed command on allowed key
    let resp = send_and_read(&mut carol_client, b"GET user:profile\r\n");
    assert_eq!(resp, "$10\r\nalice_data\r\n");

    // 4. Test forbidden command on allowed key -> NOPERM command
    let resp = send_and_read(&mut carol_client, b"SET user:profile new_data\r\n");
    assert!(resp.starts_with("-NOPERM") && resp.contains("permissions to run the 'set' command"));

    // 5. Test allowed command on forbidden key -> NOPERM key
    let resp = send_and_read(&mut carol_client, b"GET secret:token\r\n");
    assert!(resp.starts_with("-NOPERM") && resp.contains("permissions to access one of the keys"));

    // 6. Test pipelined squashed commands enforcing ACL per command
    let pipeline = b"GET user:profile\r\nSET user:profile hack\r\nGET secret:token\r\n";
    let resp = send_and_read(&mut carol_client, pipeline);
    assert!(resp.contains("$10\r\nalice_data\r\n"));
    assert!(resp.contains("-NOPERM this user has no permissions to run the 'set' command"));
    assert!(resp.contains("-NOPERM this user has no permissions to access one of the keys"));
}

#[test]
fn test_tls_port_listener_e2e() {
    let port = 16690;
    let tls_port = 16691;
    let (cert_der, _key_der) = start_test_server_with_tls(port, tls_port, 2);

    // 1. Build a client rustls config trusting the server's self-signed cert
    let mut root_store = rustls::RootCertStore::empty();
    root_store
        .add(rustls::pki_types::CertificateDer::from(cert_der))
        .expect("Failed to add cert to root store");
    let client_config = std::sync::Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth(),
    );

    // 2. Connect to TLS port and perform TLS handshake
    let server_name = "localhost".try_into().unwrap();
    let conn = rustls::ClientConnection::new(client_config, server_name)
        .expect("Failed to create ClientConnection");
    let sock = TcpStream::connect(format!("127.0.0.1:{}", tls_port))
        .expect("Failed to connect TCP socket to TLS port");
    sock.set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut tls_client = rustls::StreamOwned::new(conn, sock);

    // 3. Send PING over TLS
    tls_client.write_all(b"PING\r\n").unwrap();
    let mut buf = [0u8; 512];
    let n = tls_client.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"+PONG\r\n");

    // 4. Send SET command over TLS
    tls_client
        .write_all(b"SET secure_key encrypted_value_99\r\n")
        .unwrap();
    let n = tls_client.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"+OK\r\n");

    // 5. Send GET command over TLS
    tls_client.write_all(b"GET secure_key\r\n").unwrap();
    let n = tls_client.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"$18\r\nencrypted_value_99\r\n");

    // 6. Connect to plain TCP port and verify shared database state
    let mut plain_client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect plain client");
    assert_eq!(
        send_and_read(&mut plain_client, b"GET secure_key\r\n"),
        "$18\r\nencrypted_value_99\r\n"
    );

    // 7. Mutate via plain TCP port and read back via TLS client
    assert_eq!(
        send_and_read(&mut plain_client, b"SET plain_key plain_val\r\n"),
        "+OK\r\n"
    );

    tls_client.write_all(b"GET plain_key\r\n").unwrap();
    let n = tls_client.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"$9\r\nplain_val\r\n");
}

#[test]
fn test_pipeline1_fast_path_and_acl_bypass_e2e() {
    let port = 16450;
    start_test_server(port, 4);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Pipeline 1 commands: individual GET and SET across different shards
    for i in 0..50 {
        let set_cmd = format!("SET p1_k_{} val_{}\r\n", i, i);
        let resp = send_and_read(&mut client, set_cmd.as_bytes());
        assert_eq!(resp, "+OK\r\n");

        let get_cmd = format!("GET p1_k_{}\r\n", i);
        let resp = send_and_read(&mut client, get_cmd.as_bytes());
        let expected = format!("${}\r\nval_{}\r\n", format!("val_{}", i).len(), i);
        assert_eq!(resp, expected);
    }

    // 2. SET with GET option (using write_resp_bulk fast path)
    let resp = send_and_read(&mut client, b"SET p1_k_0 new_val GET\r\n");
    assert_eq!(resp, "$5\r\nval_0\r\n");

    let resp = send_and_read(&mut client, b"GET p1_k_0\r\n");
    assert_eq!(resp, "$7\r\nnew_val\r\n");

    // 3. Test ACL configuration and execution
    let resp = send_and_read(
        &mut client,
        b"ACL SETUSER alice on >secretpass +@all ~*\r\n",
    );
    assert_eq!(resp, "+OK\r\n");

    let mut client2 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let resp = send_and_read(&mut client2, b"AUTH alice secretpass\r\n");
    assert_eq!(resp, "+OK\r\n");

    let resp = send_and_read(&mut client2, b"GET p1_k_0\r\n");
    assert_eq!(resp, "$7\r\nnew_val\r\n");
}

#[test]
fn test_requirepass_and_sha256_acl_enforcement_e2e() {
    let port = 19996;
    start_test_server(port, 2);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Unauthenticated ping succeeds when no password is set
    assert_eq!(send_and_read(&mut client, b"PING\r\n"), "+PONG\r\n");

    // 2. Set requirepass via CONFIG SET
    assert_eq!(
        send_and_read(&mut client, b"CONFIG SET requirepass secret123\r\n"),
        "+OK\r\n"
    );

    // 3. New connection must be blocked until authenticated
    let mut fresh_client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let noauth_resp = send_and_read(&mut fresh_client, b"PING\r\n");
    assert_eq!(noauth_resp, "-NOAUTH Authentication required.\r\n");

    // Wrong password fails
    let wrong_resp = send_and_read(&mut fresh_client, b"AUTH bad_pass\r\n");
    assert!(wrong_resp.starts_with("-WRONGPASS"));

    // Correct password succeeds
    assert_eq!(
        send_and_read(&mut fresh_client, b"AUTH secret123\r\n"),
        "+OK\r\n"
    );
    assert_eq!(send_and_read(&mut fresh_client, b"PING\r\n"), "+PONG\r\n");

    // 4. SHA-256 ACL hash authentication
    // SHA256("alice_pwd") = "4b227777d4dd1fc61c6f884f48641d02b4d121d3fd328cb08b5531fcacdabf8a"
    let sha256_hash = rudis::acl::hash_password_sha256("alice_pwd");
    assert!(sha256_hash.starts_with('#'));
    let setuser_cmd = format!("ACL SETUSER alice on {} +@all ~*\r\n", sha256_hash);
    assert_eq!(
        send_and_read(&mut fresh_client, setuser_cmd.as_bytes()),
        "+OK\r\n"
    );

    // Authenticate as alice using plaintext matching the SHA-256 hash
    let mut alice_client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    assert_eq!(
        send_and_read(&mut alice_client, b"AUTH alice alice_pwd\r\n"),
        "+OK\r\n"
    );
    assert_eq!(send_and_read(&mut alice_client, b"PING\r\n"), "+PONG\r\n");

    // Clean up requirepass
    assert_eq!(
        send_and_read(&mut fresh_client, b"CONFIG SET requirepass \"\"\r\n"),
        "+OK\r\n"
    );
    let mut unauth_client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    assert_eq!(send_and_read(&mut unauth_client, b"PING\r\n"), "+PONG\r\n");
}

#[test]
fn test_protected_mode_denies_external_clients_without_password_e2e() {
    let Some(ext_ip) = local_non_loopback_ip() else {
        eprintln!("skipping: host has no non-loopback interface");
        return;
    };
    let port = 16992;
    start_test_server(port, 2);
    let mut lo = TcpStream::connect(("127.0.0.1", port)).unwrap();

    // Default: protected mode on, default user has no password.
    assert!(resp_cmd(&mut lo, &["CONFIG", "GET", "protected-mode"]).contains("yes"));
    assert_eq!(resp_cmd(&mut lo, &["PING"]), "+PONG\r\n");
    let r = external_ping(ext_ip, port);
    assert!(
        r.starts_with("-DENIED"),
        "external client must be refused: {}",
        r
    );

    // A default-user password lifts the restriction (client must then AUTH).
    assert_eq!(
        resp_cmd(&mut lo, &["CONFIG", "SET", "requirepass", "pw"]),
        "+OK\r\n"
    );
    let r = external_ping(ext_ip, port);
    assert!(r.starts_with("-NOAUTH"), "{}", r);

    // Removing it re-arms protected mode.
    assert_eq!(resp_cmd(&mut lo, &["AUTH", "pw"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(&mut lo, &["CONFIG", "SET", "requirepass", ""]),
        "+OK\r\n"
    );
    assert!(external_ping(ext_ip, port).starts_with("-DENIED"));

    // Explicitly disabling protected mode admits external clients.
    assert_eq!(
        resp_cmd(&mut lo, &["CONFIG", "SET", "protected-mode", "no"]),
        "+OK\r\n"
    );
    assert!(resp_cmd(&mut lo, &["CONFIG", "GET", "protected-mode"]).contains("no"));
    assert_eq!(external_ping(ext_ip, port), "+PONG\r\n");
    assert!(resp_cmd(&mut lo, &["CONFIG", "SET", "protected-mode", "maybe"]).starts_with("-ERR"));
}

#[test]
fn test_mode_switch_commands_require_auth_and_acl_e2e() {
    let port = 16993;
    start_test_server(port, 2);
    let mut admin = TcpStream::connect(("127.0.0.1", port)).unwrap();
    assert_eq!(
        resp_cmd(&mut admin, &["CONFIG", "SET", "requirepass", "pw"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut admin, &["AUTH", "pw"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(
            &mut admin,
            &[
                "ACL",
                "SETUSER",
                "sub",
                "on",
                ">p",
                "-@all",
                "+subscribe",
                "+auth",
                "~*",
                "&*"
            ]
        ),
        "+OK\r\n"
    );

    // Unauthenticated: PSYNC/SYNC/SUBSCRIBE are refused and must not stream data.
    admin.write_all(b"SET secret v\r\n").unwrap();
    let _ = admin.read(&mut [0u8; 64]);
    let mut anon = TcpStream::connect(("127.0.0.1", port)).unwrap();
    anon.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    for cmd in [
        &["PSYNC", "?", "-1"][..],
        &["SYNC"][..],
        &["SUBSCRIBE", "ch"][..],
    ] {
        let r = resp_cmd(&mut anon, cmd);
        assert!(r.starts_with("-NOAUTH"), "{:?} -> {}", cmd, r);
    }
    // Still a normal connection afterwards.
    assert!(resp_cmd(&mut anon, &["GET", "secret"]).starts_with("-NOAUTH"));

    // Authenticated but not permitted: NOPERM, no replication stream.
    let mut limited = TcpStream::connect(("127.0.0.1", port)).unwrap();
    limited
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    assert_eq!(resp_cmd(&mut limited, &["AUTH", "sub", "p"]), "+OK\r\n");
    let r = resp_cmd(&mut limited, &["PSYNC", "?", "-1"]);
    assert!(r.starts_with("-NOPERM"), "{}", r);

    // Allowed SUBSCRIBE enters pub/sub mode with the same user, so ACLs still apply there.
    let r = resp_cmd(&mut limited, &["SUBSCRIBE", "ch"]);
    assert!(r.contains("subscribe"), "{}", r);
    let r = resp_cmd(&mut limited, &["PSUBSCRIBE", "p*"]);
    assert!(r.starts_with("-NOPERM"), "{}", r);
}

#[test]
fn test_acl_passwords_never_stored_or_shown_in_plaintext_e2e() {
    let port = 16994;
    start_test_server(port, 2);
    let mut admin = TcpStream::connect(("127.0.0.1", port)).unwrap();
    admin
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    let pass = "dave_plaintext_pw";
    let hash = rudis::acl::hash_password_sha256(pass); // "#<sha256 hex>"
    let hex = &hash[1..];
    assert_eq!(
        resp_cmd(
            &mut admin,
            &[
                "ACL",
                "SETUSER",
                "dave",
                "on",
                &format!(">{}", pass),
                "+@all",
                "~*"
            ]
        ),
        "+OK\r\n"
    );

    // ACL LIST / GETUSER expose only the SHA-256 digest, as Redis does.
    let list = resp_cmd(&mut admin, &["ACL", "LIST"]);
    assert!(!list.contains(pass), "{}", list);
    assert!(list.contains(&hash), "{}", list);
    let getuser = resp_cmd(&mut admin, &["ACL", "GETUSER", "dave"]);
    assert!(!getuser.contains(pass), "{}", getuser);
    assert!(
        getuser.contains(&format!("${}\r\n{}\r\n", hex.len(), hex)),
        "{}",
        getuser
    );

    // Knowing the stored hash is not enough to log in.
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    assert!(resp_cmd(&mut c, &["AUTH", "dave", &hash]).starts_with("-WRONGPASS"));
    assert!(resp_cmd(&mut c, &["AUTH", "dave", hex]).starts_with("-WRONGPASS"));
    assert_eq!(resp_cmd(&mut c, &["AUTH", "dave", pass]), "+OK\r\n");

    // Malformed '#' rules are rejected instead of silently stored.
    assert!(resp_cmd(&mut admin, &["ACL", "SETUSER", "eve", "#nothex"]).starts_with("-ERR"));

    // A default user configured with only a '#<hash>' password requires AUTH.
    let root_hash = rudis::acl::hash_password_sha256("rootpw");
    assert_eq!(
        resp_cmd(
            &mut admin,
            &["ACL", "SETUSER", "default", "resetpass", &root_hash]
        ),
        "+OK\r\n"
    );
    let mut anon = TcpStream::connect(("127.0.0.1", port)).unwrap();
    anon.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    assert!(resp_cmd(&mut anon, &["GET", "k"]).starts_with("-NOAUTH"));
    assert_eq!(resp_cmd(&mut anon, &["AUTH", "rootpw"]), "+OK\r\n");

    // requirepass round-trips through CONFIG GET without being kept in the user record.
    assert_eq!(
        resp_cmd(&mut admin, &["CONFIG", "SET", "requirepass", "rootpw2"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut admin, &["CONFIG", "GET", "requirepass"]),
        "*2\r\n$11\r\nrequirepass\r\n$7\r\nrootpw2\r\n"
    );
    let list = resp_cmd(&mut admin, &["ACL", "LIST"]);
    assert!(!list.contains("rootpw2"), "{}", list);
    let mut anon2 = TcpStream::connect(("127.0.0.1", port)).unwrap();
    anon2
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    assert!(resp_cmd(&mut anon2, &["PING"]).starts_with("-NOAUTH"));
    assert!(resp_cmd(&mut anon2, &["AUTH", "rootpw"]).starts_with("-WRONGPASS"));
    assert_eq!(resp_cmd(&mut anon2, &["AUTH", "rootpw2"]), "+OK\r\n");
}

#[test]
fn test_acl_categories_and_unknown_rules_e2e() {
    let port = 16997;
    start_test_server(port, 2);
    let mut admin = TcpStream::connect(("127.0.0.1", port)).unwrap();
    admin
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    assert_eq!(
        resp_cmd(
            &mut admin,
            &[
                "ACL",
                "SETUSER",
                "app",
                "on",
                ">apppw",
                "+@all",
                "-@dangerous",
                "~*"
            ]
        ),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(
            &mut admin,
            &["ACL", "SETUSER", "ro", "on", ">ropw", "+@read", "~*"]
        ),
        "+OK\r\n"
    );

    let mut app = TcpStream::connect(("127.0.0.1", port)).unwrap();
    app.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    assert_eq!(resp_cmd(&mut app, &["AUTH", "app", "apppw"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut app, &["SET", "k", "v"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut app, &["CLIENT", "SETNAME", "x"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut app, &["ACL", "WHOAMI"]), "$3\r\napp\r\n");
    for (cmd, name) in [
        (&["FLUSHALL"][..], "flushall"),
        (&["KEYS", "*"][..], "keys"),
        (&["CONFIG", "SET", "maxclients", "10"][..], "config|set"),
        (&["CONFIG", "GET", "maxclients"][..], "config|get"),
        (&["ACL", "SETUSER", "app", "+@all"][..], "acl|setuser"),
        (&["CLIENT", "KILL", "ID", "999999"][..], "client|kill"),
    ] {
        let r = resp_cmd(&mut app, cmd);
        assert_eq!(
            r,
            format!(
                "-NOPERM this user has no permissions to run the '{}' command\r\n",
                name
            ),
            "{:?}",
            cmd
        );
    }
    assert_eq!(resp_cmd(&mut app, &["GET", "k"]), "$1\r\nv\r\n");

    let mut ro = TcpStream::connect(("127.0.0.1", port)).unwrap();
    ro.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    assert_eq!(resp_cmd(&mut ro, &["AUTH", "ro", "ropw"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut ro, &["GET", "k"]), "$1\r\nv\r\n");
    assert!(resp_cmd(&mut ro, &["SET", "k", "w"]).starts_with("-NOPERM"));
    assert!(resp_cmd(&mut ro, &["DEL", "k"]).starts_with("-NOPERM"));

    // Unknown rules are errors, not silently ignored, and change nothing.
    let r = resp_cmd(&mut admin, &["ACL", "SETUSER", "ro", "+set", "+nosuchcmd"]);
    assert_eq!(
        r,
        "-ERR Error in ACL SETUSER modifier '+nosuchcmd': Unknown command or category name in ACL\r\n"
    );
    assert!(resp_cmd(&mut ro, &["SET", "k", "w"]).starts_with("-NOPERM"));
    assert!(resp_cmd(&mut admin, &["ACL", "SETUSER", "ro", "+@bogus"]).starts_with("-ERR"));
    assert!(resp_cmd(&mut admin, &["ACL", "SETUSER", "ro", "whatever"]).starts_with("-ERR"));

    // ACL GETUSER describes the effective command set in replayable form.
    let getuser = resp_cmd(&mut admin, &["ACL", "GETUSER", "app"]);
    assert!(getuser.contains("+@all -@dangerous"), "{}", getuser);
    assert!(!getuser.contains("-get "), "{}", getuser);
}

#[test]
fn test_acl_channel_permissions_e2e() {
    let port = 16998;
    start_test_server(port, 2);
    let mut admin = TcpStream::connect(("127.0.0.1", port)).unwrap();
    admin
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    assert_eq!(
        resp_cmd(
            &mut admin,
            &[
                "ACL", "SETUSER", "news", "on", ">np", "+@all", "~*", "&news.*"
            ]
        ),
        "+OK\r\n"
    );
    // Redis 7 default: new users get no channels unless granted.
    assert_eq!(
        resp_cmd(
            &mut admin,
            &["ACL", "SETUSER", "nochan", "on", ">cp", "+@all", "~*"]
        ),
        "+OK\r\n"
    );
    let getuser = resp_cmd(&mut admin, &["ACL", "GETUSER", "news"]);
    assert!(
        getuser.contains("$8\r\nchannels\r\n$7\r\n&news.*\r\n"),
        "{}",
        getuser
    );
    let list = resp_cmd(&mut admin, &["ACL", "LIST"]);
    assert!(list.contains("user nochan on"), "{}", list);
    assert!(list.contains("resetchannels"), "{}", list);
    const DENIED: &str =
        "-NOPERM this user has no permissions to access one of the channels used as arguments\r\n";

    let mut nochan = TcpStream::connect(("127.0.0.1", port)).unwrap();
    nochan
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    assert_eq!(resp_cmd(&mut nochan, &["AUTH", "nochan", "cp"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut nochan, &["PUBLISH", "news.a", "m"]), DENIED);
    assert_eq!(resp_cmd(&mut nochan, &["SUBSCRIBE", "news.a"]), DENIED);

    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    assert_eq!(resp_cmd(&mut c, &["AUTH", "news", "np"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["PUBLISH", "news.a", "m"]), ":0\r\n");
    assert_eq!(resp_cmd(&mut c, &["PUBLISH", "sports", "m"]), DENIED);
    assert_eq!(resp_cmd(&mut c, &["SPUBLISH", "sports", "m"]), DENIED);
    // Pipelined (squashable) PUBLISH is checked too.
    let r = send_and_read(
        &mut c,
        b"PUBLISH news.b m\r\nPUBLISH sports m\r\nPUBLISH news.c m\r\n",
    );
    assert_eq!(r, format!(":0\r\n{}:0\r\n", DENIED));
    // PSUBSCRIBE needs a literal pattern match.
    assert_eq!(resp_cmd(&mut c, &["PSUBSCRIBE", "*"]), DENIED);
    assert_eq!(resp_cmd(&mut c, &["SUBSCRIBE", "news.a", "sports"]), DENIED);
    let r = resp_cmd(&mut c, &["SUBSCRIBE", "news.a"]);
    assert!(r.contains("subscribe") && r.contains("news.a"), "{}", r);
    // Inside pub/sub mode the same rules apply.
    assert_eq!(resp_cmd(&mut c, &["SUBSCRIBE", "sports"]), DENIED);
    let r = resp_cmd(&mut c, &["PSUBSCRIBE", "news.*"]);
    assert!(r.contains("psubscribe"), "{}", r);
}

#[test]
fn test_acl_read_write_key_permissions_e2e() {
    let port = 16999;
    start_test_server(port, 2);
    let mut admin = TcpStream::connect(("127.0.0.1", port)).unwrap();
    admin
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    for (k, v) in [("r:1", "a"), ("w:1", "b"), ("x1", "c")] {
        assert_eq!(resp_cmd(&mut admin, &["SET", k, v]), "+OK\r\n");
    }
    assert_eq!(
        resp_cmd(
            &mut admin,
            &[
                "ACL", "SETUSER", "rw", "on", ">p", "+@all", "%R~r:*", "%W~w:*", "~x?"
            ]
        ),
        "+OK\r\n"
    );
    let getuser = resp_cmd(&mut admin, &["ACL", "GETUSER", "rw"]);
    assert!(getuser.contains("%R~r:* %W~w:* ~x?"), "{}", getuser);

    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    assert_eq!(resp_cmd(&mut c, &["AUTH", "rw", "p"]), "+OK\r\n");
    const DENIED: &str =
        "-NOPERM this user has no permissions to access one of the keys used as arguments\r\n";
    // Read-only pattern: reads OK, writes denied.
    assert_eq!(resp_cmd(&mut c, &["GET", "r:1"]), "$1\r\na\r\n");
    assert_eq!(resp_cmd(&mut c, &["SET", "r:1", "z"]), DENIED);
    assert_eq!(resp_cmd(&mut c, &["DEL", "r:1"]), DENIED);
    // Write-only pattern: blind writes OK, anything returning the value denied.
    assert_eq!(resp_cmd(&mut c, &["SET", "w:1", "z"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["LPUSH", "w:list", "z"]), ":1\r\n");
    assert_eq!(resp_cmd(&mut c, &["GET", "w:1"]), DENIED);
    assert_eq!(resp_cmd(&mut c, &["SET", "w:1", "y", "GET"]), DENIED);
    assert_eq!(resp_cmd(&mut c, &["GETDEL", "w:1"]), DENIED);
    // EXISTS needs neither read nor write, just a matching pattern.
    assert_eq!(resp_cmd(&mut c, &["EXISTS", "w:1"]), ":1\r\n");
    assert_eq!(resp_cmd(&mut c, &["EXISTS", "other"]), DENIED);
    // Full glob syntax in key patterns.
    assert_eq!(resp_cmd(&mut c, &["GET", "x1"]), "$1\r\nc\r\n");
    assert_eq!(resp_cmd(&mut c, &["GET", "x12"]), DENIED);
    // Pipelined (squashable) commands get the same checks.
    let r = send_and_read(&mut c, b"GET r:1\r\nGET w:1\r\nGET x1\r\n");
    assert_eq!(r, format!("$1\r\na\r\n{}$1\r\nc\r\n", DENIED));
}

#[test]
fn test_script_redis_call_enforces_caller_acl_e2e() {
    let port = 16989;
    start_test_server(port, 1);
    let mut admin = TcpStream::connect(("127.0.0.1", port)).unwrap();
    admin
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    assert_eq!(resp_cmd(&mut admin, &["SET", "secret", "s"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut admin, &["SET", "ok:1", "v"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(
            &mut admin,
            &[
                "ACL", "SETUSER", "lua", "on", ">p", "-@all", "+eval", "+get", "~ok:*"
            ]
        ),
        "+OK\r\n"
    );
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    assert_eq!(resp_cmd(&mut c, &["AUTH", "lua", "p"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(
            &mut c,
            &["EVAL", "return redis.call('get', KEYS[1])", "1", "ok:1"]
        ),
        "$1\r\nv\r\n"
    );
    // A key outside the user's patterns, smuggled in as a script argument.
    let r = resp_cmd(&mut c, &["EVAL", "return redis.call('get','secret')", "0"]);
    assert!(r.starts_with("-NOPERM") && !r.contains("$1\r\ns"), "{}", r);
    let r = resp_cmd(&mut c, &["EVAL", "return redis.pcall('get','secret')", "0"]);
    assert!(r.starts_with("-NOPERM"), "{}", r);
    // A command the user lacks.
    let r = resp_cmd(
        &mut c,
        &["EVAL", "return redis.call('set','ok:1','x')", "0"],
    );
    assert!(
        r.starts_with("-NOPERM") && r.contains("'set' command"),
        "{}",
        r
    );
    let r = resp_cmd(
        &mut c,
        &[
            "EVAL",
            "return redis.call('config','set','maxclients','5')",
            "0",
        ],
    );
    assert!(r.starts_with("-NOPERM"), "{}", r);
    assert_eq!(resp_cmd(&mut admin, &["GET", "ok:1"]), "$1\r\nv\r\n");
}

#[test]
fn test_acl_completeness_e2e() {
    let port: u16 = 17970;
    let port_s = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-acl-e2e-{port}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let acl_path = dir.join("users.acl");
    std::fs::write(
        &acl_path,
        "# initial acl file\nuser default on nopass ~* &* +@all\nuser alice on >alicepass ~k:* &ch:* -@all +get +set +select|0 +multi +exec\n",
    )
    .unwrap();
    let acl_s = acl_path.to_str().unwrap();
    let args = [
        "--port",
        &port_s,
        "--threads",
        "2",
        "--no-pin",
        "--aclfile",
        acl_s,
    ];
    let mut child = spawn_rudis_listening(&args, port);
    let connect = || {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s
    };

    let mut admin = connect();
    // ACL CAT & GENPASS
    let cats = resp_cmd(&mut admin, &["ACL", "CAT"]);
    assert!(cats.contains("scripting") && cats.contains("transaction"));
    let tx_cat = resp_cmd(&mut admin, &["ACL", "CAT", "transaction"]);
    assert!(tx_cat.contains("multi"));
    let gp = resp_cmd(&mut admin, &["ACL", "GENPASS", "128"]);
    assert!(gp.starts_with("$32\r\n"), "{gp}");

    // ACL DRYRUN checks command, key, channel, and first-arg rules
    assert_eq!(
        resp_cmd(&mut admin, &["ACL", "DRYRUN", "alice", "GET", "k:1"]),
        "+OK\r\n"
    );
    let dry_cmd = resp_cmd(&mut admin, &["ACL", "DRYRUN", "alice", "INCR", "k:1"]);
    assert!(
        dry_cmd.contains("has no permissions to run the 'incr' command"),
        "{dry_cmd}"
    );
    let dry_key = resp_cmd(&mut admin, &["ACL", "DRYRUN", "alice", "GET", "other:1"]);
    assert!(
        dry_key.contains("has no permissions to access one of the keys used as arguments"),
        "{dry_key}"
    );
    assert_eq!(
        resp_cmd(&mut admin, &["ACL", "DRYRUN", "alice", "SELECT", "0"]),
        "+OK\r\n"
    );
    let dry_sel = resp_cmd(&mut admin, &["ACL", "DRYRUN", "alice", "SELECT", "1"]);
    assert!(
        dry_sel.contains("has no permissions to run the 'select' command"),
        "{dry_sel}"
    );

    // Client authenticated as alice exercises first-arg rules, ACL LOG, and EXEC recheck
    let mut c = connect();
    assert_eq!(resp_cmd(&mut c, &["ACL", "LOG", "RESET"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["AUTH", "alice", "alicepass"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["SELECT", "0"]), "+OK\r\n");
    let sel_err = resp_cmd(&mut c, &["SELECT", "2"]);
    assert!(sel_err.starts_with("-NOPERM"), "{sel_err}");

    // Queue a valid command in MULTI, revoke key access before EXEC, verify EXEC-time recheck
    assert_eq!(resp_cmd(&mut c, &["MULTI"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["SET", "k:1", "val"]), "+QUEUED\r\n");
    assert_eq!(
        resp_cmd(
            &mut admin,
            &["ACL", "SETUSER", "alice", "resetkeys", "~allowed:*"]
        ),
        "+OK\r\n"
    );
    let exec_resp = resp_cmd(&mut c, &["EXEC"]);
    assert!(
        exec_resp.contains("no permission to touch the specified keys"),
        "{exec_resp}"
    );

    // ACL LOG & INFO stats
    let log_resp = resp_cmd(&mut admin, &["ACL", "LOG"]);
    assert!(
        log_resp.starts_with("*2\r\n*20\r\n"),
        "expected 2 entries of 20 elements each, got: {log_resp}"
    );
    assert!(log_resp.contains("multi") && log_resp.contains("k:1"));
    let info = resp_cmd(&mut admin, &["INFO", "stats"]);
    assert!(info.contains("acl_access_denied_cmd:1\r\n"), "{info}");
    assert!(info.contains("acl_access_denied_key:1\r\n"), "{info}");

    // ACL SAVE & LOAD round-trip
    assert_eq!(resp_cmd(&mut admin, &["ACL", "SAVE"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut admin, &["ACL", "LOAD"]), "+OK\r\n");

    drop(c);
    drop(admin);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_mtls_client_auth_and_config_set_e2e() {
    let port = 17975;
    let tls_port = 17976;
    let dir = std::env::temp_dir().join(format!("rudis-mtls-e2e-{}-{}", std::process::id(), port));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // Generate CA, server cert, valid client cert, and untrusted client cert
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let issuer = rcgen::Issuer::new(ca_params, ca_key);

    let srv_key = rcgen::KeyPair::generate().unwrap();
    let srv_cert =
        rcgen::CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .unwrap()
            .signed_by(&srv_key, &issuer)
            .unwrap();

    let cli_key = rcgen::KeyPair::generate().unwrap();
    let cli_cert = rcgen::CertificateParams::new(vec!["client.local".to_string()])
        .unwrap()
        .signed_by(&cli_key, &issuer)
        .unwrap();

    let bad_key = rcgen::KeyPair::generate().unwrap();
    let bad_cert = rcgen::CertificateParams::new(vec!["untrusted.local".to_string()])
        .unwrap()
        .self_signed(&bad_key)
        .unwrap();

    let ca_path = dir.join("ca.pem");
    let srv_cert_path = dir.join("server.pem");
    let srv_key_path = dir.join("server.key");
    let conf_path = dir.join("rudis.conf");

    std::fs::write(&ca_path, ca_cert.pem()).unwrap();
    std::fs::write(&srv_cert_path, srv_cert.pem()).unwrap();
    std::fs::write(&srv_key_path, srv_key.serialize_pem()).unwrap();
    std::fs::write(
        &conf_path,
        format!(
            "port {}\ntls-port {}\ntls-cert-file {}\ntls-key-file {}\ntls-ca-cert-file {}\ntls-auth-clients yes\n",
            port,
            tls_port,
            srv_cert_path.display(),
            srv_key_path.display(),
            ca_path.display(),
        ),
    )
    .unwrap();

    let mut srv = spawn_rudis_listening(
        &[conf_path.to_str().unwrap(), "--threads", "1", "--no-pin"],
        port,
    );

    let mut plain = TcpStream::connect(("127.0.0.1", port)).unwrap();
    plain
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let cfg_auth = resp_cmd(&mut plain, &["CONFIG", "GET", "tls-auth-clients"]);
    assert!(cfg_auth.contains("\r\nyes\r\n"), "{cfg_auth}");
    let cfg_ca = resp_cmd(&mut plain, &["CONFIG", "GET", "tls-ca-cert-file"]);
    assert!(cfg_ca.contains(&ca_path.display().to_string()), "{cfg_ca}");

    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(rustls::pki_types::CertificateDer::from(
            ca_cert.der().to_vec(),
        ))
        .unwrap();

    let anon_cfg = std::sync::Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots.clone())
            .with_no_client_auth(),
    );
    let valid_cfg = std::sync::Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots.clone())
            .with_client_auth_cert(
                vec![rustls::pki_types::CertificateDer::from(
                    cli_cert.der().to_vec(),
                )],
                rustls::pki_types::PrivateKeyDer::try_from(cli_key.serialize_der()).unwrap(),
            )
            .unwrap(),
    );
    let untrusted_cfg = std::sync::Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(
                vec![rustls::pki_types::CertificateDer::from(
                    bad_cert.der().to_vec(),
                )],
                rustls::pki_types::PrivateKeyDer::try_from(bad_key.serialize_der()).unwrap(),
            )
            .unwrap(),
    );

    let tls_ping = |cfg: std::sync::Arc<rustls::ClientConfig>| -> Result<String, String> {
        let server_name = rustls::pki_types::ServerName::try_from("localhost")
            .unwrap()
            .to_owned();
        let conn = rustls::ClientConnection::new(cfg, server_name).map_err(|e| e.to_string())?;
        let sock = TcpStream::connect(("127.0.0.1", tls_port)).map_err(|e| e.to_string())?;
        sock.set_read_timeout(Some(Duration::from_secs(3)))
            .map_err(|e| e.to_string())?;
        let mut stream = rustls::StreamOwned::new(conn, sock);
        stream
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .map_err(|e| e.to_string())?;
        stream.flush().map_err(|e| e.to_string())?;
        let mut buf = [0u8; 64];
        let n = stream.read(&mut buf).map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("EOF".to_string());
        }
        Ok(String::from_utf8_lossy(&buf[..n]).to_string())
    };

    // 1. With tls-auth-clients=yes: anonymous & untrusted fail, valid client cert succeeds
    assert!(tls_ping(anon_cfg.clone()).is_err());
    assert!(tls_ping(untrusted_cfg.clone()).is_err());
    assert_eq!(tls_ping(valid_cfg.clone()).unwrap(), "+PONG\r\n");

    // 2. CONFIG SET tls-auth-clients no -> anonymous client now succeeds
    assert_eq!(
        resp_cmd(&mut plain, &["CONFIG", "SET", "tls-auth-clients", "no"]),
        "+OK\r\n"
    );
    assert_eq!(tls_ping(anon_cfg.clone()).unwrap(), "+PONG\r\n");

    // 3. CONFIG SET tls-auth-clients optional -> anonymous and valid succeed, untrusted fails
    assert_eq!(
        resp_cmd(
            &mut plain,
            &["CONFIG", "SET", "tls-auth-clients", "optional"]
        ),
        "+OK\r\n"
    );
    assert_eq!(tls_ping(anon_cfg).unwrap(), "+PONG\r\n");
    assert_eq!(tls_ping(valid_cfg).unwrap(), "+PONG\r\n");
    assert!(tls_ping(untrusted_cfg).is_err());

    // 4. CONFIG REWRITE persists tls-auth-clients optional
    assert_eq!(resp_cmd(&mut plain, &["CONFIG", "REWRITE"]), "+OK\r\n");
    let rewritten = std::fs::read_to_string(&conf_path).unwrap();
    assert!(
        rewritten.contains("tls-auth-clients optional"),
        "{rewritten}"
    );

    drop(plain);
    shutdown_and_wait(port, &mut srv);
    let _ = std::fs::remove_dir_all(&dir);
}
