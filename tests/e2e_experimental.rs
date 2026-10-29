//! End-to-end tests: experimental (see `tests/common` for the helpers).

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
fn test_option2_vector_search_hnsw_e2e() {
    let port = 16520;
    start_test_server(port, 2);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Add vectors to HNSW index
    assert_eq!(
        send_and_read(&mut client, b"VADD v_idx doc1 1.0 0.0 0.0\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"VADD v_idx doc2 0.0 1.0 0.0\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"VADD v_idx doc3 0.9 0.1 0.0\r\n"),
        "+OK\r\n"
    );

    // 2. Query index info
    let info = send_and_read(&mut client, b"VINFO v_idx\r\n");
    assert!(info.contains("num_elements"));
    assert!(info.contains(":3\r\n"));
    assert!(info.contains("dimension"));
    assert!(info.contains(":3\r\n"));

    // 3. Compute vector similarity (Cosine metric by default)
    // doc1 [1,0,0] vs doc3 [0.9, 0.1, 0] is very close (distance close to 0)
    let sim_1_3 = send_and_read(&mut client, b"VSIM v_idx doc1 doc3\r\n");
    assert!(sim_1_3.starts_with("$"));

    // doc1 [1,0,0] vs doc2 [0,1,0] is orthogonal (distance 1.0)
    let sim_1_2 = send_and_read(&mut client, b"VSIM v_idx doc1 doc2\r\n");
    assert!(sim_1_2.contains("1.000000"));

    // 4. Query top-2 nearest neighbors for [1.0, 0.0, 0.0]
    let query_resp = send_and_read(&mut client, b"VQUERY v_idx 2 1.0 0.0 0.0\r\n");
    assert!(query_resp.contains("doc1"));
    assert!(query_resp.contains("doc3"));

    // 5. Delete element
    assert_eq!(send_and_read(&mut client, b"VDEL v_idx doc1\r\n"), ":1\r\n");
    let info_after = send_and_read(&mut client, b"VINFO v_idx\r\n");
    assert!(info_after.contains(":2\r\n"));
}

#[test]
fn test_sq8_quantized_vector_and_rerank_e2e() {
    let port = 16560;
    start_test_server(port, 2);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Ingest vectors with SQ8 quantization and tiered storage flag
    assert_eq!(
        send_and_read(
            &mut client,
            b"VADD doc_sq8 docA 1.0 0.0 0.0 QUANTIZE TIERED\r\n"
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut client,
            b"VADD doc_sq8 docB 0.0 1.0 0.0 QUANTIZE TIERED\r\n"
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut client,
            b"VADD doc_sq8 docC 0.88 0.12 0.0 QUANTIZE TIERED\r\n"
        ),
        "+OK\r\n"
    );

    // 2. Query with full precision reranking
    let query_resp = send_and_read(&mut client, b"VQUERY doc_sq8 2 1.0 0.0 0.0 RERANK\r\n");
    assert!(query_resp.contains("docA"));
    assert!(query_resp.contains("docC"));

    // 3. Verify VINFO reflects elements
    let info = send_and_read(&mut client, b"VINFO doc_sq8\r\n");
    assert!(info.contains(":3\r\n"));
}

#[test]
fn test_redis_json_engine_e2e() {
    let port = 16570;
    start_test_server(port, 2);
    std::thread::sleep(Duration::from_millis(50));

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. JSON.SET root
    let doc = r#"{"name":"Bob","age":28,"tags":["rust","io_uring"],"online":true}"#;
    let set_cmd = format!(
        "*4\r\n$8\r\nJSON.SET\r\n$6\r\nuser:1\r\n$1\r\n$\r\n${}\r\n{}\r\n",
        doc.len(),
        doc
    );
    assert_eq!(send_and_read(&mut stream, set_cmd.as_bytes()), "+OK\r\n");

    // 2. JSON.GET root
    let resp = send_and_read(
        &mut stream,
        b"*3\r\n$8\r\nJSON.GET\r\n$6\r\nuser:1\r\n$1\r\n$\r\n",
    );
    assert!(resp.contains("Bob"));
    assert!(resp.contains("io_uring"));

    // 3. JSON.GET path $.name
    let resp = send_and_read(
        &mut stream,
        b"*3\r\n$8\r\nJSON.GET\r\n$6\r\nuser:1\r\n$6\r\n$.name\r\n",
    );
    assert!(resp.contains("\"Bob\""));

    // 4. JSON.TYPE
    let resp = send_and_read(
        &mut stream,
        b"*3\r\n$9\r\nJSON.TYPE\r\n$6\r\nuser:1\r\n$6\r\n$.tags\r\n",
    );
    assert_eq!(resp, "+array\r\n");

    // 5. JSON.NUMINCRBY
    let resp = send_and_read(
        &mut stream,
        b"*4\r\n$14\r\nJSON.NUMINCRBY\r\n$6\r\nuser:1\r\n$5\r\n$.age\r\n$1\r\n2\r\n",
    );
    assert!(resp.contains("30"));

    // 6. JSON.ARRAPPEND
    let resp = send_and_read(
        &mut stream,
        b"*4\r\n$14\r\nJSON.ARRAPPEND\r\n$6\r\nuser:1\r\n$6\r\n$.tags\r\n$11\r\n\"high_perf\"\r\n",
    );
    assert_eq!(resp, ":3\r\n");

    // 7. JSON.ARRLEN
    let resp = send_and_read(
        &mut stream,
        b"*3\r\n$11\r\nJSON.ARRLEN\r\n$6\r\nuser:1\r\n$6\r\n$.tags\r\n",
    );
    assert_eq!(resp, ":3\r\n");

    // 8. JSON.ARRPOP
    let resp = send_and_read(
        &mut stream,
        b"*3\r\n$11\r\nJSON.ARRPOP\r\n$6\r\nuser:1\r\n$6\r\n$.tags\r\n",
    );
    assert!(resp.contains("\"high_perf\""));

    // 9. JSON.TOGGLE
    let resp = send_and_read(
        &mut stream,
        b"*3\r\n$11\r\nJSON.TOGGLE\r\n$6\r\nuser:1\r\n$8\r\n$.online\r\n",
    );
    assert!(resp.contains("false"));

    // 10. JSON.OBJKEYS
    let resp = send_and_read(
        &mut stream,
        b"*3\r\n$12\r\nJSON.OBJKEYS\r\n$6\r\nuser:1\r\n$1\r\n$\r\n",
    );
    assert!(resp.contains("name"));
    assert!(resp.contains("age"));

    // 11. JSON.OBJLEN
    let resp = send_and_read(
        &mut stream,
        b"*3\r\n$11\r\nJSON.OBJLEN\r\n$6\r\nuser:1\r\n$1\r\n$\r\n",
    );
    assert_eq!(resp, ":4\r\n");

    // 12. JSON.DEL nested
    let resp = send_and_read(
        &mut stream,
        b"*3\r\n$8\r\nJSON.DEL\r\n$6\r\nuser:1\r\n$8\r\n$.online\r\n",
    );
    assert_eq!(resp, ":1\r\n");

    // 13. JSON.DEL root
    let resp = send_and_read(&mut stream, b"*2\r\n$8\r\nJSON.DEL\r\n$6\r\nuser:1\r\n");
    assert_eq!(resp, ":1\r\n");

    // 14. Verify deleted
    let resp = send_and_read(&mut stream, b"*2\r\n$8\r\nJSON.GET\r\n$6\r\nuser:1\r\n");
    assert_eq!(resp, "$-1\r\n");
}

#[test]
fn test_probabilistic_data_structures_e2e() {
    let port = 16590;
    start_test_server(port, 2);
    std::thread::sleep(Duration::from_millis(50));

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Bloom Filter (BF.*)
    assert_eq!(
        send_and_read(&mut stream, b"BF.RESERVE mybf 0.01 1000\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"BF.RESERVE mybf 0.01 1000\r\n"),
        "-ERR item exists\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"BF.ADD mybf apple\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"BF.ADD mybf apple\r\n"),
        ":0\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"BF.EXISTS mybf apple\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"BF.EXISTS mybf orange\r\n"),
        ":0\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"BF.MADD mybf banana grape\r\n"),
        "*2\r\n:1\r\n:1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"BF.MEXISTS mybf apple banana melon\r\n"),
        "*3\r\n:1\r\n:1\r\n:0\r\n"
    );
    let info = send_and_read(&mut stream, b"BF.INFO mybf\r\n");
    assert!(info.contains("Capacity"));
    assert!(info.contains("1000"));

    // 2. Cuckoo Filter (CF.*)
    assert_eq!(
        send_and_read(&mut stream, b"CF.RESERVE mycf 1000\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"CF.RESERVE mycf 1000\r\n"),
        "-ERR item exists\r\n"
    );
    assert_eq!(send_and_read(&mut stream, b"CF.ADD mycf foo\r\n"), ":1\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"CF.ADDNX mycf foo\r\n"),
        ":0\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"CF.ADDNX mycf bar\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"CF.EXISTS mycf foo\r\n"),
        ":1\r\n"
    );
    assert_eq!(send_and_read(&mut stream, b"CF.DEL mycf foo\r\n"), ":1\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"CF.EXISTS mycf foo\r\n"),
        ":0\r\n"
    );
    let cf_info = send_and_read(&mut stream, b"CF.INFO mycf\r\n");
    assert!(cf_info.contains("Number of buckets"));

    // 3. Count-Min Sketch (CMS.*)
    assert_eq!(
        send_and_read(&mut stream, b"CMS.INITBYDIM mycms 200 5\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"CMS.INCRBY mycms item1 42 item2 17\r\n"),
        "*2\r\n:42\r\n:17\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"CMS.QUERY mycms item1 item2 item3\r\n"),
        "*3\r\n:42\r\n:17\r\n:0\r\n"
    );
    let cms_info = send_and_read(&mut stream, b"CMS.INFO mycms\r\n");
    assert!(cms_info.contains("width"));
    assert!(cms_info.contains("depth"));

    // 4. Top-K (TOPK.*)
    assert_eq!(
        send_and_read(&mut stream, b"TOPK.RESERVE mytopk 3\r\n"),
        "+OK\r\n"
    );
    let add_res = send_and_read(
        &mut stream,
        b"TOPK.ADD mytopk alpha alpha alpha beta beta gamma\r\n",
    );
    assert!(add_res.starts_with("*6\r\n"));
    assert_eq!(
        send_and_read(&mut stream, b"TOPK.QUERY mytopk alpha beta gamma delta\r\n"),
        "*4\r\n:1\r\n:1\r\n:1\r\n:0\r\n"
    );
    let topk_list = send_and_read(&mut stream, b"TOPK.LIST mytopk\r\n");
    assert!(topk_list.contains("alpha"));
    assert!(topk_list.contains("beta"));
    assert!(topk_list.contains("gamma"));
    let topk_info = send_and_read(&mut stream, b"TOPK.INFO mytopk\r\n");
    assert!(topk_info.contains("k"));
}

#[test]
fn test_redisearch_fulltext_and_hybrid_vector_e2e() {
    let port = 16620;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Create index on HASH
    let create_resp = send_and_read(
        &mut client,
        b"FT.CREATE idx:books ON HASH PREFIX 1 book: SCHEMA title TEXT WEIGHT 2.0 author TEXT price NUMERIC SORTABLE category TAG SEPARATOR ,\r\n"
    );
    assert_eq!(create_resp, "+OK\r\n");

    // 2. Duplicate index creation should fail
    let dup_resp = send_and_read(
        &mut client,
        b"FT.CREATE idx:books ON HASH PREFIX 1 book: SCHEMA title TEXT\r\n",
    );
    assert!(dup_resp.contains("ERR Index already exists"));

    // 3. Inspect index metadata via FT.INFO
    let info_resp = send_and_read(&mut client, b"FT.INFO idx:books\r\n");
    assert!(info_resp.contains("idx:books"));
    assert!(info_resp.contains("num_docs"));

    // 4. Populate books via HSET (automatically indexed via hook)
    let hset1 = b"*10\r\n$4\r\nHSET\r\n$6\r\nbook:1\r\n$5\r\ntitle\r\n$14\r\nRust in Action\r\n$6\r\nauthor\r\n$12\r\nTim McNamara\r\n$5\r\nprice\r\n$4\r\n45.0\r\n$8\r\ncategory\r\n$16\r\ntech,programming\r\n";
    assert_eq!(send_and_read(&mut client, hset1), ":4\r\n");

    let hset2 = b"*10\r\n$4\r\nHSET\r\n$6\r\nbook:2\r\n$5\r\ntitle\r\n$29\r\nThe Rust Programming Language\r\n$6\r\nauthor\r\n$13\r\nSteve Klabnik\r\n$5\r\nprice\r\n$5\r\n39.99\r\n$8\r\ncategory\r\n$9\r\ntech,rust\r\n";
    assert_eq!(send_and_read(&mut client, hset2), ":4\r\n");

    let hset3 = b"*10\r\n$4\r\nHSET\r\n$6\r\nbook:3\r\n$5\r\ntitle\r\n$37\r\nDesigning Data-Intensive Applications\r\n$6\r\nauthor\r\n$16\r\nMartin Kleppmann\r\n$5\r\nprice\r\n$4\r\n55.0\r\n$8\r\ncategory\r\n$13\r\ntech,database\r\n";
    assert_eq!(send_and_read(&mut client, hset3), ":4\r\n");

    // 5. Query 1: Keyword search for "Rust" (matches book:1 and book:2)
    let search1 = send_and_read(
        &mut client,
        b"*3\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:books\r\n$4\r\nRust\r\n",
    );
    assert!(
        search1.starts_with("*5\r\n:2\r\n"),
        "Search 'Rust' should return 2 hits. Resp: {}",
        search1
    );
    assert!(search1.contains("book:1"));
    assert!(search1.contains("book:2"));

    // 6. Query 2: Keyword search for "Data-Intensive" (matches book:3)
    let search2 = send_and_read(
        &mut client,
        b"*3\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:books\r\n$14\r\nData-Intensive\r\n",
    );
    assert!(
        search2.starts_with("*3\r\n:1\r\n"),
        "Search 'Data-Intensive' should return 1 hit. Resp: {}",
        search2
    );
    assert!(search2.contains("book:3"));

    // 7. Query 3: Numeric range query: @price:[40 50] (matches book:1 with price 45.0)
    let search3 = send_and_read(
        &mut client,
        b"*3\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:books\r\n$14\r\n@price:[40 50]\r\n",
    );
    assert!(
        search3.starts_with("*3\r\n:1\r\n"),
        "Range query should return 1 hit. Resp: {}",
        search3
    );
    assert!(search3.contains("book:1"));

    // 8. Query 4: Tag filter: @category:{database} (matches book:3)
    let search4 = send_and_read(
        &mut client,
        b"*3\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:books\r\n$20\r\n@category:{database}\r\n",
    );
    assert!(
        search4.starts_with("*3\r\n:1\r\n"),
        "Tag query should return 1 hit. Resp: {}",
        search4
    );
    assert!(search4.contains("book:3"));

    // 9. Query 5: Prefix query: "progra*" (matches programming in book:2 title)
    let search5 = send_and_read(
        &mut client,
        b"*3\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:books\r\n$7\r\nprogra*\r\n",
    );
    assert!(
        search5.starts_with("*3\r\n:1\r\n"),
        "Prefix query should return 1 hit for book:2. Resp: {}",
        search5
    );
    assert!(search5.contains("book:2"));

    // 10. Query 6: NOCONTENT flag (returns doc IDs only)
    let search_nocontent = send_and_read(
        &mut client,
        b"*4\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:books\r\n$4\r\nRust\r\n$9\r\nNOCONTENT\r\n",
    );
    assert!(
        search_nocontent.starts_with("*3\r\n:2\r\n")
            && search_nocontent.contains("book:1")
            && search_nocontent.contains("book:2"),
        "Unexpected NOCONTENT response: {}",
        search_nocontent
    );

    // 11. Query 7: FT.EXPLAIN
    let explain_resp = send_and_read(
        &mut client,
        b"*3\r\n$10\r\nFT.EXPLAIN\r\n$9\r\nidx:books\r\n$15\r\nRust | database\r\n",
    );
    assert!(
        explain_resp.contains("Or"),
        "Explain output should describe parsed AST"
    );

    // 12. Document deletion via DEL automatically removes from index
    assert_eq!(send_and_read(&mut client, b"DEL book:1\r\n"), ":1\r\n");
    let search_after_del = send_and_read(&mut client, b"FT.SEARCH idx:books Action\r\n");
    assert_eq!(search_after_del, "*1\r\n:0\r\n");

    // 13. Drop index
    assert_eq!(
        send_and_read(&mut client, b"FT.DROPINDEX idx:books\r\n"),
        "+OK\r\n"
    );
    let search_after_drop = send_and_read(&mut client, b"FT.SEARCH idx:books Rust\r\n");
    assert!(search_after_drop.contains("ERR Unknown Index name"));

    // 14. JSON Document Indexing via FT.CREATE and JSON.SET
    assert_eq!(
        send_and_read(
            &mut client,
            b"FT.CREATE idx:users ON JSON PREFIX 1 user: SCHEMA name TEXT role TAG\r\n"
        ),
        "+OK\r\n"
    );
    let json_payload = b"{\"name\":\"Alice Engineer\",\"role\":\"admin\"}";
    let mut json_set_cmd = format!(
        "*4\r\n$8\r\nJSON.SET\r\n$6\r\nuser:1\r\n$1\r\n$\r\n${}\r\n",
        json_payload.len()
    )
    .into_bytes();
    json_set_cmd.extend_from_slice(json_payload);
    json_set_cmd.extend_from_slice(b"\r\n");
    assert_eq!(send_and_read(&mut client, &json_set_cmd), "+OK\r\n");

    let search_json = send_and_read(
        &mut client,
        b"*3\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:users\r\n$5\r\nAlice\r\n",
    );
    assert!(search_json.contains("user:1"));
    assert!(search_json.contains("Alice Engineer"));

    // 15. Vector Indexing and FT.SEARCH KNN with PARAMS
    assert_eq!(
        send_and_read(
            &mut client,
            b"FT.CREATE idx:vectors ON HASH PREFIX 1 vec: SCHEMA title TEXT embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 3 DISTANCE_METRIC COSINE\r\n"
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut client,
            b"HSET vec:doc1 title First embedding 1.0,0.0,0.0\r\n"
        ),
        ":2\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut client,
            b"HSET vec:doc2 title Second embedding 0.0,1.0,0.0\r\n"
        ),
        ":2\r\n"
    );

    let mut q_vec_bytes = Vec::new();
    for val in [1.0f32, 0.1f32, 0.0f32] {
        q_vec_bytes.extend_from_slice(&val.to_le_bytes());
    }

    let mut search_vec_cmd = Vec::new();
    search_vec_cmd.extend_from_slice(b"*7\r\n$9\r\nFT.SEARCH\r\n$11\r\nidx:vectors\r\n$27\r\n*=>[KNN 1 @embedding $blob]\r\n$6\r\nPARAMS\r\n$1\r\n2\r\n$4\r\nblob\r\n");
    search_vec_cmd.extend_from_slice(format!("${}\r\n", q_vec_bytes.len()).as_bytes());
    search_vec_cmd.extend_from_slice(&q_vec_bytes);
    search_vec_cmd.extend_from_slice(b"\r\n");

    let vec_search_res = send_and_read(&mut client, &search_vec_cmd);
    assert!(vec_search_res.contains("vec:doc1"));
    assert!(!vec_search_res.contains("vec:doc2"));
}

#[test]
#[cfg_attr(not(feature = "xdp"), ignore = "needs the xdp cargo feature")]
fn test_af_xdp_ebpf_kernel_bypass_e2e() {
    let port = 16621;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Inspect XDP engine info
    let info = send_and_read(&mut client, b"XDP.INFO\r\n");
    assert!(info.contains("interface:eth0"));
    assert!(info.contains("mode:"));
    assert!(info.contains("umem_frame_size:2048"));

    // 2. Add eBPF packet filter rules
    let r1 = send_and_read(&mut client, b"XDP.RULE ADD DROP 10.0.0.0/8\r\n");
    assert_eq!(r1, ":1\r\n");

    let r2 = send_and_read(&mut client, b"XDP.RULE ADD PASS 192.168.1.0/24\r\n");
    assert_eq!(r2, ":2\r\n");

    // 3. List active eBPF rules
    let rules = send_and_read(&mut client, b"XDP.RULE LIST\r\n");
    assert!(rules.starts_with("*2\r\n"));
    assert!(rules.contains("action:DROP cidr:10.0.0.0/8"));
    assert!(rules.contains("action:PASS cidr:192.168.1.0/24"));

    // 4. Inject test packets via XDP.PACKET diagnostic command
    // Packet from 10.1.2.3 (should be DROPPED by rule 1)
    let mut pkt_drop = vec![0u8; 40];
    pkt_drop[0] = 0x45;
    pkt_drop[12] = 10;
    pkt_drop[13] = 1;
    pkt_drop[14] = 2;
    pkt_drop[15] = 3;
    let mut cmd_drop = format!("*2\r\n$10\r\nXDP.PACKET\r\n${}\r\n", pkt_drop.len()).into_bytes();
    cmd_drop.extend_from_slice(&pkt_drop);
    cmd_drop.extend_from_slice(b"\r\n");
    assert_eq!(send_and_read(&mut client, &cmd_drop), "+DROP\r\n");

    // Packet from 192.168.1.50 (should be PASSED by rule 2)
    let mut pkt_pass = vec![0u8; 40];
    pkt_pass[0] = 0x45;
    pkt_pass[12] = 192;
    pkt_pass[13] = 168;
    pkt_pass[14] = 1;
    pkt_pass[15] = 50;
    let mut cmd_pass = format!("*2\r\n$10\r\nXDP.PACKET\r\n${}\r\n", pkt_pass.len()).into_bytes();
    cmd_pass.extend_from_slice(&pkt_pass);
    cmd_pass.extend_from_slice(b"\r\n");
    assert_eq!(send_and_read(&mut client, &cmd_pass), "+PASS\r\n");

    // Packet from 172.16.0.1 (no match -> redirected to userspace UMEM ring)
    let mut pkt_redir = vec![0u8; 40];
    pkt_redir[0] = 0x45;
    pkt_redir[12] = 172;
    pkt_redir[13] = 16;
    pkt_redir[14] = 0;
    pkt_redir[15] = 1;
    let mut cmd_redir = format!("*2\r\n$10\r\nXDP.PACKET\r\n${}\r\n", pkt_redir.len()).into_bytes();
    cmd_redir.extend_from_slice(&pkt_redir);
    cmd_redir.extend_from_slice(b"\r\n");
    assert_eq!(send_and_read(&mut client, &cmd_redir), "+REDIRECT\r\n");

    // 5. Check real-time XDP stats
    let stats = send_and_read(&mut client, b"XDP.STATS\r\n");
    assert!(stats.contains("dropped_packets"));
    assert!(stats.contains("pass_packets"));
    assert!(stats.contains("redirected_packets"));

    // 6. Delete rule 1
    assert_eq!(send_and_read(&mut client, b"XDP.RULE DEL 1\r\n"), "+OK\r\n");

    // Packet from 10.1.2.3 now redirects instead of dropping
    assert_eq!(send_and_read(&mut client, &cmd_drop), "+REDIRECT\r\n");
}

#[test]
fn test_crdt_cross_shard_routing_e2e() {
    let port = 16650;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut client1 =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client 1");
    let mut client2 =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client 2");

    // Identify keys mapping to distinct shards in a 4-shard cluster
    let mut shard_keys = std::collections::HashMap::new();
    let mut counter = 0;
    while shard_keys.len() < num_shards {
        let key_str = format!("crdt_key_{}", counter);
        let shard = rudis::router::target_shard(key_str.as_bytes(), num_shards);
        shard_keys.entry(shard).or_insert(key_str);
        counter += 1;
    }

    for (shard, key) in &shard_keys {
        // Write CRDT LWW register from Client 1
        let val = format!("val_shard_{}", shard);
        let resp = send_and_read(
            &mut client1,
            format!("CRDT.SET {} {}\r\n", key, val).as_bytes(),
        );
        assert!(resp.starts_with("+OK"));

        // Read back from Client 2 (cross-shard pipeline/dispatch)
        let resp = send_and_read(&mut client2, format!("CRDT.GET {}\r\n", key).as_bytes());
        assert_eq!(resp, format!("${}\r\n{}\r\n", val.len(), val));

        // Test CRDT counter on this shard: increment by 42 from client1, then by 1 from client2
        let counter_key = format!("{}:cnt", key);
        let resp = send_and_read(
            &mut client1,
            format!("CRDT.INCRBY {} 42\r\n", counter_key).as_bytes(),
        );
        assert_eq!(resp, ":42\r\n");

        // Verify and increment counter from Client 2 across shard boundary
        let resp = send_and_read(
            &mut client2,
            format!("CRDT.INCRBY {} 1\r\n", counter_key).as_bytes(),
        );
        assert_eq!(resp, ":43\r\n");

        // Test CRDT ORSet on this shard: SADD from Client 1, SMEMBERS from Client 2
        let set_key = format!("{}:set", key);
        let resp = send_and_read(
            &mut client1,
            format!("CRDT.SADD {} member_a\r\n", set_key).as_bytes(),
        );
        assert_eq!(resp, ":1\r\n");

        let resp = send_and_read(
            &mut client2,
            format!("CRDT.SMEMBERS {}\r\n", set_key).as_bytes(),
        );
        assert_eq!(resp, "*1\r\n$8\r\nmember_a\r\n");

        // Remove member from Client 2 and verify SMEMBERS empty from Client 1
        let resp = send_and_read(
            &mut client2,
            format!("CRDT.SREM {} member_a\r\n", set_key).as_bytes(),
        );
        assert_eq!(resp, ":1\r\n");

        let resp = send_and_read(
            &mut client1,
            format!("CRDT.SMEMBERS {}\r\n", set_key).as_bytes(),
        );
        assert_eq!(resp, "*0\r\n");

        // Delete register from Client 2 and verify nil from Client 1
        let resp = send_and_read(&mut client2, format!("CRDT.DEL {}\r\n", key).as_bytes());
        assert_eq!(resp, ":1\r\n");

        let resp = send_and_read(&mut client1, format!("CRDT.GET {}\r\n", key).as_bytes());
        assert_eq!(resp, "$-1\r\n");
    }
}

#[test]
fn test_json_mget_multi_shard_parallel_fanout_e2e() {
    let port = 16760;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Populate JSON documents mapped across distinct shards
    assert_eq!(
        send_and_read(
            &mut client,
            b"JSON.SET user:1 $ '{\"name\":\"alice\",\"age\":30,\"score\":95}'\r\n"
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut client,
            b"JSON.SET user:2 $ '{\"name\":\"bob\",\"age\":25,\"score\":88}'\r\n"
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut client,
            b"JSON.SET user:3 $ '{\"name\":\"carol\",\"age\":35,\"score\":92}'\r\n"
        ),
        "+OK\r\n"
    );

    // 2. Multi-shard JSON.MGET on specific path $.score
    let resp = send_and_read(
        &mut client,
        b"JSON.MGET user:1 user:2 user:missing user:3 $.score\r\n",
    );
    assert_eq!(
        resp, "*4\r\n$2\r\n95\r\n$2\r\n88\r\n$-1\r\n$2\r\n92\r\n",
        "JSON.MGET should preserve exact requested order across shards"
    );

    // 3. Multi-shard JSON.MGET on root path $
    let resp_root = send_and_read(&mut client, b"JSON.MGET user:1 user:missing $\r\n");
    assert!(resp_root.starts_with("*2\r\n"));
    assert!(resp_root.contains("alice"));
    assert!(resp_root.ends_with("$-1\r\n"));
}

#[test]
fn test_redisearch_on_json_secondary_index_e2e() {
    let port = 16773;
    let num_shards = 2;
    start_test_server(port, num_shards);

    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client");
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    // 1. Create index ON JSON with JSONPath fields and AS aliases
    assert_eq!(
        send_and_read(
            &mut client,
            b"FT.CREATE idx:inventory ON JSON PREFIX 1 item: SCHEMA $.title AS title TEXT $.price AS price NUMERIC $.tags.* AS tags TAG $.details.in_stock AS in_stock NUMERIC\r\n"
        ),
        "+OK\r\n"
    );

    // 2. Populate JSON documents via JSON.SET
    let item1 = br#"{"title": "High performance Rust distributed database", "price": 99.99, "tags": ["rust", "database", "distributed"], "details": {"in_stock": 1}}"#;
    let mut cmd1 = format!(
        "*4\r\n$8\r\nJSON.SET\r\n$6\r\nitem:1\r\n$1\r\n$\r\n${}\r\n",
        item1.len()
    )
    .into_bytes();
    cmd1.extend_from_slice(item1);
    cmd1.extend_from_slice(b"\r\n");
    assert_eq!(send_and_read(&mut client, &cmd1), "+OK\r\n");

    let item2 = br#"{"title": "Dragonfly in-memory data store in C++", "price": 49.50, "tags": ["cache", "memory"], "details": {"in_stock": 0}}"#;
    let mut cmd2 = format!(
        "*4\r\n$8\r\nJSON.SET\r\n$6\r\nitem:2\r\n$1\r\n$\r\n${}\r\n",
        item2.len()
    )
    .into_bytes();
    cmd2.extend_from_slice(item2);
    cmd2.extend_from_slice(b"\r\n");
    assert_eq!(send_and_read(&mut client, &cmd2), "+OK\r\n");

    let item3 = br#"{"title": "Kafka distributed stream broker in Scala and Java", "price": 120.00, "tags": ["streaming", "distributed"], "details": {"in_stock": 1}}"#;
    let mut cmd3 = format!(
        "*4\r\n$8\r\nJSON.SET\r\n$6\r\nitem:3\r\n$1\r\n$\r\n${}\r\n",
        item3.len()
    )
    .into_bytes();
    cmd3.extend_from_slice(item3);
    cmd3.extend_from_slice(b"\r\n");
    assert_eq!(send_and_read(&mut client, &cmd3), "+OK\r\n");

    // 3. Query 1: Full-text search for "distributed" (matches item:1 and item:3)
    let search1 = send_and_read(
        &mut client,
        b"*3\r\n$9\r\nFT.SEARCH\r\n$13\r\nidx:inventory\r\n$11\r\ndistributed\r\n",
    );
    assert!(
        search1.starts_with("*5\r\n:2\r\n"),
        "Expected 2 hits, got: {}",
        search1
    );
    assert!(search1.contains("item:1"));
    assert!(search1.contains("item:3"));

    // 4. Query 2: Numeric range on price: @price:[40 100] (matches item:1 and item:2)
    let search2 = send_and_read(
        &mut client,
        b"*3\r\n$9\r\nFT.SEARCH\r\n$13\r\nidx:inventory\r\n$15\r\n@price:[40 100]\r\n",
    );
    assert!(
        search2.starts_with("*5\r\n:2\r\n"),
        "Expected 2 hits, got: {}",
        search2
    );
    assert!(search2.contains("item:1"));
    assert!(search2.contains("item:2"));

    // 5. Query 3: Tag filter: @tags:{rust} (matches item:1)
    let q3 = "@tags:{rust}";
    let search3_cmd = format!(
        "*3\r\n$9\r\nFT.SEARCH\r\n$13\r\nidx:inventory\r\n${}\r\n{}\r\n",
        q3.len(),
        q3
    );
    let search3 = send_and_read(&mut client, search3_cmd.as_bytes());
    assert!(
        search3.starts_with("*3\r\n:1\r\n"),
        "Expected 1 hit, got: {}",
        search3
    );
    assert!(search3.contains("item:1"));

    // 6. Query 4: Nested JSONPath numeric range: @in_stock:[1 1] (matches item:1 and item:3)
    let q4 = "@in_stock:[1 1]";
    let search4_cmd = format!(
        "*3\r\n$9\r\nFT.SEARCH\r\n$13\r\nidx:inventory\r\n${}\r\n{}\r\n",
        q4.len(),
        q4
    );
    let search4 = send_and_read(&mut client, search4_cmd.as_bytes());
    assert!(
        search4.starts_with("*5\r\n:2\r\n"),
        "Expected 2 hits, got: {}",
        search4
    );
    assert!(search4.contains("item:1"));
    assert!(search4.contains("item:3"));

    // 7. Mutate subpath: JSON.SET item:2 $.price 35.00
    let mutate_cmd = "*4\r\n$8\r\nJSON.SET\r\n$6\r\nitem:2\r\n$7\r\n$.price\r\n$5\r\n35.00\r\n";
    assert_eq!(send_and_read(&mut client, mutate_cmd.as_bytes()), "+OK\r\n");
    let q_mut = "@price:[30 40]";
    let search_mut_cmd = format!(
        "*3\r\n$9\r\nFT.SEARCH\r\n$13\r\nidx:inventory\r\n${}\r\n{}\r\n",
        q_mut.len(),
        q_mut
    );
    let search_mutated = send_and_read(&mut client, search_mut_cmd.as_bytes());
    assert!(
        search_mutated.starts_with("*3\r\n:1\r\n"),
        "Expected 1 hit after subpath update, got: {}",
        search_mutated
    );
    assert!(search_mutated.contains("item:2"));

    // 8. Test RETURN fields
    let search_return = send_and_read(
        &mut client,
        b"*7\r\n$9\r\nFT.SEARCH\r\n$13\r\nidx:inventory\r\n$4\r\nRust\r\n$6\r\nRETURN\r\n$1\r\n2\r\n$5\r\ntitle\r\n$5\r\nprice\r\n",
    );
    assert!(search_return.contains("item:1"));
    assert!(search_return.contains("High performance Rust distributed database"));

    // 9. Document deletion via JSON.DEL
    assert_eq!(
        send_and_read(&mut client, b"JSON.DEL item:1 $\r\n"),
        ":1\r\n"
    );
    let search_after_del = send_and_read(
        &mut client,
        b"*3\r\n$9\r\nFT.SEARCH\r\n$13\r\nidx:inventory\r\n$4\r\nRust\r\n",
    );
    assert_eq!(search_after_del, "*1\r\n:0\r\n");

    // 10. Clean up index
    assert_eq!(
        send_and_read(&mut client, b"FT.DROPINDEX idx:inventory\r\n"),
        "+OK\r\n"
    );
}

#[test]
fn test_simd_vector_distance_acceleration_hnsw_e2e() {
    let port = 16774;
    let num_shards = 2;
    start_test_server(port, num_shards);

    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client");
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    // 1. Ingest 16-dimensional vectors into HNSW index
    // doc_a: mostly along axis 0
    let v_a = "1.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0";
    assert_eq!(
        send_and_read(
            &mut client,
            format!("VADD simd_idx doc_a {}\r\n", v_a).as_bytes()
        ),
        "+OK\r\n"
    );

    // doc_b: mostly along axis 1
    let v_b = "0.0 1.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0";
    assert_eq!(
        send_and_read(
            &mut client,
            format!("VADD simd_idx doc_b {}\r\n", v_b).as_bytes()
        ),
        "+OK\r\n"
    );

    // doc_c: close to doc_a (0.95 on axis 0, 0.05 on axis 1)
    let v_c = "0.95 0.05 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0";
    assert_eq!(
        send_and_read(
            &mut client,
            format!("VADD simd_idx doc_c {}\r\n", v_c).as_bytes()
        ),
        "+OK\r\n"
    );

    // doc_d: close to doc_b (0.05 on axis 0, 0.95 on axis 1)
    let v_d = "0.05 0.95 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0";
    assert_eq!(
        send_and_read(
            &mut client,
            format!("VADD simd_idx doc_d {}\r\n", v_d).as_bytes()
        ),
        "+OK\r\n"
    );

    // 2. Query top-2 nearest neighbors to doc_a
    let q_a = "1.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0";
    let query_res = send_and_read(
        &mut client,
        format!("VQUERY simd_idx 2 {}\r\n", q_a).as_bytes(),
    );
    assert!(
        query_res.starts_with("*4\r\n"),
        "Expected 2 key-dist pairs, got: {}",
        query_res
    );
    assert!(query_res.contains("doc_a"));
    assert!(query_res.contains("doc_c"));

    // 3. VSIM across metrics
    // Cosine distance between doc_a and doc_c (small distance)
    let sim_ac = send_and_read(&mut client, b"VSIM simd_idx doc_a doc_c COSINE\r\n");
    assert!(sim_ac.starts_with('$'));
    let dist_ac: f32 = sim_ac
        .lines()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("Parsed float distance");
    assert!(dist_ac < 0.05, "Expected close distance, got: {}", dist_ac);

    // Cosine distance between doc_a and doc_b (orthogonal -> ~1.0)
    let sim_ab = send_and_read(&mut client, b"VSIM simd_idx doc_a doc_b COSINE\r\n");
    let dist_ab: f32 = sim_ab
        .lines()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("Parsed float distance");
    assert!(
        (dist_ab - 1.0).abs() < 0.05,
        "Expected orthogonal ~1.0 distance, got: {}",
        dist_ab
    );

    // L2 Euclidean distance between doc_a and doc_b (~sqrt(2) = 1.414)
    let sim_l2 = send_and_read(&mut client, b"VSIM simd_idx doc_a doc_b L2\r\n");
    let dist_l2: f32 = sim_l2
        .lines()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("Parsed float distance");
    assert!(
        (dist_l2 - std::f32::consts::SQRT_2).abs() < 0.05,
        "Expected L2 sqrt(2), got: {}",
        dist_l2
    );

    // 4. VDEL doc_a
    assert_eq!(
        send_and_read(&mut client, b"VDEL simd_idx doc_a\r\n"),
        ":1\r\n"
    );

    // 5. Query after deletion: doc_c should now be the top-1 result
    let query_after_del = send_and_read(
        &mut client,
        format!("VQUERY simd_idx 1 {}\r\n", q_a).as_bytes(),
    );
    assert!(query_after_del.contains("doc_c"));
    assert!(!query_after_del.contains("doc_a"));

    // 6. VINFO verification
    let info = send_and_read(&mut client, b"VINFO simd_idx\r\n");
    assert!(info.starts_with("*8\r\n"));
}

#[test]
#[cfg_attr(not(feature = "xdp"), ignore = "needs the xdp cargo feature")]
fn test_af_xdp_kernel_bypass_zero_copy_rings_e2e() {
    let port = 16775;
    let num_shards = 2;
    start_test_server(port, num_shards);

    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client");
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    // 1. Verify XDP.INFO shows kernel bypass engine and active queues
    let info = send_and_read(&mut client, b"XDP.INFO\r\n");
    assert!(info.contains("active_xsk_queues:"));
    assert!(info.contains("interface:"));

    // 2. Query XDP.SOCKET for queue 0
    let sock_stat = send_and_read(&mut client, b"XDP.SOCKET 0\r\n");
    assert!(sock_stat.starts_with("*8\r\n"));
    assert!(sock_stat.contains("rx_len"));
    assert!(sock_stat.contains("fill_len"));

    // 3. Inject RESP command directly into AF_XDP zero-copy Rx ring for Shard 0
    let resp_cmd = b"*3\r\n$3\r\nSET\r\n$7\r\nxdp_key\r\n$7\r\nxdp_val\r\n";
    let mut inject_frame = format!(
        "*3\r\n$10\r\nXDP.INJECT\r\n$1\r\n0\r\n${}\r\n",
        resp_cmd.len()
    )
    .into_bytes();
    inject_frame.extend_from_slice(resp_cmd);
    inject_frame.extend_from_slice(b"\r\n");

    assert_eq!(send_and_read(&mut client, &inject_frame), "+OK\r\n");

    // Give shard worker kernel bypass loop a moment to drain the Rx ring
    std::thread::sleep(Duration::from_millis(50));

    // 4. Verify state mutation over standard TCP interface
    assert_eq!(
        send_and_read(&mut client, b"GET xdp_key\r\n"),
        "$7\r\nxdp_val\r\n"
    );

    // 5. Inject a raw Ethernet + IPv4 + TCP packet containing a Redis command
    // Construct 54-byte Ethernet/IPv4/TCP frame
    let mut raw_packet = vec![0u8; 54];
    raw_packet[12] = 0x08;
    raw_packet[13] = 0x00; // Ethernet IPv4
    raw_packet[14] = 0x45; // IPv4, header len 20 bytes (5 * 4)
    raw_packet[14 + 9] = 6; // TCP protocol
    raw_packet[14 + 12] = 192;
    raw_packet[14 + 13] = 168;
    raw_packet[14 + 14] = 1;
    raw_packet[14 + 15] = 100; // src IP: 192.168.1.100
    raw_packet[34 + 12] = 0x50; // TCP data offset = 20 bytes (5 * 4)

    let cmd_payload = b"*3\r\n$3\r\nSET\r\n$8\r\nxdp_net2\r\n$4\r\npass\r\n";
    raw_packet.extend_from_slice(cmd_payload);

    let mut inject_net_frame = format!(
        "*3\r\n$10\r\nXDP.INJECT\r\n$1\r\n0\r\n${}\r\n",
        raw_packet.len()
    )
    .into_bytes();
    inject_net_frame.extend_from_slice(&raw_packet);
    inject_net_frame.extend_from_slice(b"\r\n");

    assert_eq!(send_and_read(&mut client, &inject_net_frame), "+OK\r\n");

    std::thread::sleep(Duration::from_millis(50));

    assert_eq!(
        send_and_read(&mut client, b"GET xdp_net2\r\n"),
        "$4\r\npass\r\n"
    );

    // 6. Test eBPF firewall rule enforcement: DROP 10.0.0.0/8
    let rule_add_resp = send_and_read(&mut client, b"XDP.RULE ADD DROP 10.0.0.0/8\r\n");
    assert!(rule_add_resp.starts_with(':'));
    let rule_id: u32 = rule_add_resp
        .trim_start_matches(':')
        .trim()
        .parse()
        .expect("Parsed rule ID");

    // Construct raw IPv4 packet from 10.1.2.3
    let mut drop_packet = vec![0u8; 40];
    drop_packet[0] = 0x45; // IPv4
    drop_packet[9] = 6;
    drop_packet[12] = 10;
    drop_packet[13] = 1;
    drop_packet[14] = 2;
    drop_packet[15] = 3;
    drop_packet.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$7\r\ndropkey\r\n$7\r\ndropval\r\n");

    let mut test_packet_cmd =
        format!("*2\r\n$10\r\nXDP.PACKET\r\n${}\r\n", drop_packet.len()).into_bytes();
    test_packet_cmd.extend_from_slice(&drop_packet);
    test_packet_cmd.extend_from_slice(b"\r\n");

    let action_resp = send_and_read(&mut client, &test_packet_cmd);
    assert_eq!(action_resp, "+DROP\r\n");

    // Clean up rule
    assert_eq!(
        send_and_read(
            &mut client,
            format!("XDP.RULE DEL {}\r\n", rule_id).as_bytes()
        ),
        "+OK\r\n"
    );
}

#[test]
fn test_sharded_scatter_gather_search_e2e() {
    let port = 17010;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Create index across 4 shards
    assert_eq!(
        send_and_read(
            &mut client,
            b"FT.CREATE idx:multishard ON HASH PREFIX 1 item: SCHEMA title TEXT price NUMERIC SORTABLE tag TAG\r\n"
        ),
        "+OK\r\n"
    );

    // 2. Populate 12 documents that distribute across shards
    for i in 1..=12 {
        let title = format!("Rust high performance cluster item {}", i);
        let price = (i * 10).to_string();
        let item_key = format!("item:{}", i);
        let cmd = format!(
            "*8\r\n$4\r\nHSET\r\n${}\r\n{}\r\n$5\r\ntitle\r\n${}\r\n{}\r\n$5\r\nprice\r\n${}\r\n{}\r\n$3\r\ntag\r\n$20\r\nhardware,distributed\r\n",
            item_key.len(),
            item_key,
            title.len(),
            title,
            price.len(),
            price
        );
        let resp = send_and_read(&mut client, cmd.as_bytes());
        assert_eq!(resp, ":3\r\n");
    }

    // 3. Scatter-gather search across all 4 shards: query "cluster" NOCONTENT
    let search_all = send_and_read(
        &mut client,
        b"FT.SEARCH idx:multishard cluster NOCONTENT\r\n",
    );
    assert!(
        search_all.starts_with("*11\r\n:12\r\n"),
        "All 12 documents from all 4 shards should be gathered, got: {}",
        search_all
    );

    // 4. Sorted search with pagination across shards: SORTBY price ASC LIMIT 0 5 NOCONTENT
    let search_paged = send_and_read(
        &mut client,
        b"FT.SEARCH idx:multishard cluster NOCONTENT SORTBY price ASC LIMIT 0 5\r\n",
    );
    assert_eq!(
        search_paged,
        "*6\r\n:12\r\n$6\r\nitem:1\r\n$6\r\nitem:2\r\n$6\r\nitem:3\r\n$6\r\nitem:4\r\n$6\r\nitem:5\r\n"
    );

    // 5. Delete item:1, verify it is immediately absent from scatter-gather search
    assert_eq!(send_and_read(&mut client, b"DEL item:1\r\n"), ":1\r\n");
    let search_after_del = send_and_read(
        &mut client,
        b"FT.SEARCH idx:multishard cluster NOCONTENT\r\n",
    );
    assert!(search_after_del.contains(":11\r\n"));
    assert!(!search_after_del.contains("item:1\r\n"));

    // 6. Drop index
    assert_eq!(
        send_and_read(&mut client, b"FT.DROPINDEX idx:multishard\r\n"),
        "+OK\r\n"
    );
    let search_dropped = send_and_read(&mut client, b"FT.SEARCH idx:multishard cluster\r\n");
    assert!(search_dropped.contains("ERR Unknown Index name"));
}

#[test]
fn test_numeric_range_search_across_shards_e2e() {
    let port = 17030;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Create index with TEXT and NUMERIC fields
    let create_resp = send_and_read(
        &mut client,
        b"FT.CREATE idx:catalog ON HASH PREFIX 1 item: SCHEMA title TEXT price NUMERIC SORTABLE stock NUMERIC SORTABLE\r\n",
    );
    assert_eq!(create_resp, "+OK\r\n");

    // 2. Insert items spanning across the 4 shards
    let items = [
        ("item:1", "item gaming mouse", "29.99", "100"),
        ("item:2", "item mechanical keyboard", "89.50", "45"),
        ("item:3", "item usb-c cable", "9.99", "250"),
        ("item:4", "item 4k monitor", "299.00", "15"),
        ("item:5", "item desk mat", "19.99", "80"),
        ("item:6", "item noise canceling headphones", "149.00", "30"),
        ("item:7", "item webcam pro", "69.99", "50"),
        ("item:8", "item microphone arm", "39.99", "60"),
    ];

    for (key, title, price, stock) in items {
        let cmd = format!(
            "*8\r\n$4\r\nHSET\r\n${}\r\n{}\r\n$5\r\ntitle\r\n${}\r\n{}\r\n$5\r\nprice\r\n${}\r\n{}\r\n$5\r\nstock\r\n${}\r\n{}\r\n",
            key.len(),
            key,
            title.len(),
            title,
            price.len(),
            price,
            stock.len(),
            stock
        );
        let resp = send_and_read(&mut client, cmd.as_bytes());
        assert_eq!(resp, ":3\r\n");
    }

    // Helper to format RESP command with automatic length calculation
    let format_resp_cmd = |args: &[&str]| -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            out.extend_from_slice(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
        }
        out
    };

    // 3. Search numeric range @price:[20 75] NOCONTENT
    // Should match: item:1 (29.99), item:7 (69.99), item:8 (39.99) -> 3 items
    let q1 = format_resp_cmd(&["FT.SEARCH", "idx:catalog", "@price:[20 75]", "NOCONTENT"]);
    let search_resp = send_and_read(&mut client, &q1);
    assert!(
        search_resp.contains(":3\r\n"),
        "Expected 3 matches, got: {}",
        search_resp
    );
    assert!(search_resp.contains("item:1"));
    assert!(search_resp.contains("item:7"));
    assert!(search_resp.contains("item:8"));
    assert!(!search_resp.contains("item:3")); // 9.99 < 20
    assert!(!search_resp.contains("item:2")); // 89.50 > 75

    // 4. Combined text + numeric range: @price:[50 300] with word "item"
    // Should match: item:2 (89.50), item:4 (299.00), item:6 (149.00), item:7 (69.99) -> 4 items
    let q2 = format_resp_cmd(&[
        "FT.SEARCH",
        "idx:catalog",
        "item @price:[50 300]",
        "NOCONTENT",
    ]);
    let search_resp2 = send_and_read(&mut client, &q2);
    assert!(
        search_resp2.contains(":4\r\n"),
        "Expected 4 matches, got: {}",
        search_resp2
    );
    assert!(search_resp2.contains("item:2"));
    assert!(search_resp2.contains("item:4"));
    assert!(search_resp2.contains("item:6"));
    assert!(search_resp2.contains("item:7"));

    // 5. Delete item:7 (69.99)
    assert_eq!(send_and_read(&mut client, b"DEL item:7\r\n"), ":1\r\n");

    // 6. Re-search @price:[20 75] NOCONTENT -> only 2 items remain
    let q3 = format_resp_cmd(&["FT.SEARCH", "idx:catalog", "@price:[20 75]", "NOCONTENT"]);
    let search_after_del = send_and_read(&mut client, &q3);
    assert!(
        search_after_del.contains(":2\r\n"),
        "Expected 2 matches, got: {}",
        search_after_del
    );
    assert!(!search_after_del.contains("item:7"));
    assert!(search_after_del.contains("item:1"));
    assert!(search_after_del.contains("item:8"));

    // Cleanup: Drop index
    assert_eq!(
        send_and_read(&mut client, b"FT.DROPINDEX idx:catalog\r\n"),
        "+OK\r\n"
    );
}

#[test]
fn test_ft_aggregate_multishard_pipeline_e2e() {
    let port = 17035;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    let format_resp_cmd = |args: &[&str]| -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            out.extend_from_slice(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
        }
        out
    };

    // 1. Create index
    let create_cmd = format_resp_cmd(&[
        "FT.CREATE",
        "idx:sales",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "sale:",
        "SCHEMA",
        "region",
        "TAG",
        "revenue",
        "NUMERIC",
        "SORTABLE",
    ]);
    assert_eq!(send_and_read(&mut client, &create_cmd), "+OK\r\n");

    // 2. Insert transactions across 4 shards
    let sales = [
        ("sale:1", "north", "100"),
        ("sale:2", "north", "250"),
        ("sale:3", "north", "150"),
        ("sale:4", "south", "80"),
        ("sale:5", "south", "120"),
        ("sale:6", "east", "300"),
        ("sale:7", "east", "400"),
        ("sale:8", "west", "50"),
        ("sale:9", "west", "150"),
        ("sale:10", "west", "100"),
    ];

    for (key, region, revenue) in sales {
        let set_cmd = format_resp_cmd(&["HSET", key, "region", region, "revenue", revenue]);
        let resp = send_and_read(&mut client, &set_cmd);
        assert_eq!(resp, ":2\r\n");
    }

    // 3. Test FT.AGGREGATE with GROUPBY + REDUCE + APPLY + SORTBY + LIMIT
    // GROUPBY 1 @region REDUCE COUNT 0 AS count REDUCE SUM 1 @revenue AS sum_rev APPLY @sum_rev * 1.05 AS rev_tax SORTBY 2 @rev_tax DESC LIMIT 0 2
    let agg_cmd = format_resp_cmd(&[
        "FT.AGGREGATE",
        "idx:sales",
        "*",
        "LOAD",
        "2",
        "@region",
        "@revenue",
        "GROUPBY",
        "1",
        "@region",
        "REDUCE",
        "COUNT",
        "0",
        "AS",
        "count",
        "REDUCE",
        "SUM",
        "1",
        "@revenue",
        "AS",
        "sum_rev",
        "APPLY",
        "@sum_rev * 1.05",
        "AS",
        "rev_tax",
        "SORTBY",
        "2",
        "@rev_tax",
        "DESC",
        "LIMIT",
        "0",
        "2",
    ]);

    let agg_resp = send_and_read(&mut client, &agg_cmd);
    // Should return 2 rows: East (700 * 1.05 = 735), North (500 * 1.05 = 525)
    assert!(
        agg_resp.starts_with("*3\r\n:2\r\n"),
        "Expected 2 rows, got: {}",
        agg_resp
    );
    assert!(agg_resp.contains("east"));
    assert!(agg_resp.contains("735"));
    assert!(agg_resp.contains("north"));
    assert!(agg_resp.contains("525"));

    // 4. Test global aggregation: GROUPBY 0 REDUCE COUNT 0 AS total REDUCE SUM 1 @revenue AS grand_total
    let global_agg_cmd = format_resp_cmd(&[
        "FT.AGGREGATE",
        "idx:sales",
        "*",
        "LOAD",
        "1",
        "@revenue",
        "GROUPBY",
        "0",
        "REDUCE",
        "COUNT",
        "0",
        "AS",
        "total",
        "REDUCE",
        "SUM",
        "1",
        "@revenue",
        "AS",
        "grand_total",
    ]);

    let global_resp = send_and_read(&mut client, &global_agg_cmd);
    assert!(
        global_resp.starts_with("*2\r\n:1\r\n"),
        "Expected 1 global row, got: {}",
        global_resp
    );
    assert!(global_resp.contains("total") && global_resp.contains("10"));
    assert!(global_resp.contains("grand_total") && global_resp.contains("1700"));

    // Cleanup: Drop index
    let drop_cmd = format_resp_cmd(&["FT.DROPINDEX", "idx:sales"]);
    assert_eq!(send_and_read(&mut client, &drop_cmd), "+OK\r\n");
}

#[test]
fn test_ft_search_hnsw_hybrid_rrf_multishard_e2e() {
    let port = 19140;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    let format_resp_cmd = |args: &[&str]| -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            out.extend_from_slice(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
        }
        out
    };

    // 1. Create FT index with HNSW vector field + TEXT + TAG across 4 shards
    let create_cmd = format_resp_cmd(&[
        "FT.CREATE",
        "idx:ai_docs",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "aidoc:",
        "SCHEMA",
        "content",
        "TEXT",
        "category",
        "TAG",
        "embedding",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "3",
        "DISTANCE_METRIC",
        "COSINE",
    ]);
    assert_eq!(send_and_read(&mut client, &create_cmd), "+OK\r\n");

    // 2. Insert 4 documents across shards:
    // aidoc:1 -> strong BM25 ("rust" x2) AND strong vector similarity to [1, 0, 0] -> wins RRF!
    // aidoc:2 -> no "rust" keyword, exact vector [1, 0, 0] -> #1 in pure KNN, lower in RRF
    // aidoc:3 -> has "rust" keyword, orthogonal vector [0, 1, 0]
    // aidoc:4 -> category "archive", vector [0.9, 0.4, 0.0]
    let docs = [
        (
            "aidoc:1",
            "rust vector database in rust for rag",
            "tech",
            "0.96, 0.28, 0.0",
        ),
        (
            "aidoc:2",
            "semantic memory store for agents",
            "tech",
            "1.0, 0.0, 0.0",
        ),
        (
            "aidoc:3",
            "rust compiler internals guide",
            "tech",
            "0.0, 1.0, 0.0",
        ),
        (
            "aidoc:4",
            "legacy archive document",
            "archive",
            "0.9, 0.4, 0.0",
        ),
    ];

    for (key, content, category, emb) in docs {
        let hset_cmd = format_resp_cmd(&[
            "HSET",
            key,
            "content",
            content,
            "category",
            category,
            "embedding",
            emb,
        ]);
        assert_eq!(send_and_read(&mut client, &hset_cmd), ":3\r\n");
    }

    let q_bytes: Vec<u8> = vec![1.0f32, 0.0f32, 0.0f32]
        .into_iter()
        .flat_map(|f| f.to_le_bytes())
        .collect();

    // 3. Pure HNSW KNN search across shards: *=>[KNN 2 @embedding $vec]
    let mut knn_cmd = Vec::new();
    let knn_args: &[&[u8]] = &[
        b"FT.SEARCH",
        b"idx:ai_docs",
        b"*=>[KNN 2 @embedding $vec]",
        b"PARAMS",
        b"2",
        b"vec",
        &q_bytes,
        b"NOCONTENT",
    ];
    knn_cmd.extend_from_slice(format!("*{}\r\n", knn_args.len()).as_bytes());
    for arg in knn_args {
        knn_cmd.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        knn_cmd.extend_from_slice(arg);
        knn_cmd.extend_from_slice(b"\r\n");
    }
    let knn_resp = send_and_read(&mut client, &knn_cmd);
    assert!(
        knn_resp.starts_with("*3\r\n:2\r\n$7\r\naidoc:2\r\n$7\r\naidoc:1\r\n"),
        "Unexpected pure KNN response: {}",
        knn_resp
    );

    // 4. Pre-filtered HNSW KNN search: @category:{archive}=>[KNN 1 @embedding $vec]
    let mut filt_cmd = Vec::new();
    let filt_args: &[&[u8]] = &[
        b"FT.SEARCH",
        b"idx:ai_docs",
        b"@category:{archive}=>[KNN 1 @embedding $vec]",
        b"PARAMS",
        b"2",
        b"vec",
        &q_bytes,
        b"NOCONTENT",
    ];
    filt_cmd.extend_from_slice(format!("*{}\r\n", filt_args.len()).as_bytes());
    for arg in filt_args {
        filt_cmd.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        filt_cmd.extend_from_slice(arg);
        filt_cmd.extend_from_slice(b"\r\n");
    }
    let filt_resp = send_and_read(&mut client, &filt_cmd);
    assert_eq!(filt_resp, "*2\r\n:1\r\n$7\r\naidoc:4\r\n");

    // 5. Hybrid BM25 + HNSW Reciprocal Rank Fusion (RRF):
    // Query: rust=>[KNN 2 @embedding $vec] RRF 60 WITHSCORES NOCONTENT
    // BM25("rust") matches aidoc:1 and aidoc:3.
    // KNN(2) matches aidoc:2 and aidoc:1.
    // aidoc:1 appears in BOTH rankings (BM25 + KNN), so RRF ranks aidoc:1 #1!
    let mut rrf_cmd = Vec::new();
    let rrf_args: &[&[u8]] = &[
        b"FT.SEARCH",
        b"idx:ai_docs",
        b"rust=>[KNN 2 @embedding $vec]",
        b"PARAMS",
        b"2",
        b"vec",
        &q_bytes,
        b"RRF",
        b"60",
        b"WITHSCORES",
        b"NOCONTENT",
    ];
    rrf_cmd.extend_from_slice(format!("*{}\r\n", rrf_args.len()).as_bytes());
    for arg in rrf_args {
        rrf_cmd.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        rrf_cmd.extend_from_slice(arg);
        rrf_cmd.extend_from_slice(b"\r\n");
    }
    let rrf_resp = send_and_read(&mut client, &rrf_cmd);
    assert!(
        rrf_resp.starts_with("*7\r\n:3\r\n$7\r\naidoc:1\r\n"),
        "Expected aidoc:1 to rank #1 in RRF hybrid search, got: {}",
        rrf_resp
    );

    // Cleanup
    let drop_cmd = format_resp_cmd(&["FT.DROPINDEX", "idx:ai_docs"]);
    assert_eq!(send_and_read(&mut client, &drop_cmd), "+OK\r\n");
}

#[test]
fn test_redis8_vector_sets_and_cross_shard_routing_e2e() {
    let port = 19150;
    start_test_server(port, 4);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let format_resp_cmd = |args: &[&str]| -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            out.extend_from_slice(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
        }
        out
    };

    // 1. Redis 8 VADD with VALUES, SETATTR, NOQUANT, M, EF
    let vadd1 = format_resp_cmd(&[
        "VADD",
        "movies:vset",
        "VALUES",
        "3",
        "1.0",
        "0.0",
        "0.0",
        "m1",
        "NOQUANT",
        "M",
        "16",
        "EF",
        "64",
        "SETATTR",
        r#"{"year":1999,"genre":"scifi"}"#,
    ]);
    assert_eq!(send_and_read(&mut client, &vadd1), ":1\r\n");

    // Re-adding m1 updates and returns :0
    assert_eq!(send_and_read(&mut client, &vadd1), ":0\r\n");

    let vadd2 = format_resp_cmd(&[
        "VADD",
        "movies:vset",
        "VALUES",
        "3",
        "0.9",
        "0.1",
        "0.0",
        "m2",
        "NOQUANT",
        "SETATTR",
        r#"{"year":2014,"genre":"scifi"}"#,
    ]);
    assert_eq!(send_and_read(&mut client, &vadd2), ":1\r\n");

    let vadd3 = format_resp_cmd(&[
        "VADD",
        "movies:vset",
        "VALUES",
        "3",
        "0.95",
        "0.05",
        "0.0",
        "m3",
        "NOQUANT",
        "SETATTR",
        r#"{"year":1972,"genre":"drama"}"#,
    ]);
    assert_eq!(send_and_read(&mut client, &vadd3), ":1\r\n");

    // 2. Keyspace integration: TYPE, EXISTS, VCARD, VDIM, VISMEMBER, VINFO
    assert_eq!(
        send_and_read(&mut client, b"TYPE movies:vset\r\n"),
        "+vectorset\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"EXISTS movies:vset\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"VCARD movies:vset\r\n"),
        ":3\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"VDIM movies:vset\r\n"),
        ":3\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"VISMEMBER movies:vset m2\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"VISMEMBER movies:vset missing\r\n"),
        ":0\r\n"
    );

    let vinfo = send_and_read(&mut client, b"VINFO movies:vset\r\n");
    assert!(vinfo.starts_with("*18\r\n"), "VINFO: {}", vinfo);
    assert!(vinfo.contains("quant-type\r\n$3\r\nf32\r\n"));
    assert!(vinfo.contains("vector-dim\r\n:3\r\n"));
    assert!(vinfo.contains("size\r\n:3\r\n"));
    assert!(vinfo.contains("attributes-count\r\n:3\r\n"));

    // 3. VGETATTR / VSETATTR / VEMB / VLINKS / VRANDMEMBER
    let attr = send_and_read(&mut client, b"VGETATTR movies:vset m1\r\n");
    assert!(attr.contains(r#""year":1999"#));
    let setattr_cmd = format_resp_cmd(&[
        "VSETATTR",
        "movies:vset",
        "m1",
        r#"{"year":2000,"genre":"scifi"}"#,
    ]);
    assert_eq!(send_and_read(&mut client, &setattr_cmd), ":1\r\n");
    assert!(send_and_read(&mut client, b"VGETATTR movies:vset m1\r\n").contains("2000"));

    let vemb = send_and_read(&mut client, b"VEMB movies:vset m1\r\n");
    assert!(vemb.starts_with("*3\r\n"), "VEMB: {}", vemb);
    let vemb_raw = send_and_read(&mut client, b"VEMB movies:vset m1 RAW\r\n");
    assert!(
        vemb_raw.starts_with("*3\r\n$4\r\nfp32\r\n"),
        "VEMB RAW: {}",
        vemb_raw
    );

    let vlinks = send_and_read(&mut client, b"VLINKS movies:vset m1 WITHSCORES\r\n");
    assert!(vlinks.starts_with("*"), "VLINKS: {}", vlinks);

    let vrand = send_and_read(&mut client, b"VRANDMEMBER movies:vset 2\r\n");
    assert!(vrand.starts_with("*2\r\n"), "VRANDMEMBER: {}", vrand);

    // 4. VSIM with ELE, VALUES, WITHSCORES, WITHATTRIBS, FILTER
    let vsim_ele = format_resp_cmd(&["VSIM", "movies:vset", "ELE", "m1", "COUNT", "2"]);
    let resp_ele = send_and_read(&mut client, &vsim_ele);
    assert!(
        resp_ele.starts_with("*2\r\n$2\r\nm1\r\n$2\r\nm3\r\n"),
        "VSIM ELE: {}",
        resp_ele
    );

    let vsim_filtered = format_resp_cmd(&[
        "VSIM",
        "movies:vset",
        "VALUES",
        "3",
        "1.0",
        "0.0",
        "0.0",
        "WITHSCORES",
        "WITHATTRIBS",
        "FILTER",
        r#".genre == "scifi" and .year >= 2005"#,
    ]);
    let resp_filt = send_and_read(&mut client, &vsim_filtered);
    assert!(
        resp_filt.starts_with("*3\r\n$2\r\nm2\r\n") && !resp_filt.contains("m3"),
        "VSIM FILTER: {}",
        resp_filt
    );

    // 5. Cross-shard VQUERY & VREM cleanup
    let vq = send_and_read(&mut client, b"VQUERY movies:vset 3 1.0 0.0 0.0\r\n");
    assert!(vq.starts_with("*6\r\n"), "Cross-shard VQUERY: {}", vq);

    assert_eq!(
        send_and_read(&mut client, b"VREM movies:vset m1\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"VREM movies:vset m2\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"VREM movies:vset m3\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"EXISTS movies:vset\r\n"),
        ":0\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"TYPE movies:vset\r\n"),
        "+none\r\n"
    );
}

#[test]
fn test_ft_create_backfill_ft_info_vector_stats_and_vector_range_e2e() {
    let port = 19151;
    start_test_server(port, 4);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let format_resp_cmd = |args: &[&str]| -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            out.extend_from_slice(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
        }
        out
    };

    // 1. Insert HASH documents BEFORE FT.CREATE to verify backfill across shards
    for (id, cat, vec_str) in [
        ("bfitem:1", "gpu", "1.0, 0.0, 0.0"),
        ("bfitem:2", "cpu", "0.8, 0.2, 0.0"),
        ("bfitem:3", "gpu", "-1.0, 0.0, 0.0"),
    ] {
        let cmd = format_resp_cmd(&["HSET", id, "cat", cat, "vec", vec_str]);
        assert_eq!(send_and_read(&mut client, &cmd), ":2\r\n");
    }

    // 2. Create search index with HNSW vector field after data already exists
    let create = format_resp_cmd(&[
        "FT.CREATE",
        "idx:bfitems",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "bfitem:",
        "SCHEMA",
        "cat",
        "TAG",
        "vec",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "3",
        "DISTANCE_METRIC",
        "L2",
    ]);
    assert_eq!(send_and_read(&mut client, &create), "+OK\r\n");

    // 3. FT.INFO should report backfilled num_docs=3, num_vectors=3, algorithm=HNSW, vector_index_sz_mb, and hash_indexing_failures
    let info = send_and_read(&mut client, b"FT.INFO idx:bfitems\r\n");
    assert!(info.contains("num_docs\r\n:3\r\n"), "FT.INFO: {}", info);
    assert!(info.contains("num_vectors\r\n:3\r\n"), "FT.INFO: {}", info);
    assert!(info.contains("HNSW"), "FT.INFO: {}", info);
    assert!(info.contains("vector_index_sz_mb"), "FT.INFO: {}", info);
    assert!(info.contains("hash_indexing_failures"), "FT.INFO: {}", info);

    // 4. FT.SEARCH with @vec:[VECTOR_RANGE ...], DIALECT 2, and TIMEOUT 5000
    let search = format_resp_cmd(&[
        "FT.SEARCH",
        "idx:bfitems",
        "@vec:[VECTOR_RANGE 0.5 $q]=>{$YIELD_DISTANCE_AS: dist}",
        "PARAMS",
        "2",
        "q",
        "1.0, 0.0, 0.0",
        "DIALECT",
        "2",
        "TIMEOUT",
        "5000",
    ]);
    let s_resp = send_and_read(&mut client, &search);
    assert!(
        s_resp.starts_with("*5\r\n:2\r\n$8\r\nbfitem:1\r\n") && s_resp.contains("bfitem:2"),
        "VECTOR_RANGE search: {}",
        s_resp
    );
}

#[test]
fn test_llm_quota_governor_reserve_and_settle_e2e() {
    let port = 19153;
    start_test_server(port, 4);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // 1. Reserve 700 tokens under RPM=2, TPM=1000
    let r1 = send_and_read(
        &mut client,
        b"LLM.QUOTA.RESERVE tenant:openai RPM 2 TPM 1000 EST_TOKENS 700 WINDOW 60000\r\n",
    );
    assert_eq!(r1, "*4\r\n:1\r\n:1\r\n:300\r\n:0\r\n");

    // 2. Try reserving 500 tokens -> rejected because 700 + 500 > 1000
    let r2 = send_and_read(
        &mut client,
        b"LLM.QUOTA.RESERVE tenant:openai RPM 2 TPM 1000 EST_TOKENS 500\r\n",
    );
    assert!(
        r2.starts_with("*4\r\n:0\r\n$-1\r\n:300\r\n:"),
        "Expected TPM rejection: {}",
        r2
    );

    // 3. Settle reservation 1 with ACTUAL_TOKENS 400 (refunds 300 tokens -> net delta -300)
    let settle = send_and_read(
        &mut client,
        b"LLM.QUOTA.SETTLE tenant:openai 1 ACTUAL_TOKENS 400\r\n",
    );
    assert_eq!(settle, "*2\r\n:1\r\n:-300\r\n");

    // 4. Now reserving 500 tokens succeeds (400 + 500 = 900 <= 1000, remaining=100)
    let r3 = send_and_read(
        &mut client,
        b"LLM.QUOTA.RESERVE tenant:openai RPM 2 TPM 1000 EST_TOKENS 500\r\n",
    );
    assert_eq!(r3, "*4\r\n:1\r\n:2\r\n:100\r\n:0\r\n");

    // 5. Inspect LLM.QUOTA.INFO
    let info = send_and_read(&mut client, b"LLM.QUOTA.INFO tenant:openai\r\n");
    assert!(
        info.contains("active_requests\r\n:2\r\n")
            && info.contains("used_tokens\r\n:400\r\n")
            && info.contains("reserved_tokens\r\n:500\r\n"),
        "LLM.QUOTA.INFO: {}",
        info
    );
}

#[test]
fn test_ft_hybrid_alter_list_profile_and_multi_vector_json_e2e() {
    let port = 19154;
    start_test_server(port, 4);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // 1. Pre-populate multi-chunk JSON documents
    let j1 = r#"{"title":"rust hnsw guide","tag":"ai","chunks":[{"emb":[1.0,0.0,0.0]},{"emb":[0.0,1.0,0.0]}]}"#;
    let j2 = r#"{"title":"rust parser guide","tag":"sys","chunks":[{"emb":[1.0,0.0,0.0]}]}"#;
    for (k, body) in [("ragdoc:1", j1), ("ragdoc:2", j2)] {
        let cmd = format_resp_cmd(&["JSON.SET", k, "$", body]);
        assert_eq!(send_and_read(&mut client, &cmd), "+OK\r\n");
    }

    // 2. Create JSON index with multi-vector path $.chunks[*].emb
    let create = format_resp_cmd(&[
        "FT.CREATE",
        "idx:rag_a3",
        "ON",
        "JSON",
        "PREFIX",
        "1",
        "ragdoc:",
        "SCHEMA",
        "$.title",
        "AS",
        "title",
        "TEXT",
        "$.chunks[*].emb",
        "AS",
        "emb",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "3",
        "DISTANCE_METRIC",
        "COSINE",
    ]);
    assert_eq!(send_and_read(&mut client, &create), "+OK\r\n");

    // 3. FT._LIST includes idx:rag_a3
    let list_resp = send_and_read(&mut client, b"FT._LIST\r\n");
    assert!(
        list_resp.contains("idx:rag_a3"),
        "FT._LIST should list idx:rag_a3, got: {}",
        list_resp
    );

    // 4. FT.ALTER adds $.tag AS tag TAG and backfills existing JSON documents
    let alter = format_resp_cmd(&[
        "FT.ALTER",
        "idx:rag_a3",
        "SCHEMA",
        "ADD",
        "$.tag",
        "AS",
        "tag",
        "TAG",
    ]);
    assert_eq!(send_and_read(&mut client, &alter), "+OK\r\n");
    let tag_search = format_resp_cmd(&["FT.SEARCH", "idx:rag_a3", "@tag:{ai}", "NOCONTENT"]);
    assert_eq!(
        send_and_read(&mut client, &tag_search),
        "*2\r\n:1\r\n$8\r\nragdoc:1\r\n"
    );

    // 5. FT.HYBRID with SCORER LINEAR and multi-vector chunk hit ([0, 1, 0] matches ragdoc:1's 2nd chunk)
    let q_bytes: Vec<u8> = [0.0f32, 1.0, 0.0]
        .into_iter()
        .flat_map(|f| f.to_le_bytes())
        .collect();
    let hybrid_cmd = format_resp_bytes(&[
        b"FT.HYBRID".as_slice(),
        b"idx:rag_a3".as_slice(),
        b"rust".as_slice(),
        b"[KNN 2 @emb $q]".as_slice(),
        b"SCORER".as_slice(),
        b"LINEAR".as_slice(),
        b"ALPHA".as_slice(),
        b"0.3".as_slice(),
        b"BETA".as_slice(),
        b"0.7".as_slice(),
        b"PARAMS".as_slice(),
        b"2".as_slice(),
        b"q".as_slice(),
        q_bytes.as_slice(),
        b"NOCONTENT".as_slice(),
        b"WITHSCORES".as_slice(),
    ]);
    let hybrid_resp = send_and_read(&mut client, &hybrid_cmd);
    assert!(
        hybrid_resp.starts_with("*5\r\n:2\r\n$8\r\nragdoc:1\r\n"),
        "FT.HYBRID LINEAR should rank ragdoc:1 first: {}",
        hybrid_resp
    );

    // 6. FT.PROFILE returns *2 [search_results, profile_details]
    let prof_cmd = format_resp_cmd(&[
        "FT.PROFILE",
        "idx:rag_a3",
        "SEARCH",
        "QUERY",
        "rust",
        "NOCONTENT",
    ]);
    let prof_resp = send_and_read(&mut client, &prof_cmd);
    assert!(
        prof_resp.starts_with("*2\r\n*3\r\n:2\r\n")
            && prof_resp.contains("Total profile time")
            && prof_resp.contains("Query plan"),
        "FT.PROFILE response: {}",
        prof_resp
    );
}

#[test]
fn test_agent_checkpoint_dag_and_tool_idempotency_e2e() {
    let port = 19155;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // 1. AGENT.CHECKPOINT.PUT DAG nodes: root -> step_a -> step_b
    let put_root = format_resp_cmd(&[
        "AGENT.CHECKPOINT.PUT",
        "thread:agent4",
        "root",
        "STATE",
        r#"{"messages":["init"]}"#,
        "META",
        r#"{"node":"start"}"#,
    ]);
    assert_eq!(send_and_read(&mut client, &put_root), ":1\r\n");

    let put_a = format_resp_cmd(&[
        "AGENT.CHECKPOINT.PUT",
        "thread:agent4",
        "step_a",
        "PARENT",
        "root",
        "STATE",
        r#"{"messages":["init","plan"]}"#,
    ]);
    assert_eq!(send_and_read(&mut client, &put_a), ":2\r\n");

    let put_b = format_resp_cmd(&[
        "AGENT.CHECKPOINT.PUT",
        "thread:agent4",
        "step_b",
        "PARENT",
        "step_a",
        "STATE",
        r#"{"messages":["init","plan","done"]}"#,
    ]);
    assert_eq!(send_and_read(&mut client, &put_b), ":3\r\n");

    // 2. AGENT.CHECKPOINT.GET latest (step_b) and specific (root)
    let get_latest = format_resp_cmd(&["AGENT.CHECKPOINT.GET", "thread:agent4"]);
    let latest_resp = send_and_read(&mut client, &get_latest);
    assert!(
        latest_resp.contains("step_b") && latest_resp.contains("step_a"),
        "latest checkpoint: {}",
        latest_resp
    );

    let get_root = format_resp_cmd(&["AGENT.CHECKPOINT.GET", "thread:agent4", "STEP", "root"]);
    let root_resp = send_and_read(&mut client, &get_root);
    assert!(
        root_resp.contains("root") && root_resp.contains(r#"{"node":"start"}"#),
        "root checkpoint: {}",
        root_resp
    );

    // 3. AGENT.CHECKPOINT.HISTORY walks DAG lineage step_b -> step_a -> root
    let hist_cmd = format_resp_cmd(&[
        "AGENT.CHECKPOINT.HISTORY",
        "thread:agent4",
        "FROM",
        "step_b",
        "LIMIT",
        "10",
    ]);
    let hist_resp = send_and_read(&mut client, &hist_cmd);
    assert!(
        hist_resp.starts_with("*3\r\n")
            && hist_resp.contains("step_b")
            && hist_resp.contains("step_a")
            && hist_resp.contains("root"),
        "history response: {}",
        hist_resp
    );

    // 4. AGENT.TOOL.CLAIM & AGENT.TOOL.COMPLETE idempotency
    let claim1 = format_resp_cmd(&[
        "AGENT.TOOL.CLAIM",
        "tools:agent4",
        "call_42",
        "TTL",
        "30000",
        "INPUT",
        r#"{"amount":100}"#,
    ]);
    let claim1_resp = send_and_read(&mut client, &claim1);
    assert!(
        claim1_resp.starts_with("*3\r\n$7\r\nCLAIMED\r\n$-1\r\n"),
        "first claim should be CLAIMED: {}",
        claim1_resp
    );

    // Duplicate claim while in-flight -> IN_PROGRESS
    let claim2_resp = send_and_read(&mut client, &claim1);
    assert!(
        claim2_resp.starts_with("*3\r\n$11\r\nIN_PROGRESS\r\n$-1\r\n"),
        "second claim should be IN_PROGRESS: {}",
        claim2_resp
    );

    // Complete tool call
    let complete = format_resp_cmd(&[
        "AGENT.TOOL.COMPLETE",
        "tools:agent4",
        "call_42",
        "OUTPUT",
        r#"{"tx_id":"tx_999"}"#,
    ]);
    assert_eq!(send_and_read(&mut client, &complete), ":1\r\n");

    // Subsequent claim returns cached output with COMPLETED status
    let claim3_resp = send_and_read(&mut client, &claim1);
    assert!(
        claim3_resp.starts_with("*3\r\n$9\r\nCOMPLETED\r\n")
            && claim3_resp.contains(r#"{"tx_id":"tx_999"}"#),
        "third claim should return COMPLETED with cached output: {}",
        claim3_resp
    );
}

#[test]
#[cfg_attr(not(feature = "mcp"), ignore = "needs the mcp cargo feature")]
fn test_mcp_protocol_tools_call_and_rpc_e2e() {
    let port = 19156;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // 1. MCP.TOOLS returns built-in MCP tool definitions
    let tools_cmd = format_resp_cmd(&["MCP.TOOLS"]);
    let tools_resp = send_and_read(&mut client, &tools_cmd);
    assert!(
        tools_resp.contains("rudis_kv_set")
            && tools_resp.contains("rudis_semantic_get")
            && tools_resp.contains("rudis_agent_memory_context"),
        "MCP.TOOLS should list built-in tools: {}",
        tools_resp
    );

    // 2. MCP.CALL rudis_kv_set and rudis_kv_get
    let call_set = format_resp_cmd(&[
        "MCP.CALL",
        "rudis_kv_set",
        r#"{"key":"mcp:k1","value":"mcp_hello"}"#,
    ]);
    let set_resp = send_and_read(&mut client, &call_set);
    assert!(
        set_resp.contains(r#""isError":false"#) && set_resp.contains("OK"),
        "MCP.CALL rudis_kv_set: {}",
        set_resp
    );

    let call_get = format_resp_cmd(&["MCP.CALL", "rudis_kv_get", r#"{"key":"mcp:k1"}"#]);
    let get_resp = send_and_read(&mut client, &call_get);
    assert!(
        get_resp.contains(r#""isError":false"#) && get_resp.contains("mcp_hello"),
        "MCP.CALL rudis_kv_get: {}",
        get_resp
    );

    // 3. MCP.RPC initialize & tools/call (rudis_agent_memory_add + rudis_agent_memory_context)
    let init_rpc = format_resp_cmd(&[
        "MCP.RPC",
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
    ]);
    let init_resp = send_and_read(&mut client, &init_rpc);
    assert!(
        init_resp.contains("rudis-mcp") && init_resp.contains("2024-11-05"),
        "MCP.RPC initialize: {}",
        init_resp
    );

    let mem_add_rpc = format_resp_cmd(&[
        "MCP.RPC",
        r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"rudis_agent_memory_add","arguments":{"session":"mcp_sess","role":"user","content":"remember secret code 7788","vector":[1.0,0.0,0.0]}}}"#,
    ]);
    let mem_add_resp = send_and_read(&mut client, &mem_add_rpc);
    assert!(
        mem_add_resp.contains(r#""isError":false"#),
        "MCP.RPC tools/call rudis_agent_memory_add: {}",
        mem_add_resp
    );

    let mem_ctx_rpc = format_resp_cmd(&[
        "MCP.RPC",
        r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"rudis_agent_memory_context","arguments":{"session":"mcp_sess","max_tokens":500,"query_vector":[1.0,0.0,0.0],"recall_k":2}}}"#,
    ]);
    let mem_ctx_resp = send_and_read(&mut client, &mem_ctx_rpc);
    assert!(
        mem_ctx_resp.contains("remember secret code 7788")
            && mem_ctx_resp.contains(r#""isError":false"#),
        "MCP.RPC tools/call rudis_agent_memory_context: {}",
        mem_ctx_resp
    );
}

/// Deletes that leave each table segment's entry arena still over half live
/// do not trim it inline; the shard cron's arena trim then gives that space
/// back, visible in `MEMORY STATS` as shrinking free-arena bytes, with every
/// surviving key intact.
#[test]
fn test_cron_trims_entry_arenas_left_part_empty_by_deletes_e2e() {
    let port = 18011;
    start_test_server(port, 1);
    let mut c = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let n = 100_000;
    for batch in 0..(n / 1000) {
        let mut cmd = String::from("MSET");
        for i in 0..1000 {
            cmd.push_str(&format!(" t:{:06} v", batch * 1000 + i));
        }
        cmd.push_str("\r\n");
        assert_eq!(send_and_read(&mut c, cmd.as_bytes()), "+OK\r\n");
    }
    let free_arena = |c: &mut TcpStream| -> u64 {
        let raw = send_and_read_reply(c, b"MEMORY STATS\r\n");
        let toks: Vec<&str> = raw.split("\r\n").collect();
        let i = toks
            .iter()
            .position(|t| *t == "rudis.free-arena.bytes")
            .expect("field");
        toks[i + 1].trim_start_matches(':').parse().unwrap()
    };
    // Delete 30% (every arena stays over half live).
    for batch in 0..(n / 1000) {
        let mut cmd = String::from("DEL");
        for i in 0..1000 {
            let k = batch * 1000 + i;
            if k % 10 < 3 {
                cmd.push_str(&format!(" t:{k:06}"));
            }
        }
        cmd.push_str("\r\n");
        assert_eq!(send_and_read(&mut c, cmd.as_bytes()), ":300\r\n");
    }
    let before = free_arena(&mut c);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut after = before;
    while std::time::Instant::now() < deadline {
        thread::sleep(Duration::from_millis(300));
        after = free_arena(&mut c);
        if after * 2 < before {
            break;
        }
    }
    assert!(
        after * 2 < before,
        "free arena {before} -> {after}: cron trim did not run"
    );
    assert_eq!(send_and_read(&mut c, b"DBSIZE\r\n"), ":70000\r\n");
    assert_eq!(
        send_and_read(&mut c, b"EXISTS t:000002 t:000004 t:099999\r\n"),
        ":2\r\n"
    );
}

/// MCP and XDP are cargo features (default on): a build without them
/// refuses their commands, the rest of the server unchanged.
#[test]
fn test_mcp_and_xdp_follow_their_cargo_features_e2e() {
    let port = 29993u16;
    start_test_server(port, 2);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mcp = resp_cmd_all(&mut c, &["MCP.TOOLS"]);
    let xdp = resp_cmd_all(&mut c, &["XDP.RULE", "LIST"]);
    if cfg!(feature = "mcp") {
        assert!(mcp.starts_with('*'), "{mcp}");
    } else {
        assert!(mcp.contains("cargo feature mcp"), "{mcp}");
    }
    if cfg!(feature = "xdp") {
        assert!(!xdp.starts_with('-'), "{xdp}");
    } else {
        assert!(xdp.contains("cargo feature xdp"), "{xdp}");
    }
    assert_eq!(resp_cmd_all(&mut c, &["PING"]), "+PONG\r\n");
}
