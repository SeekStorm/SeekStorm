//! End-to-End Test (Spinning Up both Client and Server)
//! Test crate: tests need to run sequentially in the defined order (in alphabetical order of method name), not in parallel (see RUST_TEST_THREADS = "1" in .cargo/config.toml).
//! Use: cargo test -p seekstorm_client_rs
//! Note: The tests will automatically build the server binary if it doesn't exist, and launch it for testing. It will also clean up the index_test folder before running the tests.
//! If a test fails, the server process needs to be killed manually, e.g. by restarting VSC

use seekstorm::{
    highlighter::Highlight,
    index::{
        ApikeyQuotaObject, Clustering, CreateIndexRequest, Document, DocumentCompression,
        FrequentwordType, GetDocumentRequest, LexicalSimilarity, NgramSet, SearchRequestObject,
        StemmerType, StopwordType, TokenizerType,
    },
    search::{QueryRewriting, QueryType, ResultType, SearchMode},
    vector::Inference,
};
use seekstorm_client_rs::api_endpoints::RestClient;
use std::{env, fs, path::PathBuf, sync::LazyLock, time::Duration};
use tokio::process::Command;
use tokio::time::sleep;

pub static CLIENT: LazyLock<RestClient> = LazyLock::new(|| RestClient::new());

pub static SERVER_BINARY_PATH: LazyLock<PathBuf> = LazyLock::new(|| get_server_binary_path());

pub static BASE_URL: &str = "http://127.0.0.1:80";
pub static DEMO_API_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
pub static MASTER_API_KEY: &str = "/iWStCpyfpd/BVlHOFtwnMgrFrmof4jGq/OQDWXQzcM=";

fn get_server_binary_path() -> PathBuf {
    // 1. Start from the client package directory (where the test executes)
    let mut path = env::current_dir().expect("Failed to get current dir");

    // 2. Pop out to the workspace root directory
    path.pop();

    // 3. Navigate into the shared workspace target output directory
    path.push("target");

    // If you run 'cargo test', it builds under 'debug'.
    // If you run 'cargo test --release', it builds under 'release'.
    if cfg!(debug_assertions) {
        path.push("debug");
    } else {
        path.push("release");
    }

    // 4. Append the executable binary name (adds .exe automatically on Windows)
    path.push(if cfg!(target_os = "windows") {
        "seekstorm_server.exe"
    } else {
        "seekstorm_server"
    });

    path
}

/// Info
#[tokio::test]
async fn test_20_live() {
    // ### automatically build the server binary if it doesn't exist

    // 1. Force cargo to verify and build the server binary before proceeding
    let mut build_cmd = Command::new("cargo");
    build_cmd.args(["build", "-p", "seekstorm_server"]);

    // Automatically match debug/release profile of your current test execution
    if !cfg!(debug_assertions) {
        build_cmd.arg("--release");
    }

    let build_status = build_cmd
        .status()
        .await
        .expect("Failed to run cargo build command");
    assert!(
        build_status.success(),
        "E2E Setup Error: Failed to compile seekstorm_server automatically"
    );

    // ###

    assert!(
        SERVER_BINARY_PATH.exists(),
        "Server binary not found at {:?}. Did you run 'cargo build' first?",
        SERVER_BINARY_PATH
    );

    //clear index_test folder
    let index_path = SERVER_BINARY_PATH.parent().unwrap().join("index_test");
    let _ = fs::remove_dir_all(index_path);

    // 1. Automatically launch the freshly built server binary
    let mut server_child = Command::new(&*SERVER_BINARY_PATH)
        .arg(format!("local_ip=127.0.0.1"))
        .arg(format!("local_port=80"))
        .arg(format!("index_path=index_test"))
        // 👇 Set environment variables specifically for the server process here
        //.env("MASTER_KEY_SECRET", "1234")
        .spawn()
        .expect("Failed to start seekstorm_server binary");

    // 2. Let the server bind to the port
    sleep(Duration::from_millis(600)).await;

    // 3. Test your library against it
    let result = CLIENT.live(BASE_URL).await;

    // 4. Assertions
    assert!(result.is_ok());

    // 5. Teardown server safely
    let _ = server_child.kill().await;
}

/// create_apikey test
#[tokio::test]
async fn test_21_create_apikey() {
    assert!(
        SERVER_BINARY_PATH.exists(),
        "Server binary not found at {:?}. Did you run 'cargo build' first?",
        SERVER_BINARY_PATH
    );

    // 1. Automatically launch the freshly built server binary
    let mut server_child = Command::new(&*SERVER_BINARY_PATH)
        .arg(format!("local_ip=127.0.0.1"))
        .arg(format!("local_port=80"))
        .arg(format!("index_path=index_test"))
        .spawn()
        .expect("Failed to start seekstorm_server binary");

    // 2. Let the server bind to the port
    sleep(Duration::from_millis(600)).await;

    // 3. Test your library against it
    let apikey_quota_object = ApikeyQuotaObject {
        indices_max: 10,
        indices_size_max: 100_000_000_000,
        documents_max: 100_000_000,
        operations_max: 1_000_000_000,
        rate_limit: None,
        demo: true,
        ..Default::default()
    };

    let result = CLIENT
        //todo: demo apikey flag
        //todo: check if master api key is always the same?
        .create_apikey(BASE_URL, MASTER_API_KEY, &apikey_quota_object)
        .await;

    assert_eq!(result, Ok(DEMO_API_KEY.to_string()));

    // 4. Teardown server safely
    let _ = server_child.kill().await;
}

/// create_index test
#[tokio::test]
async fn test_22_create_index() {
    assert!(
        SERVER_BINARY_PATH.exists(),
        "Server binary not found at {:?}. Did you run 'cargo build' first?",
        SERVER_BINARY_PATH
    );

    // 1. Automatically launch the freshly built server binary
    let mut server_child = Command::new(&*SERVER_BINARY_PATH)
        .arg(format!("local_ip=127.0.0.1"))
        .arg(format!("local_port=80"))
        .arg(format!("index_path=index_test"))
        .spawn()
        .expect("Failed to start seekstorm_server binary");

    // 2. Let the server bind to the port
    sleep(Duration::from_millis(600)).await;

    // 3. Test your library against it
    let schema_json = r#"
    [{"field":"title","field_type":"Text","store":false,"index_lexical":false},
    {"field":"body","field_type":"Text","store":true,"index_lexical":true,"longest":true},
    {"field":"url","field_type":"Text","store":false,"index_lexical":false}]"#;
    let schema = serde_json::from_str(schema_json).unwrap();

    let create_index_request = CreateIndexRequest {
        index_name: "test_index".into(),
        similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::UnicodeAlphanumeric,
        stemmer: StemmerType::None,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::English,
        synonyms: Vec::new(), //not supported in REST API?
        ngram_indexing: NgramSet::NgramFF as u8 | NgramSet::NgramFFF as u8,
        document_compression: DocumentCompression::Snappy,
        spelling_correction: None,
        query_completion: None,
        clustering: Clustering::None,
        inference: Inference::None,
        schema,
    };
    let result = CLIENT
        .create_index(BASE_URL, DEMO_API_KEY, &create_index_request)
        .await;

    assert_eq!(result, Ok(0));

    // 4. Teardown server safely
    let _ = server_child.kill().await;
}

/// index_documents test
#[tokio::test]
async fn test_23_index_documents() {
    assert!(
        SERVER_BINARY_PATH.exists(),
        "Server binary not found at {:?}. Did you run 'cargo build' first?",
        SERVER_BINARY_PATH
    );

    // 1. Automatically launch the freshly built server binary
    let mut server_child = Command::new(&*SERVER_BINARY_PATH)
        .arg(format!("local_ip=127.0.0.1"))
        .arg(format!("local_port=80"))
        .arg(format!("index_path=index_test"))
        .spawn()
        .expect("Failed to start seekstorm_server binary");

    // 2. Let the server bind to the port
    sleep(Duration::from_millis(600)).await;

    // index document
    let document_json = r#"
    {"title":"title1 test","body":"body1","url":"url1"}"#;
    let document: Document = serde_json::from_str(document_json).unwrap();

    let _result = CLIENT
        .index_document(BASE_URL, DEMO_API_KEY, 0, &document)
        .await;

    // index documents
    let documents_json = r#"
    [{"title":"title1 test","body":"body1","url":"url1"},
    {"title":"title2","body":"body2 test","url":"url2"},
    {"title":"title3 test","body":"body3 test","url":"url3"}]"#;
    let documents_vec: Vec<Document> = serde_json::from_str(documents_json).unwrap();

    let _result = CLIENT
        .index_documents(BASE_URL, DEMO_API_KEY, 0, &documents_vec)
        .await;

    // commit
    let result = CLIENT.commit_index(BASE_URL, DEMO_API_KEY, 0).await;

    assert_eq!(result, Ok(4));

    // 4. Teardown server safely
    let _ = server_child.kill().await;
}

/// query_index test
#[tokio::test]
async fn test_24_query_index() {
    assert!(
        SERVER_BINARY_PATH.exists(),
        "Server binary not found at {:?}. Did you run 'cargo build' first?",
        SERVER_BINARY_PATH
    );

    // 1. Automatically launch the freshly built server binary
    let mut server_child = Command::new(&*SERVER_BINARY_PATH)
        .arg(format!("local_ip=127.0.0.1"))
        .arg(format!("local_port=80"))
        .arg(format!("index_path=index_test"))
        .spawn()
        .expect("Failed to start seekstorm_server binary");

    // 2. Let the server bind to the port
    sleep(Duration::from_millis(600)).await;

    // 3. Test your library against it
    let query = "+body2 +test".into();

    let search_request_object = SearchRequestObject {
        query_string: query,
        query_vector: None,
        enable_empty_query: false,
        offset: 0,
        length: 10,
        result_type: ResultType::TopkCount,
        query_type_default: QueryType::Intersection,
        search_mode: SearchMode::Lexical,
        realtime: false,
        query_rewriting: QueryRewriting::SearchOnly,
        highlights: Vec::new(),
        fields: Vec::new(),
        field_filter: Vec::new(),
        facet_filter: Vec::new(),
        distance_fields: Vec::new(),
        query_facets: Vec::new(),
        result_sort: Vec::new(),
    };

    let result_object = CLIENT
        .query_index(BASE_URL, DEMO_API_KEY, 0, search_request_object)
        .await;

    //test for SearchResultObject.count_total==1
    assert_eq!(
        result_object
            .as_ref()
            .map(|search_result| search_result.count_total),
        Ok(1)
    );

    for result in result_object.as_ref().unwrap().results.iter() {
        println!(
            "result {:?} rank {:?} body field {:?}",
            result.get("_id"),
            result.get("_score"),
            result.get("body")
        );
    }
    println!(
        "result counts {} {} {}",
        result_object.as_ref().unwrap().results.len(),
        result_object.as_ref().unwrap().count,
        result_object.as_ref().unwrap().count_total
    );

    // 4. Teardown server safely
    let _ = server_child.kill().await;
}

/// get_document test
#[tokio::test]
async fn test_25_get_document() {
    assert!(
        SERVER_BINARY_PATH.exists(),
        "Server binary not found at {:?}. Did you run 'cargo build' first?",
        SERVER_BINARY_PATH
    );

    // 1. Automatically launch the freshly built server binary
    let mut server_child = Command::new(&*SERVER_BINARY_PATH)
        .arg(format!("local_ip=127.0.0.1"))
        .arg(format!("local_port=80"))
        .arg(format!("index_path=index_test"))
        .spawn()
        .expect("Failed to start seekstorm_server binary");

    // 2. Let the server bind to the port
    sleep(Duration::from_millis(600)).await;

    let highlights: Vec<Highlight> = vec![Highlight {
        field: "body".to_string(),
        name: String::new(),
        fragment_number: 2,
        fragment_size: 160,
        highlight_markup: true,
        ..Default::default()
    }];

    let get_document_request = GetDocumentRequest {
        query_terms: Vec::new(),
        highlights: highlights,
        fields: Vec::new(),
        distance_fields: Vec::new(),
    };

    let doc = CLIENT
        .get_document(BASE_URL, DEMO_API_KEY, 0, 0, &get_document_request)
        .await
        .unwrap();

    assert_eq!(doc.len(), 1);

    // 4. Teardown server safely
    let _ = server_child.kill().await;
}
