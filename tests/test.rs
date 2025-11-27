//! Test crate: tests need to run sequentially in the defined order (in alphabetical order of method name), not in parallel (see RUST_TEST_THREADS = "1" in .cargo/config.toml).
//! Use: cargo test
//! To show output use: cargo test -- --show-output

use seekstorm::commit::{Close, Commit};
use seekstorm::index::{
    AccessType, DeleteDocument, FileType, FrequentwordType, IndexDocument, IndexDocuments,
    IndexMetaObject, NgramSet, SimilarityType, StemmerType, StopwordType, TokenizerType,
    create_index, open_index,
};
use seekstorm::search::{QueryRewriting, QueryType, ResultType, Search};
use std::collections::HashSet;
use std::{fs, path::Path};

#[tokio::test]
/// create_index test
async fn test_01_create_index() {
    let index_path = Path::new("tests/index_test/");
    let _ = fs::remove_dir_all(index_path);

    let schema_json = r#"
    [{"field":"title","field_type":"Text","stored":false,"indexed":false},
    {"field":"body","field_type":"Text","stored":true,"indexed":true,"longest":true},
    {"field":"url","field_type":"Text","stored":false,"indexed":false}]"#;
    let schema = serde_json::from_str(schema_json).unwrap();

    let meta = IndexMetaObject {
        id: 0,
        name: "test_index".into(),
        similarity: SimilarityType::Bm25f,
        tokenizer: TokenizerType::UnicodeAlphanumeric,
        stemmer: StemmerType::None,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::English,
        ngram_indexing: NgramSet::NgramFF as u8 | NgramSet::NgramFFF as u8,
        access_type: AccessType::Mmap,
        spelling_correction: None,
    };

    let segment_number_bits1 = 11;
    let index_arc = create_index(
        index_path,
        meta,
        &schema,
        &Vec::new(),
        segment_number_bits1,
        false,
        None,
    )
    .await
    .unwrap();
    let index = index_arc.read().await;

    let result = index.meta.id;
    assert_eq!(result, 0);
    index_arc.close().await;
}

#[tokio::test]
/// index document
async fn test_02_index_document() {
    // open index
    let index_path = Path::new("tests/index_test/");
    let index_arc = open_index(index_path, false).await.unwrap();

    // index document
    let document_json = r#"
    {"title":"title1 test","body":"body1","url":"url1"}"#;
    let document = serde_json::from_str(document_json).unwrap();
    index_arc.index_document(document, FileType::None).await;

    // index documents
    let documents_json = r#"
    [{"title":"title1 test","body":"body1","url":"url1"},
    {"title":"title2","body":"body2 test","url":"url2"},
    {"title":"title3 test","body":"body3 test","url":"url3"}]"#;
    let documents_vec = serde_json::from_str(documents_json).unwrap();
    index_arc.index_documents(documents_vec).await;

    // wait until all index threads are finished and commit
    index_arc.commit().await;

    let result = index_arc.read().await.indexed_doc_count().await;
    assert_eq!(result, 4);

    index_arc.close().await;
}

#[tokio::test]
/// query index
async fn test_03_query_index() {
    // open index
    let index_path = Path::new("tests/index_test/");
    let index_arc = open_index(index_path, false).await.unwrap();

    let result = index_arc.read().await.indexed_doc_count().await;
    assert_eq!(result, 4);

    let query = "+body2 +test".into();
    let result_list = index_arc
        .search(
            query,
            QueryType::Intersection,
            0,
            10,
            ResultType::TopkCount,
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            QueryRewriting::SearchOnly,
        )
        .await;

    let result = result_list.results.len();
    assert_eq!(result, 1);

    let result = result_list.result_count;
    assert_eq!(result, 1);

    let result = result_list.result_count_total;
    assert_eq!(result, 1);

    index_arc.close().await;
}

#[tokio::test]
/// clear index
async fn test_04_clear_index() {
    // open index
    let index_path = Path::new("tests/index_test/");
    let index_arc = open_index(index_path, false).await.unwrap();

    let result = index_arc.read().await.indexed_doc_count().await;
    assert_eq!(result, 4);

    // clear index
    index_arc.write().await.clear_index().await;

    let result = index_arc.read().await.indexed_doc_count().await;
    assert_eq!(result, 0);

    // index document
    let document_json = r#"
    {"title":"title1 test","body":"body1","url":"url1"}"#;
    let document = serde_json::from_str(document_json).unwrap();
    index_arc.index_document(document, FileType::None).await;

    // wait until all index threads are finished and commit
    index_arc.commit().await;

    let result = index_arc.read().await.indexed_doc_count().await;
    assert_eq!(result, 1);
    println!("indexed_doc_count: {}", result);

    // query index
    let query = "body1".into();
    let result_list = index_arc
        .search(
            query,
            QueryType::Union,
            0,
            10,
            ResultType::TopkCount,
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            QueryRewriting::SearchOnly,
        )
        .await;

    for r in result_list.results.iter() {
        println!("result doc_id: {}", r.doc_id);
    }

    let result = result_list.result_count_total;
    assert_eq!(result, 1);

    index_arc.close().await;
}

#[tokio::test]
/// get document
async fn test_05_get_document() {
    // open index
    let index_path = Path::new("tests/index_test/");
    let index_arc = open_index(index_path, false).await.unwrap();

    let result = index_arc.read().await.indexed_doc_count().await;
    assert_eq!(result, 1);

    let highlighter = None;
    let return_fields_filter = HashSet::new();
    let distance_fields = Vec::new();
    let index = index_arc.read().await;

    let doc = index
        .get_document(
            0,
            false,
            &highlighter,
            &return_fields_filter,
            &distance_fields,
        )
        .await
        .unwrap();

    let value = doc.get("body").unwrap().to_owned();
    let result = serde_json::from_value::<String>(value).unwrap();

    assert_eq!(result, "body1");
    index_arc.close().await;
}

#[tokio::test]
/// delete document
async fn test_06_delete_document() {
    // open index
    let index_path = Path::new("tests/index_test/");
    let index_arc = open_index(index_path, false).await.unwrap();

    let result = index_arc.read().await.indexed_doc_count().await;
    assert_eq!(result, 1);

    // query index before delete
    let query = "body1".into();
    let result_list = index_arc
        .search(
            query,
            QueryType::Union,
            0,
            10,
            ResultType::TopkCount,
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            QueryRewriting::SearchOnly,
        )
        .await;

    let result = result_list.result_count_total;
    assert_eq!(result, 1);

    // delete document
    index_arc
        .delete_document(result_list.results[0].doc_id as u64)
        .await;

    // query index after delete
    let query = "body1".into();
    let result_list = index_arc
        .search(
            query,
            QueryType::Union,
            0,
            10,
            ResultType::TopkCount,
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            QueryRewriting::SearchOnly,
        )
        .await;

    let result = result_list.result_count_total;
    assert_eq!(result, 0);

    let result = index_arc.read().await.current_doc_count().await;
    assert_eq!(result, 0);
}
