//! Regression tests: realtime (`include_uncommitted = true`) totals must
//! exclude deleted documents, for both `Count` and `TopkCount`.
//!
//! Root causes were in seekstorm/src/realtime_search.rs: the single-term
//! tail redefined `filtered` without deletes/facets and fell back to the raw
//! `posting_count`; `union_count_uncommitted` never consulted `delete_hashset`;
//! and the `union_scan_uncommitted` single-hit branch double-counted filtered
//! docs (callee and caller each added one).
//! Depends on the single-term Count fix (#71) for committed reads.
//!
//! Self-contained (own index directory). The only waits are bounded polls
//! for the async indexer to settle (fails loudly on timeout, no sleeps
//! past settled state).

use seekstorm::commit::Commit;
use seekstorm::index::{
    AccessType, Close, Clustering, DeleteDocuments, DocumentCompression, FrequentwordType,
    IndexDocuments, IndexMetaObject, LexicalSimilarity, NgramSet, StemmerType, StopwordType,
    TokenizerType, create_index,
};
use seekstorm::search::{QueryRewriting, QueryType, ResultType, Search, SearchMode};
use seekstorm::vector::Inference;
use std::{fs, path::Path, time::Duration};

fn meta() -> IndexMetaObject {
    IndexMetaObject {
        id: 0,
        name: "realtime_delete_consistency".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::UnicodeAlphanumeric,
        stemmer: StemmerType::None,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::None,
        ngram_indexing: NgramSet::SingleTerm as u8,
        document_compression: DocumentCompression::Snappy,
        access_type: AccessType::Mmap,
        spelling_correction: None,
        query_completion: None,
        clustering: Clustering::None,
        inference: Inference::None,
    }
}

fn schema() -> Vec<seekstorm::index::SchemaField> {
    serde_json::from_str(
        r#"[{"field":"title","field_type":"Text","store":true,"index_lexical":true}]"#,
    )
    .unwrap()
}

fn doc(title: &str) -> seekstorm::index::Document {
    serde_json::from_str(&format!(r#"{{"title":"{title}"}}"#)).unwrap()
}

async fn search(
    index: &seekstorm::index::IndexArc,
    query: &str,
    query_type: QueryType,
    result_type: ResultType,
    uncommitted: bool,
) -> seekstorm::search::ResultObject {
    index
        .search(
            query.into(),
            None,
            query_type,
            SearchMode::Lexical,
            false,
            0,
            100,
            result_type,
            uncommitted,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            QueryRewriting::SearchOnly,
        )
        .await
}

async fn fresh(dir: &str) -> seekstorm::index::IndexArc {
    let path = Path::new(dir);
    let _ = fs::remove_dir_all(path);
    create_index(path, meta(), &schema(), &Vec::new(), 11, true, None)
        .await
        .unwrap()
}

/// Wait until the async indexer has settled `expected` docs (bounded).
/// NOTE: `indexed_doc_count` is a high-water mark written when the index
/// task *starts* (index.rs), so it cannot be used here. Settle on the
/// observable search state instead: all docs must be visible first.
async fn settle_search(
    index: &seekstorm::index::IndexArc,
    query: &str,
    query_type: QueryType,
    expected: usize,
) {
    for _ in 0..200 {
        let r = search(
            index,
            query,
            query_type.clone(),
            ResultType::TopkCount,
            true,
        )
        .await;
        if r.result_count_total == expected && r.results.len() == expected.min(100) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("realtime search did not settle to {expected} docs in time");
}

/// Single-term realtime totals must drop a deleted uncommitted document.
#[tokio::test]
async fn realtime_single_count_excludes_deleted() {
    let index = fresh("tests/index_delete_realtime_single/").await;
    let docs: Vec<_> = (0..5).map(|i| doc(&format!("base doc{i}"))).collect();
    index.index_documents(docs).await;
    index.commit().await;
    let docs: Vec<_> = (0..5).map(|i| doc(&format!("base extra{i}"))).collect();
    index.index_documents(docs).await;
    settle_search(&index, "base", QueryType::Union, 10).await;
    index.delete_documents(vec![9, 0]).await;

    let topk = search(
        &index,
        "base",
        QueryType::Union,
        ResultType::TopkCount,
        true,
    )
    .await;
    let count = search(&index, "base", QueryType::Union, ResultType::Count, true).await;
    assert_eq!(topk.result_count_total, 8);
    assert_eq!(count.result_count_total, 8);
    index.close().await;
}

/// OR realtime totals must drop deleted uncommitted documents (no double count).
#[tokio::test]
async fn realtime_union_count_excludes_deleted() {
    let index = fresh("tests/index_delete_realtime_union/").await;
    let mut docs = vec![];
    for i in 0..10 {
        docs.push(doc(&format!("alpha uniq{i}")));
    }
    for i in 0..10 {
        docs.push(doc(&format!("beta uniqb{i}")));
    }
    index.index_documents(docs).await;
    settle_search(&index, "alpha beta", QueryType::Union, 20).await;
    index.delete_documents(vec![0, 1, 10]).await;

    let topk = search(
        &index,
        "alpha beta",
        QueryType::Union,
        ResultType::TopkCount,
        true,
    )
    .await;
    let count = search(
        &index,
        "alpha beta",
        QueryType::Union,
        ResultType::Count,
        true,
    )
    .await;
    assert_eq!(topk.result_count_total, 17);
    assert_eq!(topk.results.len(), 17);
    assert_eq!(count.result_count_total, 17);
    index.close().await;
}
