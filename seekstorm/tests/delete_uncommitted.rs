//! Regression tests: deletes of not-yet-committed documents must survive
//! the next commit instead of being silently dropped.
//!
//! Self-contained (own index directory), deterministic, no sleeps.

use seekstorm::commit::Commit;
use seekstorm::index::{
    AccessType, Close, Clustering, DeleteDocuments, DocumentCompression, FrequentwordType,
    IndexDocuments, IndexMetaObject, LexicalSimilarity, NgramSet, StemmerType, StopwordType,
    TokenizerType, create_index,
};
use seekstorm::search::{QueryRewriting, QueryType, ResultType, Search, SearchMode};
use seekstorm::vector::Inference;
use std::{fs, path::Path};

fn meta() -> IndexMetaObject {
    IndexMetaObject {
        id: 0,
        name: "delete_uncommitted".into(),
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

async fn count(
    index: &seekstorm::index::IndexArc,
    query: &str,
    uncommitted: bool,
) -> usize {
    index
        .search(
            query.into(),
            None,
            QueryType::Union,
            SearchMode::Lexical,
            false,
            0,
            100,
            ResultType::TopkCount,
            uncommitted,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            QueryRewriting::SearchOnly,
        )
        .await
        .result_count_total
}

async fn fresh(dir: &str) -> seekstorm::index::IndexArc {
    let path = Path::new(dir);
    let _ = fs::remove_dir_all(path);
    create_index(path, meta(), &schema(), &Vec::new(), 11, true, None)
        .await
        .unwrap()
}

/// Delete-before-commit must stick: the document stays gone after commit.
#[tokio::test]
async fn delete_uncommitted_survives_commit() {
    let index = fresh("tests/index_delete_uncommitted/").await;
    index
        .index_documents(vec![doc("hello world"), doc("foo bar")])
        .await;
    index.delete_documents(vec![0]).await;
    // Must not underflow/panic while the async index task is still running.
    let _ = index.read().await.current_doc_count().await;
    index.commit().await;
    assert_eq!(count(&index, "hello", false).await, 0);
    assert_eq!(index.read().await.current_doc_count().await, 1);
    index.close().await;
}

/// Control: delete-after-commit keeps working as before.
#[tokio::test]
async fn delete_committed_control() {
    let index = fresh("tests/index_delete_committed/").await;
    index
        .index_documents(vec![doc("hello world"), doc("foo bar")])
        .await;
    index.commit().await;
    index.delete_documents(vec![0]).await;
    index.commit().await;
    assert_eq!(count(&index, "hello", false).await, 0);
    assert_eq!(index.read().await.current_doc_count().await, 1);
    index.close().await;
}

/// Out-of-range deletes stay silent no-ops: no panic, counts unchanged.
#[tokio::test]
async fn delete_out_of_range_ignored() {
    let index = fresh("tests/index_delete_range/").await;
    index
        .index_documents(vec![doc("hello world"), doc("foo bar")])
        .await;
    index.commit().await;
    index.delete_documents(Vec::new()).await;
    index.delete_documents(vec![u64::MAX]).await;
    index.delete_documents(vec![999_999]).await;
    index.delete_documents(vec![0, 0, 0]).await;
    index.commit().await;
    assert_eq!(count(&index, "hello", false).await, 0);
    assert_eq!(index.read().await.current_doc_count().await, 1);
    index.close().await;
}

/// clear_index reuses the docid namespace: a stale pre-clear docid addresses
/// whatever new document reuses that number (documents id-reuse semantics).
#[tokio::test]
async fn delete_reused_docid_after_clear() {
    let index = fresh("tests/index_delete_clear/").await;
    index
        .index_documents(vec![doc("hello world"), doc("foo bar")])
        .await;
    index.commit().await;
    index.write().await.clear_index().await;
    index
        .index_documents(vec![doc("alphaone"), doc("betaone")])
        .await;
    index.delete_documents(vec![0]).await;
    index.commit().await;
    assert_eq!(count(&index, "alphaone", false).await, 0);
    assert_eq!(count(&index, "betaone", false).await, 1);
    index.close().await;
}
