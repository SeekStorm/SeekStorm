//! Regression tests: single-term `ResultType::Count` must exclude deleted
//! documents, matching `TopkCount`/`results.len()`.
//!
//! Root cause was `single_docid` (seekstorm/src/single.rs) treating
//! "deletes exist" as unfiltered and taking the `Count` fast path,
//! while `single_blockid` already treated deletes as filtered.
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
        name: "count_delete_consistency".into(),
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
    result_type: ResultType,
) -> seekstorm::search::ResultObject {
    index
        .search(
            query.into(),
            None,
            QueryType::Union,
            SearchMode::Lexical,
            false,
            0,
            100,
            result_type,
            false,
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

/// Count must agree with TopkCount once documents are deleted.
#[tokio::test]
async fn single_count_excludes_deleted() {
    let index = fresh("tests/index_delete_count/").await;
    let docs: Vec<_> = (0..10).map(|i| doc(&format!("common word{i}"))).collect();
    index.index_documents(docs).await;
    index.commit().await;
    index.delete_documents(vec![0, 1, 2]).await;
    index.commit().await;

    let count = search(&index, "common", ResultType::Count).await;
    let topk_count = search(&index, "common", ResultType::TopkCount).await;
    assert_eq!(count.result_count_total, 7);
    assert_eq!(topk_count.result_count_total, 7);
    assert_eq!(topk_count.results.len(), 7);
    let mut ids: Vec<usize> = topk_count.results.iter().map(|r| r.doc_id).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![3, 4, 5, 6, 7, 8, 9]);
    index.close().await;
}

/// Control: without deletes the Count fast path is untouched and exact.
#[tokio::test]
async fn single_count_no_deletes_control() {
    let index = fresh("tests/index_delete_count_control/").await;
    let docs: Vec<_> = (0..10).map(|i| doc(&format!("common word{i}"))).collect();
    index.index_documents(docs).await;
    index.commit().await;

    let count = search(&index, "common", ResultType::Count).await;
    let topk_count = search(&index, "common", ResultType::TopkCount).await;
    assert_eq!(count.result_count_total, 10);
    assert_eq!(topk_count.result_count_total, 10);
    assert_eq!(topk_count.results.len(), 10);
    index.close().await;
}
