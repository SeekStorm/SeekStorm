//! Regression tests: multi-term `Union` (OR) `result_count_total` must exclude
//! deleted documents, matching `results.len()`.
//!
//! Root cause was `union_docid_2` (seekstorm/src/union.rs) computing the
//! total from raw `posting_count`s whenever no NOT/field filter was present,
//! ignoring `delete_hashset` (commit does not rewrite posting lists).
//! Depends on the single-term Count fix (#71) for the per-term slow path.
//!
//! Self-contained (own index directory), deterministic, no sleeps.

use seekstorm::commit::Commit;
use seekstorm::index::{
    AccessType, Close, Clustering, DeleteDocuments, DocumentCompression, FrequentwordType,
    IndexConfig, IndexDocuments, LexicalSimilarity, NgramSet, StemmerType, StopwordType,
    TokenizerType, create_index,
};
use seekstorm::search::{QueryRewriting, QueryType, ResultType, Search, SearchMode, SearchRequest};
use seekstorm::vector::Inference;
use std::{fs, path::Path};

fn index_config() -> IndexConfig {
    IndexConfig {
        name: "union_delete_consistency".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::UnicodeAlphanumeric,
        frequent_words: FrequentwordType::None,
        ngram_indexing: NgramSet::SingleTerm as u8,
        ..Default::default()
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
) -> seekstorm::search::SearchResult {
    index
        .search(SearchRequest {
            query_string: query.into(),
            length: 100,
            result_type: result_type,
            ..Default::default()
        })
        .await
}

async fn fresh(dir: &str) -> seekstorm::index::IndexArc {
    let path = Path::new(dir);
    let _ = fs::remove_dir_all(path);
    create_index(path, index_config(), &schema(), &[], true, None)
        .await
        .unwrap()
}

/// OR total must drop deleted documents (disjoint terms).
#[tokio::test]
async fn union_count_excludes_deleted() {
    let index = fresh("tests/index_delete_union/").await;
    let mut docs = vec![];
    for i in 0..10 {
        docs.push(doc(&format!("alpha uniq{i}")));
    }
    for i in 0..10 {
        docs.push(doc(&format!("beta uniqb{i}")));
    }
    index.index_documents(docs).await;
    index.commit().await;
    index.delete_documents(vec![0, 1, 10]).await;
    index.commit().await;

    let count = search(&index, "alpha beta", ResultType::Count).await;
    let topk_count = search(&index, "alpha beta", ResultType::TopkCount).await;
    assert_eq!(count.count_total, 17);
    assert_eq!(topk_count.count_total, 17);
    assert_eq!(topk_count.results.len(), 17);
    index.close().await;
}

/// Control: without deletes the OR fast path is untouched and exact.
#[tokio::test]
async fn union_count_no_deletes_control() {
    let index = fresh("tests/index_delete_union_control/").await;
    let mut docs = vec![];
    for i in 0..10 {
        docs.push(doc(&format!("alpha uniq{i}")));
    }
    for i in 0..10 {
        docs.push(doc(&format!("beta uniqb{i}")));
    }
    index.index_documents(docs).await;
    index.commit().await;

    let count = search(&index, "alpha beta", ResultType::Count).await;
    let topk_count = search(&index, "alpha beta", ResultType::TopkCount).await;
    assert_eq!(count.count_total, 20);
    assert_eq!(topk_count.count_total, 20);
    index.close().await;
}
