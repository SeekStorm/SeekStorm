//! Regression tests: reading a document with no stored fields must return
//! `Err`, not panic with a reversed slice (`[262144..0]`).
//!
//! Root cause was `get_document_shard` (seekstorm/src/doc_store.rs) only
//! treating `previous_pointer == pointer` as "not found"; a non-stored-only
//! (or empty) document leaves slot 0 with `previous_pointer > pointer`.
//!
//! Self-contained (own index directory), deterministic, no sleeps.

use seekstorm::commit::Commit;
use seekstorm::index::{
    AccessType, Close, Clustering, DocumentCompression, FrequentwordType, IndexDocuments,
    IndexMetaObject, LexicalSimilarity, NgramSet, StemmerType, StopwordType, TokenizerType,
    create_index,
};
use seekstorm::vector::Inference;
use std::{collections::HashSet, fs, path::Path};

fn meta(access_type: AccessType) -> IndexMetaObject {
    IndexMetaObject {
        id: 0,
        name: "get_nonstored_doc".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::UnicodeAlphanumeric,
        stemmer: StemmerType::None,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::None,
        ngram_indexing: NgramSet::SingleTerm as u8,
        document_compression: DocumentCompression::Snappy,
        access_type,
        spelling_correction: None,
        query_completion: None,
        clustering: Clustering::None,
        inference: Inference::None,
    }
}

fn schema() -> Vec<seekstorm::index::SchemaField> {
    serde_json::from_str(
        r#"[{"field":"nostore","field_type":"Text","store":false,"index_lexical":true},{"field":"title","field_type":"Text","store":true,"index_lexical":true}]"#,
    )
    .unwrap()
}

async fn fresh(dir: &str, access_type: AccessType) -> seekstorm::index::IndexArc {
    let path = Path::new(dir);
    let _ = fs::remove_dir_all(path);
    create_index(
        path,
        meta(access_type),
        &schema(),
        &Vec::new(),
        11,
        true,
        None,
    )
    .await
    .unwrap()
}

async fn index_one(index: &seekstorm::index::IndexArc, json: &str) {
    let doc: seekstorm::index::Document = serde_json::from_str(json).unwrap();
    index.index_documents(vec![doc]).await;
    index.commit().await;
}

async fn get0(index: &seekstorm::index::IndexArc) -> Result<seekstorm::index::Document, String> {
    index
        .read()
        .await
        .get_document(0, false, &None, &HashSet::new(), &[])
        .await
}

/// A document with only non-stored fields must read as Err (mmap branch).
#[tokio::test]
async fn get_nonstored_only_mmap() {
    let index = fresh("tests/index_get_nonstored_mmap/", AccessType::Mmap).await;
    index_one(&index, r#"{"nostore":"only indexed content"}"#).await;
    assert!(get0(&index).await.is_err());
    index.close().await;
}

/// Same for the RAM branch.
#[tokio::test]
async fn get_nonstored_only_ram() {
    let index = fresh("tests/index_get_nonstored_ram/", AccessType::Ram).await;
    index_one(&index, r#"{"nostore":"only indexed content"}"#).await;
    assert!(get0(&index).await.is_err());
    index.close().await;
}

/// Control: a normal stored document still reads back.
#[tokio::test]
async fn get_stored_control() {
    let index = fresh("tests/index_get_control/", AccessType::Mmap).await;
    index_one(&index, r#"{"title":"hello world"}"#).await;
    assert!(get0(&index).await.is_ok());
    index.close().await;
}
