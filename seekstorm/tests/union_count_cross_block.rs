//! Regression test: multi-term `Union` `Count`/`TopkCount` must be consistent
//! across repeated queries when terms span multiple blocks.
//!
//! `union_count` sorted terms including end-flagged ones, so a wrong-block
//! term could land at index 0 and corrupt the initial count and bitmap.

use seekstorm::commit::Commit;
use seekstorm::index::{
    AccessType, Close, Clustering, DocumentCompression, FrequentwordType, IndexDocuments,
    IndexMetaObject, LexicalSimilarity, NgramSet, StemmerType, StopwordType, TokenizerType,
    create_index,
};
use seekstorm::search::{QueryRewriting, QueryType, ResultType, Search, SearchMode};
use seekstorm::vector::Inference;
use std::{fs, path::Path};

fn meta() -> IndexMetaObject {
    IndexMetaObject {
        id: 0,
        name: "union_count_cross_block".into(),
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
    // Single shard so terms span multiple blocks (with default sharding each
    // shard holds a single block and the bug is unreachable).
    create_index(path, meta(), &schema(), &Vec::new(), 11, true, Some(1))
        .await
        .unwrap()
}

/// 3-term union across multiple blocks must return consistent results.
///
/// Sizes are asymmetric on purpose: the end-flagged term in block 1 (alpha,
/// 60k) outranks the valid terms (beta/gamma, 5k each), so without the fix
/// the sort deterministically puts it first and the count is wrong.
#[tokio::test]
async fn union_count_cross_block_consistent() {
    let index = fresh("tests/index_union_cross_block/").await;
    let mut docs = vec![];

    // block 0: alpha (docid 0..60000)
    for i in 0..60000 {
        docs.push(doc(&format!("alpha item{i}")));
    }
    // padding crossing the 65536 block boundary
    for i in 0..6000 {
        docs.push(doc(&format!("padding filler{i}")));
    }
    // block 1: beta (docid 66000..71000)
    for i in 0..5000 {
        docs.push(doc(&format!("beta item{i}")));
    }
    // block 1: gamma (docid 71000..76000)
    for i in 0..5000 {
        docs.push(doc(&format!("gamma item{i}")));
    }

    index.index_documents(docs).await;
    index.commit().await;

    // 60k alpha + 5k beta + 5k gamma = 70_000; padding must not match.
    let mut prev_count = 0;
    for rep in 0..20 {
        let count = search(&index, "alpha beta gamma", ResultType::Count).await;
        let topk_count = search(&index, "alpha beta gamma", ResultType::TopkCount).await;

        assert_eq!(
            count.result_count_total, 70_000,
            "Count absolute value mismatch at rep {rep}: expected 70_000, got {}",
            count.result_count_total
        );
        if rep == 0 {
            prev_count = count.result_count_total;
        } else {
            assert_eq!(
                count.result_count_total, prev_count,
                "Count jitter at rep {rep}: {prev_count} vs {}",
                count.result_count_total
            );
        }
        assert_eq!(
            count.result_count_total, topk_count.result_count_total,
            "Count and TopkCount mismatch at rep {rep}"
        );
    }

    index.close().await;
}

/// Control: 2-term union (no cross-block issue) must be stable.
#[tokio::test]
async fn union_count_two_terms_stable() {
    let index = fresh("tests/index_union_two_terms/").await;
    let mut docs = vec![];

    for i in 0..10 {
        docs.push(doc(&format!("alpha uniq{i}")));
    }
    for i in 0..10 {
        docs.push(doc(&format!("beta uniq{i}")));
    }

    index.index_documents(docs).await;
    index.commit().await;

    let count = search(&index, "alpha beta", ResultType::Count).await;
    let topk_count = search(&index, "alpha beta", ResultType::TopkCount).await;

    assert_eq!(count.result_count_total, 20);
    assert_eq!(topk_count.result_count_total, 20);

    index.close().await;
}
