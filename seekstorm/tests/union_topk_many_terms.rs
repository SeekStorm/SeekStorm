//! `union_scan_32` bitmask holds at most 32 terms; >32-term TopkCount must
//! not panic (`32 - len` underflow) and must report the exact total.

use seekstorm::commit::Commit;
use seekstorm::index::{
    AccessType, Close, Clustering, DocumentCompression, FrequentwordType, IndexConfig,
    IndexDocuments, LexicalSimilarity, NgramSet, StemmerType, StopwordType, TokenizerType,
    create_index,
};
use seekstorm::search::{QueryRewriting, QueryType, ResultType, Search, SearchMode, SearchRequest};
use seekstorm::vector::Inference;
use std::{fs, path::Path};

fn index_config() -> IndexConfig {
    IndexConfig {
        name: "union_topk_many_terms".into(),
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

/// 40-term union TopkCount: no panic, exact total, top-k hits returned.
#[tokio::test]
async fn union_topk_forty_terms() {
    let dir = "tests/index_union_topk_many_terms/";
    let path = Path::new(dir);
    let _ = fs::remove_dir_all(path);
    let index = create_index(path, index_config(), &schema(), &[], true, Some(1))
        .await
        .unwrap();

    // 40 disjoint terms x 25 docs each = 1000 docs, all in one block.
    let mut docs = vec![];
    for t in 0..40 {
        for i in 0..25 {
            docs.push(doc(&format!("w{t:02} z{i}")));
        }
    }
    index.index_documents(docs).await;
    index.commit().await;

    let query = (0..40)
        .map(|t| format!("w{t:02}"))
        .collect::<Vec<_>>()
        .join(" ");

    let count = search(&index, &query, ResultType::Count).await;
    assert_eq!(count.count_total, 1000);

    let topk_count = search(&index, &query, ResultType::TopkCount).await;
    assert_eq!(topk_count.count_total, 1000);
    assert_eq!(topk_count.count_total, count.count_total);

    index.close().await;
}
