//! `union_scan_32` bitmask holds at most 32 terms; >32-term TopkCount must
//! not panic (`32 - len` underflow) and must report the exact total.

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
        name: "union_topk_many_terms".into(),
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

/// 40-term union TopkCount: no panic, exact total, top-k hits returned.
#[tokio::test]
async fn union_topk_forty_terms() {
    let dir = "tests/index_union_topk_many_terms/";
    let path = Path::new(dir);
    let _ = fs::remove_dir_all(path);
    let index = create_index(path, meta(), &schema(), &Vec::new(), 11, true, Some(1))
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
    assert_eq!(count.result_count_total, 1000);

    let topk_count = search(&index, &query, ResultType::TopkCount).await;
    assert_eq!(topk_count.result_count_total, 1000);
    assert_eq!(topk_count.result_count_total, count.result_count_total);

    index.close().await;
}
