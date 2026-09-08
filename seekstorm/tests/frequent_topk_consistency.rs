//! Repro: multi-term Topk/TopkCount over all-frequent terms must return results,
//! not an empty heap. Pure public API.
//! Run: cargo test -p seekstorm --test frequent_topk_consistency
use seekstorm::commit::Commit;
use seekstorm::index::{
    AccessType, Close, Clustering, Document, DocumentCompression, FileType, FrequentwordType,
    IndexDocument, IndexMetaObject, LexicalSimilarity, NgramSet, StemmerType, StopwordType,
    TokenizerType, create_index,
};
use seekstorm::search::{QueryRewriting, QueryType, ResultType, Search, SearchMode};
use seekstorm::vector::Inference;
use std::{fs, path::Path};

async fn search(
    index_arc: &seekstorm::index::IndexArc,
    q: &str,
    qt: QueryType,
    rt: ResultType,
    len: usize,
) -> seekstorm::search::ResultObject {
    index_arc
        .search(
            q.into(),
            None,
            qt,
            SearchMode::Lexical,
            false,
            0,
            len,
            rt,
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            QueryRewriting::SearchOnly,
        )
        .await
}

#[tokio::test]
async fn frequent_terms_topk_returns_results() {
    let index_path = Path::new("/tmp/repro_frequent_topk/");
    let _ = fs::remove_dir_all(index_path);
    let schema_json = r#"
    [{"field":"title","field_type":"Text","store":false,"index_lexical":true,"longest":true}]"#;
    let schema = serde_json::from_str(schema_json).unwrap();
    let meta = IndexMetaObject {
        id: 0,
        name: "repro".into(),
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
    };
    // NOTE: keep on the single-shard fast path; multi-shard covered by suite.
    let index_arc = create_index(index_path, meta, &schema, &Vec::new(), 11, false, Some(1))
        .await
        .unwrap();

    // 3000 docs: "common" in all (100%), "half" in evens (50%) -> every query
    // term covers >=50% (all_terms_frequent); top_k=10 < 3000/256.
    for i in 0..3000 {
        let mut t = String::from("common ");
        if i % 2 == 0 {
            t.push_str("half ");
        }
        let doc: Document = serde_json::from_str(&format!(r#"{{"title":"{t}"}}"#)).unwrap();
        index_arc.index_document(doc, FileType::None).await;
    }
    index_arc.commit().await;

    // Intersection Topk must not come back empty with 1500 matches.
    let rlo = search(
        &index_arc,
        "common half",
        QueryType::Intersection,
        ResultType::Topk,
        10,
    )
    .await;
    assert_eq!(
        rlo.results.len(),
        10,
        "BUG: intersection Topk empty for all-frequent terms"
    );

    // TopkCount: same results, consistent totals.
    let rtc = search(
        &index_arc,
        "common half",
        QueryType::Intersection,
        ResultType::TopkCount,
        10,
    )
    .await;
    assert_eq!(
        rtc.results.len(),
        10,
        "BUG: intersection TopkCount results empty"
    );
    assert_eq!(
        rtc.result_count_total, 1500,
        "intersection total must count all matches"
    );

    // Counts were (and stay) correct — the bug only dropped ranking.
    let c = search(
        &index_arc,
        "common half",
        QueryType::Intersection,
        ResultType::Count,
        10,
    )
    .await;
    assert_eq!(c.result_count_total, 1500);

    index_arc.close().await;
    let _ = fs::remove_dir_all(index_path);
}
