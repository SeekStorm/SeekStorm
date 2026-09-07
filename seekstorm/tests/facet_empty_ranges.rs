//! Repro: numeric range QueryFacet with empty `ranges` must not panic the search.
//! Public API only: create_index / index_document(s) / commit / search.
//! Run: cargo test -p seekstorm --test facet_empty_ranges

use seekstorm::commit::Commit;
use seekstorm::index::{
    AccessType, Close, Clustering, Document, DocumentCompression, FileType, FrequentwordType,
    IndexDocument, IndexMetaObject, LexicalSimilarity, NgramSet, StemmerType, StopwordType,
    TokenizerType, create_index,
};
use seekstorm::search::{
    QueryFacet, QueryRewriting, QueryType, RangeType, ResultType, Search, SearchMode,
};
use seekstorm::vector::Inference;
use std::{fs, path::Path};

#[tokio::test]
async fn repro_range_facet_empty_ranges_no_panic() {
    let index_path = Path::new("/tmp/repro_facet_empty_ranges/");
    let _ = fs::remove_dir_all(index_path);

    let schema_json = r#"
    [{"field":"title","field_type":"Text","store":true,"index_lexical":true,"longest":true},
    {"field":"age","field_type":"U8","store":true,"index_lexical":false,"facet":true}]"#;
    let schema = serde_json::from_str(schema_json).unwrap();
    let meta = IndexMetaObject {
        id: 0,
        name: "repro_facet".into(),
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
    let index_arc = create_index(index_path, meta, &schema, &Vec::new(), 11, false, None)
        .await
        .unwrap();

    let documents_json = r#"
    [{"title":"hello world","age":25},
    {"title":"hello there","age":30}]"#;
    let documents_vec: Vec<Document> = serde_json::from_str(documents_json).unwrap();
    for document in documents_vec {
        index_arc.index_document(document, FileType::None).await;
    }
    // commit() drains in-flight index tasks via the shard semaphore.
    index_arc.commit().await;

    // Sanity without facets: both docs match.
    let rlo = index_arc
        .search(
            "hello".into(),
            None,
            QueryType::Union,
            SearchMode::Lexical,
            false,
            0,
            10,
            ResultType::Count,
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            QueryRewriting::SearchOnly,
        )
        .await;
    assert_eq!(rlo.result_count_total, 2, "setup: Count without facets");

    // Same query + numeric range facet with EMPTY ranges: must not panic.
    // Pre-fix: `idx as u16 - 1` on Err(0) underflows in facet_count/union_count.
    let query_facets = vec![QueryFacet::U8 {
        field: "age".into(),
        range_type: RangeType::CountWithinRange,
        ranges: Vec::new(),
    }];
    let rlo = index_arc
        .search(
            "hello".into(),
            None,
            QueryType::Union,
            SearchMode::Lexical,
            false,
            0,
            10,
            ResultType::Count,
            false,
            Vec::new(),
            query_facets,
            Vec::new(),
            Vec::new(),
            QueryRewriting::SearchOnly,
        )
        .await;
    assert_eq!(
        rlo.result_count_total, 2,
        "Count with empty-ranges facet must still count matches"
    );

    index_arc.close().await;
    let _ = fs::remove_dir_all(index_path);
}
