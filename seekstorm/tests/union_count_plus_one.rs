//! Same-block overlapping union must count exactly (pins the `+1` encoding).

use seekstorm::commit::Commit;
use seekstorm::index::{
    AccessType, Close, Clustering, DocumentCompression, FrequentwordType, IndexArc, IndexDocuments,
    IndexMetaObject, LexicalSimilarity, NgramSet, StemmerType, StopwordType, TokenizerType,
    create_index,
};
use seekstorm::search::{QueryRewriting, QueryType, ResultType, Search, SearchMode};
use seekstorm::vector::Inference;
use std::{fs, path::Path};

fn meta() -> IndexMetaObject {
    IndexMetaObject {
        id: 0,
        name: "union_count_plus_one".into(),
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
    index: &IndexArc,
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

#[tokio::test]
async fn union_count_overlap_same_block() {
    let dir = "tests/index_union_plus_one/";
    let path = Path::new(dir);
    let _ = fs::remove_dir_all(path);
    let index = create_index(path, meta(), &schema(), &Vec::new(), 11, true, Some(1))
        .await
        .unwrap();

    // Overlapping docs: doc2, doc4 contain BOTH alpha and beta.
    let docs = vec![
        doc("alpha only item"),
        doc("beta only item"),
        doc("alpha beta both item"),
        doc("alpha only item"),
        doc("alpha beta both item"),
        doc("beta only item"),
    ];
    index.index_documents(docs).await;
    index.commit().await;

    // 6 docs total in the same block; all contain at least one of alpha/beta.
    let count = search(&index, "alpha beta", ResultType::Count).await;
    let topk_count = search(&index, "alpha beta", ResultType::TopkCount).await;

    let expected = 6;
    println!("Count result: {}", count.result_count_total);
    println!("TopkCount result: {}", topk_count.result_count_total);
    assert_eq!(count.result_count_total, expected);
    assert_eq!(topk_count.result_count_total, expected);

    index.close().await;
}
