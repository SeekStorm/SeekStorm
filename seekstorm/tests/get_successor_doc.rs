//! Repro: get_document on a valid doc following doc(s) with no stored fields.
//! Stale-zero predecessor slots made it slice from 0 and panic.
//! Matrix: Ram/Mmap x None/Snappy. Public API only.
use seekstorm::commit::Commit;
use seekstorm::index::{
    AccessType, Close, Clustering, Document, DocumentCompression, FileType, FrequentwordType,
    IndexDocument, IndexMetaObject, LexicalSimilarity, NgramSet, StemmerType, StopwordType,
    TokenizerType, create_index,
};
use seekstorm::vector::Inference;
use std::{collections::HashSet, fs, path::Path};

use futures::FutureExt;

async fn run_case(access: AccessType, compression: DocumentCompression, tag: &str) {
    let dir = format!("/tmp/repro_succ_{tag}/");
    let index_path = Path::new(&dir);
    let _ = fs::remove_dir_all(index_path);
    let schema_json = r#"
    [{"field":"title","field_type":"Text","store":true,"index_lexical":true,"longest":true},
    {"field":"tag","field_type":"Text","store":false,"index_lexical":true}]"#;
    let schema = serde_json::from_str(schema_json).unwrap();
    let meta = IndexMetaObject {
        id: 0,
        name: "succ".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::UnicodeAlphanumeric,
        stemmer: StemmerType::None,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::None,
        ngram_indexing: NgramSet::SingleTerm as u8,
        document_compression: compression,
        access_type: access,
        spelling_correction: None,
        query_completion: None,
        clustering: Clustering::None,
        inference: Inference::None,
    };
    let index_arc = create_index(index_path, meta, &schema, &Vec::new(), 11, false, Some(1))
        .await
        .unwrap();
    // doc 0: indexed-only field -> no stored content -> slot stays 0.
    // doc 1: another empty one (run of empties).
    // doc 2: stored content right after the empty run.
    // doc 3: stored baseline (real predecessor).
    // doc 4: empty again; doc 5: stored content after a MIXED run
    //   (real, empty, real) -> exercises the backward scan beyond what a
    //   simple clamp-to-table would fix.
    for json in [
        r#"{"tag":"hello"}"#,
        r#"{"tag":"world"}"#,
        r#"{"title":"hello world"}"#,
        r#"{"title":"baseline doc"}"#,
        r#"{"tag":"again"}"#,
        r#"{"title":"mixed successor"}"#,
    ] {
        let doc: Document = serde_json::from_str(json).unwrap();
        index_arc.index_document(doc, FileType::None).await;
    }
    index_arc.commit().await;

    let index = index_arc.read().await;
    // Empty docs themselves stay not-found (documented behavior, cf. #72).
    assert!(
        index
            .get_document(0, false, &None, &HashSet::new(), &Vec::new())
            .await
            .is_err()
    );
    assert!(
        index
            .get_document(1, false, &None, &HashSet::new(), &Vec::new())
            .await
            .is_err()
    );
    // The valid successors must come back with EXACT content, not panic.
    for (id, want) in [
        (2, "hello world"),
        (3, "baseline doc"),
        (5, "mixed successor"),
    ] {
        let doc = index
            .get_document(id, false, &None, &HashSet::new(), &Vec::new())
            .await
            .unwrap_or_else(|e| panic!("BUG [{tag}]: valid doc {id} unreadable: {e}"));
        let title: String = serde_json::from_value(doc.get("title").unwrap().clone()).unwrap();
        assert_eq!(title, want, "BUG [{tag}]: wrong content for doc {id}");
    }
    drop(index);
    index_arc.close().await;
    let _ = fs::remove_dir_all(index_path);
    println!("CASE {tag}: ok");
}

#[tokio::test]
async fn successor_after_empty_matrix() {
    let mut fails = 0;
    for (access, compression, tag) in [
        (AccessType::Ram, DocumentCompression::None, "ram-none"),
        (AccessType::Ram, DocumentCompression::Snappy, "ram-snappy"),
        (AccessType::Mmap, DocumentCompression::None, "mmap-none"),
        (AccessType::Mmap, DocumentCompression::Snappy, "mmap-snappy"),
    ] {
        // per-case panic isolation: one panicking combo must not hide the rest
        let r = std::panic::AssertUnwindSafe(run_case(access, compression, tag))
            .catch_unwind()
            .await;
        match r {
            Ok(()) => {}
            Err(_) => {
                fails += 1;
                println!("CASE {tag}: PANIC");
            }
        }
    }
    assert_eq!(fails, 0, "BUG: {fails} combos panic/unreadable");
}
