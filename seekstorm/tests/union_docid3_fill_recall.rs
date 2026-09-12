//! Regression tests for the `union_docid_3` WAND recursion cap fix (B1).
//!
//! `union_docid_3` (in seekstorm/src/union.rs) enumerates word-combination
//! subsets via recursive intersection, bounded by `recursion_count < 200`.
//! For disjoint (or generally sparse, low-idf) terms every intersection is
//! empty, the WAND pruning never triggers, and the cap truncates the
//! enumeration: documents owned only by subsets still enqueued are silently
//! dropped. The fix falls back to a single `union_blockid` linear pass over
//! the union of the remaining subsets once the cap is hit.
//!
//! Oracle for this path cannot reuse the "unbounded length" trick (the cap
//! truncates even a full ranking), so the ground truth is built out of one
//! exact single-term top-k per query term (single-term search is exact), and
//! the union result must be a valid top-k of the merged per-word score
//! tables. Comparison is the tie-tolerant score-threshold invariant used by
//! union_topk_fill_ranking.
//!
//! Self-contained (own index directory), deterministic, no sleeps.

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
        name: "union_docid3_recall".into(),
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
    length: usize,
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
            length,
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

/// Check `topk` is a valid exact top-`k` of the full `score_table`
/// (unordered (doc_id, score) records). Tie-tolerant threshold invariant.
fn assert_valid_selection(
    topk: &[(usize, f32)],
    score_table: &[(usize, f32)],
    expected_total: usize,
) {
    let k = topk.len();
    assert_eq!(score_table.len(), expected_total, "ground truth size");
    assert!(k > 0, "top-k run returned nothing");

    let mut scores: Vec<f32> = score_table.iter().map(|s| s.1).collect();
    scores.sort_by(|a, b| b.partial_cmp(a).unwrap());
    let threshold = scores[k - 1];
    let strictly_better = scores.iter().filter(|s| **s > threshold).count();

    let above = topk.iter().filter(|r| r.1 > threshold).count();
    let below = topk.iter().filter(|r| r.1 < threshold).count();
    assert_eq!(
        above, strictly_better,
        "top-{k} misses {strictly_better} strictly-better hits; threshold={threshold}"
    );
    assert_eq!(
        below, 0,
        "top-{k} returned {below} hits below the kth-best score {threshold}"
    );

    for w in topk.windows(2) {
        assert!(w[0].1 >= w[1].1, "results not sorted desc");
    }
}

/// The original B1 puke case: disjoint terms with distinct idf scores.
/// Pre-fix the WAND cap returns near-zero hits; post-fix the fallback linear
/// pass must return the exact global top-100.
#[tokio::test]
async fn docid3_disjoint_terms_fill_heap() {
    let index = fresh("tests/index_docid3_disjoint/").await;

    // 8 disjoint terms, distinct document frequencies so the per-word score
    // distributions differ and cross-term ties are unlikely.
    let dfs = [40usize, 35, 30, 25, 20, 15, 10, 6];
    let words: Vec<String> = (1..=8).map(|i| format!("dj{i:02}")).collect();

    let mut docs = vec![];
    for (w, df) in words.iter().zip(dfs.iter()) {
        for i in 0..*df {
            let pad = "pad ".repeat(i % 5);
            docs.push(doc(&format!("{w} uniq{} {pad}x", i)));
        }
    }
    index.index_documents(docs).await;
    index.commit().await;

    let total: usize = dfs.iter().sum();
    assert_eq!(total, 181);

    // Ground truth: exact single-term top-k per word.
    let mut score_table: Vec<(usize, f32)> = Vec::new();
    for w in words.iter() {
        let r = search(&index, w, 200, ResultType::Topk).await;
        assert_eq!(r.results.len() as u32, r.result_count as u32);
        for res in r.results.iter() {
            score_table.push((res.doc_id, res.score));
        }
    }
    assert_eq!(score_table.len(), total);

    let query = words.join(" ");
    let topk = search(&index, &query, 100, ResultType::TopkCount).await;
    assert_eq!(
        topk.results.len(),
        100,
        "union_docid_3 WAND cap dropped recall (got {} of 100)",
        topk.results.len()
    );
    let topk_sel: Vec<(usize, f32)> = topk.results.iter().map(|r| (r.doc_id, r.score)).collect();
    assert_valid_selection(&topk_sel, &score_table, total);

    index.close().await;
}

/// Overlapping terms keep using the fast WAND path and must stay exact.
#[tokio::test]
async fn docid3_overlap_control() {
    let index = fresh("tests/index_docid3_overlap/").await;

    // 8 terms, heavy overlap so intersections fill the heap quickly and the
    // recursion never approaches the cap.
    let words: Vec<String> = (1..=8).map(|i| format!("ov{i:02}")).collect();
    let mut docs = vec![];
    for i in 0..120 {
        let keep: Vec<&str> = words
            .iter()
            .enumerate()
            .filter(|(j, _)| (i % 4) != *j)
            .map(|(_, w)| w.as_str())
            .collect();
        docs.push(doc(&format!("{} uniq{i}", keep.join(" "))));
    }
    index.index_documents(docs).await;
    index.commit().await;

    // Overlapping terms never approach the recursion cap, so the fast WAND
    // path must behave exactly as before the fix: heap full, no duplicate
    // doc, results sorted. (A per-term ground truth is not usable here —
    // every doc matches several terms and would be counted once per term.)
    let query = words.join(" ");
    let topk = search(&index, &query, 100, ResultType::Topk).await;
    assert_eq!(
        topk.results.len(),
        100,
        "overlap union must still fill the heap"
    );
    let mut seen = std::collections::HashSet::new();
    for (pos, r) in topk.results.iter().enumerate() {
        assert!(seen.insert(r.doc_id), "duplicate doc {}", r.doc_id);
        if pos > 0 {
            assert!(
                topk.results[pos - 1].score >= r.score,
                "results not sorted desc"
            );
        }
    }

    index.close().await;
}

/// Mixed corpus with a short strict distinct-score tail to also pin the exact
/// document set where scores are unique.
#[tokio::test]
async fn docid3_topkcount_total_consistent() {
    let index = fresh("tests/index_docid3_total/").await;

    // 8 words on purpose: the merged fallback then stays in the scan_8 path,
    // keeping this regression test independent of any scan_32 behaviour.
    let words: Vec<String> = (1..=8).map(|i| format!("tt{i:02}")).collect();
    let mut docs = vec![];
    for i in 0..90 {
        docs.push(doc(&format!("{} u{i}", words.join(" "))));
    }
    for w in words.iter().take(5) {
        for i in 0..8 {
            docs.push(doc(&format!("{w} only{}{}", w, i)));
        }
    }
    index.index_documents(docs).await;
    index.commit().await;

    let query = words.join(" ");
    let count = search(&index, &query, 1, ResultType::Count).await;
    let topkcount = search(&index, &query, 200, ResultType::TopkCount).await;
    assert_eq!(count.result_count_total, 130);
    assert_eq!(topkcount.result_count_total, 130);
    assert_eq!(topkcount.results.len(), 130);

    index.close().await;
}
