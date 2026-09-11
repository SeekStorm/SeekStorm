//! Strict regression tests for the `Union` scan fill-exemption fix (B2).
//!
//! `union_scan_32` (seekstorm/src/union.rs) had an inner gate
//! `query_terms_max_score_sum > _elements[0].score` that lacked the
//! "heap not yet full" (`current_heap_size < top_k`) exemption that every
//! sibling path (scan_8, docid_2, docid_3) has. Once the heap filled, every
//! same-score follower was vetoed, so many-term Unions under-filled the heap
//! (observed: 14/100 on uniform low-score docs, 1/100 on disjoint docid-sorted
//! corpora).
//!
//! Oracle strategy (no reimplementation of BM25): run the exact same engine at
//! two truncation levels. With `length == total hits`, the heap is never
//! full, so no candidate is ever vetoed and the result is the precise global
//! ranking. The truncated `top_k` run must behave like an exact top-`k`
//! selection over that global ranking: it returns exactly `k` hits, contains
//! every hit whose score is strictly above the `k`th-best score, and contains
//! no hit strictly below it. Doc identity at the exact-score tie boundary may
//! legitimately differ between two deterministic-but-arbitrary tie selections,
//! so the assertion is deliberately tie-tolerant.
//!
//! On top of that, a strictly-distinct-score corpus locks the exact doc set,
//! because with no ties the top-`k` set is unique.
//!
//! Self-contained (own index directories), deterministic, no sleeps.

use seekstorm::commit::Commit;
use seekstorm::index::{
    AccessType, Close, Clustering, DocumentCompression, FrequentwordType, IndexDocuments,
    IndexMetaObject, LexicalSimilarity, NgramSet, StemmerType, StopwordType, TokenizerType,
    create_index,
};
use seekstorm::search::{QueryRewriting, QueryType, ResultObject, ResultType, Search, SearchMode};
use seekstorm::vector::Inference;
use std::{fs, path::Path};

fn meta() -> IndexMetaObject {
    IndexMetaObject {
        id: 0,
        name: "union_topk_fill_ranking".into(),
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
    offset: usize,
    length: usize,
    result_type: ResultType,
) -> ResultObject {
    index
        .search(
            query.into(),
            None,
            QueryType::Union,
            SearchMode::Lexical,
            false,
            offset,
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

/// Assert `topk` equals a valid exact top-`k` of the full ranking `oracle`.
/// Tie-tolerant: only the score threshold structure is compared.
fn assert_valid_topk(topk: &ResultObject, oracle: &ResultObject) {
    let k = topk.results.len();
    assert!(k > 0, "top_k run returned nothing");
    assert_eq!(
        topk.result_count_total, oracle.result_count_total,
        "hit totals disagree between the truncated and full runs"
    );

    let mut scores: Vec<f32> = oracle.results.iter().map(|r| r.score).collect();
    scores.sort_by(|a, b| b.partial_cmp(a).unwrap());
    let threshold = scores[k - 1];
    let strictly_better = scores.iter().filter(|s| **s > threshold).count();

    let above = topk.results.iter().filter(|r| r.score > threshold).count();
    let below = topk.results.iter().filter(|r| r.score < threshold).count();
    assert_eq!(
        above, strictly_better,
        "top-{} misses {}-size strictly-better group; threshold={threshold}",
        k, strictly_better
    );
    assert_eq!(
        below, 0,
        "top-{} returned {below} hits below the kth-best score {threshold}",
        k
    );

    for w in topk.results.windows(2) {
        assert!(w[0].score >= w[1].score, "results not sorted desc");
    }
}

/// Sorted multiset of (doc_id, exact f32 score bits); safe only when no two
/// hits share the same score, so the top-`k` doc set is unique.
fn strict_key(results: &[(usize, u32)]) -> Vec<(usize, u32)> {
    let mut v = results.to_vec();
    v.sort_unstable();
    v
}

/// `>8`-term Union (hits `union_scan_32`) with a clear score gap: the top-100
/// must contain the entire high-score tier plus exactly 60 of the tied
/// low-score tier, none of them worse than the boundary score.
#[tokio::test]
async fn scan32_topk_matches_global_ranking() {
    let index = fresh("tests/index_scan32_ranking/").await;

    let words: Vec<String> = (1..=12).map(|i| format!("w{i:02}")).collect();

    let mut docs = vec![];
    // High-score tier: every doc carries all 12 terms.
    for i in 0..40 {
        let title = words.join(" ") + &format!(" hi{i}");
        docs.push(doc(&title));
    }
    // Low-score tier: each doc carries exactly one term (uniform, low idf).
    for (w, n) in words.iter().zip(0..12) {
        for i in (n * 80)..(n * 80 + 80) {
            docs.push(doc(&format!("{w} lo{i}")));
        }
    }
    index.index_documents(docs).await;
    index.commit().await;

    let query = words.join(" ");
    let total = search(&index, &query, 0, 1, ResultType::Count).await;
    assert_eq!(total.result_count_total, 1000);

    // Oracle: length == total hits => heap never full => exact global ranking.
    let oracle = search(&index, &query, 0, 1000, ResultType::TopkCount).await;
    assert_eq!(oracle.results.len(), 1000);

    let topk = search(&index, &query, 0, 100, ResultType::TopkCount).await;
    assert_eq!(topk.results.len(), 100);
    assert_valid_topk(&topk, &oracle);

    index.close().await;
}

/// Directly reproduces the original B2 puke case: disjoint terms, uniform
/// low idf, sparse docs — pre-fix this filled only a handful of the heap.
#[tokio::test]
async fn scan32_sparse_uniform_fills_heap() {
    let index = fresh("tests/index_scan32_sparse/").await;

    let mut titles = vec![];
    // 20 disjoint terms, 100 docs each (2000 total under the 65536 block size).
    for w in 1..=20 {
        for i in 0..100 {
            titles.push(format!("spar{w:02} uniq{}{w:02}", i));
        }
    }
    let docs: Vec<seekstorm::index::Document> = titles.iter().map(|t| doc(t)).collect();
    index.index_documents(docs).await;
    index.commit().await;

    let query = (1..=20).map(|i| format!("spar{i:02}")).collect::<Vec<_>>().join(" ");

    let total = search(&index, &query, 0, 1, ResultType::Count).await;
    assert_eq!(total.result_count_total, 2000);

    let oracle = search(&index, &query, 0, 2000, ResultType::Topk).await;
    assert_eq!(oracle.results.len(), 2000);

    let topk = search(&index, &query, 0, 100, ResultType::Topk).await;
    assert_eq!(topk.results.len(), 100, "scan_32 under-filled the heap");
    assert_valid_topk(&topk, &oracle);

    index.close().await;
}

/// Strict distinct-score corpus: every doc length differs, so every score is
/// distinct and the top-`k` doc set is unique — here the exact doc/score
/// multiset must match the global ranking exactly.
#[tokio::test]
async fn scan32_distinct_scores_exact_selection() {
    let index = fresh("tests/index_scan32_distinct/").await;

    let words: Vec<String> = (1..=11).map(|i| format!("d{i:02}")).collect();
    let mut docs = vec![];
    // Each doc: all 11 query words + a unique number of inert pad tokens, so
    // lengths (hence BM25 document-length normalization, hence scores) are all
    // strictly distinct.
    for i in 0..400 {
        let pad = "pad ".repeat(i % 13);
        docs.push(doc(&format!("{} {pad}rare{i} finite{i}", words.join(" "))));
    }
    index.index_documents(docs).await;
    index.commit().await;

    let query = words.join(" ");
    let oracle = search(&index, &query, 0, 400, ResultType::Topk).await;
    assert_eq!(oracle.results.len(), 400);

    let topk = search(&index, &query, 0, 100, ResultType::Topk).await;
    assert_eq!(topk.results.len(), 100);

    let a: Vec<(usize, u32)> = topk.results.iter().map(|r| (r.doc_id, r.score.to_bits())).collect();
    let b: Vec<(usize, u32)> = oracle.results[..100].iter().map(|r| (r.doc_id, r.score.to_bits())).collect();
    assert_eq!(
        strict_key(&a),
        strict_key(&b),
        "distinct-score top-100 must match the global ranking exactly"
    );

    index.close().await;
}

/// TopkCount and Topk must agree on selection and on the returned count.
#[tokio::test]
async fn scan32_topkcount_and_topk_agree() {
    let index = fresh("tests/index_scan32_agree/").await;

    let words: Vec<String> = (1..=11).map(|i| format!("a{i:02}")).collect();
    let mut docs = vec![];
    for i in 0..120 {
        let terms = words
            .iter()
            .enumerate()
            .filter(|(j, _)| (i + j) % 3 != 0)
            .map(|(_, w)| w.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        docs.push(doc(&format!("{terms} doc{i}")));
    }
    for i in 0..40 {
        let title = words.iter().cloned().collect::<Vec<_>>().join(" ") + &format!(" g{i}");
        docs.push(doc(&title));
    }
    index.index_documents(docs).await;
    index.commit().await;

    let query = words.join(" ");
    let oracle = search(&index, &query, 0, 160, ResultType::Topk).await;
    let topk = search(&index, &query, 0, 50, ResultType::Topk).await;
    let topk_count = search(&index, &query, 0, 50, ResultType::TopkCount).await;

    assert_eq!(topk.result_count_total, oracle.result_count_total);
    assert_eq!(topk_count.result_count_total, oracle.result_count_total);
    assert_valid_topk(&topk, &oracle);
    assert_valid_topk(&topk_count, &oracle);

    let a: Vec<(usize, u32)> = topk.results.iter().map(|r| (r.doc_id, r.score.to_bits())).collect();
    let b: Vec<(usize, u32)> = topk_count.results.iter().map(|r| (r.doc_id, r.score.to_bits())).collect();
    assert_eq!(strict_key(&a), strict_key(&b));

    index.close().await;
}

/// Pagination sanity on the fixed path: an offset slice of a scan_32 ranking
/// must match the corresponding slice of the full ranking's scores.
#[tokio::test]
async fn scan32_pagination_is_consistent() {
    let index = fresh("tests/index_scan32_pagination/").await;

    let words: Vec<String> = (1..=13).map(|i| format!("p{i:02}")).collect();
    let mut docs = vec![];
    for i in 0..250 {
        let title = format!("{} doc{i}", words.join(" "));
        docs.push(doc(&title));
    }
    index.index_documents(docs).await;
    index.commit().await;

    let query = words.join(" ");
    let full = search(&index, &query, 0, 250, ResultType::Topk).await;
    let page = search(&index, &query, 30, 40, ResultType::Topk).await;

    assert_eq!(page.results.len(), 40);
    let pa: Vec<u32> = page.results.iter().map(|r| r.score.to_bits()).collect();
    let fb: Vec<u32> = full.results[30..70].iter().map(|r| r.score.to_bits()).collect();
    assert_eq!(pa, fb, "pagination scores must match the full ranking slice");

    index.close().await;
}