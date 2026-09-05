//! Regression tests: Bitmap-encoded AND/OR `Count` must exclude deleted
//! documents, matching `TopkCount`.
//!
//! Root cause was `intersection_docid` (seekstorm/src/intersection.rs)
//! computing `filtered` without `delete_hashset`, so Bitmap blocks took the
//! raw-popcount fast path (and the overlap subtracted by the OR path was raw too).
//! Small posting lists use Array/RLE paths (already correct); Bitmap needs
//! >=4096 scattered postings per shard, hence the single shard + volume here.
//!
//! Self-contained (own index directory), deterministic (xorshift), no sleeps.

use seekstorm::commit::Commit;
use seekstorm::index::{
    AccessType, Close, Clustering, DeleteDocuments, DocumentCompression, FrequentwordType,
    IndexDocuments, IndexMetaObject, LexicalSimilarity, NgramSet, StemmerType, StopwordType,
    TokenizerType, create_index,
};
use seekstorm::search::{QueryRewriting, QueryType, ResultType, Search, SearchMode};
use seekstorm::vector::Inference;
use std::{collections::HashSet, fs, path::Path};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn meta() -> IndexMetaObject {
    IndexMetaObject {
        id: 0,
        name: "bitmap_delete_consistency".into(),
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

async fn search(
    index: &seekstorm::index::IndexArc,
    query: &str,
    query_type: QueryType,
    result_type: ResultType,
) -> seekstorm::search::ResultObject {
    index
        .search(
            query.into(),
            None,
            query_type,
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

async fn setup() -> (seekstorm::index::IndexArc, HashSet<u64>, HashSet<u64>) {
    const N: u64 = 12000;
    let path = Path::new("tests/index_delete_bitmap/");
    let _ = fs::remove_dir_all(path);
    // Single shard: per-shard postings stay >= 4096 scattered docs, which
    // forces Bitmap encoding (RLE aborts past 2048 runs).
    let index = create_index(path, meta(), &schema(), &Vec::new(), 11, true, Some(1))
        .await
        .unwrap();
    let mut rng = Rng(0xabc);
    let mut set_x = HashSet::new();
    while set_x.len() < 4500 {
        set_x.insert(rng.below(N));
    }
    let mut set_y = HashSet::new();
    while set_y.len() < 4500 {
        set_y.insert(rng.below(N));
    }
    let mut rng = Rng(0x1);
    let docs: Vec<seekstorm::index::Document> = (0..N)
        .map(|i| {
            let mut t = format!("doc number {i}");
            if set_x.contains(&i) {
                t.push_str(" termx");
            }
            if set_y.contains(&i) {
                t.push_str(" termy");
            }
            t.push_str(&format!(" w{}", rng.below(100000)));
            serde_json::from_str(&format!(r#"{{"title":"{t}"}}"#)).unwrap()
        })
        .collect();
    index.index_documents(docs).await;
    index.commit().await;

    let vx: Vec<u64> = set_x.iter().copied().collect();
    let overlap: Vec<u64> = set_x.intersection(&set_y).copied().collect();
    let mut rng = Rng(0xdef);
    let mut del = HashSet::new();
    while del.len() < 400 {
        del.insert(vx[rng.below(vx.len() as u64) as usize]);
    }
    for i in 0..50.min(overlap.len()) {
        del.insert(overlap[i]);
    }
    index.delete_documents(del.iter().copied().collect()).await;
    index.commit().await;
    (index, set_x, set_y)
}

/// Bitmap AND total must drop deleted documents.
#[tokio::test]
async fn bitmap_and_count_excludes_deleted() {
    let (index, set_x, set_y) = setup().await;
    // Recompute ground truth with the same seeds as setup.
    let (live_and, _) = ground_truth(&set_x, &set_y);
    let count = search(
        &index,
        "termx termy",
        QueryType::Intersection,
        ResultType::Count,
    )
    .await;
    let topk = search(
        &index,
        "termx termy",
        QueryType::Intersection,
        ResultType::TopkCount,
    )
    .await;
    assert_eq!(count.result_count_total, live_and.len());
    assert_eq!(topk.result_count_total, live_and.len());
    index.close().await;
}

/// Bitmap OR total must drop deleted documents.
#[tokio::test]
async fn bitmap_or_count_excludes_deleted() {
    let (index, set_x, set_y) = setup().await;
    let (_, live_or) = ground_truth(&set_x, &set_y);
    let count = search(&index, "termx termy", QueryType::Union, ResultType::Count).await;
    let topk = search(
        &index,
        "termx termy",
        QueryType::Union,
        ResultType::TopkCount,
    )
    .await;
    assert_eq!(count.result_count_total, live_or.len());
    assert_eq!(topk.result_count_total, live_or.len());
    index.close().await;
}

fn ground_truth(set_x: &HashSet<u64>, set_y: &HashSet<u64>) -> (HashSet<u64>, HashSet<u64>) {
    let vx: Vec<u64> = set_x.iter().copied().collect();
    let overlap: Vec<u64> = set_x.intersection(set_y).copied().collect();
    let mut rng = Rng(0xdef);
    let mut del = HashSet::new();
    while del.len() < 400 {
        del.insert(vx[rng.below(vx.len() as u64) as usize]);
    }
    for i in 0..50.min(overlap.len()) {
        del.insert(overlap[i]);
    }
    let live_x: HashSet<u64> = set_x.difference(&del).copied().collect();
    let live_y: HashSet<u64> = set_y.difference(&del).copied().collect();
    let live_and: HashSet<u64> = live_x.intersection(&live_y).copied().collect();
    let live_or: HashSet<u64> = live_x.union(&live_y).copied().collect();
    (live_and, live_or)
}
