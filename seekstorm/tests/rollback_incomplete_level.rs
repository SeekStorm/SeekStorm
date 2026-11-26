//! Regression test for the RAM-mode rollback of an incomplete index level.
//!
//! `ROARING_BLOCK_SIZE` is 65,536 and is not configurable, so the test pays for
//! a real level boundary: 65,600 documents leave a 64-document incomplete
//! second level, and every later write commit merges that level back into level
//! 0 and rolls it back.
//!
//! Before the fix, the per-term part of that rollback in `commit_lexical_shard`
//! called `item.1.blocks.remove(idx)`, although `blocks` holds one entry per
//! level *where the term occurs* and not one entry per level id. For a term
//! that first occurs in the incomplete level (`len == 1`) the call panicked
//! with "removal index (is 1) should be < len (is 1)". The panic happened in
//! the spawned commit task, so `commit` only reported the `JoinError`: the
//! level index was left untouched while `byte_array_blocks` had already been
//! truncated, and every later commit died with "index out of bounds: the len is
//! 1 but the index is 1". The index stayed at the last full level while writes
//! kept reporting success.
//!
//! The panics happen in spawned tasks, so the test installs a panic hook that
//! records instead of printing and asserts that nothing was recorded.
//!
//! The same test covers the second defect it surfaced: `open_index` restores the
//! committed document count but not `Shard::block_id`, which `Shard::new` leaves
//! at 0. The first write after a reopen then commits the restored, incomplete
//! level as block 0 and the merged postings are mislabeled, which is observable
//! as a panic in `update_list_max_impact_score`/`get_max_score`.

use seekstorm::prelude::*;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// `ROARING_BLOCK_SIZE`, the number of documents per index level.
const BOUNDARY: usize = 65_536;
/// Two levels: one full and a 64-document incomplete one.
const DOCUMENTS: usize = 65_600;
/// Documents indexed between two commits.
const PER_COMMIT: usize = 8;

/// Minimal single-shard RAM configuration for the level-rollback regression.
/// It is built by hand so that the environment cannot switch the test to the
/// mmap path or to a different shard layout.
fn index_config() -> IndexConfig {
    IndexConfig {
        id: 0,
        name: "rollback_incomplete_level".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::UnicodeAlphanumeric,
        stemmer: StemmerType::None,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::None,
        ngram_indexing: NgramSet::SingleTerm as u8,
        document_compression: DocumentCompression::None,
        access_type: AccessType::Ram,
        spelling_correction: None,
        query_completion: None,
        clustering: Clustering::None,
        inference: Inference::None,
        segment_number_bits: 10,
    }
}

fn schema() -> Vec<SchemaField> {
    vec![
        SchemaField::new("_id").store(true).index_lexical(false),
        SchemaField::new("body").store(false).index_lexical(true),
    ]
}

/// Every document carries `common`; documents past the level boundary also
/// carry a unique `tail{id}` that exists only in the incomplete level - the
/// term shape that tripped the 5.2.1 rollback.
fn document(id: usize) -> Document {
    let body = if id < BOUNDARY {
        "common".to_string()
    } else {
        format!("common tail{id}")
    };
    let mut document = Document::default();
    document.insert("_id".to_string(), Value::String(format!("doc{id}")));
    document.insert("body".to_string(), Value::String(body));
    document
}

/// Indexing is spawned on the index runtime and only bounded by a per-shard
/// semaphore, so wait until every document reached the shard before committing.
async fn wait_until_indexed(index: &IndexArc, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let indexed = index.read().await.indexed_doc_count().await;
        if indexed >= expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "indexing stalled at {indexed} of {expected} documents"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn index_documents(index: &IndexArc, from: usize, to: usize) {
    index
        .index_documents((from..to).map(document).collect())
        .await;
    wait_until_indexed(index, to).await;
}

async fn committed_documents(index: &IndexArc) -> usize {
    index.read().await.committed_doc_count().await
}

/// Exhaustive committed count: `length: 0` with `ResultType::Count` disables
/// the top-k heap, so nothing is pruned.
async fn committed_count(index: &IndexArc, query: &str) -> usize {
    index
        .search(SearchRequest {
            query_string: query,
            enable_empty_query: true,
            length: 0,
            result_type: ResultType::Count,
            include_uncommitted: false,
            ..SearchRequest::default()
        })
        .await
        .count_total
}

/// A scratch index directory under the system temp dir, removed on drop.
struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after the epoch")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("seekstorm-{name}-{}-{unique}", std::process::id()));
        std::fs::create_dir_all(&path).expect("temp index dir");
        Self(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Restores the process panic hook even when an assertion fails, so a failing
/// run does not silence the rest of the suite.
struct PanicHookGuard(Option<Box<dyn Fn(&std::panic::PanicHookInfo<'_>) + Sync + Send>>);

impl Drop for PanicHookGuard {
    fn drop(&mut self) {
        if let Some(hook) = self.0.take() {
            std::panic::set_hook(hook);
        }
    }
}

type RecordedPanics = Arc<Mutex<Vec<String>>>;

/// The commit fan-out spawns one task per shard and only logs the `JoinError`
/// of a panicking task, so record panics instead of printing them. Drop the
/// returned guard before asserting: assertions made while the recording hook
/// is installed would print nothing.
fn record_panics() -> (RecordedPanics, PanicHookGuard) {
    let recorded: RecordedPanics = Arc::new(Mutex::new(Vec::new()));
    let previous_hook = std::panic::take_hook();
    let sink = Arc::clone(&recorded);
    std::panic::set_hook(Box::new(move |info| {
        sink.lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(info.to_string());
    }));
    (recorded, PanicHookGuard(Some(previous_hook)))
}

fn recorded_panics(recorded: &RecordedPanics) -> Vec<String> {
    recorded
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

#[tokio::test]
async fn ram_commit_rolls_back_incomplete_level_without_panicking() {
    let started = Instant::now();
    let root = TempDir::new("rollback");
    let path = root.path().join("rollback");
    let index = create_index(&path, index_config(), &schema(), &[])
        .mute(true)
        .force_shard_number(1)
        .await
        .expect("index creates");
    {
        let guard = index.read().await;
        assert_eq!(guard.shard_count().await, 1, "one shard is required");
        assert_eq!(guard.index_config.access_type, AccessType::Ram);
        assert_eq!(guard.index_config.segment_number_bits, 10);
    }

    // The commit fan-out spawns one task per shard, so panics are recorded
    // and asserted on together with the committed state.
    let (panics, restore_hook) = record_panics();

    // The build commit: 65,600 documents leave a full level plus an incomplete
    // one, and the rollback of that incomplete level runs here already.
    index_documents(&index, 0, DOCUMENTS).await;
    index.commit().await;
    let after_build = committed_documents(&index).await;
    let levels_after_build = index.read().await.level_count().await;

    let mut written = DOCUMENTS;
    let mut after_commits = Vec::new();
    for _ in 0..3 {
        index_documents(&index, written, written + PER_COMMIT).await;
        written += PER_COMMIT;
        index.commit().await;
        after_commits.push(committed_documents(&index).await);
    }

    let searchable_common = committed_count(&index, "common").await;
    // One tail from the first incomplete level (merged back into level 0 by the
    // rollback) and one from the last commit must both survive.
    let first_tail = committed_count(&index, &format!("tail{BOUNDARY}")).await;
    let last_tail = committed_count(&index, &format!("tail{}", written - 1)).await;

    // Restore the default hook before asserting, so the assertion message is
    // the one that is actually printed.
    drop(restore_hook);
    let recorded = recorded_panics(&panics);
    assert!(
        recorded.is_empty(),
        "the commit fan-out panicked {} time(s): {recorded:?}",
        recorded.len()
    );
    assert_eq!(
        after_build, DOCUMENTS,
        "the build commit must persist every document"
    );
    assert_eq!(
        levels_after_build, 2,
        "65,600 documents must leave a full level plus an incomplete one"
    );
    for (round, committed) in after_commits.iter().enumerate() {
        assert_eq!(
            *committed,
            DOCUMENTS + (round + 1) * PER_COMMIT,
            "commit {} must advance the committed document count",
            round + 1
        );
    }
    assert_eq!(
        searchable_common, written,
        "every written document must be searchable after the third commit"
    );
    assert_eq!(first_tail, 1, "the first tail term must survive");
    assert_eq!(last_tail, 1, "the last tail term must survive");

    // Reopen with an incomplete last level and write again. `open_index`
    // restores the committed document count but leaves `Shard::block_id` at the
    // `Shard::new` default of 0, so without the restore the first write lands
    // in block 0 and the commit that merges and rolls back the restored
    // incomplete level mislabels the merged postings: the merged block is
    // registered for block 1 while its docids belong to block 0, and
    // `update_list_max_impact_score`/`get_max_score` then read an empty block.
    let reopened_at = written;
    let (reopen_panics, restore_hook) = record_panics();
    index.close().await;
    drop(index);
    let reopened = open_index(&path).await.expect("index reopens");
    let restored_committed = committed_documents(&reopened).await;
    let restored_searchable = committed_count(&reopened, "common").await;
    let restored_tail = committed_count(&reopened, &format!("tail{BOUNDARY}")).await;

    index_documents(&reopened, written, written + PER_COMMIT).await;
    written += PER_COMMIT;
    reopened.commit().await;
    let after_reopen_commit = committed_documents(&reopened).await;
    let after_reopen_searchable = committed_count(&reopened, "common").await;
    let after_reopen_last_tail = committed_count(&reopened, &format!("tail{}", written - 1)).await;

    drop(restore_hook);
    let recorded = recorded_panics(&reopen_panics);
    assert!(
        recorded.is_empty(),
        "the first write commit after a reopen panicked {} time(s): {recorded:?}",
        recorded.len()
    );
    assert_eq!(
        restored_committed, reopened_at,
        "reopen must restore every committed document"
    );
    assert_eq!(
        restored_searchable, reopened_at,
        "reopen must keep every document searchable"
    );
    assert_eq!(
        restored_tail, 1,
        "the first tail term must survive a reopen"
    );
    assert_eq!(
        after_reopen_commit,
        reopened_at + PER_COMMIT,
        "the first write commit after a reopen must advance the committed count"
    );
    assert_eq!(
        after_reopen_searchable,
        reopened_at + PER_COMMIT,
        "the documents written after a reopen must be searchable"
    );
    assert_eq!(
        after_reopen_last_tail, 1,
        "the last tail term written after a reopen must be searchable"
    );

    reopened.close().await;
    eprintln!(
        "seekstorm rollback regression: {written} documents in {:?}",
        started.elapsed()
    );
}
