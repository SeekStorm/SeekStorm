//! Lexical search benchmark of SeekStorm over 5 million Wikipedia articles, with 903 search queries,
//! - of different query types (AND, OR, PHRASE)
//! - of different keyword numbers,
//! - of different result types (Count, TopkCount, Topk),
//! - measuring total result number, average latency, and percentile latencies.
//! -
//! Start benchmark with `cargo bench --features gxhash`.
//! ⚠️ Before running the benchmark, copy the Wikipedia data manually from https://www.dropbox.com/s/wwnfnu441w1ec9p/wiki-articles.json.bz2 into the `benches/test_data` directory and decompress.
//! ⚠️ Make sure the console buffer is large enough to display all benchmark output.
//!
//! This benchmark is loosely based on https://github.com/quickwit-oss/search-benchmark-game.
//! It is intended mainly for development regression tests when evaluating future search improvements.
//! It runs on any platform, whereas the original search-benchmark-game relies on unix-specific dependencies, commands and shell syntax.
//! By using per-query median latency (of 5 non-consecutive runs per query), it separates the latency attributable to the search algorithm/implementation from incidental execution noise that is outside your control.
//!
//! For a faithful fork of search-benchmark-game that includes SeekStorm, Tantivy and Lucene see https://github.com/SeekStorm/search-benchmark-game
//! That benchmark is used for our published results, see https://seekstorm.github.io/search-benchmark-game

use num_format::Locale;
use num_format::ToFormattedString;
use seekstorm::index::{
    AccessType, Clustering, DocumentCompression, FrequentwordType, IndexMetaObject,
    LexicalSimilarity, NgramSet, SchemaField, StemmerType, StopwordType, TokenizerType,
    create_index, open_index,
};
use seekstorm::ingest::IngestJson;
use seekstorm::search::{
    FacetFilter, QueryFacet, QueryRewriting, QueryType, ResultSort, ResultType, Search, SearchMode,
};
use seekstorm::vector::Inference;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};

use rand::{self, RngExt, rng};
/// Implements percentile, mean, median and maximum metrics for latency.
///
/// A 99th percentile latency of X milliseconds means that 99% of the latencies are below X
pub(crate) struct Percentile {
    pub count: i32,
    pub size: i32,
    pub reservoir: Vec<i64>,
    pub median: i64,
    pub percentile50: i64,
    pub percentile75: i64,
    pub percentile90: i64,
    pub percentile95: i64,
    pub percentile98: i64,
    pub percentile99: i64,
    pub percentile999: i64,
    pub maximum: i64,
    pub sum: i64,
}

impl Default for Percentile {
    fn default() -> Self {
        Self {
            size: 10_000,
            count: 0,
            reservoir: Vec::new(),
            median: 0,
            percentile50: 0,
            percentile75: 0,
            percentile90: 0,
            percentile95: 0,
            percentile98: 0,
            percentile99: 0,
            percentile999: 0,
            maximum: 0,
            sum: 0,
        }
    }
}

impl Percentile {
    pub(crate) fn add(&mut self, x: i64) {
        if self.count == i32::MAX {
            return;
        }

        if x > self.maximum {
            self.maximum = x;
        }
        self.sum += x;

        if self.count < self.size {
            self.reservoir.push(x);
        } else {
            let p = rng().random_range(0..self.count);

            if p < self.size {
                self.reservoir[p as usize] = x;
            }
        }
        self.count += 1;
    }

    pub(crate) fn calc(&mut self) {
        let n = self.reservoir.len();
        if n < 1 {
            return;
        }
        self.reservoir.sort();
        self.median = self.reservoir[n / 2];
        if n.is_multiple_of(2) {
            self.median = (self.median + self.reservoir[n / 2 - 1]) / 2;
        }
        self.percentile50 = self.reservoir[(n * 50) / 100];
        self.percentile75 = self.reservoir[(n * 75) / 100];
        self.percentile90 = self.reservoir[(n * 90) / 100];
        self.percentile95 = self.reservoir[(n * 95) / 100];
        self.percentile98 = self.reservoir[(n * 98) / 100];
        self.percentile99 = self.reservoir[(n * 99) / 100];
        self.percentile999 = self.reservoir[(n * 999) / 1000];
    }
}

fn data_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("benches")
        .join("test_data")
        .join(name)
}

const QUERY_TYPE_FILTERS: [&str; 16] = [
    "ALL",                        //0
    "global",                     //1 - no term, phrase, two-phase-critic
    "intersection",               //2
    "intersection:num_tokens_2",  //3
    "intersection:num_tokens_3",  //4
    "intersection:num_tokens_>3", //5
    "phrase",                     //6
    "phrase:num_tokens_2",        //7
    "phrase:num_tokens_3",        //8
    "phrase:num_tokens_>3",       //9
    "term",                       //10
    "two-phase-critic",           //11
    "union",                      //12
    "union:num_tokens_2",         //13
    "union:num_tokens_3",         //14
    "union:num_tokens_>3",        //15
];
#[allow(missing_docs)]
#[derive(Default, Debug, serde::Deserialize, serde::Serialize, Clone)]
pub struct QueryObject {
    pub query: String,
    pub tags: Vec<String>,
}

const SAMPLE_COUNT: usize = 5;

struct ResultEntry {
    query_index: usize,
    search_time: [i64; SAMPLE_COUNT],
    result_type: ResultType,
    result_count: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("Benchmark started ...");

    // create index

    let ingest_path = data_path("wiki-articles.json");
    let index_path = data_path("benchmark_index");
    let query_path = data_path("search-benchmark-game-queries.txt");
    let count_path = data_path("result_count_total.bin");

    let index_arc = match open_index(&index_path).await {
        Ok(index) => index,
        Err(err) => {
            // does exists, but incompatible version
            if err
                .to_string()
                .contains("incompatible index format version")
            {
                println!("{}", err);
                return Ok(());
            }

            if !ingest_path.exists() {
                println!(
                    "\n{} does not exist. ⚠️ Before running the benchmark, copy the Wikipedia data manually from https://www.dropbox.com/s/wwnfnu441w1ec9p/wiki-articles.json.bz2 into the `benches/test_data` directory and decompress.",
                    ingest_path.display()
                );
                return Ok(());
            }

            let start_time = Instant::now();

            let wikipedia_schema_json = r#"
                [{"field":"title","field_type":"Text","store":false,"index_lexical":false},
                {"field":"body","field_type":"Text","store":false,"index_lexical":true,"longest":true},
                {"field":"url","field_type":"Text","store":false,"index_lexical":false}]"#;

            let schema: Vec<SchemaField> = serde_json::from_str(wikipedia_schema_json).unwrap();

            let meta = IndexMetaObject {
                id: 0,
                name: "wikipedia".to_string(),
                lexical_similarity: LexicalSimilarity::Bm25f,
                tokenizer: TokenizerType::AsciiAlphabetic,
                stemmer: StemmerType::None,
                stop_words: StopwordType::None,
                frequent_words: FrequentwordType::English,
                //ngram_indexing: NgramSet::SingleTerm as u8,
                //ngram_indexing: NgramSet::NgramFF as u8,
                ngram_indexing: NgramSet::SingleTerm as u8
                    | NgramSet::NgramFF as u8
                    | NgramSet::NgramFR as u8
                    | NgramSet::NgramRF as u8
                    | NgramSet::NgramFFF as u8,
                document_compression: DocumentCompression::Snappy,
                access_type: AccessType::Mmap,
                spelling_correction: None,
                query_completion: None,
                clustering: Clustering::None,
                inference: Inference::None,
                segment_number_bits: 11,
            };

            let mut index_arc = create_index(&index_path, meta, &schema, &Vec::new(), false, None)
                .await
                .unwrap();

            // ingest data
            index_arc.ingest_json(&ingest_path).await;

            let index_time = start_time.elapsed().as_secs();
            println!("Indexing time: {} s", index_time);

            index_arc
        }
    };

    // benchmark parameters

    let cool_time = Duration::from_secs(10);
    let result_length = 10usize;
    let search_mode = SearchMode::Lexical;
    let thread_number = 1;

    // import queries from file

    let mut query_vec: Vec<(String, QueryType, Vec<String>)> = Vec::new();
    let file = File::open(query_path).unwrap();
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line_string = line.unwrap();

        if line_string.is_empty() {
            continue;
        }

        // deserialize query json
        let query_object = serde_json::from_str::<QueryObject>(&line_string).unwrap();

        let query_type = if query_object.tags.contains(&"intersection".to_string()) {
            QueryType::Intersection
        } else if query_object.tags.contains(&"phrase".to_string())
            || query_object.tags.contains(&"two-phase-critic".to_string())
        {
            QueryType::Phrase
        } else {
            QueryType::Union
        };

        // query_type is not used, just informational for debugging
        query_vec.push((
            query_object.query.to_lowercase(),
            query_type,
            query_object.tags,
        ));
    }

    thread::sleep(cool_time);

    // query and result_type loops

    let query_count = query_vec.len() as i64;
    let permits = Arc::new(Semaphore::new(thread_number));

    let result_list: Vec<ResultEntry> = Vec::with_capacity(query_count as usize);
    let result_list_arc = Arc::new(Mutex::new(result_list));

    let query_facets: Vec<QueryFacet> = Vec::new();
    let facet_filter: Vec<FacetFilter> = Vec::new();
    let result_sort: Vec<ResultSort> = Vec::new();

    // only the median of the sample search times is used - throwing away outliers also serves as warmup and reduces incidental execution noise
    for sample_index in 0..SAMPLE_COUNT {
        for (query_index, query) in query_vec.iter().enumerate() {
            for (result_type_index, result_type) in
                [ResultType::TopkCount, ResultType::Count, ResultType::Topk]
                    .iter()
                    .enumerate()
            {
                let permit_thread = permits.clone().acquire_owned().await.unwrap();

                let query_index_clone = query_index;
                let query_clone = query.clone();
                let index_arc_clone = index_arc.clone();
                let result_list_clone = result_list_arc.clone();
                let result_type_clone = result_type.clone();
                let result_type_clone2 = result_type.clone();
                let search_mode_clone = search_mode.clone();
                let result_length_clone = result_length;
                let include_uncommitted = false;
                let _query_facets_clone = query_facets.clone();
                let _facet_filter_clone = facet_filter.clone();
                let _result_sort_clone = result_sort.clone();

                tokio::spawn(async move {
                    let start_time = Instant::now();

                    let result_object = index_arc_clone
                        .search(
                            query_clone.0,
                            None,
                            query_clone.1,
                            search_mode_clone,
                            false,
                            0,
                            result_length_clone,
                            result_type_clone,
                            include_uncommitted,
                            Vec::new(),
                            Vec::new(),
                            Vec::new(),
                            Vec::new(),
                            QueryRewriting::SearchOnly,
                        )
                        .await;

                    let search_time = start_time.elapsed().as_nanos() as i64;

                    // record search time
                    let mut result_list_mut = result_list_clone.lock().await;
                    if sample_index == 0 {
                        result_list_mut.push(ResultEntry {
                            query_index: query_index_clone,
                            search_time: [search_time; SAMPLE_COUNT],
                            result_type: result_type_clone2,
                            result_count: result_object.result_count_total,
                        });
                    } else {
                        result_list_mut[query_index * 3 + result_type_index].search_time
                            [sample_index] = search_time;
                    }
                    drop(result_list_mut);
                    drop(permit_thread);
                });
            }
        }
    }

    let _permit = permits.acquire().await?;

    println!("\nBenchmarking finished.");

    let mut result_list = result_list_arc.lock().await;
    // save result count for each query for verification against consecutive runs
    // you need to enable this only to update the included result_count_total.bin, if you added or modified queries or result types
    /*
    let mut file: File = File::create(&count_path)?;
    for result in result_list.iter() {
        file.write_all(&result.result_count.to_le_bytes())?;
    }
    file.flush()?;
    println!("Saved result count for each query to {:?}", count_path);
    */

    // verify result count for each query
    let mut file = File::open(&count_path)?;
    for result in result_list.iter_mut() {
        let mut buffer = [0u8; 8];
        file.read_exact(&mut buffer)?;
        let read_result_count = usize::from_le_bytes(buffer);
        if read_result_count != result.result_count {
            println!(
                "Mismatch for query {}  result type {:?}  expected {}, got {}",
                query_vec[result.query_index].0,
                result.result_type,
                result.result_count,
                read_result_count
            );
        }

        // sort search_time samples for each query
        result.search_time.sort();
    }
    println!("Verified result count for each query.");

    // display min, median, and max search times for each query
    println!("\nDisplaying min, median, and max search times for each query:");
    println!("------------------------------------------------------------");
    for result in result_list.iter() {
        let min_search_time = result.search_time.first().cloned().unwrap_or(0);
        let median_search_time = result.search_time[SAMPLE_COUNT / 2];
        let max_search_time = result.search_time.last().cloned().unwrap_or(0);
        println!(
            "Query: {} Query Type: {:?} Result Type: {:<9} Min: {} µs, Median: {} µs, Max: {} µs, Result Count Total: {}",
            query_vec[result.query_index].0,
            query_vec[result.query_index].1,
            format!("{:?}", result.result_type),
            (min_search_time / 1000).to_formatted_string(&Locale::en),
            (median_search_time / 1000).to_formatted_string(&Locale::en),
            (max_search_time / 1000).to_formatted_string(&Locale::en),
            result.result_count.to_formatted_string(&Locale::en)
        );
    }

    // summarize and display results
    println!("\nDisplaying summarized search times for each query type and result type:");
    println!("------------------------------------------------------------");
    drop(result_list);

    for query_type in QUERY_TYPE_FILTERS {
        for result_type in [ResultType::TopkCount, ResultType::Count, ResultType::Topk] {
            let result_list = result_list_arc.lock().await;

            let filtered_results: Vec<&ResultEntry> = result_list
                .iter()
                .filter(|entry| {
                    query_vec[entry.query_index].2.iter().any(|tag| {
                        ((tag.as_str() == query_type) || (query_type == "ALL"))
                            && entry.result_type == result_type
                    })
                })
                .collect();

            let total_search_time: i64 = filtered_results
                .iter()
                .map(|entry| entry.search_time[SAMPLE_COUNT / 2]) // use median search time
                .sum();
            let average_search_time = if !filtered_results.is_empty() {
                total_search_time / filtered_results.len() as i64
            } else {
                0
            };

            let result_count_total_sum: usize = filtered_results
                .iter()
                .map(|entry| entry.result_count) // use median search time
                .sum();

            println!(
                "Query Type: {:?}, Result Type: {:<9}, Average Search Time: {} µs, Matching Query Count: {}, Result Count Total Sum: {}",
                query_type,
                format!("{:?}", result_type),
                (average_search_time / 1000).to_formatted_string(&Locale::en),
                filtered_results.len().to_formatted_string(&Locale::en), //the number of queries that matched the current query type - not the sum of the total result counts
                result_count_total_sum.to_formatted_string(&Locale::en)
            );

            // calculate and  display tail latency statistics

            let mut sample_search_time: Percentile = Percentile::default();
            for filtered_result in filtered_results.iter() {
                sample_search_time.add(filtered_result.search_time[SAMPLE_COUNT / 2]); // use median search time
            }
            sample_search_time.calc();

            println!(
                "\nSearch Latency\nmean: {} µs\nmedian: {} µs\nP75: {} µs\nP90: {} µs\nP95: {} µs\nP98: {} µs\nP99: {} µs\nP99.9: {} µs\nmax: {} µs\n",
                (sample_search_time.sum / 1_000 / filtered_results.len() as i64)
                    .to_formatted_string(&Locale::en),
                (sample_search_time.median / 1_000).to_formatted_string(&Locale::en),
                (sample_search_time.percentile75 / 1_000).to_formatted_string(&Locale::en),
                (sample_search_time.percentile90 / 1_000).to_formatted_string(&Locale::en),
                (sample_search_time.percentile95 / 1_000).to_formatted_string(&Locale::en),
                (sample_search_time.percentile98 / 1_000).to_formatted_string(&Locale::en),
                (sample_search_time.percentile99 / 1_000).to_formatted_string(&Locale::en),
                (sample_search_time.percentile999 / 1_000).to_formatted_string(&Locale::en),
                (sample_search_time.maximum / 1_000).to_formatted_string(&Locale::en)
            );
        }
    }
    Ok(())
}
