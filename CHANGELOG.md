# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.2.1] - 2026-02-06

### Fixed

- Fixed update document(s) REST API endpoint document array detection fixed. There was an issue when the document itself contained a `[`-char.
- Fixed issue #57. `index_document`/`index_posting` caused an exception after a previously committed incomplete level due to a wrong posting list `CompressionType` deserialization.

## [2.2.0] - 2026-01-30

### Added

- Multiple `document_compression` methods: `None`, `Snappy`, `Lz4`, `Zstd`.  
  **Faster search (200% median, 110% mean)** and **50% faster indexing** with Snappy, compared to Zstandard, **if documents are stored and loaded** from the document store.  
  Now **you have control** over the best balance of index size, indexing speed, and query latency **for your use case**.  
  Some search benchmarks measure only pure search performance, but **real-world usage almost always includes loading documents** from the document store as well.

  | Compression | indexing speed | index size | query latency (mean) | query latency (median) |
  | :---------- | -------------: | ---------: | -------------------: | ---------------------: |
  | Snappy      |  24 docs/sec   | 10,535 MB  |              0.45 ms |                0.24 ms |
  | Zstandard   |  16 docs/sec   |  9,026 MB  |              0.95 ms |                0.72 ms |

  *Benchmark: 5 million Wikipedia documents, 3 fields stored and returned, 2 fields indexed and searched*

- Added `FieldType::Json` that supports hierarchical JSON Objects. Fixes issue #55 .
  - The text is extracted from all levels of the JSON object and combined into a single text string (values only, not keys) for indexing and highlighting.  
  - In that respect, it is similar to combined fields in other search engines.  
  - Despite being indexed as a single text string, the hierarchical structure of an JSON object is preserved in the stored document, so that it can be retrieved in the search results.  
  - When using search with highlighting, make sure to use a `name` different from `field`, otherwise the Json `field` in the results will be overwritten with the highlight (snippet) text.

### Changed

- The order of fields in the Json document is now preserved between indexing and query.
  - `Document` has been changed from `HashMap<String, Value>` to `IndexMap<String, Value>`.
  - `utoipa` now uses `features = ["indexmap"]`.
  - `serde_json` now uses `features = ["preserve_order"]`.  

### Fixed

- Fixed issue #57 with word segmentation in `TokenizerType::UnicodeAlphanumericZH`.

## [2.1.0] - 2026-01-28

### Added

- `search` now supports an **empty query**: similar to an iterator across all indexed documents, but **all search parameters** are supported, apart from query and field_filter:
  - result_type: ResultType,
  - include_uncommitted: bool,
  - query_facets: Vec<QueryFacet>, 
  - facet_filter: Vec<FacetFilter>, 
  - result_sort: Vec<ResultSort>,  
    For search **with empty query**, if no sort field is specified, then the search results are sorted by `_id` in `descending` order per default (newest first).  
    For search **with query** the sort default is `_score`.  
    If `result_sort` is used with any other than the special field `_id`, then a min-heap consumes RAM proportional to **offset + length**.  
    Per default or if sort field `_id` is specified, the consumed RAM is proportional to **length**.  
- Added search parameter `enable_empty_query`: if `true`, then for an empty query, the results are returned from the iterator; otherwise, for an empty query, no results are returned.
  - With an empty query, the **embedded Web UI** now allows to browse page-wise through the whole index. Try, e.g., our [Wikipedia Demo](https://github.com/SeekStorm/SeekStorm?tab=readme-ov-file#demo-time).
- Now it is possible to sort results by two special fields _id, and _score, ascending and descending, both for search with query and with empty query.
	- no need to set those field in schema as facet=true, in order to sort them, unlike with other fields.
	- the two special fields are also automatically injected in the result documents.
	- if the one of the two special fields is set in result_sort, all other sort fields after this are ignored for sorting (tie-break after a field where no two elemands are the same just doesnt make sense)
- default sort, if no sort_field ist specified, or sort after last tie-break: 
	- empty query: docid descending
	- with query: score descending
- Iterator adds new `include_deleted` parameter: if true, also deleted document IDs are included in the result.
- Iterator adds new `include_document` parameter: if true, the documents are also returned along with their document IDs.
- Iterator adds a new `fields` parameter: a list of field names that specifies which fields to return, if include_document is true.  
  If the fields parameter is empty or not present, then all stored fields are returned.
- Added unit tests for get_iterator and search with empty query.

### Changed
- Iterator API changed: in addition to document IDs, now optionally the documents themselves are returned. Optionally, a list of field names specifies which document fields to return.
  - method: `get_docid(Option<doc_id:u64>,skip:usize,take:isize)` -> `get_iterator(Option<doc_id:u64>,skip:usize,take:isize,include_doc:bool)` 
  - return object: `tuple (skip,Vec<doc_id>)` -> `IteratorResult {skip:usize, results: Vec<IteratorResultItem>}` and `IteratorResultItem{doc_id:u64, doc: Option<Document>}`
- Iterator REST API changed: Instead of `GET` with `request object` there are now two alternative options:
  - `GET` with `query parameters` (in the URL after the question mark '?').
  - `POST` with `request object`.
- Spelling correction dictionary and completion list are only saved, if the index has been modified AND if auto-creation was enabled  (SchemaField.completion_source=true, SchemaField.dictionary_source=true).
  If auto-creation is disabled then any static completion lists and spelling correction dictionaries are not overwritten.
- Skip any invoked commit if no uncommitted documents (per index and per shard).

## [2.0.0] - 2026-01-22

### Added

- Document ID iterator `get_docid`, both for SeekStorm library and server. 
  The document ID iterator allows to iterate over all document IDs and documents in the entire index, forward or backward. 
  It enables efficient sequential access to every document, even in very large indexes, without running a search. 
  Paging through the index works without collecting document IDs to Min-heap in size-limited RAM first.
  The iterator guarantees that only valid document IDs are returned, even though document IDs are not strictly continuous. 
  Document IDs can also be fetched in batches, reducing round trips and significantly improving performance, especially when using the REST API.
  Typical use cases include index export, conversion, analytics, audits, and inspection.

  Explanation of "eventually continuous" docid:
  In SeekStorm, document IDs become continuous over time. In a multi-sharded index, each shard maintains its own document ID space. 
  Because documents are distributed across shards in a non-deterministic, load-dependent way, shard-local document IDs advance at different rates. 
  When these are mapped to global document IDs, temporary gaps can appear.
  As a result, simply iterating from 0 to the total document count may encounter invalid IDs near the end. 
  The Document ID Iterator abstracts this complexity and reliably returns only valid document IDs.

  - docid=None, take>0: **skip first s document IDs**, then **take next t document IDs** of an index.
  - docid=None, take<0: **skip last s document IDs**, then **take previous t document IDs** of an index.
  - docid=Some, take>0: **skip next s document IDs**, then **take next t document IDs** of an index, relative to a given document ID, with end-of-index indicator.
  - docid=Some, take<0: **skip previous s document IDs**, then **take previous t document IDs**, relative to a given document ID, with start-of-index indicator.

### Changed

- The calculation of the global document ID from the shard document ID during aggregation has been changed from a bit-shift operation to a modulo operation to ensure gapless document IDs 
  (apart from document IDs in the last incomplete index level of each shard), even if the number of shards is not a power of 2.
- Due to sharding document IDs are not guaranteed to be continuous and gapless! Always use `search` or the new iterator `get_docid` first to obtain a valid document IDs! 
  Added a warning hereof to the documentation of get_document and delete_document for both the library API and REST API.
- Index format (`INDEX_FORMAT_VERSION_MAJOR`) changed: different document ID calculation. 
- Query auto-completion implementation simplified.

### Fixed

- Fixed typo in `include_uncommitted` parameter.
- Fixed an issue with query auto-completion if the input contained spaces.
- ReadmeDoctests fixed!
- The changed document ID calculation together with the new Document ID iterator `get_docid` fixes #54 .

## [1.2.5] - 2026-01-17

### Added

- [Early query completion expansion](https://seekstorm.com/blog/query-auto-completion-(QAC)/#sliding-window-completion-expansion): if a query with >=2 terms returns less than max_completion_entries, but a completion with 3 terms is returned, it is expanded with more query terms.  
  Previously, only the last incomplete term of a query was completed, now the completion is expanded early to look one more term ahead. The intended full query is reached earlier, saving even more time.  
  Previously: `united states ol` -> `united states olympic`  
  Now:        `united states ol` -> `united states olympic`, `united states olympic trials`, `united states olympic curling`, `united states olympic basketball`, `united states olympic committee`

## [1.2.4] - 2026-01-15

### Added

- New `SpellingCorrection.count_threshold`: The minimum frequency count per index for dictionary words to be eligible for spelling correction can now be set by the user for more control over the dictionary generation.
  If count_threshold is too high, some correct words might be missed from the dictionary and deemed misspelled, 
  if count_threshold too low, some misspelled words from the corpus might be considered correct and added to the dictionary.
  Dictionary terms eligible for spelling correction (frequency count >= count_threshold) consume much more RAM, than the candidates (frequency count < count_threshold), 
  but the terms below count_threshold will be included in dictionary.csv too. 

### Improved

- Better auto-generated dictionary for spelling correction
  - `SpellingCorrection.max_dictionary_entries` now limits the number of words in the dictionary with a frequency count >= count_threshold (previously including words with a frequency count < count_threshold).  
  - For spelling correction, numbers are now always ignored and not added to the dictionary. The are always deemed correct during spelling correction.
  - For spelling correction, internal count_threshold per level removed, to allow uniform distributed terms to reach the dictionary.
  - For spelling correction, internal count_threshold per doc decreased from 2 to 1, allowing to derive terms when only a single, short field is enabled with `SchemaField.dictionary_source`.

## [1.2.3] - 2026-01-11

### Changed

- Updated [OpenAPI definition](https://github.com/SeekStorm/SeekStorm/tree/main/src/seekstorm_server/openapi) files.

## [1.2.2] - 2026-01-11

### Changed

- Updated [OpenAPI definition](https://github.com/SeekStorm/SeekStorm/tree/main/src/seekstorm_server/openapi) files.

## [1.2.1] - 2026-01-11

### Added

- Completion of spelling corrected query.

### Changed

- Highlighting of completions in dropdown reversed. Now the part the user didn't type will be highlighted, while the part they typed remains plain.
- Font size of input and completion dropdown are now identical.

## [1.2.0] - 2026-01-09

### Added

- Typo-tolerant Query Auto-Completion (QAC) and Instant Search: see [blog post](https://seekstorm.com/blog/query-auto-completion-(QAC)/).
  - The completions are automatically derived in real-time from indexed documents, not from a query log:
    - works, even if no query log is available, especially for domain-specific, newly created indices or few users.
    - works for new or domain-specific terms.
    - allows out-of-the-box domain specific suggestions
    - prevents inconsistencies between completions and index content.  
      Query logs contains invalid queries: because its sourced from a different index or because users don't know the content of your index.
    - Works for the long tail of queries, that never made it to a log.
    - SchemaField.completion_source : if both indexed=true and completion_source=true then the n-grams (unigrams, bigrams, trigrams) from this field are added to the auto-completion list.
    - SchemaField.dictionary_source : if both indexed=true and dictionary_source=true then the terms from this field are added to dictionary to the spelling correction dictionary.
    - create_index QueryCompletion    max_completion_entries: Maximum number of completion entries to generate during indexing
    - create_index SpellingCorrection max_dictionary_entries: Maximum number of dictionary entries to generate during indexing
    - If `SchemaField.completion_source=false` for all fields, then a manually generated completion list can be used: {index_path}/completions.csv
    - If `SchemaField.dictionary_source=false` for all fields, then a manually generated dictionary can be used: {index_path}/dictionary.csv
  - Completion prevents spelling errors and saves time.
  - Language-, content- and domain independent.
  - Additionally allows to use hand crafted completion files.
  - Ghosting: highlighting the suggested text within the search box in the UI
  - QueryRewriting::SearchOnly/SearchSuggest/SearchCorrect/SuggestOnly.complete : enable query completions in addition to spelling corrections
  - The embedded Web UI of the SeekStorm server now supports query auto-correction, query auto-completion, and instant search.
  - Queries with spelling mistakes are marked with a wavy red underline. You get a "did you mean"-like choice to search for the corrected or the original query.
  - For a demo of the Query Auto-Completion (QAC) see [Build a Wikipedia search engine with the SeekStorm server](https://github.com/SeekStorm/SeekStorm?tab=readme-ov-file#build-a-wikipedia-search-engine-with-the-seekstorm-server).

### Fixed

- Fixed paging (offset>0) for search in sharded index.
- When query spelling correction/completion is enabled (`QueryRewriting::SearchSuggest/SearchRewrite/SuggestOnly`) it now supports/preserves phrases "",  
  but query spelling correction/completion is automatically disabled if +- operators are used, or if a opening quote is used after the first term, or if a closing quote is used before the last term.
- In the Web UI query spelling corrected queries were not used when switching to next or previous page. Fixed!
- If in index schema a field is set to "longest"=true, but "indexed"=false, then fallback to auto-detection of longest field.  

### Changed

- QueryRewriting::SearchCorrect changed to QueryRewriting::SearchRewrite 
- Moved index_arc.close() from commit.rs to index.rs
- symspell_rs dependency replaced with symspell_complete_rs

## [1.1.3] - 2025-12-03

### Added

- hello endpoint added to seekstorm server: http://127.0.0.1/api/v1/hello -> returns "SeekStorm server 1.1.3"

### Fixed

- seekstorm server commandline disabled if no terminal/tti (docker parameter -ti) detected. Fixes #39 .

## [1.1.2] - 2025-11-30

### Fixed

- normalisation/folding of ligatures and roman numerals fixed.

## [1.1.1] - 2025-11-30

### Fixed

- If TokenizerType::UnicodeAlphanumericFolded is selected, then diacritics, ligatures, and accents in the query string are now folded prior to spelling correction.
- Examples for query spelling correction added to README.md (in create_index and search).
- Examples for specifying Boolean queries via query operators and query type have been added to README.md.
- Query operators added to library documentation.
- Limit the size of the min-heap per shard to s=cmp::min(offset+length, shard.indexed_doc_count).  
  This prevents an out-of-memory error if the query parameter length is set to usize::MAX, while the actual query results would still fit in memory (see issue #15).

## [1.1.0] - 2025-11-27

### Added

- Added query spelling correction / typo-tolerant search / fuzzy queries by integrating [SymSpell](https://github.com/wolfgarbe/symspell_rs), both to SeekStorm library and SeekStorm server REST API.
  - New `create_index`, `IndexMetaObject` parameter property: `spelling_correction: Option<SpellingCorrection>`: 
    - enables automatic incremental creation of the Symspell dictionary during the indexing of documents.
  - New `search` parameter: `query_rewriting: QueryRewriting`: Enables query rewriting features such as spelling correction and suggestions.
    - `SearchOnly`: Query rewriting disabled, returs query results for query as-is, returns no suggestions for misspelled query terms.
    - `SearchSuggest`: Query rewriting disabled, returns query results for spelling original query string, returns suggestions for misspelled query terms.
    - `SearchCorrect`: Query rewriting enabled, returns query results for spelling corrected query string, returns suggestions for misspelled query terms.
    - `SuggestOnly`: Query rewriting disabled, returns no query results, only suggestions for misspelled query terms.
  - The spelling correction of multi-term query strings handles three cases:
    1. mistakenly inserted space into a correct term led to two incorrect terms: `hels inki` -> `helsinki`
    2. mistakenly omitted space between two correct terms led to one incorrect combined term: `modernart` -> `modern art`
    3. multiple independent input terms with/without spelling errors: `cinese indastrialication` -> `chinese industrialization`

### Changed

- lazy_static! replaced with LazyLock.
- Improvements regarding issue #15 (library documentation):
  - Documentation tests (doctest) enabled and code examples fixed.
  - README.md and FACETED_SEARCH.md included in documentation tests (doctest) to ensure that code examples are always correct and up-to-date.

### Fixed

- PR #52 fixes compile on macOS.

## [1.0.0] - 2025-10-22

### Improved

- 5x faster indexing speed with sharded index: lock-free concurrent indexing with document paritioned index shards (enables 100% processor cores saturation).
- 4x shorter query latency with intra-query concurrency.
- Faster index loading.
- Faster clear_index.
- Benchmarks updated.

### Added

- index.indexed_doc_count()
- index.committed_doc_count()
- index.uncommitted_doc_count()
- SchemaField has now `longest` property. This allows to annotate (manually set) the longest field in schema.  
  Otherwise the longest field will be automatically detected in first index_document.
  Setting/detecting the longest field ensures efficient index encoding.

### Changed

- The index is now organized in document paritioned shards, allowing lock-free, concurrent index_document() threads distributed over shards.
- New create_index parameter `force_shard_number`: allows to set the number of shards. If None, then the number of physical processor cores is used.
- Incompatible index format (INDEX_FORMAT_VERSION_MAJOR changed).
- Errror message in console when loading index with incompatible index format at server start.
- no re-initialization on every commit: strip0.positions_compressed = vec![0; MAX_POSITIONS_PER_TERM * 2];
- segment.segment = AHashMap::with_capacity(500);
- multithreading within index_document() removed.
- Index.index_option references to parent index from shards.
- open_index error handling refactored.

- fn create_index() -> Index changed to async fn create_index() -> IndexArc
- index.close_index (sync) -> index_arc.close().await (async).
- index_arc.close() disconnects index_file_mmap from index_file, otherwise we cannot reuse the file (e.g. with open_index) when the program is still running.
- unit tests fixed for multi-sharded index.
- get_document changed from sync to async.
- clear_index() from sync to async.
- get_file changed from sync to async.
- get_facet_value from sync to async.

## [0.14.1] - 2025-10-12

### Improved

- Faster tokenizer and indexing.

## [0.14.0] - 2025-09-12

### Improved

- Maximum cardinality of distinct string facet values increased from 65_535 (16 bit) to 4_294_967_295 (32 bit). 
  - FieldType::String32 and FieldType::StringSet32 added, that allow a cardinality of 4_294_967_295 (32 bit) distinct string facet values,  
    while FieldType::String and FieldType::StringSet were renamed to FieldType::String16 and FieldType::StringSet16 that allow only a cardinality of 65_535 (16 bit) distinct string facet values, but are space-saving. 
  - QueryFacet::String32 and QueryFacet::StringSet32 added, that allow a cardinality of 4_294_967_295 (32 bit) distinct string facet values,  
    while QueryFacet::String and QueryFacet::StringSet were renamed to QueryFacet::String16 and QueryFacet::StringSet16 that allow only a cardinality of 65_535 (16 bit) distinct string facet values, but are space-saving. 
  - FacetFilter::String32 and FacetFilter::StringSet32 added, FacetFilter::String and FacetFilter::StringSet renamed to FacetFilter::String16 and FacetFilter::StringSet16.  
  - FilterSparse::String32 and FilterSparse::StringSet32 added, FilterSparse::String and FilterSparse::StringSet renamed to FilterSparse::String16 and FilterSparse::StringSet16

### Changed

- Index format changed (INDEX_FORMAT_VERSION_MAJOR changed).

## [0.13.3] - 2025-08-26

### Fixed

- hash32 fixed for platforms without aes or sse2.

## [0.13.2] - 2025-08-25

### Added

- rustdocflags added in config.toml and cargo.toml

## [0.13.1] - 2025-08-23

### Improved

- Faster and complete topk results for union queries > 8 terms by using MAXSCORE.

### Fixed

- Required target_features for using gxhash fixed.

## [0.13.0] - 2025-08-08

### Added

- N-gram indexing: N-grams are indexed in addition to single terms, for faster phrase search, at the cost of higher index size.
  - N-grams not as parts of terms, but as combination of consecutive terms. See [NGRAM_SEARCH.md](https://github.com/SeekStorm/SeekStorm/blob/main/NGRAM_SEARCH.md).  
  - N-Gram indexing improves **phrase** query latency on average by factor **2.14 (114%)**, maximum tail latency by factor **7.51 (651%)**, and some phrase queries up to **3 orders of magnitude**.
  - Allows to enable a combination of different types of N-gram indexing: see NgramSet  
    - SingleTerm 
    - NgramFF  : frequent frequent
    - NgramFR  : frequent rare
    - NgramRF  : rare frequent
    - NgramFFF : frequent frequent frequent
    - NgramRFF : rare frequent frequent
    - NgramFFR : frequent frequent rare
    - NgramFRF : frequent rare frequent
  - Previously N-gram indexing was not configurable, but always set to the equivalent of NgramFF.
  - IndexMetaObject.ngram_indexing property added, used in create_index library method. 
  - CreateIndexRequest ngram_indexing property added, used in create_index REST API endpoint.
  - Ngram indexing only effects phrase search.
  - BM25 scores (SimilarityType::Bm25f) are almost identical for both ngram and single term indexing. There are only small differences for phrase search resulting from  
    normalization (32bit->8bit->32bit lossy logarithmic compression/decompression) that is used for posting_count_ngram1/2/3, but not for single term posting_counts.
  - Default ngram_indexing: NgramSet::NgramFF as u8 | NgramSet::NgramFFF as u8,

### Improved

- MAX_QUERY_TERM_NUMBER increased from 10 to 100.
- 2-term union count latency improved.
- DOCUMENT_LENGTH_COMPRESSION array now pre-calculated algorithmically with byte4_to_int instead of pre-defined values.
- faster document length compression with int_to_byte4 instead of norm_frequency (binary search in DOCUMENT_LENGTH_COMPRESSION table).
- int_to_byte4 is used also for compression of n-gram frequent_term positions_count (previously only for doc/field length compression) 
- 256 limit for the maximum number of frequentwords (FrequentwordType::Custom) removed (because frequentword_index is not stored anymore).

### Changed

- Index format changed (INDEX_FORMAT_VERSION_MAJOR changed).
  - Instead of u8 index to frequentword_posting_counts we now store the u8 compressed posting_count both for frequent and rare Ngram terms.
  - AHash replaced with GxHash, which is faster and provides stable hashes across different dependency versions, platforms and hardware. This improves index persistence and portability.
  - NgramType encoded into hash.
  - Ngrams with 3 terms allowed.

- in compress_postinglist posting_count_ngram1/2 are taken from decode_posting_list_counts instead from precalculated frequentword_posting_counts. 
  - update_frequentword_posting_counts removed.
  - precondition for ngrams with rare terms.

### Fixed

- Error in manual commit during intermittent indexing fixed: "Unable to index_file.set_len in commit".
- Realtime search BM25 scoring fixed: posting_counts are now based on the sum of committed and uncommitted documents (previously only uncommitted).
- Realtime search BM25 scoring fixed: now both terms of the ngram are taken into account.

## [0.12.27] - 2025-05-14

### Fixed

- very rare position compression bug fixed.

## [0.12.26] - 2025-05-13

### Fixed

- Put winapi crate behind conditional compilation #[cfg(target_os = "windows")]

## [0.12.25] - 2025-05-12

### Improved

- Faster index_document, commit, clear_index: 
  Increased SEGMENT_KEY_CAPACITY prevents HashMap resizing during indexing.
  vector/hashmap reuse instead of reinitialization.

### Fixed

- Intersection between RLE-RLE and RLE-Bitmap compressed posting lists fixed.

## [0.12.24] - 2025-05-02

### Fixed

- Fixes a 85% performance drop (Windows 11 24H2/Intel hybrid CPUs only) caused by a faulty Windows 11 24H2 update,  
  that changed the task scheduler behavior into under-utilizing the P-Cores over E-cores of Intel hybrid CPUs.
  This is a workaround until Microsoft fixes the issue in a future update.
  The fix solves the issue for the SeekStorm server, if you embedd the SeekStorm library into your own code you have to apply the fix as well.
  See blog post for details: https://seekstorm.com/blog/80-percent-performance-drop/

## [0.12.23] - 2025-04-28

### Added

- Ingestion of files in [CSV](https://en.wikipedia.org/wiki/Comma-separated_values), SSV, TSV, PSV format with `ingest_csv()` method and seekstorm_server command line `ingest`:  
  configurable header, delimiter char, quoting, number of skipped document, number of indexed documents.
- `stop_words` parameter (predefined languages and custom) added to create_index IndexMetaObject:  
  Stop words are not indexed for compact index and faster queries.
- `frequent_words` parameter (predefined languages and custom) added to create_index IndexMetaObject:  
  consecutive frequent words are indexed as n-gram combinations for short posting lists and fast phrase queries.
- TokenizerType `Whitespace` and `WhitespaceLowercase` added.
- `truncate()` and `substring()` utils.

## [0.12.22] - 2025-04-16

### Fixed

- Problem fixed where an intersection in an very small index didn't return results (all_terms_frequent).
- Early termination fixed in single_blockid: did not guarantee most relevant results for filtered single term queries with result type Topk.

## [0.12.21] - 2025-03-27

### Added

- Stemming for 18 languages added: new property `IndexMetaObject.stemmer: StemmerType` 
- Allow to specify the markup tags to insert **before** and **after** each highlighted term. Default is "<b>" "</b>" (by @DanLLC).

### Changed

- Updated [OpenAPI documents](https://github.com/SeekStorm/SeekStorm/tree/main/src/seekstorm_server) directory.

### Fixed

- Fixed read_f32() in utils.rs

## [0.12.20] - 2025-03-05

### Changed

- PDF ingestion via `pdfium` dependency moved behind a new `pdf` feature flag which is enabled by default.  
  You can disable the SeekStorm default features by using `seekstorm = { version = "0.12.19", default-features = false }` in the cargo.toml of your application.  
  This can be useful to reduce the size of your application or if there are dependency version conflicts.
- feature flags documented in README.md

## [0.12.19] - 2025-03-04

### Fixed

- Backward compatibility to indexes created prior v0.12.18 restored.

## [0.12.18] - 2025-03-03

### Fixed

- Fixes intersection_vector16 for target_arch != "x86_64".
- Fixes issue where multiple indices per API key were not correctly reloaded after server restart (IndexMetaObject.id #[serde(skip)] removed).
- Fixes issue #39 with commandline() in server.rs for docker environment without -ti parameter (run interactively with a tty session).

### Changed

- Updated to Rust edition 2024.
- Changed serde_json::from_str(&value.to_string()).unwrap_or(value.to_string()).to_string() -> serde_json::from_value::<String>(value.clone()).unwrap_or(value.to_string())

## [0.12.17] - 2025-02-15

### Fixed

- Fixed issue in clear_index.

## [0.12.16] - 2025-02-14

### Added

- Basic tests added (issue #33): cargo test
- New method current_doc_count() returns the number of indexed documents - deleted documents.

### Fixed

- Fixed issue #32 in clear_index.

## [0.12.15] - 2025-02-12

### Fixed

- Fixed issue #34 - refactoring of http_server (by @gabriel-v)

## [0.12.14] - 2025-02-10

### Fixed

- Fixed issue #36 panic at realtime search
- Fixed a possible issue in clear_index

## [0.12.13] - 2025-02-08

### Improved

- Intersection speed for ResultType::Count improved.

### Fixed

- Fixes issue #31 for queries with query parameter length=0 and ResultType::TopK or ResultType::TopkCount. 
  - If you specify length=0, resultType::TopkCount will automatically downgraded to resultType::Count and return the number of results only, without returning the results itself.
  - If you don't specify the length in the REST API, a default of 10 will be used.

## [0.12.12] - 2025-02-07

### Fixed

- Fixes an issue in clear_index that prevented the facet.json file from being created in commit after clear_index, causing problems after reloading the index. Fixes issue #27.

## [0.12.11] - 2025-02-03

### Fixed

- clear_index fixed. Fixes issue #26 .

## [0.12.10] - 2025-02-01

### Fixed

- Fixed indexing postings with more than 8_192 positions.
- Fixed an issue with Chinese word segmentation, where a hyphen within a string was interpreted as a NOT ('-') operator in front of one of the resulting segmented words.
- Fixed an issue with NOT query terms that are RLE compressed.
- Fixed an issue for union > 8 terms with custom result sorting.

## [0.12.9] - 2025-01-29

### Fixed

- Automatic resize of postings_buffer in index_posting.
- Fixed a subtract with overflow exception when real time search was enabled.
- Fixed exception if > 10 query terms.
- Fixed stack overflow in some long union queries.
- Fixed endless loop while intersecting RLE-compressed posting lists.
- Updated rand from v0.8.5 to v0.9.0.

## [0.12.8] - 2025-01-24

### Fixed

- Removed unsafe std::slice::from_raw_parts to cast arrays of different element types, which caused unaligned data exceptions.

## [0.12.7] - 2025-01-19

### Fixed

- Endless loop while intersecting multiple RLE-compressed posting lists fixed. Fixes issue #21 .

## [0.12.6] - 2025-01-16

### Fixed

- Exception while intersecting multiple RLE-compressed posting lists fixed. Fixes issue #21 .

## [0.12.5] - 2025-01-14

### Fixed

- Endless loop while intersecting multiple RLE-compressed posting lists fixed. Fixes issue #21 .

## [0.12.4] - 2025-01-12

### Fixed

- Fixed a subtract with overflow exception that occurred in debug mode when committing, and the previous commit was < 64k documents. Fixes issue #22 .
- Changed cast_byte_ushort_slice and cast_byte_ulong_slice to either take mutable references and return mutable ones or take immutable references and return immutable ones.
- Added index_file and docstore_file flush in commit.

## [0.12.3] - 2024-12-21

### Added

- Docker file and container added (#17).
- https://hub.docker.com/r/wolfgarbe/seekstorm_server
- `docker run -ti -p "8000:80" wolfgarbe/seekstorm_server:v0.12.3`
- Added a server welcome web page with instructions how to create an API key and index.

### Fixed

- Exception handling if docker is run without the -ti parameter (run interactively with a tty session).

## [0.12.2] - 2024-12-14

### Changed

- hyper crate upgraded from v0.14.31 to v1.5.1

## [0.12.1] - 2024-12-12

### Added

- Delete Documents(s) REST API endpoint now supports deleting all documents in the index (clear index).

### Changed

- REST API documentation improved.
- Library documentation improved.
- All public methods, structs, enums and properties got document comments. 
- Stricter linting: missing_docs, unused_import_braces, trivial_casts, trivial_numeric_casts, unused_qualifications.

## [0.12.0] - 2024-12-11

### Added

- Code first OpenAPI documentation generation added for SeekStorm server REST API. 
- New console command `openapi` to create `openapi.json` and `openapi.yml`.
- Pregenerated [openapi files](https://github.com/SeekStorm/SeekStorm/tree/main/src/seekstorm_server) directory.
- SeekStorm server [REST API online documentation](https://seekstorm.apidocumentation.com/reference).
- Constructor for SchemaField added.

## [0.11.1] - 2024-12-05

### Changed

- In index_document skip tokenizing fields with !schema_field.indexed.
- New property SchemaField.field_id to indicate the field order in the schema map.
- In master.js, the fields in the schema map are now sorted by field_id to preserve the field order of the schema.json.
- In the Web UI, the preview panel is now always using 100% height, independent from the number of results in the result panel.

## [0.11.0] - 2024-11-28

### Added

- New tokenizer UnicodeAlphanumericZH (enable SeekStorm Cargo feature 'zh')
  - Implements Chinese word segmentation to segment continuous Chinese text into tokens for indexing and search.
  - Supports mixed Latin and Chinese texts
  - Supports Chinese sentence boundary chars for KWIC snippets ahd highlighting.
- get_synonyms, set_synonyms and add_synonyms library methods and REST API endpoints added 
  - Updated synonyms only affect subsequently indexed documents.

### Changed

- Commit now waits until all previously started index_posting have finished (multi-threading).

## [0.10.0] - 2024-11-25

### Added

- One-way and multi-way synonym definition per index added
  - Synonyms parameter added to create_index (both library and REST API).
  - Allows to define term synonyms per index.
  - Supports both one-way and multi-way synonyms.
  - Synonym support for result highlighting.
  - Currently only single term synonyms without spaces are supported.
- New server console command `create` to manually create a demo API key (`delete` to delete the demo API key and asociated indices).

## [0.9.0] - 2024-11-18

### Added

- PDF ingestion: Ingest PDF files and directories including sub-directories.  
- Ingest via console or REST API, search via Web UI or REST API.
- Stores original PDF file in index, served via REST API endpoint.
- Embedded PDF viewer in Web UI. 
- Result sorting in Web UI (Score/Date/Price/Distance acending/descending).  
- Numeric facet filter and histogram in Web UI (Date/Price/Distance...).  
- Min/Max numeric facet aggregation (Date/Price/Distance...).

- Library
  - `create_index` now creates a `files` subdirectory in index, to store copies of ingested PDF files.
  - new public method `ingest_pdf`: ingests a given pdf file or all pdf files in a given path, recursively.
  - new public methods `index_pdf_bytes` and `index_pdf_file`
    - converts pdf to text and indexes it
    - extracts title from metatag, or first line of text, or from filename
    - extracts creation date from metatag, or from file creation date (Unix timestamp: the number of seconds since 1 January 1970)
    - copy_file in index_pdf copies all ingested pdf files to "files" subdirectory in index
  - new public method `get_file`: gets a file from the "files" sub-directory in index
  - new FieldType `Timestamp`, identical to I64, but enables an UI to interpret I64 as timestamp without resorting to a specific field name as indicator.
  - new min/max value detection for numeric facet fields.
  - new public method `get_index_facets_minmax` (min/max aggregation values of numerical facet fields).

- Server
  - new REST API endpoint `index_file`: POST api/v1/index/{index_id}/file/{doc_id} 
    calls index_file_api, index_pdf_bytes and indexes the PDF file in the specified api_key and index_id
  - new REST API endpoint `get_file`:   GET  api/v1/index/{index_id}/file/{doc_id}
    calls get_file_api, get_file and returns the PDF file associated with the doc_id in the specified api_key and index_id with "Content-Type", "application/pdf"
  - REST API error messages added
  - Web UI automatically uses one of three options for result preview: 
    - result links to public web URL (e.g. Wikipedia articles) or
    - fields with path to private PDF documents via REST API (e.g. PDF ingestion) or
    - text fields.
  - WEB UI automatically displays both PDF previews (PDF ingestion) and text previews (JSON ingestion) when hovering with the mouse over the result list.
  - WEB UI now with result sort: score/newest/oldest.
  - seekstorm readme updated ([PDF search demo](https://github.com/SeekStorm/SeekStorm/?tab=readme-ov-file#build-a-pdf-search-engine-with-the-seekstorm-server)). 
  - seekstorm_server readme updated ([Console commands](https://github.com/SeekStorm/SeekStorm/blob/main/src/seekstorm_server/README.md#console-commands)).
  - If the server console command `ingest` is used with a single path parameter only, then default demo API key and index_id=0 are used.
  - New [command line parameter](https://github.com/SeekStorm/SeekStorm/tree/main/src/seekstorm_server#command-line-parameters) `ingest_path` to set the default path for data files to ingest with the console command `ingest`, if entered without absolute path/filename.
  - `get_index_stats_api` returns now `facets_minmax` property from `get_index_facets_minmax` (min/max aggregation values of numerical facet fields).
  - `SearchResultObject.facets` changed from `Vec<Facet>` to `HashMap<String,Facet>` (`HashMap<field_name,Vec<facet_label,facet_count>>`)

## [0.8.0] - 2024-10-30

### Added

- Help for server console commands added
- Added a hint of color to console messages
- Preparation for ingest of local files in PDF format (WIP).

### Changed

- TokenizerType::UnicodeAlphanumeric and TokenizerType::UnicodeAlphanumericFolded now allow '+' '-' '#' in middle or end of a term: c++, c#, block-max, top-k

### Fixed

- query_type_default for server REST API and for library fixed
- multifield decoding case fixed: three embedded fields with each a single position
- Checks if path/file exists for ingest console command

## [0.7.6] - 2024-10-25

### Changed

- Set default query_type to Intersection for REST API, if not specified in SearchRequestObject
- Set default similarity_type to Bm25fProximity for REST API, if not specified in CreateIndexRequest
- Set default tokenizer_type to UnicodeAlphanumeric for REST API, if not specified in CreateIndexRequest

## [0.7.5] - 2024-10-24

### Fixed

- Indexing of documents with > 2 fields fixed

## [0.7.4] - 2024-10-22

### Fixed

- Search with more than 2 indexed fields fixed

## [0.7.3] - 2024-10-18

### Fixed

- Fixed BM25 component precalculation for get_bm25f_multiterm_multifield

## [0.7.2] - 2024-10-18

### Fixed

- Multi-platform build fixed

## [0.7.1] - 2024-10-18

### Fixed

- Parameters of intersection_vector16 method fixed for aarch64 build

## [0.7.0] - 2024-10-11

### Added

- The SeekStorm server now allows to ingest local data files in [JSON](https://en.wikipedia.org/wiki/JSON), [Newline-delimited JSON](https://github.com/ndjson/ndjson-spec) (ndjson), and [Concatenated JSON](https://en.wikipedia.org/wiki/JSON_streaming) format via console `ingest [data_filename] [api_key] [index_id]` command.  
  The document ingestion is streamed without loading the whole document vector into memory to allow for unlimited file size while keeping RAM consumption low.
- ingest_json() is also available as public method in the SeekStorm library.
- The [Embedded web UI](https://github.com/SeekStorm/SeekStorm/blob/main/src/seekstorm_server#open-embedded-web-ui-in-browser) of SeekStorm multi-tenancy server now allows to search and display results from any index in your web browser without coding.  
  The field names to display in the web UI can be automatically detected or pre-defined. 
- seekstorm_server readme updated ([src/seekstorm_server/README.md](https://github.com/SeekStorm/SeekStorm/blob/main/src/seekstorm_server/README.md#command-line-parameters)).
- Optionally specify the default query type (Union/Intersection) via REST API (Intersection is default if not specified).

### Changed

- jQuery v3.3.1 updated to v3.7.1 (used in server web UI).

## [0.6.2] - 2024-10-10

### Fixed

- Exception fixed that occurred when intersecting terms with RLE compressed posting list [(#5)](https://github.com/SeekStorm/SeekStorm/issues/5). 

## [0.6.1] - 2024-10-05

### Fixed

- Exception fixed that occurred when intersecting more than two terms, including two terms with RLE compressed posting list [(#5)](https://github.com/SeekStorm/SeekStorm/issues/5). 
- Exception fixed that occurred when searching terms with 65.536 postings per block. The fix requires reindexing if your index has been affected!

## [0.6.0] - 2024-09-30

### Added

- Range query facets for Point field type added: Calculating the distance between a specified facet field of type Point and a base Point, in kilometers or miles,  
  using Euclidian distance (Pythagoras theorem) with Equirectangular approximation. The distance ranges are equivalent to concentric circles of different radius around the base point.
  The numbers of matching results that fall into the defined range query facet buckets are counted and returned in the ResultObject facets property.

### Changed

- Filtering of facet field type Point changed from distance to distance range, to make behaviour equivalent to the other numerical field types.

### Fixed

- Search query_facets, facets_filter, result_sort parameter documentation fixed.

## [0.5.0] - 2024-09-25

### Added

- **New Unicode character folding/normalization tokenizer** (diacritics, accents, umlauts, bold, italic, full-width ...)  
  **'TokenizerType::UnicodeAlphanumericFolded'** and method **fold_diacritics_accents_zalgo_umlaut()** 
  convert text with **diacritics, accents, zalgo text, umlaut, bold, italic, full-width UTF-8 characters** into its basic representation.  
  Unicode UTF-8 has made life so much easier compared to the old code pages, but its endless possibilities also pose challenges in parsing and indexing.  
  The challenge is that the same basic letter might be represented by different UTF8 characters if they contain diacritics, accents, or are bold, italic, or full-width.  
  Sometimes, users can't search because the keyboard doesn't have these letters or they don't know how to enter, or they even don't know what that letter looks like.  
  Sometimes the document to be ingested is already written without diacritics for the same reasons.  
  We don't want to search for every variant separately, most often we even don't know that they exist in the index.  
  We want to have all results, for every variant, no matter which variant is entered in the query, 
  e.g. for indexing LinkedIn posts that make use of external bold/italic formatters or for indexing documents in accented languages.  
  It is important that the search engine supports the character folding rather than external preprocessing, as we want to have both:  
  **enter the query in any character form**, **receive all results independent from their character form**, but **have them returned in their original, unaltered characters**.
- **New apostroph handling** in **'TokenizerType::UnicodeAlphanumericFolded'** prevents that short meaningless term parts preceding or following the apostroph get indexed (e.g. "s" in "someone's") or become part of the query.

## [0.4.0] - 2024-09-20

### Added

- **Sorting of results by any facet field** stated in the result_sort search parameter, ascending or descending.  
  Multiple sort fields are combined by "sort by, then sort by" ("tie-breaking"-algorithm).  
  After all sort fields are considered, the results are sorted by BM25 score as last clause.  
  If no sort fields are stated, then the results are just sorted by BM25 score.
  Currently, sorting by fields is more expensive, as it prevents WAND search acceleration.

- **Geo proximity search**
  - New field type Point: array of 2 * f64 coordinate values (longitude and latitude) as defined in https://geojson.org.  
    Internally encoded into a single 64 bit Morton code for efficient range queries.  
    get_facet_value decodes Morton code back to Point.  
  - New public methods: encode_morton_2_d(point:&Point)->u64, decode_morton_2_d(code: u64)->Point,  
    euclidian_distance(point1: &Point, point2: &Point, unit: &DistanceUnit)->f64,  
    point_distance_to_morton_range(point: &Point,distance: f64,unit: &DistanceUnit)-> Range<u64>  
  - New property ResultSort.base that allows to specify a query base value (e.g. of type Point for current location coordinate) during search.  
    Then results won't be ordered by the field values but by the distance between the field value and query base value, e.g. for geo proximity search.
  - New parameter distance_fields: &Vec<DistanceField> for the get_document method allows to insert distance fields into result documents,  
    calculating the distance between a specified facet field of type Point and a base Point, in kilometers or miles,  
    using Euclidian distance (Pythagoras theorem) with Equirectangular approximation.
  - SearchRequestObject in query_index_api and GetDocumentRequest in get_document_api (server REST API) have a new property distance_fields: Vec<DistanceField>.

- **get_facet_value()** : Returns value from facet field for a doc_id even if schema stored=false (field not stored in document JSON).  
  Facet fields are more compact than fields stored in document JSON.
  Facet fields are faster because no document loading, decompression and JSON decoding is required.  
  Facet fields are always memory mapped, internally always stored with fixed byte length layout.
  Facet fields are instantly updatable without re-indexing the whole document.

## [0.3.0] - 2024-08-26

### Added

- Multi-value string facets added (FieldType::StringSet)

### Improved

- get_index_string_facets refactored

### Fixed

- facet_filter field not found exception handling

## [0.2.0] - 2024-08-21

### Added

- String & Numeric Range Facets implemented: Counting and filtering of matching results.
- Supports u8, u16, u32, u64, i8, i16, i32, i64, f32, f64, String types for facet.
- Numeric RangeType CountAboveRange/CountBelowRange/CountWithinRange implemented.
- Schema has a new field property: "field_facet":true (default=false).
- Index has a new get_index_string_facets method: returns a list of facet fields, each with field name and a list of unique values and their count (number of times a specific value appears in the whole index).
- The search ResultObject now has a facets property. Returns index facets (value appears in the whole index) if querystring.is_empty, otherwise query facets (values appear in docs that match the query).
- [FACETED_SEARCH.md](https://github.com/SeekStorm/SeekStorm/blob/main/FACETED_SEARCH.md) : Introduction to faceted search

### Improved

- BM25 component precalculation.
- Query performance improved.
- Documentation improved.
- ARCHITECTURE.md updated.

### Fixed

- Result count for unions fixed if the index contained deleted documents.

### Changed

- Schema (breaking change: properties renamed)

## [0.1.18] - 2024-07-30

### Added

- delete_document, delete_documents, delete_documents_by_query, update_document, update_documents implemented, both for library and server REST API (single docid/document and vector of docid/document)
- Query index REST API now returns also _id and _score fields for each result
- Documentation updated
- test_api.rest updated
- SearchRequestObject contains result_type parameter (Count, Topk, TopkCount)

### Fixed

- Fixed count_total for realtime search with field filters

## [0.1.17] - 2024-07-18

### Changed

- "detect longest field id" only if index_mut.indexed_field_vec.len()>1
- ResultListObject.result_count_estimated (count) removed
- ResultListObject.result_count_total i64 -> usize; serde rename countEvaluated removed
- count_evaluated renamed to count_total in master.js and api_endpoints.rs
- early termination invoked earlier in intersection_blockid
- sort_by -> sort_unstable_by
- Normal intersection with galloping when SIMD not supported for the architecture.

## [0.1.16] - 2024-07-16

### Added

- Add support for processor architectures other than x86 (i.e. Apple Silicon) (#1)

## [0.1.15] - 2024-06-14

### Improved

- Query performance improved.
- Documentation improved.
- New mute parameter for open_index and create_index: prevents emitting of status messages, e.g. when using pipes. 

### Changed

- commit changed from Index/sync to IndexArc/async

## [0.1.14] - 2024-06-12

### Fixed

- Fix corruption of a committed document that could occur under specific conditions.

## [0.1.13] - 2024-06-03

### Fixed

- Intermittent indexing with multiple commits of incomplete levels (< 65_536 documents) fixed
- get_document of uncommitted docs with a docid within the 64k range of an already committed incomplete level fixed
- Result count for searches that both include results from uncommitted and committed documents fixed
- Server handles empty index directories (manually deleted files) gracefully

### Improved

- Index compression ratio improved

## [0.1.12] - 2024-03-25

### Fixed

- Indexing (field_indexed:true) and highlighting (field_stored:true, highlights:... ) number fields (e.g. field_type="I64") fixed

### Changed

- REST API examples in test_api.rest updated

## [0.1.11] - 2024-03-24

### Added

- get_document REST API endpoint with highlights and fields parameter implemented

### Changed

- REST API examples in test_api.rest updated
- Increased REST API robustness
- Server documentation for REST API endpoints improved (readme + doc)
- REST API query index via GET now supports alternatively url query parameter (only query,offst,lengh,realtime) or JSON request object (all parameters)

## [0.1.10] - 2024-03-21

### Fixed

- Realtime search fixed
- REST API examples fixed

## [0.1.9] - 2024-03-20

### Added

- Highlighter parameter added to get_document.
highlighter generates fragments (snippets, summaries) from each specified field to provide a "keyword in context" (KWIC) functionality.
With highlight_markup the matching query terms within the fragments can be highlighted with HTML markup.
- Fields parameter added to get_document.
fields allows to specify at query time which stored fields to return 

### Fixed

- Server REST API exception handling fixed
- REST API examples fixed

### Changed

- More verbose REST API status message
- Documentation updated

### Removed
