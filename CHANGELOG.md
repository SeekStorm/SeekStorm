# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2024-08-01

### Added

- String & Numeric Range Facets implemented: Counting and filtering of matching results.
- Supports u8, u16, u32, u64, i8, i16, i32, i64, f32, f64, String types for facet.
- Numeric RangeType CountAboveRange/CountBelowRange/CountWithinRange implemented.
- Schema has a new field property: "field_facet":true (default=false).
- Index has a new get_index_string_facets method: returns a list of facet fields, each with field name and a list of unique values and their count (number of times a specific value appears in the whole index).
- The search ResultObject now has a facets property. Returns index facets (value appears in the whole index) if querystring.is_empty, otherwise query facets (values appear in docs that match the query).
- [FACETED_SEARCH.md](https://github.com/SeekStorm/SeekStorm/blob/master/FACETED_SEARCH.md) : Introduction to faceted search

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
