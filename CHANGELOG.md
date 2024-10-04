# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.1] - 2024-10-05

### Fixed

- Exception fixed that occurred when intersecting more than two terms, including two terms with RLE compressed posting list [(#5)](https://github.com/SeekStorm/SeekStorm/issues/5). 

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
