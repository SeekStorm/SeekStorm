# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


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
