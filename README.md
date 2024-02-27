# SeekStorm

SeekStorm is an open-source, sub-millisecond full-text search library & multi-tenancy server implemented in Rust.

Scalability and performance are our fundamental design goals. Index size and latency grow linearly with the number of indexed documents, while the RAM consumption remains constant, ensuring scalability.

Development started in 2015, in [production](https://seekstorm.com) since 2020, rust porting in 2023, released as open source in 2024.

SeekStorm is open source licensed under under the [Apache License 2.0](https://github.com/SeekStorm/SeekStorm?tab=Apache-2.0-1-ov-file#readme)

### SeekStorm is not only a high-performance search library, but also multi-tenancy search server 

  * with RESTful API
  * multi-tenancy index management
  * API-key management
  * embedded web server and UI

### Why SeekStorm?

**Performance**  
Lower latency, higher througput, lower cost and energy consumption, especially for multi-field and concurrent queries

**Relevance**  
Term proximity ranking provides more relevant results compared to BM25

**Real-time**  
True real-time search, as opposed to NRT: every indexed document is immediately searchable

<img width="600" src="https://miro.medium.com/v2/resize:fit:4800/format:webp/1*7IvVqlClkfHvmywr4hyofw.jpeg" alt="Benchmark">
<br>
<br>
<img width="600" src="https://miro.medium.com/v2/resize:fit:4800/format:webp/1*GQ0wk-MghqXhO3Sa3Hehdw.jpeg" alt="Benchmark">
<br>
<br>
<img width="600" src="https://miro.medium.com/v2/resize:fit:4800/format:webp/1*YpkRW-_bbltmyLgTKkutOA.jpeg" alt="Benchmark">

### Benchmark

https://seekstorm.com/blog/sneak-peek-seekstorm-rust/

### Why latency matters

* Search speed might be good enough for a single search. Below <10 ms people can't tell latency anymore. Search latency might be small compared to internet network latency.
* But search engine performance still matters when used in a server or service for many concurrent users and requests for maximum scaling, and throughput, and low processor load, cost.
* With performant search technology you can serve many concurrent users, at low latency with fewer servers, less cost, and less energy consumption, lower carbon footprint.
* It also ensures low latency even for complex and challenging queries: instant search, fuzzy search, faceted search, and union/intersection/phrase of very frequent terms.
* Besides average latencies we also need to reduce tail latencies, which are often overlooked, but can cause loss of customers, and revenue and can cause a bad user experience.
* It is always advisable to engineer your search infrastructure with enough performance headroom, to keep those tail latencies in check, even on periods of high concurrent load.
* Also, even if a human user might not notice the latency, it still might make a big difference in autonomous stock market, defense applications or RAG which requires multiple queries.

### Features

* Full-text search
* true real-time search, with negligible performance impact
* incremental indexing
* multithreaded indexing & search
* unlimited field number, field length & index size
* compressed document store: ZStandard
* boolean queries: AND, OR, PHRASE, NOT
* field filtering
* BM25F and BM25F_Proximity ranking
* KWIC snippets, highlighting
* billion-scale index
* Language independent
* API keys
* RESTful API with CORS

Query types
+ OR  disjunction  union
+ AND conjunction intersection
+ ""  phrase
+ \-   NOT

Result types
+ TopK
+ Count
+ TopKCount


### Why keyword search remains a core building block in the advent of vector search and LLMs

Despite what the hype-cycles https://www.bitecode.dev/p/hype-cycles want you to believe, keyword search is not dead, as NoSQL wasn't the death of SQL.

You should maintain a toolbox, and choose the best tool for your task at hand. https://seekstorm.com/blog/vector-search-vs-keyword-search1/

Keyword search is just a filter for a set of documents, returning those where certain keywords occur in, usually combined with a ranking metric like BM25.
A very basic and core functionality, that is very challenging to implement at scale with low latency.
Because the functionality is so basic, there is an unlimited number of application fields.
It is a component, to be used together with other components.
There are uses cases which can be solved better today with vector search and LLMs, but for many more keyword search is still the best solution.
Keyword search is exact, lossless, and it is very fast, with better scaling, better latency, lower cost and energy consumption.
Vector search works with semantic similarity, returning results within with a given proximity and probability. 

#### Keyword search (lexical search)
If you search for exact results like proper names, numbers, license plates, domain names, and phrases (e.g. plagiarism detection) then keyword search is your friend. Vector search on the other hand will bury the exact result that you are looking for among a myriad results that are only somehow semantically related. At the same time, if you don’t know the exact terms, or you are interested in a broader topic, meaning or synonym, no matter what exact terms are used, then keyword search will fail you.

* high indexing speed (for large document numbers)
* smaller index size
* high query speed and throughput (for large document numbers)
* lower infrastructure cost per document and per query, lower energy consumption
* good scaling (for large document numbers)
* perfect precision (for exact keyword match)
* recall: perfect for exact keyword match, low for semantic meaning
* unable to capture meaning and similarity
* efficient and lossless for exact keyword and phrase search

#### Vector search
Vector search is perfect if you don’t know the exact query terms, or you are interested in a broader topic, meaning or synonym, no matter what exact query terms are used. But if you are looking for exact terms, e.g. proper names, numbers, license plates, domain names, and phrases then you should always use keyword search. Vector search will but bury the exact result that you are looking for among a myriad results that are only somehow related.
It has a good recall, but low precision, and higher latency. It is prone to false positives, e.g. in in plagiarism detection as exact words and word order get lost.

Vector search enables you to search not only for similar text, but everything that can be transformed to a vector: text, images, audio ...
It works for similar images, face recognition or finger prints and it enables you to do magic things like queen - woman + man = king

* slower indexing speed (for large document numbers)
* larger index size
* lower query speed and throughput (for large document numbers)
* higher infrastructure cost per document and per query, higher energy consumption
* limited scaling (for large document numbers)
* lower precision (for exact keyword match)
* recall: high for semantic meaning (80/90%), medium for exact keyword match
* able to capture meaning and similarity
* inefficient and lossy for exact keyword and phrase search


### Why Rust

We have (partially) ported the SeekStorm codebase from C# to Rust
+ Factor 2..4x performance gain (latency and throughput)
+ No slow first run (no cold start costs due to just-in-time compilation)
+ Stable latencies (no garbage collection delays)
+ Less memory consumption (no ramping up until the next garbage collection)
+ No framework dependencies (CLR or JVM virtual machines)
+ Ahead-of-time instead of just-in-time compilation
+ Memory safe language https://www.whitehouse.gov/oncd/briefing-room/2024/02/26/press-release-technical-report/ 
- Steep learning curve (previously development time divided between designing algorithms and debugging, now between translating and fighting with the compiler borrow checker)

Rust is great for performance-critical applications 🚀 that deal with big data and/or many concurrent users. 
Fast algorithms will shine even more with a performance-conscious programming language 🙂

### Architecture

see [ARCHITECTURE.md](https://github.com/SeekStorm/SeekStorm/blob/main/ARCHITECTURE.md) 
