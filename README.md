SeekStorm<br>
[![Crates.io](https://img.shields.io/crates/v/seekstorm.svg)](https://crates.io/crates/seekstorm)
[![Downloads](https://img.shields.io/crates/d/seekstorm.svg?style=flat-square)](https://crates.io/crates/seekstorm)
[![Documentation](https://docs.rs/seekstorm/badge.svg)](https://docs.rs/seekstorm)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/SeekStorm/SeekStorm?tab=Apache-2.0-1-ov-file#readme)
========

**SeekStorm** is an **open-source, sub-millisecond full-text search library** & **multi-tenancy server** implemented in **Rust**.

Development started in 2015, in [production](https://seekstorm.com) since 2020, Rust port in 2023, open sourced in 2024, work in progress.

SeekStorm is open source licensed under under the [Apache License 2.0](https://github.com/SeekStorm/SeekStorm?tab=Apache-2.0-1-ov-file#readme)

Blog Posts: https://seekstorm.com/blog/sneak-peek-seekstorm-rust/

### SeekStorm high-performance search library

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
* Index either in RAM or memory mapped files

Query types
+ OR  disjunction  union
+ AND conjunction intersection
+ ""  phrase
+ \-   NOT

Result types
+ TopK
+ Count
+ TopKCount

### SeekStorm multi-tenancy search server 

  * with RESTful API
  * multi-tenancy index management
  * API-key management
  * embedded web server and UI
  * Cross-platform: runs on Linux and Windows (other OS untested)

---

## Why SeekStorm?

**Performance**  
Lower latency, higher throughput, lower cost & energy consumption, esp. for multi-field and concurrent queries.  
Low tail latencies ensure a smooth user experience and prevent loss of customers and revenue.

**Scaling**  
Maintains low latency, high throughput, and low RAM consumption even for billion-scale indices.  
Unlimited field number, field length & index size.

**Relevance**  
Term proximity ranking provides more relevant results compared to BM25.

**Real-time**  
True real-time search, as opposed to NRT: every indexed document is immediately searchable.

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

---

## Keyword search remains a core building block in the advent of vector search and LLMs

Despite what the hype-cycles https://www.bitecode.dev/p/hype-cycles want you to believe, keyword search is not dead, as NoSQL wasn't the death of SQL.

You should maintain a toolbox, and choose the best tool for your task at hand. https://seekstorm.com/blog/vector-search-vs-keyword-search1/

Keyword search is just a filter for a set of documents, returning those where certain keywords occur in, usually combined with a ranking metric like BM25.
A very basic and core functionality, that is very challenging to implement at scale with low latency.
Because the functionality is so basic, there is an unlimited number of application fields.
It is a component, to be used together with other components.
There are uses cases which can be solved better today with vector search and LLMs, but for many more keyword search is still the best solution.
Keyword search is exact, lossless, and it is very fast, with better scaling, better latency, lower cost and energy consumption.
Vector search works with semantic similarity, returning results within with a given proximity and probability. 

### Keyword search (lexical search)
If you search for exact results like proper names, numbers, license plates, domain names, and phrases (e.g. plagiarism detection) then keyword search is your friend. Vector search on the other hand will bury the exact result that you are looking for among a myriad results that are only somehow semantically related. At the same time, if you don’t know the exact terms, or you are interested in a broader topic, meaning or synonym, no matter what exact terms are used, then keyword search will fail you.

```diff
- works with text data only
- unable to capture context, meaning and semantic similarity
- low recall for semantic meaning
+ perfect recall for exact keyword match 
+ perfect precision (for exact keyword match)
+ high query speed and throughput (for large document numbers)
+ high indexing speed (for large document numbers)
+ incremental indexing fully supported
+ smaller index size
+ lower infrastructure cost per document and per query, lower energy consumption
+ good scalability (for large document numbers)
+ perfect for exact keyword and phrase search, no false positives
+ perfect explainability
+ efficient and lossless for exact keyword and phrase search
+ works with new vocabulary out of the box
+ works with any language out of the box
+ works perfect with long-tail vocabulary out of the box
+ works perfect with any rare language or domain-specific vocabulary out of the box
+ RAG (Retrieval-augmented generation) based on keyword search offers unrestricted real-time capabilities.
```


### Vector search
Vector search is perfect if you don’t know the exact query terms, or you are interested in a broader topic, meaning or synonym, no matter what exact query terms are used. But if you are looking for exact terms, e.g. proper names, numbers, license plates, domain names, and phrases (e.g. plagiarism detection) then you should always use keyword search. Vector search will but bury the exact result that you are looking for among a myriad results that are only somehow related. It has a good recall, but low precision, and higher latency. It is prone to false positives, e.g. in in plagiarism detection as exact words and word order get lost.

Vector search enables you to search not only for similar text, but everything that can be transformed to a vector: text, images (face recognition, finger prints), audio and it enables you to do magic things like queen - woman + man = king.

```diff
+ works with any data that can be transformed to a vector: text, image, audio ...
+ able to capture context, meaning, and semantic similarity
+ high recall for semantic meaning (90%)
- lower recall for exact keyword match
- lower precision (for exact keyword match)
- lower query speed and throughput (for large document numbers)
- lower indexing speed (for large document numbers)
- incremental indexing is expensive and requires rebuilding the entire index periodically, which is extremely time-consuming and ressource intensive.
- larger index size
- higher infrastructure cost per document and per query, higher energy consumption
- limited scalability (for large document numbers)
- unsuitible for exact keyword and phrase search, many false positives
- low explainability makes it difficult to spot manipulations, bias and root cause of retieval/ranking problems
- inefficient and lossy for exact keyword and phrase search
- Additional effort and cost to create embeddings and keep them updated for every language and domain. Even if the number of indexed documents is small, the embeddings have to created from a large corpus before nevertheless.
- Limited real-time capability due to limited recency of embeddings
- works only with vocabulary known at the time of embedding creation
- works only with the languages of the corpus from which the embeddings have been derived
- works only with long-tail vocabulary that was sufficiently represented in the corpus from which the embeddings have been derived
- works only with rare language or domain-specific vocabulary that was sufficiently represented in the corpus from which the embeddings have been derived
- RAG (Retrieval-augmented generation) based on vector search offers only limited real-time capabilities, as it can't process new vocabulary that arrived after the embedding generation
```

<br>

> **Vector search is not an replacement for keyword search, but a complementary addition** - best to be used within a hybrid solution where the strengths of both approaches are combined. **Keyword search is not outdated, but time-proven**.

---

## Why Rust

We have (partially) ported the SeekStorm codebase from C# to Rust
+ Factor 2..4x performance gain vs. C# (latency and throughput)
+ No slow first run (no cold start costs due to just-in-time compilation)
+ Stable latencies (no garbage collection delays)
+ Less memory consumption (no ramping up until the next garbage collection)
+ No framework dependencies (CLR or JVM virtual machines)
+ Ahead-of-time instead of just-in-time compilation
+ Memory safe language https://www.whitehouse.gov/oncd/briefing-room/2024/02/26/press-release-technical-report/ 
- Steep learning curve (previously development time divided between designing algorithms and debugging, now between translating and fighting with the compiler borrow checker)

Rust is great for performance-critical applications 🚀 that deal with big data and/or many concurrent users. 
Fast algorithms will shine even more with a performance-conscious programming language 🙂

---

## Architecture

see [ARCHITECTURE.md](https://github.com/SeekStorm/SeekStorm/blob/main/ARCHITECTURE.md) 

---

### Building

```
cargo build --release
```

&#x26A0; **WARNING**: make sure to set the MASTER_KEY_SECRET environment variable to a secret, otherwise your generated API keys will be compromised.

### Documentation

[https://docs.rs/seekstorm](https://docs.rs/seekstorm)

**Build documentation**

```
cargo doc --no-deps
```
**Access documentation locally**

SeekStorm\target\doc\seekstorm\index.html  
SeekStorm\target\doc\seekstorm_server\index.html  

### Usage of the library

Add required crates to your project
```
cargo add seekstorm
cargo add tokio
cargo add serde_json
```

```
use std::{collections::HashSet, error::Error, path::Path, sync::Arc};
use seekstorm::{index::*,search::*,highlighter::*};
use tokio::sync::RwLock;
```

use an asynchronous Rust runtime
```
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
```

create index
```
let index_path=Path::new("C:/index/");

let schema_json = r#"
[{"field_name":"title","field_type":"Text","field_stored":false,"field_indexed":false},
{"field_name":"body","field_type":"Text","field_stored":true,"field_indexed":true},
{"field_name":"url","field_type":"Text","field_stored":false,"field_indexed":false}]"#;
let schema=serde_json::from_str(schema_json).unwrap();

let meta = IndexMetaObject {
    id: 0,
    name: "test_index".to_string(),
    similarity:SimilarityType::Bm25f,
    tokenizer:TokenizerType::AsciiAlphabetic,
    access_type: AccessType::Mmap,
};

let serialize_schema=true;
let segment_number_bits1=11;
let index=create_index(index_path,meta,&schema,serialize_schema,segment_number_bits1).unwrap();
let _index_arc = Arc::new(RwLock::new(index));
```

open index (alternatively to create index)
```
let index_path=Path::new("C:/index/");
let index_arc=open_index(index_path).await.unwrap(); 
```
index documents
```
let documents_json = r#"
[{"title":"title1 test","body":"body1","url":"url1"},
{"title":"title2","body":"body2 test","url":"url2"},
{"title":"title3 test","body":"body3 test","url":"url3"}]"#;
let documents_vec=serde_json::from_str(documents_json).unwrap();

index_arc.index_documents(documents_vec).await; 
```
search index
```
let query="test".to_string();
let offset=0;
let length=10;
let query_type=QueryType::Intersection; 
let result_type=ResultType::TopkCount;
let include_uncommitted=false;
let field_filter=Vec::new();
let result_list = index_arc.search(query, query_type, offset, length, result_type,include_uncommitted,field_filter).await;
```
display results
```
let highlights:Vec<Highlight>= vec![
    Highlight {
        field: "body".to_string(),
        name:String::new(),
        fragment_number: 2,
        fragment_size: 160,
        highlight_markup: true,
    },
];    

let highlighter=Some(highlighter(highlights, result_list.query_term_strings));
let fields_hashset= HashSet::new();
let mut index=index_arc.write().await;
for result in result_list.results.iter() {
  let doc=index.get_document(result.doc_id,false,&highlighter,&fields_hashset).unwrap();
  println!("result {} rank {} body field {:?}" , result.doc_id,result.score, doc.get("body"));
}
```
clear index
```
index.clear_index();
```
delete index
```
index.delete_index();
```
close index
```
index.close_index();
```
seekstorm library version string
```
let version=version();
println!("version {}",version);
```
end of main function
```
   Ok(())
}
```

---

## Demo time 

### Build a Wikipedia search engine with the SeekStorm server

A quick step-by-step tutorial on how to build a Wikipedia search engine from a Wikipedia corpus using the SeekStorm server in 5 easy steps.

<img src="assets/wikipedia_demo.png" width="600">

**Download SeekStorm**

[Download SeekStorm from the GitHub repository](https://github.com/SeekStorm/SeekStorm/archive/refs/heads/main.zip)  
Unzip in directory of your choice, open in Visual Studio code.

or alternatively

```
git clone https://github.com/SeekStorm/SeekStorm.git
```

**Build SeekStorm**

Install Rust (if not yet present): https://www.rust-lang.org/tools/install  

In the terminal of Visual Studio Code type:
```
cargo build --release
```

**Get Wikipedia corpus**

Preprocessed English Wikipedia corpus (5,032,105 documents, 8,28 GB decompressed). 
Although wiki-articles.json has a .JSON extension, it is not a valid JSON file. 
It is a text file, where every line contains a JSON object with url, title and body attributes. 
The format is called ndjson, also referred to as "Newline delimited JSON".

[Download Wikipedia corpus](https://www.dropbox.com/s/wwnfnu441w1ec9p/wiki-articles.json.bz2?dl=0)

Decompresss Wikipedia corpus. 

https://gnuwin32.sourceforge.net/packages/bzip2.htm
```
bunzip2 wiki-articles.json.bz2
```

Move the decompressed wiki-articles.json to the release directory

**Start SeekStorm server**
```
cd target/release
```
```
./seekstorm_server local_ip="0.0.0.0" local_port=80
```

**Indexing** 

Type 'ingest' into the command line of the running SeekStorm server: 
```
ingest
```

This creates the demo index  and indexes the local wikipedia file.

**Start searching within the embedded WebUI**

Open embedded Web UI in browser: [http://127.0.0.1](http://127.0.0.1)

Enter a query into the search box 

**Testing the REST API endpoints**

Open src/seekstorm_server/test_api.rest in VSC together with the VSC extension "Rest client" to execute API calls and inspect responses

[interactive API endpoint examples](https://github.com/SeekStorm/SeekStorm/blob/master/src/seekstorm_server/test_api.rest)

Set the 'individual API key' in test_api.rest to the api key displayed in the server console when you typed 'index' above.

**Remove demo index**

Type 'delete' into the command line of the running SeekStorm server: 
```
delete
```

**Shutdown server**

Type 'quit' into the commandline of the running SeekStorm server.

### Online Demo: DeepHN Hacker News search

Full-text search 30M Hacker News posts AND linked web pages

[DeepHN.org](https://deephn.org/)

<img src="assets/deephn_demo.png" width="600">

The DeepHN demo is still based on the SeekStorm C# codebase.  
We are currently porting all required missing features.  
See roadmap below.  

---

## Roadmap

The Rust port is not yet feature complete. The following features are currently ported.

**Porting** 
* delete document
* faceted search
* autosuggestion, spelling correction, instant search
* fuzzy search
* more tokenizer types (stemming, umlauts, apostrophes, CJK)

