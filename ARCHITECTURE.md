# Architecture

SeekStorm is an open-source, sub-millisecond full-text search library & multi-tenancy server implemented in Rust.

Scalability and performance are the two fundamental design goals.

Index size and latency grow linearly with the number of indexed documents, while the RAM consumption remains constant, ensuring scalability.

## Index

The index is based on an inverted index. The index can either be kept in RAM or memory mapped files. In both cases it is fully persistent on disk.
The identical index file format for both RAM and memory mapping mode, allows to switch the index access mode for an existing index at any time.
* Ram: no disc access at search time for minimal latency, even after cold start, at the cost of longer index load time and higher RAM consumption as the whole index is preloaded to RAM.
* Mmap: disc access via mmap during search time, for minimal RAM consumption, high scalability, and minimal index load time. With Mmap disk access is cached by the OS, being persistent between program starts until reboot.

</br>

* index.bin : contains posting lists with document IDs and term positions. Posting lists are compressed with roaring bitmaps. Term positions of each field are delta compressed and VINT encoded.
* index.json : contains index meta data such as similarity (e.g. Bm25), access type (e.g. Ram/Mmap), tokenizer (e.g. AsciiAlphabetic).
* delete.bin : contains document IDs of deleted documents. By manually deleting the delete.bin file the deleted documents can be recovered (until compaction).
* facet.bin : contains the serialized values of all facet fields of all documents in the index
* facet.json : contains the unique values of all facet fields of all documents in the index
* synonyms.json : contains the synonyms that were created with the synonyms parameter in create_index. Can be manually modified, but becomes effective only after restart and only for subsequently indexed documents.

**SeekStorm server index directory structure**

First hierarchy level: API keys  
Second hierarchy level: Indices per API key  
```
seekstorm_index/  
├─ 0/  
│  ├─ 0  
│  ├─ 1  
│  ├─ 2  
├─ 1/  
│  ├─ 0  
│  ├─ 1  
```

* apikey.json : contains API key hash and quotas

You can manually delete, copy, or backup and restore both API key and index directories (shutdown server first and then restart).

## Search

* DaaT (Document-at-a-Time) intersection and union: 
  + prevents writing long intermediate result lists in RAM of TaaT (Term-at-a-Time)
  + allows streaming to enable scalability for huge indexes
* SIMD vector processing hardware support for intersection and union of roaring bitmaps compressed posting lists
* Galloping intersection
* Improved Block-max WAND
* N-gram indexing of frequent terms

## Database schema

Every document can contain an arbitrary number of fields of different types.

Every field can be searched and filtered individually or all field together globally.

* schema.json : contains the definition of fields, their field types, and whether they are stored and/or indexed.

## Document store

The documents are stored in JSON format and compressed with Zstandard.

The index schema defines which fields of the documents are stored in the document store and can be part of the returned search results.

* docstore.bin : contains the compressed documents

## Limits

There are **no** limits on the number of 
* indices
* documents
* fields
* field length
* terms

There is a limit of maximum 65_536 distinct
* string facet values per facet field. 
* numerical ranges per facet field. 
