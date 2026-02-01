use memmap2::Mmap;
use serde_json::json;
use std::collections::{HashSet, VecDeque};
use std::fs;
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;

use crate::geo_search::euclidian_distance;
use crate::highlighter::{Highlighter, top_fragments_from_field};
use crate::index::{
    AccessType, DistanceField, Document, DocumentCompression, FILE_PATH, FieldType, Index,
    ROARING_BLOCK_SIZE, Shard,
};
use crate::search::FacetValue;
use crate::utils::{read_u32, write_u32};

impl Shard {
    pub(crate) fn get_file_shard(&self, doc_id: usize) -> Result<Vec<u8>, String> {
        let file_path = Path::new(&self.index_path_string)
            .join(FILE_PATH)
            .join(doc_id.to_string() + ".pdf");

        if let Ok(data) = fs::read(file_path) {
            Ok(data)
        } else {
            Err("not found".into())
        }
    }

    pub(crate) fn get_document_shard(
        &self,
        doc_id: usize,
        include_uncommitted: bool,
        highlighter_option: &Option<Highlighter>,
        fields: &HashSet<String>,
        distance_fields: &[DistanceField],
    ) -> Result<Document, String> {
        if !self.delete_hashset.is_empty() && self.delete_hashset.contains(&doc_id) {
            return Err("not found".to_owned());
        }

        if doc_id >= self.indexed_doc_count {
            return Err("not found".to_owned());
        }
        let block_id = doc_id >> 16;

        let is_uncommitted = doc_id >= self.committed_doc_count;
        if is_uncommitted && !(include_uncommitted && self.uncommitted) {
            return Err("not found".to_owned());
        }

        if self.stored_field_names.is_empty() {
            return Err("not found".to_owned());
        }

        let doc_id_local = doc_id & 0b11111111_11111111;

        let mut doc = if self.meta.access_type == AccessType::Ram || is_uncommitted {
            let docstore_pointer_docs = if is_uncommitted {
                &self.compressed_docstore_segment_block_buffer
            } else {
                &self.level_index[block_id].docstore_pointer_docs
            };

            let position = doc_id_local * 4;
            let pointer = read_u32(docstore_pointer_docs, position) as usize;

            let previous_pointer = if doc_id == self.committed_doc_count || doc_id_local == 0 {
                ROARING_BLOCK_SIZE * 4
            } else {
                read_u32(docstore_pointer_docs, position - 4) as usize
            };

            if previous_pointer == pointer {
                return Err("not found".to_owned());
            }

            let compressed_doc = &docstore_pointer_docs[previous_pointer..pointer];

            match self.meta.document_compression {
                DocumentCompression::None => {
                    let doc: Document = serde_json::from_slice(compressed_doc).unwrap();
                    doc
                }
                DocumentCompression::Snappy => {
                    let decompressed_doc = snap::raw::Decoder::new()
                        .decompress_vec(compressed_doc)
                        .unwrap();
                    let doc: Document = serde_json::from_slice(&decompressed_doc).unwrap();
                    doc
                }
                DocumentCompression::Lz4 => {
                    let decompressed_doc =
                        lz4_flex::decompress_size_prepended(compressed_doc).unwrap();
                    let doc: Document = serde_json::from_slice(&decompressed_doc).unwrap();
                    doc
                }
                DocumentCompression::Zstd => {
                    let decompressed_doc = zstd::decode_all(compressed_doc).unwrap();
                    let doc: Document = serde_json::from_slice(&decompressed_doc).unwrap();
                    doc
                }
            }
        } else {
            let level = doc_id >> 16;

            let pointer;
            let previous_pointer;
            let position =
                self.level_index[level].docstore_pointer_docs_pointer + (doc_id_local * 4);

            if doc_id_local == 0 {
                previous_pointer = ROARING_BLOCK_SIZE * 4;
                pointer = read_u32(&self.docstore_file_mmap, position) as usize;
            } else {
                previous_pointer = read_u32(&self.docstore_file_mmap, position - 4) as usize;
                pointer = read_u32(&self.docstore_file_mmap, position) as usize;
            };

            if previous_pointer == pointer {
                return Err(format!("not found {} {}", previous_pointer, pointer));
            }

            let compressed_doc = &self.docstore_file_mmap[(self.level_index[level]
                .docstore_pointer_docs_pointer
                + previous_pointer)
                ..(self.level_index[level].docstore_pointer_docs_pointer + pointer)];

            match self.meta.document_compression {
                DocumentCompression::None => {
                    let doc: Document = serde_json::from_slice(compressed_doc).unwrap();
                    doc
                }
                DocumentCompression::Snappy => {
                    let decompressed_doc = snap::raw::Decoder::new()
                        .decompress_vec(compressed_doc)
                        .unwrap();
                    let doc: Document = serde_json::from_slice(&decompressed_doc).unwrap();
                    doc
                }
                DocumentCompression::Lz4 => {
                    let decompressed_doc =
                        lz4_flex::decompress_size_prepended(compressed_doc).unwrap();
                    let doc: Document = serde_json::from_slice(&decompressed_doc).unwrap();
                    doc
                }
                DocumentCompression::Zstd => {
                    let decompressed_doc = zstd::decode_all(compressed_doc).unwrap();
                    let doc: Document = serde_json::from_slice(&decompressed_doc).unwrap();
                    doc
                }
            }
        };

        if let Some(highlighter) = highlighter_option {
            let mut kwic_vec: VecDeque<String> = VecDeque::new();
            for highlight in highlighter.highlights.iter() {
                let kwic =
                    top_fragments_from_field(self, &doc, &highlighter.query_terms_ac, highlight)
                        .unwrap();
                kwic_vec.push_back(kwic);
            }

            for highlight in highlighter.highlights.iter() {
                let kwic = kwic_vec.pop_front().unwrap();
                doc.insert(
                    (if highlight.name.is_empty() {
                        &highlight.field
                    } else {
                        &highlight.name
                    })
                    .to_string(),
                    json!(kwic),
                );
            }
        }

        for distance_field in distance_fields.iter() {
            if let Some(idx) = self.facets_map.get(&distance_field.field)
                && self.facets[*idx].field_type == FieldType::Point
                && let FacetValue::Point(point) =
                    self.get_facet_value_shard(&distance_field.field, doc_id)
            {
                let distance =
                    euclidian_distance(&point, &distance_field.base, &distance_field.unit);

                doc.insert(distance_field.distance.clone(), json!(distance));
            }
        }

        if !fields.is_empty() {
            for key in self.stored_field_names.iter() {
                if !fields.contains(key) {
                    doc.shift_remove(key);
                }
            }
        }

        Ok(doc)
    }

    pub(crate) fn copy_file(&self, source_path: &Path, doc_id: usize) -> io::Result<u64> {
        let dir_path = Path::new(&self.index_path_string).join(FILE_PATH);
        if !dir_path.exists() {
            fs::create_dir_all(&dir_path).unwrap();
        }

        let file_path = dir_path.join(doc_id.to_string() + ".pdf");
        fs::copy(source_path, file_path)
    }

    pub(crate) fn write_file(&self, file_bytes: &[u8], doc_id: usize) -> io::Result<u64> {
        let dir_path = Path::new(&self.index_path_string).join(FILE_PATH);
        if !dir_path.exists() {
            fs::create_dir_all(&dir_path).unwrap();
        }

        let file_path = dir_path.join(doc_id.to_string() + ".pdf");

        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(file_path)?;

        let _ = file.write_all(file_bytes);
        Ok(file_bytes.len() as u64)
    }

    pub(crate) fn store_document(&mut self, doc_id: usize, document: Document) {
        let mut document = document;

        let keys: Vec<String> = document.keys().cloned().collect();
        for key in keys.into_iter() {
            if !self.schema_map.contains_key(&key) || !self.schema_map.get(&key).unwrap().stored {
                document.shift_remove(&key);
            }
        }

        if document.is_empty() {
            return;
        }

        let mut compressed = match self.meta.document_compression {
            DocumentCompression::None => serde_json::to_vec(&document).unwrap(),
            DocumentCompression::Snappy => {
                let serialized = serde_json::to_vec(&document).unwrap();
                snap::raw::Encoder::new().compress_vec(&serialized).unwrap()
            }
            DocumentCompression::Lz4 => {
                let serialized = serde_json::to_vec(&document).unwrap();
                lz4_flex::compress_prepend_size(&serialized)
            }
            DocumentCompression::Zstd => {
                let serialized = serde_json::to_vec(&document).unwrap();
                zstd::encode_all(serialized.as_slice(), 1).unwrap()
            }
        };

        self.compressed_docstore_segment_block_buffer
            .append(&mut compressed);

        write_u32(
            self.compressed_docstore_segment_block_buffer.len() as u32,
            &mut self.compressed_docstore_segment_block_buffer,
            (doc_id & 0b11111111_11111111) * 4,
        );
    }

    pub(crate) fn commit_docstore(
        &mut self,
        indexed_doc_count: usize,
        is_last_level_incomplete: bool,
    ) {
        let size_uncommitted = self.compressed_docstore_segment_block_buffer.len();
        let level = self.level_index.len() - 1;

        if is_last_level_incomplete {
            let docstore_file_end = self.docstore_file.metadata().unwrap().len();

            let size_committed =
                docstore_file_end as usize - self.last_level_docstore_file_start_pos as usize - 4;
            let size_committed_docs = size_committed - (4 * ROARING_BLOCK_SIZE);
            let size_sum = size_uncommitted + size_committed_docs;

            let _ = self
                .docstore_file
                .seek(SeekFrom::Start(self.last_level_docstore_file_start_pos));

            let _ = self.docstore_file.write(&(size_sum as u32).to_le_bytes());

            let committed_doc_count = (self.committed_doc_count - 1 % ROARING_BLOCK_SIZE) + 1;
            let indexed_doc_count = (indexed_doc_count - 1 % ROARING_BLOCK_SIZE) + 1;

            for i in committed_doc_count..indexed_doc_count {
                let pointer = read_u32(&self.compressed_docstore_segment_block_buffer, i * 4);

                write_u32(
                    pointer + size_committed_docs as u32,
                    &mut self.compressed_docstore_segment_block_buffer,
                    i * 4,
                );
            }

            let _ = self.docstore_file.seek(SeekFrom::Start(
                self.last_level_docstore_file_start_pos + 4 + committed_doc_count as u64 * 4,
            ));

            let _ = self.docstore_file.write(
                &self.compressed_docstore_segment_block_buffer
                    [committed_doc_count * 4..ROARING_BLOCK_SIZE * 4],
            );

            let _ = self.docstore_file.seek(SeekFrom::Start(docstore_file_end));

            let _ = self
                .docstore_file
                .write(&self.compressed_docstore_segment_block_buffer[4 * ROARING_BLOCK_SIZE..]);

            if self.meta.access_type == AccessType::Ram {
                self.level_index[level]
                    .docstore_pointer_docs
                    .extend_from_slice(
                        &self.compressed_docstore_segment_block_buffer[4 * ROARING_BLOCK_SIZE..],
                    );
                self.level_index[level].docstore_pointer_docs
                    [committed_doc_count * 4..ROARING_BLOCK_SIZE * 4]
                    .copy_from_slice(
                        &self.compressed_docstore_segment_block_buffer
                            [committed_doc_count * 4..ROARING_BLOCK_SIZE * 4],
                    );
            }
        } else {
            let _ = self
                .docstore_file
                .write(&(size_uncommitted as u32).to_le_bytes());

            self.level_index[level].docstore_pointer_docs_pointer =
                self.docstore_file.stream_position().unwrap() as usize;

            let _ = self
                .docstore_file
                .write(&self.compressed_docstore_segment_block_buffer);

            if self.meta.access_type == AccessType::Ram {
                self.level_index[level].docstore_pointer_docs.append(
                    &mut self
                        .compressed_docstore_segment_block_buffer
                        .drain(..)
                        .collect(),
                );
            }
        }

        let _ = self.docstore_file.flush();

        self.compressed_docstore_segment_block_buffer = vec![0; ROARING_BLOCK_SIZE * 4];

        if self.meta.access_type == AccessType::Mmap {
            self.docstore_file_mmap =
                unsafe { Mmap::map(&self.docstore_file).expect("Unable to create Mmap") };
        }
    }
}

impl Index {
    /// Get file for document id
    /// Arguments:
    /// * `doc_id`: Document ID that specifies which file to load from the document store of the index.
    ///   ⚠️ Use search or get_iterator first to obtain a valid doc_id. Document IDs are not guaranteed to be continuous and gapless!
    ///
    /// Returns:
    /// * `Vec<u8>`: The file content as a byte vector.
    ///
    pub async fn get_file(&self, doc_id: usize) -> Result<Vec<u8>, String> {
        let shard_id = doc_id % self.shard_number;
        let doc_id_shard = doc_id / self.shard_number;

        self.shard_vec[shard_id]
            .read()
            .await
            .get_file_shard(doc_id_shard)
    }

    /// Get document for document id
    /// Arguments:
    /// * `doc_id`: Document ID that specifies which document to load from the document store of the index.
    ///   ⚠️ Use search or get_iterator first to obtain a valid doc_id. Document IDs are not guaranteed to be continuous and gapless!
    /// * `include_uncommitted`: Return also documents which have not yet been committed.
    /// * `highlighter_option`: Specifies the extraction of keyword-in-context (KWIC) fragments from fields in documents, and the highlighting of the query terms within.
    /// * `fields`: Specifies which of the stored fields to return with each document. Default: If empty return all stored fields
    /// * `distance_fields`: insert distance fields into result documents, calculating the distance between a specified facet field of type Point and a base Point, in kilometers or miles.
    ///   using Euclidian distance (Pythagoras theorem) with Equirectangular approximation.
    pub async fn get_document(
        &self,
        doc_id: usize,
        include_uncommitted: bool,
        highlighter_option: &Option<Highlighter>,
        fields: &HashSet<String>,
        distance_fields: &[DistanceField],
    ) -> Result<Document, String> {
        let shard_id = doc_id % self.shard_number;
        let doc_id_shard = doc_id / self.shard_number;

        self.shard_vec[shard_id].read().await.get_document_shard(
            doc_id_shard,
            include_uncommitted,
            highlighter_option,
            fields,
            distance_fields,
        )
    }
}
