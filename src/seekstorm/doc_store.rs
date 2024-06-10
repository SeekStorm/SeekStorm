use memmap2::Mmap;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Seek, SeekFrom, Write};

use crate::highlighter::{top_fragments_from_field, Highlighter};
use crate::index::{AccessType, Document, Index, ROARING_BLOCK_SIZE};
use crate::utils::{read_u32, write_u32};

impl Index {
    pub(crate) fn store_document(&mut self, doc_id: usize, document: HashMap<String, Value>) {
        let mut document = document;

        let keys: Vec<String> = document.keys().cloned().collect();
        for key in keys.into_iter() {
            if !self.schema_map.contains_key(&key)
                || !self.schema_map.get(&key).unwrap().field_stored
            {
                document.remove(&key);
            }
        }

        if document.is_empty() {
            return;
        }

        let document_string = serde_json::to_string(&document).unwrap();

        let mut compressed = zstd::encode_all(document_string.as_bytes(), 1).unwrap();

        self.compressed_docstore_segment_block_buffer
            .append(&mut compressed);

        write_u32(
            self.compressed_docstore_segment_block_buffer.len() as u32,
            &mut self.compressed_docstore_segment_block_buffer,
            (doc_id & 0b11111111_11111111) * 4,
        );
    }

    pub(crate) fn commit_docstore(&mut self, is_last_level_incomplete: bool) {
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
            let indexed_doc_count = (self.indexed_doc_count - 1 % ROARING_BLOCK_SIZE) + 1;

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

        self.compressed_docstore_segment_block_buffer = vec![0; ROARING_BLOCK_SIZE * 4];

        if self.meta.access_type == AccessType::Mmap {
            self.docstore_file_mmap =
                unsafe { Mmap::map(&self.docstore_file).expect("Unable to create Mmap") };
        }
    }

    /// Get document for document id
    /// Arguments:
    /// * `doc_id`: Specifies which document to load from the document store of the index.
    /// * `include_uncommited`: Return also documents which have not yet been committed.
    /// * `highlighter_option`: Specifies the extraction of keyword-in-context (KWIC) fragments from fields in documents, and the highlighting of the query terms within.
    /// * `fields`: Specifies which of the stored fields to return with each document. Default: If empty return all stoed fields
    pub fn get_document(
        &self,
        doc_id: usize,
        include_uncommited: bool,
        highlighter_option: &Option<Highlighter>,
        fields: &HashSet<String>,
    ) -> Result<Document, String> {
        if doc_id >= self.indexed_doc_count {
            return Err("not found1".to_owned());
        }
        let block_id = doc_id >> 16;

        let is_uncommitted = doc_id >= self.committed_doc_count;
        if is_uncommitted && !(include_uncommited && self.uncommitted) {
            return Err("not found2".to_owned());
        }

        if self.stored_field_names.is_empty() {
            return Err("not found3".to_owned());
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
                return Err("not found4".to_owned());
            }

            let compressed_doc = &docstore_pointer_docs[previous_pointer..pointer];
            let decompressed_doc = zstd::decode_all(compressed_doc).unwrap();
            let doc: Document = serde_json::from_slice(&decompressed_doc).unwrap();

            doc
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
                return Err(format!("not found5 {} {}", previous_pointer, pointer));
            }

            let compressed_doc = &self.docstore_file_mmap[(self.level_index[level]
                .docstore_pointer_docs_pointer
                + previous_pointer)
                ..(self.level_index[level].docstore_pointer_docs_pointer + pointer)];

            let decompressed_doc = zstd::decode_all(compressed_doc).unwrap();
            let doc: Document = serde_json::from_slice(&decompressed_doc).unwrap();

            doc
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
                    serde_json::Value::String(kwic),
                );
            }
        }

        if !fields.is_empty() {
            for key in self.stored_field_names.iter() {
                if !fields.contains(key) {
                    doc.remove(key);
                }
            }
        }

        Ok(doc)
    }
}
