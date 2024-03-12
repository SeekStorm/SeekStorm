use memmap2::Mmap;
use serde_json::Value;
use std::collections::HashMap;
use std::io::{Seek, Write};

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

    pub(crate) fn commit_level_docstore(&mut self) {
        let size = self.compressed_docstore_segment_block_buffer.len();

        let level = self.level_index.len() - 1;
        self.level_index[level].docstore_pointer_docs_pointer =
            self.docstore_file.stream_position().unwrap() as usize;

        let _ = self.docstore_file.write(&(size as u32).to_le_bytes());
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
        self.compressed_docstore_segment_block_buffer = vec![0; ROARING_BLOCK_SIZE * 4];

        self.docstore_file_mmap =
            unsafe { Mmap::map(&self.docstore_file).expect("Unable to create Mmap") };
    }

    /// Get document for document id
    pub fn get_document(
        &self,
        doc_id: usize,
        include_uncommited: bool,
    ) -> Result<Document, String> {
        if doc_id >= self.indexed_doc_count {
            return Err("not found".to_owned());
        }
        let block_id = doc_id >> 16;
        if block_id == self.level_index.len() && !(include_uncommited && self.level0_uncommitted) {
            return Err("not found".to_owned());
        }

        if !self.field_store_flag {
            return Err("not found".to_owned());
        }

        let doc_id_local = doc_id & 0b11111111_11111111;

        if self.meta.access_type == AccessType::Ram {
            let docstore_pointer_docs = if block_id == self.level_index.len() {
                &self.compressed_docstore_segment_block_buffer
            } else {
                &self.level_index[block_id].docstore_pointer_docs
            };

            let position = doc_id_local * 4;
            let pointer = read_u32(docstore_pointer_docs, position) as usize;

            let previous_pointer = if doc_id_local == 0 {
                ROARING_BLOCK_SIZE * 4
            } else {
                read_u32(docstore_pointer_docs, position - 4) as usize
            };

            if previous_pointer == pointer {
                return Err("not found".to_owned());
            }

            let compressed_doc = &docstore_pointer_docs[previous_pointer..pointer];
            let decompressed_doc = zstd::decode_all(compressed_doc).unwrap();
            let doc: Document = serde_json::from_slice(&decompressed_doc).unwrap();
            Ok(doc)
        } else {
            let level = doc_id >> 16;

            let pointer;
            let previous_pointer;
            let position =
                self.level_index[level].docstore_pointer_docs_pointer + (doc_id_local * 4);

            if doc_id_local == 0 {
                previous_pointer = 65_536 * 4;
                pointer = read_u32(&self.docstore_file_mmap, position) as usize;
            } else {
                previous_pointer = read_u32(&self.docstore_file_mmap, position - 4) as usize;
                pointer = read_u32(&self.docstore_file_mmap, position) as usize;
            };

            if previous_pointer == pointer {
                return Err("not found".to_owned());
            }

            let compressed_doc = &self.docstore_file_mmap[(self.level_index[level]
                .docstore_pointer_docs_pointer
                + previous_pointer)
                ..(self.level_index[level].docstore_pointer_docs_pointer + pointer)];

            let decompressed_doc = zstd::decode_all(compressed_doc).unwrap();
            let doc: Document = serde_json::from_slice(&decompressed_doc).unwrap();
            Ok(doc)
        }
    }
}
