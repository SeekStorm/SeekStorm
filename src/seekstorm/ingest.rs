use std::{
    ffi::OsStr,
    fs::{self, metadata, File},
    io::{self, BufReader, Read},
    path::Path,
    sync::Arc,
    thread::available_parallelism,
    time::Instant,
};

use async_recursion::async_recursion;
use num_format::{Locale, ToFormattedString};
use serde_json::Deserializer;
use tokio::sync::RwLock;

use crate::{
    commit::Commit,
    index::{Document, Index, IndexDocument},
};

fn read_skipping_ws(mut reader: impl Read) -> io::Result<u8> {
    loop {
        let mut byte = 0u8;
        reader.read_exact(std::slice::from_mut(&mut byte))?;
        if !byte.is_ascii_whitespace() {
            return Ok(byte);
        }
    }
}

pub async fn index_pdf(_index_arc: Arc<RwLock<Index>>, _data_path: &Path, _docid: &mut i64) {}

#[async_recursion]
pub async fn path_recurse(index_arc: Arc<RwLock<Index>>, data_path: &Path, docid: &mut i64) {
    let paths = fs::read_dir(data_path).unwrap();

    for path in paths {
        let index_arc_clone = index_arc.clone();
        if let Ok(path) = path {
            let md = metadata(path.path()).unwrap();
            if md.is_file() {
                if let Some(extension) = Path::new(&path.path().display().to_string())
                    .extension()
                    .and_then(OsStr::to_str)
                {
                    if extension.to_lowercase() == "pdf" {
                        index_pdf(index_arc_clone, &path.path(), docid).await;
                    }
                }
            } else {
                path_recurse(index_arc_clone, &path.path(), docid).await;
            }
        }
    }
}

/// Ingest local data files in [PDF](https://en.wikipedia.org/wiki/PDF) format via console command.
/// If a directory is provided, the function will recursively search for all PDF files.
pub async fn ingest_pdf(index_arc: Arc<RwLock<Index>>, data_path: &Path) {
    match data_path.exists() {
        true => {
            println!("ingesting PDF from: {}", data_path.display());

            let start_time = Instant::now();
            let mut docid: i64 = 0;

            let thread_number = available_parallelism().unwrap().get();
            let index_arc_clone2 = index_arc.clone();
            let mut index_arc_clone3 = index_arc.clone();
            let index_ref = index_arc_clone2.read().await;
            let index_permits = index_ref.permits.clone();
            drop(index_ref);

            let md = metadata(data_path).unwrap();
            if md.is_file() {
                if let Some(extension) = Path::new(&data_path.display().to_string())
                    .extension()
                    .and_then(OsStr::to_str)
                {
                    if extension.to_lowercase() == "pdf" {
                        index_pdf(index_arc, data_path, &mut docid).await;
                    }
                }
            } else {
                path_recurse(index_arc, data_path, &mut docid).await;
            }

            let mut permit_vec = Vec::new();
            for _i in 0..thread_number {
                permit_vec.push(index_permits.acquire().await.unwrap());
            }

            index_arc_clone3.commit().await;

            let elapsed_time = start_time.elapsed().as_nanos();

            println!(
                "Indexing finished: docs {}  docs/sec {}  docs/day {} minutes {:.2} seconds {}",
                docid.to_formatted_string(&Locale::en),
                (docid as u128 * 1_000_000_000 / elapsed_time).to_formatted_string(&Locale::en),
                ((docid as u128 * 1_000_000_000 / elapsed_time) * 3600 * 24)
                    .to_formatted_string(&Locale::en),
                elapsed_time as f64 / 1_000_000_000.0 / 60.0,
                elapsed_time / 1_000_000_000
            );
        }
        false => {
            println!("data file not found: {}", data_path.display());
        }
    }
}

/// Ingest local data files in [JSON](https://en.wikipedia.org/wiki/JSON), [Newline-delimited JSON](https://github.com/ndjson/ndjson-spec) (ndjson), and [Concatenated JSON](https://en.wikipedia.org/wiki/JSON_streaming) formats via console command.  
/// The document ingestion is streamed without loading the whole document vector into memory to allwow for unlimited file size while keeping RAM consumption low.
pub async fn ingest_json(index_arc: Arc<RwLock<Index>>, data_path: &Path) {
    match data_path.exists() {
        true => {
            println!("ingesting data from: {}", data_path.display());

            let start_time = Instant::now();
            let mut docid: i64 = 0;

            let thread_number = available_parallelism().unwrap().get();
            let index_arc_clone2 = index_arc.clone();
            let index_ref = index_arc_clone2.read().await;
            let index_permits = index_ref.permits.clone();
            drop(index_ref);

            let index_arc_clone = index_arc.clone();
            let file = File::open(data_path).unwrap();
            let mut reader = BufReader::new(file);

            let is_vector = read_skipping_ws(&mut reader).unwrap() == b'[';

            if !is_vector {
                println!("Newline-delimited JSON (ndjson) or Concatenated JSON detected");
                reader.seek_relative(-1).unwrap();

                for doc_object in Deserializer::from_reader(reader).into_iter::<Document>() {
                    let index_arc_clone_clone = index_arc_clone.clone();

                    index_arc_clone_clone
                        .index_document(doc_object.unwrap())
                        .await;
                    docid += 1;
                }
            } else {
                println!("JSON detected");

                let index_arc_clone_clone = index_arc_clone.clone();
                loop {
                    let next_obj = Deserializer::from_reader(reader.by_ref())
                        .into_iter::<Document>()
                        .next();
                    match next_obj {
                        Some(doc_object) => {
                            index_arc_clone_clone
                                .index_document(doc_object.unwrap())
                                .await
                        }
                        None => break,
                    }

                    docid += 1;

                    match read_skipping_ws(reader.by_ref()).unwrap() {
                        b',' => {}
                        b']' => break,
                        _ => break,
                    }
                }
            }

            let mut permit_vec = Vec::new();
            for _i in 0..thread_number {
                permit_vec.push(index_permits.acquire().await.unwrap());
            }

            let mut index_arc_clone3 = index_arc.clone();

            index_arc_clone3.commit().await;

            let elapsed_time = start_time.elapsed().as_nanos();

            println!(
                "Indexing finished: docs {}  docs/sec {}  docs/day {} minutes {:.2} seconds {}",
                docid.to_formatted_string(&Locale::en),
                (docid as u128 * 1_000_000_000 / elapsed_time).to_formatted_string(&Locale::en),
                ((docid as u128 * 1_000_000_000 / elapsed_time) * 3600 * 24)
                    .to_formatted_string(&Locale::en),
                elapsed_time as f64 / 1_000_000_000.0 / 60.0,
                elapsed_time / 1_000_000_000
            );
        }
        false => {
            println!("data file not found: {}", data_path.display());
        }
    }
}
