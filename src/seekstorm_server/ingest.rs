use std::{
    env::current_exe,
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
    sync::Arc,
    thread::available_parallelism,
    time::Instant,
};

use num_format::{Locale, ToFormattedString};
use seekstorm::index::{Document, Index, IndexDocument};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct DocumentObject {
    pub title: String,
    pub body: String,
    pub url: String,
}

pub(crate) async fn ingest_ndjson(index_arc: Arc<RwLock<Index>>, data_path_str: &str) {
    let batch_size = 100_000;

    let mut data_path = Path::new(&data_path_str);
    let mut absolute_path = current_exe().unwrap();
    if !data_path.is_absolute() {
        absolute_path.pop();
        absolute_path.push(data_path_str);
        data_path = &absolute_path;
    }

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

            for _i in 0..1 {
                let index_arc_clone = index_arc.clone();
                let file = File::open(data_path).unwrap();
                let reader = BufReader::new(file);

                for line in reader.lines() {
                    let index_arc_clone_clone = index_arc_clone.clone();
                    let line_string = line.unwrap();

                    let doc_object: Document =
                        serde_json::from_str::<Document>(&line_string).unwrap();

                    index_arc_clone_clone.index_document(doc_object).await;

                    docid += 1;

                    if docid % batch_size == 0 {
                        println!(
                            "indexed documents {}",
                            docid.to_formatted_string(&Locale::en)
                        );
                    }
                }
            }

            let mut permit_vec = Vec::new();
            for _i in 0..thread_number {
                permit_vec.push(index_permits.acquire().await.unwrap());
            }

            let mut index_mut = index_arc.write().await;
            let indexed_doc_count = index_mut.indexed_doc_count;
            index_mut.commit_level(indexed_doc_count);

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
