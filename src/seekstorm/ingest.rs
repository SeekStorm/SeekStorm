use std::{
    collections::HashMap,
    ffi::OsStr,
    fs::{File, metadata},
    io::{self, BufReader, Read},
    path::Path,
    sync::Arc,
    time::{Instant, SystemTime},
};

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use colored::Colorize;
use csv::{ReaderBuilder, Terminator};
use num_format::{Locale, ToFormattedString};
#[cfg(feature = "pdf")]
use pdfium_render::prelude::{PdfDocumentMetadataTagType, Pdfium};
use serde_json::{Deserializer, json};
use tokio::sync::RwLock;
use walkdir::WalkDir;

use crate::{
    commit::Commit,
    index::{Document, FileType, Index, IndexArc, IndexDocument},
    utils::truncate,
};

use lazy_static::lazy_static;

#[cfg(feature = "pdf")]
type PdfDocument<'a> = pdfium_render::prelude::PdfDocument<'a>;
#[cfg(not(feature = "pdf"))]
type PdfDocument<'a> = ();

#[cfg(feature = "pdf")]
lazy_static! {
    pub(crate) static ref pdfium_option: Option<Pdfium> = if let Ok(pdfium) =
        Pdfium::bind_to_library(Pdfium::pdfium_platform_library_name_at_path("./"))
            .or_else(|_| Pdfium::bind_to_system_library())
    {
        Some(Pdfium::new(pdfium))
    } else {
        None
    };
}

fn read_skipping_ws(mut reader: impl Read) -> io::Result<u8> {
    loop {
        let mut byte = 0u8;
        reader.read_exact(std::slice::from_mut(&mut byte))?;
        if !byte.is_ascii_whitespace() {
            return Ok(byte);
        }
    }
}

/// Index PDF file from local disk.
/// - converts pdf to text and indexes it
/// - extracts title from metatag, or first line of text, or from filename
/// - extracts creation date from metatag, or from file creation date (Unix timestamp: the number of seconds since 1 January 1970)
/// - copies all ingested pdf files to "files" subdirectory in index
/// # Arguments
/// * `file_path` - Path to the file
/// # Returns
/// * `Result<(), String>` - Ok(()) or Err(String)
#[allow(clippy::too_many_arguments)]
#[allow(async_fn_in_trait)]
pub trait IndexPdfFile {
    /// Index PDF file from local disk.
    /// - converts pdf to text and indexes it
    /// - extracts title from metatag, or first line of text, or from filename
    /// - extracts creation date from metatag, or from file creation date (Unix timestamp: the number of seconds since 1 January 1970)
    /// - copies all ingested pdf files to "files" subdirectory in index
    /// # Arguments
    /// * `file_path` - Path to the file
    /// # Returns
    /// * `Result<(), String>` - Ok(()) or Err(String)
    async fn index_pdf_file(&self, file_path: &Path) -> Result<(), String>;
}

impl IndexPdfFile for IndexArc {
    /// Index PDF file from local disk.
    /// - converts pdf to text and indexes it
    /// - extracts title from metatag, or first line of text, or from filename
    /// - extracts creation date from metatag, or from file creation date (Unix timestamp: the number of seconds since 1 January 1970)
    /// - copies all ingested pdf files to "files" subdirectory in index
    async fn index_pdf_file(&self, file_path: &Path) -> Result<(), String> {
        #[cfg(feature = "pdf")]
        {
            if let Some(pdfium) = pdfium_option.as_ref() {
                let file_size = file_path.metadata().unwrap().len() as usize;

                let date: DateTime<Utc> = if let Ok(metadata) = metadata(file_path) {
                    if let Ok(time) = metadata.created() {
                        time
                    } else {
                        SystemTime::now()
                    }
                } else {
                    SystemTime::now()
                }
                .into();
                let file_date = date.timestamp();

                if let Ok(pdf) = pdfium.load_pdf_from_file(file_path, None) {
                    self.index_pdf(
                        file_path,
                        file_size,
                        file_date,
                        FileType::Path(file_path.into()),
                        pdf,
                    )
                    .await;
                    Ok(())
                } else {
                    println!("can't read PDF {} {}", file_path.display(), file_size);
                    Err("can't read PDF".to_string())
                }
            } else {
                println!(
                    "Pdfium library not found: download and copy into the same folder as the seekstorm_server.exe: https://github.com/bblanchon/pdfium-binaries"
                );
                Err("Pdfium library not found".to_string())
            }
        }
        #[cfg(not(feature = "pdf"))]
        {
            println!("pdf feature flag not enabled");
            Err("pdf feature flag not enabled".to_string())
        }
    }
}

/// Index PDF file from byte array.
/// - converts pdf to text and indexes it
/// - extracts title from metatag, or first line of text, or from filename
/// - extracts creation date from metatag, or from file creation date (Unix timestamp: the number of seconds since 1 January 1970)
/// - copies all ingested pdf files to "files" subdirectory in index
/// # Arguments
/// * `file_path` - Path to the file (fallback, if title and date can't be extracted)
/// * `file_date` - File creation date (Unix timestamp: the number of seconds since 1 January 1970) (fallback, if date can't be extracted)
/// * `file_bytes` - Byte array of the file
#[allow(clippy::too_many_arguments)]
#[allow(async_fn_in_trait)]
pub trait IndexPdfBytes {
    /// Index PDF file from byte array.
    /// - converts pdf to text and indexes it
    /// - extracts title from metatag, or first line of text, or from filename
    /// - extracts creation date from metatag, or from file creation date (Unix timestamp: the number of seconds since 1 January 1970)
    /// - copies all ingested pdf files to "files" subdirectory in index
    /// # Arguments
    /// * `file_path` - Path to the file (fallback, if title and date can't be extracted)
    /// * `file_date` - File creation date (Unix timestamp: the number of seconds since 1 January 1970) (fallback, if date can't be extracted)
    /// * `file_bytes` - Byte array of the file
    async fn index_pdf_bytes(
        &self,
        file_path: &Path,
        file_date: i64,
        file_bytes: &[u8],
    ) -> Result<(), String>;
}

#[cfg(feature = "pdf")]
impl IndexPdfBytes for IndexArc {
    /// Index PDF file from byte array.
    /// - converts pdf to text and indexes it
    /// - extracts title from metatag, or first line of text, or from filename
    /// - extracts creation date from metatag, or from file creation date (Unix timestamp: the number of seconds since 1 January 1970)
    /// - copies all ingested pdf files to "files" subdirectory in index
    /// # Arguments
    /// * `file_path` - Path to the file (fallback, if title and date can't be extracted)
    /// * `file_date` - File creation date (Unix timestamp: the number of seconds since 1 January 1970) (fallback, if date can't be extracted)
    /// * `file_bytes` - Byte array of the file
    async fn index_pdf_bytes(
        &self,
        file_path: &Path,
        file_date: i64,
        file_bytes: &[u8],
    ) -> Result<(), String> {
        if let Some(pdfium) = pdfium_option.as_ref() {
            let file_size = file_bytes.len();
            if let Ok(pdf) = pdfium.load_pdf_from_byte_slice(file_bytes, None) {
                self.index_pdf(
                    file_path,
                    file_size,
                    file_date,
                    FileType::Bytes(file_path.into(), file_bytes.into()),
                    pdf,
                )
                .await;
                Ok(())
            } else {
                println!("can't read PDF {} {}", file_path.display(), file_size);
                Err("can't read PDF".to_string())
            }
        } else {
            println!(
                "Pdfium library not found: download and copy into the same folder as the seekstorm_server.exe: https://github.com/bblanchon/pdfium-binaries"
            );
            Err("Pdfium library not found".to_string())
        }
    }
}

#[cfg(not(feature = "pdf"))]
impl IndexPdfBytes for IndexArc {
    /// Index PDF file from byte array.
    /// - converts pdf to text and indexes it
    /// - extracts title from metatag, or first line of text, or from filename
    /// - extracts creation date from metatag, or from file creation date (Unix timestamp: the number of seconds since 1 January 1970)
    /// - copies all ingested pdf files to "files" subdirectory in index
    /// # Arguments
    /// * `file_path` - Path to the file (fallback, if title and date can't be extracted)
    /// * `file_date` - File creation date (Unix timestamp: the number of seconds since 1 January 1970) (fallback, if date can't be extracted)
    /// * `file_bytes` - Byte array of the file
    async fn index_pdf_bytes(
        &self,
        file_path: &Path,
        file_date: i64,
        file_bytes: &[u8],
    ) -> Result<(), String> {
        println!("pdf feature flag not enabled");
        Err("pdf feature flag not enabled".to_string())
    }
}

/// Index PDF file from local disk or byte array.
/// - converts pdf to text and indexes it
/// - extracts title from metatag, or first line of text, or from filename
/// - extracts creation date from metatag, or from file creation date (Unix timestamp: the number of seconds since 1 January 1970)
/// - copies all ingested pdf files to "files" subdirectory in index
/// # Arguments
/// * `file_path` - Path to the file (fallback, if title and date can't be extracted)
/// * `file_date` - File creation date (Unix timestamp: the number of seconds since 1 January 1970) (fallback, if date can't be extracted)
/// * `file` - FileType::Path or FileType::Bytes
/// * `pdf` - pdfium_render::prelude::PdfDocument
#[allow(clippy::too_many_arguments)]
#[allow(async_fn_in_trait)]
trait IndexPdf {
    async fn index_pdf(
        &self,
        file_path: &Path,
        file_size: usize,
        file_date: i64,
        file: FileType,
        pdf: PdfDocument<'_>,
    );
}

#[cfg(feature = "pdf")]
impl IndexPdf for IndexArc {
    /// Index PDF file from local disk or byte array.
    /// - converts pdf to text and indexes it
    /// - extracts title from metatag, or first line of text, or from filename
    /// - extracts creation date from metatag, or from file creation date (Unix timestamp: the number of seconds since 1 January 1970)
    /// - copies all ingested pdf files to "files" subdirectory in index
    async fn index_pdf(
        &self,
        file_path: &Path,
        file_size: usize,
        file_date: i64,
        file: FileType,
        pdf: PdfDocument<'_>,
    ) {
        let mut text = String::with_capacity(file_size);

        pdf.pages().iter().for_each(|page| {
            text.push_str(&page.text().unwrap().all());
            text.push_str(" \n");
        });

        if text.is_empty() {
            println!("can't extract text from PDF {}", file_path.display(),);
        } else {
            let meta = pdf.metadata();

            let title = if let Some(title) = meta.get(PdfDocumentMetadataTagType::Title) {
                title.value().to_owned()
            } else {
                let mut i = 0;
                let mut lines = text.lines();
                loop {
                    i += 1;
                    if let Some(title) = lines.next() {
                        if title.trim().len() > 1 {
                            break truncate(title, 160).trim().to_owned();
                        } else if i < 10 {
                            continue;
                        }
                    }

                    break file_path
                        .file_stem()
                        .unwrap()
                        .to_string_lossy()
                        .to_string()
                        .replace("_", "");
                }
            };

            let mut creation_timestamp =
                if let Some(date) = meta.get(PdfDocumentMetadataTagType::CreationDate) {
                    let mut date_string = if date.value().starts_with("D:") {
                        &date.value()[2..]
                    } else {
                        &date.value()[0..]
                    };

                    if date_string.len() > 14
                        && date_string
                            .chars()
                            .nth(14)
                            .unwrap()
                            .eq_ignore_ascii_case(&'z')
                    {
                        date_string = &date_string[0..14];
                    }

                    if date_string.len() == 14
                        || date_string.len() == 19
                        || date_string.len() == 20
                        || date_string.len() == 21
                    {
                        let mut date_string2 = String::with_capacity(23);
                        date_string2.push_str(&date_string[0..4]);
                        date_string2.push('-');
                        date_string2.push_str(&date_string[4..6]);
                        date_string2.push('-');
                        date_string2.push_str(&date_string[6..8]);
                        date_string2.push('T');
                        date_string2.push_str(&date_string[8..10]);
                        date_string2.push(':');
                        date_string2.push_str(&date_string[10..12]);
                        date_string2.push(':');
                        date_string2.push_str(&date_string[12..14]);
                        if date_string.len() == 14 {
                            date_string2.push_str("+00:00")
                        } else if date_string.chars().nth(17).unwrap() == '\'' {
                            date_string2.push_str(&date_string[14..17]);
                            date_string2.push(':');
                            date_string2.push_str(&date_string[18..20]);
                        } else {
                            date_string2.push_str(&date_string[14..17]);
                            date_string2.push(':');
                            date_string2.push_str(&date_string[17..19]);
                        }

                        if let Ok(date) = DateTime::parse_from_rfc3339(&date_string2) {
                            date.timestamp()
                        } else {
                            file_date
                        }
                    } else if let Ok(date) =
                        NaiveDateTime::parse_from_str(date.value(), "%a %b %e %H:%M:%S %Y")
                            .map(|ndt| Utc.from_utc_datetime(&ndt))
                    {
                        date.timestamp()
                    } else if let Ok(date) =
                        NaiveDateTime::parse_from_str(date.value(), "%Y/%m/%d %H:%M:%S")
                            .map(|ndt| Utc.from_utc_datetime(&ndt))
                    {
                        date.timestamp()
                    } else if let Ok(date) =
                        NaiveDateTime::parse_from_str(date.value(), "%m/%e/%Y %H:%M:%S")
                            .map(|ndt| Utc.from_utc_datetime(&ndt))
                    {
                        date.timestamp()
                    } else {
                        file_date
                    }
                } else {
                    file_date
                };

            if creation_timestamp > Utc::now().timestamp() || creation_timestamp < 0 {
                creation_timestamp = file_date;
            }

            let document: Document = HashMap::from([
                ("title".to_string(), json!(title)),
                ("body".to_string(), json!(text)),
                ("url".to_string(), json!(&file_path.display().to_string())),
                ("date".to_string(), json!(creation_timestamp)),
            ]);

            self.index_document(document, file).await;

            let date_time = Utc.timestamp_opt(creation_timestamp, 0).unwrap();
            println!(
                "indexed {} {} {} {}",
                date_time.format("%d/%m/%Y %H:%M"),
                file_path.display(),
                text.len().to_formatted_string(&Locale::en),
                title
            );
        }
    }
}

#[cfg(not(feature = "pdf"))]
impl IndexPdf for IndexArc {
    /// Index PDF file from local disk or byte array.
    /// - converts pdf to text and indexes it
    /// - extracts title from metatag, or first line of text, or from filename
    /// - extracts creation date from metatag, or from file creation date (Unix timestamp: the number of seconds since 1 January 1970)
    /// - copies all ingested pdf files to "files" subdirectory in index
    async fn index_pdf(
        &self,
        file_path: &Path,
        file_size: usize,
        file_date: i64,
        file: FileType,
        pdf: PdfDocument<'_>,
    ) {
        println!("pdf feature flag not enabled");
    }
}

pub(crate) async fn path_recurse(
    index_arc: &Arc<RwLock<Index>>,
    data_path: &Path,
    docid: &mut usize,
) {
    for entry in WalkDir::new(data_path) {
        let entry = entry.unwrap();
        let path = entry.path();

        let md = metadata(path).unwrap();
        if md.is_file()
            && let Some(extension) = path.extension().and_then(OsStr::to_str)
            && extension.to_lowercase() == "pdf"
            && index_arc.index_pdf_file(path).await.is_ok()
        {
            *docid += 1;
        };
    }
}

/// Index PDF files from local disk directory and sub-directories or from file.
/// - converts pdf to text and indexes it
/// - extracts title from metatag, or first line of text, or from filename
/// - extracts creation date from metatag, or from file creation date (Unix timestamp: the number of seconds since 1 January 1970)
/// - copies all ingested pdf files to "files" subdirectory in index
/// # Arguments
/// * `file_path` - Path to the file
#[allow(clippy::too_many_arguments)]
#[allow(async_fn_in_trait)]
pub trait IngestPdf {
    /// Index PDF files from local disk directory and sub-directories or from file.
    /// - converts pdf to text and indexes it
    /// - extracts title from metatag, or first line of text, or from filename
    /// - extracts creation date from metatag, or from file creation date (Unix timestamp: the number of seconds since 1 January 1970)
    /// - copies all ingested pdf files to "files" subdirectory in index
    /// # Arguments
    /// * `file_path` - Path to the file
    async fn ingest_pdf(&mut self, file_path: &Path);
}

#[cfg(feature = "pdf")]
impl IngestPdf for IndexArc {
    /// Index PDF files from local disk directory and sub-directories or from file.
    /// - converts pdf to text and indexes it
    /// - extracts title from metatag, or first line of text, or from filename
    /// - extracts creation date from metatag, or from file creation date (Unix timestamp: the number of seconds since 1 January 1970)
    /// - copies all ingested pdf files to "files" subdirectory in index
    async fn ingest_pdf(&mut self, data_path: &Path) {
        if pdfium_option.is_some() {
            match data_path.exists() {
                true => {
                    println!("ingesting PDF from: {}", data_path.display());

                    let start_time = Instant::now();
                    let mut docid = 0usize;

                    let index_ref = self.read().await;
                    drop(index_ref);

                    let md = metadata(data_path).unwrap();
                    if md.is_file() {
                        if let Some(extension) = Path::new(&data_path.display().to_string())
                            .extension()
                            .and_then(OsStr::to_str)
                            && extension.to_lowercase() == "pdf"
                            && self.index_pdf_file(data_path).await.is_ok()
                        {
                            docid += 1;
                        }
                    } else {
                        path_recurse(self, data_path, &mut docid).await;
                    }

                    self.commit().await;

                    let elapsed_time = start_time.elapsed().as_nanos();

                    println!(
                        "{}: docs {}  docs/sec {}  docs/day {} minutes {:.2} seconds {}",
                        "Indexing finished".green(),
                        docid.to_formatted_string(&Locale::en),
                        (docid as u128 * 1_000_000_000 / elapsed_time)
                            .to_formatted_string(&Locale::en),
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
        } else {
            println!(
                "Pdfium library not found: download and copy into the same folder as the seekstorm_server.exe: https://github.com/bblanchon/pdfium-binaries"
            )
        }
    }
}

#[cfg(not(feature = "pdf"))]
impl IngestPdf for IndexArc {
    /// Index PDF files from local disk directory and sub-directories or from file.
    /// - converts pdf to text and indexes it
    /// - extracts title from metatag, or first line of text, or from filename
    /// - extracts creation date from metatag, or from file creation date (Unix timestamp: the number of seconds since 1 January 1970)
    /// - copies all ingested pdf files to "files" subdirectory in index
    async fn ingest_pdf(&mut self, data_path: &Path) {
        println!("pdf feature flag not enabled");
    }
}

/// Ingest local data files in [JSON](https://en.wikipedia.org/wiki/JSON), [Newline-delimited JSON](https://github.com/ndjson/ndjson-spec) (ndjson), and [Concatenated JSON](https://en.wikipedia.org/wiki/JSON_streaming) formats via console command.  
/// The document ingestion is streamed without loading the whole document vector into memory to allwow for unlimited file size while keeping RAM consumption low.
#[allow(clippy::too_many_arguments)]
#[allow(async_fn_in_trait)]
pub trait IngestJson {
    /// Ingest local data files in [JSON](https://en.wikipedia.org/wiki/JSON), [Newline-delimited JSON](https://github.com/ndjson/ndjson-spec) (ndjson), and [Concatenated JSON](https://en.wikipedia.org/wiki/JSON_streaming) formats via console command.  
    /// The document ingestion is streamed without loading the whole document vector into memory to allwow for unlimited file size while keeping RAM consumption low.
    async fn ingest_json(&mut self, data_path: &Path);
}

impl IngestJson for IndexArc {
    /// Ingest local data files in [JSON](https://en.wikipedia.org/wiki/JSON), [Newline-delimited JSON](https://github.com/ndjson/ndjson-spec) (ndjson), and [Concatenated JSON](https://en.wikipedia.org/wiki/JSON_streaming) formats via console command.  
    /// The document ingestion is streamed without loading the whole document vector into memory to allwow for unlimited file size while keeping RAM consumption low.
    async fn ingest_json(&mut self, data_path: &Path) {
        match data_path.exists() {
            true => {
                println!("ingesting data from: {}", data_path.display());

                let start_time = Instant::now();
                let mut docid: i64 = 0;

                let index_arc_clone2 = self.clone();
                let index_ref = index_arc_clone2.read().await;
                drop(index_ref);

                let index_arc_clone = self.clone();
                let file = File::open(data_path).unwrap();
                let mut reader = BufReader::new(file);

                let is_vector = read_skipping_ws(&mut reader).unwrap() == b'[';

                if !is_vector {
                    println!("Newline-delimited JSON (ndjson) or Concatenated JSON detected");
                    reader.seek_relative(-1).unwrap();

                    for doc_object in Deserializer::from_reader(reader).into_iter::<Document>() {
                        let index_arc_clone_clone = index_arc_clone.clone();

                        index_arc_clone_clone
                            .index_document(doc_object.unwrap(), FileType::None)
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
                                    .index_document(doc_object.unwrap(), FileType::None)
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

                self.commit().await;

                let elapsed_time = start_time.elapsed().as_nanos();

                let date: DateTime<Utc> = DateTime::from(SystemTime::now());

                println!(
                    "{}: {} shards {}  ngrams {:08b}  docs {}  docs/sec {}  docs/day {} minutes {:.2} seconds {}",
                    "Indexing finished".green(),
                    date.format("%D"),
                    index_arc_clone.read().await.shard_count().await,
                    self.read().await.meta.ngram_indexing,
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
}

#[allow(async_fn_in_trait)]
/// Ingest local data files in [CSV](https://en.wikipedia.org/wiki/Comma-separated_values).  
/// The document ingestion is streamed without loading the whole document vector into memory to allwow for unlimited file size while keeping RAM consumption low.
pub trait IngestCsv {
    /// Ingest local data files in [CSV](https://en.wikipedia.org/wiki/Comma-separated_values).  
    /// The document ingestion is streamed without loading the whole document vector into memory to allwow for unlimited file size while keeping RAM consumption low.
    async fn ingest_csv(
        &mut self,
        data_path: &Path,
        has_header: bool,
        quoting: bool,
        delimiter: u8,
        skip_docs: Option<usize>,
        num_docs: Option<usize>,
    );
}

impl IngestCsv for IndexArc {
    /// Ingest local data files in [CSV](https://en.wikipedia.org/wiki/Comma-separated_values).  
    /// The document ingestion is streamed without loading the whole document vector into memory to allwow for unlimited file size while keeping RAM consumption low.
    async fn ingest_csv(
        &mut self,
        data_path: &Path,
        has_header: bool,
        quoting: bool,
        delimiter: u8,
        skip_docs: Option<usize>,
        num_docs: Option<usize>,
    ) {
        match data_path.exists() {
            true => {
                println!("ingesting data from: {}", data_path.display());

                let start_time = Instant::now();
                let mut docid: usize = 0;

                let index_arc_clone2 = self.clone();
                let index_ref = index_arc_clone2.read().await;
                drop(index_ref);

                let index_arc_clone = self.clone();
                let index_arc_clone_clone = index_arc_clone.clone();

                let index_ref = index_arc_clone.read().await;
                let mut schema_vec: Vec<String> = vec!["".to_string(); index_ref.schema_map.len()];
                for (key, value) in index_ref.schema_map.iter() {
                    schema_vec[value.field_id] = key.clone();
                }
                drop(index_ref);

                let mut rdr = ReaderBuilder::new()
                    .has_headers(has_header)
                    .quoting(quoting)
                    .delimiter(delimiter)
                    .terminator(Terminator::CRLF)
                    .from_path(data_path)
                    .unwrap();

                let skip = skip_docs.unwrap_or(0);
                let max = num_docs.unwrap_or(usize::MAX);
                let mut i: usize = 0;
                let mut record = csv::StringRecord::new();
                while rdr.read_record(&mut record).unwrap() && docid < max {
                    if i < skip {
                        i += 1;
                        continue;
                    }
                    let mut document: Document = HashMap::new();
                    for (i, element) in record.iter().enumerate() {
                        document.insert(schema_vec[i].clone(), json!(element));
                    }

                    index_arc_clone_clone
                        .index_document(document, FileType::None)
                        .await;
                    docid += 1;
                }

                self.commit().await;

                let elapsed_time = start_time.elapsed().as_nanos();

                println!(
                    "{}: docs {}  docs/sec {}  docs/day {} minutes {:.2} seconds {}",
                    "Indexing finished".green(),
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
}
