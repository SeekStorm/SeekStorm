use std::{path::Path, time::Duration};

use reqwest::StatusCode;
use seekstorm::{
    index::{
        ApikeyQuotaObject, CreateIndexRequest, DeleteApikeyRequest, Document, GetDocumentRequest,
        GetIteratorRequest, IndexResponseObject, SearchRequestObject, SearchResultObject,
    },
    iterator::IteratorResult,
};

/// RestClient is a wrapper around reqwest::Client that provides methods for interacting with the SeekStorm server API.
pub struct RestClient {
    /// The reqwest::Client used for making HTTP requests.
    pub client: reqwest::Client,
}

impl Default for RestClient {
    fn default() -> Self {
        Self::new()
    }
}

impl RestClient {
    /// Creates a new instance of RestClient with a configured reqwest::Client.
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .tcp_nodelay(true)
                .pool_idle_timeout(Duration::from_secs(90))
                .build()
                .expect("failed to build reqwest client"),
        }
    }

    /// Live
    /// Returns a live message with the SeekStorm server version.
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    ///
    /// Returns:
    /// * `String`: The live message from the server.
    pub async fn live(&self, base_url: &str) -> Result<String, (StatusCode, String)> {
        let url = format!("{}/api/v1/live", base_url);
        if let Ok(response) = self.client.get(&url).send().await {
            if let status = response.status()
                && let Ok(body) = response.text().await
            {
                if status.is_success() {
                    Ok(body)
                } else {
                    Err((status, body))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }

    /// Create API Key
    /// Creates an API key and returns the Base64 encoded API key.  
    /// Expects the Base64 encoded master API key in the header.  
    /// Use the **master API key displayed** in the server console at startup.
    ///  
    /// WARNING: make sure to set the MASTER_KEY_SECRET environment variable to a secret, otherwise your generated API keys will be compromised.  
    /// For development purposes you may also use the SeekStorm server console command 'create' to create an demo API key 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA='.
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    /// * `master_apikey`: The Base64 encoded master API key.
    /// * `api_key_quota_object`: The API key quota object.
    ///
    /// Returns:
    /// * `String`: The Base64 encoded API key.
    pub async fn create_apikey(
        &self,
        base_url: &str,
        master_apikey: &str,
        api_key_quota_object: &ApikeyQuotaObject,
    ) -> Result<String, (StatusCode, String)> {
        let url = format!("{}/api/v1/apikey", base_url);
        if let Ok(response) = self
            .client
            .post(&url)
            .json(&api_key_quota_object)
            .header("apikey", master_apikey)
            .send()
            .await
        {
            if let status = response.status()
                && let Ok(body) = response.text().await
            {
                if status.is_success() {
                    Ok(body)
                } else {
                    Err((status, body))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }

    /// Delete API Key
    /// Deletes an API key and returns the number of remaining API keys.
    /// Expects the Base64 encoded master API key in the header.
    /// WARNING: This will delete all indices and documents associated with the API key.
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    /// * `apikey_base64`: The Base64 encoded API key to delete.
    /// * `master_apikey_base64`: The Base64 encoded master API key.
    ///
    /// Returns:
    /// * `u64`: The number of remaining API keys.
    pub async fn delete_apikey(
        &self,
        base_url: &str,
        apikey_base64: &str,
        master_apikey_base64: &str,
    ) -> Result<u64, (StatusCode, String)> {
        let delete_apikey_request = DeleteApikeyRequest {
            apikey_base64: apikey_base64.to_string(),
        };

        let url = format!("{}/api/v1/apikey", base_url);
        if let Ok(response) = self
            .client
            .delete(&url)
            .json(&delete_apikey_request)
            .header("apikey", master_apikey_base64)
            .send()
            .await
        {
            if let status = response.status()
                && let Ok(body) = response.text().await
            {
                if status.is_success() {
                    body.parse::<u64>().map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to parse response as u64".to_string(),
                        )
                    })
                } else {
                    Err((status, body))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }

    /// Get API Key Info
    /// Get info about all indices associated with the specified API key
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    /// * `apikey_base64`: The Base64 encoded API key.
    ///
    /// Returns:
    /// * `Vec<IndexResponseObject>`: A vector of index response objects.
    pub async fn get_apikey_info(
        &self,
        base_url: &str,
        apikey_base64: &str,
    ) -> Result<Vec<IndexResponseObject>, (StatusCode, String)> {
        let url = format!("{}/api/v1/apikey", base_url);
        if let Ok(response) = self
            .client
            .get(&url)
            .header("apikey", apikey_base64)
            .send()
            .await
        {
            if let status = response.status()
                && let Ok(response_bytes) = response.bytes().await
            {
                if status.is_success() {
                    if let Ok(index_response_object_vec) =
                        serde_json::from_slice::<Vec<IndexResponseObject>>(&response_bytes)
                    {
                        Ok(index_response_object_vec)
                    } else {
                        Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Deserialization error".to_string(),
                        ))
                    }
                } else {
                    let response_string = str::from_utf8(&response_bytes).unwrap();
                    Err((status, response_string.to_string()))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }

    /// Create Index
    /// Create an index within the directory associated with the specified API key and return the index_id.
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    /// * `apikey_base64`: The Base64 encoded API key.
    /// * `create_index_request`: The request object containing index creation details.
    ///
    /// Returns:
    /// * `u64`: The ID of the created index.
    pub async fn create_index(
        &self,
        base_url: &str,
        apikey_base64: &str,
        create_index_request: &CreateIndexRequest,
    ) -> Result<u64, (StatusCode, String)> {
        let url = format!("{}/api/v1/index", base_url);
        if let Ok(response) = self
            .client
            .post(&url)
            .json(&create_index_request)
            .header("apikey", apikey_base64)
            .send()
            .await
        {
            if let status = response.status()
                && let Ok(body) = response.text().await
            {
                if status.is_success() {
                    body.parse::<u64>().map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to parse response as u64".to_string(),
                        )
                    })
                } else {
                    Err((status, body))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }

    /// Delete Index
    /// Delete an index within the directory associated with the specified API key and return the number of remaining indices.
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    /// * `apikey_base64`: The Base64 encoded API key.
    /// * `index_id`: The ID of the index to delete.
    ///
    /// Returns:
    /// * `u64`: The number of remaining indices.
    pub async fn delete_index(
        &self,
        base_url: &str,
        apikey_base64: &str,
        index_id: u64,
    ) -> Result<u64, (StatusCode, String)> {
        let url = format!("{}/api/v1/index/{}", base_url, index_id);
        if let Ok(response) = self
            .client
            .delete(&url)
            .header("apikey", apikey_base64)
            .send()
            .await
        {
            if let status = response.status()
                && let Ok(body) = response.text().await
            {
                if status.is_success() {
                    body.parse::<u64>().map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to parse response as u64".to_string(),
                        )
                    })
                } else {
                    Err((status, body))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }

    /// Clear index
    /// Clearindex with index_id
    /// Immediately effective, independent of commit.
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    /// * `apikey_base64`: The Base64 encoded API key.
    /// * `index_id`: The ID of the index to clear.
    ///
    /// Returns:
    /// * `usize`: The number of documents remaining in the index.
    pub async fn clear_index(
        &self,
        base_url: &str,
        apikey_base64: &str,
        index_id: u64,
    ) -> Result<usize, (StatusCode, String)> {
        let url = format!("{}/api/v1/index/{}/doc", base_url, index_id);
        let request_object = "clear";
        if let Ok(response) = self
            .client
            .delete(&url)
            .header("apikey", apikey_base64)
            .json(&request_object)
            .send()
            .await
        {
            if let status = response.status()
                && let Ok(body) = response.text().await
            {
                if status.is_success() {
                    body.parse::<usize>().map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to parse response as usize".to_string(),
                        )
                    })
                } else {
                    Err((status, body))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }

    /// Commit Index
    /// Commit moves indexed documents from the intermediate uncompressed data structure (array lists/HashMap, queryable by realtime search) in RAM
    /// to the final compressed data structure (roaring bitmap) on Mmap or disk -
    /// which is persistent, more compact, with lower query latency and allows search with realtime=false.
    /// Commit is invoked automatically each time 64K documents are newly indexed as well as on close_index (e.g. server quit).
    /// There is no way to prevent this automatic commit by not manually invoking it.
    /// But commit can also be invoked manually at any time at any number of newly indexed documents.
    /// commit is a **hard commit** for persistence on disk. A **soft commit** for searchability
    /// is invoked implicitly with every index_doc,
    /// i.e. the document can immediately searched and included in the search results
    /// if it matches the query AND the query paramter realtime=true is enabled.
    /// **Use commit with caution, as it is an expensive operation**.
    /// **Usually, there is no need to invoke it manually**, as it is invoked automatically every 64k documents and when the index is closed with close_index.
    /// Before terminating the program, always call close_index (commit), otherwise all documents indexed since last (manual or automatic) commit are lost.
    /// There are only 2 reasons that justify a manual commit:
    /// 1. if you want to search newly indexed documents without using realtime=true for search performance reasons or
    /// 2. if after indexing new documents there won't be more documents indexed (for some time),
    ///    so there won't be (soon) a commit invoked automatically at the next 64k threshold or close_index,
    ///    but you still need immediate persistence guarantees on disk to protect against data loss in the event of a crash.
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    /// * `apikey_base64`: The Base64 encoded API key.
    /// * `index_id`: The ID of the index to commit.
    ///
    /// Returns:
    /// * `u64`: The number of documents indexed after the commit.
    pub async fn commit_index(
        &self,
        base_url: &str,
        apikey_base64: &str,
        index_id: u64,
    ) -> Result<u64, (StatusCode, String)> {
        let url = format!("{}/api/v1/index/{}", base_url, index_id);
        if let Ok(response) = self
            .client
            .patch(&url)
            .header("apikey", apikey_base64)
            .send()
            .await
        {
            if let status = response.status()
                && let Ok(body) = response.text().await
            {
                if status.is_success() {
                    body.parse::<u64>().map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to parse response as u64".to_string(),
                        )
                    })
                } else {
                    Err((status, body))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }

    /// Get Index Info
    /// Get index Info from index with index_id
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    /// * `apikey_base64`: The Base64 encoded API key.
    /// * `index_id`: The ID of the index to retrieve information from.
    ///
    /// Returns:
    /// * `IndexResponseObject`: The index information object.
    pub async fn get_index_info(
        &self,
        base_url: &str,
        apikey_base64: &str,
        index_id: u64,
    ) -> Result<IndexResponseObject, (StatusCode, String)> {
        let url = format!("{}/api/v1/index/{}", base_url, index_id);
        if let Ok(response) = self
            .client
            .get(&url)
            .header("apikey", apikey_base64)
            .send()
            .await
        {
            if let status = response.status()
                && let Ok(response_bytes) = response.bytes().await
            {
                if status.is_success() {
                    serde_json::from_slice::<IndexResponseObject>(&response_bytes).map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Deserialization error".to_string(),
                        )
                    })
                } else {
                    let response_string = str::from_utf8(&response_bytes).unwrap();
                    Err((status, response_string.to_string()))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }

    /// Index Document
    /// Index a JSON document or an array of JSON documents (bulk), each consisting of arbitrary key-value pairs to the index with the specified apikey and index_id, and return the number of indexed docs.
    /// Index documents enables true real-time search (as opposed to near realtime.search):
    /// When in query_index the parameter `realtime` is set to `true` then indexed, but uncommitted documents are immediately included in the search results, without requiring a commit or refresh.
    /// Therefore a explicit commit_index is almost never required, as it is invoked automatically after 64k documents are indexed or on close_index for persistence.
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    /// * `apikey_base64`: The Base64 encoded API key.
    /// * `index_id`: The ID of the index to which the document should be indexed.
    /// * `document`: The document to be indexed, represented as a `Document` struct.
    ///
    /// Returns:
    /// * `usize`: The number of documents indexed.
    pub async fn index_document(
        &self,
        base_url: &str,
        apikey_base64: &str,
        index_id: u64,
        document: &Document,
    ) -> Result<usize, (StatusCode, String)> {
        let url = format!("{}/api/v1/index/{}/doc", base_url, index_id);
        if let Ok(response) = self
            .client
            .post(&url)
            .json(document)
            .header("apikey", apikey_base64)
            .send()
            .await
        {
            if let status = response.status()
                && let Ok(body) = response.text().await
            {
                if status.is_success() {
                    body.parse::<usize>().map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to parse response as usize".to_string(),
                        )
                    })
                } else {
                    Err((status, body))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }

    /// Index Documents
    /// Index a JSON document or an array of JSON documents (bulk), each consisting of arbitrary key-value pairs to the index with the specified apikey and index_id, and return the number of indexed docs.
    /// Index documents enables true real-time search (as opposed to near realtime.search):
    /// When in query_index the parameter `realtime` is set to `true` then indexed, but uncommitted documents are immediately included in the search results, without requiring a commit or refresh.
    /// Therefore a explicit commit_index is almost never required, as it is invoked automatically after 64k documents are indexed or on close_index for persistence.
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    /// * `apikey_base64`: The Base64 encoded API key.
    /// * `index_id`: The ID of the index to which the documents should be indexed.
    /// * `documents`: The documents to be indexed, represented as a slice of `Document` structs.
    ///
    /// Returns:
    /// * `usize`: The number of documents indexed.
    pub async fn index_documents(
        &self,
        base_url: &str,
        apikey_base64: &str,
        index_id: u64,
        documents: &[Document],
    ) -> Result<usize, (StatusCode, String)> {
        let url = format!("{}/api/v1/index/{}/doc", base_url, index_id);
        if let Ok(response) = self
            .client
            .post(&url)
            .json(documents)
            .header("apikey", apikey_base64)
            .send()
            .await
        {
            if let status = response.status()
                && let Ok(body) = response.text().await
            {
                if status.is_success() {
                    body.parse::<usize>().map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to parse response as usize".to_string(),
                        )
                    })
                } else {
                    Err((status, body))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }

    /// Index PDF file
    /// Index PDF file (byte array) to the index with the specified apikey and index_id, and return the number of indexed docs.
    /// - Converts PDF to a JSON document with "title", "body", "url" and "date" fields and indexes it.
    /// - extracts title from metatag, or first line of text, or from filename
    /// - extracts creation date from metatag, or from file creation date (Unix timestamp: the number of seconds since 1 January 1970)
    /// - copies all ingested pdf files to "files" subdirectory in index
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    /// * `apikey_base64`: The Base64 encoded API key.
    /// * `index_id`: The ID of the index to which the PDF file should be indexed.
    /// * `file_path`: The path to the PDF file to be indexed.
    /// * `file_date`: The creation date of the PDF file (Unix timestamp).
    /// * `document`: The PDF file content as a byte array.
    ///
    /// Returns:
    /// * `usize`: The number of documents indexed.
    pub async fn index_pdf(
        &self,
        base_url: &str,
        apikey_base64: &str,
        index_id: u64,
        file_path: &Path,
        file_date: i64,
        document: Vec<u8>,
    ) -> Result<usize, (StatusCode, String)> {
        let url = format!("{}/api/v1/index/{}/file", base_url, index_id);
        if let Ok(response) = self
            .client
            .post(&url)
            .body(document)
            .header("apikey", apikey_base64)
            .header("file", file_path.to_string_lossy().to_string())
            .header("date", file_date.to_string())
            .send()
            .await
        {
            if let status = response.status()
                && let Ok(body) = response.text().await
            {
                if status.is_success() {
                    body.parse::<usize>().map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to parse response as usize".to_string(),
                        )
                    })
                } else {
                    Err((status, body))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }

    /// Get PDF file
    /// Get PDF file from index with index_id
    /// ⚠️ Use search or get_iterator first to obtain a valid doc_id. Document IDs are not guaranteed to be continuous and gapless!
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    /// * `apikey_base64`: The Base64 encoded API key.
    /// * `index_id`: The ID of the index from which the PDF file should be retrieved.
    /// * `doc_id`: The ID of the document to be retrieved.
    ///
    /// Returns:
    /// * `Vec<u8>`: The PDF file content as a byte array.
    pub async fn get_pdf(
        &self,
        base_url: &str,
        apikey_base64: &str,
        index_id: u64,
        doc_id: u64,
    ) -> Result<Vec<u8>, (StatusCode, String)> {
        let url = format!("{}/api/v1/index/{}/file/{}", base_url, index_id, doc_id);
        if let Ok(response) = self
            .client
            .get(&url)
            .header("apikey", apikey_base64)
            .send()
            .await
        {
            if let status = response.status()
                && let Ok(response_bytes) = response.bytes().await
            {
                if status.is_success() {
                    Ok(response_bytes.to_vec())
                } else {
                    let response_string = str::from_utf8(&response_bytes).unwrap();
                    Err((status, response_string.to_string()))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }

    /// Get Document
    /// Get document from index with index_id
    /// ⚠️ Use search or get_iterator first to obtain a valid doc_id. Document IDs are not guaranteed to be continuous and gapless!
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    /// * `apikey_base64`: The Base64 encoded API key.
    /// * `index_id`: The ID of the index from which the document should be retrieved
    /// * `doc_id`: The ID of the document to be retrieved.
    /// * `get_document_request`: The request object containing document retrieval details.
    ///
    /// Returns:
    /// * `Document`: The retrieved document.
    pub async fn get_document(
        &self,
        base_url: &str,
        apikey_base64: &str,
        index_id: u64,
        doc_id: u64,
        get_document_request: &GetDocumentRequest,
    ) -> Result<Document, (StatusCode, String)> {
        let url = format!("{}/api/v1/index/{}/doc/{}", base_url, index_id, doc_id);
        if let Ok(response) = self
            .client
            .get(&url)
            .json(get_document_request)
            .header("apikey", apikey_base64)
            .send()
            .await
        {
            if let status = response.status()
                && let Ok(response_bytes) = response.bytes().await
            {
                if status.is_success() {
                    serde_json::from_slice::<Document>(&response_bytes).map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Deserialization error".to_string(),
                        )
                    })
                } else {
                    let response_string = str::from_utf8(&response_bytes).unwrap();
                    Err((status, response_string.to_string()))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }

    /// Update Document
    /// Update a JSON document or an array of JSON documents (bulk), each consisting of arbitrary key-value pairs to the index with the specified apikey and index_id, and return the number of indexed docs.
    /// Update document is a combination of delete_document and index_document.
    /// All current limitations of delete_document apply.
    /// Update documents enables true real-time search (as opposed to near realtime.search):
    /// When in query_index the parameter `realtime` is set to `true` then indexed, but uncommitted documents are immediately included in the search results, without requiring a commit or refresh.
    /// Therefore a explicit commit_index is almost never required, as it is invoked automatically after 64k documents are indexed or on close_index for persistence.
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    /// * `apikey_base64`: The Base64 encoded API key.
    /// * `index_id`: The ID of the index to update the document in.
    /// * `docid_document`: A tuple containing the document ID and the document to be updated.
    ///
    /// Returns:
    /// * `usize`: The number of indexed documents.
    pub async fn update_document(
        &self,
        base_url: &str,
        apikey_base64: &str,
        index_id: u64,
        docid_document: (u64, Document),
    ) -> Result<usize, (StatusCode, String)> {
        let url = format!("{}/api/v1/index/{}/doc", base_url, index_id);
        if let Ok(response) = self
            .client
            .patch(&url)
            .json(&docid_document)
            .header("apikey", apikey_base64)
            .send()
            .await
        {
            if let status = response.status()
                && let Ok(body) = response.text().await
            {
                if status.is_success() {
                    body.parse::<usize>().map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to parse response as usize".to_string(),
                        )
                    })
                } else {
                    Err((status, body))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }

    /// Update Documents
    /// Update a JSON document or an array of JSON documents (bulk), each consisting of arbitrary key-value pairs to the index with the specified apikey and index_id, and return the number of indexed docs.
    /// Update document is a combination of delete_document and index_document.
    /// All current limitations of delete_document apply.
    /// Update documents enables true real-time search (as opposed to near realtime.search):
    /// When in query_index the parameter `realtime` is set to `true` then indexed, but uncommitted documents are immediately included in the search results, without requiring a commit or refresh.
    /// Therefore a explicit commit_index is almost never required, as it is invoked automatically after 64k documents are indexed or on close_index for persistence.
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    /// * `apikey_base64`: The Base64 encoded API key.
    /// * `index_id`: The ID of the index to update the documents in.
    /// * `docid_document_vec`: A vector of tuples, each containing a document ID and the corresponding document to be updated.
    ///
    /// Returns:
    /// * `usize`: The number of indexed documents.
    pub async fn update_documents(
        &self,
        base_url: &str,
        apikey_base64: &str,
        index_id: u64,
        docid_document_vec: Vec<(u64, Document)>,
    ) -> Result<usize, (StatusCode, String)> {
        let url = format!("{}/api/v1/index/{}/doc", base_url, index_id);
        if let Ok(response) = self
            .client
            .patch(&url)
            .json(&docid_document_vec)
            .header("apikey", apikey_base64)
            .send()
            .await
        {
            if let status = response.status()
                && let Ok(body) = response.text().await
            {
                if status.is_success() {
                    body.parse::<usize>().map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to parse response as usize".to_string(),
                        )
                    })
                } else {
                    Err((status, body))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }

    /// Delete Document by document ID
    /// Delete document by document_id from index with index_id
    /// ⚠️ Use search or get_iterator first to obtain a valid doc_id. Document IDs are not guaranteed to be continuous and gapless!
    /// Immediately effective, independent of commit.
    /// Index space used by deleted documents is not reclaimed (until compaction is implemented), but result_count_total is updated.
    /// By manually deleting the delete.bin file the deleted documents can be recovered (until compaction).
    /// Deleted documents impact performance, especially but not limited to counting (Count, TopKCount). They also increase the size of the index (until compaction is implemented).
    /// For minimal query latency delete index and reindexing documents is preferred over deleting documents (until compaction is implemented).
    /// BM25 scores are not updated (until compaction is implemented), but the impact is minimal.
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    /// * `apikey_base64`: The Base64 encoded API key.
    /// * `index_id`: The ID of the index from which the document should be deleted.
    /// * `doc_id`: The ID of the document to be deleted.
    ///
    /// Returns:
    /// * `usize`: The number of indexed documents.
    pub async fn delete_document_by_docid(
        &self,
        base_url: &str,
        apikey_base64: &str,
        index_id: u64,
        doc_id: u64,
    ) -> Result<usize, (StatusCode, String)> {
        let url = format!("{}/api/v1/index/{}/doc/{}", base_url, index_id, doc_id);
        if let Ok(response) = self
            .client
            .delete(&url)
            .header("apikey", apikey_base64)
            .send()
            .await
        {
            if let status = response.status()
                && let Ok(body) = response.text().await
            {
                if status.is_success() {
                    body.parse::<usize>().map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to parse response as usize".to_string(),
                        )
                    })
                } else {
                    Err((status, body))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }

    /// Delete Document by document ID
    /// Delete document by document_id from index with index_id
    /// ⚠️ Use search or get_iterator first to obtain a valid doc_id. Document IDs are not guaranteed to be continuous and gapless!
    /// Immediately effective, independent of commit.
    /// Index space used by deleted documents is not reclaimed (until compaction is implemented), but result_count_total is updated.
    /// By manually deleting the delete.bin file the deleted documents can be recovered (until compaction).
    /// Deleted documents impact performance, especially but not limited to counting (Count, TopKCount). They also increase the size of the index (until compaction is implemented).
    /// For minimal query latency delete index and reindexing documents is preferred over deleting documents (until compaction is implemented).
    /// BM25 scores are not updated (until compaction is implemented), but the impact is minimal.
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    /// * `apikey_base64`: The Base64 encoded API key.
    /// * `index_id`: The ID of the index from which the document should be deleted.
    /// * `doc_id_vec`: A vector of document IDs to be deleted.
    ///
    /// Returns:
    /// * `usize`: The number of indexed documents.
    pub async fn delete_documents_by_docid(
        &self,
        base_url: &str,
        apikey_base64: &str,
        index_id: u64,
        doc_id_vec: Vec<u64>,
    ) -> Result<usize, (StatusCode, String)> {
        let url = format!("{}/api/v1/index/{}/doc", base_url, index_id);
        if let Ok(response) = self
            .client
            .delete(&url)
            .header("apikey", apikey_base64)
            .json(&doc_id_vec)
            .send()
            .await
        {
            if let status = response.status()
                && let Ok(body) = response.text().await
            {
                if status.is_success() {
                    body.parse::<usize>().map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to parse response as usize".to_string(),
                        )
                    })
                } else {
                    Err((status, body))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }

    /// Delete Document(s) by Query
    /// Delete document by document_id, by array of document_id (bulk), by query (SearchRequestObject) from index with index_id, or clear all documents from index.
    /// Immediately effective, independent of commit.
    /// Index space used by deleted documents is not reclaimed (until compaction is implemented), but result_count_total is updated.
    /// By manually deleting the delete.bin file the deleted documents can be recovered (until compaction).
    /// Deleted documents impact performance, especially but not limited to counting (Count, TopKCount). They also increase the size of the index (until compaction is implemented).
    /// For minimal query latency delete index and reindexing documents is preferred over deleting documents (until compaction is implemented).
    /// BM25 scores are not updated (until compaction is implemented), but the impact is minimal.
    /// Document ID can by obtained by search. When deleting by query (SearchRequestObject), it is advised to perform a dry run search first, to see which documents will be deleted.
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    /// * `apikey_base64`: The Base64 encoded API key.
    /// * `index_id`: The ID of the index from which the documents should be deleted.
    /// * `query`: The search query object specifying which documents to delete.
    ///
    /// Returns:
    /// * `usize`: The number of indexed documents.
    pub async fn delete_documents_by_query(
        &self,
        base_url: &str,
        apikey_base64: &str,
        index_id: u64,
        query: &SearchRequestObject,
    ) -> Result<usize, (StatusCode, String)> {
        let url = format!("{}/api/v1/index/{}/doc", base_url, index_id);
        if let Ok(response) = self
            .client
            .delete(&url)
            .json(query)
            .header("apikey", apikey_base64)
            .send()
            .await
        {
            if let status = response.status()
                && let Ok(body) = response.text().await
            {
                if status.is_success() {
                    body.parse::<usize>().map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to parse response as usize".to_string(),
                        )
                    })
                } else {
                    Err((status, body))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }

    /// Document iterator
    /// Document iterator via GET and POST are identical, only the way parameters are passed differ.
    /// The document iterator allows to iterate over all document IDs and documents in the entire index, forward or backward.
    /// It enables efficient sequential access to every document, even in very large indexes, without running a search.
    /// Paging through the index works without collecting document IDs to Min-heap in size-limited RAM first.
    /// The iterator guarantees that only valid document IDs are returned, even though document IDs are not strictly continuous.
    /// Document IDs can also be fetched in batches, reducing round trips and significantly improving performance, especially when using the REST API.
    /// Typical use cases include index export, conversion, analytics, audits, and inspection.
    /// Explanation of "eventually continuous" docid:
    /// In SeekStorm, document IDs become continuous over time. In a multi-sharded index, each shard maintains its own document ID space.
    /// Because documents are distributed across shards in a non-deterministic, load-dependent way, shard-local document IDs advance at different rates.
    /// When these are mapped to global document IDs, temporary gaps can appear.
    /// As a result, simply iterating from 0 to the total document count may encounter invalid IDs near the end.
    /// The Document Iterator abstracts this complexity and reliably returns only valid document IDs.
    /// # Parameters
    /// - docid=None, take>0: **skip first s document IDs**, then **take next t document IDs** of an index.
    /// - docid=None, take<0: **skip last s document IDs**, then **take previous t document IDs** of an index.
    /// - docid=Some, take>0: **skip next s document IDs**, then **take next t document IDs** of an index, relative to a given document ID, with end-of-index indicator.
    /// - docid=Some, take<0: **skip previous s document IDs**, then **take previous t document IDs**, relative to a given document ID, with start-of-index indicator.
    /// - take=0: does not make sense, that defies the purpose of get_iterator.
    /// - The sign of take indicates the direction of iteration: positive take for forward iteration, negative take for backward iteration.
    /// - The skip parameter is always positive, indicating the number of document IDs to skip before taking document IDs. The skip direction is determined by the sign of take too.
    /// - include_document: if true, the documents are also retrieved along with their document IDs.
    ///
    /// Next page:     take last  docid from previous result set, skip=1, take=+page_size
    /// Previous page: take first docid from previous result set, skip=1, take=-page_size
    /// Returns an IteratorResult, consisting of the number of actually skipped document IDs, and a list of taken document IDs and documents, sorted ascending).
    /// Detect end/begin of index during iteration: if returned vec.len() < requested take || if returned skip <requested skip
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    /// * `apikey_base64`: The Base64 encoded API key.
    /// * `index_id`: The ID of the index to iterate over.
    /// * `request`: The request object containing the parameters for the document iterator, including docid, skip, take, and include_document.
    ///
    /// Returns:
    /// * `IteratorResult`: The result of the iteration, including the number of skipped document IDs and the list of taken document IDs and documents.
    pub async fn document_iterator(
        &self,
        base_url: &str,
        apikey_base64: &str,
        index_id: u64,
        request: GetIteratorRequest,
    ) -> Result<IteratorResult, (StatusCode, String)> {
        let url = format!("{}/api/v1/index/{}/iterator", base_url, index_id);
        if let Ok(response) = self
            .client
            .post(&url)
            .header("apikey", apikey_base64)
            .json(&request)
            .send()
            .await
        {
            if let status = response.status()
                && let Ok(response_bytes) = response.bytes().await
            {
                if status.is_success() {
                    serde_json::from_slice::<IteratorResult>(&response_bytes).map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Deserialization error".to_string(),
                        )
                    })
                } else {
                    let response_string = str::from_utf8(&response_bytes).unwrap();
                    Err((status, response_string.to_string()))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }

    /// Query Index
    /// Query results from index with index_id
    /// The following parameters are supported:
    /// - Result type
    /// - Result sorting
    /// - Realtime search
    /// - Field filter
    /// - Fields to include in search results
    /// - Distance fields: derived fields from distance calculations
    /// - Highlights: keyword-in-context snippets and term highlighting
    /// - Query facets: which facets fields to calculate and return at query time
    /// - Facet filter: filter facets by field and value
    /// - Result sort: sort results by field and direction
    /// - Query type default: default query type, if not specified in query
    ///
    /// Arguments:
    /// * `base_url`: The base URL of the SeekStorm server.
    /// * `apikey_base64`: The Base64 encoded API key.
    /// * `index_id`: The ID of the index to query.
    /// * `request`: The search request object containing the query parameters.
    ///
    /// Returns:
    /// * `SearchResultObject`: The search results from the server.
    pub async fn query_index(
        &self,
        base_url: &str,
        apikey_base64: &str,
        index_id: u64,
        request: SearchRequestObject,
    ) -> Result<SearchResultObject, (StatusCode, String)> {
        let url = format!("{}/api/v1/index/{}/query", base_url, index_id);
        if let Ok(response) = self
            .client
            .post(&url)
            .header("apikey", apikey_base64)
            .json(&request)
            .send()
            .await
        {
            if let status = response.status()
                && let Ok(response_bytes) = response.bytes().await
            {
                if status.is_success() {
                    serde_json::from_slice::<SearchResultObject>(&response_bytes).map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Deserialization error".to_string(),
                        )
                    })
                } else {
                    let response_string = str::from_utf8(&response_bytes).unwrap();
                    Err((status, response_string.to_string()))
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
            }
        } else {
            Err((StatusCode::INTERNAL_SERVER_ERROR, String::new()))
        }
    }
}
