use std::{collections::HashMap, sync::Arc};

use base64::{engine::general_purpose, Engine as _};
use seekstorm::index::IndexArc;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::http_server::calculate_hash;

/// Quota per API key
#[derive(Default, Debug, Clone, Deserialize, Serialize, ToSchema)]
pub(crate) struct ApikeyQuotaObject {
    /// number of indices per API key
    pub indices_max: u64,
    /// combined index size per API key in MB
    pub indices_size_max: u64,
    /// combined number of documents in all indices per API key
    pub documents_max: u64,
    /// operations per month per API key: index/update/delete/query doc
    pub operations_max: u64,
    /// queries per sec per API key
    pub rate_limit: u64,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct ApikeyObject {
    /// API key id: self maintained, also used for index directory path
    pub id: u64,
    /// self maintained, pure informational
    pub apikey_hash: u128,
    /// Quota per API key
    pub quota: ApikeyQuotaObject,

    #[serde(skip)]
    pub index_list: HashMap<u64, IndexArc>,
}

pub(crate) async fn get_apikey_hash(
    api_key_base64: String,
    apikey_list: &Arc<tokio::sync::RwLock<HashMap<u128, ApikeyObject>>>,
) -> Option<u128> {
    match general_purpose::STANDARD.decode(api_key_base64) {
        Ok(apikey) => {
            let apikey_hash = calculate_hash(&apikey) as u128;
            let apikey_list_ref = apikey_list.read().await;

            if apikey_list_ref.contains_key(&apikey_hash) {
                Some(apikey_hash)
            } else {
                None
            }
        }
        Err(_e) => None,
    }
}
