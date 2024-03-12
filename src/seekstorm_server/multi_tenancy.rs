use std::{collections::HashMap, sync::Arc};

use base64::{engine::general_purpose, Engine as _};
use seekstorm::index::IndexArc;
use serde::{Deserialize, Serialize};

use crate::http_server::calculate_hash;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct ApikeyQuotaObject {
    pub indices_max: u64,
    pub indices_size_max: u64,
    pub documents_max: u64,
    pub operations_max: u64,
    pub rate_limit: u64,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct ApikeyObject {
    pub id: u64,
    pub apikey_hash: u128,
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
