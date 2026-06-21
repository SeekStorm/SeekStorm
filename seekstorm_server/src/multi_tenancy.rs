use std::{collections::HashMap, sync::Arc};

use base64::{Engine as _, engine::general_purpose};
use seekstorm::index::ApikeyObject;

use crate::http_server::calculate_hash;

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
