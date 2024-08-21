use base64::{engine::general_purpose, Engine};
use crossbeam_channel::{bounded, select, Receiver};
use seekstorm::index::{SimilarityType, TokenizerType};
use std::{collections::HashMap, env::current_exe, fs, io, path::Path, sync::Arc};
use tokio::sync::RwLock;

use crate::{
    api_endpoints::{create_apikey_api, create_index_api, delete_apikey_api, open_all_apikeys},
    http_server::{calculate_hash, http_server},
    ingest::ingest_ndjson,
    multi_tenancy::{ApikeyObject, ApikeyQuotaObject},
};

fn ctrl_channel() -> Result<Receiver<()>, ctrlc::Error> {
    let (sender, receiver) = bounded(20);
    ctrlc::set_handler(move || {
        let _ = sender.send(());
    })?;

    Ok(receiver)
}

async fn commandline(sender: crossbeam_channel::Sender<String>) {
    loop {
        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(_n) => {
                let input_string = input.trim().to_lowercase().to_owned();
                let _ = sender.send(input_string.to_string());
                if input_string == "quit" {
                    break;
                }
            }
            Err(error) => println!("error: {error}"),
        }
    }
}

pub(crate) async fn initialize(params: HashMap<String, String>) {
    let mut index_path_str = "seekstorm_index";
    if params.contains_key("index_path") {
        index_path_str = params.get("index_path").unwrap();
    }
    let mut index_path = Path::new(&index_path_str);
    let mut absolute_path = current_exe().unwrap();
    if !index_path.is_absolute() {
        absolute_path.pop();
        absolute_path.push(index_path_str);
        index_path = &absolute_path;
    }

    if !index_path.exists() {
        match fs::create_dir_all(index_path) {
            Ok(_v) => {}
            Err(_e) => {
                println!("index_path could not be created: {}", index_path.display());
            }
        }

        println!(
            "index_path did not exists, new directory created: {}",
            index_path.display()
        );
    }

    let apikey_list_map: HashMap<u128, ApikeyObject> = HashMap::new();
    let apikey_list = Arc::new(RwLock::new(apikey_list_map));
    let apikey_list_clone = apikey_list.clone();

    let index_path = Path::new(&index_path).to_path_buf();
    let mut apikey_list_mut = apikey_list.write().await;
    open_all_apikeys(&index_path, &mut apikey_list_mut).await;
    drop(apikey_list_mut);

    let (sender_commandline, receiver_commandline) = bounded(20);
    let receiver_ctrl_c = ctrl_channel().unwrap();
    tokio::spawn(async { commandline(sender_commandline).await });

    let mut local_ip = "0.0.0.0".to_string();
    let mut local_port = 80;
    if params.contains_key("local_ip") {
        local_ip = params.get("local_ip").unwrap().to_string();
    }
    if params.contains_key("local_port") {
        local_port = params.get("local_port").unwrap().parse::<u16>().unwrap();
    }

    let index_path_local = index_path.clone();

    tokio::spawn(async move {
        http_server(&index_path_local, apikey_list, &local_ip, &local_port).await
    });

    let demo_api_key = [0u8; 32];
    let demo_api_key_base64 = general_purpose::STANDARD.encode(demo_api_key);

    loop {
        select! {

            recv(receiver_ctrl_c) -> _ => {
                println!("Committing all indices ...");
                let mut apikey_list_mut=apikey_list_clone.write().await;
                for apikey in apikey_list_mut.iter_mut()
                {
                    for index in apikey.1.index_list.iter_mut()
                    {
                        let mut index_mut=index.1.write().await;
                        index_mut.close_index();
                    }
                }
                drop(apikey_list_mut);

                println!("Server stopped by Ctrl-C");
                return;
            }

            recv(receiver_commandline) -> message => {


                let m=message.unwrap();
                let parameter:Vec<&str>=m.split_whitespace().collect();
                let command=if parameter.is_empty() {""} else {parameter[0]};

                match command
                {
                    "ingest" =>
                    {

                        let apikey_quota_object=ApikeyQuotaObject {..Default::default()};

                        let mut apikey_list_mut = apikey_list_clone.write().await;
                        let apikey_object=create_apikey_api(
                            &index_path,
                            apikey_quota_object,
                            &demo_api_key,
                            &mut apikey_list_mut,
                        );

                        let wikipedia_schema_json = r#"
                        [{"field":"title","field_type":"Text","stored":true,"indexed":true,"boost":10.0},
                        {"field":"body","field_type":"Text","stored":true,"indexed":true},
                        {"field":"url","field_type":"Text","stored":true,"indexed":false}]"#;
                        let schema = serde_json::from_str(wikipedia_schema_json).unwrap();

                        let index_id = create_index_api(
                            &index_path,
                            "wikipedia".to_string(),
                            schema,
                            SimilarityType::Bm25fProximity,
                            TokenizerType::UnicodeAlphanumeric,
                            apikey_object,
                        );

                        let apikey_hash = calculate_hash(&demo_api_key) as u128;
                        let apikey_object=apikey_list_mut.get(&apikey_hash).unwrap();
                        let index_arc=apikey_object.index_list.get(&index_id).unwrap();
                        let index_arc_clone=index_arc.clone();

                        drop(apikey_list_mut);

                        let data_path=if parameter.len()>1 {parameter[1]} else {"wiki-articles.json"};
                        ingest_ndjson(index_arc_clone,data_path).await;

                        println!("Set the 'individual API key' in test_api.rest to '{}' when testing the REST API endpoints",demo_api_key_base64);
                    },

                    "delete" =>
                    {
                        println!("delete api_key");
                        let apikey_hash = calculate_hash(&demo_api_key) as u128;
                        let mut apikey_list_mut = apikey_list_clone.write().await;
                        let _ = delete_apikey_api(&index_path, &mut apikey_list_mut, apikey_hash);
                        drop(apikey_list_mut);
                    },

                    "list" =>
                    {
                        println!("delete indices");
                    },

                    "quit" =>
                    {
                        println!("Committing all indices ...");
                        let mut apikey_list_mut=apikey_list_clone.write().await;
                        for apikey in apikey_list_mut.iter_mut()
                        {
                            for index in apikey.1.index_list.iter_mut()
                            {
                                let mut index_mut=index.1.write().await;
                                index_mut.close_index();
                            }
                        }
                        drop(apikey_list_mut);

                        println!("Server stopped by quit");
                        return;
                    },

                    &_ => {

                    }
                }
            }
        }
    }
}
