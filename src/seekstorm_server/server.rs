use base64::{Engine, engine::general_purpose};
use colored::Colorize;
use crossbeam_channel::{Receiver, bounded, select};
use seekstorm::{
    index::{FrequentwordType, NgramSet, SimilarityType, StemmerType, StopwordType, TokenizerType},
    ingest::{IngestCsv, IngestJson, IngestPdf},
};
use std::{
    collections::HashMap,
    env::current_exe,
    ffi::OsStr,
    fs::{self, metadata},
    path::Path,
    sync::Arc,
};
use tokio::sync::RwLock;

use crate::{
    api_endpoints::{
        create_apikey_api, create_index_api, delete_apikey_api, generate_openapi, open_all_apikeys,
    },
    http_server::{calculate_hash, http_server},
    multi_tenancy::{ApikeyObject, ApikeyQuotaObject, get_apikey_hash},
};

const WIKIPEDIA_FILENAME: &str = "wiki-articles.json";
const MSMARCO_FILENAME: &str = "fulldocs.tsv";

fn ctrl_channel() -> Result<Receiver<()>, ctrlc::Error> {
    let (sender, receiver) = bounded(20);
    ctrlc::set_handler(move || {
        let _ = sender.send(());
    })?;

    Ok(receiver)
}

async fn commandline(sender: crossbeam_channel::Sender<String>) {
    for line in std::io::stdin().lines() {
        if let Ok(line) = line {
            if sender.send(line.clone()).is_err() {
                return;
            }

            if line.to_lowercase() == "quit" {
                return;
            }
        } else {
            println!("stdin read error, try using -ti parameter in docker environment");
            break;
        }
    }
}

pub(crate) async fn initialize(params: HashMap<String, String>) {
    let mut ingest_path_str = "";
    if params.contains_key("ingest_path") {
        ingest_path_str = params.get("ingest_path").unwrap();
    }
    let mut ingest_path = Path::new(&ingest_path_str);
    let mut absolute_path = current_exe().unwrap();
    if !ingest_path.is_absolute() {
        absolute_path.pop();
        absolute_path.push(ingest_path_str);
        ingest_path = &absolute_path;
    }
    println!("Ingest path: {}", ingest_path.display());

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

                if let Ok(m) = message {
                let parameter:Vec<&str>=m.split_whitespace().collect();
                let command=if parameter.is_empty() {""} else {&parameter[0].to_lowercase()};

                let mut dash:HashMap<String,String>=HashMap::new();
                for (i,component) in parameter.iter().enumerate(){
                    if let Some(key_stripped) = component.strip_prefix("-") && i+1<parameter.len() {dash.insert(key_stripped.to_string(),parameter[i+1].to_string());}
                }

                match command
                {

                    "ingest" =>
                    {
                        if !parameter.is_empty() {

                            let data_path_str=if parameter.len()>1 {
                                parameter[1]
                            } else {
                                WIKIPEDIA_FILENAME
                            };
                            let mut data_path = Path::new(&data_path_str);
                            let mut absolute_path = ingest_path.to_path_buf();
                            if !data_path.is_absolute() {
                                absolute_path.push(data_path_str);
                                data_path = &absolute_path;
                            }

                            if data_path.exists() {

                                let apikey_list_clone2=apikey_list_clone.clone();
                                let apikey_option=if dash.contains_key("k") {
                                    get_apikey_hash(dash.get("k").unwrap_or(&"".to_owned()).to_string(), &apikey_list_clone2).await
                                } else {
                                    None
                                };

                                let mut apikey_list_mut = apikey_list_clone.write().await;
                                let apikey_object_option=if dash.contains_key("k") {

                                    if let Some(apikey_hash) = apikey_option
                                    {
                                        apikey_list_mut.get_mut(&apikey_hash)
                                    } else if dash.get("k").unwrap_or(&"".to_owned())==&demo_api_key_base64{

                                        let apikey_quota_object=ApikeyQuotaObject {..Default::default()};
                                        Some(create_apikey_api(
                                            &index_path,
                                            apikey_quota_object,
                                            &demo_api_key,
                                            &mut apikey_list_mut,
                                        ))
                                    } else{
                                        None
                                    }
                                } else {
                                    let apikey_quota_object=ApikeyQuotaObject {..Default::default()};
                                    Some(create_apikey_api(
                                        &index_path,
                                        apikey_quota_object,
                                        &demo_api_key,
                                        &mut apikey_list_mut,
                                    ))
                                };

                                if let Some(apikey_object) = apikey_object_option {

                                    let md = metadata(data_path).unwrap();


                                    if dash.contains_key("i") || !md.is_file() || data_path.display().to_string().to_lowercase().ends_with(".pdf")
                                    || data_path.display().to_string().to_lowercase().ends_with(WIKIPEDIA_FILENAME) || data_path.display().to_string().to_lowercase().ends_with(MSMARCO_FILENAME) ||
                                    (parameter.len()==1 && !apikey_object.index_list.is_empty() && apikey_object.index_list.contains_key(&0))  {
                                        let index_id=if dash.contains_key("i") {
                                            dash.get("i").and_then(|value| value.parse::<u64>().ok()).unwrap_or(0)
                                        } else if apikey_object.index_list.is_empty() || !apikey_object.index_list.contains_key(&0) {
                                                let indexname_schemajson = if md.is_file() && data_path.display().to_string().to_lowercase().ends_with(WIKIPEDIA_FILENAME)
                                                {("wikipedia_demo",r#"
                                                [{"field":"title","field_type":"Text","stored":true,"indexed":true,"boost":10.0},
                                                {"field":"body","field_type":"Text","stored":true,"indexed":true},
                                                {"field":"url","field_type":"Text","stored":true,"indexed":false}]"#,SimilarityType::Bm25fProximity,TokenizerType::UnicodeAlphanumeric  )} 
                                                else if md.is_file() && data_path.display().to_string().to_lowercase().ends_with(MSMARCO_FILENAME)
                                                {("msmarco_demo", r#"
                                                    [{"field":"url","field_type":"Text","stored":false,"indexed":false},
                                                    {"field":"title","field_type":"Text","stored":false,"indexed":false},
                                                    {"field":"body","field_type":"Text","stored":false,"indexed":true}]"# ,SimilarityType::Bm25f,TokenizerType::UnicodeAlphanumeric )
                                                }
                                                else {("pdf_demo", r#"
                                                [{"field":"title","field_type":"Text","stored":true,"indexed":true,"boost":10.0},
                                                {"field":"body","field_type":"Text","stored":true,"indexed":true},
                                                {"field":"url","field_type":"Text","stored":true,"indexed":false},
                                                {"field":"date","field_type":"Timestamp","stored":true,"indexed":false,"facet":true}]"#,SimilarityType::Bm25fProximity,TokenizerType::UnicodeAlphanumeric )};

                                                create_index_api(
                                                    &index_path,
                                                    indexname_schemajson.0.into(),
                                                    serde_json::from_str(indexname_schemajson.1).unwrap(),
                                                    indexname_schemajson.2,
                                                    indexname_schemajson.3,
                                                    StemmerType::None,
                                                    StopwordType::None,
                                                    FrequentwordType::English,
                                                    NgramSet::NgramFF as u8 | NgramSet::NgramFFF as u8,
                                                    Vec::new(),
                                                    apikey_object,
                                                )
                                            } else {
                                                0
                                            };

                                        if let Some(index_arc) = apikey_object.index_list.get_mut(&index_id) {


                                            if md.is_file() {
                                                let extension = dash
                                                .get("t")
                                                .map(|ext| ext.as_str())
                                                .or_else(|| Path::new(&data_path).extension().and_then(OsStr::to_str));
                                                if let Some(extension)=extension {
                                                    match extension.to_lowercase().as_str() {
                                                        "pdf" =>{
                                                            index_arc.ingest_pdf(data_path).await;
                                                        }
                                                        "json" =>{
                                                            index_arc.ingest_json(data_path).await;
                                                        }
                                                        "csv" =>{
                                                            index_arc.ingest_csv(
                                                                data_path,
                                                                dash.get("h").unwrap_or(&"false".to_owned()).to_lowercase()=="true",
                                                                dash.get("q").unwrap_or(&"true".to_owned()).to_lowercase()=="true",
                                                                dash.get("d").unwrap_or(&",".to_owned()).as_bytes()[0],
                                                                dash.get("s").and_then(|value| value.parse::<usize>().ok()),
                                                                dash.get("n").and_then(|value| value.parse::<usize>().ok())
                                                            ).await;
                                                        }
                                                        "ssv" =>{
                                                            index_arc.ingest_csv(
                                                                data_path,
                                                                dash.get("h").unwrap_or(&"false".to_owned()).to_lowercase()=="true",
                                                                dash.get("q").unwrap_or(&"true".to_owned()).to_lowercase()=="true",
                                                                dash.get("d").unwrap_or(&";".to_owned()).as_bytes()[0],
                                                                dash.get("s").and_then(|value| value.parse::<usize>().ok()),
                                                                dash.get("n").and_then(|value| value.parse::<usize>().ok())
                                                            ).await;
                                                        }
                                                        "tsv" =>{
                                                            index_arc.ingest_csv(
                                                                data_path,
                                                                dash.get("h").unwrap_or(&"false".to_owned()).to_lowercase()=="true",
                                                                dash.get("q").unwrap_or(&"true".to_owned()).to_lowercase()=="true",
                                                                dash.get("d").unwrap_or(&"\t".to_owned()).as_bytes()[0],
                                                                dash.get("s").and_then(|value| value.parse::<usize>().ok()),
                                                                dash.get("n").and_then(|value| value.parse::<usize>().ok())
                                                            ).await;
                                                        }
                                                        "psv" =>{
                                                            index_arc.ingest_csv(
                                                                data_path,
                                                                dash.get("h").unwrap_or(&"false".to_owned()).to_lowercase()=="true",
                                                                dash.get("q").unwrap_or(&"true".to_owned()).to_lowercase()=="true",
                                                                dash.get("d").unwrap_or(&"|".to_owned()).as_bytes()[0],
                                                                dash.get("s").and_then(|value| value.parse::<usize>().ok()),
                                                                dash.get("n").and_then(|value| value.parse::<usize>().ok())
                                                            ).await;
                                                        }
                                                        _ =>{
                                                            println!("{} {}","File extension not supported:".bright_red(),extension);
                                                        }
                                                    }
                                                } else {
                                                    println!("{} {}","File extension not found:".bright_red(),data_path.display());
                                                }
                                            } else {
                                                index_arc.ingest_pdf(data_path).await;
                                            }

                                            println!("{} to '{}' and 'index_id' to {} when using the REST API endpoints in test_api.rest/cURL/master.js","Set the 'individual api_key'".yellow(),dash.get("k").unwrap_or(&demo_api_key_base64),index_id);
                                        } else {
                                            println!("{} {}: Create schema/index first. Schema and index are automatically created only for {} and PDF files.","Index not found".bright_red(),index_id ,WIKIPEDIA_FILENAME);
                                            println!("For other JSON files, you need to create an index first via REST API (e.g. via CURL) and then use the console command: ingest [file_path] -k [api_key] -i [index_id]");
                                            println!("See details: https://github.com/SeekStorm/SeekStorm/blob/main/src/seekstorm_server/README.md#console-commands");
                                        }
                                    } else{
                                            println!("{}: Create schema/index first! Schema and index are automatically created only for {} and PDF files.","Index not specified or found".bright_red(),WIKIPEDIA_FILENAME);
                                            println!("For other JSON files, you need to create an index first via REST API (e.g. via CURL) and then use the console command: ingest [file_path] -k [api_key] -i [index_id]");
                                            println!("See details: https://github.com/SeekStorm/SeekStorm/blob/main/src/seekstorm_server/README.md#console-commands");
                                    }

                                } else {
                                    println!("{} {}. Create a valid API key first via REST API or use the demo API key {}","API key not found".bright_red(),dash.get("k").unwrap_or(&String::new()),demo_api_key_base64);
                                }
                            } else {
                                println!("{} {}","Path not found".bright_red(),data_path.display());
                            }

                        } else {
                            println!("{} ingest [data_path] [apikey] [index_id]","Missing parameters:".bright_red());
                        }
                    },



                    "create" =>
                    {
                        println!("create demo api_key");
                        let mut apikey_list_mut = apikey_list_clone.write().await;
                        let apikey_quota_object=ApikeyQuotaObject {..Default::default()};
                        create_apikey_api(
                            &index_path,
                            apikey_quota_object,
                            &demo_api_key,
                            &mut apikey_list_mut,
                        );
                    }

                    "delete" =>
                    {
                        println!("delete demo api_key");
                        let apikey_hash = calculate_hash(&demo_api_key) as u128;
                        let mut apikey_list_mut = apikey_list_clone.write().await;
                        let _ = delete_apikey_api(&index_path, &mut apikey_list_mut, apikey_hash);
                        drop(apikey_list_mut);
                    },

                    "list" =>
                    {
                        println!("delete indices");
                    },

                    "openapi" =>
                    {
                        generate_openapi();
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

                    "help" =>
                    {
                        println!("{}","Server console commands:".yellow());
                        println!();
                        println!("{:40} Index {} if present in the seekstorm_server.exe directory or the directory specified by the command line parameter `ingest_path`.","ingest".green(),WIKIPEDIA_FILENAME);
                        println!("{:40} Index a local file in PDF, CSV, JSON, Newline-delimited JSON, or Concatenated JSON format, from the seekstorm_server.exe directory or the directory specified by the command line parameter.","ingest [data_path]".green());
                        println!("{:40} Index a local file in PDF, CSV, JSON, Newline-delimited JSON, or Concatenated JSON format.","ingest [file_path] -t [type] -k [api_key] -i [index_id] -d [delimiter] -h [header] -q [quoting] -s [skip] -n [num]".green());
                        println!("{:40} Create the demo API key manually to allow a subsequent custom create index via REST API.","create".green());
                        println!("{:40} Delete the demo API key and all its indices.","delete".green());
                        println!("{:40} Create OpenAPI JSON file.","openapi".green());
                        println!("{:40} Stop the server.","quit".green());
                        println!("{:40} Show this help.","help".green());
                        println!();
                        println!("{} https://github.com/SeekStorm/SeekStorm/blob/main/src/seekstorm_server/README.md#console-commands","See details:".yellow());
                    },

                    &_ => {
                        println!("unknown command: {}",command);
                    }
                }

            }

            }
        }
    }
}
