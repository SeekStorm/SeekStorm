use base64::{Engine, engine::general_purpose};
use colored::Colorize;
use crossbeam_channel::{Receiver, bounded, select};

use indexmap::IndexMap;

use num_cpus::get_physical;
use num_format::{Locale, ToFormattedString};

use seekstorm::{
    index::{
        ApikeyObject, ApikeyQuotaObject, Close, Clustering, DocumentCompression, FrequentwordType,
        IS_AVX2, IS_NEON, Info, LexicalSimilarity, NgramSet, StemmerType, StopwordType,
        TokenizerType,
    },
    ingest::{
        IngestCsv, IngestJson, IngestPdf, display_index_info, ingest_sift, read_fvecs, read_ivecs,
    },
    search::{QueryRewriting, QueryType, ResultType, Search, SearchMode},
    utils::dir_size,
    vector::{Embedding, Inference, Model, Quantization},
    vector_similarity::{AnnMode, VectorSimilarity},
};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    env::current_exe,
    ffi::OsStr,
    fs::{self, metadata},
    io::{self, IsTerminal},
    path::Path,
    sync::Arc,
    thread::available_parallelism,
    time::Instant,
};
use tabled::{
    Table,
    settings::{
        Color, Modify, Remove, Style, Width,
        object::{Columns, Rows},
        style::{BorderColor, HorizontalLine},
    },
};
use tokio::sync::RwLock;

use crate::{
    MASTER_KEY_SECRET, VERSION,
    api_endpoints::{
        create_apikey_api, create_index_api, delete_apikey_api, generate_openapi, open_all_apikeys,
    },
    http_server::{calculate_hash, http_server},
    multi_tenancy::get_apikey_hash,
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
    #[allow(clippy::manual_flatten)]
    for line in io::stdin().lines() {
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

    let force_shard_number: Option<usize> = params
        .get("force_shard_number")
        .and_then(|cores| cores.parse::<usize>().ok());

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

    let mut local_ip = "0.0.0.0".to_string();
    let mut local_port = 80;
    if params.contains_key("local_ip") {
        local_ip = params.get("local_ip").unwrap().to_string();
    }
    if params.contains_key("local_port") {
        local_port = params.get("local_port").unwrap().parse::<u16>().unwrap();
    }

    let index_path = Path::new(&index_path).to_path_buf();

    let mut hasher = Sha256::new();
    hasher.update(MASTER_KEY_SECRET.to_string());
    let master_apikey = hasher.finalize();
    let master_apikey_base64 = general_purpose::STANDARD.encode(master_apikey);

    let info_entries = vec![
        Info {
            entry: "SeekStorm server",
            value: VERSION.to_string(),
        },
        Info {
            entry: "ingest path",
            value: ingest_path.display().to_string(),
        },
        Info {
            entry: "index path",
            value: index_path.display().to_string(),
        },
        Info {
            entry: "SIMD / processor",
            value: if *IS_AVX2 {
                "AVX2".to_string()
            } else if *IS_NEON {
                "NEON".to_string()
            } else {
                "disabled".to_string()
            } + " / "
                + num_cpus::get().to_string().as_str()
                + " logical, "
                + get_physical().to_string().as_str()
                + " physical cores, "
                + available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or_else(|_| num_cpus::get())
                    .to_string()
                    .as_str(),
        },
        Info {
            entry: "web server (UI, REST API) ",
            value: format!(
                "http://localhost:{}  ({}:{})",
                local_port, local_ip, local_port
            ),
        },
        Info {
            entry: "master API key ⚠️",
            value: master_apikey_base64.clone(),
        },
        Info {
            entry: "Help",
            value: "Enter 'help' for console commands".to_string(),
        },
        Info {
            entry: "Shutdown server",
            value: "Enter 'quit' or press CTRL-C".to_string(),
        },
    ];

    let mut table = Table::new(info_entries);

    table
        .with(
            Style::modern()
                .remove_horizontal()
                .horizontals([(1, HorizontalLine::inherit(Style::modern()))]),
        )
        .with(BorderColor::filled(Color::FG_BRIGHT_BLACK));
    table.modify(
        Columns::first(),
        BorderColor::filled(Color::FG_BRIGHT_BLACK),
    );
    table.modify(Columns::last(), BorderColor::filled(Color::FG_BRIGHT_BLACK));

    table.modify(Columns::first(), Width::increase(25));
    table.modify(Columns::last(), Width::truncate(49).suffix("..."));
    table.modify(Columns::last(), Width::increase(50));

    table.with(Remove::row(Rows::first()));
    table.with(Modify::new(Rows::one(0)).with(Color::FG_BRIGHT_GREEN));
    table.with(Modify::new(Rows::one(5)).with(Color::FG_BRIGHT_RED));
    table.with(Modify::new(Rows::one(6)).with(Color::FG_YELLOW));
    table.with(Modify::new(Rows::one(7)).with(Color::FG_YELLOW));

    println!("{}", table);

    let mut apikey_list_mut = apikey_list.write().await;

    let start_time = Instant::now();
    open_all_apikeys(&index_path, &mut apikey_list_mut).await;
    let elapsed_time = start_time.elapsed().as_nanos();

    let tenants_count = apikey_list_mut.len();
    let mut index_count = 0;
    for key in apikey_list_mut.iter() {
        index_count += key.1.index_list.len();
    }

    let index_size = dir_size(Path::new(&index_path)).unwrap_or(0);

    let info_entries = vec![
        Info {
            entry: "Multitenancy",
            value: "".to_string(),
        },
        Info {
            entry: "Number of tenants",
            value: tenants_count.to_string(),
        },
        Info {
            entry: "Number of indices",
            value: index_count.to_string(),
        },
        Info {
            entry: "Overall disk usage",
            value: index_size.to_formatted_string(&Locale::en) + " bytes",
        },
    ];

    let mut table = Table::new(info_entries);

    table
        .with(
            Style::modern()
                .remove_horizontal()
                .horizontals([(1, HorizontalLine::inherit(Style::modern()))]),
        )
        .with(BorderColor::filled(Color::FG_BRIGHT_BLACK));
    table.modify(
        Columns::first(),
        BorderColor::filled(Color::FG_BRIGHT_BLACK),
    );
    table.modify(Columns::last(), BorderColor::filled(Color::FG_BRIGHT_BLACK));

    table.modify(Columns::first(), Width::increase(26));
    table.modify(Columns::last(), Width::truncate(49).suffix("..."));
    table.modify(Columns::last(), Width::increase(50));

    table.with(Remove::row(Rows::first()));
    table.with(Modify::new(Rows::one(0)).with(Color::FG_CYAN));

    println!("{}", table);

    let time_label: &'static str = "load time";
    let time_value = (elapsed_time / 1_000_000_000).to_string() + " s";
    for apikey in apikey_list_mut.iter() {
        let index_size =
            dir_size(Path::new(&index_path).join(apikey.1.id.to_string())).unwrap_or(0) as usize;

        let mut indexed_doc_count_sum = 0;
        for index in apikey.1.index_list.iter() {
            indexed_doc_count_sum += index.1.read().await.indexed_doc_count().await;
        }

        let info_entries = vec![
            Info {
                entry: "Tenant",
                value: "".to_string(),
            },
            Info {
                entry: "tenant id",
                value: apikey.1.id.to_string(),
            },
            Info {
                entry: "indices quota",
                value: format!(
                    "{: >2}",
                    apikey.1.index_list.len() * 100 / apikey.1.quota.indices_max
                ) + "% | "
                    + &apikey.1.index_list.len().to_string()
                    + " of "
                    + &apikey.1.quota.indices_max.to_string(),
            },
            Info {
                entry: "documents quota",
                value: format!(
                    "{: >2}",
                    indexed_doc_count_sum * 100 / apikey.1.quota.documents_max
                ) + "% | "
                    + &indexed_doc_count_sum.to_formatted_string(&Locale::en)
                    + " of "
                    + &apikey
                        .1
                        .quota
                        .documents_max
                        .to_formatted_string(&Locale::en),
            },
            Info {
                entry: "operations quota",
                value: format!(
                    "{: >2}",
                    indexed_doc_count_sum * 100 / apikey.1.quota.operations_max
                ) + "% | "
                    + &indexed_doc_count_sum.to_formatted_string(&Locale::en)
                    + " of "
                    + &apikey
                        .1
                        .quota
                        .operations_max
                        .to_formatted_string(&Locale::en),
            },
            Info {
                entry: "disk usage quota",
                value: format!("{: >2}", index_size * 100 / apikey.1.quota.indices_size_max)
                    + "% | "
                    + &index_size.to_formatted_string(&Locale::en)
                    + " of "
                    + &apikey
                        .1
                        .quota
                        .indices_size_max
                        .to_formatted_string(&Locale::en)
                    + " bytes",
            },
            Info {
                entry: "rate limit",
                value: format!("{:?}", apikey.1.quota.rate_limit),
            },
        ];

        let mut table = Table::new(info_entries);

        table
            .with(
                Style::modern()
                    .remove_horizontal()
                    .horizontals([(1, HorizontalLine::inherit(Style::modern()))]),
            )
            .with(BorderColor::filled(Color::FG_BRIGHT_BLACK));
        table.modify(
            Columns::first(),
            BorderColor::filled(Color::FG_BRIGHT_BLACK),
        );
        table.modify(Columns::last(), BorderColor::filled(Color::FG_BRIGHT_BLACK));

        table.modify(Columns::first(), Width::increase(26));
        table.modify(Columns::last(), Width::truncate(49).suffix("..."));
        table.modify(Columns::last(), Width::increase(50));

        table.with(Remove::row(Rows::first()));
        table.with(Modify::new(Rows::one(0)).with(Color::FG_CYAN));

        println!("{}", table);

        for index in apikey.1.index_list.iter() {
            display_index_info(index.1, time_label, time_value.clone()).await;
        }
    }

    drop(apikey_list_mut);

    let stdin = io::stdin();
    let terminal = stdin.is_terminal();
    let receiver_ctrl_c = ctrl_channel().unwrap();
    let (sender_commandline, receiver_commandline) = bounded(20);
    if terminal {
        tokio::spawn(async { commandline(sender_commandline).await });
    } else {
        println!(
            "Standard input is not a terminal. Console commands are disabled. Use docker parameter '-ti terminal/tti' to enable."
        );
    }

    let index_path_local = index_path.clone();

    tokio::spawn(async move {
        http_server(
            &index_path_local,
            apikey_list,
            &local_ip,
            &local_port,
            &force_shard_number,
        )
        .await
    });

    let demo_api_key = [0u8; 32];
    let demo_api_key_base64 = general_purpose::STANDARD.encode(demo_api_key);

    loop {
        if terminal {
            select! {

                recv(receiver_ctrl_c) -> _ => {
                    println!("Committing all indices ...");
                    let mut apikey_list_mut=apikey_list_clone.write().await;
                    for apikey in apikey_list_mut.iter_mut()
                    {
                        for index in apikey.1.index_list.iter_mut()
                        {
                            index.1.close().await;
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
                        "searchsift" =>
                        {
                            println!("search sift start");
                            let mut apikey_list_mut = apikey_list_clone.write().await;
                            let apikey_hash = calculate_hash(&demo_api_key) as u128;
                            if let Some(apikey_object) = apikey_list_mut.get_mut(&apikey_hash) {
                                let index_id=0;
                                if let Some(index_arc) = apikey_object.index_list.get_mut(&index_id) {

                                    let query="";

                                    let topk=10;
                                    let similarity_threshold=None;
                                    let field_filter=Vec::new();
                                    let search_mode=SearchMode::Vector { similarity_threshold , ann_mode:AnnMode::Nprobe(15)};

                                    let mut search_time_sum=0;
                                    let mut results_sum=0;
                                    let mut result_count_total_sum=0;
                                    let mut observed_cluster_count_sum=0;
                                    let mut observed_vector_count_sum=0;
                                    let mut recall_count_sum=0;


                                    let data_path_str=if parameter.len()>1 {
                                        parameter[1]
                                    } else {
                                        r"C:\linux_remote\testset"
                                    };


                                    if let Ok(ground_truth) = read_ivecs(&Path::new(data_path_str).join("sift_groundtruth.ivecs").to_string_lossy()) {
                                    if let Ok(queries) = read_fvecs(&Path::new(data_path_str).join("sift_query.fvecs").to_string_lossy()) {




                                        if true {

                                        let queries_len=queries.len();

                                        for (query_idx, query_embedding) in queries.into_iter().enumerate().take(queries_len) {

                                            let ground_truth_for_query:IndexMap<usize, usize> = ground_truth[query_idx].iter().take(topk).enumerate().map(|(i, x)| (*x as usize, i)).collect();


                                            let query_embedding=Embedding::F32(query_embedding);

                                            let start_time = Instant::now();

                                            let result_object_vector = index_arc
                                            .search(
                                                query.to_string(),
                                                Some(query_embedding.clone()),
                                                QueryType::Intersection,
                                                search_mode.clone(),
                                                false,
                                                0,
                                                topk,
                                                ResultType::Topk,
                                                false,
                                                field_filter.clone(),
                                                Vec::new(),
                                                Vec::new(),
                                                Vec::new(),
                                                QueryRewriting::SearchOnly,
                                            )
                                            .await;

                                            let search_time = start_time.elapsed().as_nanos() as i64;
                                            search_time_sum+=search_time;
                                            results_sum+=result_object_vector.results.len();
                                            result_count_total_sum+=result_object_vector.result_count_total;
                                            observed_cluster_count_sum+=result_object_vector.observed_cluster_count;
                                            observed_vector_count_sum+=result_object_vector.observed_vector_count;


                                            let mut recall_count=0;
                                            for result in result_object_vector.results.iter() {


                                                if ground_truth_for_query.contains_key(&result.doc_id) {
                                                    recall_count+=1;
                                                }
                                            }

                                            recall_count_sum+=recall_count;
                                        }

                                        let indexed_vector_count=index_arc.read().await.indexed_vector_count().await;
                                        let indexed_cluster_count=index_arc.read().await.indexed_cluster_count().await;

                                        println!("Inference {:?} Similarity {:?} QueryMode: {:?} Search time: {} µs  result count {} result count total: {} clusters observed: {:.2}% ({} of {}) vectors observed: {:.2}% ({} of {}) recall: {:.2}%",
                                        index_arc.read().await.meta.inference,
                                        index_arc.read().await.vector_similarity,
                                        search_mode,
                                         (search_time_sum as usize/1000/queries_len).to_formatted_string(&Locale::en), results_sum.to_formatted_string(&Locale::en), result_count_total_sum.to_formatted_string(&Locale::en),
                                        (observed_cluster_count_sum as f64) / queries_len as f64 / (indexed_cluster_count as f64) * 100.0,(observed_cluster_count_sum/queries_len).to_formatted_string(&Locale::en) , indexed_cluster_count.to_formatted_string(&Locale::en),
                                        (observed_vector_count_sum as f64) / queries_len as f64 / (indexed_vector_count as f64) * 100.0,(observed_vector_count_sum/queries_len).to_formatted_string(&Locale::en) , indexed_vector_count.to_formatted_string(&Locale::en),
                                        (recall_count_sum as f64) / queries_len as f64 / (topk as f64) * 100.0);
                                        println!();

                                        }

                                    } else {
                                        println!("Failed to read query query file");
                                    }

                                    } else {
                                        println!("Failed to read query ground_truth file");
                                    }
                                }
                            }
                        }

                        "search" =>
                        {
                            println!("search start");
                            let mut apikey_list_mut = apikey_list_clone.write().await;
                            let apikey_hash = calculate_hash(&demo_api_key) as u128;
                            if let Some(apikey_object) = apikey_list_mut.get_mut(&apikey_hash) {
                                let index_id=0;
                                if let Some(index_arc) = apikey_object.index_list.get_mut(&index_id) {


                                    let query="rosy panther";
                                    let len=10;
                                    let similarity_threshold=Some(0.7);
                                    let field_filter=Vec::new();
                                    let fields_hashset=HashSet::new();
                                    let mut recall_set=HashSet::new();


                                    let start_time = Instant::now();

                                    let result_object_vector = index_arc
                                    .search(
                                        query.to_string(),
                                        None,
                                        QueryType::Intersection,
                                        SearchMode::Vector { similarity_threshold , ann_mode: AnnMode::Similaritythreshold(0.0) },
                                        false,
                                        0,
                                        len,
                                        ResultType::Topk,
                                        false,
                                        field_filter.clone(),
                                        Vec::new(),
                                        Vec::new(),
                                        Vec::new(),
                                        QueryRewriting::SearchOnly,
                                    )
                                    .await;

                                    let search_time = start_time.elapsed().as_nanos() as i64;

                                    let mut min_cluster_score = f32::MAX;
                                    for (i, result) in result_object_vector.results.iter().enumerate() {
                                        let doc = index_arc.read().await.get_document(result.doc_id, false,&None, &fields_hashset, &Vec::new()).await.ok();
                                        let title= if let Some(doc) = &doc { if let Some(title) = doc.get("title") { title.to_string() } else { "".to_string() } } else {"".to_string()};
                                        #[cfg(feature = "vb")]
                                        if result.cluster_score < min_cluster_score {
                                            min_cluster_score = result.cluster_score;
                                        }
                                        #[cfg(not(feature = "vb"))]
                                        {
                                        min_cluster_score=0.0;
                                        }

                                        recall_set.insert(result.doc_id);

                                        #[cfg(feature = "vb")]
                                        println!("Top {}: doc_id: {}, similarity: {}, lexical_score: {}, vector_score: {},  cluster_score: {}, shard_id: {}, level_id: {}, cluster_id: {}, field_id: {}, chunk_id: {}, source: {:?}, document: {} ", i+1, result.doc_id, result.score, result.lexical_score, result.vector_score, result.cluster_score, result.shard_id, result.level_id, result.cluster_id, result.field_id,result.chunk_id, result.source, title);
                                        #[cfg(not(feature = "vb"))]
                                        println!("Top {}: doc_id: {}, similarity: {},  document: {} ", i+1, result.doc_id, result.score, title);
                                    }

                                    let indexed_vector_count=index_arc.read().await.indexed_vector_count().await;
                                    let indexed_cluster_count=index_arc.read().await.indexed_cluster_count().await;

                                    println!("Search time: {} µs  result count {} result count total: {} clusters observed: {:.2}% ({} of {}) vectors observed: {:.2}% ({} of {})", (search_time/1000).to_formatted_string(&Locale::en), result_object_vector.results.len(), result_object_vector.result_count_total,
                                    (result_object_vector.observed_cluster_count as f64) / (indexed_cluster_count as f64) * 100.0,result_object_vector.observed_cluster_count.to_formatted_string(&Locale::en) , indexed_cluster_count.to_formatted_string(&Locale::en),
                                    (result_object_vector.observed_vector_count as f64) / (indexed_vector_count as f64) * 100.0,result_object_vector.observed_vector_count.to_formatted_string(&Locale::en) , indexed_vector_count.to_formatted_string(&Locale::en));
                                    println!("Minimum cluster score in results: {}", min_cluster_score);
                                    println!();


                                    let start_time = Instant::now();

                                    let result_object_vector = index_arc
                                    .search(
                                        query.to_string(),
                                        None,
                                        QueryType::Intersection,
                                        SearchMode::Vector { similarity_threshold , ann_mode: AnnMode::Similaritythreshold(min_cluster_score)},
                                        false,
                                        0,
                                        len,
                                        ResultType::Topk,
                                        false,
                                        field_filter.clone(),
                                        Vec::new(),
                                        Vec::new(),
                                        Vec::new(),
                                        QueryRewriting::SearchOnly,
                                    )
                                    .await;

                                    let mut recall_count=0;
                                    let search_time = start_time.elapsed().as_nanos() as i64;
                                    for (i, result) in result_object_vector.results.iter().enumerate() {
                                        let doc = index_arc.read().await.get_document(result.doc_id, false,&None, &fields_hashset, &Vec::new()).await.ok();
                                        let title= if let Some(doc) = &doc { if let Some(title) = doc.get("title") { title.to_string() } else { "".to_string() } } else {"".to_string()};
                                        #[cfg(feature = "vb")]
                                        println!("Top {}: doc_id: {}, similarity: {}, lexical_score: {}, vector_score: {},  cluster_score: {}, shard_id: {}, level_id: {}, cluster_id: {}, field_id: {}, chunk_id: {}, source: {:?}, document: {} ", i+1, result.doc_id, result.score, result.lexical_score, result.vector_score, result.cluster_score, result.shard_id, result.level_id, result.cluster_id, result.field_id,result.chunk_id, result.source, title);
                                        #[cfg(not(feature = "vb"))]
                                        println!("Top {}: doc_id: {}, similarity: {},  document: {} ", i+1, result.doc_id, result.score, title);
                                        if recall_set.contains(&result.doc_id) { recall_count+=1; }
                                    }
                                    println!("Search time: {} µs  result count {} result count total: {} clusters observed: {:.2}% ({} of {}) vectors observed: {:.2}% ({} of {}) recall: {}%", (search_time/1000).to_formatted_string(&Locale::en), result_object_vector.results.len(), result_object_vector.result_count_total,
                                    (result_object_vector.observed_cluster_count as f64) / (index_arc.read().await.indexed_cluster_count as f64) * 100.0,result_object_vector.observed_cluster_count.to_formatted_string(&Locale::en) , index_arc.read().await.indexed_cluster_count.to_formatted_string(&Locale::en),
                                    (result_object_vector.observed_vector_count as f64) / (index_arc.read().await.indexed_vector_count as f64) * 100.0,result_object_vector.observed_vector_count.to_formatted_string(&Locale::en) , index_arc.read().await.indexed_vector_count.to_formatted_string(&Locale::en),
                                    (recall_count as f64) / (recall_set.len() as f64) * 100.0);
                                    println!();



                                    let start_time = Instant::now();
                                    let result_object_vector = index_arc
                                    .search(
                                        query.to_string(),
                                        None,
                                        QueryType::Intersection,
                                        SearchMode::Vector { similarity_threshold  , ann_mode: AnnMode::Nprobe(55) },
                                        false,
                                        0,
                                        len,
                                        ResultType::Topk,
                                        false,
                                        field_filter.clone(),
                                        Vec::new(),
                                        Vec::new(),
                                        Vec::new(),
                                        QueryRewriting::SearchOnly,
                                    )
                                    .await;

                                    let mut recall_count=0;
                                    let search_time = start_time.elapsed().as_nanos() as i64;
                                    for (i, result) in result_object_vector.results.iter().enumerate() {
                                        let doc = index_arc.read().await.get_document(result.doc_id, false,&None, &fields_hashset, &Vec::new()).await.ok();
                                        let title= if let Some(doc) = &doc { if let Some(title) = doc.get("title") { title.to_string() } else { "".to_string() } } else {"".to_string()};

                                        #[cfg(feature = "vb")]
                                        println!("Top {}: doc_id: {}, similarity: {}, lexical_score: {}, vector_score: {},  cluster_score: {}, shard_id: {}, level_id: {}, cluster_id: {}, field_id: {}, chunk_id: {}, source: {:?}, document: {} ", i+1, result.doc_id, result.score, result.lexical_score, result.vector_score, result.cluster_score, result.shard_id, result.level_id, result.cluster_id, result.field_id,result.chunk_id, result.source, title);
                                        #[cfg(not(feature = "vb"))]
                                        println!("Top {}: doc_id: {}, similarity: {},  document: {} ", i+1, result.doc_id, result.score, title);

                                        if recall_set.contains(&result.doc_id) { recall_count+=1; }
                                    }
                                    println!("Search time: {} µs  result count {} result count total: {} clusters observed: {:.2}% ({} of {}) vectors observed: {:.2}% ({} of {}) recall: {}%", (search_time/1000).to_formatted_string(&Locale::en), result_object_vector.results.len(), result_object_vector.result_count_total,
                                    (result_object_vector.observed_cluster_count as f64) / (index_arc.read().await.indexed_cluster_count as f64) * 100.0,result_object_vector.observed_cluster_count.to_formatted_string(&Locale::en) , index_arc.read().await.indexed_cluster_count.to_formatted_string(&Locale::en),
                                    (result_object_vector.observed_vector_count as f64) / (index_arc.read().await.indexed_vector_count as f64) * 100.0,result_object_vector.observed_vector_count.to_formatted_string(&Locale::en) , index_arc.read().await.indexed_vector_count.to_formatted_string(&Locale::en),
                                    (recall_count as f64) / (recall_set.len() as f64) * 100.0);


                                }
                            }
                        }

                        "ingestcsv" =>
                        {
                            println!("ingest csv start");
                            let mut apikey_list_mut = apikey_list_clone.write().await;
                            let apikey_hash = calculate_hash(&demo_api_key) as u128;
                            if let Some(apikey_object) = apikey_list_mut.get_mut(&apikey_hash) {
                                let index_id=0;
                                if let Some(index_arc) = apikey_object.index_list.get_mut(&index_id) {
                                    let mut index_arc_clone=index_arc.clone();
                                    index_arc_clone.ingest_csv(Path::new("C:/data/wikipedia/amzscout.csv"), true, true,true, b',', None, None).await;
                                }
                            }
                        }

                        "ingestsift" =>
                        {

                            let data_path_str=if parameter.len()>1 {
                                parameter[1]
                            } else {
                                r"C:\linux_remote\testset"
                            };

                            use seekstorm::vector::Precision;
                            println!("ingest sift start");
                            let mut apikey_list_mut = apikey_list_clone.write().await;

                            let apikey_quota_object=ApikeyQuotaObject {
                                indices_max: 10,
                                indices_size_max: 100_000_000_000,
                                documents_max: 100_000_000,
                                operations_max: 1_000_000_000,
                                rate_limit:None,
                                 ..Default::default()
                            };
                            create_apikey_api(
                                &index_path,
                                apikey_quota_object,
                                &demo_api_key,
                                &mut apikey_list_mut,
                            );

                            let apikey_hash = calculate_hash(&demo_api_key) as u128;
                            if let Some(apikey_object) = apikey_list_mut.get_mut(&apikey_hash) {
                                let indexname_schemajson =


                                    {("sift1m",r#"
                                    [{"field":"vector","field_type":"Binary","store":false,"index_lexical":false,"index_vector":true}]"#,

                                    LexicalSimilarity::Bm25f,TokenizerType::UnicodeAlphanumeric)};

                                let _ =create_index_api(
                                    &index_path,
                                    indexname_schemajson.0.into(),
                                    serde_json::from_str(indexname_schemajson.1).unwrap(),
                                    indexname_schemajson.2,
                                    indexname_schemajson.3,
                                    StemmerType::None,
                                    StopwordType::None,
                                    FrequentwordType::English,
                                    NgramSet::SingleTerm as u8 ,
                                    DocumentCompression::Snappy,
                                    Vec::new(),
                                    force_shard_number,
                                    apikey_object,

                                    None,
                                    None,
                                    false,
                                    Clustering::Auto,
                                    Inference::External { dimensions: 128 , precision: Precision::F32, quantization: Quantization::ScalarQuantizationI8,similarity:VectorSimilarity::Euclidean } ,
                                ).await;

                                let index_id=0;
                                if let Some(index_arc) = apikey_object.index_list.get_mut(&index_id) {

                                    ingest_sift(index_arc, Path::new(&Path::new(data_path_str).join("sift_base.fvecs")), None).await;
                                }
                            }
                        }

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

                                            let apikey_quota_object=ApikeyQuotaObject {
                                                indices_max: 10,
                                                indices_size_max: 100_000_000_000,
                                                documents_max: 100_000_000,
                                                operations_max: 1_000_000_000,
                                                rate_limit:None,
                                                ..Default::default()
                                            };
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
                                        let apikey_quota_object=ApikeyQuotaObject {
                                            indices_max: 10,
                                            indices_size_max: 100_000_000_000,
                                            documents_max: 100_000_000,
                                            operations_max: 1_000_000_000,
                                            rate_limit:None,
                                            ..Default::default()
                                        };
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
                                                [{"field":"title","field_type":"Text","store":true,"index_lexical":true,"index_vector":true,"dictionary_source": false, "completion_source": false,"boost":10.0},
                                                {"field":"body","field_type":"Text","store":true,"index_lexical":true,"index_vector":true,"longest":true,"dictionary_source": false},
                                                {"field":"url","field_type":"Text","store":true,"index_lexical":false}]"#,LexicalSimilarity::Bm25fProximity,TokenizerType::UnicodeAlphanumericFolded  )}
                                                    else if md.is_file() && data_path.display().to_string().to_lowercase().ends_with(MSMARCO_FILENAME)
                                                    {("msmarco_demo", r#"
                                                    [{"field":"url","field_type":"Text","store":false,"index_lexical":false},
                                                    {"field":"title","field_type":"Text","store":false,"index_lexical":false},
                                                    {"field":"body","field_type":"Text","store":false,"index_lexical":true,"longest":true}]"# ,LexicalSimilarity::Bm25f,TokenizerType::UnicodeAlphanumeric )
                                                    }
                                                    else {("pdf_demo", r#"
                                                [{"field":"title","field_type":"Text","store":true,"index_lexical":true,"boost":10.0},
                                                {"field":"body","field_type":"Text","store":true,"index_lexical":true,"longest":true},
                                                {"field":"url","field_type":"Text","store":true,"index_lexical":false},
                                                {"field":"date","field_type":"Timestamp","store":true,"index_lexical":false,"facet":true}]"#,LexicalSimilarity::Bm25fProximity,TokenizerType::UnicodeAlphanumeric )};

                                                    create_index_api(
                                                        &index_path,
                                                        indexname_schemajson.0.into(),
                                                        serde_json::from_str(indexname_schemajson.1).unwrap(),
                                                        indexname_schemajson.2,
                                                        indexname_schemajson.3,
                                                        StemmerType::None,
                                                        StopwordType::None,
                                                        FrequentwordType::English,
                                                        NgramSet::NgramFF as u8 ,
                                                        DocumentCompression::Snappy,
                                                        Vec::new(),
                                                        force_shard_number,
                                                        apikey_object,
                                                        None,
                                                        None,
                                                        false,
                                                        Clustering::Auto,
                                                        Inference::Model2Vec { model: Model::PotionBase2M, chunk_size: 1000, quantization: Quantization::TurboQuantI8 },
                                                    ).await
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
                                                                    dash.get("f").unwrap_or(&"true".to_owned()).to_lowercase()=="true",
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
                                                                    dash.get("f").unwrap_or(&"true".to_owned()).to_lowercase()=="true",
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
                                                                    dash.get("f").unwrap_or(&"true".to_owned()).to_lowercase()=="true",
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
                                                                    dash.get("f").unwrap_or(&"true".to_owned()).to_lowercase()=="true",
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
                            let apikey_quota_object=ApikeyQuotaObject {
                                indices_max: 10,
                                indices_size_max: 100_000_000_000,
                                documents_max: 100_000_000,
                                operations_max: 1_000_000_000,
                                rate_limit:None,
                                ..Default::default()
                            };
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
                                    index.1.close().await;
                                }
                            }
                            drop(apikey_list_mut);

                            println!("Server stopped by quit");
                            return;
                        },

                        "info" =>
                        {
                            let apikey_list_ref=apikey_list_clone.read().await;
                            for apikey in apikey_list_ref.iter() {
                                for index in apikey.1.index_list.iter() {
                                    display_index_info(index.1, "info", "0 s".to_string()).await;
                                }
                            }
                        }

                        "help" =>
                        {
                            println!("{}","Server console commands:".yellow());
                            println!();
                            println!("{:40} Index {} if present in the seekstorm_server.exe directory or the directory specified by the command line parameter `ingest_path`.","ingest".green(),WIKIPEDIA_FILENAME);
                            println!("{:40} Index a local file in PDF, CSV, JSON, Newline-delimited JSON, or Concatenated JSON format, from the seekstorm_server.exe directory or the directory specified by the command line parameter.","ingest [data_path]".green());
                            println!("{:40} Index a local file in PDF, CSV, JSON, Newline-delimited JSON, or Concatenated JSON format.","ingest [file_path] -t [type] -k [api_key] -i [index_id] -d [delimiter] -h [header] -q [quoting] -s [skip] -n [num]".green());
                            println!("{:40} Create the demo API key manually to allow a subsequent custom create index via REST API.","create".green());
                            println!("{:40} Delete the demo API key and all its indices.","delete".green());
                            println!("{:40} Display current index information.","info".green());
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
        } else {
            select! {
                recv(receiver_ctrl_c) -> _ => {
                    println!("Committing all indices ...");
                    let mut apikey_list_mut=apikey_list_clone.write().await;
                    for apikey in apikey_list_mut.iter_mut()
                    {
                        for index in apikey.1.index_list.iter_mut()
                        {
                            index.1.close().await;
                        }
                    }
                    drop(apikey_list_mut);

                    println!("Server stopped by Ctrl-C");
                    return;
                }
            }
        }
    }
}
