use crossbeam_channel::{bounded, select, Receiver};
use std::{collections::HashMap, fs, io, path::Path, sync::Arc};
use tokio::sync::RwLock;

use crate::{
    api_endpoints::open_all_apikeys, http_server::http_server, multi_tenancy::ApikeyObject,
};

pub(crate) const DEBUG: bool = false;

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
                println!("{} detected", input_string);
                if input_string == "quit" {
                    break;
                }
            }
            Err(error) => println!("error: {error}"),
        }
    }
}

pub(crate) async fn initialize(params: HashMap<String, String>) {
    let mut index_path = "/seekstorm_index".to_string();
    if params.contains_key("index_path") {
        index_path = params.get("index_path").unwrap().to_string();
    }
    let abs_path_buf = fs::canonicalize(&index_path);
    match abs_path_buf {
        Ok(v) => {
            let abs_path = v.as_path();
            if !Path::new(&abs_path).exists() {
                fs::create_dir_all(abs_path).unwrap();
                println!(
                    "index_path did not exists, new directory created: {}",
                    abs_path.to_string_lossy()
                );
                return;
            }
        }
        Err(_e) => {
            println!("index_path not found: {}", index_path);
            return;
        }
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

    loop {
        select! {

            recv(receiver_ctrl_c) -> _ => {

                println!("Server stopped by Ctrl-C");
                return;
            }

            recv(receiver_commandline) -> message => {

                let command=message.unwrap();

                match command.as_str()
                {

                    "quit" =>
                    {
                        println!("Committing all indices ...");
                        let mut apikey_list_mut=apikey_list_clone.write().await;
                        for apikey in apikey_list_mut.iter_mut()
                        {
                            for index in apikey.1.index_list.iter_mut()
                            {
                                let mut index_mut=index.1.write().await;
                                let indexed_doc_count=index_mut.indexed_doc_count;
                                index_mut.commit_level(indexed_doc_count);
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
