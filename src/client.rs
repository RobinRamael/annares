use itertools::*;
use std::net::SocketAddr;
use structopt::StructOpt;

mod chord;
use chord::client::Client;

mod keys;
use keys::Key;

mod utils;

#[derive(StructOpt, Debug)]
struct BaseCli {
    #[structopt(short = "p", long = "peer", parse(try_from_str=utils::parse_peer_flag))]
    peer: SocketAddr,
}

#[derive(StructOpt, Debug)]
struct StoreValueArgs {
    #[structopt(flatten)]
    base: BaseCli,

    value: String,
}

#[derive(StructOpt, Debug)]
struct GetKeyArgs {
    #[structopt(flatten)]
    base: BaseCli,

    key: String,
}

#[derive(StructOpt, Debug)]
struct GetStatusArgs {
    #[structopt(flatten)]
    base: BaseCli,
}

#[derive(StructOpt, Debug)]
struct ShutDownArgs {
    #[structopt(flatten)]
    base: BaseCli,
}

#[derive(StructOpt, Debug)]
#[structopt(name = "client")]
enum Cli {
    #[structopt(name = "store")]
    StoreValue(StoreValueArgs),

    #[structopt(name = "get")]
    GetKey(GetKeyArgs),

    #[structopt(name = "status")]
    GetStatus(GetStatusArgs),

    #[structopt(name = "shutdown")]
    ShutDown(ShutDownArgs),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match Cli::from_args() {
        Cli::StoreValue(cfg) => {
            let key = Key::hash(&cfg.value);
            let holding_node = Client::find_successor(&cfg.base.peer, key)
                .await
                .expect("Failed to contact first node");

            let returned_key = Client::store_value(&holding_node, cfg.value)
                .await
                .expect("Failed to store value");

            assert_eq!(key, returned_key);

            println!("Stored in {}", holding_node);
            println!("under {}", key.as_hex_string());
        }
        Cli::GetKey(cfg) => {
            let key = Key::try_from(cfg.key).expect("Failed to parse key :(");

            let holding_node = Client::find_successor(&cfg.base.peer, key)
                .await
                .expect("Failed to contact first node");

            let value = Client::get_key(&holding_node, key)
                .await
                .expect("Failed to get key");

            println!("Value {}", value);
            println!("was in {}", holding_node);
        }
        Cli::GetStatus(cfg) => {
            let (successor, predecessor, data) = Client::get_status(&cfg.base.peer)
                .await
                .expect("Failed to get status");

            println!(
                "Node: {} ({})",
                cfg.base.peer,
                Key::from_addr(cfg.base.peer)
            );
            println!(
                "predecessor: {} {}",
                predecessor
                    .map(|p| p.to_string())
                    .unwrap_or(" - ".to_string()),
                predecessor
                    .map(|p| format!("({})", Key::from_addr(p)))
                    .unwrap_or("".to_string()),
            );
            println!("Successor: {} ({})", successor, Key::from_addr(successor));

            println!("\ndata: ");

            for (key, value) in data.into_iter().sorted() {
                println!("{}: {}", key, value);
            }
        }
        Cli::ShutDown(cfg) => {
            Client::shut_down(&cfg.base.peer)
                .await
                .expect("Shutdown request failed");
            println!("Client {} is shutting down", &cfg.base.peer);
        }
    };
    Ok(())
}
