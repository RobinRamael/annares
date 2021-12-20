use itertools::*;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use structopt::StructOpt;

mod chord;
use chord::client::Client;
use chord::node::Locatable;

mod keys;
mod timed_lock;
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

    #[structopt(short, long)]
    long: bool,
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

    #[structopt(name = "overview")]
    Overview,
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
            let (successors, predecessor, data, fingers) = Client::get_status(&cfg.base.peer)
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
            print!("Sucessors: ");

            for addr in successors.iter() {
                print!("{} {}, ", addr, addr.key())
            }

            println!("");

            println!("Fingers:");

            for (idx, finger) in fingers.iter() {
                println!(
                    "  {}: {} {}",
                    idx,
                    finger.map(|p| p.to_string()).unwrap_or(" - ".to_string()),
                    finger
                        .map(|p| format!("({})", Key::from_addr(p)))
                        .unwrap_or("".to_string()),
                )
            }

            println!("");

            for (key, value) in data.into_iter().sorted_by_key(|(k, _)| k.clone()) {
                println!("{}: {}", key, value);
            }

            if cfg.long {
                println!("");

                let finger_keys: Vec<_> = fingers
                    .into_iter()
                    .map(|(_, f)| f)
                    .filter_map(|f| f)
                    .map(|f| (f.key(), f))
                    .collect();

                let finger_locations: Vec<Key> = (0..255)
                    .map(|idx| {
                        cfg.base
                            .peer
                            .key()
                            .cyclic_add(Key::two_to_the_power_of(idx))
                    })
                    .collect();

                for i in 0..=255 {
                    print!("{}: ", i);
                    if cfg.base.peer.key().arr[31] == i {
                        print!("HOME ");
                    }
                    for (j, loc) in finger_locations.iter().enumerate() {
                        if loc.arr[31] == i {
                            print!("{} ({}), ", j, loc);
                        }
                    }
                    for (key, addr) in finger_keys.iter() {
                        if key.arr[31] == i {
                            print!("{} ({}), ", addr, key)
                        }
                    }
                    println!("");
                }
            }
        }
        Cli::Overview => {
            let addrs: Vec<_> = readlines("./.currently_running")
                .iter()
                .map(|ln| {
                    utils::ipv6_loopback_socketaddr(ln.parse().expect("failed to parse address"))
                })
                .collect();

            let value_keys: Vec<Key> = readlines("./.added_keys")
                .into_iter()
                .map(|s| s.try_into().expect("failed to parse keys"))
                .collect();

            let addr_keys: Vec<_> = addrs.iter().map(|a| (a.key(), a)).collect();

            for i in 0..255 {
                print!("{}: ", i);
                for (key, addr) in addr_keys.iter() {
                    if key.arr[31] == i {
                        print!("{} ({}), ", addr, key)
                    }
                }
                for key in &value_keys {
                    if key.arr[31] == i {
                        print!("{} (key), ", key);
                    }
                }
                println!("");
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

fn readlines(path: &str) -> Vec<String> {
    let nodes_file = File::open(path).expect("no such file");
    let buf = BufReader::new(nodes_file);
    buf.lines()
        .map(|ln| ln.expect("Could not parse line"))
        .collect()
}
