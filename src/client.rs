use std::collections::HashMap;
use std::net::{AddrParseError, SocketAddr};
use structopt::StructOpt;

mod peering;
use peering::client::Client;
use peering::grpc::*;

mod keys;
use keys::Key;

mod utils;
use utils::shorten;

fn parse_peer_flag(addr_s: &str) -> Result<SocketAddr, AddrParseError> {
    addr_s
        .parse::<SocketAddr>()
        .map_or_else(|_| format!("[::1]{:}", addr_s).parse(), |s| Ok(s))
}

#[derive(StructOpt, Debug)]
struct BaseCli {
    #[structopt(short = "p", long = "peer", parse(try_from_str=parse_peer_flag))]
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
        Cli::StoreValue(cfg) => match Client::store(&cfg.base.peer, cfg.value, None).await {
            Ok((key, addr)) => {
                println!("Stored in {}", addr);
                println!("under {}", key.as_hex_string());
            }
            Err(err) => {
                println!("{:?}", err);
            }
        },
        Cli::GetKey(cfg) => {
            let parsed_key = Key::try_from(cfg.key).expect("Failed to parse key :(");

            match Client::get(&cfg.base.peer, &parsed_key).await {
                Ok((value, addr)) => {
                    println!("Value {}", value);
                    println!("was in {}", addr);
                }
                Err(err) => {
                    println!("{:?}", err);
                }
            }
        }
        Cli::GetStatus(cfg) => match Client::get_status(&cfg.base.peer).await {
            Ok((primary_store, secondary_store, secondant_map, peers)) => {
                println!("State of {}:\n", cfg.base.peer);
                print!(" => {} Peers:", peers.len());

                for peer in peers {
                    print!("{}, ", peer.addr)
                }
                println!("\n");

                println!("Primary data:");

                for KeyValuePair { key, value } in primary_store.iter() {
                    println!("  - {}: {}", shorten(key), value);
                }

                println!("\nSecondary data:");

                for SecondaryStoreEntry { addr, entries } in secondary_store {
                    if entries.len() > 0 {
                        println!("  - {}: ", addr);
                        for KeyValuePair { key, value } in entries {
                            println!("    - {}: {}", shorten(&key), value);
                        }
                    }
                }

                println!("\nSecondants");
                let data_map: HashMap<_, _> = primary_store
                    .into_iter()
                    .map(|kv| {
                        let KeyValuePair { key, value } = kv;
                        (key, value)
                    })
                    .collect();

                for SecondantStoreEntry { key, addrs } in secondant_map {
                    println!(
                        "  - {} ({}) : {}",
                        shorten(&key),
                        data_map.get(&key).unwrap_or(&"ERROR".to_string()),
                        addrs.join(", ")
                    );
                }
            }
            Err(err) => {
                println!("{:?}", err);
            }
        },
        Cli::ShutDown(cfg) => {
            Client::shut_down(&cfg.base.peer)
                .await
                .expect("Shutdown request failed");
            println!("Client {} is shutting down", &cfg.base.peer);
        }
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv6Addr};
    #[test]
    fn parse_regular_addr() {
        assert_eq!(
            parse_peer_flag("[::1]:5000"),
            Ok(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0x1)),
                5000
            ))
        )
    }

    #[test]
    fn parse_just_the_port() {
        assert_eq!(
            parse_peer_flag(":1234"),
            Ok(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0x1)),
                1234
            ))
        )
    }
}
