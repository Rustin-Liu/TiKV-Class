#[macro_use]
extern crate clap;

use kvs::{KvStore, KvsServer, Result};
use slog::*;
use std::env;
use std::net::SocketAddr;
use std::process::exit;
use structopt::StructOpt;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    enum Engine {
        kvs,
        sled
    }
}

#[derive(StructOpt, Debug)]
#[structopt(name = "kvs-server")]
struct Opt {
    #[structopt(
        long,
        help = "Start the server and begin listening for the server address",
        value_name = "IP:PORT",
        raw(default_value = "DEFAULT_LISTENING_ADDRESS"),
        parse(try_from_str)
    )]
    addr: SocketAddr,
    #[structopt(
        long,
        help = "Start with which store engine.",
        value_name = "ENGINE-NAME",
        raw(possible_values = "&Engine::variants()")
    )]
    engine: Option<Engine>,
}

fn main() {
    let mut opt = Opt::from_args();
    if opt.engine.is_none() {
        opt.engine = Some(Engine::kvs);
    }
    if let Err(e) = run(opt) {
        eprintln!("{}", e);
        exit(1);
    }
}

fn run(opt: Opt) -> Result<()> {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!());
    let server = match opt.engine.unwrap_or(Engine::kvs) {
        Engine::kvs => KvsServer::new(logger, KvStore::open(env::current_dir()?)?),
        _ => {
            error!(logger, "Wrong engine!");
            exit(1);
        }
    };
    server.init(opt.addr);
    Ok(())
}
