#[macro_use]
extern crate clap;

use kvs::{KvEngine, MyKvStore, KvsServer, Result, SledKvs};
use slog::*;
use std::env::current_dir;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::exit;
use std::{env, fs};
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
    let res = current_engine().and_then(move |engine| {
        if opt.engine.is_none() {
            opt.engine = engine;
        }
        if engine.is_some() && opt.engine != engine {
            eprintln!("Error: the wrong engine name");
            exit(1);
        }
        run(opt)
    });
    if let Err(e) = res {
        eprintln!("{}", e);
        exit(1);
    }
}

fn run(opt: Opt) -> Result<()> {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!());
    let engine = opt.engine.unwrap_or(Engine::kvs);

    info!(logger, "kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!(logger, "Storage engine: {}", engine);
    info!(logger, "Listening on {}", opt.addr);

    let current_dir_path = current_dir()?;

    write_engine_meta(current_dir_path.clone(), engine)?;

    match engine {
        Engine::kvs => start_engine(
            KvsServer::new(logger, MyKvStore::open(current_dir_path)?),
            opt.addr,
        ),
        Engine::sled => start_engine(
            KvsServer::new(logger, SledKvs::new(sled::open(current_dir_path)?)),
            opt.addr,
        ),
    }?;
    Ok(())
}

// Start engine with address.
fn start_engine<E: KvEngine>(server: KvsServer<E>, addr: SocketAddr) -> Result<()> {
    server.start(addr)
}

// Write engine name to meta file.
fn write_engine_meta(current_dir_path: PathBuf, engine_name: Engine)-> Result<()> {
    fs::write(current_dir_path.join("meta"), format!("{}", engine_name))?;
    Ok(())
}

// Get current engine name from meta file.
fn current_engine() -> Result<Option<Engine>> {
    let engine = current_dir()?.join("meta");
    if !engine.exists() {
        return Ok(None);
    }

    match fs::read_to_string(engine)?.parse() {
        Ok(engine) => Ok(Some(engine)),
        Err(_) => Ok(None),
    }
}
