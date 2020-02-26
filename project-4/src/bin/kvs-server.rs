#[macro_use]
extern crate clap;
#[macro_use(slog_o)]
extern crate slog;
#[macro_use]
extern crate slog_scope;
use kvs::{
    KvEngine, KvsServer, MyKvStore, RayonThreadPool, Result, SharedQueueThreadPool, SledKvs,
    ThreadPool,
};
use slog::Drain;
use std::env::current_dir;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::exit;
use std::{env, fs};
use structopt::StructOpt;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const DEFAULT_THREAD_POOL_SIZE: &str = "8";

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    enum Engine {
        kvs,
        sled
    }
}

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    enum Pool {
        shared,
        rayon,
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
    #[structopt(
        long,
        help = "Start with which thread pool.",
        value_name = "THREAD-POOL-NAME",
        raw(possible_values = "&Pool::variants()")
    )]
    thread_pool: Option<Pool>,
    #[structopt(
        long,
        help = "The thread pool size",
        value_name = "SIZE",
        raw(default_value = "DEFAULT_THREAD_POOL_SIZE")
    )]
    thread_pool_size: String,
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
    let logger = slog::Logger::root(drain, slog_o!());
    let _guard = slog_scope::set_global_logger(logger);

    let engine = opt.engine.unwrap_or(Engine::kvs);
    let thread_pool = opt.thread_pool.unwrap_or(Pool::shared);
    let thread_pool_size: u32 = opt.thread_pool_size.parse().unwrap();
    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", engine);
    info!("Listening on {}", opt.addr);

    let current_dir_path = current_dir()?;
    write_engine_meta(&current_dir_path, engine)?;

    match (engine, thread_pool) {
        (Engine::kvs, Pool::shared) => start_engine(
            KvsServer::new(
                MyKvStore::open(current_dir_path)?,
                SharedQueueThreadPool::new(thread_pool_size)?,
            ),
            opt.addr,
        )?,
        (Engine::kvs, Pool::rayon) => start_engine(
            KvsServer::new(
                MyKvStore::open(current_dir_path)?,
                RayonThreadPool::new(thread_pool_size)?,
            ),
            opt.addr,
        )?,
        (Engine::sled, Pool::shared) => start_engine(
            KvsServer::new(
                SledKvs::new(sled::open(current_dir_path)?),
                SharedQueueThreadPool::new(thread_pool_size)?,
            ),
            opt.addr,
        )?,
        (Engine::sled, Pool::rayon) => start_engine(
            KvsServer::new(
                SledKvs::new(sled::open(current_dir_path)?),
                RayonThreadPool::new(thread_pool_size)?,
            ),
            opt.addr,
        )?,
    };

    Ok(())
}

// Start engine with address.
fn start_engine<E: KvEngine, P: ThreadPool>(
    server: KvsServer<E, P>,
    addr: SocketAddr,
) -> Result<()> {
    server.start(addr)
}

// Write engine name to meta file.
fn write_engine_meta(current_dir_path: &PathBuf, engine_name: Engine) -> Result<()> {
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
