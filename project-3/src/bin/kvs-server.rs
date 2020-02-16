use clap::AppSettings;
use kvs::Result;
use slog::*;
use std::net::SocketAddr;
use std::process::exit;
use structopt::StructOpt;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";

#[derive(StructOpt, Debug)]
#[structopt(name = "kvs-server")]
struct Opt {
    #[structopt(
        long,
        help = "Start the server and begin listening for the server address",
        value_name = "IP:PORT",
        default_value = "127.0.0.1:4000",
        parse(try_from_str)
    )]
    addr: SocketAddr,
    #[structopt(
        long,
        help = "Start with which store engine.",
        value_name = "ENGINE-NAME",
        default_value = "kvs"
    )]
    engine: String,
}

fn main() {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!());
    let opt = Opt::from_args();
    if let Err(e) = run(opt) {
        eprintln!("{}", e);
        exit(1);
    }
}

fn run(opt: Opt) -> Result<()> {
    unimplemented!()
}
