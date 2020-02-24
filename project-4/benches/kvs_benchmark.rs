#[macro_use]
extern crate criterion;

use assert_cmd::prelude::*;
use criterion::{BatchSize, Criterion, ParameterizedBenchmark};
use kvs::{KvEngine, KvsClient, KvsServer, MyKvStore, SharedQueueThreadPool, ThreadPool};
use std::net::SocketAddr;
use std::process::Command;
use std::str::FromStr;
use std::sync::mpsc;
use std::time::Duration;
use std::{iter, thread};
use tempfile::{tempdir, TempDir};

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";

fn shared_queue_kvs_write_bench(c: &mut Criterion) {
    let thread_nums = vec![1];
    c.bench_function_over_inputs(
        "shared_queue_kvs",
        |b, &num| {
            thread::spawn(move || {
                let address = SocketAddr::from_str(DEFAULT_LISTENING_ADDRESS).unwrap();
                let temp_dir = tempdir().unwrap();
                let server = KvsServer::new(
                    MyKvStore::open(temp_dir.path()).unwrap(),
                    SharedQueueThreadPool::new(num).unwrap(),
                );
                server.start(address);
            });
            b.iter(|| {
                thread::sleep(Duration::from_secs(3));
                let mut client =
                    KvsClient::init(SocketAddr::from_str(DEFAULT_LISTENING_ADDRESS).unwrap())
                        .unwrap();
                for i in 1..10 {
                    client
                        .set(format!("key{}", i), "value".to_string())
                        .unwrap();
                }
            });
        },
        thread_nums,
    );
}

criterion_group!(benches, shared_queue_kvs_write_bench);
criterion_main!(benches);
