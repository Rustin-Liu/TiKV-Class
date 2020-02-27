#[macro_use]
extern crate criterion;

use assert_cmd::prelude::*;
use criterion::Criterion;
use kvs::KvsClient;
use rand::prelude::*;
use std::net::SocketAddr;
use std::process::Command;
use std::str::FromStr;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const KVS: &str = "kvs";
const SHARED_POOL: &str = "shared";

fn shared_queue_kvs_write_bench(c: &mut Criterion) {
    let thread_nums = vec![2, 4, 6, 8];
    c.bench_function_over_inputs(
        "shared_queue_kvs",
        |b, &num| {
            let temp_dir = tempdir().unwrap();
            let mut server = Command::cargo_bin("kvs-server").unwrap();
            let mut child = server
                .args(&[
                    "--engine",
                    KVS,
                    "--addr",
                    DEFAULT_LISTENING_ADDRESS,
                    "--thread_pool",
                    SHARED_POOL,
                    "--thread_pool_size",
                    &num.to_string(),
                ])
                .current_dir(&temp_dir)
                .spawn()
                .unwrap();
            let (sender, receiver) = mpsc::sync_channel(0);
            let handle = thread::spawn(move || {
                let _ = receiver.recv(); // wait for main thread to finish
                child.kill().expect("server exited before killed");
            });
            thread::sleep(Duration::from_secs(1));
            b.iter(|| {
                let mut client =
                    KvsClient::init(SocketAddr::from_str(DEFAULT_LISTENING_ADDRESS).unwrap())
                        .unwrap();
                for i in 1..(1 << 10) {
                    client
                        .set(format!("key{}", i), "value".to_string())
                        .unwrap();
                }
            });
            sender.send(()).unwrap();
            handle.join().unwrap();
        },
        thread_nums,
    );
}

fn shared_queue_kvs_read_bench(c: &mut Criterion) {
    let thread_nums = vec![2, 4, 6, 8];
    c.bench_function_over_inputs(
        "shared_queue_kvs",
        |b, &num| {
            let temp_dir = tempdir().unwrap();
            let mut server = Command::cargo_bin("kvs-server").unwrap();
            let mut child = server
                .args(&[
                    "--engine",
                    KVS,
                    "--addr",
                    DEFAULT_LISTENING_ADDRESS,
                    "--thread_pool",
                    SHARED_POOL,
                    "--thread_pool_size",
                    &num.to_string(),
                ])
                .current_dir(&temp_dir)
                .spawn()
                .unwrap();
            let (sender, receiver) = mpsc::sync_channel(0);
            let handle = thread::spawn(move || {
                let _ = receiver.recv(); // wait for main thread to finish
                child.kill().expect("server exited before killed");
            });
            thread::sleep(Duration::from_secs(1));
            let address = SocketAddr::from_str(DEFAULT_LISTENING_ADDRESS).unwrap();
            let mut client = KvsClient::init(address).unwrap();
            for i in 1..(1 << 10) {
                client
                    .set(format!("key{}", i), "value".to_string())
                    .unwrap();
            }
            let mut rng = StdRng::seed_from_u64(64);
            b.iter(|| {
                let mut client = KvsClient::init(address).unwrap();
                client
                    .get(format!("key{}", rng.gen_range(0, 1 << 10)))
                    .unwrap();
            });
            sender.send(()).unwrap();
            handle.join().unwrap();
        },
        thread_nums,
    );
}

criterion_group!(
    benches,
    shared_queue_kvs_write_bench,
    shared_queue_kvs_read_bench
);
criterion_main!(benches);
