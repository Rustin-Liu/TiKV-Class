#[macro_use]
extern crate criterion;

use criterion::{BatchSize, Criterion, ParameterizedBenchmark};
use kvs::{KvEngine, MyKvStore, SledKvs};
use rand::prelude::*;
use std::iter;
use tempfile::TempDir;

fn write_bench(c: &mut Criterion) {
    let bench = ParameterizedBenchmark::new(
        "kvs",
        |b, _| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    MyKvStore::open(temp_dir.path()).unwrap()
                },
                |mut my_kvs| {
                    for i in 1..100 {
                        my_kvs
                            .set(format!("key{}", i), "value".to_string())
                            .unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        },
        iter::once(()),
    )
    .with_function("sled", |b, _| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                SledKvs::new(sled::open(temp_dir).unwrap())
            },
            |mut sled_kvs| {
                for i in 1..100 {
                    sled_kvs
                        .set(format!("key{}", i), "value".to_string())
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    c.bench("write_bench", bench);
}

fn read_bench(c: &mut Criterion) {
    let bench = ParameterizedBenchmark::new(
        "kvs",
        |b, &i| {
            let temp_dir = TempDir::new().unwrap();
            let mut store = MyKvStore::open(temp_dir.path()).unwrap();
            for key_i in 1..i {
                store
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = StdRng::seed_from_u64(64);
            b.iter(|| {
                store
                    .get(format!("key{}", rng.gen_range(0, 1 << i)))
                    .unwrap();
            })
        },
        vec![1 << 10],
    )
    .with_function("sled", |b, &i| {
        let temp_dir = TempDir::new().unwrap();
        let mut sled_kvs = SledKvs::new(sled::open(temp_dir).unwrap());
        for key_i in 1..i {
            sled_kvs
                .set(format!("key{}", key_i), "value".to_string())
                .unwrap();
        }
        let mut rng = StdRng::seed_from_u64(64);
        b.iter(|| {
            sled_kvs.get(format!("key{}", rng.gen_range(0, i))).unwrap();
        })
    });
    c.bench("read_bench", bench);
}

criterion_group!(benches, write_bench, read_bench);
criterion_main!(benches);
