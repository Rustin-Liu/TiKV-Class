use criterion::{criterion_group, criterion_main, BatchSize, Criterion, ParameterizedBenchmark};
use kvs::{KvEngine, MyKvStore, SledKvs};
use rand::prelude::*;
use std::env::current_dir;
use std::iter;

fn write_bench(c: &mut Criterion) {
    let bench = ParameterizedBenchmark::new(
        "kvs",
        |b, _| {
            b.iter_batched(
                || {
                    let current_dir_path = current_dir().unwrap();
                    MyKvStore::open(current_dir_path).unwrap()
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
                    let current_dir_path = current_dir().unwrap();
                    SledKvs::new(sled::open(current_dir_path).unwrap())
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
        |b, i| {
            let current_dir_path = current_dir().unwrap();
            let mut store = MyKvStore::open(current_dir_path).unwrap();
            for key_i in 1.. *i {
                store
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = StdRng::from_rng(thread_rng()).unwrap();
            b.iter(|| {
                store
                    .get(format!("key{}", rng.gen_range(1, 1 << *i)))
                    .unwrap();
            })
        },
        vec![1<<10],
    )
        .with_function("sled", |b, i| {
            let current_dir_path = current_dir().unwrap();
            let mut sled_kvs = SledKvs::new(sled::open(current_dir_path).unwrap());
            for key_i in 1..*i {
                sled_kvs
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = StdRng::from_rng(thread_rng()).unwrap();
            b.iter(|| {
                sled_kvs
                    .get(format!("key{}", rng.gen_range(1, *i)))
                    .unwrap();
            })
        });
    c.bench("read_bench", bench);
}

criterion_group!(benches, write_bench, read_bench);
criterion_main!(benches);
