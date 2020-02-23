use rayon::ThreadPool as RThreadPool;

use crate::{Result, ThreadPool};

/// Rayon thread pool.
pub struct RayonThreadPool {
    thread_pool: RThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(size: u32) -> Result<Self>
    where
        Self: Sized,
    {
        assert!(size > 0, "size must more than 0");
        Ok(RayonThreadPool {
            thread_pool: rayon::ThreadPoolBuilder::new()
                .num_threads(size as usize)
                .build()?,
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.thread_pool.spawn(job)
    }
}
