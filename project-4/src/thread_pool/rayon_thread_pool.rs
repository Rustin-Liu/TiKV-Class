use rayon::ThreadPool as RThreadPool;

use crate::{Result, ThreadPool};

/// Rayon thread pool.
pub struct RayonThreadPool {
    thread_pool: RThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(_size: u32) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(RayonThreadPool {
            thread_pool: rayon::ThreadPoolBuilder::new().num_threads(8).build()?,
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.thread_pool.spawn(job)
    }
}
