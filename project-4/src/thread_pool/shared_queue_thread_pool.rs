use crate::{ThreadPool,Result};

/// Shared queue thread pool.
pub struct SharedQueueThreadPool;

impl ThreadPool  for SharedQueueThreadPool{
    fn new(size: u32) -> Result<Self> where
        Self: Sized {
        unimplemented!()
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        unimplemented!()
    }
}