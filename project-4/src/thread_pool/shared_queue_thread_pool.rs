use crate::{Result, ThreadPool};

/// Shared queue thread pool.
pub struct SharedQueueThreadPool;

impl ThreadPool for SharedQueueThreadPool {
    fn new(_size: u32) -> Result<Self>
    where
        Self: Sized,
    {
        unimplemented!()
    }

    fn spawn<F>(&self, _job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        unimplemented!()
    }
}
