use crate::{Result, ThreadPool};

/// Rayon thread pool.
pub struct RayonThreadPool;

impl ThreadPool for RayonThreadPool {
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
