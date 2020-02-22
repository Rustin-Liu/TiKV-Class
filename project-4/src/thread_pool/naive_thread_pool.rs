use crate::ThreadPool;

use crate::Result;
use std::thread;

/// NativeThreadPool is a simple non-shared thread pool.
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_size: u32) -> Result<Self> {
        Ok(NaiveThreadPool)
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}
