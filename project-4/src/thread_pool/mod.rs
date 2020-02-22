//! This module provides various thread pools. All thread pools should implement
//! the `ThreadPool` trait.

mod naive_thread_pool;
pub use naive_thread_pool::*;
mod shared_queue_thread_pool;
pub use shared_queue_thread_pool::*;
mod rayon_thread_pool;
pub use rayon_thread_pool::*;

use crate::Result;

/// The trait that all thread pools should implement.
pub trait ThreadPool{
    /// Create a thread pool.
    ///
    /// Return an error if any thread spawn failed.
    fn new(size: u32) -> Result<Self> where
        Self: Sized;

    /// Spawns a function into the thread pool.
    ///
    /// Spawning always succeeds, but if the function panics the thread pool
    /// continues to operate with the same number of threads — the thread
    /// count is not reduced nor is the thread pool destroyed, corrupted or
    /// invalidated.
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}