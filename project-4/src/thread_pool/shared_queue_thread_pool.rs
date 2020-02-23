use crate::{Result, ThreadPool};
use crossbeam::channel::{self, Receiver, Sender};
use std::thread;

/// A shared queue thread pool.
///
/// If a spawn failed because panic, it will start a new thread.
pub struct SharedQueueThreadPool {
    sender: Sender<Box<dyn FnOnce() + Send + 'static>>,
}

impl ThreadPool for SharedQueueThreadPool {
    /// Create a thread pool.
    ///
    /// It use MPMC to add and execute the task.
    fn new(size: u32) -> Result<Self>
    where
        Self: Sized,
    {
        assert!(size > 0, "size must more than 0");
        let (sender, receiver) = channel::unbounded::<Box<dyn FnOnce() + Send + 'static>>();
        for _ in 0..size {
            let task_receiver = TaskReceiver(receiver.clone());
            thread::Builder::new().spawn(move || run_tasks(task_receiver))?;
        }
        Ok(SharedQueueThreadPool { sender })
    }

    /// Spawns a function into the thread pool.
    ///
    /// # Panics
    ///
    /// Panics if the thread pool has no thread.
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender
            .send(Box::new(job))
            .expect("The thread pool is full.");
    }
}

#[derive(Clone)]
struct TaskReceiver(Receiver<Box<dyn FnOnce() + Send + 'static>>);

impl Drop for TaskReceiver {
    fn drop(&mut self) {
        if thread::panicking() {
            let task_receiver = self.clone();
            if let Err(e) = thread::Builder::new().spawn(move || run_tasks(task_receiver)) {
                error!("Failed to spawn a new thread: {}", e);
            }
        }
    }
}

fn run_tasks(receiver: TaskReceiver) {
    loop {
        match receiver.0.recv() {
            Ok(task) => {
                task();
            }
            Err(_) => debug!("Thread exits because the thread pool is destroyed."),
        }
    }
}
