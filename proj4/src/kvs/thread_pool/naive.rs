use crate::kvs::thread_pool::ThreadPool;
use std::thread;

pub struct NaiveThreadPool {}

impl ThreadPool for NaiveThreadPool {
    fn new(threads: u32) -> crate::kvs::Result<Self>
    where
        Self: Sized,
    {
        Ok(NaiveThreadPool {})
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}
