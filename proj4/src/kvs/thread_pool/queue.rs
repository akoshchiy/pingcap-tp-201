use crate::kvs::thread_pool::ThreadPool;

pub struct SharedQueueThreadPool {
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> crate::kvs::Result<Self> where Self: Sized {
        todo!()
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        todo!()
    }
}