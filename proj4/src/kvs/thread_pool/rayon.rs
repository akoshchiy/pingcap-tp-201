use crate::kvs::thread_pool::ThreadPool;

pub struct RayonThreadPool {

}

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> crate::kvs::Result<Self> where Self: Sized {
        todo!()
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        todo!()
    }
}