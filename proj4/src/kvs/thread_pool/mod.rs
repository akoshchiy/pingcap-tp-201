mod naive;
mod queue;
mod rayon;

use crate::kvs::err::Result;

pub use self::naive::NaiveThreadPool;
pub use self::queue::SharedQueueThreadPool;
pub use self::rayon::RayonThreadPool;

pub trait ThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}
