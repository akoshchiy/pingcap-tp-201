use crate::kvs::thread_pool::ThreadPool;
use crate::kvs::KvError;
use crate::kvs::Result;
use rayon::ThreadPoolBuilder;
use std::sync::Arc;

#[derive(Clone)]
pub struct RayonThreadPool {
    pool: Arc<rayon::ThreadPool>,
}

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        let pool = ThreadPoolBuilder::new()
            .num_threads(threads as usize)
            .build()
            .map_err(|err| KvError::PoolBuild {
                msg: err.to_string(),
            })?;
        Ok(RayonThreadPool {
            pool: Arc::new(pool),
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.pool.spawn(job);
    }
}
