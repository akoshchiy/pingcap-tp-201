use std::panic::catch_unwind;
use std::thread;
use std::thread::JoinHandle;
use crossbeam::channel::{bounded, Receiver, Sender, unbounded};
use slog::Logger;
use crate::kvs::Result;
use crate::kvs::thread_pool::ThreadPool;

const QUEUE_SIZE: usize = 10;

pub struct SharedQueueThreadPool {
    sender: Sender<QueueMessage>,
    join_handles: Vec<JoinHandle<()>>,
    thread_count: usize,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self>
        where Self: Sized {
        let (sender, receiver) = unbounded();

        let mut join_handles = Vec::with_capacity(threads as usize);

        for _ in 0..threads {
            let thread_receiver = receiver.clone();
            let join_handle = thread::spawn(move || {
                thread_loop(thread_receiver);
            });
            join_handles.push(join_handle);
        };

        Ok(SharedQueueThreadPool { thread_count: threads as usize, sender, join_handles })
    }

    fn spawn<F>(&self, job: F)
        where
            F: FnOnce() + Send + 'static,
    {
        self.sender.send(QueueMessage::Job(Box::new(job)));
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        for _ in 0..self.thread_count {
            self.sender.send(QueueMessage::Stop);
        }
        while let Some(cur_thread) = self.join_handles.pop() {
            cur_thread.join();
        }
    }
}

enum QueueMessage {
    Job(Box<dyn FnOnce() + Send + 'static>),
    Stop,
}

fn thread_loop(receiver: Receiver<QueueMessage>) {
    loop {
        let res = catch_unwind(|| {
            loop {
                let msg = receiver.recv().unwrap();
                match msg {
                    QueueMessage::Job(job) => {
                        job();
                        println!("job complete");
                    }
                    QueueMessage::Stop => {
                        return;
                    }
                };
            }
        });

        if res.is_ok() {
            return;
        }
    }
}