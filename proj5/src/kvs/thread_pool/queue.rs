use crate::kvs::thread_pool::ThreadPool;
use crate::kvs::Result;
use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use slog::Logger;
use std::panic::catch_unwind;
use std::thread;
use std::thread::JoinHandle;

const QUEUE_SIZE: usize = 10;

#[derive(Clone)]
pub struct SharedQueueThreadPool {
    sender: Sender<QueueMessage>,
    thread_count: usize,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        let (sender, receiver) = unbounded();

        for _ in 0..threads {
            let thread_receiver = receiver.clone();
            thread::spawn(move || {
                thread_loop(thread_receiver);
            });
        }

        Ok(SharedQueueThreadPool {
            thread_count: threads as usize,
            sender,
        })
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
    }
}

enum QueueMessage {
    Job(Box<dyn FnOnce() + Send + 'static>),
    Stop,
}

fn thread_loop(receiver: Receiver<QueueMessage>) {
    loop {
        let res = catch_unwind(|| loop {
            let msg = receiver.recv().unwrap();
            match msg {
                QueueMessage::Job(job) => {
                    job();
                }
                QueueMessage::Stop => {
                    return;
                }
            };
        });

        if res.is_ok() {
            return;
        }
    }
}
