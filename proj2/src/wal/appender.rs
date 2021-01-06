use std::fs::File;

pub(crate) struct LogAppender {
    file: File,
}