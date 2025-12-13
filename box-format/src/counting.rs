//! Counting writer wrapper for tracking bytes written.

use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;
use tokio::io::AsyncWrite;

pin_project! {
    /// A writer wrapper that counts bytes written through it.
    pub struct CountingWriter<W> {
        #[pin]
        inner: W,
        bytes_written: u64,
    }
}

impl<W> CountingWriter<W> {
    /// Create a new counting writer wrapping the given writer.
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            bytes_written: 0,
        }
    }

    /// Get the total number of bytes written through this writer.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Consume this wrapper and return the inner writer.
    #[allow(dead_code)]
    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: AsyncWrite> AsyncWrite for CountingWriter<W> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        let this = self.project();
        match this.inner.poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                *this.bytes_written += n as u64;
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().inner.poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn test_counting_writer() {
        let mut buf = Vec::new();
        let mut writer = CountingWriter::new(&mut buf);

        writer.write_all(b"hello").await.unwrap();
        assert_eq!(writer.bytes_written(), 5);

        writer.write_all(b" world").await.unwrap();
        assert_eq!(writer.bytes_written(), 11);

        writer.flush().await.unwrap();
        assert_eq!(writer.into_inner(), b"hello world");
    }
}
