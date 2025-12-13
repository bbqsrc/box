//! Hashing wrappers for computing digests while reading or writing.

use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use digest::Digest;
use pin_project_lite::pin_project;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite, ReadBuf};

pin_project! {
    /// A reader wrapper that computes a hash digest while data is read through it.
    ///
    /// The hash is updated as data passes through `poll_read`.
    pub struct HashingReader<R, D> {
        #[pin]
        inner: R,
        hasher: D,
        bytes_read: u64,
    }
}

impl<R, D: Digest + Default> HashingReader<R, D> {
    /// Create a new hashing reader wrapping the given reader.
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: D::default(),
            bytes_read: 0,
        }
    }

    /// Get the total number of bytes read through this reader.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Get the hash as bytes (finalizes the hasher).
    pub fn finalize_bytes(self) -> Vec<u8> {
        self.hasher.finalize().to_vec()
    }
}

impl<R: AsyncRead, D: Digest> AsyncRead for HashingReader<R, D> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        let this = self.project();
        let before = buf.filled().len();

        match this.inner.poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let after = buf.filled().len();
                let new_bytes = &buf.filled()[before..after];
                if !new_bytes.is_empty() {
                    this.hasher.update(new_bytes);
                    *this.bytes_read += new_bytes.len() as u64;
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl<R: AsyncBufRead, D: Digest> AsyncBufRead for HashingReader<R, D> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
        self.project().inner.poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let this = self.project();
        *this.bytes_read += amt as u64;
        this.inner.consume(amt);
    }
}

pin_project! {
    /// A writer wrapper that computes a hash digest while data is written through it.
    ///
    /// The hash is updated as data passes through `poll_write`.
    pub struct HashingWriter<W, D> {
        #[pin]
        inner: W,
        hasher: D,
        bytes_written: u64,
    }
}

impl<W, D: Digest + Default> HashingWriter<W, D> {
    /// Create a new hashing writer wrapping the given writer.
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: D::default(),
            bytes_written: 0,
        }
    }

    /// Get the total number of bytes written through this writer.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Get the hash as bytes (finalizes the hasher).
    pub fn finalize_bytes(self) -> Vec<u8> {
        self.hasher.finalize().to_vec()
    }

    /// Get the inner writer back.
    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: AsyncWrite, D: Digest> AsyncWrite for HashingWriter<W, D> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        let this = self.project();

        match this.inner.poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                if n > 0 {
                    this.hasher.update(&buf[..n]);
                    *this.bytes_written += n as u64;
                }
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

    #[tokio::test]
    async fn test_hashing_reader() {
        let data = b"hello world";
        let cursor = std::io::Cursor::new(data.as_slice());
        let buf_reader = BufReader::new(cursor);

        let mut hashing = HashingReader::<_, blake3::Hasher>::new(buf_reader);
        let mut output = Vec::new();
        hashing.read_to_end(&mut output).await.unwrap();

        assert_eq!(output, data);
        assert_eq!(hashing.bytes_read(), 11);

        let hash = hashing.finalize_bytes();
        // blake3 produces 32 bytes
        assert_eq!(hash.len(), 32);
    }

    #[tokio::test]
    async fn test_hashing_writer() {
        let data = b"hello world";
        let buffer = Vec::new();

        let mut hashing = HashingWriter::<_, blake3::Hasher>::new(buffer);
        hashing.write_all(data).await.unwrap();
        hashing.flush().await.unwrap();

        assert_eq!(hashing.bytes_written(), 11);

        let hash = hashing.finalize_bytes();
        // blake3 produces 32 bytes
        assert_eq!(hash.len(), 32);

        // Verify hash matches what we'd get from hashing directly
        let expected_hash = blake3::hash(data);
        assert_eq!(hash, expected_hash.as_bytes());
    }
}
