//! Hashing wrappers for computing digests while reading or writing.

use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use digest::Digest;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite, ReadBuf};

/// A reader wrapper that computes a hash digest while data is read through it.
///
/// The hash is updated as data passes through `poll_read`.
pub struct HashingReader<R, D> {
    inner: R,
    hasher: D,
    bytes_read: u64,
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

impl<R: AsyncRead + Unpin, D: Digest + Unpin> AsyncRead for HashingReader<R, D> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        let before = buf.filled().len();

        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let after = buf.filled().len();
                let new_bytes = &buf.filled()[before..after];
                if !new_bytes.is_empty() {
                    self.hasher.update(new_bytes);
                    self.bytes_read += new_bytes.len() as u64;
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl<R: AsyncBufRead + Unpin, D: Digest + Unpin> AsyncBufRead for HashingReader<R, D> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let this = self.get_mut();
        this.bytes_read += amt as u64;
        Pin::new(&mut this.inner).consume(amt);
    }
}

/// A writer wrapper that computes a hash digest while data is written through it.
///
/// The hash is updated as data passes through `poll_write`.
pub struct HashingWriter<W, D> {
    inner: W,
    hasher: D,
    bytes_written: u64,
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

impl<W: AsyncWrite + Unpin, D: Digest + Unpin> AsyncWrite for HashingWriter<W, D> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                if n > 0 {
                    self.hasher.update(&buf[..n]);
                    self.bytes_written += n as u64;
                }
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
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
