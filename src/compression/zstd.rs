//! Zstd compression state machines using zstd-safe.
//!
//! These are pure buffer-to-buffer operations with no I/O traits.

use std::io::{Error, ErrorKind, Result};
use zstd_safe::{CCtx, DCtx, InBuffer, OutBuffer, ResetDirective, get_error_name};

use super::StreamStatus;

/// Convert a zstd error code to an io::Error.
fn zstd_error(code: usize) -> Error {
    Error::new(ErrorKind::Other, get_error_name(code))
}

/// Streaming Zstd compressor.
///
/// Uses zstd-safe's CCtx for buffer-to-buffer compression without I/O traits.
pub struct ZstdCompressor<'a> {
    ctx: CCtx<'a>,
    level: i32,
}

impl ZstdCompressor<'_> {
    /// Create a new compressor with the specified compression level.
    pub fn new(level: i32) -> Result<Self> {
        let mut ctx = CCtx::create();
        ctx.init(level).map_err(zstd_error)?;
        Ok(Self { ctx, level })
    }

    /// Create a compressor with a dictionary.
    pub fn with_dictionary(level: i32, dictionary: &[u8]) -> Result<Self> {
        let mut ctx = CCtx::create();
        ctx.init(level).map_err(zstd_error)?;
        ctx.load_dictionary(dictionary).map_err(zstd_error)?;
        Ok(Self { ctx, level })
    }

    /// Process input data into output buffer.
    ///
    /// Call repeatedly until all input is consumed.
    /// Returns how many bytes were consumed from input and produced to output.
    pub fn compress(&mut self, input: &[u8], output: &mut [u8]) -> Result<StreamStatus> {
        let mut in_buf = InBuffer::around(input);
        let mut out_buf = OutBuffer::around(output);

        self.ctx
            .compress_stream(&mut out_buf, &mut in_buf)
            .map_err(zstd_error)?;

        Ok(StreamStatus::Progress {
            bytes_consumed: in_buf.pos(),
            bytes_produced: out_buf.pos(),
        })
    }

    /// Finish compression and flush remaining data.
    ///
    /// Call repeatedly until `StreamStatus::Done` is returned.
    pub fn finish(&mut self, output: &mut [u8]) -> Result<StreamStatus> {
        let mut out_buf = OutBuffer::around(output);

        let remaining = self.ctx.end_stream(&mut out_buf).map_err(zstd_error)?;

        if remaining == 0 {
            Ok(StreamStatus::Done {
                bytes_consumed: 0,
                bytes_produced: out_buf.pos(),
            })
        } else {
            Ok(StreamStatus::Progress {
                bytes_consumed: 0,
                bytes_produced: out_buf.pos(),
            })
        }
    }

    /// Reset the compressor for reuse.
    pub fn reset(&mut self) -> Result<()> {
        self.ctx
            .reset(ResetDirective::SessionOnly)
            .map_err(zstd_error)?;
        self.ctx.init(self.level).map_err(zstd_error)?;
        Ok(())
    }
}

/// Streaming Zstd decompressor.
///
/// Uses zstd-safe's DCtx for buffer-to-buffer decompression without I/O traits.
pub struct ZstdDecompressor<'a> {
    ctx: DCtx<'a>,
}

impl ZstdDecompressor<'_> {
    /// Create a new decompressor.
    pub fn new() -> Result<Self> {
        let mut ctx = DCtx::create();
        ctx.init().map_err(zstd_error)?;
        Ok(Self { ctx })
    }

    /// Create a decompressor with a dictionary.
    pub fn with_dictionary(dictionary: &[u8]) -> Result<Self> {
        let mut ctx = DCtx::create();
        ctx.init().map_err(zstd_error)?;
        ctx.load_dictionary(dictionary).map_err(zstd_error)?;
        Ok(Self { ctx })
    }

    /// Decompress input data into output buffer.
    ///
    /// Call repeatedly until all input is consumed and `StreamStatus::Done` is returned.
    /// Returns how many bytes were consumed from input and produced to output.
    pub fn decompress(&mut self, input: &[u8], output: &mut [u8]) -> Result<StreamStatus> {
        let mut in_buf = InBuffer::around(input);
        let mut out_buf = OutBuffer::around(output);

        let hint = self
            .ctx
            .decompress_stream(&mut out_buf, &mut in_buf)
            .map_err(zstd_error)?;

        // hint == 0 means frame is complete
        if hint == 0 && in_buf.pos() == input.len() {
            Ok(StreamStatus::Done {
                bytes_consumed: in_buf.pos(),
                bytes_produced: out_buf.pos(),
            })
        } else {
            Ok(StreamStatus::Progress {
                bytes_consumed: in_buf.pos(),
                bytes_produced: out_buf.pos(),
            })
        }
    }

    /// Reset the decompressor for reuse.
    pub fn reset(&mut self) -> Result<()> {
        self.ctx
            .reset(ResetDirective::SessionOnly)
            .map_err(zstd_error)?;
        self.ctx.init().map_err(zstd_error)?;
        Ok(())
    }
}

impl Default for ZstdDecompressor<'_> {
    fn default() -> Self {
        Self::new().expect("failed to create decompressor")
    }
}

/// Compress a complete buffer using the state machine.
///
/// Convenience function that handles the full compress + finish cycle.
pub fn compress_buffer(input: &[u8], level: i32, dictionary: Option<&[u8]>) -> Result<Vec<u8>> {
    let mut compressor = match dictionary {
        Some(dict) => ZstdCompressor::with_dictionary(level, dict)?,
        None => ZstdCompressor::new(level)?,
    };

    // Allocate output buffer (zstd provides a bound)
    let bound = zstd_safe::compress_bound(input.len());
    let mut output = vec![0u8; bound];
    let mut out_pos = 0;
    let mut in_pos = 0;

    // Compress all input
    while in_pos < input.len() {
        match compressor.compress(&input[in_pos..], &mut output[out_pos..])? {
            StreamStatus::Progress {
                bytes_consumed,
                bytes_produced,
            } => {
                in_pos += bytes_consumed;
                out_pos += bytes_produced;
            }
            StreamStatus::Done { .. } => break,
        }
    }

    // Finish compression
    loop {
        match compressor.finish(&mut output[out_pos..])? {
            StreamStatus::Progress { bytes_produced, .. } => {
                out_pos += bytes_produced;
            }
            StreamStatus::Done { bytes_produced, .. } => {
                out_pos += bytes_produced;
                break;
            }
        }
    }

    output.truncate(out_pos);
    Ok(output)
}

/// Decompress a complete buffer using the state machine.
///
/// Convenience function that handles the full decompress cycle.
pub fn decompress_buffer(input: &[u8], dictionary: Option<&[u8]>) -> Result<Vec<u8>> {
    let mut decompressor = match dictionary {
        Some(dict) => ZstdDecompressor::with_dictionary(dict)?,
        None => ZstdDecompressor::new()?,
    };

    // Start with a reasonable output buffer, grow as needed
    let mut output = vec![0u8; input.len() * 4];
    let mut out_pos = 0;
    let mut in_pos = 0;

    loop {
        // Ensure we have output space
        if out_pos >= output.len() {
            output.resize(output.len() * 2, 0);
        }

        match decompressor.decompress(&input[in_pos..], &mut output[out_pos..])? {
            StreamStatus::Progress {
                bytes_consumed,
                bytes_produced,
            } => {
                in_pos += bytes_consumed;
                out_pos += bytes_produced;

                // If no progress was made and we have input, we need more output space
                if bytes_consumed == 0 && bytes_produced == 0 && in_pos < input.len() {
                    output.resize(output.len() * 2, 0);
                }
            }
            StreamStatus::Done {
                bytes_consumed: _,
                bytes_produced,
            } => {
                out_pos += bytes_produced;
                break;
            }
        }
    }

    output.truncate(out_pos);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress_roundtrip() {
        let data = b"hello world hello world hello world hello world";
        let compressed = compress_buffer(data, 3, None).unwrap();
        let decompressed = decompress_buffer(&compressed, None).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_streaming_compress() {
        let data = b"hello world hello world hello world hello world";
        let mut compressor = ZstdCompressor::new(3).unwrap();

        let mut output = vec![0u8; 1024];
        let mut out_pos = 0;

        // Compress in small chunks
        for chunk in data.chunks(10) {
            match compressor.compress(chunk, &mut output[out_pos..]).unwrap() {
                StreamStatus::Progress { bytes_produced, .. } => {
                    out_pos += bytes_produced;
                }
                StreamStatus::Done { bytes_produced, .. } => {
                    out_pos += bytes_produced;
                }
            }
        }

        // Finish
        loop {
            match compressor.finish(&mut output[out_pos..]).unwrap() {
                StreamStatus::Progress { bytes_produced, .. } => {
                    out_pos += bytes_produced;
                }
                StreamStatus::Done { bytes_produced, .. } => {
                    out_pos += bytes_produced;
                    break;
                }
            }
        }

        output.truncate(out_pos);

        // Verify roundtrip
        let decompressed = decompress_buffer(&output, None).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_with_dictionary() {
        let samples: Vec<&[u8]> = vec![b"hello world"; 100];
        let dict = zstd::dict::from_samples(&samples, 1024).unwrap();

        let data = b"hello world hello world hello world";
        let compressed = compress_buffer(data, 3, Some(&dict)).unwrap();
        let decompressed = decompress_buffer(&compressed, Some(&dict)).unwrap();
        assert_eq!(decompressed, data);
    }
}
