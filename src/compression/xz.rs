//! XZ/LZMA compression state machines using xz2::Stream.
//!
//! These are pure buffer-to-buffer operations with no I/O traits.

use std::io::{Error, ErrorKind, Result};
use xz2::stream::{Action, Check, Status, Stream};

use super::StreamStatus;

/// Streaming XZ compressor.
///
/// Uses xz2's Stream for buffer-to-buffer compression without I/O traits.
pub struct XzCompressor {
    stream: Stream,
}

impl XzCompressor {
    /// Create a new compressor with the specified compression level (0-9).
    pub fn new(level: u32) -> Result<Self> {
        let stream = Stream::new_easy_encoder(level, Check::Crc64)
            .map_err(|e| Error::new(ErrorKind::Other, e))?;
        Ok(Self { stream })
    }

    /// Process input data into output buffer.
    ///
    /// Call repeatedly until all input is consumed.
    /// Returns how many bytes were consumed from input and produced to output.
    pub fn compress(&mut self, input: &[u8], output: &mut [u8]) -> Result<StreamStatus> {
        let before_in = self.stream.total_in();
        let before_out = self.stream.total_out();

        self.stream
            .process(input, output, Action::Run)
            .map_err(|e| Error::new(ErrorKind::Other, e))?;

        Ok(StreamStatus::Progress {
            bytes_consumed: (self.stream.total_in() - before_in) as usize,
            bytes_produced: (self.stream.total_out() - before_out) as usize,
        })
    }

    /// Finish compression and flush remaining data.
    ///
    /// Call repeatedly until `StreamStatus::Done` is returned.
    pub fn finish(&mut self, output: &mut [u8]) -> Result<StreamStatus> {
        let before_out = self.stream.total_out();

        match self
            .stream
            .process(&[], output, Action::Finish)
            .map_err(|e| Error::new(ErrorKind::Other, e))?
        {
            Status::StreamEnd => Ok(StreamStatus::Done {
                bytes_consumed: 0,
                bytes_produced: (self.stream.total_out() - before_out) as usize,
            }),
            Status::Ok | Status::MemNeeded => Ok(StreamStatus::Progress {
                bytes_consumed: 0,
                bytes_produced: (self.stream.total_out() - before_out) as usize,
            }),
            Status::GetCheck => Ok(StreamStatus::Progress {
                bytes_consumed: 0,
                bytes_produced: (self.stream.total_out() - before_out) as usize,
            }),
        }
    }
}

/// Streaming XZ decompressor.
///
/// Uses xz2's Stream for buffer-to-buffer decompression without I/O traits.
pub struct XzDecompressor {
    stream: Stream,
}

impl XzDecompressor {
    /// Create a new decompressor.
    pub fn new() -> Result<Self> {
        let stream =
            Stream::new_stream_decoder(u64::MAX, 0).map_err(|e| Error::new(ErrorKind::Other, e))?;
        Ok(Self { stream })
    }

    /// Decompress input data into output buffer.
    ///
    /// Call repeatedly until all input is consumed and `StreamStatus::Done` is returned.
    /// Returns how many bytes were consumed from input and produced to output.
    pub fn decompress(&mut self, input: &[u8], output: &mut [u8]) -> Result<StreamStatus> {
        let before_in = self.stream.total_in();
        let before_out = self.stream.total_out();

        match self
            .stream
            .process(input, output, Action::Run)
            .map_err(|e| Error::new(ErrorKind::Other, e))?
        {
            Status::StreamEnd => Ok(StreamStatus::Done {
                bytes_consumed: (self.stream.total_in() - before_in) as usize,
                bytes_produced: (self.stream.total_out() - before_out) as usize,
            }),
            Status::Ok | Status::MemNeeded | Status::GetCheck => Ok(StreamStatus::Progress {
                bytes_consumed: (self.stream.total_in() - before_in) as usize,
                bytes_produced: (self.stream.total_out() - before_out) as usize,
            }),
        }
    }
}

impl Default for XzDecompressor {
    fn default() -> Self {
        Self::new().expect("failed to create decompressor")
    }
}

/// Compress a complete buffer using the state machine.
///
/// Convenience function that handles the full compress + finish cycle.
pub fn compress_buffer(input: &[u8], level: u32) -> Result<Vec<u8>> {
    let mut compressor = XzCompressor::new(level)?;

    // XZ has significant overhead, allocate generously
    let mut output = vec![0u8; input.len() + 1024];
    let mut out_pos = 0;
    let mut in_pos = 0;

    // Compress all input
    while in_pos < input.len() {
        // Ensure we have output space
        if out_pos >= output.len() {
            output.resize(output.len() * 2, 0);
        }

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
        // Ensure we have output space
        if out_pos >= output.len() {
            output.resize(output.len() * 2, 0);
        }

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
pub fn decompress_buffer(input: &[u8]) -> Result<Vec<u8>> {
    let mut decompressor = XzDecompressor::new()?;

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
        let compressed = compress_buffer(data, 6).unwrap();
        let decompressed = decompress_buffer(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_streaming_compress() {
        let data = b"hello world hello world hello world hello world";
        let mut compressor = XzCompressor::new(6).unwrap();

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
        let decompressed = decompress_buffer(&output).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_different_levels() {
        let data = b"hello world hello world hello world hello world";

        for level in [0, 3, 6, 9] {
            let compressed = compress_buffer(data, level).unwrap();
            let decompressed = decompress_buffer(&compressed).unwrap();
            assert_eq!(decompressed, data, "failed at level {}", level);
        }
    }
}
