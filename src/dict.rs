//! Zstd dictionary training for improved compression ratios.
//!
//! This module provides utilities for training Zstd dictionaries from file samples.
//! When an archive contains many similar files, a trained dictionary can significantly
//! improve compression ratios.

/// Default dictionary size in bytes (32KB).
pub const DEFAULT_DICT_SIZE: usize = 32 * 1024;

/// Minimum total sample size for training (should be ~100x dictionary size).
pub const MIN_SAMPLE_SIZE_MULTIPLIER: usize = 100;

/// Maximum sample size per file (4KB).
pub const MAX_SAMPLE_PER_FILE: usize = 4 * 1024;

/// Collects samples from files for Zstd dictionary training.
#[derive(Debug, Default)]
pub struct DictionaryTrainer {
    samples: Vec<Vec<u8>>,
    total_size: usize,
}

impl DictionaryTrainer {
    /// Create a new dictionary trainer.
    pub fn new() -> Self {
        Self {
            samples: Vec::new(),
            total_size: 0,
        }
    }

    /// Add a sample from file content.
    /// Samples are typically the first few KB of each file.
    pub fn add_sample(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }

        // Limit sample size per file
        let sample = if data.len() > MAX_SAMPLE_PER_FILE {
            &data[..MAX_SAMPLE_PER_FILE]
        } else {
            data
        };

        self.total_size += sample.len();
        self.samples.push(sample.to_vec());
    }

    /// Add a sample, taking ownership of the data.
    pub fn add_sample_owned(&mut self, mut data: Vec<u8>) {
        if data.is_empty() {
            return;
        }

        // Limit sample size per file
        if data.len() > MAX_SAMPLE_PER_FILE {
            data.truncate(MAX_SAMPLE_PER_FILE);
        }

        self.total_size += data.len();
        self.samples.push(data);
    }

    /// Returns the total size of collected samples.
    pub fn total_size(&self) -> usize {
        self.total_size
    }

    /// Returns the number of samples collected.
    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }

    /// Check if we have enough samples to train a dictionary of the given size.
    pub fn has_enough_samples(&self, dict_size: usize) -> bool {
        self.total_size >= dict_size * MIN_SAMPLE_SIZE_MULTIPLIER
    }

    /// Train a dictionary from the collected samples.
    ///
    /// Returns `None` if:
    /// - No samples were collected
    /// - Not enough sample data for the requested dictionary size
    /// - Training fails for any reason
    ///
    /// The recommended dictionary size is 32KB-128KB.
    #[cfg(feature = "zstd")]
    pub fn train(self, dict_size: usize) -> Option<Vec<u8>> {
        if self.samples.is_empty() {
            tracing::debug!("no samples collected for dictionary training");
            return None;
        }

        if !self.has_enough_samples(dict_size) {
            tracing::debug!(
                total_size = self.total_size,
                required = dict_size * MIN_SAMPLE_SIZE_MULTIPLIER,
                "insufficient samples for dictionary training"
            );
            return None;
        }

        tracing::debug!(
            samples = self.samples.len(),
            total_size = self.total_size,
            dict_size,
            "training zstd dictionary"
        );

        match zstd::dict::from_samples(&self.samples, dict_size) {
            Ok(dict) => {
                tracing::debug!(dict_len = dict.len(), "dictionary trained successfully");
                Some(dict)
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to train dictionary");
                None
            }
        }
    }

    /// Train a dictionary with the default size.
    #[cfg(feature = "zstd")]
    pub fn train_default(self) -> Option<Vec<u8>> {
        self.train(DEFAULT_DICT_SIZE)
    }
}

#[cfg(all(test, feature = "zstd"))]
mod tests {
    use super::*;

    #[test]
    fn test_trainer_empty() {
        let trainer = DictionaryTrainer::new();
        assert!(trainer.train(DEFAULT_DICT_SIZE).is_none());
    }

    #[test]
    fn test_trainer_insufficient_samples() {
        let mut trainer = DictionaryTrainer::new();
        // Add only a small amount of data
        trainer.add_sample(b"hello world");
        assert!(!trainer.has_enough_samples(DEFAULT_DICT_SIZE));
        assert!(trainer.train(DEFAULT_DICT_SIZE).is_none());
    }

    #[test]
    fn test_trainer_with_samples() {
        let mut trainer = DictionaryTrainer::new();

        // Generate enough sample data (need ~100x dict size)
        // For a small test dictionary of 1KB, we need ~100KB of samples
        let sample = b"This is a test sample with some repeated content. ".repeat(100);
        for _ in 0..30 {
            trainer.add_sample(&sample);
        }

        assert!(trainer.has_enough_samples(1024));

        // Train a small dictionary
        let dict = trainer.train(1024);
        assert!(dict.is_some());
        let dict = dict.unwrap();
        assert!(!dict.is_empty());
    }

    #[test]
    fn test_sample_truncation() {
        let mut trainer = DictionaryTrainer::new();
        let large_sample = vec![0u8; MAX_SAMPLE_PER_FILE * 2];
        trainer.add_sample(&large_sample);

        assert_eq!(trainer.total_size(), MAX_SAMPLE_PER_FILE);
        assert_eq!(trainer.samples[0].len(), MAX_SAMPLE_PER_FILE);
    }
}
