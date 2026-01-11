#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "std")]
mod builder;
mod error;
mod fst;
mod node;

#[cfg(feature = "std")]
pub use builder::FstBuilder;
pub use error::{BuildError, FstError};
pub use fst::{Fst, PrefixIter};
