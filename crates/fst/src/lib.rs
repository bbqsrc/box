#![no_std]

extern crate alloc;

mod builder;
mod error;
mod fst;
mod node;

pub use builder::{BuilderPrefixIter, FstBuilder};
pub use error::{BuildError, FstError};
pub use fst::{Fst, PrefixIter};
pub use node::FstValue;
