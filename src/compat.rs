//! Compatibility layer for std/no_std builds.
//!
//! This module re-exports types from either std or alloc/core
//! depending on the `std` feature flag.

// Collections
#[cfg(feature = "std")]
pub use std::collections::{BTreeMap, VecDeque};
#[cfg(not(feature = "std"))]
pub use alloc::collections::{BTreeMap, VecDeque};

// HashMap from hashbrown (works in both std and no_std)
pub use hashbrown::HashMap;

// Cow
#[cfg(feature = "std")]
pub use std::borrow::Cow;
#[cfg(not(feature = "std"))]
pub use alloc::borrow::Cow;

// Vec and vec! macro
#[cfg(feature = "std")]
pub use std::vec::Vec;
#[cfg(not(feature = "std"))]
pub use alloc::vec::Vec;

#[cfg(feature = "std")]
pub use std::vec;
#[cfg(not(feature = "std"))]
pub use alloc::vec;

// Box
#[cfg(feature = "std")]
pub use std::boxed::Box;
#[cfg(not(feature = "std"))]
pub use alloc::boxed::Box;

// String
#[cfg(feature = "std")]
pub use std::string::String;
#[cfg(not(feature = "std"))]
pub use alloc::string::String;

// Core types that are always from core
pub use core::num::NonZeroU64;
pub use core::fmt;
pub use core::str;
