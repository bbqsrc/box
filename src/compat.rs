//! Compatibility layer for std/no_std builds.
//!
//! This module re-exports types from either std or alloc/core
//! depending on the `std` feature flag.

// Collections
#[cfg(not(feature = "std"))]
pub use alloc::collections::{BTreeMap, VecDeque};
#[cfg(feature = "std")]
pub use std::collections::{BTreeMap, VecDeque};

// HashMap from hashbrown (works in both std and no_std)
pub use hashbrown::HashMap;

// Cow
#[cfg(not(feature = "std"))]
pub use alloc::borrow::Cow;
#[cfg(feature = "std")]
pub use std::borrow::Cow;

// Vec and vec! macro
#[cfg(not(feature = "std"))]
pub use alloc::vec::Vec;
#[cfg(feature = "std")]
pub use std::vec::Vec;

#[cfg(not(feature = "std"))]
pub use alloc::vec;
#[cfg(feature = "std")]
pub use std::vec;

// Box
#[cfg(not(feature = "std"))]
pub use alloc::boxed::Box;
#[cfg(feature = "std")]
pub use std::boxed::Box;

// String
#[cfg(not(feature = "std"))]
pub use alloc::string::String;
#[cfg(feature = "std")]
pub use std::string::String;

// Core types that are always from core
pub use core::fmt;
pub use core::num::NonZeroU64;
pub use core::str;
