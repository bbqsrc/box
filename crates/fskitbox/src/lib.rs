//! FSKit-based filesystem driver for Box archives.
//!
//! This crate provides an implementation of Apple's FSKit FSUnaryFileSystem
//! protocol to mount Box archives as read-only filesystems on macOS 15.4+.

#![cfg(target_os = "macos")]

pub mod bindings;
pub mod extension;
pub mod filesystem;
pub mod item;
pub mod volume;

pub use extension::{fskitbox_extension_main, register_filesystem};
pub use filesystem::BoxFS;
pub use item::BoxItem;
pub use volume::BoxVolume;
