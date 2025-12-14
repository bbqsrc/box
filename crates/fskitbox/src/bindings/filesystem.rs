//! FSUnaryFileSystem and FSUnaryFileSystemOperations bindings.

use objc2::rc::Retained;
use objc2::runtime::NSObject;
use objc2::{extern_class, extern_methods};

use super::types::FSContainerState;

// MARK: - FSFileSystemBase

extern_class!(
    /// Base class for all filesystem implementations.
    #[unsafe(super(NSObject))]
    #[derive(Debug)]
    pub struct FSFileSystemBase;
);

impl FSFileSystemBase {
    extern_methods!(
        /// Get the current container state.
        #[unsafe(method(containerState))]
        pub fn container_state(&self) -> FSContainerState;

        /// Set the container state.
        #[unsafe(method(setContainerState:))]
        pub fn set_container_state(&self, state: FSContainerState);
    );
}

// MARK: - FSUnaryFileSystem

extern_class!(
    /// A unary filesystem (one resource maps to one volume).
    ///
    /// Most filesystems fit this pattern: HFS+, ExFAT, NTFS, etc.
    #[unsafe(super(FSFileSystemBase, NSObject))]
    #[derive(Debug)]
    pub struct FSUnaryFileSystem;
);

impl FSUnaryFileSystem {
    extern_methods!(
        /// Create a new unary filesystem.
        #[unsafe(method(new))]
        pub fn new() -> Retained<Self>;
    );
}

// Note: FSUnaryFileSystemOperations protocol is implemented directly on BoxFS
// using define_class! with the appropriate method implementations.
// See src/filesystem.rs for the implementation.
