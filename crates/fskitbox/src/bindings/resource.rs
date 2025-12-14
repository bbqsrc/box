//! FSResource and FSBlockDeviceResource bindings.

use objc2::rc::Retained;
use objc2::runtime::NSObject;
use objc2::{extern_class, extern_methods};
use objc2_foundation::NSURL;

// MARK: - FSResource

extern_class!(
    /// Base class for filesystem resources.
    ///
    /// A resource represents the backing store for a filesystem,
    /// such as a block device or a file.
    #[unsafe(super(NSObject))]
    #[derive(Debug)]
    pub struct FSResource;
);

impl FSResource {
    extern_methods!(
        /// Check if the resource is revoked/disconnected.
        #[unsafe(method(isRevoked))]
        pub fn is_revoked(&self) -> bool;
    );
}

// MARK: - FSBlockDeviceResource

extern_class!(
    /// A block device resource.
    ///
    /// Represents a disk partition or similar block-addressable storage.
    #[unsafe(super(FSResource, NSObject))]
    #[derive(Debug)]
    pub struct FSBlockDeviceResource;
);

impl FSBlockDeviceResource {
    extern_methods!(
        /// Get the block size in bytes.
        #[unsafe(method(blockSize))]
        pub fn block_size(&self) -> u64;

        /// Get the total number of blocks.
        #[unsafe(method(blockCount))]
        pub fn block_count(&self) -> u64;

        /// Check if the device is read-only.
        #[unsafe(method(isReadOnly))]
        pub fn is_read_only(&self) -> bool;

        /// Check if the device is removable.
        #[unsafe(method(isRemovable))]
        pub fn is_removable(&self) -> bool;
    );
}

// MARK: - FSGenericURLResource

extern_class!(
    /// A URL-based resource.
    ///
    /// Represents a file or directory path as a resource.
    #[unsafe(super(FSResource, NSObject))]
    #[derive(Debug)]
    pub struct FSGenericURLResource;
);

impl FSGenericURLResource {
    extern_methods!(
        /// Get the URL of the resource.
        #[unsafe(method(url))]
        pub fn url(&self) -> Retained<NSURL>;
    );
}
