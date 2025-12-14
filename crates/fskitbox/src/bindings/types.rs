//! Core FSKit types: FSFileName, FSItemAttributes, FSProbeResult, etc.

use objc2::encode::{Encode, Encoding};
use objc2::rc::Retained;
use objc2::runtime::NSObject;
use objc2::{extern_class, extern_methods};
use objc2_foundation::{NSData, NSString, NSUUID};

// MARK: - FSFileName

extern_class!(
    /// A filename represented as bytes or a UTF-8 string.
    #[unsafe(super(NSObject))]
    #[derive(Debug, PartialEq, Eq, Hash)]
    pub struct FSFileName;
);

impl FSFileName {
    extern_methods!(
        /// Create a filename from a UTF-8 string.
        #[unsafe(method(nameWithString:))]
        pub fn with_string(string: &NSString) -> Retained<Self>;

        /// Create a filename from raw bytes.
        #[unsafe(method(nameWithData:))]
        pub fn with_data(data: &NSData) -> Retained<Self>;

        /// Get the filename as a string, if it's valid UTF-8.
        #[unsafe(method(string))]
        pub fn string(&self) -> Option<Retained<NSString>>;

        /// Get the filename as raw bytes.
        #[unsafe(method(data))]
        pub fn data(&self) -> Retained<NSData>;
    );
}

// MARK: - FSItemIdentifier

/// Unique identifier for filesystem items.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FSItemIdentifier(pub u64);

impl FSItemIdentifier {
    /// The root directory identifier.
    pub const ROOT_DIRECTORY: Self = Self(1);

    /// Invalid identifier.
    pub const INVALID: Self = Self(0);

    /// Create a new identifier from a raw value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Get the raw value.
    pub const fn raw(self) -> u64 {
        self.0
    }
}

// Implement Encode for FSItemIdentifier so it can be passed to Objective-C
unsafe impl Encode for FSItemIdentifier {
    const ENCODING: Encoding = u64::ENCODING;
}

// MARK: - FSItemType

/// Type of filesystem item.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FSItemType {
    /// Unknown item type.
    Unknown = 0,
    /// Regular file.
    File = 1,
    /// Directory.
    Directory = 2,
    /// Symbolic link.
    SymLink = 3,
    /// Block device.
    BlockDevice = 4,
    /// Character device.
    CharDevice = 5,
    /// FIFO (named pipe).
    FIFO = 6,
    /// Socket.
    Socket = 7,
}

unsafe impl Encode for FSItemType {
    const ENCODING: Encoding = u32::ENCODING;
}

// MARK: - FSItemAttributes

extern_class!(
    /// Attributes of a filesystem item.
    #[unsafe(super(NSObject))]
    #[derive(Debug)]
    pub struct FSItemAttributes;
);

impl FSItemAttributes {
    extern_methods!(
        /// Create new item attributes.
        #[unsafe(method(new))]
        pub fn new() -> Retained<Self>;

        // Setters
        #[unsafe(method(setUid:))]
        pub fn set_uid(&self, uid: u32);

        #[unsafe(method(setGid:))]
        pub fn set_gid(&self, gid: u32);

        #[unsafe(method(setMode:))]
        pub fn set_mode(&self, mode: u32);

        #[unsafe(method(setType:))]
        pub fn set_type(&self, item_type: FSItemType);

        #[unsafe(method(setLinkCount:))]
        pub fn set_link_count(&self, count: u32);

        #[unsafe(method(setFlags:))]
        pub fn set_flags(&self, flags: u32);

        #[unsafe(method(setSize:))]
        pub fn set_size(&self, size: u64);

        #[unsafe(method(setAllocSize:))]
        pub fn set_alloc_size(&self, size: u64);

        #[unsafe(method(setFileID:))]
        pub fn set_file_id(&self, file_id: FSItemIdentifier);

        #[unsafe(method(setParentID:))]
        pub fn set_parent_id(&self, parent_id: FSItemIdentifier);

        // Getters
        #[unsafe(method(uid))]
        pub fn uid(&self) -> u32;

        #[unsafe(method(gid))]
        pub fn gid(&self) -> u32;

        #[unsafe(method(mode))]
        pub fn mode(&self) -> u32;

        #[unsafe(method(type))]
        pub fn item_type(&self) -> FSItemType;

        #[unsafe(method(linkCount))]
        pub fn link_count(&self) -> u32;

        #[unsafe(method(flags))]
        pub fn flags(&self) -> u32;

        #[unsafe(method(size))]
        pub fn size(&self) -> u64;

        #[unsafe(method(allocSize))]
        pub fn alloc_size(&self) -> u64;

        #[unsafe(method(fileID))]
        pub fn file_id(&self) -> FSItemIdentifier;

        #[unsafe(method(parentID))]
        pub fn parent_id(&self) -> FSItemIdentifier;
    );
}

// MARK: - FSContainerIdentifier

extern_class!(
    /// Identifier for a filesystem container.
    #[unsafe(super(NSObject))]
    #[derive(Debug, PartialEq, Eq, Hash)]
    pub struct FSContainerIdentifier;
);

impl FSContainerIdentifier {
    extern_methods!(
        /// Create a container identifier from a UUID.
        #[unsafe(method(containerIdentifierWithUUID:))]
        pub fn with_uuid(uuid: &NSUUID) -> Retained<Self>;

        /// Get the UUID.
        #[unsafe(method(uuid))]
        pub fn uuid(&self) -> Retained<NSUUID>;
    );
}

// MARK: - FSVolumeIdentifier

extern_class!(
    /// Identifier for a volume.
    #[unsafe(super(NSObject))]
    #[derive(Debug, PartialEq, Eq, Hash)]
    pub struct FSVolumeIdentifier;
);

impl FSVolumeIdentifier {
    extern_methods!(
        /// Create a volume identifier from a UUID.
        #[unsafe(method(volumeIdentifierWithUUID:))]
        pub fn with_uuid(uuid: &NSUUID) -> Retained<Self>;

        /// Get the UUID.
        #[unsafe(method(uuid))]
        pub fn uuid(&self) -> Retained<NSUUID>;
    );
}

// MARK: - FSMatchResult

/// Result of matching a resource.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FSMatchResult {
    /// Resource not recognized.
    NotRecognized = 0,
    /// Resource recognized but not usable.
    Recognized = 1,
    /// Resource recognized and usable.
    Usable = 2,
}

unsafe impl Encode for FSMatchResult {
    const ENCODING: Encoding = i32::ENCODING;
}

// MARK: - FSProbeResult

extern_class!(
    /// Result of probing a resource.
    #[unsafe(super(NSObject))]
    #[derive(Debug)]
    pub struct FSProbeResult;
);

impl FSProbeResult {
    extern_methods!(
        /// Create a probe result indicating the resource was recognized.
        #[unsafe(method(resultWithResult:name:containerID:))]
        pub fn with_result_name_container(
            result: FSMatchResult,
            name: &NSString,
            container_id: &FSContainerIdentifier,
        ) -> Retained<Self>;

        /// Create a probe result for an unrecognized resource.
        #[unsafe(method(resultNotRecognized))]
        pub fn not_recognized() -> Retained<Self>;
    );
}

// MARK: - FSContainerState

/// State of a filesystem container.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FSContainerState {
    /// Container is not ready.
    NotReady = 0,
    /// Container is blocked.
    Blocked = 1,
    /// Container is ready.
    Ready = 2,
    /// Container is active.
    Active = 3,
}

unsafe impl Encode for FSContainerState {
    const ENCODING: Encoding = i32::ENCODING;
}

// MARK: - FSItem

extern_class!(
    /// Base class for filesystem items.
    #[unsafe(super(NSObject))]
    #[derive(Debug)]
    pub struct FSItem;
);

impl FSItem {
    extern_methods!(
        /// Create a new item with an identifier.
        #[unsafe(method(itemWithIdentifier:))]
        pub fn with_identifier(identifier: FSItemIdentifier) -> Retained<Self>;

        /// Get the item's identifier.
        #[unsafe(method(identifier))]
        pub fn identifier(&self) -> FSItemIdentifier;
    );
}

// MARK: - FSTaskOptions

extern_class!(
    /// Options for filesystem tasks.
    #[unsafe(super(NSObject))]
    #[derive(Debug)]
    pub struct FSTaskOptions;
);

impl FSTaskOptions {
    extern_methods!(
        /// Check if the force option is set.
        #[unsafe(method(isForce))]
        pub fn is_force(&self) -> bool;

        /// Check if the read-only option is set.
        #[unsafe(method(isReadOnly))]
        pub fn is_read_only(&self) -> bool;
    );
}
