//! FSVolume and related protocol bindings.

use objc2::rc::Retained;
use objc2::runtime::NSObject;
use objc2::{extern_class, extern_methods};
use objc2_foundation::NSString;

use super::types::{FSItem, FSVolumeIdentifier};

// MARK: - FSVolume

extern_class!(
    /// Base class for filesystem volumes.
    #[unsafe(super(NSObject))]
    #[derive(Debug)]
    pub struct FSVolume;
);

impl FSVolume {
    extern_methods!(
        /// Get the volume identifier.
        #[unsafe(method(volumeID))]
        pub fn volume_id(&self) -> Retained<FSVolumeIdentifier>;

        /// Get the volume name.
        #[unsafe(method(volumeName))]
        pub fn volume_name(&self) -> Retained<NSString>;

        /// Set the volume name.
        #[unsafe(method(setVolumeName:))]
        pub fn set_volume_name(&self, name: &NSString);

        /// Get the root item of the volume.
        #[unsafe(method(rootItem))]
        pub fn root_item(&self) -> Option<Retained<FSItem>>;

        /// Set the root item.
        #[unsafe(method(setRootItem:))]
        pub fn set_root_item(&self, item: &FSItem);
    );
}

// Note: The init method `initWithVolumeID:volumeName:` is called via msg_send!
// when initializing subclasses (like BoxVolume) rather than being bound here.

// Note: FSVolume protocol traits (FSVolumeOperations, FSVolumePathConfOperations, etc.)
// are implemented directly on the BoxVolume class using define_class! rather than
// as separate protocol traits. This is because objc2's extern_protocol! is primarily
// for consuming existing protocols, not for implementing them in Rust.
//
// The actual volume operations are implemented in src/volume.rs on the BoxVolume class.
