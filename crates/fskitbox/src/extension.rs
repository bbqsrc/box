//! App extension entry point for fskitbox.
//!
//! This module provides the main entry point for the FSKit extension.
//! The extension is registered with the system via Info.plist and runs
//! as a separate process managed by fskitd.

use objc2::ClassType;
use objc2::rc::Retained;

use crate::bindings::is_fskit_available;
use crate::filesystem::BoxFS;

/// Register the BoxFS class with the Objective-C runtime.
///
/// This ensures the class is available when FSKit looks for it
/// via the NSExtensionPrincipalClass key in Info.plist.
pub fn register_filesystem() -> Option<Retained<BoxFS>> {
    if !is_fskit_available() {
        tracing::error!("FSKit is not available on this system (requires macOS 15.4+)");
        return None;
    }

    tracing::info!("Registering BoxFS filesystem with FSKit");

    // Create and return the filesystem instance
    // FSKit will retain this and call its protocol methods
    Some(BoxFS::new())
}

/// Main entry point for the extension.
///
/// This is called by the extension host when the extension is loaded.
/// It should set up logging and register the filesystem class.
#[unsafe(no_mangle)]
pub extern "C" fn fskitbox_extension_main() {
    tracing::info!("fskitbox extension starting");

    // The BoxFS class is automatically registered when its module is loaded
    // due to the define_class! macro. FSKit will instantiate it based on
    // the NSExtensionPrincipalClass in Info.plist.

    tracing::info!("BoxFS class registered: {:?}", BoxFS::class());
}
