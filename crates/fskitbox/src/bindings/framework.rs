//! FSKit framework linking and availability definitions.

use objc2::runtime::AnyClass;

// Link to FSKit.framework at build time.
#[link(name = "FSKit", kind = "framework")]
unsafe extern "C" {}

/// Check if FSKit is available on the current system.
/// FSKit requires macOS 15.4 or later.
pub fn is_fskit_available() -> bool {
    // Check if FSUnaryFileSystem class exists
    AnyClass::get(c"FSUnaryFileSystem").is_some()
}
