// Minimal host application for fskitbox FSKit extension.
// This app exists solely to host the filesystem extension.

import AppKit

@main
struct FskitboxApp {
    static func main() {
        // The host app can be a simple status bar app or invisible.
        // For now, we just run a minimal event loop.
        let app = NSApplication.shared
        app.setActivationPolicy(.accessory)
        app.run()
    }
}
