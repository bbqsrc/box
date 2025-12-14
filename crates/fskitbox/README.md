# fskitbox

FSKit-based filesystem driver for Box archives on macOS 15.4+.

## Overview

This crate provides a pure Rust implementation of Apple's FSKit `FSUnaryFileSystem` protocol to mount Box archives as read-only filesystems. It serves as a native macOS alternative to the FUSE-based `fusebox`.

## Requirements

- macOS 15.4 (Sequoia) or later
- Xcode 16.1 or later
- XcodeGen (`brew install xcodegen`)
- Valid Apple Developer signing identity

## Building

1. Build the Rust static library:

```bash
cargo build --release -p fskitbox
```

2. Generate the Xcode project:

```bash
cd crates/fskitbox
xcodegen generate
```

3. Build and sign the app bundle:

```bash
xcodebuild -scheme fskitbox -configuration Release
```

## Architecture

```
fskitbox/
├── src/
│   ├── lib.rs           # Library root
│   ├── bindings/        # FSKit Objective-C bindings via objc2
│   │   ├── mod.rs
│   │   ├── framework.rs # FSKit framework linking
│   │   ├── types.rs     # FSFileName, FSItemAttributes, etc.
│   │   ├── resource.rs  # FSResource, FSBlockDeviceResource
│   │   ├── volume.rs    # FSVolume bindings
│   │   └── filesystem.rs # FSUnaryFileSystem bindings
│   ├── filesystem.rs    # BoxFS implementation
│   ├── volume.rs        # BoxVolume implementation
│   ├── item.rs          # BoxItem record wrapper
│   └── extension.rs     # App extension entry point
├── host/                # Minimal host app (Swift)
├── extension/           # Extension entry point (ObjC)
├── Info.plist           # Extension metadata
├── fskitbox.entitlements
└── project.yml          # XcodeGen specification
```

## FSKit Protocols Implemented

- `FSUnaryFileSystemOperations`: `probeResource`, `loadResource`, `unloadResource`
- `FSVolumePathConfOperations`: Path configuration properties
- `FSVolumeOperations`: Core volume operations (TODO)
- `FSVolumeOpenCloseOperations`: File open/close (TODO)
- `FSVolumeReadWriteOperations`: File read (TODO)

## Status

This is an early implementation. The FSKit bindings and basic structure are in place, but full volume operations need to be implemented to match the functionality of fusebox.

## License

Apache-2.0 OR MIT
