# Box Open Standard Archive Format

[![Actions Status](https://github.com/bbqsrc/box/workflows/Continuous%20Integration/badge.svg)](https://github.com/bbqsrc/box/actions)

The `.box` file format is designed to be a modern successor to formats such as `.zip` and `.7z`,
removing several painpoints:

* **Cross-platform path support**, with relative paths only and platform-agnostic separators
* **UTF-8 only, unicode normalised** path names and string data
* Extensible with **space-efficient attributes in key-value pairs** for records and whole archives
  * This mechanism is how Unix and Windows-specific file attributes are carried
* Configurable optional **byte-alignment of files** (so you can memory map data easily)
* Support for files and directories
  * Support for symlinks, hard links and other platform-specific specialities is coming
* **Multiple compression methods within a single archive**, including:
  * Stored (no compress)
  * Brotli
  * DEFLATE
  * Snappy
  * xz
  * Zstandard
* Well-defined, **open specification** of file format (in progress)

See the `fusebox` repo for an example of the `.box` file format being used with a FUSE driver, also
written in Rust. :smile:

## Support platforms

* Windows
* macOS
* Linux
* iOS
* Android

## License

Licensed under either of

* Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
