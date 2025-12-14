# Box - Open Standard Archive Format

[![Actions Status](https://github.com/bbqsrc/box/workflows/CI/badge.svg)](https://github.com/bbqsrc/box/actions)

<table>
<tr><td>⚠️<td>Box is under active development. Do not expect compatibility between versions until v1.0.
</table>

The `.box` file format and related tooling is designed to be a modern successor to formats such as
`.zip` and `.7z`, removing several painpoints and introducing modern features and expectations.

## Crates

| Crate | Description |
|-------|-------------|
| **box-format** (this crate) | Core library for reading and writing `.box` archives |
| [**cli**](cli/) | Command line tool for working with `.box` archives |
| [**fusebox**](crates/fusebox/) | FUSE driver for mounting archives (Linux/macOS) |
| [**projfsbox**](crates/projfsbox/) | Windows ProjFS driver for mounting archives |
| [**fskitbox**](crates/fskitbox/) | macOS FSKit driver for mounting archives (macOS 15.4+) |

## Features

<table>
<tr><td>🌉<td><strong>Cross-platform path support</strong>, with relative paths only and platform-agnostic separators
<tr><td>🌐<td><strong>UTF-8 only, Unicode normalised</strong> path names and string data
<tr><td>👩‍🚀<td>Extensible with <strong>space-efficient attributes in key-value pairs</strong> for records and whole archives
<tr><td>↔️<td>Configurable optional <strong>byte-alignment of files</strong> to enable easy memory mapping
<tr><td>💽<td><strong>FST-based path index</strong> for fast O(m) lookups and efficient prefix queries
<tr><td>📁<td>Support for <strong>directories, files and links</strong>
<tr><td>🗜️<td><strong>Multiple compression methods</strong> within a single archive
<tr><td>🔒<td><strong>BLAKE3 checksums</strong> for data integrity verification
<tr><td>🖥️<td>A <strong>truly cross-platform command line tool</strong>
<tr><td>📜<td>Well-defined, <strong><a href="SPEC.md">open specification</a></strong> of file format
</table>

### Compression methods

- Stored (no compression)
- Brotli
- DEFLATE
- Snappy
- xz
- Zstandard

## Supported platforms

- Windows
- macOS
- Linux
- iOS
- Android

## License

Licensed under either of

* Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
