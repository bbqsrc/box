# Box - Open Standard Archive Format

[![Actions Status](https://github.com/bbqsrc/box/workflows/Continuous%20Integration/badge.svg)](https://github.com/bbqsrc/box/actions)

<table>
<tr><td>⚠️<td>Box is under active development. Do not expect compatibility between versions until v1.0.
</table>

The `.box` file format and related tooling is designed to be a modern successor to formats such as 
`.zip` and `.7z`, removing several painpoints and introducing modern features and expectations:

## Features

<table>
<tr><td>🌉<td><strong>Cross-platform path support</strong>, with relative paths only and platform-agnostic separators
<tr><td>🌐<td><strong>UTF-8 only, Unicode normalised</strong> path names and string data
<tr><td>👩‍🚀<td>Extensible with <strong>space-efficient attributes in key-value pairs</strong> for records and whole archives
<tr><td>↔️<td>Configurable optional <strong>byte-alignment of files</strong> to enable easy memory mapping
<tr><td>💽<td><strong>Index-based metadata</strong> for tree-based structuring, mapping closely to how filesystems work
<tr><td>📁<td>Support for <strong>directories, files and links</strong>
<tr><td>🗜️<td><strong>Multiple compression methods</strong> within a single archive
<tr><td>🔒<td><strong>BLAKE3 checksums</strong> for data integrity verification
<tr><td>🖥️<td>A <strong>truly cross-platform command line tool</strong>
<tr><td>📜<td>Well-defined, <strong><a href="SPEC.md">open specification</a></strong> of file format
</table>

### Compression methods

Currently supported compression methods:

<ul>
<li> Stored (no compression)
<li> Brotli
<li> DEFLATE
<li> Snappy
<li> xz
<li> Zstandard
</ul>

## Supported platforms

* Windows
* macOS
* Linux
* iOS
* Android

---

See the `fusebox` repo for an example of the `.box` file format being used with a FUSE driver, also
written in Rust. :smile:

## License

Licensed under either of

* Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
