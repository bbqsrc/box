<table>
<tr><td>⚠️<td>Box is under active development. Do not expect compatibility between versions until v1.0.
</table>

# Box - Open Standard Archive Format

[![Actions Status](https://github.com/bbqsrc/box/workflows/Continuous%20Integration/badge.svg)](https://github.com/bbqsrc/box/actions)

* [`box` command line application](box)
* [`box-format` Rust crate and open specification](box-format)
* [`fusebox` demonstration FUSE driver](fusebox)

The `.box` file format and related tooling is designed to be a modern successor to formats such as 
`.zip` and `.7z`, removing several painpoints and introducing modern features and expectations:

## Features

<table>
<tr><td>🌉<td><strong>Cross-platform path support</strong>, with relative paths only and platform-agnostic separators
<tr><td>🌐<td><strong>UTF-8 only, Unicode normalised</strong> path names and string data
<tr><td>🔍<td>FST-based indexing for <strong>extremely fast path lookups</strong>
<tr><td>👩‍🚀<td>Extensible with <strong>space-efficient attributes in key-value pairs</strong> for records and whole archives
<tr><td>↔️<td>Configurable optional <strong>byte-alignment of files</strong> to enable easy memory mapping
<tr><td>💽<td><strong>Inode-based metadata</strong> for tree-based structuring, mapping closely to how filesystems work
<tr><td>📁<td>Support for <strong>directories, files and links</strong>
<tr><td>🗜️<td><strong>Multiple compression methods</strong> within a single archive
<tr><td>🖥️<td>A <strong>truly cross-platform command line tool</strong>
<tr><td>📜<td>Well-defined, <strong>open specification</strong> of file format (due before v1.0)
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

## License

### box-format

The `box-format` crate is licensed under either of

 * Apache License, Version 2.0, ([box-format/LICENSE-APACHE](box-format/LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([box-format/LICENSE-MIT](box-format/LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### box

The `box` crate is licensed under

* European Union Public License, version 1.2, ([box/LICENSE](box/LICENSE) or https://joinup.ec.europa.eu/collection/eupl/eupl-text-11-12)

The EUPL is a **copyleft, GPL-compatible license** managed by the European Union, translated into **multiple languages**. See [this introduction](https://joinup.ec.europa.eu/collection/eupl/introduction-eupl-licence) for information about the purpose, objectives and translations of the license. See also the [license compatibility matrix](https://joinup.ec.europa.eu/collection/eupl/matrix-eupl-compatible-open-source-licences).