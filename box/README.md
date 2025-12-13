# Box - Open Standard Archive Format

<table>
<tr><td>⚠️<td>Box is under active development. Do not expect compatibility between versions until v1.0.
</table>

[![Actions Status](https://github.com/bbqsrc/box/workflows/Continuous%20Integration/badge.svg)](https://github.com/bbqsrc/box/actions)

* [`box` command line application](https://github.com/bbqsrc/box/tree/main/box)
* [`box-format` Rust crate and open specification](https://github.com/bbqsrc/box/tree/main/box-format)
* [`fusebox` demonstration FUSE driver](https://github.com/bbqsrc/box/tree/main/fusebox)

The `.box` file format and related tooling is designed to be a modern successor to formats such as 
`.zip` and `.7z`, removing several painpoints and introducing modern features and expectations:

## Features

<table>
<tr><td>🌉<td><strong>Cross-platform path support</strong>, with relative paths only and platform-agnostic separators
<tr><td>🌐<td><strong>UTF-8 only, Unicode normalised</strong> path names and string data
<tr><td>🔍<td>FST-based indexing for <strong>extremely fast path lookups</strong>
<tr><td>👩‍🚀<td>Extensible with <strong>space-efficient attributes in key-value pairs</strong> for records and whole archives
<tr><td>↔️<td>Configurable optional <strong>byte-alignment of files</strong> to enable easy memory mapping
<tr><td>💽<td><strong>Index-based metadata</strong> for tree-based structuring, mapping closely to how filesystems work
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

## Screenshots

### Unix

<img width="809" src="https://user-images.githubusercontent.com/279099/92532977-d17a6480-f231-11ea-8d8c-637918d2c6cc.png">

### Windows

<img width="815" src="https://user-images.githubusercontent.com/279099/92532802-6466cf00-f231-11ea-9fc1-4e9342b37dd3.png">

## Supported platforms

* Windows
* macOS
* Linux

## License

Licensed under

* European Union Public License, version 1.2, ([LICENSE](LICENSE) or https://joinup.ec.europa.eu/collection/eupl/eupl-text-11-12)

The EUPL is a **copyleft, GPL-compatible license** managed by the European Union, translated into **multiple languages**. See [this introduction](https://joinup.ec.europa.eu/collection/eupl/introduction-eupl-licence) for information about the purpose, objectives and translations of the license. See also the [license compatibility matrix](https://joinup.ec.europa.eu/collection/eupl/matrix-eupl-compatible-open-source-licences).