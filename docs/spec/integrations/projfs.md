# Windows ProjFS integration

> [spec:box:req:projfs-provider.root]
> On Windows, `BoxProvider` MUST coordinate one opened archive, a Tokio runtime,
> a decompressed-file cache, directory-enumeration sessions, the virtualization
> root, and the active namespace context. Starting the provider MUST register
> callbacks for enumeration start/get/end, placeholder information, file data,
> and name queries, and callback-visible mutable state MUST remain protected by
> the provider's locks.

> [spec:box:req:projfs-provider.root.lifecycle]
> Provider startup MUST mark the root as a placeholder directory, tolerate the
> already-marked result, give ProjFS a stable provider context for the callback
> lifetime, and store the returned virtualization context. `stop` and provider
> destruction MUST take that context at most once and stop virtualization when
> it is present.

> [spec:box:req:projfs-provider.root.enumeration]
> Enumeration start MUST resolve either the root or an archive directory and
> create a GUID-keyed session. The get callback MUST populate root or directory
> children, apply case-insensitive `*` and `?` matching, sort names
> case-insensitively, resume at the saved cursor after a full ProjFS buffer, and
> reset and repopulate on the restart flag. Enumeration end MUST remove the
> session; missing sessions and non-directory paths MUST yield file-not-found
> HRESULTs.

> [spec:box:req:projfs-provider.root.placeholders]
> Placeholder conversion MUST expose ordinary and chunked files with their
> decompressed length and read-only/archive attributes, a directory with
> directory attributes and zero size, and an internal link as a zero-length
> read-only/archive item. On Windows 10 version 2004 or later, an external link
> MUST use ProjFS extended symlink information with its stored target normalized
> to Windows separators for both enumeration and placeholder creation. Box
> creation and modification times MUST convert to Windows FILETIME, with the
> current time as fallback and last-write time not earlier than creation time.

> [spec:box:req:projfs-provider.root.file-data-and-errors]
> File-data callbacks MUST resolve the Windows relative path through `BoxPath`,
> accept ordinary and chunked file records, cache the whole reconstructed logical
> file by record index, copy only the requested in-range bytes through a ProjFS
> aligned buffer, and free that buffer after `PrjWriteFileData`. Chunked files
> MUST be reconstructed from their indexed blocks before entering that shared
> cache. Root/name queries MUST report existence without hydration, and path,
> allocation, I/O, and Windows API failures MUST be returned as the corresponding
> HRESULT rather than crossing the callback boundary as Rust errors.
