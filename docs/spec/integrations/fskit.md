# macOS FSKit integration

> [spec:box:req:fskit-extension.root]
> On macOS, the FSKit library MUST expose the Objective-C-visible `BoxFS`,
> `BoxVolume`, and Box item mapping together with the C entrypoint
> `fskitbox_extension_main`. Registration MUST return no filesystem when the
> `FSUnaryFileSystem` runtime class is unavailable and otherwise MUST create a
> `BoxFS` instance whose class is registered for the app extension.

> [spec:box:req:fskit-extension.root.probe-load-lifecycle]
> Probing MUST recognize only URL-backed resources whose first four bytes are
> `\xFFBOX`, returning a usable result named from the backing filename. Loading
> MUST open that path through the filesystem's Tokio runtime, create and retain a
> volume, and move the container to `Ready`; failures MUST invoke the reply block
> with a `BoxFSErrorDomain` error. Unloading MUST release the retained volume,
> move the container to `NotReady`, and reply without an error.

> [spec:box:req:fskit-extension.root.read-only-volume]
> The volume MUST advertise one link, 255-byte names, restricted ownership
> changes, no name truncation, a 64-KiB xattr limit, and read-only operation.
> Mount, close, and synchronize MAY complete without mutation; unmount MUST clear
> cached content. Write-mode opens, writes, creation, and removal MUST reply with
> `EROFS`.

> [spec:box:req:fskit-extension.root.record-mapping-and-cache]
> FSKit item identifiers MUST reserve 0 as invalid and 1 as root and map archive
> record index `n` to `n + 2`. Metadata mapping MUST classify ordinary and
> chunked files, directories, and internal/external links; derive mode, owner,
> group, timestamps, and size with the implemented defaults; and support
> root/directory child lookup by name. Reads currently MUST accept only ordinary
> file records, cache their complete decompressed bytes by record index, return
> clamped slices with EOF state, and evict a record when its item is reclaimed.
