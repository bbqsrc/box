# C archive API

> [spec:box:req:c-api.root]
> The C-facing library MUST export `box_file_reader_open`, which accepts a
> marshalled path and returns an owned `BoxFileReader` handle, and
> `box_file_reader_extract_all`, which borrows such a handle and extracts every
> member to a marshalled destination path. These exports MUST use the declared
> CFFI return and argument marshallers so their generated ABI agrees on handle,
> path, unit, and error representation.

> [spec:box:req:c-api.root.runtime-and-ownership]
> Both C operations MUST execute async archive work by blocking on one
> process-wide Tokio runtime initialized on first use. The open operation MUST
> transfer its boxed reader through the owning handle marshaler; extraction MUST
> borrow rather than consume that reader, and archive or extraction failures
> MUST be returned through the CFFI `Result` error channel.
