# Compression codecs and streams

> [spec:box:req:compression.root]
> Compression dispatch MUST recognize Stored `0x00`, Zstd `0x10`, and XZ `0x20` in a record's high nibble. Stored MUST copy bytes unchanged; Zstd and XZ MUST use their corresponding frame decoders when those features are compiled. An unknown or unavailable identifier MUST remain representable in metadata and MUST produce an `InvalidInput` error when compression or decompression is attempted, rather than silently treating the bytes as Stored.

> [spec:box:def:compression.root.codecs]
> CompressionConfig selects a codec, string-valued options, and optional Zstd dictionary bytes. Zstd's absent `level` option uses the library default (currently level 3). XZ's absent `level` uses preset 6, and XZ frames are emitted with CRC64. The standard random-access block size is 2,097,152 bytes, while codec selection for a complete file changes to Stored when its size is less than 96 bytes.

> [spec:box:req:compression.root.stored]
> A Stored writer MUST write the input byte-for-byte and record equal compressed and decompressed byte counts. Buffer decompression of Stored data MUST return the same bytes without consulting a dictionary. Automatic size selection MUST discard compression options and dictionary state when it changes a file smaller than 96 bytes to Stored.

> [spec:box:req:compression.root.stream-state]
> A streaming codec call MUST report the exact input bytes consumed and output bytes produced as either Progress or Done. Callers MUST advance both buffers by those counts, MUST call compressor finish repeatedly until Done, and MUST propagate codec errors. Zstd decompression reaches Done only when its frame is complete and the supplied input slice has been consumed; XZ reaches Done on `StreamEnd`. Resetting a Zstd compressor MUST retain its loaded dictionary for the next session, while resetting a Zstd decompressor reinitializes a dictionary-free session.
