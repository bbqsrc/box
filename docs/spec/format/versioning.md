# Versioned metadata

> [spec:box:sem:versioning.root]
> The header version byte selects metadata decoding. Version `0` uses the legacy decoder; every nonzero byte currently uses the version 1 decoder. Thus an otherwise parseable archive with an unknown nonzero version is interpreted using the v1 layout rather than rejected solely for its version byte. New writers identify their output as version `1`.

> [spec:box:def:versioning.root.v0]
> Version 0 metadata is a root RecordIndex vector, then record count and records, then attribute-key names and the archive AttrMap. Its record tag is a whole byte: File `0`, Directory `1`, Link `2`, or ExternalLink `3`; a File carries a separate compression byte, and a Directory serializes its child RecordIndex vector. Attribute keys have no type byte and are exposed as JSON-typed. Version 0 has no ChunkedFile, compression dictionary, path FST, or block FST.

> [spec:box:req:versioning.root.v1]
> A version 1 writer MUST serialize, in order, typed attribute keys, archive AttrMap, optional dictionary, record count and combined-tag records, path-FST envelope, and block-FST envelope. It MUST omit root and directory-child vectors because paths encode the hierarchy. A version 1 reader MUST also accept end of file immediately after the path-FST envelope as an absent block index. Both version decoders MUST apply checked host-size conversion, remaining-byte count bounds, overflow checks, contextual truncation errors, and the two-form AttrMap byte-count validation of the envelope rule.
