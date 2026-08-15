# Finite-state indexes

## Serialized BFST

> [spec:box:syn:fst-format.root]
> A serialized BFST is a 24-byte header, `node_count` eight-byte node-index entries, a variable hot section, and a variable cold section. Node 0 is the root. In archive metadata the BFST bytes are preceded by a little-endian `u64` byte length that excludes the length field itself; zero length represents no FST.

> [spec:box:def:fst-format.root.header-index]
> The BFST header is magic `BFST` at `0x00`, version `1` at `0x04`, zero flags at `0x05`, two zero reserved bytes, little-endian `u32 node_count` at `0x08`, little-endian `u64 entry_count` at `0x0C`, and little-endian `u32 cold_offset` at `0x14`. The hot section begins at `24 + node_count * 8`. Each node-index entry is a little-endian `u32` offset relative to the hot-section start followed by a little-endian `u32` offset relative to the cold-section start.

> [spec:box:def:fst-format.root.nodes]
> A node's hot data is flags, Vu64 edge count, lookup data, then one little-endian `u16` cold offset per edge. Flag bit 0 marks a final node and bit 1 selects indexed lookup. Nodes with at most 16 edges store their sorted first-label bytes; nodes with at least 17 edges store a 256-byte table whose entries are edge indices or `0xFF`. At each node-relative cold offset an edge is Vu64 label length, label bytes, Vu64-compatible output, and little-endian `u32` target node; a final node appends its final output after all edges.

> [spec:box:req:fst-format.root.validation]
> BFST opening MUST reject fewer than 24 bytes, magic other than `BFST`, and a version other than `1`; the archive envelope parser MUST reject a declared BFST length that exceeds remaining metadata. Builders MUST emit zero header flags and reserved bytes. The current constructor performs no eager whole-graph validation beyond that header, so callers MUST treat node/index offsets and edge bodies as trusted builder output before querying them.

> [spec:box:req:fst-format.root.build]
> Ordered builder insertion MUST require keys in strict bytewise lexicographic order, MUST return distinct errors for a duplicate and an out-of-order key, and finishing a builder with no keys MUST fail. Archive path and block-index construction MUST sort collected entries before ordered insertion. The replacement insertion API MAY accept unsorted keys and MUST replace an existing value without increasing entry count; serialized nodes and their outgoing edges MUST retain bytewise traversal order.

## Query behavior

> [spec:box:sem:fst-queries.root]
> Exact lookup starts at node 0, matches each compressed radix-edge label against the remaining key, and accumulates edge outputs with wrapping addition. It returns the accumulated value plus the final output only when all key bytes are consumed at a final node; a missing edge, partial label, extra suffix, or non-final endpoint yields no value.

> [spec:box:def:fst-queries.root.archive-indexes]
> The path FST maps complete U+001F-separated BoxPath byte strings to nonzero `u64` RecordIndex values. A block-FST key is exactly 16 bytes: the record index as big-endian `u64`, then the block's logical start as big-endian `u64`; its value is the physical archive offset of that block. Big-endian key fields make byte ordering agree with record-index order and then logical-offset order.

> [spec:box:req:fst-queries.root.prefix]
> Prefix iteration MUST yield every final key below the matching radix position, including when the requested prefix ends inside an edge label, and MUST yield none for a mismatching prefix. An empty prefix MUST enumerate all entries. Results MUST follow serialized edge order and include accumulated outputs. Callback traversal MUST stop and return false when its callback returns false, and MUST return true after exhaustive traversal or when the prefix is absent.

> [spec:box:req:fst-queries.root.predecessor]
> Block lookup for `(record_index, logical_offset)` MUST restrict iteration to the eight-byte big-endian record-index prefix, ignore keys whose total length is not 16, and select the last block start not greater than the requested logical offset. It MUST return no block when the record has no such entry. `find_block` exposes `(physical_offset, block_logical_offset)`; ordered block enumeration and `next_block` expose `(logical_offset, physical_offset)`, with `next_block` selecting the first logical offset strictly greater than its argument.
