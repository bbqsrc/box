/// Node encoding/decoding - optimized for hot/cold data separation.
///
/// Format v1 with hot/cold sections:
/// ```text
/// [Header: 16 bytes]
/// [Node Index: node_count × 8 bytes]  <- (hot_off: u32, cold_off: u32)
/// [Hot Section]                        <- flags, edge_count, first_bytes, offsets
/// [Cold Section]                       <- edge labels, outputs, target_node_ids
/// [Footer: 16 bytes]
/// ```
use fastvlq::{WriteVlqExt, decode_vu64_slice};

/// Header constants
pub const MAGIC: &[u8; 4] = b"BFST";
pub const VERSION: u8 = 1;
pub const HEADER_SIZE: usize = 16;
pub const FOOTER_SIZE: usize = 16;
pub const INDEX_ENTRY_SIZE: usize = 8;

/// Node type thresholds (ART-style adaptive)
pub const INDEXED_THRESHOLD: usize = 17; // Use 256-byte index for 17+ edges

/// Flag bits
pub const FLAG_IS_FINAL: u8 = 0x01;
pub const FLAG_INDEXED: u8 = 0x02; // Large node with 256-byte index

/// Write the FST header.
pub fn write_header(entry_count: u64, buf: &mut Vec<u8>) {
    buf.extend_from_slice(MAGIC);
    buf.push(VERSION);
    buf.push(0); // flags
    buf.push(0); // reserved
    buf.push(0); // reserved
    buf.extend_from_slice(&entry_count.to_le_bytes());
}

/// Footer data parsed from FST.
#[derive(Debug, Clone, Copy)]
pub struct Footer {
    pub root_node_id: u32,
    pub node_count: u32,
    pub hot_start: u32,
    pub cold_start: u32,
}

/// Write the FST footer.
pub fn write_footer(footer: &Footer, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&footer.root_node_id.to_le_bytes());
    buf.extend_from_slice(&footer.node_count.to_le_bytes());
    buf.extend_from_slice(&footer.hot_start.to_le_bytes());
    buf.extend_from_slice(&footer.cold_start.to_le_bytes());
}

/// Parse the FST header, returns entry count.
pub fn read_header(data: &[u8]) -> Result<u64, crate::FstError> {
    use crate::FstError;

    if data.len() < HEADER_SIZE {
        return Err(FstError::TooShort);
    }
    if &data[0..4] != MAGIC {
        return Err(FstError::InvalidMagic);
    }
    if data[4] != VERSION {
        return Err(FstError::UnsupportedVersion(data[4]));
    }

    let entry_count = u64::from_le_bytes(data[8..16].try_into().unwrap());
    Ok(entry_count)
}

/// Parse the FST footer.
pub fn read_footer(data: &[u8]) -> Option<Footer> {
    if data.len() < FOOTER_SIZE {
        return None;
    }
    let footer_start = data.len() - FOOTER_SIZE;
    let f = &data[footer_start..];

    Some(Footer {
        root_node_id: u32::from_le_bytes([f[0], f[1], f[2], f[3]]),
        node_count: u32::from_le_bytes([f[4], f[5], f[6], f[7]]),
        hot_start: u32::from_le_bytes([f[8], f[9], f[10], f[11]]),
        cold_start: u32::from_le_bytes([f[12], f[13], f[14], f[15]]),
    })
}

/// Read a VLQ u64 directly from slice.
#[inline(always)]
fn read_vlq(data: &[u8], pos: &mut usize) -> Option<u64> {
    let (value, len) = decode_vu64_slice(data.get(*pos..)?)?;
    *pos += len;
    Some(value)
}

/// Write a VLQ u64 to a buffer.
fn write_vlq(buf: &mut Vec<u8>, value: u64) -> std::io::Result<()> {
    buf.write_vu64(value)
}

/// Index entry for a node.
#[derive(Debug, Clone, Copy)]
pub struct NodeIndex {
    pub hot_offset: u32,
    pub cold_offset: u32,
}

impl NodeIndex {
    #[inline]
    pub fn from_bytes(data: &[u8]) -> Self {
        Self {
            hot_offset: u32::from_le_bytes([data[0], data[1], data[2], data[3]]),
            cold_offset: u32::from_le_bytes([data[4], data[5], data[6], data[7]]),
        }
    }

    pub fn write(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.hot_offset.to_le_bytes());
        buf.extend_from_slice(&self.cold_offset.to_le_bytes());
    }
}

/// Serialized edge in cold section.
#[derive(Debug, Clone)]
pub struct EdgeRef<'a> {
    pub label: &'a [u8],
    pub output: u64,
    pub target_node_id: u32,
}

/// Node format type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeFormat {
    /// Compact format: linear first_bytes array with binary search / SIMD
    Compact,
    /// Indexed format: 256-byte index for O(1) lookup
    Indexed,
}

/// Node reference with hot/cold data separation.
#[derive(Debug)]
pub struct NodeRef<'a> {
    pub is_final: bool,
    pub final_output: u64,
    pub(crate) format: NodeFormat,
    edge_count: usize,
    /// For Compact: first_bytes array; For Indexed: 256-byte index table
    index_data: &'a [u8],
    offsets: &'a [u8],
    cold_data: &'a [u8],
}

impl<'a> NodeRef<'a> {
    /// Parse a node from hot and cold section data.
    /// hot_data: slice starting at this node's hot section offset
    /// cold_data: slice starting at this node's cold section offset
    pub fn from_sections(hot_data: &'a [u8], cold_data: &'a [u8]) -> Option<Self> {
        if hot_data.is_empty() {
            return None;
        }

        let flags = hot_data[0];
        let is_final = (flags & FLAG_IS_FINAL) != 0;
        let is_indexed = (flags & FLAG_INDEXED) != 0;
        let mut pos = 1;

        let edge_count = read_vlq(hot_data, &mut pos)? as usize;

        let (format, index_data, offsets) = if is_indexed {
            // Indexed format: 256-byte index table + offsets
            let index_table = hot_data.get(pos..pos + 256)?;
            pos += 256;
            let offsets_len = edge_count * 2;
            let offsets = hot_data.get(pos..pos + offsets_len)?;
            (NodeFormat::Indexed, index_table, offsets)
        } else {
            // Compact format: first_bytes array + offsets
            let first_bytes = hot_data.get(pos..pos + edge_count)?;
            pos += edge_count;
            let offsets_len = edge_count * 2;
            let offsets = hot_data.get(pos..pos + offsets_len)?;
            (NodeFormat::Compact, first_bytes, offsets)
        };

        // Parse final_output from end of cold data
        let final_output = if is_final {
            Self::parse_final_output(cold_data, edge_count)?
        } else {
            0
        };

        Some(NodeRef {
            is_final,
            final_output,
            format,
            edge_count,
            index_data,
            offsets,
            cold_data,
        })
    }

    /// Parse final_output from cold data (it's after all edge data).
    fn parse_final_output(cold_data: &[u8], edge_count: usize) -> Option<u64> {
        let mut pos = 0;

        // Skip all edges
        for _ in 0..edge_count {
            let label_len = read_vlq(cold_data, &mut pos)? as usize;
            pos += label_len;
            read_vlq(cold_data, &mut pos)?; // output
            pos += 4; // target_node_id
        }

        // Read final_output
        read_vlq(cold_data, &mut pos)
    }

    /// Number of edges.
    #[inline]
    pub fn edge_count(&self) -> usize {
        self.edge_count
    }

    /// Get edge by index. O(1) via offset table.
    #[inline]
    pub fn edge(&self, index: usize) -> Option<EdgeRef<'a>> {
        if index >= self.edge_count {
            return None;
        }

        // Read u16 LE offset
        let off_idx = index * 2;
        let offset =
            u16::from_le_bytes([self.offsets[off_idx], self.offsets[off_idx + 1]]) as usize;

        let mut pos = offset;

        let label_len = read_vlq(self.cold_data, &mut pos)? as usize;
        let label = self.cold_data.get(pos..pos + label_len)?;
        pos += label_len;
        let output = read_vlq(self.cold_data, &mut pos)?;
        let target_node_id = u32::from_le_bytes([
            self.cold_data[pos],
            self.cold_data[pos + 1],
            self.cold_data[pos + 2],
            self.cold_data[pos + 3],
        ]);

        Some(EdgeRef {
            label,
            output,
            target_node_id,
        })
    }

    /// Find edge whose label starts with the given byte.
    /// Uses O(1) index lookup for large nodes, SIMD/binary search for small nodes.
    #[inline]
    pub fn find_edge(&self, target: u8) -> Option<EdgeRef<'a>> {
        if self.edge_count == 0 {
            return None;
        }

        match self.format {
            NodeFormat::Indexed => {
                // O(1) lookup via 256-byte index table
                let edge_idx = self.index_data[target as usize];
                if edge_idx == 0xFF {
                    None
                } else {
                    self.edge(edge_idx as usize)
                }
            }
            NodeFormat::Compact => {
                #[cfg(any(
                    all(target_arch = "x86_64", target_feature = "sse2"),
                    all(target_arch = "aarch64", target_feature = "neon")
                ))]
                if self.edge_count >= 16 {
                    return self.find_edge_simd(target);
                }

                // Binary search on first_bytes
                match self.index_data.binary_search(&target) {
                    Ok(idx) => self.edge(idx),
                    Err(_) => None,
                }
            }
        }
    }

    /// SIMD edge search for x86_64 with SSE2.
    #[cfg(all(target_arch = "x86_64", target_feature = "sse2"))]
    #[inline]
    fn find_edge_simd(&self, target: u8) -> Option<EdgeRef<'a>> {
        use std::arch::x86_64::*;

        unsafe {
            let needle = _mm_set1_epi8(target as i8);
            let mut i = 0;

            // Process 16 bytes at a time
            while i + 16 <= self.edge_count {
                let chunk = _mm_loadu_si128(self.index_data.as_ptr().add(i) as *const __m128i);
                let cmp = _mm_cmpeq_epi8(chunk, needle);
                let mask = _mm_movemask_epi8(cmp) as u32;

                if mask != 0 {
                    let idx = i + mask.trailing_zeros() as usize;
                    return self.edge(idx);
                }
                i += 16;
            }

            // Handle remainder with binary search
            if i < self.edge_count {
                let remainder = &self.index_data[i..self.edge_count];
                if let Ok(rel_idx) = remainder.binary_search(&target) {
                    return self.edge(i + rel_idx);
                }
            }

            None
        }
    }

    /// SIMD edge search for aarch64 with NEON.
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    #[inline]
    fn find_edge_simd(&self, target: u8) -> Option<EdgeRef<'a>> {
        use std::arch::aarch64::*;

        unsafe {
            let needle = vdupq_n_u8(target);
            let mut i = 0;

            // Process 16 bytes at a time
            while i + 16 <= self.edge_count {
                let chunk = vld1q_u8(self.index_data.as_ptr().add(i));
                let cmp = vceqq_u8(chunk, needle);

                // Check if any match - reduce to 64-bit halves
                let low = vgetq_lane_u64(vreinterpretq_u64_u8(cmp), 0);
                let high = vgetq_lane_u64(vreinterpretq_u64_u8(cmp), 1);

                if low != 0 {
                    let idx = i + (low.trailing_zeros() / 8) as usize;
                    return self.edge(idx);
                }
                if high != 0 {
                    let idx = i + 8 + (high.trailing_zeros() / 8) as usize;
                    return self.edge(idx);
                }
                i += 16;
            }

            // Handle remainder with binary search
            if i < self.edge_count {
                let remainder = &self.index_data[i..self.edge_count];
                if let Ok(rel_idx) = remainder.binary_search(&target) {
                    return self.edge(i + rel_idx);
                }
            }

            None
        }
    }

    /// Iterate all edges.
    #[allow(dead_code)]
    pub fn edges(&self) -> impl Iterator<Item = EdgeRef<'a>> + '_ {
        (0..self.edge_count).filter_map(|i| self.edge(i))
    }
}

/// Intermediate node data for building.
#[derive(Debug)]
pub struct NodeData {
    pub is_final: bool,
    pub final_output: u64,
    pub edges: Vec<EdgeData>,
}

/// Intermediate edge data for building.
#[derive(Debug)]
pub struct EdgeData {
    pub label: Vec<u8>,
    pub output: u64,
    pub target_node_id: u32,
}

/// Write a node's hot section data, returns bytes written.
pub fn write_node_hot(node: &NodeData, buf: &mut Vec<u8>) -> std::io::Result<usize> {
    let start = buf.len();
    let edge_count = node.edges.len();
    let use_indexed = edge_count >= INDEXED_THRESHOLD;

    // Flags
    let mut flags = 0u8;
    if node.is_final {
        flags |= FLAG_IS_FINAL;
    }
    if use_indexed {
        flags |= FLAG_INDEXED;
    }
    buf.push(flags);

    // Edge count
    write_vlq(buf, edge_count as u64)?;

    if use_indexed {
        // Indexed format: 256-byte index table
        // index[byte] = edge_idx, or 0xFF if no edge for that byte
        let mut index_table = [0xFFu8; 256];
        for (i, edge) in node.edges.iter().enumerate() {
            let first_byte = edge.label.first().copied().unwrap_or(0);
            index_table[first_byte as usize] = i as u8;
        }
        buf.extend_from_slice(&index_table);
    } else {
        // Compact format: first_bytes array
        for edge in &node.edges {
            buf.push(edge.label.first().copied().unwrap_or(0));
        }
    }

    // Offsets array placeholder (will be filled by cold writer)
    for _ in 0..edge_count {
        buf.extend_from_slice(&0u16.to_le_bytes());
    }

    Ok(buf.len() - start)
}

/// Write a node's cold section data and update hot section offsets.
/// Returns bytes written to cold section.
pub fn write_node_cold(
    node: &NodeData,
    hot_buf: &mut [u8],
    hot_offsets_start: usize,
    cold_buf: &mut Vec<u8>,
) -> std::io::Result<usize> {
    let cold_start = cold_buf.len();

    // Write edge data, recording offsets
    for (i, edge) in node.edges.iter().enumerate() {
        let offset = (cold_buf.len() - cold_start) as u16;
        let off_bytes = offset.to_le_bytes();
        hot_buf[hot_offsets_start + i * 2] = off_bytes[0];
        hot_buf[hot_offsets_start + i * 2 + 1] = off_bytes[1];

        write_vlq(cold_buf, edge.label.len() as u64)?;
        cold_buf.extend_from_slice(&edge.label);
        write_vlq(cold_buf, edge.output)?;
        cold_buf.extend_from_slice(&edge.target_node_id.to_le_bytes());
    }

    // Final output
    if node.is_final {
        write_vlq(cold_buf, node.final_output)?;
    }

    Ok(cold_buf.len() - cold_start)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_roundtrip() {
        let node = NodeData {
            is_final: true,
            final_output: 42,
            edges: vec![
                EdgeData {
                    label: b"bar".to_vec(),
                    output: 100,
                    target_node_id: 0,
                },
                EdgeData {
                    label: b"foo".to_vec(),
                    output: 200,
                    target_node_id: 1,
                },
            ],
        };

        let mut hot_buf = Vec::new();
        write_node_hot(&node, &mut hot_buf).unwrap();

        // Calculate offsets start: flags(1) + vlq(1) + first_bytes(2) = 4
        let offsets_start = 1 + 1 + node.edges.len();

        let mut cold_buf = Vec::new();
        write_node_cold(&node, &mut hot_buf, offsets_start, &mut cold_buf).unwrap();

        let parsed = NodeRef::from_sections(&hot_buf, &cold_buf).unwrap();
        assert!(parsed.is_final);
        assert_eq!(parsed.final_output, 42);
        assert_eq!(parsed.edge_count(), 2);

        let edge0 = parsed.edge(0).unwrap();
        assert_eq!(edge0.label, b"bar");
        assert_eq!(edge0.output, 100);

        let edge1 = parsed.edge(1).unwrap();
        assert_eq!(edge1.label, b"foo");
        assert_eq!(edge1.output, 200);
    }

    #[test]
    fn test_find_edge() {
        let node = NodeData {
            is_final: false,
            final_output: 0,
            edges: vec![
                EdgeData {
                    label: b"apple".to_vec(),
                    output: 1,
                    target_node_id: 10,
                },
                EdgeData {
                    label: b"banana".to_vec(),
                    output: 2,
                    target_node_id: 20,
                },
                EdgeData {
                    label: b"cherry".to_vec(),
                    output: 3,
                    target_node_id: 30,
                },
            ],
        };

        let mut hot_buf = Vec::new();
        write_node_hot(&node, &mut hot_buf).unwrap();
        let offsets_start = 1 + 1 + node.edges.len();

        let mut cold_buf = Vec::new();
        write_node_cold(&node, &mut hot_buf, offsets_start, &mut cold_buf).unwrap();

        let parsed = NodeRef::from_sections(&hot_buf, &cold_buf).unwrap();

        let e = parsed.find_edge(b'a').unwrap();
        assert_eq!(e.label, b"apple");
        assert_eq!(e.output, 1);

        let e = parsed.find_edge(b'b').unwrap();
        assert_eq!(e.label, b"banana");

        let e = parsed.find_edge(b'c').unwrap();
        assert_eq!(e.label, b"cherry");

        assert!(parsed.find_edge(b'd').is_none());
        assert!(parsed.find_edge(b'A').is_none());
    }

    #[test]
    fn test_indexed_node() {
        // Create a node with >= INDEXED_THRESHOLD edges to trigger indexed format
        let mut edges = Vec::new();
        for i in 0..20u8 {
            edges.push(EdgeData {
                label: vec![b'a' + i],
                output: i as u64,
                target_node_id: i as u32,
            });
        }

        let node = NodeData {
            is_final: true,
            final_output: 999,
            edges,
        };

        let mut hot_buf = Vec::new();
        write_node_hot(&node, &mut hot_buf).unwrap();

        // For indexed: flags(1) + vlq(1) + index_table(256)
        let offsets_start = 1 + 1 + 256;

        let mut cold_buf = Vec::new();
        write_node_cold(&node, &mut hot_buf, offsets_start, &mut cold_buf).unwrap();

        let parsed = NodeRef::from_sections(&hot_buf, &cold_buf).unwrap();
        assert!(parsed.is_final);
        assert_eq!(parsed.final_output, 999);
        assert_eq!(parsed.edge_count(), 20);
        assert_eq!(parsed.format, NodeFormat::Indexed);

        // Test O(1) lookup
        let e = parsed.find_edge(b'a').unwrap();
        assert_eq!(e.label, b"a");
        assert_eq!(e.output, 0);

        let e = parsed.find_edge(b't').unwrap();
        assert_eq!(e.label, b"t");
        assert_eq!(e.output, 19);

        // Non-existent edge
        assert!(parsed.find_edge(b'z').is_none());
        assert!(parsed.find_edge(b'A').is_none());
    }

    #[test]
    fn test_header_roundtrip() {
        let mut buf = Vec::new();
        write_header(12345, &mut buf);
        assert_eq!(buf.len(), HEADER_SIZE);

        let count = read_header(&buf).unwrap();
        assert_eq!(count, 12345);
    }

    #[test]
    fn test_footer_roundtrip() {
        let footer = Footer {
            root_node_id: 42,
            node_count: 100,
            hot_start: 816,
            cold_start: 2000,
        };

        let mut buf = Vec::new();
        write_footer(&footer, &mut buf);
        assert_eq!(buf.len(), FOOTER_SIZE);

        let parsed = read_footer(&buf).unwrap();
        assert_eq!(parsed.root_node_id, 42);
        assert_eq!(parsed.node_count, 100);
        assert_eq!(parsed.hot_start, 816);
        assert_eq!(parsed.cold_start, 2000);
    }

    #[test]
    fn test_empty_node() {
        let node = NodeData {
            is_final: true,
            final_output: 999,
            edges: vec![],
        };

        let mut hot_buf = Vec::new();
        write_node_hot(&node, &mut hot_buf).unwrap();

        let mut cold_buf = Vec::new();
        write_node_cold(&node, &mut hot_buf, 2, &mut cold_buf).unwrap();

        let parsed = NodeRef::from_sections(&hot_buf, &cold_buf).unwrap();
        assert!(parsed.is_final);
        assert_eq!(parsed.final_output, 999);
        assert_eq!(parsed.edge_count(), 0);
        assert!(parsed.find_edge(b'x').is_none());
    }
}
