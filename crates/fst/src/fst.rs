use crate::error::FstError;
use crate::node::{HEADER_SIZE, Header, INDEX_ENTRY_SIZE, NodeIndex, NodeRef, read_header};

/// Prefetch memory for read.
#[inline(always)]
fn prefetch_read(ptr: *const u8) {
    #[cfg(target_arch = "aarch64")]
    unsafe {
        std::arch::asm!("prfm pldl1keep, [{ptr}]", ptr = in(reg) ptr, options(nostack, preserves_flags));
    }
    #[cfg(all(target_arch = "x86_64", target_feature = "sse"))]
    unsafe {
        std::arch::x86_64::_mm_prefetch(ptr as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
}

/// Fast memcmp using SIMD for longer slices.
#[inline(always)]
fn fast_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let len = a.len();

    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    if len >= 16 {
        return fast_eq_simd_neon(a, b);
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "sse2"))]
    if len >= 16 {
        return fast_eq_simd_sse2(a, b);
    }

    a == b
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
#[inline]
fn fast_eq_simd_neon(a: &[u8], b: &[u8]) -> bool {
    use std::arch::aarch64::*;

    unsafe {
        let mut i = 0;
        let len = a.len();

        // Compare 16 bytes at a time
        while i + 16 <= len {
            let va = vld1q_u8(a.as_ptr().add(i));
            let vb = vld1q_u8(b.as_ptr().add(i));
            let cmp = vceqq_u8(va, vb);
            let min = vminvq_u8(cmp);
            if min != 0xFF {
                return false;
            }
            i += 16;
        }

        // Compare remainder
        a[i..] == b[i..]
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "sse2"))]
#[inline]
fn fast_eq_simd_sse2(a: &[u8], b: &[u8]) -> bool {
    use std::arch::x86_64::*;

    unsafe {
        let mut i = 0;
        let len = a.len();

        // Compare 16 bytes at a time
        while i + 16 <= len {
            let va = _mm_loadu_si128(a.as_ptr().add(i) as *const __m128i);
            let vb = _mm_loadu_si128(b.as_ptr().add(i) as *const __m128i);
            let cmp = _mm_cmpeq_epi8(va, vb);
            let mask = _mm_movemask_epi8(cmp);
            if mask != 0xFFFF {
                return false;
            }
            i += 16;
        }

        // Compare remainder
        a[i..] == b[i..]
    }
}

/// Internal state for prefix traversal.
#[derive(Clone)]
struct EachState {
    node_id: u32,
    edge_idx: usize,
    key_len: usize,
    output_sum: u64,
    emitted_final: bool,
}

/// An immutable FST that can be queried.
///
/// Generic over `D: AsRef<[u8]>` to support zero-copy from memory-mapped data.
pub struct Fst<D> {
    data: D,
    header: Header,
}

impl<D: AsRef<[u8]>> Fst<D> {
    /// Open an FST from bytes.
    pub fn new(data: D) -> Result<Self, FstError> {
        let bytes = data.as_ref();

        if bytes.len() < HEADER_SIZE {
            return Err(FstError::TooShort);
        }

        let header = read_header(bytes)?;

        Ok(Self { data, header })
    }

    /// Get node index entry by node ID.
    #[inline]
    fn get_node_index(&self, node_id: u32) -> NodeIndex {
        let bytes = self.data.as_ref();
        let idx_offset = HEADER_SIZE + (node_id as usize) * INDEX_ENTRY_SIZE;
        NodeIndex::from_bytes(&bytes[idx_offset..])
    }

    /// Get node by ID.
    #[inline]
    fn get_node(&self, node_id: u32) -> Option<NodeRef<'_>> {
        let bytes = self.data.as_ref();
        let idx = self.get_node_index(node_id);

        let hot_start = self.header.hot_offset() + idx.hot_offset as usize;
        let cold_start = self.header.cold_offset as usize + idx.cold_offset as usize;

        let hot_data = &bytes[hot_start..];
        let cold_data = &bytes[cold_start..];

        NodeRef::from_sections(hot_data, cold_data)
    }

    /// Get the value for an exact key match.
    pub fn get(&self, key: &[u8]) -> Option<u64> {
        let bytes = self.data.as_ref();
        let mut current_node_id = 0u32; // Root is always node 0
        let mut remaining = key;
        let mut output_sum: u64 = 0;

        while !remaining.is_empty() {
            let node = self.get_node(current_node_id)?;

            let first_byte = remaining[0];
            let edge = node.find_edge(first_byte)?;

            // Prefetch next node while we compare the label
            let next_idx = self.get_node_index(edge.target_node_id);
            prefetch_read(
                bytes
                    .as_ptr()
                    .wrapping_add(self.header.hot_offset() + next_idx.hot_offset as usize),
            );

            // Check if edge label matches
            if remaining.len() < edge.label.len() {
                return None;
            }
            if !fast_eq(&remaining[..edge.label.len()], edge.label) {
                return None;
            }

            output_sum = output_sum.wrapping_add(edge.output);
            remaining = &remaining[edge.label.len()..];
            current_node_id = edge.target_node_id;
        }

        // Check if we ended at a final node
        let node = self.get_node(current_node_id)?;

        if node.is_final {
            Some(output_sum.wrapping_add(node.final_output))
        } else {
            None
        }
    }

    /// Check if a key exists.
    pub fn contains(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Number of entries in the FST.
    pub fn len(&self) -> u64 {
        self.header.entry_count
    }

    /// Check if the FST is empty.
    pub fn is_empty(&self) -> bool {
        self.header.entry_count == 0
    }

    /// Iterate all entries with a given prefix.
    pub fn prefix_iter(&self, prefix: &[u8]) -> PrefixIter<'_, D> {
        PrefixIter::new(self, prefix)
    }

    /// Zero-allocation prefix traversal via callback.
    pub fn prefix_each<F>(&self, prefix: &[u8], mut f: F) -> bool
    where
        F: FnMut(&[u8], u64) -> bool,
    {
        let bytes = self.data.as_ref();
        let mut key_buffer = Vec::new();
        let mut stack = Vec::new();

        // Navigate to prefix node
        let (start_node_id, start_output) = match self.navigate_to_prefix(prefix, &mut key_buffer) {
            Some(x) => x,
            None => return true,
        };

        stack.push(EachState {
            node_id: start_node_id,
            edge_idx: 0,
            key_len: key_buffer.len(),
            output_sum: start_output,
            emitted_final: false,
        });

        loop {
            let stack_len = stack.len();
            if stack_len == 0 {
                return true;
            }

            let state = &mut stack[stack_len - 1];
            let node = match self.get_node(state.node_id) {
                Some(n) => n,
                None => return true,
            };

            // Emit this node if final and not yet emitted
            if node.is_final && !state.emitted_final {
                state.emitted_final = true;
                let value = state.output_sum.wrapping_add(node.final_output);
                if !f(&key_buffer, value) {
                    return false;
                }
            }

            // Try next edge
            if state.edge_idx < node.edge_count() {
                let edge = match node.edge(state.edge_idx) {
                    Some(e) => e,
                    None => return true,
                };
                state.edge_idx += 1;

                let child_output = state.output_sum.wrapping_add(edge.output);
                let child_node_id = edge.target_node_id;

                // Prefetch child node
                let child_idx = self.get_node_index(child_node_id);
                prefetch_read(
                    bytes
                        .as_ptr()
                        .wrapping_add(self.header.hot_offset() + child_idx.hot_offset as usize),
                );

                key_buffer.extend_from_slice(edge.label);
                let child_key_len = key_buffer.len();

                stack.push(EachState {
                    node_id: child_node_id,
                    edge_idx: 0,
                    key_len: child_key_len,
                    output_sum: child_output,
                    emitted_final: false,
                });
            } else {
                stack.pop();
                if let Some(parent) = stack.last() {
                    key_buffer.truncate(parent.key_len);
                }
            }
        }
    }

    /// Navigate to prefix node.
    fn navigate_to_prefix(&self, prefix: &[u8], key_buffer: &mut Vec<u8>) -> Option<(u32, u64)> {
        let bytes = self.data.as_ref();
        let mut current_node_id = 0u32; // Root is always node 0
        let mut remaining = prefix;
        let mut output_sum: u64 = 0;

        while !remaining.is_empty() {
            let node = self.get_node(current_node_id)?;

            let first_byte = remaining[0];
            let edge = node.find_edge(first_byte)?;

            let match_len = edge
                .label
                .iter()
                .zip(remaining.iter())
                .take_while(|(a, b)| a == b)
                .count();

            if match_len < edge.label.len() && match_len < remaining.len() {
                return None;
            }

            output_sum = output_sum.wrapping_add(edge.output);

            // Prefetch next node
            let next_idx = self.get_node_index(edge.target_node_id);
            prefetch_read(
                bytes
                    .as_ptr()
                    .wrapping_add(self.header.hot_offset() + next_idx.hot_offset as usize),
            );

            if match_len >= remaining.len() {
                if match_len < edge.label.len() {
                    key_buffer.extend_from_slice(edge.label);
                    return Some((edge.target_node_id, output_sum));
                }
            }

            key_buffer.extend_from_slice(edge.label);
            remaining = &remaining[match_len..];
            current_node_id = edge.target_node_id;
        }

        Some((current_node_id, output_sum))
    }

    /// Get the raw bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.data.as_ref()
    }
}

/// Compact iterator state.
struct IterState {
    node_id: u32,
    edge_idx: usize,
    key_len: usize,
    output_sum: u64,
    emitted_final: bool,
}

/// Iterator over all entries with a given prefix.
pub struct PrefixIter<'a, D> {
    fst: &'a Fst<D>,
    key_buffer: Vec<u8>,
    stack: Vec<IterState>,
}

impl<'a, D: AsRef<[u8]>> PrefixIter<'a, D> {
    fn new(fst: &'a Fst<D>, prefix: &[u8]) -> Self {
        let mut iter = Self {
            fst,
            key_buffer: Vec::new(),
            stack: Vec::new(),
        };

        if let Some((node_id, output_sum)) = iter.navigate_to_prefix(prefix) {
            iter.stack.push(IterState {
                node_id,
                edge_idx: 0,
                key_len: iter.key_buffer.len(),
                output_sum,
                emitted_final: false,
            });
        }

        iter
    }

    fn navigate_to_prefix(&mut self, prefix: &[u8]) -> Option<(u32, u64)> {
        let bytes = self.fst.data.as_ref();
        let mut current_node_id = 0u32; // Root is always node 0
        let mut remaining = prefix;
        let mut output_sum: u64 = 0;

        while !remaining.is_empty() {
            let node = self.fst.get_node(current_node_id)?;

            let first_byte = remaining[0];
            let edge = node.find_edge(first_byte)?;

            let match_len = edge
                .label
                .iter()
                .zip(remaining.iter())
                .take_while(|(a, b)| a == b)
                .count();

            if match_len < edge.label.len() && match_len < remaining.len() {
                return None;
            }

            output_sum = output_sum.wrapping_add(edge.output);

            // Prefetch next node
            let next_idx = self.fst.get_node_index(edge.target_node_id);
            prefetch_read(
                bytes
                    .as_ptr()
                    .wrapping_add(self.fst.header.hot_offset() + next_idx.hot_offset as usize),
            );

            if match_len >= remaining.len() {
                if match_len < edge.label.len() {
                    self.key_buffer.extend_from_slice(edge.label);
                    return Some((edge.target_node_id, output_sum));
                }
            }

            self.key_buffer.extend_from_slice(edge.label);
            remaining = &remaining[match_len..];
            current_node_id = edge.target_node_id;
        }

        Some((current_node_id, output_sum))
    }
}

impl<D: AsRef<[u8]>> Iterator for PrefixIter<'_, D> {
    type Item = (Vec<u8>, u64);

    fn next(&mut self) -> Option<Self::Item> {
        let bytes = self.fst.data.as_ref();

        loop {
            let stack_len = self.stack.len();
            if stack_len == 0 {
                return None;
            }

            let state = &mut self.stack[stack_len - 1];
            let node = self.fst.get_node(state.node_id)?;

            // Emit this node if final and not yet emitted
            if node.is_final && !state.emitted_final {
                state.emitted_final = true;
                let key = self.key_buffer.clone();
                let value = state.output_sum.wrapping_add(node.final_output);
                return Some((key, value));
            }

            // Try next edge
            if state.edge_idx < node.edge_count() {
                let edge = node.edge(state.edge_idx)?;
                state.edge_idx += 1;

                let child_output = state.output_sum.wrapping_add(edge.output);
                let child_node_id = edge.target_node_id;

                // Prefetch child node
                let child_idx = self.fst.get_node_index(child_node_id);
                prefetch_read(
                    bytes
                        .as_ptr()
                        .wrapping_add(self.fst.header.hot_offset() + child_idx.hot_offset as usize),
                );

                self.key_buffer.extend_from_slice(edge.label);
                let child_key_len = self.key_buffer.len();

                self.stack.push(IterState {
                    node_id: child_node_id,
                    edge_idx: 0,
                    key_len: child_key_len,
                    output_sum: child_output,
                    emitted_final: false,
                });
            } else {
                self.stack.pop();
                if let Some(parent) = self.stack.last() {
                    self.key_buffer.truncate(parent.key_len);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FstBuilder;

    #[test]
    fn test_get_single() {
        let mut builder = FstBuilder::new();
        builder.insert(b"hello", 42).unwrap();
        let data = builder.finish().unwrap();

        let fst = Fst::new(data).unwrap();
        assert_eq!(fst.get(b"hello"), Some(42));
        assert_eq!(fst.get(b"hell"), None);
        assert_eq!(fst.get(b"hello!"), None);
        assert_eq!(fst.get(b"world"), None);
    }

    #[test]
    fn test_get_multiple() {
        let mut builder = FstBuilder::new();
        builder.insert(b"bar", 1).unwrap();
        builder.insert(b"baz", 2).unwrap();
        builder.insert(b"foo", 3).unwrap();
        let data = builder.finish().unwrap();

        let fst = Fst::new(data).unwrap();
        assert_eq!(fst.get(b"bar"), Some(1));
        assert_eq!(fst.get(b"baz"), Some(2));
        assert_eq!(fst.get(b"foo"), Some(3));
        assert_eq!(fst.get(b"ba"), None);
        assert_eq!(fst.get(b"qux"), None);
    }

    #[test]
    fn test_shared_prefix() {
        let mut builder = FstBuilder::new();
        builder.insert(b"test", 1).unwrap();
        builder.insert(b"testing", 2).unwrap();
        builder.insert(b"tests", 3).unwrap();
        let data = builder.finish().unwrap();

        let fst = Fst::new(data).unwrap();
        assert_eq!(fst.get(b"test"), Some(1));
        assert_eq!(fst.get(b"testing"), Some(2));
        assert_eq!(fst.get(b"tests"), Some(3));
        assert_eq!(fst.get(b"tes"), None);
    }

    #[test]
    fn test_prefix_iter() {
        let mut builder = FstBuilder::new();
        builder.insert(b"foo", 1).unwrap();
        builder.insert(b"foobar", 2).unwrap();
        builder.insert(b"foobaz", 3).unwrap();
        builder.insert(b"qux", 4).unwrap();
        let data = builder.finish().unwrap();

        let fst = Fst::new(data).unwrap();

        let results: Vec<_> = fst.prefix_iter(b"foo").collect();
        assert_eq!(results.len(), 3);
        assert!(results.contains(&(b"foo".to_vec(), 1)));
        assert!(results.contains(&(b"foobar".to_vec(), 2)));
        assert!(results.contains(&(b"foobaz".to_vec(), 3)));

        let results: Vec<_> = fst.prefix_iter(b"foob").collect();
        assert_eq!(results.len(), 2);

        let results: Vec<_> = fst.prefix_iter(b"q").collect();
        assert_eq!(results.len(), 1);
        assert!(results.contains(&(b"qux".to_vec(), 4)));

        let results: Vec<_> = fst.prefix_iter(b"xyz").collect();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_prefix_iter_all() {
        let mut builder = FstBuilder::new();
        builder.insert(b"a", 1).unwrap();
        builder.insert(b"b", 2).unwrap();
        builder.insert(b"c", 3).unwrap();
        let data = builder.finish().unwrap();

        let fst = Fst::new(data).unwrap();

        let results: Vec<_> = fst.prefix_iter(b"").collect();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_prefix_each() {
        let mut builder = FstBuilder::new();
        builder.insert(b"foo", 1).unwrap();
        builder.insert(b"foobar", 2).unwrap();
        builder.insert(b"foobaz", 3).unwrap();
        builder.insert(b"qux", 4).unwrap();
        let data = builder.finish().unwrap();

        let fst = Fst::new(data).unwrap();

        let mut results = Vec::new();
        fst.prefix_each(b"foo", |key, value| {
            results.push((key.to_vec(), value));
            true
        });

        assert_eq!(results.len(), 3);
        assert!(results.contains(&(b"foo".to_vec(), 1)));
        assert!(results.contains(&(b"foobar".to_vec(), 2)));
        assert!(results.contains(&(b"foobaz".to_vec(), 3)));

        // Test early exit
        let mut count = 0;
        let completed = fst.prefix_each(b"foo", |_, _| {
            count += 1;
            count < 2
        });
        assert!(!completed);
        assert_eq!(count, 2);
    }
}
