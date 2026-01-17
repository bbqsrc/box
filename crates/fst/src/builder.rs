use alloc::{collections::VecDeque, vec::Vec};

use core::cmp::Ordering;
use core::marker::PhantomData;
use core::mem::take;

use hashbrown::{HashMap, HashSet};

use crate::error::BuildError;
use crate::node::{
    EdgeData, FstValue, HEADER_SIZE, Header, INDEX_ENTRY_SIZE, INDEXED_THRESHOLD, NodeData,
    NodeIndex, write_header, write_node_cold, write_node_hot,
};
use fastvint::encode_vu64;

/// A builder for constructing radix tree FSTs.
///
/// Keys must be inserted in lexicographic order.
/// Generic over value type `V` (default: `u64`).
pub struct FstBuilder<V: FstValue = u64> {
    root: BuilderNode<V>,
    len: u64,
    last_key: Option<Vec<u8>>,
    _marker: PhantomData<V>,
}

struct BuilderNode<V: FstValue> {
    is_final: bool,
    final_output: V,
    edges: Vec<BuilderEdge<V>>,
}

impl<V: FstValue> Default for BuilderNode<V> {
    fn default() -> Self {
        Self {
            is_final: false,
            final_output: V::default(),
            edges: Vec::new(),
        }
    }
}

struct BuilderEdge<V: FstValue> {
    label: Vec<u8>,
    output: V,
    target: BuilderNode<V>,
}

impl<V: FstValue> FstBuilder<V> {
    /// Create a new FST builder.
    pub fn new() -> Self {
        Self {
            root: BuilderNode::default(),
            len: 0,
            last_key: None,
            _marker: PhantomData,
        }
    }

    /// Insert a key-value pair.
    ///
    /// Keys must be inserted in lexicographic order.
    pub fn insert(&mut self, key: &[u8], value: V) -> Result<(), BuildError> {
        // Validate ordering
        if let Some(ref last) = self.last_key {
            match key.cmp(last.as_slice()) {
                Ordering::Less => return Err(BuildError::OutOfOrder),
                Ordering::Equal => return Err(BuildError::DuplicateKey),
                Ordering::Greater => {}
            }
        }

        Self::insert_into_node(&mut self.root, key, value);
        self.last_key = Some(key.to_vec());
        self.len += 1;
        Ok(())
    }

    /// Number of keys inserted.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Check if the builder is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Look up a key in the in-memory tree.
    pub fn get(&self, key: &[u8]) -> Option<V> {
        Self::get_from_node(&self.root, key)
    }

    /// Check if a key exists.
    pub fn contains(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Iterate all entries with a given prefix.
    pub fn prefix_iter(&self, prefix: &[u8]) -> BuilderPrefixIter<'_, V> {
        BuilderPrefixIter::new(&self.root, prefix)
    }

    /// Insert or replace a key-value pair.
    ///
    /// Unlike `insert()`, this does not require sorted order and allows
    /// replacing existing keys. Useful for merging multiple FSTs.
    pub fn insert_or_replace(&mut self, key: &[u8], value: V) {
        let is_new = Self::insert_or_replace_node(&mut self.root, key, value);
        if is_new {
            self.len += 1;
        }
        // Update last_key if this key is greater (maintains partial ordering info)
        if self
            .last_key
            .as_ref()
            .map_or(true, |last| key > last.as_slice())
        {
            self.last_key = Some(key.to_vec());
        }
    }

    fn get_from_node(node: &BuilderNode<V>, key: &[u8]) -> Option<V> {
        if key.is_empty() {
            return if node.is_final {
                Some(node.final_output)
            } else {
                None
            };
        }

        let first_byte = key[0];

        // Binary search for edge with matching first byte (edges are sorted by label)
        let edge = node.edges.binary_search_by(|e| {
            if e.label.is_empty() {
                Ordering::Less
            } else {
                e.label[0].cmp(&first_byte)
            }
        });

        let idx = match edge {
            Ok(i) => i,
            Err(_) => return None, // No edge with matching first byte
        };

        let edge = &node.edges[idx];

        // Check if key starts with edge label
        if key.len() < edge.label.len() {
            return None; // Key is shorter than edge label
        }
        if &key[..edge.label.len()] != edge.label.as_slice() {
            return None; // Key doesn't match edge label
        }

        // Recurse with remaining key
        Self::get_from_node(&edge.target, &key[edge.label.len()..])
    }

    /// Insert or replace, returns true if this was a new key.
    fn insert_or_replace_node(node: &mut BuilderNode<V>, key: &[u8], value: V) -> bool {
        if key.is_empty() {
            let is_new = !node.is_final;
            node.is_final = true;
            node.final_output = value;
            return is_new;
        }

        let first_byte = key[0];

        // Find edge with matching first byte
        let edge_idx = node
            .edges
            .iter()
            .position(|e| !e.label.is_empty() && e.label[0] == first_byte);

        match edge_idx {
            Some(idx) => {
                let edge = &mut node.edges[idx];
                let common_len = common_prefix_len(&edge.label, key);

                if common_len == edge.label.len() {
                    // Edge label is a prefix of key, recurse into target
                    Self::insert_or_replace_node(&mut edge.target, &key[common_len..], value)
                } else {
                    // Need to split the edge
                    let old_label = take(&mut edge.label);
                    let old_target = take(&mut edge.target);

                    // Create new intermediate node
                    let mut intermediate = BuilderNode::default();

                    // Add old suffix as edge from intermediate
                    intermediate.edges.push(BuilderEdge {
                        label: old_label[common_len..].to_vec(),
                        output: V::default(),
                        target: old_target,
                    });

                    // Add new suffix as edge from intermediate (if there's remaining key)
                    if common_len < key.len() {
                        intermediate.edges.push(BuilderEdge {
                            label: key[common_len..].to_vec(),
                            output: V::default(),
                            target: BuilderNode {
                                is_final: true,
                                final_output: value,
                                edges: Vec::new(),
                            },
                        });
                        intermediate.edges.sort_by(|a, b| a.label.cmp(&b.label));
                    } else {
                        // Key ends at the split point
                        intermediate.is_final = true;
                        intermediate.final_output = value;
                    }

                    // Update current edge to point to intermediate
                    edge.label = old_label[..common_len].to_vec();
                    edge.output = V::default();
                    edge.target = intermediate;

                    true // New key
                }
            }
            None => {
                // No matching edge, create new one
                node.edges.push(BuilderEdge {
                    label: key.to_vec(),
                    output: V::default(),
                    target: BuilderNode {
                        is_final: true,
                        final_output: value,
                        edges: Vec::new(),
                    },
                });
                node.edges.sort_by(|a, b| a.label.cmp(&b.label));
                true // New key
            }
        }
    }

    /// Finish building and return the serialized FST bytes.
    pub fn finish(self) -> Result<Vec<u8>, BuildError> {
        if self.len == 0 {
            return Err(BuildError::Empty);
        }

        // Step 1: Assign node IDs and collect NodeData (depth-first)
        let mut nodes: Vec<NodeData<V>> = Vec::new();
        let _root_id = self.collect_nodes(&self.root, &mut nodes);

        let node_count = nodes.len() as u32;

        // Step 2: Calculate layout
        let index_size = node_count as usize * INDEX_ENTRY_SIZE;
        let hot_start = HEADER_SIZE + index_size;

        // Step 3: Write hot section, tracking offsets
        let mut hot_buf = Vec::new();
        let mut hot_offsets: Vec<u32> = Vec::with_capacity(nodes.len());
        let mut offsets_positions: Vec<usize> = Vec::with_capacity(nodes.len());

        for node in &nodes {
            hot_offsets.push(hot_buf.len() as u32);
            let before_len = hot_buf.len();
            write_node_hot(node, &mut hot_buf);

            // Calculate where offsets start in this node's hot data
            // Compact: flags(1) + vlq(edge_count) + first_bytes(n)
            // Indexed: flags(1) + vlq(edge_count) + index_table(256)
            let vlq_len = vlq_len(node.edges.len() as u64);
            let index_size = if node.edges.len() >= INDEXED_THRESHOLD {
                256 // 256-byte index table
            } else {
                node.edges.len() // first_bytes array
            };
            let offsets_pos = before_len + 1 + vlq_len + index_size;
            offsets_positions.push(offsets_pos);
        }

        let cold_start = hot_start + hot_buf.len();

        // Step 4: Write cold section, updating hot offsets
        let mut cold_buf = Vec::new();
        let mut cold_offsets: Vec<u32> = Vec::with_capacity(nodes.len());

        for (i, node) in nodes.iter().enumerate() {
            cold_offsets.push(cold_buf.len() as u32);
            write_node_cold(node, &mut hot_buf, offsets_positions[i], &mut cold_buf);
        }

        // Step 5: Assemble final buffer
        let total_size = HEADER_SIZE + index_size + hot_buf.len() + cold_buf.len();
        let mut buf = Vec::with_capacity(total_size);

        // Header (includes node_count and cold_offset)
        let header = Header {
            entry_count: self.len,
            node_count,
            cold_offset: cold_start as u32,
        };
        write_header(&header, &mut buf);

        // Index
        for i in 0..nodes.len() {
            let idx = NodeIndex {
                hot_offset: hot_offsets[i],
                cold_offset: cold_offsets[i],
            };
            idx.write(&mut buf);
        }

        // Hot section
        buf.extend_from_slice(&hot_buf);

        // Cold section
        buf.extend_from_slice(&cold_buf);

        Ok(buf)
    }
}

/// Calculate VLQ encoded length for a value.
#[inline]
fn vlq_len(value: u64) -> usize {
    encode_vu64(value).len() as usize
}

// Private implementation
impl<V: FstValue> FstBuilder<V> {
    fn insert_into_node(node: &mut BuilderNode<V>, key: &[u8], value: V) {
        if key.is_empty() {
            node.is_final = true;
            node.final_output = value;
            return;
        }

        let first_byte = key[0];

        // Find edge with matching first byte
        let edge_idx = node
            .edges
            .iter()
            .position(|e| !e.label.is_empty() && e.label[0] == first_byte);

        match edge_idx {
            Some(idx) => {
                // Found edge with same first byte - need to handle prefix
                let edge = &mut node.edges[idx];
                let common_len = common_prefix_len(&edge.label, key);

                if common_len == edge.label.len() {
                    // Edge label is a prefix of key, recurse into target
                    Self::insert_into_node(&mut edge.target, &key[common_len..], value);
                } else {
                    // Need to split the edge
                    let old_label = take(&mut edge.label);
                    let old_target = take(&mut edge.target);

                    // Create new intermediate node
                    let mut intermediate = BuilderNode::default();

                    // Add old suffix as edge from intermediate
                    intermediate.edges.push(BuilderEdge {
                        label: old_label[common_len..].to_vec(),
                        output: V::default(),
                        target: old_target,
                    });

                    // Add new suffix as edge from intermediate (if there's remaining key)
                    if common_len < key.len() {
                        intermediate.edges.push(BuilderEdge {
                            label: key[common_len..].to_vec(),
                            output: V::default(),
                            target: BuilderNode {
                                is_final: true,
                                final_output: value,
                                edges: Vec::new(),
                            },
                        });
                        // Sort edges by first byte
                        intermediate.edges.sort_by(|a, b| a.label.cmp(&b.label));
                    } else {
                        // Key ends at the split point
                        intermediate.is_final = true;
                        intermediate.final_output = value;
                    }

                    // Update current edge to point to intermediate
                    edge.label = old_label[..common_len].to_vec();
                    edge.output = V::default();
                    edge.target = intermediate;
                }
            }
            None => {
                // No matching edge, create new one
                node.edges.push(BuilderEdge {
                    label: key.to_vec(),
                    output: V::default(),
                    target: BuilderNode {
                        is_final: true,
                        final_output: value,
                        edges: Vec::new(),
                    },
                });
                // Keep edges sorted by first byte
                node.edges.sort_by(|a, b| a.label.cmp(&b.label));
            }
        }
    }

    /// Collect nodes breadth-first for better cache locality.
    /// Returns the root node ID (always 0 in BFS order).
    fn collect_nodes(&self, root: &BuilderNode<V>, nodes: &mut Vec<NodeData<V>>) -> u32 {
        use VecDeque;

        // Phase 1: Assign node IDs in BFS order
        // Map from node pointer to assigned ID
        let mut node_ids: HashMap<*const BuilderNode<V>, u32> = HashMap::new();
        let mut queue: VecDeque<&BuilderNode<V>> = VecDeque::new();

        queue.push_back(root);
        let mut next_id = 0u32;

        while let Some(node) = queue.pop_front() {
            let ptr = node as *const BuilderNode<V>;
            if node_ids.contains_key(&ptr) {
                continue;
            }
            node_ids.insert(ptr, next_id);
            next_id += 1;

            // Enqueue children
            for edge in &node.edges {
                queue.push_back(&edge.target);
            }
        }

        // Phase 2: Build NodeData in BFS order
        queue.push_back(root);
        let mut visited: HashSet<*const BuilderNode<V>> = HashSet::new();

        while let Some(node) = queue.pop_front() {
            let ptr = node as *const BuilderNode<V>;
            if visited.contains(&ptr) {
                continue;
            }
            visited.insert(ptr);

            let edges: Vec<EdgeData<V>> = node
                .edges
                .iter()
                .map(|edge| {
                    let child_ptr = &edge.target as *const BuilderNode<V>;
                    let child_id = node_ids[&child_ptr];
                    EdgeData {
                        label: edge.label.clone(),
                        output: edge.output,
                        target_node_id: child_id,
                    }
                })
                .collect();

            nodes.push(NodeData {
                is_final: node.is_final,
                final_output: node.final_output,
                edges,
            });

            // Enqueue children for processing
            for edge in &node.edges {
                queue.push_back(&edge.target);
            }
        }

        0 // Root is always node 0 in BFS order
    }
}

impl<V: FstValue> Default for FstBuilder<V> {
    fn default() -> Self {
        Self::new()
    }
}

/// Iterator state for prefix iteration.
struct IterState<'a, V: FstValue> {
    node: &'a BuilderNode<V>,
    edge_idx: usize,
    key_len: usize,
    emitted_final: bool,
}

/// Iterator over entries with a given prefix in the in-memory builder tree.
pub struct BuilderPrefixIter<'a, V: FstValue = u64> {
    stack: Vec<IterState<'a, V>>,
    key_buffer: Vec<u8>,
}

impl<'a, V: FstValue> BuilderPrefixIter<'a, V> {
    fn new(root: &'a BuilderNode<V>, prefix: &[u8]) -> Self {
        let mut iter = Self {
            stack: Vec::new(),
            key_buffer: Vec::new(),
        };

        // Navigate to prefix node
        if let Some((node, consumed)) = Self::navigate_to_prefix(root, prefix, &mut iter.key_buffer)
        {
            // If we consumed the whole prefix, start iteration from this node
            if consumed == prefix.len() {
                iter.stack.push(IterState {
                    node,
                    edge_idx: 0,
                    key_len: iter.key_buffer.len(),
                    emitted_final: false,
                });
            }
            // If we stopped mid-edge (prefix ends inside an edge label),
            // that's handled by navigate_to_prefix returning the target node
        }

        iter
    }

    /// Navigate to the node at the given prefix.
    /// Returns (node, bytes_consumed) or None if prefix doesn't exist.
    fn navigate_to_prefix<'b>(
        node: &'b BuilderNode<V>,
        prefix: &[u8],
        key_buffer: &mut Vec<u8>,
    ) -> Option<(&'b BuilderNode<V>, usize)> {
        if prefix.is_empty() {
            return Some((node, 0));
        }

        let first_byte = prefix[0];

        for edge in &node.edges {
            if edge.label.is_empty() || edge.label[0] != first_byte {
                continue;
            }

            let match_len = edge
                .label
                .iter()
                .zip(prefix.iter())
                .take_while(|(a, b)| a == b)
                .count();

            if match_len == 0 {
                continue;
            }

            // If edge label fully matches prefix start
            if match_len == edge.label.len() {
                key_buffer.extend_from_slice(&edge.label);
                if match_len >= prefix.len() {
                    // Prefix exhausted, return target node
                    return Some((&edge.target, prefix.len()));
                }
                // Continue navigating
                return Self::navigate_to_prefix(&edge.target, &prefix[match_len..], key_buffer)
                    .map(|(n, c)| (n, match_len + c));
            }

            // Prefix ends mid-edge - if prefix is fully matched, return target
            if match_len >= prefix.len() {
                key_buffer.extend_from_slice(&edge.label);
                return Some((&edge.target, prefix.len()));
            }

            // Partial match but prefix continues - no match
            return None;
        }

        None
    }
}

impl<'a, V: FstValue> Iterator for BuilderPrefixIter<'a, V> {
    type Item = (Vec<u8>, V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let stack_len = self.stack.len();
            if stack_len == 0 {
                return None;
            }

            let state = &mut self.stack[stack_len - 1];

            // Emit this node if final and not yet emitted
            if state.node.is_final && !state.emitted_final {
                state.emitted_final = true;
                let key = self.key_buffer.clone();
                let value = state.node.final_output;
                return Some((key, value));
            }

            // Try next edge
            if state.edge_idx < state.node.edges.len() {
                let edge = &state.node.edges[state.edge_idx];
                state.edge_idx += 1;

                self.key_buffer.extend_from_slice(&edge.label);
                let child_key_len = self.key_buffer.len();

                self.stack.push(IterState {
                    node: &edge.target,
                    edge_idx: 0,
                    key_len: child_key_len,
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

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_key() {
        let mut builder: FstBuilder<u64> = FstBuilder::new();
        builder.insert(b"hello", 42).unwrap();
        assert_eq!(builder.len(), 1);

        let data = builder.finish().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_multiple_keys() {
        let mut builder: FstBuilder<u64> = FstBuilder::new();
        builder.insert(b"bar", 1).unwrap();
        builder.insert(b"baz", 2).unwrap();
        builder.insert(b"foo", 3).unwrap();
        assert_eq!(builder.len(), 3);

        let data = builder.finish().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_shared_prefix() {
        let mut builder: FstBuilder<u64> = FstBuilder::new();
        builder.insert(b"test", 1).unwrap();
        builder.insert(b"testing", 2).unwrap();
        builder.insert(b"tests", 3).unwrap();
        assert_eq!(builder.len(), 3);

        let data = builder.finish().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_out_of_order() {
        let mut builder: FstBuilder<u64> = FstBuilder::new();
        builder.insert(b"foo", 1).unwrap();
        assert!(matches!(
            builder.insert(b"bar", 2),
            Err(BuildError::OutOfOrder)
        ));
    }

    #[test]
    fn test_duplicate() {
        let mut builder: FstBuilder<u64> = FstBuilder::new();
        builder.insert(b"foo", 1).unwrap();
        assert!(matches!(
            builder.insert(b"foo", 2),
            Err(BuildError::DuplicateKey)
        ));
    }

    #[test]
    fn test_empty() {
        let builder: FstBuilder<u64> = FstBuilder::new();
        assert!(matches!(builder.finish(), Err(BuildError::Empty)));
    }

    #[test]
    fn test_get() {
        let mut builder: FstBuilder<u64> = FstBuilder::new();
        builder.insert(b"bar", 1).unwrap();
        builder.insert(b"baz", 2).unwrap();
        builder.insert(b"foo", 3).unwrap();

        assert_eq!(builder.get(b"bar"), Some(1));
        assert_eq!(builder.get(b"baz"), Some(2));
        assert_eq!(builder.get(b"foo"), Some(3));
        assert_eq!(builder.get(b"ba"), None);
        assert_eq!(builder.get(b"qux"), None);
        assert_eq!(builder.get(b""), None);
    }

    #[test]
    fn test_get_shared_prefix() {
        let mut builder: FstBuilder<u64> = FstBuilder::new();
        builder.insert(b"test", 1).unwrap();
        builder.insert(b"testing", 2).unwrap();
        builder.insert(b"tests", 3).unwrap();

        assert_eq!(builder.get(b"test"), Some(1));
        assert_eq!(builder.get(b"testing"), Some(2));
        assert_eq!(builder.get(b"tests"), Some(3));
        assert_eq!(builder.get(b"tes"), None);
        assert_eq!(builder.get(b"testi"), None);
    }

    #[test]
    fn test_contains() {
        let mut builder: FstBuilder<u64> = FstBuilder::new();
        builder.insert(b"foo", 1).unwrap();
        builder.insert(b"foobar", 2).unwrap();

        assert!(builder.contains(b"foo"));
        assert!(builder.contains(b"foobar"));
        assert!(!builder.contains(b"fo"));
        assert!(!builder.contains(b"foob"));
        assert!(!builder.contains(b"baz"));
    }

    #[test]
    fn test_prefix_iter() {
        let mut builder: FstBuilder<u64> = FstBuilder::new();
        builder.insert(b"foo", 1).unwrap();
        builder.insert(b"foobar", 2).unwrap();
        builder.insert(b"foobaz", 3).unwrap();
        builder.insert(b"qux", 4).unwrap();

        let results: Vec<_> = builder.prefix_iter(b"foo").collect();
        assert_eq!(results.len(), 3);
        assert!(results.contains(&(b"foo".to_vec(), 1)));
        assert!(results.contains(&(b"foobar".to_vec(), 2)));
        assert!(results.contains(&(b"foobaz".to_vec(), 3)));

        let results: Vec<_> = builder.prefix_iter(b"foob").collect();
        assert_eq!(results.len(), 2);

        let results: Vec<_> = builder.prefix_iter(b"q").collect();
        assert_eq!(results.len(), 1);
        assert!(results.contains(&(b"qux".to_vec(), 4)));

        let results: Vec<_> = builder.prefix_iter(b"xyz").collect();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_prefix_iter_all() {
        let mut builder: FstBuilder<u64> = FstBuilder::new();
        builder.insert(b"a", 1).unwrap();
        builder.insert(b"b", 2).unwrap();
        builder.insert(b"c", 3).unwrap();

        let results: Vec<_> = builder.prefix_iter(b"").collect();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_insert_or_replace_new() {
        let mut builder: FstBuilder<u64> = FstBuilder::new();
        builder.insert_or_replace(b"foo", 1);
        builder.insert_or_replace(b"bar", 2);

        assert_eq!(builder.len(), 2);
        assert_eq!(builder.get(b"foo"), Some(1));
        assert_eq!(builder.get(b"bar"), Some(2));
    }

    #[test]
    fn test_insert_or_replace_overwrite() {
        let mut builder: FstBuilder<u64> = FstBuilder::new();
        builder.insert_or_replace(b"foo", 1);
        builder.insert_or_replace(b"foo", 99);

        assert_eq!(builder.len(), 1); // Still only one key
        assert_eq!(builder.get(b"foo"), Some(99)); // Value updated
    }

    #[test]
    fn test_insert_or_replace_out_of_order() {
        let mut builder: FstBuilder<u64> = FstBuilder::new();
        // Insert out of order - should work unlike insert()
        builder.insert_or_replace(b"foo", 1);
        builder.insert_or_replace(b"bar", 2);
        builder.insert_or_replace(b"baz", 3);

        assert_eq!(builder.len(), 3);
        assert_eq!(builder.get(b"foo"), Some(1));
        assert_eq!(builder.get(b"bar"), Some(2));
        assert_eq!(builder.get(b"baz"), Some(3));
    }

    #[test]
    fn test_insert_or_replace_with_shared_prefix() {
        let mut builder: FstBuilder<u64> = FstBuilder::new();
        builder.insert_or_replace(b"testing", 1);
        builder.insert_or_replace(b"test", 2); // Shorter key after longer

        assert_eq!(builder.len(), 2);
        assert_eq!(builder.get(b"testing"), Some(1));
        assert_eq!(builder.get(b"test"), Some(2));
    }
}
