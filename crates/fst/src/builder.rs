use crate::error::BuildError;
use crate::node::{
    EdgeData, FOOTER_SIZE, Footer, HEADER_SIZE, INDEX_ENTRY_SIZE, INDEXED_THRESHOLD, NodeData,
    NodeIndex, write_footer, write_header, write_node_cold, write_node_hot,
};
use fastvlq::encode_vu64;

/// A builder for constructing radix tree FSTs.
///
/// Keys must be inserted in lexicographic order.
pub struct FstBuilder {
    root: BuilderNode,
    len: u64,
    last_key: Option<Vec<u8>>,
}

#[derive(Default)]
struct BuilderNode {
    is_final: bool,
    final_output: u64,
    edges: Vec<BuilderEdge>,
}

struct BuilderEdge {
    label: Vec<u8>,
    output: u64,
    target: BuilderNode,
}

impl FstBuilder {
    /// Create a new FST builder.
    pub fn new() -> Self {
        Self {
            root: BuilderNode::default(),
            len: 0,
            last_key: None,
        }
    }

    /// Insert a key-value pair.
    ///
    /// Keys must be inserted in lexicographic order.
    pub fn insert(&mut self, key: &[u8], value: u64) -> Result<(), BuildError> {
        // Validate ordering
        if let Some(ref last) = self.last_key {
            match key.cmp(last.as_slice()) {
                std::cmp::Ordering::Less => return Err(BuildError::OutOfOrder),
                std::cmp::Ordering::Equal => return Err(BuildError::DuplicateKey),
                std::cmp::Ordering::Greater => {}
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

    /// Finish building and return the serialized FST bytes.
    pub fn finish(self) -> Result<Vec<u8>, BuildError> {
        if self.len == 0 {
            return Err(BuildError::Empty);
        }

        // Step 1: Assign node IDs and collect NodeData (depth-first)
        let mut nodes: Vec<NodeData> = Vec::new();
        let root_id = self.collect_nodes(&self.root, &mut nodes);

        let node_count = nodes.len() as u32;

        // Step 2: Calculate layout
        let index_start = HEADER_SIZE;
        let index_size = node_count as usize * INDEX_ENTRY_SIZE;
        let hot_start = index_start + index_size;

        // Step 3: Write hot section, tracking offsets
        let mut hot_buf = Vec::new();
        let mut hot_offsets: Vec<u32> = Vec::with_capacity(nodes.len());
        let mut offsets_positions: Vec<usize> = Vec::with_capacity(nodes.len());

        for node in &nodes {
            hot_offsets.push(hot_buf.len() as u32);
            let before_len = hot_buf.len();
            write_node_hot(node, &mut hot_buf).map_err(BuildError::Io)?;

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
            write_node_cold(node, &mut hot_buf, offsets_positions[i], &mut cold_buf)
                .map_err(BuildError::Io)?;
        }

        // Step 5: Assemble final buffer
        let total_size = HEADER_SIZE + index_size + hot_buf.len() + cold_buf.len() + FOOTER_SIZE;
        let mut buf = Vec::with_capacity(total_size);

        // Header
        write_header(self.len, &mut buf);

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

        // Footer
        let footer = Footer {
            root_node_id: root_id,
            node_count,
            hot_start: hot_start as u32,
            cold_start: cold_start as u32,
        };
        write_footer(&footer, &mut buf);

        Ok(buf)
    }
}

/// Calculate VLQ encoded length for a value.
#[inline]
fn vlq_len(value: u64) -> usize {
    encode_vu64(value).len() as usize
}

// Private implementation
impl FstBuilder {
    fn insert_into_node(node: &mut BuilderNode, key: &[u8], value: u64) {
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
                    let old_label = std::mem::take(&mut edge.label);
                    let old_target = std::mem::take(&mut edge.target);

                    // Create new intermediate node
                    let mut intermediate = BuilderNode::default();

                    // Add old suffix as edge from intermediate
                    intermediate.edges.push(BuilderEdge {
                        label: old_label[common_len..].to_vec(),
                        output: 0,
                        target: old_target,
                    });

                    // Add new suffix as edge from intermediate (if there's remaining key)
                    if common_len < key.len() {
                        intermediate.edges.push(BuilderEdge {
                            label: key[common_len..].to_vec(),
                            output: 0,
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
                    edge.output = 0;
                    edge.target = intermediate;
                }
            }
            None => {
                // No matching edge, create new one
                node.edges.push(BuilderEdge {
                    label: key.to_vec(),
                    output: 0,
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
    fn collect_nodes(&self, root: &BuilderNode, nodes: &mut Vec<NodeData>) -> u32 {
        use std::collections::VecDeque;

        // Phase 1: Assign node IDs in BFS order
        // Map from node pointer to assigned ID
        let mut node_ids: std::collections::HashMap<*const BuilderNode, u32> =
            std::collections::HashMap::new();
        let mut queue: VecDeque<&BuilderNode> = VecDeque::new();

        queue.push_back(root);
        let mut next_id = 0u32;

        while let Some(node) = queue.pop_front() {
            let ptr = node as *const BuilderNode;
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
        let mut visited: std::collections::HashSet<*const BuilderNode> =
            std::collections::HashSet::new();

        while let Some(node) = queue.pop_front() {
            let ptr = node as *const BuilderNode;
            if visited.contains(&ptr) {
                continue;
            }
            visited.insert(ptr);

            let edges: Vec<EdgeData> = node
                .edges
                .iter()
                .map(|edge| {
                    let child_ptr = &edge.target as *const BuilderNode;
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

impl Default for FstBuilder {
    fn default() -> Self {
        Self::new()
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
        let mut builder = FstBuilder::new();
        builder.insert(b"hello", 42).unwrap();
        assert_eq!(builder.len(), 1);

        let data = builder.finish().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_multiple_keys() {
        let mut builder = FstBuilder::new();
        builder.insert(b"bar", 1).unwrap();
        builder.insert(b"baz", 2).unwrap();
        builder.insert(b"foo", 3).unwrap();
        assert_eq!(builder.len(), 3);

        let data = builder.finish().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_shared_prefix() {
        let mut builder = FstBuilder::new();
        builder.insert(b"test", 1).unwrap();
        builder.insert(b"testing", 2).unwrap();
        builder.insert(b"tests", 3).unwrap();
        assert_eq!(builder.len(), 3);

        let data = builder.finish().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_out_of_order() {
        let mut builder = FstBuilder::new();
        builder.insert(b"foo", 1).unwrap();
        assert!(matches!(
            builder.insert(b"bar", 2),
            Err(BuildError::OutOfOrder)
        ));
    }

    #[test]
    fn test_duplicate() {
        let mut builder = FstBuilder::new();
        builder.insert(b"foo", 1).unwrap();
        assert!(matches!(
            builder.insert(b"foo", 2),
            Err(BuildError::DuplicateKey)
        ));
    }

    #[test]
    fn test_empty() {
        let builder = FstBuilder::new();
        assert!(matches!(builder.finish(), Err(BuildError::Empty)));
    }
}
