use std::collections::HashMap;
use std::num::NonZeroU64;
use std::{
    borrow::Cow,
    collections::{BTreeMap, VecDeque},
    fmt::Display,
};

use crate::Record;
use crate::path::BoxPath;
use crate::record::DirectoryRecord;

/// Record index within an archive.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RecordIndex(NonZeroU64);

impl RecordIndex {
    pub fn new(value: u64) -> std::io::Result<RecordIndex> {
        match NonZeroU64::new(value) {
            Some(v) => Ok(RecordIndex(v)),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "record index must not be zero",
            )),
        }
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }
}

impl From<NonZeroU64> for RecordIndex {
    fn from(value: NonZeroU64) -> Self {
        RecordIndex(value)
    }
}

/// Map of attribute key indices to raw byte values.
pub type AttrMap = HashMap<usize, Box<[u8]>>;

/// Attribute type tag stored in the archive
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AttrType {
    /// Raw bytes, no parsing
    Bytes = 0,
    /// UTF-8 string
    String = 1,
    /// UTF-8 JSON
    Json = 2,
    /// Single byte (u8)
    U8 = 3,
    /// Variable-length signed 32-bit (zigzag)
    Vi32 = 4,
    /// Variable-length unsigned 32-bit
    Vu32 = 5,
    /// Variable-length signed 64-bit (zigzag)
    Vi64 = 6,
    /// Variable-length unsigned 64-bit
    Vu64 = 7,
    /// Fixed 16 bytes (128 bits, UUIDs)
    U128 = 8,
    /// Fixed 32 bytes (256 bits)
    U256 = 9,
    /// Minutes since Box epoch (2026-01-01 00:00:00 UTC), zigzag-encoded i64
    DateTime = 10,
}

impl AttrType {
    /// Convert from u8 tag value
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Bytes),
            1 => Some(Self::String),
            2 => Some(Self::Json),
            3 => Some(Self::U8),
            4 => Some(Self::Vi32),
            5 => Some(Self::Vu32),
            6 => Some(Self::Vi64),
            7 => Some(Self::Vu64),
            8 => Some(Self::U128),
            9 => Some(Self::U256),
            10 => Some(Self::DateTime),
            _ => None, // Reserved values 11-255
        }
    }
}

/// Interned attribute key with type information
#[derive(Debug, Clone)]
pub struct AttrKey {
    pub name: String,
    pub attr_type: AttrType,
}

#[derive(Default)]
pub struct BoxMetadata<'a> {
    /// Root "directory" keyed by record indices
    pub(crate) root: Vec<RecordIndex>,

    /// Keyed by record index (offset by -1). This means if a `RecordIndex` has the value 1, its index in this vector is 0.
    /// This is to provide compatibility with platforms such as Linux, and allow for error checking a box file.
    pub(crate) records: Vec<Record<'a>>,

    /// Interned attribute keys with type information.
    pub(crate) attr_keys: Vec<AttrKey>,

    /// The global attributes that apply to this entire box file.
    pub(crate) attrs: AttrMap,

    /// Zstd dictionary for compression/decompression.
    /// When present, all Zstd-compressed content (files and chunked blocks) uses this dictionary.
    /// None means no dictionary training was performed.
    pub(crate) dictionary: Option<Box<[u8]>>,

    /// Parsed FST for O(key_length) path lookups and iteration.
    /// None for old archives without FST support.
    pub(crate) fst: Option<box_fst::Fst<Cow<'a, [u8]>>>,

    /// Block FST for seeking within chunked files.
    /// Keys are 16 bytes: record_index (u64 BE) + logical_offset (u64 BE).
    /// Values are physical offsets within the compressed data.
    /// None for archives without chunked files or block index.
    pub(crate) block_fst: Option<box_fst::Fst<Cow<'a, [u8]>>>,
}

impl std::fmt::Debug for BoxMetadata<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoxMetadata")
            .field("root", &self.root)
            .field("records", &self.records)
            .field("attr_keys_count", &self.attr_keys.len())
            .field("attrs", &self.attrs)
            .field("dictionary", &self.dictionary.as_ref().map(|d| d.len()))
            .field("fst", &self.fst.as_ref().map(|f| f.len()))
            .field("block_fst", &self.block_fst.as_ref().map(|f| f.len()))
            .finish()
    }
}

impl<'a> BoxMetadata<'a> {
    /// Convert borrowed metadata to owned.
    pub fn into_owned(self) -> BoxMetadata<'static> {
        BoxMetadata {
            root: self.root,
            records: self.records.into_iter().map(|r| r.into_owned()).collect(),
            attr_keys: self.attr_keys,
            attrs: self.attrs,
            dictionary: self.dictionary,
            fst: self
                .fst
                .and_then(|f| box_fst::Fst::new(Cow::Owned(f.as_bytes().to_vec())).ok()),
            block_fst: self
                .block_fst
                .and_then(|f| box_fst::Fst::new(Cow::Owned(f.as_bytes().to_vec())).ok()),
        }
    }
}

#[derive(Debug)]
pub struct Records<'a, 'b> {
    meta: &'a BoxMetadata<'b>,
    entries: &'a [RecordIndex],
    base_path: Option<BoxPath<'static>>,
    cur_entry: usize,
    cur_dir: Option<Box<Records<'a, 'b>>>,
}

impl<'a, 'b> Records<'a, 'b> {
    pub(crate) fn new(
        meta: &'a BoxMetadata<'b>,
        entries: &'a [RecordIndex],
        base_path: Option<BoxPath<'static>>,
    ) -> Records<'a, 'b> {
        Records {
            meta,
            entries,
            base_path,
            cur_entry: 0,
            cur_dir: None,
        }
    }
}

#[non_exhaustive]
#[derive(Debug)]
pub struct RecordsItem<'a, 'b> {
    pub(crate) index: RecordIndex,
    pub path: BoxPath<'static>,
    pub record: &'a Record<'b>,
}

impl RecordsItem<'_, '_> {
    pub fn index(&self) -> RecordIndex {
        self.index
    }
}

impl<'a, 'b> Iterator for Records<'a, 'b> {
    type Item = RecordsItem<'a, 'b>;

    fn next(&mut self) -> Option<Self::Item> {
        // If iterating a child iterator, do it here.
        if let Some(dir) = self.cur_dir.as_mut() {
            if let Some(record) = dir.next() {
                return Some(record);
            }

            // If nothing left, clear it here.
            self.cur_dir = None;
        }
        let index = match self.entries.get(self.cur_entry) {
            Some(i) => *i,
            None => return None,
        };

        let record = self.meta.record(index)?;

        let base_path = match self.base_path.as_ref() {
            Some(x) => x.join_unchecked(record.name()),
            None => BoxPath(Cow::Owned(record.name().to_string())),
        };

        if let Record::Directory(record) = record {
            self.cur_dir = Some(Box::new(Records::new(
                self.meta,
                &record.entries,
                Some(base_path.clone()),
            )));
        }

        self.cur_entry += 1;
        Some(RecordsItem {
            index,
            path: base_path,
            record,
        })
    }
}

/// Iterator over records using FST.
pub struct FstRecordsIterator<'a, 'b> {
    meta: &'a BoxMetadata<'b>,
    inner: box_fst::PrefixIter<'a, Cow<'b, [u8]>>,
}

impl<'a, 'b> Iterator for FstRecordsIterator<'a, 'b> {
    type Item = RecordsItem<'a, 'b>;

    fn next(&mut self) -> Option<Self::Item> {
        for (path_bytes, idx) in self.inner.by_ref() {
            let Some(index) = RecordIndex::new(idx).ok() else {
                continue;
            };
            let Some(record) = self.meta.record(index) else {
                continue;
            };
            let Ok(path_str) = std::str::from_utf8(&path_bytes) else {
                continue;
            };
            return Some(RecordsItem {
                index,
                path: BoxPath(Cow::Owned(path_str.to_string())),
                record,
            });
        }
        None
    }
}

/// Iterator over metadata records - either tree-based or FST-based.
pub enum MetadataIter<'a, 'b> {
    Tree(Records<'a, 'b>),
    Fst(FstRecordsIterator<'a, 'b>),
    Empty,
}

impl<'a, 'b> Iterator for MetadataIter<'a, 'b> {
    type Item = RecordsItem<'a, 'b>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            MetadataIter::Tree(iter) => iter.next(),
            MetadataIter::Fst(iter) => iter.next(),
            MetadataIter::Empty => None,
        }
    }
}

pub struct FindRecord<'a, 'b> {
    meta: &'a BoxMetadata<'a>,
    query: VecDeque<&'b str>,
    entries: &'a [RecordIndex],
}

impl<'a, 'b> FindRecord<'a, 'b> {
    pub(crate) fn new(
        meta: &'a BoxMetadata<'a>,
        query: VecDeque<&'b str>,
        entries: &'a [RecordIndex],
    ) -> FindRecord<'a, 'b> {
        tracing::trace!("FindRecord query: {:?}", query);

        FindRecord {
            meta,
            query,
            entries,
        }
    }
}

impl<'a, 'b> Iterator for FindRecord<'a, 'b> {
    type Item = RecordIndex;

    fn next(&mut self) -> Option<Self::Item> {
        let candidate_name = self.query.pop_front()?;

        tracing::trace!("candidate_name: {}", candidate_name);

        let result = self
            .entries
            .iter()
            .map(|index| (*index, self.meta.record(*index).unwrap()))
            .find(|x| x.1.name() == candidate_name);

        match result {
            Some(v) => {
                tracing::trace!("{:?}", v);
                if self.query.is_empty() {
                    Some(v.0)
                } else if let Record::Directory(record) = v.1 {
                    let mut tmp = VecDeque::new();
                    std::mem::swap(&mut self.query, &mut tmp);
                    let result = FindRecord::new(self.meta, tmp, &record.entries).next();
                    tracing::trace!("FindRecord result: {:?}", &result);
                    result
                } else {
                    None
                }
            }
            None => None,
        }
    }
}

impl<'a> BoxMetadata<'a> {
    /// Iterate over all records in the archive.
    ///
    /// For new archives with FST, this iterates FST entries lazily.
    /// For old archives without FST, this uses tree traversal.
    pub fn iter(&self) -> MetadataIter<'_, 'a> {
        if !self.root.is_empty() {
            MetadataIter::Tree(Records::new(self, &self.root, None))
        } else if let Some(fst) = &self.fst {
            MetadataIter::Fst(FstRecordsIterator {
                meta: self,
                inner: fst.prefix_iter(&[]),
            })
        } else {
            MetadataIter::Empty
        }
    }

    #[inline(always)]
    pub fn root_records(&self) -> Vec<(RecordIndex, &Record<'a>)> {
        // Use tree if populated (old archives or during writing)
        if !self.root.is_empty() {
            return self
                .root
                .iter()
                .copied()
                .filter_map(|x| self.record(x).map(|r| (x, r)))
                .collect();
        }

        // Use FST for new archives (empty root on disk)
        if let Some(fst) = &self.fst {
            return fst
                .prefix_iter(&[])
                .filter(|(key, _)| !key.contains(&0x1f)) // No separator = root level
                .filter_map(|(_, idx)| {
                    let index = RecordIndex::new(idx).ok()?;
                    self.record(index).map(|r| (index, r))
                })
                .collect();
        }

        Vec::new()
    }

    #[inline(always)]
    pub fn dir_records(&self, dir_record: &DirectoryRecord<'_>) -> Vec<(RecordIndex, &Record<'a>)> {
        // Use tree if populated (old archives or during writing)
        if !dir_record.entries.is_empty() {
            return dir_record
                .entries
                .iter()
                .copied()
                .filter_map(|x| self.record(x).map(|r| (x, r)))
                .collect();
        }

        // For new archives with FST, we need the directory's path
        // This requires the caller to use dir_records_by_index instead
        Vec::new()
    }

    /// Get directory children by record index (works with FST).
    #[inline(always)]
    pub fn dir_records_by_index(&self, dir_index: RecordIndex) -> Vec<(RecordIndex, &Record<'a>)> {
        // First check if tree is populated
        if let Some(Record::Directory(dir)) = self.record(dir_index)
            && !dir.entries.is_empty()
        {
            return dir
                .entries
                .iter()
                .copied()
                .filter_map(|x| self.record(x).map(|r| (x, r)))
                .collect();
        }

        // Use FST - need to find the directory's path first
        let Some(fst) = &self.fst else {
            return Vec::new();
        };

        // Find the directory's path by searching FST for this index
        let dir_path: Option<Vec<u8>> = fst
            .prefix_iter(&[])
            .find(|(_, idx)| *idx == dir_index.get())
            .map(|(path, _)| path);

        let Some(mut prefix) = dir_path else {
            return Vec::new();
        };
        prefix.push(0x1f); // Add separator for children

        // Find all direct children (paths starting with prefix, no more separators)
        fst.prefix_iter(&prefix)
            .filter(|(key, _)| !key[prefix.len()..].contains(&0x1f))
            .filter_map(|(_, idx)| {
                let index = RecordIndex::new(idx).ok()?;
                self.record(index).map(|r| (index, r))
            })
            .collect()
    }

    #[inline(always)]
    pub fn index(&self, path: &BoxPath<'_>) -> Option<RecordIndex> {
        // Try FST first (O(key_length))
        if let Some(fst) = &self.fst {
            let path_bytes: &[u8] = path.as_ref();
            if let Some(value) = fst.get(path_bytes) {
                return std::num::NonZeroU64::new(value).map(RecordIndex);
            }
        }
        // Fall back to directory traversal (O(depth × entries))
        FindRecord::new(self, path.iter().collect(), &self.root).next()
    }

    #[inline(always)]
    pub fn record(&self, index: RecordIndex) -> Option<&Record<'a>> {
        self.records.get(index.get() as usize - 1)
    }

    #[inline(always)]
    pub fn record_mut(&mut self, index: RecordIndex) -> Option<&mut Record<'a>> {
        self.records.get_mut(index.get() as usize - 1)
    }

    #[inline(always)]
    pub fn insert_record(&mut self, record: Record<'a>) -> RecordIndex {
        self.records.push(record);
        RecordIndex::new(self.records.len() as u64).unwrap()
    }

    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, path: &BoxPath<'_>, key: S) -> Option<&[u8]> {
        let key = self.attr_key(key.as_ref())?;

        if let Some(record) = self.index(path).and_then(|x| self.record(x)) {
            record.attrs().get(&key).map(|x| &**x)
        } else {
            None
        }
    }

    pub fn file_attrs(&self) -> BTreeMap<&str, AttrValue<'_>> {
        let mut map = BTreeMap::new();

        for (idx, attr_key) in self.attr_keys.iter().enumerate() {
            if let Some(v) = self.attrs.get(&idx) {
                let value = self.parse_attr_value(v, attr_key.attr_type);
                map.insert(attr_key.name.as_str(), value);
            }
        }

        map
    }

    /// Parse raw bytes into AttrValue based on the stored type
    pub fn parse_attr_value<'b>(&self, v: &'b [u8], attr_type: AttrType) -> AttrValue<'b> {
        match attr_type {
            AttrType::Bytes => AttrValue::Bytes(v),
            AttrType::String => std::str::from_utf8(v)
                .map(AttrValue::String)
                .unwrap_or(AttrValue::Bytes(v)),
            AttrType::Json => std::str::from_utf8(v)
                .map(|s| {
                    serde_json::from_str(s)
                        .map(AttrValue::Json)
                        .unwrap_or(AttrValue::String(s))
                })
                .unwrap_or(AttrValue::Bytes(v)),
            AttrType::U8 => {
                if v.len() == 1 {
                    AttrValue::U8(v[0])
                } else {
                    AttrValue::Bytes(v)
                }
            }
            AttrType::Vi32 => {
                let (val, _) = fastvint::decode_vi32_slice(v);
                AttrValue::Vi32(val)
            }
            AttrType::Vu32 => fastvint::ReadVintExt::read_vu64(&mut std::io::Cursor::new(v))
                .map(|val| AttrValue::Vu32(val as u32))
                .unwrap_or(AttrValue::Bytes(v)),
            AttrType::Vi64 => {
                let (val, _) = fastvint::decode_vi64_slice(v);
                AttrValue::Vi64(val)
            }
            AttrType::Vu64 => fastvint::ReadVintExt::read_vu64(&mut std::io::Cursor::new(v))
                .map(AttrValue::Vu64)
                .unwrap_or(AttrValue::Bytes(v)),
            AttrType::U128 => {
                if v.len() == 16 {
                    v.as_chunks()
                        .0
                        .get(0)
                        .map(|arr| AttrValue::U128(arr))
                        .unwrap()
                } else {
                    AttrValue::Bytes(v)
                }
            }
            AttrType::U256 => {
                if v.len() == 32 {
                    v.as_chunks()
                        .0
                        .get(0)
                        .map(|arr| AttrValue::U256(arr))
                        .unwrap()
                } else {
                    AttrValue::Bytes(v)
                }
            }
            AttrType::DateTime => {
                let (val, _) = fastvint::decode_vi64_slice(v);
                AttrValue::DateTime(val)
            }
        }
    }

    #[inline(always)]
    pub fn file_attr<S: AsRef<str>>(&self, key: S) -> Option<&[u8]> {
        let key = self.attr_key(key.as_ref())?;

        self.attrs.get(&key).map(|v| &**v)
    }

    /// Get all attribute keys used in this archive (both file-level and record-level).
    pub fn attr_keys(&self) -> Vec<&str> {
        self.attr_keys.iter().map(|k| k.name.as_str()).collect()
    }

    /// Get all attribute keys with their indices.
    pub fn attr_keys_with_indices(&self) -> Vec<(usize, &str)> {
        self.attr_keys
            .iter()
            .enumerate()
            .map(|(idx, k)| (idx, k.name.as_str()))
            .collect()
    }

    /// Get all attribute keys with their types.
    pub fn attr_keys_with_types(&self) -> &[AttrKey] {
        &self.attr_keys
    }

    /// O(n) lookup of attribute key index.
    #[inline(always)]
    pub fn attr_key(&self, key: &str) -> Option<usize> {
        self.attr_keys.iter().position(|k| k.name == key)
    }

    /// O(n) lookup or insert of attribute key index with type.
    /// Returns an error if the key exists with a different type.
    #[inline(always)]
    pub fn attr_key_or_create(&mut self, key: &str, attr_type: AttrType) -> std::io::Result<usize> {
        if let Some(idx) = self.attr_key(key) {
            let existing = &self.attr_keys[idx];
            if existing.attr_type != attr_type {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "attribute '{}' already exists with type {:?}, cannot set as {:?}",
                        key, existing.attr_type, attr_type
                    ),
                ));
            }
            Ok(idx)
        } else {
            let idx = self.attr_keys.len();
            self.attr_keys.push(AttrKey {
                name: key.to_string(),
                attr_type,
            });
            Ok(idx)
        }
    }

    /// Get the raw archive-level attrs map (for debugging).
    pub fn raw_attrs(&self) -> &AttrMap {
        &self.attrs
    }

    /// Get the Zstd dictionary if present.
    /// When present, all Zstd-compressed content should use this dictionary for decompression.
    pub fn dictionary(&self) -> Option<&[u8]> {
        self.dictionary.as_deref()
    }

    /// Find the path for a given record index.
    ///
    /// This is O(n) for tree traversal, O(n) for FST iteration.
    /// Used primarily for resolving symlink targets during extraction.
    pub fn path_for_index(&self, target: RecordIndex) -> Option<BoxPath<'static>> {
        // Try FST first if available
        if let Some(fst) = &self.fst {
            for (path_bytes, idx) in fst.prefix_iter(&[]) {
                if idx == target.get()
                    && let Ok(path_str) = std::str::from_utf8(&path_bytes)
                {
                    return Some(BoxPath(Cow::Owned(path_str.to_string())));
                }
            }
        }

        // Fall back to tree traversal
        for item in self.iter() {
            if item.index == target {
                return Some(item.path);
            }
        }

        None
    }

    /// Resolve an attribute key index to its string name.
    pub fn attr_key_name(&self, idx: usize) -> Option<&str> {
        self.attr_keys.get(idx).map(|k| k.name.as_str())
    }

    /// Resolve an attribute key index to its type.
    pub fn attr_key_type(&self, idx: usize) -> Option<AttrType> {
        self.attr_keys.get(idx).map(|k| k.attr_type)
    }

    /// Find the block containing a logical offset within a chunked file.
    ///
    /// Returns the physical offset of the block that contains the given logical offset,
    /// along with the block's logical start offset.
    ///
    /// This uses a predecessor query on the block FST to find the largest key
    /// <= (record_index, logical_offset).
    pub fn find_block(&self, record_index: RecordIndex, logical_offset: u64) -> Option<(u64, u64)> {
        let block_fst = self.block_fst.as_ref()?;

        // Build the query key: record_index (BE) ++ logical_offset (BE)
        let mut key = [0u8; 16];
        key[..8].copy_from_slice(&record_index.get().to_be_bytes());
        key[8..].copy_from_slice(&logical_offset.to_be_bytes());

        // Find the predecessor (largest key <= query key)
        // FST iteration gives us keys in sorted order, so we find the last key <= our query
        let mut result: Option<(u64, u64)> = None;
        for (k, physical_offset) in block_fst.prefix_iter(&key[..8]) {
            // Only consider entries for this record
            if k.len() != 16 {
                continue;
            }
            let block_logical_offset = u64::from_be_bytes(k[8..16].try_into().ok()?);
            if block_logical_offset <= logical_offset {
                result = Some((physical_offset, block_logical_offset));
            } else {
                // Keys are sorted, so we can stop once we pass our target
                break;
            }
        }
        result
    }

    /// Iterate all blocks for a chunked file record.
    ///
    /// Returns (logical_offset, physical_offset) pairs in order.
    /// Returns an empty Vec if no block FST exists.
    pub fn blocks_for_record(&self, record_index: RecordIndex) -> Vec<(u64, u64)> {
        let Some(block_fst) = &self.block_fst else {
            return Vec::new();
        };

        // Build the prefix key: just the record_index (first 8 bytes)
        let mut prefix = [0u8; 8];
        prefix.copy_from_slice(&record_index.get().to_be_bytes());

        let mut blocks = Vec::new();
        for (key, physical_offset) in block_fst.prefix_iter(&prefix) {
            if key.len() == 16 {
                let logical_offset = u64::from_be_bytes(key[8..16].try_into().unwrap());
                blocks.push((logical_offset, physical_offset));
            }
        }
        blocks
    }

    /// Find the next block after a given logical offset.
    ///
    /// Returns the (logical_offset, physical_offset) of the next block, or None if
    /// there are no more blocks after the given offset.
    pub fn next_block(
        &self,
        record_index: RecordIndex,
        current_logical_offset: u64,
    ) -> Option<(u64, u64)> {
        let block_fst = self.block_fst.as_ref()?;

        // Build the prefix key: just the record_index (first 8 bytes)
        let mut prefix = [0u8; 8];
        prefix.copy_from_slice(&record_index.get().to_be_bytes());

        // Find the first block with logical_offset > current_logical_offset
        for (key, physical_offset) in block_fst.prefix_iter(&prefix) {
            if key.len() != 16 {
                continue;
            }
            let block_logical_offset = u64::from_be_bytes(key[8..16].try_into().ok()?);
            if block_logical_offset > current_logical_offset {
                return Some((block_logical_offset, physical_offset));
            }
        }
        None
    }
}

#[derive(Debug)]
pub enum AttrValue<'a> {
    Bytes(&'a [u8]),
    String(&'a str),
    Json(serde_json::Value),
    U8(u8),
    Vi32(i32),
    Vu32(u32),
    Vi64(i64),
    Vu64(u64),
    U128(&'a [u8; 16]),
    U256(&'a [u8; 32]),
    /// Minutes since Box epoch (2026-01-01 00:00:00 UTC)
    DateTime(i64),
}

impl AttrValue<'_> {
    /// Get the AttrType for this value
    pub fn attr_type(&self) -> AttrType {
        match self {
            AttrValue::Bytes(_) => AttrType::Bytes,
            AttrValue::String(_) => AttrType::String,
            AttrValue::Json(_) => AttrType::Json,
            AttrValue::U8(_) => AttrType::U8,
            AttrValue::Vi32(_) => AttrType::Vi32,
            AttrValue::Vu32(_) => AttrType::Vu32,
            AttrValue::Vi64(_) => AttrType::Vi64,
            AttrValue::Vu64(_) => AttrType::Vu64,
            AttrValue::U128(_) => AttrType::U128,
            AttrValue::U256(_) => AttrType::U256,
            AttrValue::DateTime(_) => AttrType::DateTime,
        }
    }
}

impl From<String> for AttrValue<'static> {
    fn from(s: String) -> Self {
        AttrValue::String(Box::leak(s.into_boxed_str()))
    }
}

impl<'a> From<&'a str> for AttrValue<'a> {
    fn from(s: &'a str) -> Self {
        AttrValue::String(s)
    }
}

impl From<Vec<u8>> for AttrValue<'static> {
    fn from(v: Vec<u8>) -> Self {
        AttrValue::Bytes(Box::leak(v.into_boxed_slice()))
    }
}

impl<'a> From<&'a [u8]> for AttrValue<'a> {
    fn from(v: &'a [u8]) -> Self {
        AttrValue::Bytes(v)
    }
}

impl From<serde_json::Value> for AttrValue<'static> {
    fn from(v: serde_json::Value) -> Self {
        AttrValue::Json(v)
    }
}

impl From<u8> for AttrValue<'static> {
    fn from(v: u8) -> Self {
        AttrValue::U8(v)
    }
}

impl From<i32> for AttrValue<'static> {
    fn from(v: i32) -> Self {
        AttrValue::Vi32(v)
    }
}

impl From<u32> for AttrValue<'static> {
    fn from(v: u32) -> Self {
        AttrValue::Vu32(v)
    }
}

impl From<i64> for AttrValue<'static> {
    fn from(v: i64) -> Self {
        AttrValue::Vi64(v)
    }
}

impl From<u64> for AttrValue<'static> {
    fn from(v: u64) -> Self {
        AttrValue::Vu64(v)
    }
}

impl AttrValue<'_> {
    pub fn as_raw_bytes(&self) -> Cow<'_, [u8]> {
        match self {
            AttrValue::Bytes(x) => Cow::Borrowed(*x),
            AttrValue::String(x) => Cow::Borrowed(x.as_bytes()),
            AttrValue::Json(x) => Cow::Owned(serde_json::to_vec(x).unwrap()),
            AttrValue::U8(x) => Cow::Owned(vec![*x]),
            AttrValue::Vi32(x) => Cow::Owned(fastvint::encode_vi32(*x).bytes().to_vec()),
            AttrValue::Vu32(x) => {
                let mut buf = Vec::new();
                fastvint::WriteVintExt::write_vu64(&mut buf, *x as u64).unwrap();
                Cow::Owned(buf)
            }
            AttrValue::Vi64(x) => Cow::Owned(fastvint::encode_vi64(*x).bytes().to_vec()),
            AttrValue::Vu64(x) => {
                let mut buf = Vec::new();
                fastvint::WriteVintExt::write_vu64(&mut buf, *x).unwrap();
                Cow::Owned(buf)
            }
            AttrValue::U128(x) => Cow::Borrowed(x.as_slice()),
            AttrValue::U256(x) => Cow::Borrowed(x.as_slice()),
            AttrValue::DateTime(x) => Cow::Owned(fastvint::encode_vi64(*x).bytes().to_vec()),
        }
    }
}

impl Display for AttrValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AttrValue::Bytes(bytes) => {
                let mut bytes = bytes.iter();
                if let Some(v) = bytes.next() {
                    f.write_fmt(format_args!("{:02x}", v))?;
                }
                for b in bytes {
                    f.write_fmt(format_args!(" {:02x}", b))?;
                }
                Ok(())
            }
            AttrValue::String(v) => f.write_str(v),
            AttrValue::Json(value) => {
                let v = serde_json::to_string_pretty(value).unwrap();
                f.write_str(&v)
            }
            AttrValue::U8(v) => write!(f, "{}", v),
            AttrValue::Vi32(v) => write!(f, "{}", v),
            AttrValue::Vu32(v) => write!(f, "{}", v),
            AttrValue::Vi64(v) => write!(f, "{}", v),
            AttrValue::Vu64(v) => write!(f, "{}", v),
            AttrValue::U128(bytes) => {
                for b in bytes.iter() {
                    f.write_fmt(format_args!("{:02x}", b))?;
                }
                Ok(())
            }
            AttrValue::U256(bytes) => {
                for b in bytes.iter() {
                    f.write_fmt(format_args!("{:02x}", b))?;
                }
                Ok(())
            }
            AttrValue::DateTime(v) => write!(f, "{} minutes since epoch", v),
        }
    }
}
