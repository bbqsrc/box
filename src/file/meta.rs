use std::{
    borrow::Cow,
    collections::{BTreeMap, VecDeque},
    fmt::Display,
};

use string_interner::{DefaultStringInterner, Symbol};

use super::AttrMap;
use crate::Record;
use crate::file::RecordIndex;
use crate::path::BoxPath;
use crate::record::DirectoryRecord;

#[derive(Default)]
pub struct BoxMetadata<'a> {
    /// Root "directory" keyed by record indices
    pub(crate) root: Vec<RecordIndex>,

    /// Keyed by record index (offset by -1). This means if a `RecordIndex` has the value 1, its index in this vector is 0.
    /// This is to provide compatibility with platforms such as Linux, and allow for error checking a box file.
    pub(crate) records: Vec<Record<'a>>,

    /// Interned attribute keys. Symbol::to_usize() gives the key index.
    pub(crate) attr_keys: DefaultStringInterner,

    /// The global attributes that apply to this entire box file.
    pub(crate) attrs: AttrMap,

    /// Parsed FST for O(key_length) path lookups and iteration.
    /// None for old archives without FST support.
    pub(crate) fst: Option<box_fst::Fst<Cow<'a, [u8]>>>,
}

impl std::fmt::Debug for BoxMetadata<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoxMetadata")
            .field("root", &self.root)
            .field("records", &self.records)
            .field("attr_keys", &self.attr_keys.len())
            .field("attrs", &self.attrs)
            .field("fst", &self.fst.as_ref().map(|f| f.len()))
            .finish()
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

        for (sym, key) in self.attr_keys.iter() {
            let k = sym.to_usize();
            if let Some(v) = self.attrs.get(&k) {
                // Known binary attribute types - don't try to interpret as string
                let is_binary_attr = matches!(
                    key,
                    "unix.mode"
                        | "unix.uid"
                        | "unix.gid"
                        | "created"
                        | "modified"
                        | "accessed"
                        | "blake3"
                );

                let value = if is_binary_attr {
                    AttrValue::Bytes(v)
                } else {
                    std::str::from_utf8(v)
                        .map(|v| {
                            serde_json::from_str(v)
                                .map(AttrValue::Json)
                                .unwrap_or(AttrValue::String(v))
                        })
                        .unwrap_or(AttrValue::Bytes(v))
                };
                map.insert(key, value);
            }
        }

        map
    }

    #[inline(always)]
    pub fn file_attr<S: AsRef<str>>(&self, key: S) -> Option<&Vec<u8>> {
        let key = self.attr_key(key.as_ref())?;

        self.attrs.get(&key)
    }

    /// Get all attribute keys used in this archive (both file-level and record-level).
    pub fn attr_keys(&self) -> Vec<&str> {
        self.attr_keys.iter().map(|(_, key)| key).collect()
    }

    /// Get all attribute keys with their indices.
    pub fn attr_keys_with_indices(&self) -> Vec<(usize, &str)> {
        use string_interner::Symbol;
        self.attr_keys
            .iter()
            .map(|(sym, key)| (sym.to_usize(), key))
            .collect()
    }

    /// O(1) lookup of attribute key index.
    #[inline(always)]
    pub fn attr_key(&self, key: &str) -> Option<usize> {
        self.attr_keys.get(key).map(|sym| sym.to_usize())
    }

    /// O(1) lookup or insert of attribute key index.
    #[inline(always)]
    pub fn attr_key_or_create(&mut self, key: &str) -> usize {
        self.attr_keys.get_or_intern(key).to_usize()
    }

    /// Get the raw archive-level attrs map (for debugging).
    pub fn raw_attrs(&self) -> &AttrMap {
        &self.attrs
    }

    /// Resolve an attribute key index to its string name.
    pub fn attr_key_name(&self, idx: usize) -> Option<&str> {
        use string_interner::Symbol;
        let sym = string_interner::DefaultSymbol::try_from_usize(idx)?;
        self.attr_keys.resolve(sym)
    }
}

#[derive(Debug)]
pub enum AttrValue<'a> {
    String(&'a str),
    Bytes(&'a [u8]),
    Json(serde_json::Value),
}

impl AttrValue<'_> {
    pub fn as_bytes(&self) -> Cow<'_, [u8]> {
        match self {
            AttrValue::String(x) => Cow::Borrowed(x.as_bytes()),
            AttrValue::Bytes(x) => Cow::Borrowed(*x),
            AttrValue::Json(x) => Cow::Owned(serde_json::to_vec(x).unwrap()),
        }
    }
}

impl Display for AttrValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AttrValue::String(v) => f.write_str(v),
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
            AttrValue::Json(value) => {
                let v = serde_json::to_string_pretty(value).unwrap();
                f.write_str(&v)
            }
        }
    }
}
