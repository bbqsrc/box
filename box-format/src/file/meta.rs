use super::AttrMap;
use crate::Record;
use crate::file::RecordIndex;
use crate::path::BoxPath;
use crate::record::DirectoryRecord;
use std::{
    borrow::Cow,
    collections::{BTreeMap, VecDeque},
    fmt::Display,
};
use string_interner::{DefaultStringInterner, Symbol};

pub struct BoxMetadata {
    /// Root "directory" keyed by record indices
    pub(crate) root: Vec<RecordIndex>,

    /// Keyed by record index (offset by -1). This means if a `RecordIndex` has the value 1, its index in this vector is 0.
    /// This is to provide compatibility with platforms such as Linux, and allow for error checking a box file.
    pub(crate) records: Vec<Record>,

    /// Interned attribute keys. Symbol::to_usize() gives the key index.
    pub(crate) attr_keys: DefaultStringInterner,

    /// The global attributes that apply to this entire box file.
    pub(crate) attrs: AttrMap,
}

impl std::fmt::Debug for BoxMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoxMetadata")
            .field("root", &self.root)
            .field("records", &self.records)
            .field("attr_keys", &self.attr_keys.len())
            .field("attrs", &self.attrs)
            .finish()
    }
}

impl Default for BoxMetadata {
    fn default() -> Self {
        Self {
            root: Vec::new(),
            records: Vec::new(),
            attr_keys: DefaultStringInterner::default(),
            attrs: AttrMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct Records<'a> {
    meta: &'a BoxMetadata,
    entries: &'a [RecordIndex],
    base_path: Option<BoxPath>,
    cur_entry: usize,
    cur_dir: Option<Box<Records<'a>>>,
}

impl<'a> Records<'a> {
    pub(crate) fn new(
        meta: &'a BoxMetadata,
        entries: &'a [RecordIndex],
        base_path: Option<BoxPath>,
    ) -> Records<'a> {
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
pub struct RecordsItem<'a> {
    pub(crate) index: RecordIndex,
    pub path: BoxPath,
    pub record: &'a Record,
}

impl<'a> Iterator for Records<'a> {
    type Item = RecordsItem<'a>;

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
            None => BoxPath(record.name().to_string()),
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

#[derive(Debug)]
pub struct FindRecord<'a> {
    meta: &'a BoxMetadata,
    query: VecDeque<String>,
    entries: &'a [RecordIndex],
}

impl<'a> FindRecord<'a> {
    pub(crate) fn new(
        meta: &'a BoxMetadata,
        query: VecDeque<String>,
        entries: &'a [RecordIndex],
    ) -> FindRecord<'a> {
        log::trace!("FindRecord query: {:?}", query);

        FindRecord {
            meta,
            query,
            entries,
        }
    }
}

impl<'a> Iterator for FindRecord<'a> {
    type Item = RecordIndex;

    fn next(&mut self) -> Option<Self::Item> {
        let candidate_name = self.query.pop_front()?;

        log::trace!("candidate_name: {}", &candidate_name);

        let result = self
            .entries
            .iter()
            .map(|index| (*index, self.meta.record(*index).unwrap()))
            .find(|x| x.1.name() == candidate_name);

        match result {
            Some(v) => {
                log::trace!("{:?}", v);
                if self.query.is_empty() {
                    Some(v.0)
                } else if let Record::Directory(record) = v.1 {
                    let mut tmp = VecDeque::new();
                    std::mem::swap(&mut self.query, &mut tmp);
                    let result = FindRecord::new(self.meta, tmp, &record.entries).next();
                    log::trace!("FindRecord result: {:?}", &result);
                    result
                } else {
                    None
                }
            }
            None => None,
        }
    }
}

impl BoxMetadata {
    #[inline(always)]
    pub fn iter(&self) -> Records<'_> {
        Records::new(self, &self.root, None)
    }

    #[inline(always)]
    pub fn root_records(&self) -> Vec<(RecordIndex, &Record)> {
        self.root
            .iter()
            .copied()
            .filter_map(|x| self.record(x).map(|r| (x, r)))
            .collect()
    }

    #[inline(always)]
    pub fn dir_records(&self, dir_record: &DirectoryRecord) -> Vec<(RecordIndex, &Record)> {
        dir_record
            .entries
            .iter()
            .copied()
            .filter_map(|x| self.record(x).map(|r| (x, r)))
            .collect()
    }

    #[inline(always)]
    pub fn index(&self, path: &BoxPath) -> Option<RecordIndex> {
        FindRecord::new(self, path.iter().map(str::to_string).collect(), &self.root).next()
    }

    #[inline(always)]
    pub fn record(&self, index: RecordIndex) -> Option<&Record> {
        self.records.get(index.get() as usize - 1)
    }

    #[inline(always)]
    pub fn record_mut(&mut self, index: RecordIndex) -> Option<&mut Record> {
        self.records.get_mut(index.get() as usize - 1)
    }

    #[inline(always)]
    pub fn insert_record(&mut self, record: Record) -> RecordIndex {
        self.records.push(record);
        RecordIndex::new(self.records.len() as u64).unwrap()
    }

    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, path: &BoxPath, key: S) -> Option<&[u8]> {
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
                let value = std::str::from_utf8(v)
                    .map(|v| {
                        serde_json::from_str(v)
                            .map(AttrValue::Json)
                            .unwrap_or(AttrValue::String(v))
                    })
                    .unwrap_or(AttrValue::Bytes(v));
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
