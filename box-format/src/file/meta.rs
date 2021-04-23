use super::AttrMap;
use crate::file::Inode;
use crate::path::BoxPath;
use crate::record::DirectoryRecord;
use crate::Record;
use std::{
    borrow::Cow,
    collections::{BTreeMap, VecDeque},
    fmt::Display,
};

#[derive(Debug, Default)]
pub struct BoxMetadata {
    /// Root "directory" keyed by inodes
    pub(crate) root: Vec<Inode>,

    /// Keyed by inode index (offset by -1). This means if an `Inode` has the value 1, its index in this vector is 0.
    /// This is to provide compatibility with platforms such as Linux, and allow for error checking a box file.
    pub(crate) inodes: Vec<Record>,

    /// The index of the attribute key is its interned identifier throughout this file.
    pub(crate) attr_keys: Vec<String>,

    /// The global attributes that apply to this entire box file.
    pub(crate) attrs: AttrMap,

    /// The index of paths to files.
    pub(crate) index: Option<pathtrie::fst::Fst<u64>>,
}

pub struct Records<'a> {
    meta: &'a BoxMetadata,
    inodes: &'a [Inode],
    base_path: Option<BoxPath>,
    cur_inode: usize,
    cur_dir: Option<Box<Records<'a>>>,
}

impl<'a> Records<'a> {
    pub(crate) fn new(
        meta: &'a BoxMetadata,
        inodes: &'a [Inode],
        base_path: Option<BoxPath>,
    ) -> Records<'a> {
        Records {
            meta,
            inodes,
            base_path,
            cur_inode: 0,
            cur_dir: None,
        }
    }
}

#[non_exhaustive]
pub struct RecordsItem<'a> {
    pub(crate) inode: Inode,
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
        let inode = match self.inodes.get(self.cur_inode) {
            Some(i) => *i,
            None => return None,
        };

        let record = match self.meta.record(inode) {
            Some(v) => v,
            None => return None,
        };

        let base_path = match self.base_path.as_ref() {
            Some(x) => x.join_unchecked(&record.name()),
            None => BoxPath(record.name().to_string()),
        };

        if let Record::Directory(record) = record {
            self.cur_dir = Some(Box::new(Records::new(
                self.meta,
                &*record.inodes,
                Some(base_path.clone()),
            )));
        }

        self.cur_inode += 1;
        Some(RecordsItem {
            inode,
            path: base_path,
            record,
        })
    }
}

pub struct FindRecord<'a> {
    meta: &'a BoxMetadata,
    query: VecDeque<String>,
    inodes: &'a [Inode],
}

impl<'a> FindRecord<'a> {
    pub(crate) fn new(
        meta: &'a BoxMetadata,
        query: VecDeque<String>,
        inodes: &'a [Inode],
    ) -> FindRecord<'a> {
        log::debug!("FindRecord query: {:?}", query);

        FindRecord {
            meta,
            query,
            inodes,
        }
    }
}

impl<'a> Iterator for FindRecord<'a> {
    type Item = Inode;

    fn next(&mut self) -> Option<Self::Item> {
        let candidate_name = match self.query.pop_front() {
            Some(v) => v,
            None => return None,
        };

        log::debug!("candidate_name: {}", &candidate_name);

        let result = self
            .inodes
            .iter()
            .map(|inode| (*inode, self.meta.record(*inode).unwrap()))
            .find(|x| x.1.name() == candidate_name);

        match result {
            Some(v) => {
                log::debug!("{:?}", v);
                if self.query.is_empty() {
                    Some(v.0)
                } else if let Record::Directory(record) = v.1 {
                    let mut tmp = VecDeque::new();
                    std::mem::swap(&mut self.query, &mut tmp);
                    let result = FindRecord::new(self.meta, tmp, &*record.inodes).next();
                    log::debug!("FindRecord result: {:?}", &result);
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
    pub fn iter(&self) -> Records {
        Records::new(self, &*self.root, None)
    }

    #[inline(always)]
    pub fn root_records(&self) -> Vec<(Inode, &Record)> {
        self.root
            .iter()
            .copied()
            .filter_map(|x| self.record(x).map(|r| (x, r)))
            .collect()
    }

    #[inline(always)]
    pub fn records(&self, dir_record: &DirectoryRecord) -> Vec<(Inode, &Record)> {
        dir_record
            .inodes
            .iter()
            .copied()
            .filter_map(|x| self.record(x).map(|r| (x, r)))
            .collect()
    }

    #[inline(always)]
    pub fn inode(&self, path: &BoxPath) -> Option<Inode> {
        if let Some(inode) = self.index.as_ref().and_then(|x| x.get(path)) {
            return Inode::new(inode).ok();
        };

        FindRecord::new(self, path.iter().map(str::to_string).collect(), &*self.root).next()
    }

    #[inline(always)]
    pub fn record(&self, inode: Inode) -> Option<&Record> {
        self.inodes.get(inode.get() as usize - 1)
    }

    #[inline(always)]
    pub fn record_mut(&mut self, inode: Inode) -> Option<&mut Record> {
        self.inodes.get_mut(inode.get() as usize - 1)
    }

    #[inline(always)]
    pub fn insert_record(&mut self, record: Record) -> Inode {
        self.inodes.push(record);
        Inode::new(self.inodes.len() as u64).unwrap()
    }

    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, path: &BoxPath, key: S) -> Option<&[u8]> {
        let key = self.attr_key(key.as_ref())?;

        if let Some(record) = self.inode(path).and_then(|x| self.record(x)) {
            record.attrs().get(&key).map(|x| &**x)
        } else {
            None
        }
    }

    pub fn file_attrs<'a>(&'a self) -> BTreeMap<&'a str, AttrValue<'a>> {
        let mut map = BTreeMap::new();

        for key in self.attr_keys.iter() {
            let k = self.attr_key(key).unwrap();
            if let Some(v) = self.attrs.get(&k) {
                let value = std::str::from_utf8(v)
                    .and_then(|v| {
                        Ok(serde_json::from_str(v)
                            .map(AttrValue::Json)
                            .unwrap_or_else(|_| AttrValue::String(v)))
                    })
                    .unwrap_or_else(|_| AttrValue::Bytes(v));
                map.insert(&**key, value);
            }
        }

        map
    }

    #[inline(always)]
    pub fn file_attr<S: AsRef<str>>(&self, key: S) -> Option<&Vec<u8>> {
        let key = self.attr_key(key.as_ref())?;

        self.attrs.get(&key)
    }

    #[inline(always)]
    pub fn attr_key(&self, key: &str) -> Option<usize> {
        self.attr_keys.iter().position(|r| r == key)
    }

    #[inline(always)]
    pub fn attr_key_or_create(&mut self, key: &str) -> usize {
        match self.attr_keys.iter().position(|r| r == key) {
            Some(v) => v,
            None => {
                let len = self.attr_keys.len();
                self.attr_keys.push(key.to_string());
                len
            }
        }
    }
}

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
