use super::AttrMap;
use crate::file::Inode;
use crate::path::BoxPath;
use crate::Record;

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
    pub(crate) index: Option<fst::Map>,
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

        let base_path = self
            .base_path
            .as_ref()
            .map(|x| x.join(&record.name()).unwrap())
            .or_else(|| BoxPath::new(&record.name()).ok())
            .unwrap();

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

use std::collections::VecDeque;

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

        let result = self
            .inodes
            .iter()
            .map(|inode| (*inode, self.meta.record(*inode).unwrap()))
            .find(|x| x.1.name() == candidate_name);

        match result {
            Some(v) => {
                if self.query.is_empty() {
                    return Some(v.0);
                } else {
                    let mut tmp = VecDeque::new();
                    std::mem::swap(&mut self.query, &mut tmp);
                    FindRecord::new(self.meta, tmp, self.inodes).next()
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
