use crate::file::Inode;
use crate::Record;
use super::AttrMap;

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
    // pub(crate) index: fst::Map,
}

impl BoxMetadata {
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
