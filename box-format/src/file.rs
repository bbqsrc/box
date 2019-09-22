use std::collections::HashMap;
use std::default::Default;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{prelude::*, BufReader, BufWriter, Result, SeekFrom};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use comde::Decompress;
use memmap::MmapOptions;

use crate::{
    compression::Compression,
    de::DeserializeOwned,
    header::BoxHeader,
    path::BoxPath,
    record::{DirectoryRecord, FileRecord, Record},
    ser::Serialize,
};

pub type AttrMap = HashMap<u32, Vec<u8>>;

#[derive(Debug, Default)]
pub struct BoxMetadata {
    pub(crate) records: Vec<Record>,
    pub(crate) attr_keys: Vec<String>,
    pub(crate) attrs: AttrMap,
}

impl BoxMetadata {
    pub fn records(&self) -> &[Record] {
        &*self.records
    }
}

#[derive(Debug)]
pub struct BoxFileReader {
    pub(crate) file: BufReader<File>,
    pub(crate) path: PathBuf,
    pub(crate) header: BoxHeader,
    pub(crate) meta: BoxMetadata,
}

#[derive(Debug)]
pub struct BoxFileWriter {
    pub(crate) file: BufWriter<File>,
    pub(crate) path: PathBuf,
    pub(crate) header: BoxHeader,
    pub(crate) meta: BoxMetadata,
}

impl Drop for BoxFileWriter {
    fn drop(&mut self) {
        let _ = self.finish_inner();
    }
}

#[inline(always)]
fn read_header<R: Read + Seek>(file: &mut R) -> std::io::Result<BoxHeader> {
    file.seek(SeekFrom::Start(0))?;
    BoxHeader::deserialize_owned(file)
}

#[inline(always)]
fn read_trailer<R: Read + Seek>(file: &mut R, ptr: NonZeroU64) -> std::io::Result<BoxMetadata> {
    file.seek(SeekFrom::Start(ptr.get()))?;
    BoxMetadata::deserialize_owned(file)
}

impl BoxFileWriter {
    #[inline(always)]
    fn write_header(&mut self) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        self.header.write(&mut self.file)
    }

    #[inline(always)]
    fn finish_inner(&mut self) -> std::io::Result<u64> {
        let pos = self.next_write_addr().get();
        self.header.trailer = NonZeroU64::new(pos);
        self.write_header()?;
        self.file.seek(SeekFrom::Start(pos))?;
        self.meta.write(&mut self.file)?;

        let new_pos = self.file.seek(SeekFrom::Current(0))?;
        let file = self.file.get_mut();
        file.set_len(new_pos)?;
        Ok(new_pos)
    }

    pub fn finish(mut self) -> std::io::Result<u64> {
        self.finish_inner()
    }

    #[inline(always)]
    fn next_write_addr(&self) -> NonZeroU64 {
        let offset = self
            .meta
            .records
            .iter()
            .rev()
            .find_map(|r| r.as_file())
            .map(|r| r.data.get() + r.length)
            .unwrap_or(std::mem::size_of::<BoxHeader>() as u64);

        let v = match self.header.alignment {
            None => offset,
            Some(alignment) => {
                let alignment = alignment.get();
                let diff = offset % alignment;
                if diff == 0 {
                    offset
                } else {
                    offset + (alignment - diff)
                }
            }
        };

        NonZeroU64::new(v).unwrap()
    }

    /// This will open an existing `.box` file for writing, and error if the file is not valid.
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<BoxFileWriter> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())
            .map(|mut file| {
                // Try to load the header so we can easily rewrite it when saving.
                // If header is invalid, we're not even loading a .box file.
                let (header, meta) = {
                    let mut reader = BufReader::new(&mut file);
                    let header = read_header(&mut reader)?;
                    let ptr = header.trailer.ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::Other, "no trailer found")
                    })?;
                    let meta = read_trailer(&mut reader, ptr)?;
                    (header, meta)
                };

                let f = BoxFileWriter {
                    file: BufWriter::new(file),
                    path: path.as_ref().to_path_buf().canonicalize()?,
                    header,
                    meta,
                };

                Ok(f)
            })?
    }

    /// This will create a new `.box` file for writing, and error if the file already exists.
    pub fn create<P: AsRef<Path>>(path: P) -> std::io::Result<BoxFileWriter> {
        let mut boxfile = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(path.as_ref())
            .and_then(|file| {
                Ok(BoxFileWriter {
                    file: BufWriter::new(file),
                    path: path.as_ref().to_path_buf().canonicalize()?,
                    header: BoxHeader::default(),
                    meta: BoxMetadata::default(),
                })
            })?;

        boxfile.write_header()?;

        Ok(boxfile)
    }

    /// This will create a new `.box` file for reading and writing, and error if the file already exists.
    /// Will insert byte-aligned values based on provided `alignment` value. For best results, consider a power of 2.
    pub fn create_with_alignment<P: AsRef<Path>>(
        path: P,
        alignment: NonZeroU64,
    ) -> std::io::Result<BoxFileWriter> {
        let mut boxfile = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(path.as_ref())
            .and_then(|file| {
                Ok(BoxFileWriter {
                    file: BufWriter::new(file),
                    path: path.as_ref().to_path_buf().canonicalize()?,
                    header: BoxHeader::with_alignment(alignment),
                    meta: BoxMetadata::default(),
                })
            })?;

        boxfile.write_header()?;

        Ok(boxfile)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn alignment(&self) -> Option<NonZeroU64> {
        self.header.alignment
    }

    pub fn version(&self) -> u32 {
        self.header.version
    }

    /// Will return the metadata for the `.box` if it has been provided.
    pub fn metadata(&self) -> &BoxMetadata {
        &self.meta
    }

    pub fn mkdir(&mut self, path: BoxPath, attrs: HashMap<String, Vec<u8>>) -> std::io::Result<()> {
        let attrs = attrs
            .into_iter()
            .map(|(k, v)| {
                let k = self.attr_key_for_mut(&k);
                (k, v)
            })
            .collect::<HashMap<_, _>>();

        self.meta
            .records
            .push(Record::Directory(DirectoryRecord { path, attrs }));
        Ok(())
    }

    pub fn insert<R: Read>(
        &mut self,
        compression: Compression,
        path: BoxPath,
        value: &mut R,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<&FileRecord> {
        let data = self.next_write_addr();
        let bytes = self.write_data::<R>(compression, data.get(), value)?;
        let attrs = attrs
            .into_iter()
            .map(|(k, v)| {
                let k = self.attr_key_for_mut(&k);
                (k, v)
            })
            .collect::<HashMap<_, _>>();

        // Check there isn't already a record for this path
        if self.meta.records.iter().any(|x| x.path() == &path) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "path already found",
            ));
        }

        let record = FileRecord {
            compression,
            length: bytes.write,
            decompressed_length: bytes.read,
            path,
            data,
            attrs,
        };

        self.meta.records.push(Record::File(record));

        Ok(&self.meta.records.last().unwrap().as_file().unwrap())
    }

    pub unsafe fn data(&self, record: &FileRecord) -> std::io::Result<memmap::Mmap> {
        self.read_data(record)
    }

    #[inline(always)]
    fn write_data<R: Read>(
        &mut self,
        compression: Compression,
        pos: u64,
        reader: &mut R,
    ) -> std::io::Result<comde::com::ByteCount> {
        self.file.seek(SeekFrom::Start(pos))?;
        compression.compress(&mut self.file, reader)
    }

    #[inline(always)]
    pub(crate) fn attr_key_for_mut(&mut self, key: &str) -> u32 {
        match self.meta.attr_keys.iter().position(|r| r == key) {
            Some(v) => v as u32,
            None => {
                self.meta.attr_keys.push(key.to_string());
                (self.meta.attr_keys.len() - 1) as u32
            }
        }
    }

    pub fn set_attr<S: AsRef<str>>(
        &mut self,
        path: &BoxPath,
        key: S,
        value: Vec<u8>,
    ) -> Result<()> {
        let key = self.attr_key_for_mut(key.as_ref());

        if let Some(record) = self.meta.records.iter_mut().find(|r| r.path() == path) {
            record.attrs_mut().insert(key, value);
        }

        Ok(())
    }

    #[inline(always)]
    unsafe fn read_data(&self, header: &FileRecord) -> std::io::Result<memmap::Mmap> {
        MmapOptions::new()
            .offset(header.data.get())
            .len(header.length as usize)
            .map(&self.file.get_ref())
    }
}

impl BoxFileReader {
    /// This will open an existing `.box` file for reading and writing, and error if the file is not valid.
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<BoxFileReader> {
        OpenOptions::new()
            .write(true)
            .read(true)
            .open(path.as_ref())
            .map(|mut file| {
                // Try to load the header so we can easily rewrite it when saving.
                // If header is invalid, we're not even loading a .box file.
                let (header, meta) = {
                    let mut reader = BufReader::new(&mut file);
                    let header = read_header(&mut reader)?;
                    let ptr = header.trailer.ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::Other, "no trailer found")
                    })?;
                    let meta = read_trailer(&mut reader, ptr)?;
                    (header, meta)
                };

                let f = BoxFileReader {
                    file: BufReader::new(file),
                    path: path.as_ref().to_path_buf().canonicalize()?,
                    header,
                    meta,
                };

                Ok(f)
            })?
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn alignment(&self) -> Option<NonZeroU64> {
        self.header.alignment
    }

    pub fn version(&self) -> u32 {
        self.header.version
    }

    /// Will return the metadata for the `.box` if it has been provided.
    pub fn metadata(&self) -> &BoxMetadata {
        &self.meta
    }

    pub fn decompress_value<V: Decompress>(&self, record: &FileRecord) -> std::io::Result<V> {
        let mmap = unsafe { self.read_data(record)? };
        record.compression.decompress(std::io::Cursor::new(mmap))
    }

    pub fn decompress<W: Write>(&self, record: &FileRecord, dest: W) -> std::io::Result<()> {
        let mmap = unsafe { self.read_data(record)? };
        record
            .compression
            .decompress_write(std::io::Cursor::new(mmap), dest)
    }

    pub fn attr<S: AsRef<str>>(&self, path: &BoxPath, key: S) -> Option<&Vec<u8>> {
        let key = self.attr_key_for(key.as_ref())?;

        if let Some(record) = self.meta.records.iter().find(|r| r.path() == path) {
            record.attrs().get(&key)
        } else {
            None
        }
    }

    pub unsafe fn data(&self, record: &FileRecord) -> std::io::Result<memmap::Mmap> {
        self.read_data(record)
    }

    #[inline(always)]
    pub(crate) fn attr_key_for(&self, key: &str) -> Option<u32> {
        self.meta
            .attr_keys
            .iter()
            .position(|r| r == key)
            .map(|v| v as u32)
    }

    #[inline(always)]
    unsafe fn read_data(&self, header: &FileRecord) -> std::io::Result<memmap::Mmap> {
        MmapOptions::new()
            .offset(header.data.get())
            .len(header.length as usize)
            .map(self.file.get_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn create_test_box<F: AsRef<Path>>(filename: F) {
        let _ = std::fs::remove_file(filename.as_ref());

        let mut cursor: Cursor<Vec<u8>> = Cursor::new(vec![]);
        let data = b"hello\0\0\0";

        let mut header = BoxHeader::default();
        let mut trailer = BoxMetadata::default();
        trailer.records.push(Record::File(FileRecord {
            compression: Compression::Stored,
            length: data.len() as u64,
            decompressed_length: data.len() as u64,
            data: NonZeroU64::new(std::mem::size_of::<BoxHeader>() as u64).unwrap(),
            path: BoxPath::new("hello.txt").unwrap(),
            attrs: HashMap::new(),
        }));

        header.trailer = NonZeroU64::new(std::mem::size_of::<BoxHeader>() as u64 + 8);

        header.write(&mut cursor).unwrap();
        cursor.write_all(data).unwrap();
        trailer.write(&mut cursor).unwrap();

        let mut f = File::create(filename.as_ref()).unwrap();
        f.write_all(&*cursor.get_ref()).unwrap();
    }

    #[test]
    fn create_box_file() {
        create_test_box("./smoketest.box");
    }

    #[test]
    fn read_garbage() {
        let filename = "./read_garbage.box";
        create_test_box(&filename);

        let bf = BoxFileReader::open(&filename).unwrap();
        let trailer = bf.metadata();
        println!("{:?}", bf.header);
        println!("{:?}", &trailer);
        let file_data = unsafe {
            bf.read_data(&trailer.records[0].as_file().unwrap())
                .unwrap()
        };
        println!("{:?}", &*file_data);
        assert_eq!(&*file_data, b"hello\0\0\0")
    }

    #[test]
    fn create_garbage() {
        let filename = "./create_garbage.box";
        let _ = std::fs::remove_file(&filename);
        let bf = BoxFileWriter::create(&filename).expect("Mah box");
        bf.finish().unwrap();
    }

    fn insert_impl<F>(filename: &str, f: F)
    where
        F: Fn(&str) -> BoxFileWriter,
    {
        let _ = std::fs::remove_file(&filename);
        let v =
            "This, this, this, this, this is a compressable string string string string string.\n"
                .to_string();

        {
            use std::time::SystemTime;
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .to_le_bytes();

            let mut bf = f(filename);

            let mut dir_attrs = HashMap::new();
            dir_attrs.insert("created".into(), now.to_vec());
            dir_attrs.insert("unix.acl".into(), 0o755u16.to_le_bytes().to_vec());

            let mut attrs = HashMap::new();
            attrs.insert("created".into(), now.to_vec());
            attrs.insert("unix.acl".into(), 0o644u16.to_le_bytes().to_vec());

            bf.mkdir(BoxPath::new("test").unwrap(), dir_attrs).unwrap();

            bf.insert(
                Compression::Zstd,
                BoxPath::new("test/string.txt").unwrap(),
                &mut std::io::Cursor::new(v.clone()),
                attrs.clone(),
            )
            .unwrap();
            bf.insert(
                Compression::Deflate,
                BoxPath::new("test/string2.txt").unwrap(),
                &mut std::io::Cursor::new(v.clone()),
                attrs.clone(),
            )
            .unwrap();
            println!("{:?}", &bf);
            bf.finish().unwrap();
        }

        let bf = BoxFileReader::open(&filename).expect("Mah box");
        println!("{:#?}", &bf);

        assert_eq!(
            v,
            bf.decompress_value::<String>(&bf.meta.records[1].as_file().unwrap())
                .unwrap()
        );
        assert_eq!(
            v,
            bf.decompress_value::<String>(&bf.meta.records[2].as_file().unwrap())
                .unwrap()
        );
    }

    #[test]
    fn insert() {
        insert_impl("./insert_garbage.box", |n| {
            BoxFileWriter::create(n).unwrap()
        });
        insert_impl("./insert_garbage_align8.box", |n| {
            BoxFileWriter::create_with_alignment(n, NonZeroU64::new(8).unwrap()).unwrap()
        });
        insert_impl("./insert_garbage_align7.box", |n| {
            BoxFileWriter::create_with_alignment(n, NonZeroU64::new(7).unwrap()).unwrap()
        });
    }
}
