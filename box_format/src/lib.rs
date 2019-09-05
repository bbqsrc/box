use std::collections::HashMap;
use std::default::Default;
use std::fs::OpenOptions;
use std::io::{prelude::*, SeekFrom, Result};
use std::num::NonZeroU64;
use std::path::Path;

use memmap::MmapOptions;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use comde::{
    stored::{StoredCompressor, StoredDecompressor},
    zstd::{ZstdCompressor, ZstdDecompressor},
    deflate::{DeflateCompressor, DeflateDecompressor},
    Compress, Compressor, Decompress, Decompressor, ByteCount,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Compression {
    Stored,
    Deflate,
    Zstd,
    Unknown(u32)
}

impl Compression {
    pub fn id(self) -> u32 {
        use Compression::*;

        match self {
            Stored => 0x0000_0000,
            Deflate => 0x0001_0000,
            Zstd => 0x0002_0000,
            Unknown(id) => id,
        }
    }

    fn compress<W: Write + Seek, V: Compress>(&self, writer: W, data: V) -> Result<ByteCount> {
        use Compression::*;

        match self {
            Stored => StoredCompressor.compress(writer, data),
            Deflate => DeflateCompressor.compress(writer, data),
            Zstd => ZstdCompressor.compress(writer, data),
            Unknown(id) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Cannot handle compression with id {}", id)))
        }
    }

    fn decompress<R: Read, V: Decompress>(&self, reader: R) -> Result<V> {
        use Compression::*;

        match self {
            Stored => StoredDecompressor.from_reader(reader),
            Deflate => DeflateDecompressor.from_reader(reader),
            Zstd => ZstdDecompressor.from_reader(reader),
            Unknown(id) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Cannot handle decompression with id {}", id)))
        }
    }
}

#[derive(Debug)]
struct FileHeader {
    magic_bytes: [u8; 4],
    version: u32,
    alignment: Option<NonZeroU64>,
    trailer: Option<NonZeroU64>,
}

impl FileHeader {
    fn new(trailer: Option<NonZeroU64>) -> FileHeader {
        FileHeader {
            magic_bytes: *b"BOX\0",
            version: 0x0,
            alignment: None,
            trailer,
        }
    }
}

impl Default for FileHeader {
    fn default() -> Self {
        FileHeader::new(None)
    }
}

#[derive(Debug, Default)]
pub struct BoxMetadata {
    records: Vec<FileRecord>,
    // a sneaky u64 here for key-value pair length, with each of the keys and value pairs prefixed with their own u64 lengths
    attrs: HashMap<String, Vec<u8>>,
}

impl BoxMetadata {
    pub fn records(&self) -> &[FileRecord] {
        &*self.records
    }
}

#[derive(Debug)]
pub struct FileRecord {
    /// a bytestring representing the type of compression being used, always 8 bytes.
    pub compression: Compression,

    /// The exact length of the data as written, ignoring any padding.
    pub length: u64,

    /// A hint for the size of the content when decompressed. Do not trust in absolute terms.
    pub decompressed_length: u64,

    /// The position of the data in the file
    pub data: NonZeroU64,

    /// The path of the file. A path is always relative (no leading slash),
    /// always delimited by forward slashes ("`/`"), and may not contain
    /// any `.` or `..` path chunks.
    pub path: String,

    /// Optional attributes for the given paths, such as Windows or Unix ACLs, last accessed time, etc.
    pub attrs: HashMap<String, Vec<u8>>,
}

trait Serialize {
    fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()>;
}

trait DeserializeOwned {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized;
}

impl Serialize for Compression {
    fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u32::<LittleEndian>(self.id())
    }
}

impl DeserializeOwned for Compression {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let id = reader.read_u32::<LittleEndian>()?;

        use Compression::*;

        Ok(match id {
            0x0000_0000 => Stored,
            0x0001_0000 => Deflate,
            0x0002_0000 => Zstd,
            id => Unknown(id)
        })
    }
}

impl Serialize for String {
    fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u64::<LittleEndian>(self.len() as u64)?;
        writer.write_all(self.as_bytes())
    }
}

impl DeserializeOwned for String {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let len = reader.read_u64::<LittleEndian>()?;
        let mut string = String::with_capacity(len as usize);
        reader.take(len).read_to_string(&mut string)?;
        Ok(string)
    }
}

impl Serialize for Vec<u8> {
    fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u64::<LittleEndian>(self.len() as u64)?;
        writer.write_all(&*self)
    }
}

impl DeserializeOwned for Vec<u8> {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let len = reader.read_u64::<LittleEndian>()?;
        let mut buf = Vec::with_capacity(len as usize);
        reader.take(len).read_to_end(&mut buf)?;
        Ok(buf)
    }
}

impl<T: Serialize> Serialize for Vec<T> {
    fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u64::<LittleEndian>(self.len() as u64)?;
        for item in self.iter() {
            item.write(writer)?;
        }
        Ok(())
    }
}

impl<T: DeserializeOwned> DeserializeOwned for Vec<T> {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let len = reader.read_u64::<LittleEndian>()?;
        let mut buf = Vec::with_capacity(len as usize);
        for _ in 0..len {
            buf.push(T::deserialize_owned(reader)?);
        }
        Ok(buf)
    }
}

impl<K, V> Serialize for HashMap<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u64::<LittleEndian>(self.len() as u64)?;
        for (key, value) in self.iter() {
            key.write(writer)?;
            value.write(writer)?;
        }
        Ok(())
    }
}

use std::hash::Hash;

impl<K, V> DeserializeOwned for HashMap<K, V>
where
    K: Hash + Eq + DeserializeOwned,
    V: DeserializeOwned,
{
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let len = reader.read_u64::<LittleEndian>()?;
        let mut buf = HashMap::with_capacity(len as usize);
        for _ in 0..len {
            let key = K::deserialize_owned(reader)?;
            let value = V::deserialize_owned(reader)?;
            buf.insert(key, value);
        }
        Ok(buf)
    }
}

impl Serialize for FileRecord {
    fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u32::<LittleEndian>(self.compression.id())?;
        writer.write_u64::<LittleEndian>(self.length)?;
        writer.write_u64::<LittleEndian>(self.decompressed_length)?;

        self.path.write(writer)?;
        self.attrs.write(writer)?;

        writer.write_u64::<LittleEndian>(self.data.get())
    }
}

impl DeserializeOwned for FileRecord {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let compression = Compression::deserialize_owned(reader)?;
        let length = reader.read_u64::<LittleEndian>()?;
        let decompressed_length = reader.read_u64::<LittleEndian>()?;
        let path = String::deserialize_owned(reader)?;
        let attrs = HashMap::deserialize_owned(reader)?;
        let data = reader.read_u64::<LittleEndian>()?;

        Ok(FileRecord {
            compression,
            length,
            decompressed_length,
            path,
            attrs,
            data: NonZeroU64::new(data).expect("non zero"),
        })
    }
}

impl Serialize for FileHeader {
    fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(&self.magic_bytes)?;
        writer.write_u32::<LittleEndian>(self.version)?;
        writer.write_u64::<LittleEndian>(self.alignment.map(|x| x.get()).unwrap_or(0))?;
        writer.write_u64::<LittleEndian>(self.trailer.map(|x| x.get()).unwrap_or(0))
    }
}

impl DeserializeOwned for FileHeader {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let magic_bytes = reader.read_u32::<LittleEndian>()?.to_le_bytes();

        if &magic_bytes != b"BOX\0" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Magic bytes invalid",
            ));
        }

        let version = reader.read_u32::<LittleEndian>()?;
        let alignment = NonZeroU64::new(reader.read_u64::<LittleEndian>()?);
        let trailer = reader.read_u64::<LittleEndian>()?;

        Ok(FileHeader {
            magic_bytes,
            version,
            alignment,
            trailer: NonZeroU64::new(trailer),
        })
    }
}

impl Serialize for BoxMetadata {
    fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.records.write(writer)?;
        self.attrs.write(writer)
    }
}

impl DeserializeOwned for BoxMetadata {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let records = Vec::deserialize_owned(reader)?;
        let attrs = HashMap::deserialize_owned(reader)?;

        Ok(BoxMetadata {
            records,
            attrs,
        })
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

        let mut header = FileHeader::default();
        // header.alignment = NonZeroU64::new(8);
        let mut trailer = BoxMetadata::default();
        trailer.records.push(FileRecord {
            compression: Compression::Stored,
            length: data.len() as u64,
            decompressed_length: data.len() as u64,
            data: NonZeroU64::new(std::mem::size_of::<FileHeader>() as u64).unwrap(),
            path: "hello.txt".into(),
            attrs: HashMap::new(),
        });

        header.trailer = NonZeroU64::new(std::mem::size_of::<FileHeader>() as u64 + 8);

        header.write(&mut cursor).unwrap();
        cursor.write_all(data).unwrap();
        trailer.write(&mut cursor).unwrap();

        let mut f = std::fs::File::create(filename.as_ref()).unwrap();
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

        let mut bf = BoxFile::open(&filename).unwrap();
        let trailer = bf.read_trailer().unwrap();
        println!("{:?}", bf.read_header());
        println!("{:?}", &trailer);
        let file_data = bf.read_data(&trailer.records[0]).unwrap();
        println!("{:?}", &*file_data);
        assert_eq!(&*file_data, b"hello\0\0\0")
    }

    #[test]
    fn create_garbage() {
        let filename = "./create_garbage.box";
        let _ = std::fs::remove_file(&filename);
        let mut bf = BoxFile::create(&filename).expect("Mah box");
        assert!(bf.read_header().is_ok());
        assert!(bf.read_trailer().is_ok());
    }

    #[test]
    fn insert() {
        let filename = "./insert_garbage.box";
        let _ = std::fs::remove_file(&filename);
        let v = "This, this, this, this, this is a compressable string string string string string.".to_string();
        
        {
            use std::time::SystemTime;
            let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs().to_le_bytes();

            let mut bf = BoxFile::create(&filename).expect("Mah box");
            let mut attrs = HashMap::new();

            attrs.insert("created".to_string(), now.to_vec());
            attrs.insert("unix.acl".to_string(), 0o644u16.to_le_bytes().to_vec());

            bf.insert(Compression::Zstd, "test\x1fstring.txt", v.clone(), attrs.clone())
                .unwrap();
            bf.insert(Compression::Deflate, "test\x1fstring2.txt", v.clone(), attrs.clone())
                .unwrap();
            println!("{:?}", &bf);
        }

        let bf = BoxFile::open(&filename).expect("Mah box");
        println!("{:#?}", &bf);

        assert_eq!(v, bf.data::<String>(Compression::Zstd, &bf.meta.records[0]).unwrap());
        assert_eq!(v, bf.data::<String>(Compression::Deflate, &bf.meta.records[1]).unwrap());
    }
}

#[derive(Debug)]
pub struct BoxFile {
    file: std::fs::File,
    header: FileHeader,
    meta: BoxMetadata,
}

impl BoxFile {
    /// This will open an existing `.box` file for reading and writing, and error if the file is not valid.
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<BoxFile> {
        OpenOptions::new()
            .write(true)
            .read(true)
            .open(path.as_ref())
            .map(|file| {
                let mut f = BoxFile {
                    file,
                    header: FileHeader::default(),
                    meta: BoxMetadata::default(),
                };

                // Try to load the header so we can easily rewrite it when saving.
                // If header is invalid, we're not even loading a .box file.
                f.header = f.read_header()?;
                f.meta = f.read_trailer()?;

                Ok(f)
            })?
    }

    /// This will create a new `.box` file for reading and writing, and error if the file already exists.
    pub fn create<P: AsRef<Path>>(path: P) -> std::io::Result<BoxFile> {
        let mut boxfile = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(path.as_ref())
            .map(|file| BoxFile {
                file,
                header: FileHeader::default(),
                meta: BoxMetadata::default(),
            })?;

        boxfile.write_header()?;
        let pos = boxfile.write_trailer()?;
        boxfile.header.trailer = Some(NonZeroU64::new(pos).unwrap());
        boxfile.write_header()?;

        Ok(boxfile)
    }

    /// Will return the metadata for the `.box` if it has been provided.
    pub fn metadata(&self) -> &BoxMetadata {
        &self.meta
    }

    #[inline(always)]
    pub fn next_write_addr(&self) -> NonZeroU64 {
        NonZeroU64::new(
            self.meta
                .records
                .last()
                .map(|r| r.data.get() + r.length)
                .unwrap_or(std::mem::size_of::<FileHeader>() as u64),
        )
        .unwrap()
    }

    pub fn data<V: Decompress>(
        &self,
        compression: Compression,
        record: &FileRecord,
    ) -> std::io::Result<V> {
        let mmap = self.read_data(record)?;
        compression.decompress(std::io::Cursor::new(mmap))
    }

    pub fn set_attr<P: AsRef<str>, S: AsRef<str>>(&mut self, path: P, key: S, value: Vec<u8>) -> Result<()> {
        let path = path.as_ref();
        let key = key.as_ref().to_string();

        if let Some(record) = self.meta.records.iter_mut().find(|r| r.path == path) {
            record.attrs.insert(key, value);
        }

        Ok(())
    }

    pub fn insert<P: AsRef<str>, V: Compress>(
        &mut self,
        compression: Compression,
        path: P,
        value: V,
        attrs: HashMap<String, Vec<u8>>
    ) -> std::io::Result<()> {
        let path = path.as_ref().to_string();
        let data = self.next_write_addr();
        let bytes = self.write_data::<V>(compression, data.get(), value)?;

        // Check there isn't already a record for this path
        if self.meta.records.iter().any(|x| x.path == path) {
            return Err(std::io::Error::new(std::io::ErrorKind::AlreadyExists, "path already found"));
        }

        let header = FileRecord {
            compression,
            length: bytes.write,
            decompressed_length: bytes.read,
            path,
            data,
            attrs,
        };

        self.meta.records.push(header);
        let pos = self.write_trailer()?;
        self.header.trailer = Some(NonZeroU64::new(pos).unwrap());
        self.write_header()?;

        Ok(())
    }

    #[inline(always)]
    fn read_header(&mut self) -> std::io::Result<FileHeader> {
        self.file.seek(SeekFrom::Start(0))?;
        FileHeader::deserialize_owned(&mut self.file)
    }

    #[inline(always)]
    fn write_header(&mut self) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        self.header.write(&mut self.file)
    }

    #[inline(always)]
    fn write_trailer(&mut self) -> std::io::Result<u64> {
        let pos = self.next_write_addr().get();
        self.file.set_len(pos)?;
        self.file.seek(SeekFrom::Start(pos))?;
        self.meta.write(&mut self.file)?;
        Ok(pos)
    }

    #[inline(always)]
    fn write_data<V: Compress>(&mut self, compression: Compression, pos: u64, reader: V) -> std::io::Result<comde::com::ByteCount> {
        self.file.seek(SeekFrom::Start(pos))?;
        compression.compress(&mut self.file, reader)
    }

    #[inline(always)]
    fn read_trailer(&mut self) -> std::io::Result<BoxMetadata> {
        let header = self.read_header()?;
        let ptr = header
            .trailer
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "no trailer found"))?;
        self.file.seek(SeekFrom::Start(ptr.get()))?;
        BoxMetadata::deserialize_owned(&mut self.file)
    }

    #[inline(always)]
    fn read_data(&self, header: &FileRecord) -> std::io::Result<memmap::Mmap> {
        unsafe {
            MmapOptions::new()
                .offset(header.data.get())
                .len(header.length as usize)
                .map(&self.file)
        }
    }
}
