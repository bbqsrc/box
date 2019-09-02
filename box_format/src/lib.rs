
use std::collections::HashMap;
use std::default::Default;
use std::fs::OpenOptions;
use std::io::{prelude::*, SeekFrom};
use std::num::NonZeroU64;
use std::path::Path;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

#[derive(Debug)]
struct FileHeader {
    magic_bytes: [u8; 4],
    version: u32,
    trailer: Option<NonZeroU64>,
}

impl FileHeader {
    fn new(trailer: Option<NonZeroU64>) -> FileHeader {
        FileHeader {
            magic_bytes: *b"BOX\0",
            version: 0x0,
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
struct BoxMetadata {
    alignment: Option<NonZeroU64>,
    records: Vec<RecordHeader>,
    // a sneaky u64 here for key-value pair length, with each of the keys and value pairs prefixed with their own u64 lengths
    attrs: HashMap<String, Vec<u8>>,
}

#[derive(Debug)]
struct RecordHeader {
    /// a bytestring representing the type of compression being used, always 8 bytes.
    compression: u64,

    /// The exact length of the data as written, ignoring any padding.
    length: u64,

    /// A hint for the size of the content when decompressed. Do not trust in absolute terms.
    decompressed_length: u64,

    /// The position of the data in the file
    data: NonZeroU64,

    /// The path of the file. A path is always relative (no leading slash),
    /// always delimited by forward slashes ("`/`"), and may not contain
    /// any `.` or `..` path chunks.
    path: String,

    /// Optional attributes for the given paths, such as Windows or Unix ACLs, last accessed time, etc.
    attrs: HashMap<String, Vec<u8>>,
}

trait Serialize {
    fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()>;
}

trait DeserializeOwned {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized;
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

impl Serialize for RecordHeader {
    fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u64::<LittleEndian>(self.compression)?;
        writer.write_u64::<LittleEndian>(self.length)?;
        writer.write_u64::<LittleEndian>(self.decompressed_length)?;

        self.path.write(writer)?;
        self.attrs.write(writer)?;

        writer.write_u64::<LittleEndian>(self.data.get())
    }
}

impl DeserializeOwned for RecordHeader {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let compression = reader.read_u64::<LittleEndian>()?;
        let length = reader.read_u64::<LittleEndian>()?;
        let decompressed_length = reader.read_u64::<LittleEndian>()?;
        let path = String::deserialize_owned(reader)?;
        let attrs = HashMap::deserialize_owned(reader)?;
        let data = reader.read_u64::<LittleEndian>()?;

        Ok(RecordHeader {
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
        let trailer = reader.read_u64::<LittleEndian>()?;

        Ok(FileHeader {
            magic_bytes,
            version,
            trailer: NonZeroU64::new(trailer),
        })
    }
}

impl Serialize for BoxMetadata {
    fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u64::<LittleEndian>(self.alignment.map(|x| x.get()).unwrap_or(0))?;
        self.records.write(writer)?;
        self.attrs.write(writer)
    }
}

impl DeserializeOwned for BoxMetadata {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let alignment = NonZeroU64::new(reader.read_u64::<LittleEndian>()?);
        let records = Vec::deserialize_owned(reader)?;
        let attrs = HashMap::deserialize_owned(reader)?;

        Ok(BoxMetadata {
            alignment,
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
        let mut trailer = BoxMetadata::default();
        trailer.alignment = NonZeroU64::new(8);
        trailer.records.push(RecordHeader {
            compression: 0,
            length: 8,
            decompressed_length: 8,
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
        assert!(bf.read_trailer().is_err());
    }
}

use memmap::MmapOptions;

struct BoxFile {
    file: std::fs::File,
    header: FileHeader,
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
                };

                // Try to load the header so we can easily rewrite it when saving.
                // If header is invalid, we're not even loading a .box file.
                f.header = f.read_header()?;

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
            })?;

        FileHeader::default().write(&mut boxfile.file)?;
        boxfile.file.flush()?;
        Ok(boxfile)
    }

    /// Will return the metadata for the `.box` if it has been provided. It may not exist if this file
    /// is in the process of being created, or has been loaded with invalid data.
    pub fn metadata(&mut self) -> std::io::Result<BoxMetadata> {
        self.read_trailer()
    }

    #[inline(always)]
    fn read_header(&mut self) -> std::io::Result<FileHeader> {
        self.file.seek(SeekFrom::Start(0))?;
        FileHeader::deserialize_owned(&mut self.file)
    }

    #[inline(always)]
    fn read_trailer(&mut self) -> std::io::Result<BoxMetadata> {
        let header = self.read_header()?;
        let ptr = header.trailer.ok_or(std::io::Error::new(
            std::io::ErrorKind::Other,
            "no trailer found",
        ))?;
        self.file.seek(SeekFrom::Start(ptr.get()))?;
        BoxMetadata::deserialize_owned(&mut self.file)
    }

    #[inline(always)]
    fn read_data(&mut self, header: &RecordHeader) -> std::io::Result<memmap::Mmap> {
        unsafe {
            MmapOptions::new()
                .offset(header.data.get())
                .len(header.length as usize)
                .map(&self.file)
        }
    }
}

const COMPRESSION_DEFLATE: u128 = 0x20ae5ceb5ffe488e9f3679c2fc0d44c6;
