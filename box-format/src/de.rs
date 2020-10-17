use std::collections::HashMap;
use std::io::prelude::*;
use std::num::NonZeroU64;

use byteorder::{LittleEndian, ReadBytesExt};
use fastvlq::ReadVu64Ext as _;

use crate::{
    AttrMap, BoxHeader, BoxMetadata, BoxPath, Compression, DirectoryRecord, FileRecord, LinkRecord,
    Record,
};

use crate::compression::constants::*;

pub(crate) trait DeserializeOwned {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized;
}

impl<T: DeserializeOwned> DeserializeOwned for Vec<T> {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let len: u64 = reader.read_vu64()?;
        let mut buf = Vec::with_capacity(len as usize);
        for _ in 0..len {
            buf.push(T::deserialize_owned(reader)?);
        }
        Ok(buf)
    }
}

impl DeserializeOwned for BoxPath {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        Ok(BoxPath(String::deserialize_owned(reader)?))
    }
}

impl DeserializeOwned for AttrMap {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let _byte_count = reader.read_u64::<LittleEndian>()?;
        let len: u64 = reader.read_vu64()?;
        let mut buf = HashMap::with_capacity(len as usize);
        for _ in 0..len {
            let key = reader.read_vu64()?;
            let value = Vec::deserialize_owned(reader)?;
            buf.insert(key as usize, value);
        }
        Ok(buf)
    }
}

impl DeserializeOwned for FileRecord {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let compression = Compression::deserialize_owned(reader)?;
        let length = reader.read_u64::<LittleEndian>()?;
        let decompressed_length = reader.read_u64::<LittleEndian>()?;
        let data = reader.read_u64::<LittleEndian>()?;
        let name = String::deserialize_owned(reader)?; //BoxPath::deserialize_owned(reader)?;
        let attrs = HashMap::deserialize_owned(reader)?;

        Ok(FileRecord {
            compression,
            length,
            decompressed_length,
            name,
            attrs,
            data: NonZeroU64::new(data).expect("non zero"),
        })
    }
}

use crate::file::Inode;
impl DeserializeOwned for Inode {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        reader.read_vu64().and_then(Inode::new)
    }
}

impl DeserializeOwned for DirectoryRecord {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let name = String::deserialize_owned(reader)?;

        // Inodes vec
        let len = reader.read_vu64()? as usize;
        let mut inodes = Vec::with_capacity(len);
        for _ in 0..len {
            inodes.push(Inode::deserialize_owned(reader)?);
        }

        let attrs = HashMap::deserialize_owned(reader)?;

        Ok(DirectoryRecord {
            name,
            inodes,
            attrs,
        })
    }
}

impl DeserializeOwned for LinkRecord {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let name = String::deserialize_owned(reader)?;
        let target = BoxPath::deserialize_owned(reader)?;
        let attrs = HashMap::deserialize_owned(reader)?;

        Ok(LinkRecord {
            name,
            target,
            attrs,
        })
    }
}

impl DeserializeOwned for Record {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let ty = reader.read_u8()?;
        match ty {
            0 => Ok(Record::File(FileRecord::deserialize_owned(reader)?)),
            1 => Ok(Record::Directory(DirectoryRecord::deserialize_owned(
                reader,
            )?)),
            2 => Ok(Record::Link(LinkRecord::deserialize_owned(reader)?)),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid or unsupported field type: {}", ty),
            )),
        }
    }
}

impl DeserializeOwned for BoxHeader {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let magic_bytes = reader.read_u32::<LittleEndian>()?.to_le_bytes();

        if &magic_bytes != crate::header::MAGIC_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Magic bytes invalid",
            ));
        }

        let version = reader.read_u32::<LittleEndian>()?;
        let alignment = reader.read_u64::<LittleEndian>()?;
        let trailer = reader.read_u64::<LittleEndian>()?;

        Ok(BoxHeader {
            magic_bytes,
            version,
            alignment,
            trailer: NonZeroU64::new(trailer),
        })
    }
}

impl DeserializeOwned for BoxMetadata {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let root = Vec::deserialize_owned(reader)?;
        let inodes = Vec::deserialize_owned(reader)?;
        let attr_keys = Vec::deserialize_owned(reader)?;
        let attrs = HashMap::deserialize_owned(reader)?;

        Ok(BoxMetadata {
            root,
            inodes,
            attr_keys,
            attrs,
            index: None,
        })
    }
}

impl DeserializeOwned for Compression {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let id = reader.read_u8()?;

        use Compression::*;

        Ok(match id {
            COMPRESSION_STORED => Stored,
            COMPRESSION_BROTLI => Brotli,
            COMPRESSION_DEFLATE => Deflate,
            COMPRESSION_ZSTD => Zstd,
            COMPRESSION_XZ => Xz,
            COMPRESSION_SNAPPY => Snappy,
            id => Unknown(id),
        })
    }
}

impl DeserializeOwned for String {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let len: u64 = reader.read_vu64()?;
        let mut string = String::with_capacity(len as usize);
        reader.take(len).read_to_string(&mut string)?;
        Ok(string)
    }
}

impl DeserializeOwned for Vec<u8> {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let len: u64 = reader.read_vu64()?;
        let mut buf = Vec::with_capacity(len as usize);
        reader.take(len).read_to_end(&mut buf)?;
        Ok(buf)
    }
}
