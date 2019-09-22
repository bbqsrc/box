use std::collections::HashMap;
use std::io::prelude::*;
use std::num::NonZeroU64;

use byteorder::{LittleEndian, ReadBytesExt};

use crate::{
    AttrMap, BoxHeader, BoxMetadata, BoxPath, Compression, DirectoryRecord, FileRecord, Record,
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
        let len = reader.read_u64::<LittleEndian>()?;
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

impl DeserializeOwned for u32 {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        reader.read_u32::<LittleEndian>()
    }
}

impl DeserializeOwned for AttrMap {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let _byte_count = reader.read_u64::<LittleEndian>()?;
        let len = reader.read_u64::<LittleEndian>()?;
        let mut buf = HashMap::with_capacity(len as usize);
        for _ in 0..len {
            let key = u32::deserialize_owned(reader)?;
            let value = Vec::deserialize_owned(reader)?;
            buf.insert(key, value);
        }
        Ok(buf)
    }
}

impl DeserializeOwned for FileRecord {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let compression = Compression::deserialize_owned(reader)?;
        let length = reader.read_u64::<LittleEndian>()?;
        let decompressed_length = reader.read_u64::<LittleEndian>()?;
        let path = BoxPath::deserialize_owned(reader)?;
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

impl DeserializeOwned for DirectoryRecord {
    fn deserialize_owned<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let path = BoxPath::deserialize_owned(reader)?;
        let attrs = HashMap::deserialize_owned(reader)?;

        Ok(DirectoryRecord { path, attrs })
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

        if &magic_bytes != b"BOX\0" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Magic bytes invalid",
            ));
        }

        let version = reader.read_u32::<LittleEndian>()?;
        let alignment = NonZeroU64::new(reader.read_u64::<LittleEndian>()?);
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
        let records = Vec::deserialize_owned(reader)?;
        let attr_keys = Vec::deserialize_owned(reader)?;
        let attrs = HashMap::deserialize_owned(reader)?;

        Ok(BoxMetadata {
            records,
            attr_keys,
            attrs,
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
        let len = reader.read_u64::<LittleEndian>()?;
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
        let len = reader.read_u64::<LittleEndian>()?;
        let mut buf = Vec::with_capacity(len as usize);
        reader.take(len).read_to_end(&mut buf)?;
        Ok(buf)
    }
}
