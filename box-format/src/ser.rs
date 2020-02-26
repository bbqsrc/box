use std::io::{prelude::*, SeekFrom};

use byteorder::{LittleEndian, WriteBytesExt};
use vlq::fast::WriteVlqExt;

use crate::{
    file::Inode, AttrMap, BoxHeader, BoxMetadata, BoxPath, Compression, DirectoryRecord,
    FileRecord, LinkRecord, Record,
};

pub(crate) trait Serialize {
    fn write<W: Write + Seek>(&self, writer: &mut W) -> std::io::Result<()>;
}

impl<T: Serialize> Serialize for Vec<T> {
    fn write<W: Write + Seek>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_fast_vlq(self.len() as u64)?;

        for item in self.iter() {
            item.write(writer)?;
        }
        Ok(())
    }
}

impl Serialize for String {
    fn write<W: Write + Seek>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_fast_vlq(self.len() as u64)?;
        writer.write_all(self.as_bytes())
    }
}

impl Serialize for Vec<u8> {
    fn write<W: Write + Seek>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_fast_vlq(self.len() as u64)?;
        writer.write_all(&*self)
    }
}

impl Serialize for AttrMap {
    fn write<W: Write + Seek>(&self, writer: &mut W) -> std::io::Result<()> {
        // Write the length in bytes so implementations can skip the entire map if they so choose.

        // Write it as u64::MAX, then seek back
        let size_index = writer.seek(SeekFrom::Current(0))?;
        writer.write_u64::<LittleEndian>(std::u64::MAX)?;
        writer.write_fast_vlq(self.len() as u64)?;

        for (key, value) in self.iter() {
            writer.write_fast_vlq(*key as u64)?;
            value.write(writer)?;
        }

        // Go back and write size
        let cur_index = writer.seek(SeekFrom::Current(0))?;
        writer.seek(SeekFrom::Start(size_index))?;
        writer.write_u64::<LittleEndian>(cur_index - size_index)?;
        writer.seek(SeekFrom::Start(cur_index))?;

        Ok(())
    }
}

impl Serialize for BoxPath {
    fn write<W: Write + Seek>(&self, writer: &mut W) -> std::io::Result<()> {
        self.0.write(writer)
    }
}

impl Serialize for Inode {
    fn write<W: Write + Seek>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_fast_vlq(self.get())
    }
}

impl Serialize for FileRecord {
    fn write<W: Write + Seek>(&self, writer: &mut W) -> std::io::Result<()> {
        // Record id - 0 for file
        writer.write_u8(0x0)?;

        writer.write_u8(self.compression.id())?;
        writer.write_u64::<LittleEndian>(self.length)?;
        writer.write_u64::<LittleEndian>(self.decompressed_length)?;
        writer.write_u64::<LittleEndian>(self.data.get())?;

        self.name.write(writer)?;
        self.attrs.write(writer)
    }
}

impl Serialize for DirectoryRecord {
    fn write<W: Write + Seek>(&self, writer: &mut W) -> std::io::Result<()> {
        // Record id - 1 for directory
        writer.write_u8(0x1)?;

        self.name.write(writer)?;
        self.inodes.write(writer)?;
        self.attrs.write(writer)
    }
}

impl Serialize for LinkRecord {
    fn write<W: Write + Seek>(&self, writer: &mut W) -> std::io::Result<()> {
        // Record id - 2 for symlink
        writer.write_u8(0x2)?;

        self.name.write(writer)?;
        self.target.write(writer)?;
        self.attrs.write(writer)
    }
}

impl Serialize for Record {
    fn write<W: Write + Seek>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            Record::File(file) => file.write(writer),
            Record::Directory(directory) => directory.write(writer),
            Record::Link(link) => link.write(writer),
        }
    }
}

impl Serialize for BoxHeader {
    fn write<W: Write + Seek>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(&self.magic_bytes)?;
        writer.write_u32::<LittleEndian>(self.version)?;
        writer.write_u64::<LittleEndian>(self.alignment)?;
        writer.write_u64::<LittleEndian>(self.trailer.map(|x| x.get()).unwrap_or(0))
    }
}

impl Serialize for BoxMetadata {
    fn write<W: Write + Seek>(&self, writer: &mut W) -> std::io::Result<()> {
        self.root.write(writer)?;
        self.inodes.write(writer)?;
        self.attr_keys.write(writer)?;
        self.attrs.write(writer)?;

        // Write the index
        let mut builder = fst::MapBuilder::memory();

        for x in self.iter() {
            builder
                .insert(x.path, x.inode.get())
                .expect("FST failed to generate");
        }

        writer.write_all(&builder.into_inner().unwrap())
    }
}

impl Serialize for Compression {
    fn write<W: Write + Seek>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u8(self.id())
    }
}
