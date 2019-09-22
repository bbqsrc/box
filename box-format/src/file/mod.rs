use std::collections::HashMap;
use std::io::{prelude::*, SeekFrom};
use std::num::NonZeroU64;

pub mod reader;
pub mod writer;

use crate::{de::DeserializeOwned, header::BoxHeader, record::Record};

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

#[inline(always)]
pub(self) fn read_header<R: Read + Seek>(file: &mut R) -> std::io::Result<BoxHeader> {
    file.seek(SeekFrom::Start(0))?;
    BoxHeader::deserialize_owned(file)
}

#[inline(always)]
pub(self) fn read_trailer<R: Read + Seek>(
    file: &mut R,
    ptr: NonZeroU64,
) -> std::io::Result<BoxMetadata> {
    file.seek(SeekFrom::Start(ptr.get()))?;
    BoxMetadata::deserialize_owned(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{compression::Compression, ser::Serialize, *};
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::Cursor;
    use std::path::Path;

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
        let file_data = unsafe { bf.data(&trailer.records[0].as_file().unwrap()).unwrap() };
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
            dir_attrs.insert("unix.mode".into(), 0o755u16.to_le_bytes().to_vec());

            let mut attrs = HashMap::new();
            attrs.insert("created".into(), now.to_vec());
            attrs.insert("unix.mode".into(), 0o644u16.to_le_bytes().to_vec());

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
