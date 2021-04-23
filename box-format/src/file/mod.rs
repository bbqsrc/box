use std::collections::HashMap;
use std::num::NonZeroU64;

#[repr(transparent)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Inode(NonZeroU64);

impl Inode {
    pub fn new(value: u64) -> std::io::Result<Inode> {
        match NonZeroU64::new(value) {
            Some(v) => Ok(Inode(v)),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "inode must not be zero",
            )),
        }
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }
}

pub(crate) mod meta;
#[cfg(feature = "reader")]
pub mod reader;
#[cfg(feature = "writer")]
pub mod writer;

pub use self::meta::BoxMetadata;

pub type AttrMap = HashMap<usize, Vec<u8>>;

#[cfg(test)]
mod tests {
    use crate::{compression::Compression, *};
    use std::collections::HashMap;
    use std::io::prelude::*;
    use std::io::Cursor;
    use std::path::Path;

    fn create_test_box<F: AsRef<Path>>(filename: F) {
        let _ = std::fs::remove_file(filename.as_ref());

        let mut cursor: Cursor<Vec<u8>> = Cursor::new(vec![]);
        let data = b"hello\0\0\0";
        cursor.write_all(data).unwrap();
        cursor.seek(std::io::SeekFrom::Start(0)).unwrap();

        let mut writer = BoxFileWriter::create(filename).unwrap();
        writer
            .insert(
                Compression::Stored,
                BoxPath::new("hello.txt").unwrap(),
                &mut cursor,
                HashMap::new(),
            )
            .unwrap();
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
            bf.memory_map(&trailer.inodes[0].as_file().unwrap())
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

    #[test]
    fn read_bytes() {
        let filename = "./read_bytes.box";
        create_test_box(&filename);
        let bf = BoxFileReader::open(&filename).unwrap();
        let record = bf
            .metadata()
            .inodes
            .first()
            .map(|f| f.as_file().unwrap())
            .unwrap();
        let mut reader = bf.read_bytes(&record).unwrap();
        let mut vec = vec![];
        reader.read_to_end(&mut vec).unwrap();
        assert_eq!(vec, b"hello\0\0\0")
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
            // println!("{:?}", &bf);
            bf.finish().unwrap();
        }

        let bf = BoxFileReader::open(&filename).expect("Mah box");
        println!("{:#?}", &bf);

        assert_eq!(
            v,
            bf.decompress_value::<String>(&bf.meta.inodes[1].as_file().unwrap())
                .unwrap()
        );
        assert_eq!(
            v,
            bf.decompress_value::<String>(&bf.meta.inodes[2].as_file().unwrap())
                .unwrap()
        );
    }

    #[test]
    fn insert() {
        insert_impl("./insert_garbage.box", |n| {
            BoxFileWriter::create(n).unwrap()
        });
        insert_impl("./insert_garbage_align8.box", |n| {
            BoxFileWriter::create_with_alignment(n, 8).unwrap()
        });
        insert_impl("./insert_garbage_align7.box", |n| {
            BoxFileWriter::create_with_alignment(n, 7).unwrap()
        });
    }

    #[test]
    fn read_index() {
        insert_impl("./read_index.box", |n| BoxFileWriter::create(n).unwrap());

        let bf = BoxFileReader::open("./read_index.box").unwrap();
        let fst = bf.meta.index.unwrap();

        fst.get("nothing");
    }
}
