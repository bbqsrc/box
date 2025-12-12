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
    use std::path::Path;

    async fn create_test_box<F: AsRef<Path>>(filename: F) {
        let _ = std::fs::remove_file(filename.as_ref());

        let mut cursor = std::io::Cursor::new(b"hello\0\0\0".to_vec());

        let mut writer = BoxFileWriter::create(filename).await.unwrap();
        writer
            .insert(
                Compression::Stored,
                BoxPath::new("hello.txt").unwrap(),
                &mut cursor,
                HashMap::new(),
            )
            .await
            .unwrap();
        writer.finish().await.unwrap();
    }

    #[tokio::test]
    async fn create_box_file() {
        create_test_box("./smoketest.box").await;
    }

    #[tokio::test]
    async fn read_garbage() {
        let filename = "./read_garbage.box";
        create_test_box(filename).await;

        let bf = BoxFileReader::open(filename).await.unwrap();
        let trailer = bf.metadata();
        println!("{:?}", bf.header);
        println!("{:?}", &trailer);
        let segment = bf.memory_map(trailer.inodes[0].as_file().unwrap()).unwrap();
        let file_data = segment.as_slice().unwrap();
        println!("{:?}", &file_data);
        assert_eq!(file_data, b"hello\0\0\0")
    }

    #[tokio::test]
    async fn create_garbage() {
        let filename = "./create_garbage.box";
        let _ = std::fs::remove_file(filename);
        let bf = BoxFileWriter::create(filename).await.expect("Mah box");
        bf.finish().await.unwrap();
    }

    #[tokio::test]
    async fn read_bytes() {
        let filename = "./read_bytes.box";
        create_test_box(filename).await;
        let bf = BoxFileReader::open(filename).await.unwrap();
        let record = bf
            .metadata()
            .inodes
            .first()
            .map(|f| f.as_file().unwrap())
            .unwrap();
        let mut reader = bf.read_bytes(record).await.unwrap();
        let mut vec = vec![];
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut vec)
            .await
            .unwrap();
        assert_eq!(vec, b"hello\0\0\0")
    }

    async fn insert_impl(filename: &str, mut bf: BoxFileWriter) {
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
            .await
            .unwrap();
            bf.insert(
                Compression::Deflate,
                BoxPath::new("test/string2.txt").unwrap(),
                &mut std::io::Cursor::new(v.clone()),
                attrs.clone(),
            )
            .await
            .unwrap();
            bf.finish().await.unwrap();
        }

        let bf = BoxFileReader::open(filename).await.expect("Mah box");
        println!("{:#?}", &bf);

        let mut buf1 = Vec::new();
        bf.decompress(bf.meta.inodes[1].as_file().unwrap(), &mut buf1)
            .await
            .unwrap();
        assert_eq!(v, String::from_utf8(buf1).unwrap());

        let mut buf2 = Vec::new();
        bf.decompress(bf.meta.inodes[2].as_file().unwrap(), &mut buf2)
            .await
            .unwrap();
        assert_eq!(v, String::from_utf8(buf2).unwrap());
    }

    #[tokio::test]
    async fn insert() {
        let _ = std::fs::remove_file("./insert_garbage.box");
        insert_impl(
            "./insert_garbage.box",
            BoxFileWriter::create("./insert_garbage.box").await.unwrap(),
        )
        .await;

        let _ = std::fs::remove_file("./insert_garbage_align8.box");
        insert_impl(
            "./insert_garbage_align8.box",
            BoxFileWriter::create_with_alignment("./insert_garbage_align8.box", 8)
                .await
                .unwrap(),
        )
        .await;

        let _ = std::fs::remove_file("./insert_garbage_align7.box");
        insert_impl(
            "./insert_garbage_align7.box",
            BoxFileWriter::create_with_alignment("./insert_garbage_align7.box", 7)
                .await
                .unwrap(),
        )
        .await;
    }

    // #[test]
    // fn read_index() {
    //     insert_impl("./read_index.box", |n| BoxFileWriter::create(n).unwrap());

    //     let bf = BoxFileReader::open("./read_index.box").unwrap();
    //     let fst = bf.meta.index.unwrap();

    //     fst.get("nothing");
    // }
}
