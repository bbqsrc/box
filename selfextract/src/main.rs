const DIVIDER_UUID: u128 = 0xaae8ea9c35484ee4bf28f1a25a6b3c6c;

fn main() {
    std::process::exit(run());
}

#[inline(always)]
fn run() -> i32 {
    let current_dir = match std::env::current_dir() {
        Ok(path) => path,
        Err(e) => {
            eprintln!("ERROR: Could not access current directory!");
            // eprintln!("{:?}", e);
            return 1;
        }
    };

    let path = match std::env::current_exe() {
        Ok(path) => path,
        Err(e) => {
            eprintln!("ERROR: Could not access self-extractor for opening!");
            eprintln!("{:?}", e);
            return 1;
        }
    };

    let file = match std::fs::File::open(&path) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("ERROR: Could not access self-extractor for opening!");
            eprintln!("{:?}", e);
            return 1;
        }
    };

    let mmap = match unsafe { memmap::Mmap::map(&file) } {
        Ok(v) => v,
        Err(e) => {
            eprintln!("ERROR: Could not access self-extractor for opening!");
            eprintln!("{:?}", e);
            return 1;
        }
    };
    
    let boundary = twoway::find_bytes(&mmap[..], &DIVIDER_UUID.to_le_bytes());
    let offset = match boundary {
        Some(v) => v + std::mem::size_of::<u128>(),
        None => {
            eprintln!("ERROR: Could not find embedded .box file data to extract.");
            return 1;
        }
    };

    let bf = match box_format::BoxFileReader::open_at_offset(path, offset as u64) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("ERROR: Could not read .box data!");
            eprintln!("{:?}", e);
            return 1;
        }
    };

    bf.extract_all(current_dir).unwrap();
    0
}
