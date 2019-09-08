use std::path::{Path, PathBuf};

mod error;

pub use self::error::IntoBoxPathError;

#[cfg(not(windows))]
/// The platform-specific separator as a string, used for splitting
/// platform-supplied paths and printing `BoxPath`s in the platform-preferred
/// manner.
pub const PATH_PLATFORM_SEP: &str = "/";

#[cfg(windows)]
/// The platform-specific separator as a string, used for splitting
/// platform-supplied paths and printing `BoxPath`s in the platform-preferred
/// manner.
pub const PATH_PLATFORM_SEP: &str = "\\";

/// The separator used in `BoxPath` type paths, used primarily in
/// `FileRecord` and `DirectoryRecord` fields.
pub const PATH_BOX_SEP: &str = "\x1f";

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct BoxPath(pub(crate) String);

impl<'a> PartialEq<&'a str> for BoxPath {
    fn eq(&self, other: &&'a str) -> bool {
        self.0 == *other
    }
}

pub fn sanitize<P: AsRef<Path>>(path: P) -> Option<Vec<String>> {
    use std::path::Component;
    use unic_normal::StrNormalForm;
    use unic_ucd::GeneralCategory;

    let mut out = vec![];

    for component in path.as_ref().components() {
        match component {
            Component::CurDir | Component::RootDir | Component::Prefix(_) => {}
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(os_str) => out.push(
                os_str
                    .to_str()
                    .map(|x| x.trim())
                    .filter(|x| x.len() > 0)
                    .filter(|x| {
                        !x.chars().any(|c| {
                            let cat = GeneralCategory::of(c);
                            c == '\\'
                                || cat == GeneralCategory::Control
                                || (cat.is_separator() && c != ' ')
                        })
                    })
                    .map(|x| x.nfc().collect::<String>())?,
            ),
        }
    }

    Some(out)
}

impl BoxPath {
    pub fn new<P: AsRef<Path>>(path: P) -> std::result::Result<BoxPath, IntoBoxPathError> {
        let out = sanitize(&path).ok_or(IntoBoxPathError::UnrepresentableStr)?;
        
        if out.len() == 0 {
            return Err(IntoBoxPathError::EmptyPath);
        }

        Ok(BoxPath(out.join(PATH_BOX_SEP)))
    }

    pub fn to_string(&self) -> String {
        let mut s = String::with_capacity(self.0.len());
        let mut iter = self.0.split(PATH_BOX_SEP);
        if let Some(v) = iter.next() {
            s.push_str(v);
        }
        iter.for_each(|v| {
            s.push_str(PATH_PLATFORM_SEP);
            s.push_str(v);
        });
        s
    }

    pub fn to_path_buf(&self) -> PathBuf {
        PathBuf::from(self.to_string())
    }

    pub fn levels(&self) -> usize {
        self.0.chars().filter(|c| c == &'\x1f').count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitisation() {
        let box_path = BoxPath::new("/something/../somethingelse/./foo.txt").unwrap();
        assert_eq!(box_path, "somethingelse\x1ffoo.txt");
        let box_path = BoxPath::new("../something/../somethingelse/./foo.txt/.").unwrap();
        assert_eq!(box_path, "somethingelse\x1ffoo.txt");

        // This one will do different things on Windows and Unix, because Unix loves a good backslash
        let box_path = BoxPath::new(r"..\something\..\somethingelse\.\foo.txt\.");

        #[cfg(not(windows))]
        assert!(box_path.is_err());
        #[cfg(windows)]
        assert_eq!(box_path.unwrap().0, "somethingelse\x1ffoo.txt");

        let box_path = BoxPath::new(r"..\something/..\somethingelse\./foodir\");
        #[cfg(not(windows))]
        assert!(box_path.is_err());
        #[cfg(windows)]
        assert_eq!(box_path.unwrap().0, "somethingelse\x1ffoodir");
    }

    #[test]
    fn sanitisation2() {
        // Null is a sassy fellow
        let box_path = BoxPath::new("\0");
        assert!(box_path.is_err());
    }

    #[test]
    fn sanitisation3() {
        // Blank string is a sassy fellow if you can find him
        let box_path = BoxPath::new("");
        assert!(box_path.is_err());
    }

    #[test]
    fn sanitisation4() {
        // Blank string is a sassy fellow if you can find him
        let box_path = BoxPath::new("/cant/hate//the/path");
        println!("{:?}", box_path);
        assert_eq!(box_path.unwrap().0, "cant\x1fhate\x1fthe\x1fpath");
    }

    #[test]
    fn sanitisation_bidi() {
        // Blank string is a sassy fellow if you can find him
        let box_path = BoxPath::new("this is now العَرَبِيَّة.txt");
        println!("{:?}", box_path);
        assert_eq!(
            box_path.unwrap().0,
            "this is now العَرَبِيَّة.txt"
        );
    }

    #[test]
    fn sanitisation_basmala() {
        // Blank string is a sassy fellow if you can find him
        let box_path = BoxPath::new("this is now ﷽.txt");
        println!("{:?}", box_path);
        assert_eq!(box_path.unwrap().0, "this is now ﷽.txt");
    }

    #[test]
    fn sanitisation_icecube_emoji() {
        let box_path = BoxPath::new("///🧊/🧊");
        println!("{:?}", box_path);
        assert_eq!(box_path.unwrap().0, "🧊\x1f🧊");
    }

    #[test]
    fn sanitisation_simple_self() {
        let box_path = BoxPath::new("./self");
        println!("{:?}", box_path);
        assert_eq!(box_path.unwrap().0, "self");
    }

    #[test]
    fn sanitisation_slash() {
        let box_path = BoxPath::new("/");
        println!("{:?}", box_path);
        assert!(box_path.is_err());
    }
}
