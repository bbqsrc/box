use crate::Record;
use crate::compat::{Cow, Vec};
use crate::path::BoxPath;

use super::{BoxMetadata, RecordIndex};

impl BoxMetadata<'_> {
    /// Validate a path/index/record triple before an extraction frontend turns
    /// the logical archive path into a platform path.
    pub(crate) fn validate_extraction_path(
        &self,
        path: &BoxPath<'_>,
        index: RecordIndex,
        allow_escapes: bool,
    ) -> std::io::Result<()> {
        let record = self.record(index).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("path references missing record index {}", index.get()),
            )
        })?;
        path.validate_indexed_for_extraction(record.name(), allow_escapes)?;

        if let Some(fst) = &self.fst {
            match fst.get(path.as_ref()) {
                Some(value) if value == index.get() => {}
                Some(value) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "path index maps {:?} to record {value}, not record {}",
                            path.to_string(),
                            index.get()
                        ),
                    ));
                }
                None => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("record {} has no matching path-index entry", index.get()),
                    ));
                }
            }
        }

        Ok(())
    }

    /// Resolve direct directory children together with their validated indexed
    /// paths. Legacy trees derive paths from validated single-component record
    /// names; finalized archives preserve and validate each direct FST key.
    pub(crate) fn extraction_children_by_index(
        &self,
        dir_index: RecordIndex,
        dir_path: &BoxPath<'_>,
        allow_escapes: bool,
    ) -> std::io::Result<Vec<(BoxPath<'static>, RecordIndex)>> {
        let directory = self
            .record(dir_index)
            .and_then(Record::as_directory)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("record {} is not a directory", dir_index.get()),
                )
            })?;

        if !directory.entries.is_empty() {
            let mut children = Vec::with_capacity(directory.entries.len());
            for child_index in directory.entries.iter().copied() {
                let child = self.record(child_index).ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "directory record {} references missing child record {}",
                            dir_index.get(),
                            child_index.get()
                        ),
                    )
                })?;
                let child_path = dir_path.join_unchecked(child.name());
                child_path.validate_indexed_for_extraction(child.name(), allow_escapes)?;
                children.push((child_path, child_index));
            }
            return Ok(children);
        }

        let Some(fst) = &self.fst else {
            return Ok(Vec::new());
        };
        if fst.get(dir_path.as_ref()) != Some(dir_index.get()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "directory record {} does not match its path-index entry",
                    dir_index.get()
                ),
            ));
        }

        let mut prefix = dir_path.as_ref().to_vec();
        prefix.push(0x1f);
        let mut children = Vec::new();
        for (key, value) in fst.prefix_iter(&prefix) {
            let suffix = &key[prefix.len()..];
            if suffix.contains(&0x1f) {
                continue;
            }
            let child_index = RecordIndex::try_new(value).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "path index contains zero as a child record index",
                )
            })?;
            let child = self.record(child_index).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("path index references missing child record {value}"),
                )
            })?;
            let path = std::str::from_utf8(&key).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "path index contains a non-UTF-8 child path",
                )
            })?;
            let child_path = BoxPath(Cow::Owned(path.to_string()));
            child_path.validate_indexed_for_extraction(child.name(), allow_escapes)?;
            if child_path.parent().as_ref() != Some(dir_path) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "path index child {:?} is not directly beneath {:?}",
                        child_path.to_string(),
                        dir_path.to_string()
                    ),
                ));
            }
            children.push((child_path, child_index));
        }

        Ok(children)
    }
}
