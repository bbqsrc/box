//! BoxVolume: FSVolume implementation for Box archives.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ptr;

use objc2::rc::Retained;
use objc2::runtime::{Bool, NSObject};
use objc2::{AllocAnyThread, ClassType, DefinedClass, define_class, msg_send};
use objc2_foundation::{NSError, NSString, NSUUID};

use box_format::{BoxFileReader, BoxMetadata, Record, RecordIndex};

use crate::bindings::{
    FSFileName, FSItem, FSItemAttributes, FSItemIdentifier, FSVolume, FSVolumeIdentifier,
};
use crate::item::BoxItem;

/// Internal state for BoxVolume.
pub struct BoxVolumeIvars {
    /// The Box file reader.
    pub reader: RefCell<Option<BoxFileReader>>,
    /// Tokio runtime for async operations.
    pub runtime: tokio::runtime::Runtime,
    /// Cache of decompressed file data.
    pub cache: RefCell<HashMap<RecordIndex, Vec<u8>>>,
}

impl Default for BoxVolumeIvars {
    fn default() -> Self {
        Self {
            reader: RefCell::new(None),
            runtime: tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime"),
            cache: RefCell::new(HashMap::new()),
        }
    }
}

define_class! {
    /// Volume implementation for Box archives.
    #[unsafe(super(FSVolume, NSObject))]
    #[name = "BoxVolume"]
    #[ivars = BoxVolumeIvars]
    pub struct BoxVolume;

    impl BoxVolume {
        // FSVolumePathConfOperations

        #[unsafe(method(maximumLinkCount))]
        fn maximum_link_count(&self) -> i64 {
            1 // No hard links in Box archives
        }

        #[unsafe(method(maximumNameLength))]
        fn maximum_name_length(&self) -> i64 {
            255 // Standard filename length limit
        }

        #[unsafe(method(restrictsOwnershipChanges))]
        fn restricts_ownership_changes(&self) -> bool {
            true // Read-only filesystem
        }

        #[unsafe(method(truncatesLongNames))]
        fn truncates_long_names(&self) -> bool {
            false
        }

        #[unsafe(method(maximumXattrSize))]
        fn maximum_xattr_size(&self) -> i64 {
            65536 // 64KB
        }

        #[unsafe(method(maximumFileSize))]
        fn maximum_file_size(&self) -> u64 {
            u64::MAX
        }

        // FSVolumeOperations

        #[unsafe(method(mountWithOptions:replyHandler:))]
        fn mount(&self, _options: *mut NSObject, reply: *const std::ffi::c_void) {
            tracing::debug!("BoxVolume: mount called");
            // Nothing special to do for mount
            Self::reply_void(reply, None);
        }

        #[unsafe(method(unmountWithReplyHandler:))]
        fn unmount(&self, reply: *const std::ffi::c_void) {
            tracing::debug!("BoxVolume: unmount called");
            // Clear the cache
            self.ivars().cache.borrow_mut().clear();
            Self::reply_void(reply, None);
        }

        #[unsafe(method(synchronizeWithReplyHandler:))]
        fn synchronize(&self, reply: *const std::ffi::c_void) {
            tracing::debug!("BoxVolume: synchronize called");
            // Read-only, nothing to sync
            Self::reply_void(reply, None);
        }

        #[unsafe(method(lookupItemNamed:inDirectory:replyHandler:))]
        fn lookup_item(
            &self,
            name: *mut FSFileName,
            directory: *mut FSItem,
            reply: *const std::ffi::c_void,
        ) {
            tracing::debug!("BoxVolume: lookupItemNamed called");

            type ReplyBlock = block2::Block<dyn Fn(*mut FSItem, *mut FSItemAttributes, *mut NSError)>;

            let reader_ref = self.ivars().reader.borrow();
            let reader = match reader_ref.as_ref() {
                Some(r) => r,
                None => {
                    Self::reply_lookup_error(reply, libc::EIO);
                    return;
                }
            };

            // Get the name as a string
            let name_str = if name.is_null() {
                Self::reply_lookup_error(reply, libc::EINVAL);
                return;
            } else {
                unsafe {
                    match (*name).string() {
                        Some(s) => s.to_string(),
                        None => {
                            Self::reply_lookup_error(reply, libc::EINVAL);
                            return;
                        }
                    }
                }
            };

            tracing::debug!("BoxVolume: looking up '{}'", name_str);

            // Get directory records
            let records = if directory.is_null() {
                // Root directory
                reader.metadata().root_records()
            } else {
                let dir_id = unsafe { (*directory).identifier() };
                match BoxItem::index_from_identifier(dir_id) {
                    Some(index) => reader.metadata().dir_records_by_index(index),
                    None => {
                        Self::reply_lookup_error(reply, libc::ENOENT);
                        return;
                    }
                }
            };

            // Search for the item
            for (index, record) in records.iter() {
                if record.name() == name_str {
                    let item = BoxItem::new(*index);
                    let fs_item = FSItem::with_identifier(item.identifier());
                    let attrs = self.make_attributes(record, reader.metadata(), *index);

                    unsafe {
                        let reply = reply as *const ReplyBlock;
                        if !reply.is_null() {
                            (*reply).call((
                                &*fs_item as *const _ as *mut _,
                                &*attrs as *const _ as *mut _,
                                ptr::null_mut(),
                            ));
                        }
                    }
                    return;
                }
            }

            // Not found
            Self::reply_lookup_error(reply, libc::ENOENT);
        }

        #[unsafe(method(reclaimItem:replyHandler:))]
        fn reclaim_item(&self, item: *mut FSItem, reply: *const std::ffi::c_void) {
            tracing::debug!("BoxVolume: reclaimItem called");

            // Remove from cache if present
            if !item.is_null() {
                let id = unsafe { (*item).identifier() };
                if let Some(index) = BoxItem::index_from_identifier(id) {
                    self.ivars().cache.borrow_mut().remove(&index);
                }
            }

            Self::reply_void(reply, None);
        }

        // FSVolumeOpenCloseOperations

        #[unsafe(method(openItem:withMode:replyHandler:))]
        fn open_item(&self, _item: *mut FSItem, mode: u32, reply: *const std::ffi::c_void) {
            tracing::debug!("BoxVolume: openItem called with mode {}", mode);

            // Check for write mode - we're read-only
            // O_WRONLY = 1, O_RDWR = 2
            if mode & 0x3 != 0 {
                Self::reply_void(reply, Some(libc::EROFS));
            } else {
                Self::reply_void(reply, None);
            }
        }

        #[unsafe(method(closeItem:keepingMode:replyHandler:))]
        fn close_item(&self, _item: *mut FSItem, _mode: u32, reply: *const std::ffi::c_void) {
            tracing::debug!("BoxVolume: closeItem called");
            Self::reply_void(reply, None);
        }

        // FSVolumeReadWriteOperations

        #[unsafe(method(readFromItem:offset:length:intoBuffer:replyHandler:))]
        fn read_from_item(
            &self,
            item: *mut FSItem,
            offset: u64,
            length: u64,
            buffer: *mut NSObject, // Actually NSMutableData
            reply: *const std::ffi::c_void,
        ) {
            tracing::debug!("BoxVolume: readFromItem called offset={} length={}", offset, length);

            if item.is_null() || buffer.is_null() {
                Self::reply_read(reply, 0, true, Some(libc::EINVAL));
                return;
            }

            let id = unsafe { (*item).identifier() };
            let index = match BoxItem::index_from_identifier(id) {
                Some(i) => i,
                None => {
                    Self::reply_read(reply, 0, true, Some(libc::ENOENT));
                    return;
                }
            };

            // Check cache first
            {
                let cache = self.ivars().cache.borrow();
                if let Some(data) = cache.get(&index) {
                    let start = offset as usize;
                    let end = std::cmp::min(start + length as usize, data.len());
                    if start < data.len() {
                        let slice = &data[start..end];
                        unsafe {
                            // Append to NSMutableData
                            let _: () = msg_send![buffer, appendBytes: slice.as_ptr(), length: slice.len()];
                        }
                        let bytes_read = (end - start) as u64;
                        let eof = end >= data.len();
                        Self::reply_read(reply, bytes_read, eof, None);
                        return;
                    }
                }
            }

            // Need to decompress
            let reader_ref = self.ivars().reader.borrow();
            let reader = match reader_ref.as_ref() {
                Some(r) => r,
                None => {
                    Self::reply_read(reply, 0, true, Some(libc::EIO));
                    return;
                }
            };

            let record = match reader.metadata().record(index).and_then(|r| r.as_file()) {
                Some(r) => r,
                None => {
                    Self::reply_read(reply, 0, true, Some(libc::EISDIR));
                    return;
                }
            };

            // Decompress the file
            let mut data = Vec::with_capacity(record.decompressed_length as usize);
            let result = self.ivars().runtime.block_on(reader.decompress(record, &mut data));

            match result {
                Ok(()) => {
                    // Store in cache
                    drop(reader_ref);
                    self.ivars().cache.borrow_mut().insert(index, data.clone());

                    // Return the requested portion
                    let start = offset as usize;
                    let end = std::cmp::min(start + length as usize, data.len());
                    if start < data.len() {
                        let slice = &data[start..end];
                        unsafe {
                            let _: () = msg_send![buffer, appendBytes: slice.as_ptr(), length: slice.len()];
                        }
                        let bytes_read = (end - start) as u64;
                        let eof = end >= data.len();
                        Self::reply_read(reply, bytes_read, eof, None);
                    } else {
                        Self::reply_read(reply, 0, true, None);
                    }
                }
                Err(e) => {
                    tracing::error!("BoxVolume: decompression failed: {}", e);
                    Self::reply_read(reply, 0, true, Some(libc::EIO));
                }
            }
        }

        #[unsafe(method(writeToItem:offset:fromBuffer:replyHandler:))]
        fn write_to_item(
            &self,
            _item: *mut FSItem,
            _offset: u64,
            _buffer: *mut NSObject,
            reply: *const std::ffi::c_void,
        ) {
            tracing::debug!("BoxVolume: writeToItem called - returning EROFS");
            Self::reply_read(reply, 0, false, Some(libc::EROFS));
        }

        // Write operations return EROFS

        #[unsafe(method(createItemNamed:type:inDirectory:attributes:replyHandler:))]
        fn create_item(
            &self,
            _name: *mut FSFileName,
            _item_type: u32,
            _directory: *mut FSItem,
            _attributes: *mut FSItemAttributes,
            reply: *const std::ffi::c_void,
        ) {
            tracing::debug!("BoxVolume: createItem called - returning EROFS");
            Self::reply_lookup_error(reply, libc::EROFS);
        }

        #[unsafe(method(removeItem:named:fromDirectory:replyHandler:))]
        fn remove_item(
            &self,
            _item: *mut FSItem,
            _name: *mut FSFileName,
            _directory: *mut FSItem,
            reply: *const std::ffi::c_void,
        ) {
            tracing::debug!("BoxVolume: removeItem called - returning EROFS");
            Self::reply_void(reply, Some(libc::EROFS));
        }
    }
}

impl BoxVolume {
    /// Create a new BoxVolume with the given reader.
    pub fn new_with_reader(reader: BoxFileReader) -> Retained<Self> {
        // Generate a volume ID
        let uuid = NSUUID::new();
        let volume_id = FSVolumeIdentifier::with_uuid(&uuid);
        let volume_name = NSString::from_str("Box Archive");

        // Allocate the volume with ivars, then initialize via superclass
        let mut ivars = BoxVolumeIvars::default();
        ivars.reader = RefCell::new(Some(reader));

        let this = Self::alloc().set_ivars(ivars);

        // Initialize with FSVolume's init method
        unsafe { msg_send![super(this), initWithVolumeID: &*volume_id, volumeName: &*volume_name] }
    }

    /// Get a reference to the Box reader.
    pub fn reader(&self) -> std::cell::Ref<'_, Option<BoxFileReader>> {
        self.ivars().reader.borrow()
    }

    /// Get a mutable reference to the cache.
    pub fn cache_mut(&self) -> std::cell::RefMut<'_, HashMap<RecordIndex, Vec<u8>>> {
        self.ivars().cache.borrow_mut()
    }

    /// Get a reference to the tokio runtime.
    pub fn runtime(&self) -> &tokio::runtime::Runtime {
        &self.ivars().runtime
    }

    /// Create FSItemAttributes from a Box record.
    fn make_attributes(
        &self,
        record: &Record<'_>,
        meta: &BoxMetadata,
        index: RecordIndex,
    ) -> Retained<FSItemAttributes> {
        let attrs = FSItemAttributes::new();

        attrs.set_file_id(FSItemIdentifier::new(index.get() + 2));
        attrs.set_type(BoxItem::item_type(record));
        attrs.set_mode(BoxItem::mode(record, meta));
        attrs.set_uid(BoxItem::uid(record, meta));
        attrs.set_gid(BoxItem::gid(record, meta));
        attrs.set_size(BoxItem::size(record));
        attrs.set_alloc_size(BoxItem::size(record));
        attrs.set_link_count(1);
        attrs.set_flags(0);

        attrs
    }

    /// Reply with void (no return value).
    fn reply_void(reply: *const std::ffi::c_void, error: Option<i32>) {
        type ReplyBlock = block2::Block<dyn Fn(*mut NSError)>;
        unsafe {
            let reply = reply as *const ReplyBlock;
            if !reply.is_null() {
                let error = error.map(|code| Self::make_posix_error(code));
                (*reply).call((error
                    .as_ref()
                    .map(|e| e as *const _ as *mut _)
                    .unwrap_or(ptr::null_mut()),));
            }
        }
    }

    /// Reply with lookup error.
    fn reply_lookup_error(reply: *const std::ffi::c_void, code: i32) {
        type ReplyBlock = block2::Block<dyn Fn(*mut FSItem, *mut FSItemAttributes, *mut NSError)>;
        unsafe {
            let reply = reply as *const ReplyBlock;
            if !reply.is_null() {
                let error = Self::make_posix_error(code);
                (*reply).call((
                    ptr::null_mut(),
                    ptr::null_mut(),
                    &*error as *const _ as *mut _,
                ));
            }
        }
    }

    /// Reply with read result.
    fn reply_read(reply: *const std::ffi::c_void, bytes: u64, eof: bool, error: Option<i32>) {
        type ReplyBlock = block2::Block<dyn Fn(u64, Bool, *mut NSError)>;
        unsafe {
            let reply = reply as *const ReplyBlock;
            if !reply.is_null() {
                let error = error.map(|code| Self::make_posix_error(code));
                (*reply).call((
                    bytes,
                    Bool::new(eof),
                    error
                        .as_ref()
                        .map(|e| e as *const _ as *mut _)
                        .unwrap_or(ptr::null_mut()),
                ));
            }
        }
    }

    /// Create a POSIX error.
    fn make_posix_error(code: i32) -> Retained<NSError> {
        unsafe {
            let domain = NSString::from_str("NSPOSIXErrorDomain");
            msg_send![NSError::class(), errorWithDomain: &*domain, code: code as i64, userInfo: ptr::null::<NSObject>()]
        }
    }
}
