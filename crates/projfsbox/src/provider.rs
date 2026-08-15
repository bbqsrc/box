//! BoxProvider - the main ProjFS provider coordinator.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use box_format::{BoxFileReader, Record, RecordIndex};
use tokio::runtime::Runtime;

use crate::enumeration::EnumerationSession;
use crate::file_data::logical_file_buffer;
use crate::lifecycle::{CallbackOwner, HRESULT_ALREADY_EXISTS, is_already_exists_hresult};

use windows::Win32::Storage::ProjectedFileSystem::PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT;

struct VirtualizationState {
    namespace_context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
    _callback_owner: Box<CallbackOwner<BoxProvider>>,
}

/// The main ProjFS provider that coordinates access to a `.box` archive.
///
/// This struct holds the archive reader, caches, and enumeration sessions.
/// ProjFS receives a stable weak callback owner and each callback upgrades it
/// for the duration of that call.
// [spec:box:req:projfs-provider.root]
pub struct BoxProvider {
    /// The opened box archive reader.
    pub(crate) reader: BoxFileReader,
    /// Tokio runtime for async operations (box-format is async).
    pub(crate) runtime: Runtime,
    /// Cache for decompressed file data.
    /// Key: RecordIndex, Value: decompressed bytes.
    pub(crate) cache: RwLock<HashMap<RecordIndex, Vec<u8>>>,
    /// Active enumeration sessions.
    /// Key: GUID (as u128), Value: EnumerationSession.
    pub(crate) enumerations: RwLock<HashMap<u128, EnumerationSession>>,
    /// The virtualization root path.
    pub(crate) root_path: PathBuf,
    /// ProjFS namespace handle (set after start).
    namespace_context: Mutex<Option<VirtualizationState>>,
}

// Safety: BoxProvider is thread-safe because:
// - BoxFileReader uses mmap internally which is safe for concurrent reads
// - All mutable state is protected by RwLock or Mutex
unsafe impl Send for BoxProvider {}
unsafe impl Sync for BoxProvider {}

impl BoxProvider {
    /// Create a new BoxProvider.
    pub fn new(reader: BoxFileReader, runtime: Runtime, root_path: PathBuf) -> Self {
        Self {
            reader,
            runtime,
            cache: RwLock::new(HashMap::new()),
            enumerations: RwLock::new(HashMap::new()),
            root_path,
            namespace_context: Mutex::new(None),
        }
    }

    /// Get the virtualization root path.
    pub fn root_path(&self) -> &PathBuf {
        &self.root_path
    }

    /// Get the archive path.
    pub fn archive_path(&self) -> &std::path::Path {
        self.reader.path()
    }

    /// Resolve a Windows relative path to a record index and record reference.
    ///
    /// Returns None if the path doesn't exist in the archive or represents the root.
    pub fn resolve_path(&self, windows_path: &str) -> Option<(RecordIndex, &Record<'static>)> {
        if windows_path.is_empty() {
            // Root directory - no specific record
            return None;
        }

        let box_path = crate::path::windows_to_box_path(windows_path)?;
        let metadata = self.reader.metadata();

        // Use FST index if available, otherwise linear search
        let index = metadata.index(&box_path)?;
        let record = metadata.record(index)?;

        Some((index, record))
    }

    /// Get records for the root directory.
    pub fn root_records(&self) -> Vec<(RecordIndex, &Record<'static>)> {
        self.reader.metadata().root_records().to_vec()
    }

    /// Get records for a directory by its index.
    pub fn dir_records(&self, index: RecordIndex) -> Vec<(RecordIndex, &Record<'static>)> {
        self.reader.metadata().dir_records_by_index(index).to_vec()
    }

    /// Get or decompress file data, using cache if available.
    // [spec:box:req:projfs-provider.root.file-data-and-errors]
    pub fn get_or_decompress(&self, index: RecordIndex) -> std::io::Result<Vec<u8>> {
        // Check cache first
        {
            let cache = self
                .cache
                .read()
                .map_err(|_| std::io::Error::other("ProjFS file cache is poisoned"))?;
            if let Some(cached) = cache.get(&index) {
                return Ok(cached.clone());
            }
        }

        // Get the record and reconstruct its complete logical contents. ProjFS
        // may request arbitrary aligned ranges later, so both ordinary and
        // chunked records share the same whole-file cache contract.
        let record =
            self.reader.metadata().record(index).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "Record not found")
            })?;

        let buf = match record {
            Record::File(file) => {
                let mut buf = logical_file_buffer(file.decompressed_length)?;
                self.runtime
                    .block_on(self.reader.decompress(file, &mut buf))?;
                buf
            }
            Record::ChunkedFile(file) => {
                let mut buf = logical_file_buffer(file.decompressed_length)?;
                self.runtime
                    .block_on(self.reader.decompress_chunked(file, index, &mut buf))?;
                buf
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Record is not a hydratable file",
                ));
            }
        };

        // Cache and return
        {
            let mut cache = self
                .cache
                .write()
                .map_err(|_| std::io::Error::other("ProjFS file cache is poisoned"))?;
            cache.insert(index, buf.clone());
        }

        Ok(buf)
    }

    /// Clear cached data for a record.
    pub fn clear_cache(&self, index: RecordIndex) {
        let mut cache = self.cache.write().unwrap();
        cache.remove(&index);
    }

    /// Create a new enumeration session.
    pub fn create_enumeration(&self, id: u128, dir_index: Option<RecordIndex>) {
        let session = EnumerationSession::new(id, dir_index);
        let mut enums = self.enumerations.write().unwrap();
        enums.insert(id, session);
    }

    /// Get a mutable reference to an enumeration session.
    pub fn get_enumeration_mut<F, R>(&self, id: u128, f: F) -> Option<R>
    where
        F: FnOnce(&mut EnumerationSession) -> R,
    {
        let mut enums = self.enumerations.write().unwrap();
        enums.get_mut(&id).map(f)
    }

    /// Remove an enumeration session.
    pub fn remove_enumeration(&self, id: u128) {
        let mut enums = self.enumerations.write().unwrap();
        enums.remove(&id);
    }

    /// Start the ProjFS virtualization instance.
    // [spec:box:req:projfs-provider.root]
    // [spec:box:req:projfs-provider.root.lifecycle]
    pub fn start(self: &Arc<Self>) -> Result<(), windows::core::Error> {
        use windows::Win32::Foundation::E_FAIL;
        use windows::Win32::Storage::ProjectedFileSystem::*;
        use windows::core::{Error, HRESULT, PCWSTR};

        let mut namespace_context = self
            .namespace_context
            .lock()
            .map_err(|_| Error::new(E_FAIL, "ProjFS provider lifecycle state is poisoned"))?;

        if namespace_context.is_some() {
            return Err(Error::new(
                HRESULT(HRESULT_ALREADY_EXISTS),
                "ProjFS provider is already started",
            ));
        }

        let root_wide = crate::path::to_wide_string(&self.root_path.to_string_lossy());

        // Mark directory as placeholder root (one-time setup)
        // SAFETY: root_wide is a valid null-terminated wide string
        unsafe {
            match PrjMarkDirectoryAsPlaceholder(
                PCWSTR(root_wide.as_ptr()),
                None,
                None,
                std::ptr::null(),
            ) {
                Ok(()) => {}
                Err(e) => {
                    if !is_already_exists_hresult(e.code().0) {
                        return Err(e);
                    }
                }
            }
        }

        // Set up callbacks
        let callbacks = PRJ_CALLBACKS {
            StartDirectoryEnumerationCallback: Some(crate::callbacks::start_directory_enumeration),
            EndDirectoryEnumerationCallback: Some(crate::callbacks::end_directory_enumeration),
            GetDirectoryEnumerationCallback: Some(crate::callbacks::get_directory_enumeration),
            GetPlaceholderInfoCallback: Some(crate::callbacks::get_placeholder_info),
            GetFileDataCallback: Some(crate::callbacks::get_file_data),
            QueryFileNameCallback: Some(crate::callbacks::query_file_name),
            NotificationCallback: None,
            CancelCommandCallback: None,
        };

        // ProjFS keeps this stable box address only until stop returns. The box
        // contains a Weak, and each callback upgrades it to an Arc for exactly
        // the duration of that callback.
        let callback_owner = Box::new(CallbackOwner::new(self));
        let provider_ptr = std::ptr::from_ref(callback_owner.as_ref());

        // SAFETY: All parameters are valid, callbacks are extern "system" fn
        let context = unsafe {
            PrjStartVirtualizing(
                PCWSTR(root_wide.as_ptr()),
                &callbacks,
                Some(provider_ptr as *const std::ffi::c_void),
                None,
            )?
        };

        // Store context for later cleanup
        *namespace_context = Some(VirtualizationState {
            namespace_context: context,
            _callback_owner: callback_owner,
        });

        tracing::info!(
            "Started ProjFS virtualization at {}",
            self.root_path.display()
        );

        Ok(())
    }

    /// Stop the ProjFS virtualization instance.
    // [spec:box:req:projfs-provider.root.lifecycle]
    pub fn stop(&self) {
        use windows::Win32::Storage::ProjectedFileSystem::PrjStopVirtualizing;

        let context = match self.namespace_context.lock() {
            Ok(mut context) => context.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };

        if let Some(state) = context {
            unsafe {
                PrjStopVirtualizing(state.namespace_context);
            }
            // `PrjStopVirtualizing` has returned, so no callback can still use
            // the stable callback-owner address. Dropping the state now frees
            // its Weak without retaining the provider.
            drop(state);
            tracing::info!(
                "Stopped ProjFS virtualization at {}",
                self.root_path.display()
            );
        }
    }
}

impl Drop for BoxProvider {
    fn drop(&mut self) {
        self.stop();
    }
}
