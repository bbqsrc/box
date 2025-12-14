//! BoxProvider - the main ProjFS provider coordinator.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use box_format::{BoxFileReader, Record, RecordIndex};
use tokio::runtime::Runtime;

use crate::enumeration::EnumerationSession;

use windows::Win32::Storage::ProjectedFileSystem::PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT;

/// The main ProjFS provider that coordinates access to a `.box` archive.
///
/// This struct holds the archive reader, caches, and enumeration sessions.
/// It is passed as the `instanceContext` to ProjFS and must outlive all callbacks.
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
    pub(crate) namespace_context: Mutex<Option<PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT>>,
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
    pub fn get_or_decompress(&self, index: RecordIndex) -> std::io::Result<Vec<u8>> {
        // Check cache first
        {
            let cache = self.cache.read().unwrap();
            if let Some(cached) = cache.get(&index) {
                return Ok(cached.clone());
            }
        }

        // Get the record
        let record = self
            .reader
            .metadata()
            .record(index)
            .and_then(|r| r.as_file())
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Record not found or not a file",
                )
            })?;

        // Decompress using async runtime
        let mut buf = Vec::with_capacity(record.decompressed_length as usize);
        self.runtime
            .block_on(self.reader.decompress(record, &mut buf))?;

        // Cache and return
        {
            let mut cache = self.cache.write().unwrap();
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
    pub fn start(self: Arc<Self>) -> Result<(), windows::core::Error> {
        use windows::Win32::Storage::ProjectedFileSystem::*;
        use windows::core::PCWSTR;

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
                    // Ignore ERROR_ALREADY_EXISTS (0x800700B7)
                    if e.code().0 != -2147024713 {
                        tracing::warn!("PrjMarkDirectoryAsPlaceholder: {:?}", e);
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

        // Leak Arc to get stable pointer for ProjFS lifetime
        let provider_ptr = Arc::into_raw(Arc::clone(&self));

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
        *self.namespace_context.lock().unwrap() = Some(context);

        tracing::info!(
            "Started ProjFS virtualization at {}",
            self.root_path.display()
        );

        Ok(())
    }

    /// Stop the ProjFS virtualization instance.
    pub fn stop(&self) {
        use windows::Win32::Storage::ProjectedFileSystem::PrjStopVirtualizing;

        if let Some(ctx) = self.namespace_context.lock().unwrap().take() {
            unsafe {
                PrjStopVirtualizing(ctx);
            }
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
