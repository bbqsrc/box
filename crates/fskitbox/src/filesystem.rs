//! BoxFS: FSUnaryFileSystem implementation for Box archives.

use std::cell::RefCell;
use std::path::PathBuf;
use std::ptr;

use objc2::rc::Retained;
use objc2::runtime::NSObject;
use objc2::{AllocAnyThread, ClassType, DefinedClass, define_class, msg_send};
use objc2_foundation::{NSError, NSString, NSUUID};

use box_format::BoxFileReader;

use crate::bindings::{
    FSContainerIdentifier, FSContainerState, FSGenericURLResource, FSMatchResult, FSProbeResult,
    FSResource, FSTaskOptions, FSUnaryFileSystem,
};
use crate::volume::BoxVolume;

/// Box file magic bytes: \xffBOX
const BOX_MAGIC: &[u8; 4] = b"\xffBOX";

/// Internal state for BoxFS.
pub struct BoxFSIvars {
    /// Currently loaded volume.
    pub volume: RefCell<Option<Retained<BoxVolume>>>,
    /// Container identifier for this filesystem.
    pub container_id: Retained<FSContainerIdentifier>,
    /// Tokio runtime for async operations.
    pub runtime: tokio::runtime::Runtime,
}

impl Default for BoxFSIvars {
    fn default() -> Self {
        let uuid = NSUUID::new();
        let container_id = FSContainerIdentifier::with_uuid(&uuid);
        let runtime = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
        Self {
            volume: RefCell::new(None),
            container_id,
            runtime,
        }
    }
}

define_class! {
    /// Filesystem implementation for Box archives.
    ///
    /// Implements FSUnaryFileSystemOperations to handle probing,
    /// loading, and unloading of Box archive files.
    #[unsafe(super(FSUnaryFileSystem, NSObject))]
    #[name = "BoxFS"]
    #[ivars = BoxFSIvars]
    pub struct BoxFS;

    impl BoxFS {
        // FSUnaryFileSystemOperations

        #[unsafe(method(probeResource:replyHandler:))]
        fn probe_resource(
            &self,
            resource: *mut FSResource,
            reply: *const std::ffi::c_void,
        ) {
            tracing::debug!("BoxFS: probeResource called");

            let result = if resource.is_null() {
                tracing::debug!("BoxFS: resource is null");
                FSProbeResult::not_recognized()
            } else {
                // Try to get the path from the resource
                let path = unsafe { self.get_resource_path(resource) };

                match path {
                    Some(path) => {
                        tracing::debug!("BoxFS: probing path {:?}", path);
                        if self.probe_box_file(&path) {
                            tracing::info!("BoxFS: recognized Box archive at {:?}", path);
                            let name = path
                                .file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or("Box Archive");
                            let name = NSString::from_str(name);
                            FSProbeResult::with_result_name_container(
                                FSMatchResult::Usable,
                                &name,
                                &self.ivars().container_id,
                            )
                        } else {
                            tracing::debug!("BoxFS: not a Box archive");
                            FSProbeResult::not_recognized()
                        }
                    }
                    None => {
                        tracing::debug!("BoxFS: could not get path from resource");
                        FSProbeResult::not_recognized()
                    }
                }
            };

            // Call the reply handler
            type ReplyBlock = block2::Block<dyn Fn(*mut FSProbeResult, *mut NSError)>;
            unsafe {
                let reply = reply as *const ReplyBlock;
                if !reply.is_null() {
                    (*reply).call((&*result as *const _ as *mut _, ptr::null_mut()));
                }
            }
        }

        #[unsafe(method(loadResource:options:replyHandler:))]
        fn load_resource(
            &self,
            resource: *mut FSResource,
            options: *mut FSTaskOptions,
            reply: *const std::ffi::c_void,
        ) {
            tracing::debug!("BoxFS: loadResource called");

            type ReplyBlock = block2::Block<dyn Fn(*mut NSObject, *mut NSError)>;

            // Check options
            let is_read_only = if options.is_null() {
                true
            } else {
                unsafe { (*options).is_read_only() }
            };

            if !is_read_only {
                tracing::warn!("BoxFS: write access requested but Box archives are read-only");
            }

            // Get the path from the resource
            let path = unsafe { self.get_resource_path(resource) };

            match path {
                Some(path) => {
                    tracing::info!("BoxFS: loading Box archive at {:?}", path);

                    // Open the Box file
                    let result = self.ivars().runtime.block_on(BoxFileReader::open(&path));

                    match result {
                        Ok(reader) => {
                            tracing::info!("BoxFS: successfully opened Box archive");

                            // Create the volume
                            let volume = BoxVolume::new_with_reader(reader);

                            // Store the volume
                            *self.ivars().volume.borrow_mut() = Some(volume.clone());

                            // Set container state to ready
                            self.set_container_state(FSContainerState::Ready);

                            // Return the volume
                            unsafe {
                                let reply = reply as *const ReplyBlock;
                                if !reply.is_null() {
                                    (*reply).call((
                                        &*volume as *const BoxVolume as *mut NSObject,
                                        ptr::null_mut(),
                                    ));
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("BoxFS: failed to open Box archive: {}", e);
                            self.reply_with_error(reply, &format!("Failed to open: {}", e));
                        }
                    }
                }
                None => {
                    tracing::error!("BoxFS: could not get path from resource");
                    self.reply_with_error(reply, "Could not get path from resource");
                }
            }
        }

        #[unsafe(method(unloadResource:options:replyHandler:))]
        fn unload_resource(
            &self,
            _resource: *mut FSResource,
            _options: *mut FSTaskOptions,
            reply: *const std::ffi::c_void,
        ) {
            tracing::debug!("BoxFS: unloadResource called");

            // Clear the loaded volume
            *self.ivars().volume.borrow_mut() = None;

            // Set container state back to not ready
            self.set_container_state(FSContainerState::NotReady);

            // Call reply with no error
            type ReplyBlock = block2::Block<dyn Fn(*mut NSError)>;
            unsafe {
                let reply = reply as *const ReplyBlock;
                if !reply.is_null() {
                    (*reply).call((ptr::null_mut(),));
                }
            }
        }

        #[unsafe(method(didFinishLoading))]
        fn did_finish_loading(&self) {
            tracing::debug!("BoxFS: didFinishLoading called");
        }
    }
}

impl BoxFS {
    /// Create a new BoxFS instance.
    pub fn new() -> Retained<Self> {
        let this = Self::alloc().set_ivars(BoxFSIvars::default());
        unsafe { msg_send![super(this), init] }
    }

    /// Set the container state.
    fn set_container_state(&self, state: FSContainerState) {
        unsafe {
            let _: () = msg_send![self, setContainerState: state];
        }
    }

    /// Get the path from an FSResource.
    ///
    /// # Safety
    /// The resource pointer must be valid.
    unsafe fn get_resource_path(&self, resource: *mut FSResource) -> Option<PathBuf> {
        if resource.is_null() {
            return None;
        }

        // Try to cast to FSGenericURLResource
        let url_resource = resource as *mut FSGenericURLResource;

        // Check if it's actually an FSGenericURLResource by checking class
        let is_url_resource: bool = unsafe {
            msg_send![
                resource as *mut NSObject,
                isKindOfClass: FSGenericURLResource::class()
            ]
        };

        if is_url_resource {
            let url = unsafe { (*url_resource).url() };
            // Get the file system path from the URL
            let path: Option<Retained<NSString>> = unsafe { msg_send![&*url, path] };
            path.map(|p| PathBuf::from(p.to_string()))
        } else {
            tracing::debug!("BoxFS: resource is not a URL resource");
            None
        }
    }

    /// Probe a file to check if it's a valid Box archive.
    fn probe_box_file(&self, path: &PathBuf) -> bool {
        // Read the first 4 bytes to check the magic
        match std::fs::File::open(path) {
            Ok(mut file) => {
                use std::io::Read;
                let mut magic = [0u8; 4];
                match file.read_exact(&mut magic) {
                    Ok(()) => magic == *BOX_MAGIC,
                    Err(_) => false,
                }
            }
            Err(_) => false,
        }
    }

    /// Reply with an error.
    fn reply_with_error(&self, reply: *const std::ffi::c_void, message: &str) {
        type ReplyBlock = block2::Block<dyn Fn(*mut NSObject, *mut NSError)>;
        unsafe {
            let reply = reply as *const ReplyBlock;
            if !reply.is_null() {
                let error_domain = NSString::from_str("BoxFSErrorDomain");
                let error_msg = NSString::from_str(message);
                let user_info: *mut NSObject = msg_send![
                    objc2_foundation::NSDictionary::<NSString, NSObject>::class(),
                    dictionaryWithObject: &*error_msg,
                    forKey: objc2_foundation::NSLocalizedDescriptionKey
                ];
                let error: Retained<NSError> = msg_send![
                    NSError::class(),
                    errorWithDomain: &*error_domain,
                    code: 1i64,
                    userInfo: user_info
                ];
                (*reply).call((ptr::null_mut(), &*error as *const _ as *mut _));
            }
        }
    }
}

impl Default for BoxFS {
    fn default() -> Self {
        unimplemented!("Use BoxFS::new() to create instances")
    }
}
