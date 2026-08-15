use std::sync::{Arc, Weak};

const WIN32_ERROR_ALREADY_EXISTS: u32 = 183;

pub(crate) const HRESULT_ALREADY_EXISTS: i32 =
    ((WIN32_ERROR_ALREADY_EXISTS & 0x0000_FFFF) | 0x8007_0000) as i32;

pub(crate) const fn is_already_exists_hresult(code: i32) -> bool {
    code == HRESULT_ALREADY_EXISTS
}

/// Stable callback-owned weak reference. ProjFS owns the address of this box
/// only while virtualization is active; every callback upgrades the weak
/// reference so the provider stays alive for that callback without a cycle.
// [spec:box:req:projfs-provider.root.lifecycle]
pub(crate) struct CallbackOwner<T> {
    target: Weak<T>,
}

impl<T> CallbackOwner<T> {
    pub(crate) fn new(target: &Arc<T>) -> Self {
        Self {
            target: Arc::downgrade(target),
        }
    }

    pub(crate) fn upgrade(&self) -> Option<Arc<T>> {
        self.target.upgrade()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // [spec:box:req:projfs-provider.root.lifecycle/test/unit]
    #[test]
    fn recognizes_only_already_exists() {
        assert!(is_already_exists_hresult(0x8007_00B7_u32 as i32));
        assert!(!is_already_exists_hresult(0));
        assert!(!is_already_exists_hresult(0x8000_4005_u32 as i32));
    }

    // [spec:box:req:projfs-provider.root.lifecycle/test/unit]
    #[test]
    fn callback_owner_upgrades_without_a_cycle() {
        let provider = Arc::new(42u8);
        let callback_owner = CallbackOwner::new(&provider);

        let callback_reference = callback_owner.upgrade().unwrap();
        drop(provider);
        assert_eq!(*callback_reference, 42);
        drop(callback_reference);
        assert!(callback_owner.upgrade().is_none());
    }
}
