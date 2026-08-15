use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use crate::path::BoxPath;

pub(super) struct CompressionTestProbe {
    active: AtomicUsize,
    max_active: AtomicUsize,
    prepared_count: AtomicUsize,
    deferred_path: Mutex<Option<BoxPath<'static>>>,
    work_paths: Mutex<Vec<BoxPath<'static>>>,
    preparation_order: Mutex<Vec<BoxPath<'static>>>,
    preparation_notify: tokio::sync::Notify,
}

impl Default for CompressionTestProbe {
    fn default() -> Self {
        Self {
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            prepared_count: AtomicUsize::new(0),
            deferred_path: Mutex::new(None),
            work_paths: Mutex::new(Vec::new()),
            preparation_order: Mutex::new(Vec::new()),
            preparation_notify: tokio::sync::Notify::new(),
        }
    }
}

impl CompressionTestProbe {
    pub(super) fn defer_until_another_preparation(&self, path: BoxPath<'static>) {
        *self.deferred_path.lock().unwrap() = Some(path);
    }

    pub(super) fn enter(self: &Arc<Self>, path: &BoxPath<'static>) -> CompressionTestActivity {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        self.work_paths.lock().unwrap().push(path.clone());
        CompressionTestActivity {
            probe: self.clone(),
        }
    }

    pub(super) async fn wait_if_deferred(&self, path: &BoxPath<'static>) {
        let is_deferred = self.deferred_path.lock().unwrap().as_ref() == Some(path);
        if !is_deferred {
            return;
        }

        while self.prepared_count.load(Ordering::SeqCst) == 0 {
            let notified = self.preparation_notify.notified();
            if self.prepared_count.load(Ordering::SeqCst) != 0 {
                break;
            }
            notified.await;
        }
    }

    pub(super) fn record_prepared(&self, path: BoxPath<'static>) {
        self.preparation_order.lock().unwrap().push(path);
        self.prepared_count.fetch_add(1, Ordering::SeqCst);
        self.preparation_notify.notify_waiters();
    }

    pub(super) fn max_active(&self) -> usize {
        self.max_active.load(Ordering::SeqCst)
    }

    pub(super) fn work_paths(&self) -> Vec<BoxPath<'static>> {
        self.work_paths.lock().unwrap().clone()
    }

    pub(super) fn preparation_order(&self) -> Vec<BoxPath<'static>> {
        self.preparation_order.lock().unwrap().clone()
    }
}

pub(super) struct CompressionTestActivity {
    probe: Arc<CompressionTestProbe>,
}

impl Drop for CompressionTestActivity {
    fn drop(&mut self) {
        self.probe.active.fetch_sub(1, Ordering::SeqCst);
    }
}
