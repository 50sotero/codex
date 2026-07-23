use chrono::DateTime;
use chrono::Utc;
use codex_protocol::ThreadId;

/// Persisted read state for a thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadReadState {
    /// Thread whose read state was loaded.
    pub thread_id: ThreadId,
    /// Latest persisted thread update.
    pub updated_at: DateTime<Utc>,
    /// Last persisted read high-water mark, or `None` if never marked read.
    pub read_at: Option<DateTime<Utc>>,
}

impl ThreadReadState {
    /// Return whether the thread has never been read or has a newer update.
    pub fn has_unread(&self) -> bool {
        match self.read_at.as_ref() {
            Some(read_at) => &self.updated_at > read_at,
            None => true,
        }
    }
}

/// Snapshot-based outcome of marking a set of threads as read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadReadStateMarkAllOutcome {
    /// Existing threads present in the operation's read snapshot.
    pub total_count: usize,
    /// Snapshot threads that were unread before the operation.
    pub marked_count: usize,
    /// Snapshot threads that were already read before the operation.
    pub already_read_count: usize,
    /// Observational wall-clock completion time, if at least one row matched.
    ///
    /// This is not a shared read marker. Each thread is advanced only through
    /// the `updated_at` value captured for it in the operation's read snapshot.
    pub operation_at: Option<DateTime<Utc>>,
}
