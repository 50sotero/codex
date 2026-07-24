use super::*;
use crate::runtime::test_support::test_thread_metadata;
use crate::runtime::test_support::unique_temp_dir;
use pretty_assertions::assert_eq;

fn thread_id(index: u64) -> ThreadId {
    ThreadId::from_string(&format!("00000000-0000-7000-8000-{index:012}")).expect("valid thread id")
}

async fn runtime() -> (Arc<StateRuntime>, PathBuf) {
    let home = unique_temp_dir();
    let runtime = StateRuntime::init(home.clone(), "provider".to_string())
        .await
        .expect("state DB");
    (runtime, home)
}

async fn insert(runtime: &StateRuntime, home: &Path, index: u64) -> ThreadId {
    let id = thread_id(index);
    runtime
        .upsert_thread(&test_thread_metadata(
            home,
            id,
            PathBuf::from("/tmp/project"),
        ))
        .await
        .expect("insert thread");
    id
}

async fn close(runtime: &StateRuntime, home: &Path) {
    runtime.close().await;
    tokio::fs::remove_dir_all(home)
        .await
        .expect("remove state DB");
}

#[tokio::test]
async fn mark_read_deduplicates_and_reports_missing_or_already_read_threads() {
    let (runtime, home) = runtime().await;
    let id = insert(&runtime, &home, 1).await;
    let missing = thread_id(2);
    let initial = runtime
        .get_thread_read_state(id)
        .await
        .expect("read state")
        .expect("thread");
    assert!(initial.has_unread());

    let first = runtime
        .mark_thread_ids_read(&[id, id, missing])
        .await
        .expect("mark read");
    assert_eq!(
        (
            first.total_count,
            first.marked_count,
            first.already_read_count,
            first.operation_at.is_some(),
        ),
        (1, 1, 0, true)
    );
    let current = runtime
        .get_thread_read_state(id)
        .await
        .expect("read state")
        .expect("thread");
    assert_eq!(current.read_at, Some(current.updated_at));
    assert!(!current.has_unread());

    let second = runtime
        .mark_thread_ids_read(&[id])
        .await
        .expect("mark read again");
    assert_eq!((second.total_count, second.marked_count), (1, 0));
    assert_eq!(
        runtime
            .mark_thread_ids_read(&[missing])
            .await
            .expect("mark missing"),
        crate::ThreadReadStateMarkAllOutcome {
            total_count: 0,
            marked_count: 0,
            already_read_count: 0,
            operation_at: None,
        }
    );
    close(&runtime, &home).await;
}

#[tokio::test]
async fn a_later_snapshot_becomes_unread_after_marking() {
    let (runtime, home) = runtime().await;
    let id = insert(&runtime, &home, 3).await;
    runtime
        .mark_thread_ids_read(&[id])
        .await
        .expect("mark read");
    let mut metadata = test_thread_metadata(&home, id, PathBuf::from("/tmp/project"));
    metadata.updated_at += chrono::Duration::seconds(1);
    runtime
        .upsert_thread(&metadata)
        .await
        .expect("update snapshot");
    let current = runtime
        .get_thread_read_state(id)
        .await
        .expect("read state")
        .expect("thread");
    assert_eq!(current.read_at, None);
    assert!(current.has_unread());
    close(&runtime, &home).await;
}
