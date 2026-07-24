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
async fn loads_read_state_for_persisted_threads() {
    let (runtime, home) = runtime().await;
    let id = insert(&runtime, &home, 1).await;
    let state = runtime
        .get_thread_read_state(id)
        .await
        .expect("read state")
        .expect("thread");
    assert_eq!(
        state,
        crate::ThreadReadState {
            thread_id: id,
            updated_at: test_thread_metadata(&home, id, PathBuf::from("/tmp/project")).updated_at,
            read_at: None,
        }
    );
    assert!(state.has_unread());
    assert_eq!(
        runtime
            .get_thread_read_state(thread_id(2))
            .await
            .expect("missing read state"),
        None
    );
    close(&runtime, &home).await;
}

#[tokio::test]
async fn has_unread_compares_the_persisted_high_water_marks() {
    let (runtime, home) = runtime().await;
    let id = insert(&runtime, &home, 3).await;
    let updated_at = runtime
        .get_thread_read_state(id)
        .await
        .expect("read state")
        .expect("thread")
        .updated_at;
    let read = crate::ThreadReadState {
        thread_id: id,
        updated_at,
        read_at: Some(updated_at),
    };
    assert!(!read.has_unread());
    assert!(
        crate::ThreadReadState {
            updated_at: updated_at + chrono::Duration::milliseconds(1),
            ..read
        }
        .has_unread()
    );
    close(&runtime, &home).await;
}
