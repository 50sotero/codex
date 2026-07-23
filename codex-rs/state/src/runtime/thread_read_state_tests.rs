use super::*;
use crate::runtime::test_support::test_thread_metadata;
use crate::runtime::test_support::unique_temp_dir;
use pretty_assertions::assert_eq;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::oneshot;

#[tokio::test]
async fn marking_read_tracks_future_updates_and_deduplicates_ids() {
    let codex_home = unique_temp_dir();
    let runtime = StateRuntime::init(codex_home.clone(), "test-provider".to_string())
        .await
        .expect("state db should initialize");
    let thread_id =
        ThreadId::from_string("00000000-0000-7000-8000-000000000111").expect("valid thread id");
    let missing_id =
        ThreadId::from_string("00000000-0000-7000-8000-000000000112").expect("valid thread id");
    let metadata = test_thread_metadata(&codex_home, thread_id, PathBuf::from("/tmp/project"));
    runtime
        .upsert_thread(&metadata)
        .await
        .expect("thread should insert");

    let initial = runtime
        .get_thread_read_state(thread_id)
        .await
        .expect("read state should load")
        .expect("thread should exist");
    assert_eq!(
        initial,
        crate::ThreadReadState {
            thread_id,
            updated_at: metadata.updated_at,
            read_at: None,
        }
    );
    assert!(initial.has_unread());

    let outcome = runtime
        .mark_thread_ids_read(&[thread_id, thread_id, missing_id])
        .await
        .expect("marking read should succeed");
    assert_eq!(outcome.total_count, 1);
    assert_eq!(outcome.marked_count, 1);
    assert_eq!(outcome.already_read_count, 0);
    assert!(outcome.operation_at.is_some());

    let read = runtime
        .get_thread_read_state(thread_id)
        .await
        .expect("read state should load")
        .expect("thread should exist");
    assert_eq!(read.read_at, Some(read.updated_at));
    assert!(!read.has_unread());

    let second = runtime
        .mark_thread_ids_read(&[thread_id])
        .await
        .expect("marking an already-read thread should succeed");
    assert_eq!(second.total_count, 1);
    assert_eq!(second.marked_count, 0);
    assert_eq!(second.already_read_count, 1);
    assert!(second.operation_at.is_some());

    let future_update = read.updated_at + chrono::Duration::milliseconds(1);
    assert!(
        runtime
            .touch_thread_updated_at(thread_id, future_update)
            .await
            .expect("thread update should succeed")
    );
    let unread = runtime
        .get_thread_read_state(thread_id)
        .await
        .expect("read state should load")
        .expect("thread should exist");
    assert!(unread.has_unread());

    let unmatched = runtime
        .mark_thread_ids_read(&[missing_id])
        .await
        .expect("marking a missing thread should succeed");
    assert_eq!(
        unmatched,
        crate::ThreadReadStateMarkAllOutcome {
            total_count: 0,
            marked_count: 0,
            already_read_count: 0,
            operation_at: None,
        }
    );

    runtime.close().await;
    tokio::fs::remove_dir_all(&codex_home)
        .await
        .expect("temporary state directory should be removed");
}

#[tokio::test]
async fn zero_sentinel_is_counted_as_unread_at_the_unix_epoch() {
    let codex_home = unique_temp_dir();
    let runtime = StateRuntime::init(codex_home.clone(), "test-provider".to_string())
        .await
        .expect("state db should initialize");
    let thread_id =
        ThreadId::from_string("00000000-0000-7000-8000-000000000115").expect("valid thread id");
    let metadata = test_thread_metadata(&codex_home, thread_id, PathBuf::from("/tmp/project"));
    runtime
        .upsert_thread(&metadata)
        .await
        .expect("thread should insert");
    sqlx::query("UPDATE threads SET updated_at = 0, updated_at_ms = 0 WHERE id = ?")
        .bind(thread_id.to_string())
        .execute(runtime.pool.as_ref())
        .await
        .expect("thread should move to the Unix epoch");

    let initial = runtime
        .get_thread_read_state(thread_id)
        .await
        .expect("read state should load")
        .expect("thread should exist");
    assert_eq!(initial.read_at, None);
    assert!(initial.has_unread());

    let outcome = runtime
        .mark_thread_ids_read(&[thread_id])
        .await
        .expect("marking read should succeed");
    assert_eq!(outcome.marked_count, 1);
    assert_eq!(outcome.already_read_count, 0);

    let read = runtime
        .get_thread_read_state(thread_id)
        .await
        .expect("read state should load")
        .expect("thread should exist");
    assert_eq!(
        read.read_at,
        DateTime::<Utc>::from_timestamp_millis(1),
        "the persisted marker must not reuse the zero sentinel"
    );
    assert!(!read.has_unread());

    runtime.close().await;
    tokio::fs::remove_dir_all(&codex_home)
        .await
        .expect("temporary state directory should be removed");
}

#[tokio::test]
async fn preallocated_update_that_commits_after_marking_remains_unread() {
    let codex_home = unique_temp_dir();
    let runtime = StateRuntime::init(codex_home.clone(), "test-provider".to_string())
        .await
        .expect("state db should initialize");
    let thread_id =
        ThreadId::from_string("00000000-0000-7000-8000-000000000121").expect("valid thread id");
    let metadata = test_thread_metadata(&codex_home, thread_id, PathBuf::from("/tmp/project"));
    runtime
        .upsert_thread(&metadata)
        .await
        .expect("thread should insert");

    let concurrent_update = metadata.updated_at + chrono::Duration::milliseconds(1);
    let (snapshot_sender, snapshot_receiver) = oneshot::channel();
    let mark_runtime = runtime.clone();
    let mark_task = tokio::spawn(async move {
        mark_runtime
            .mark_thread_ids_read_inner(&[thread_id], Some(snapshot_sender))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), snapshot_receiver)
        .await
        .expect("marking should capture a read snapshot")
        .expect("snapshot observer should be notified");

    let writer_runtime = runtime.clone();
    let writer = tokio::spawn(async move {
        sqlx::query("UPDATE threads SET updated_at = ?, updated_at_ms = ? WHERE id = ?")
            .bind(concurrent_update.timestamp())
            .bind(datetime_to_epoch_millis(concurrent_update))
            .bind(thread_id.to_string())
            .execute(writer_runtime.pool.as_ref())
            .await
    });
    let outcome = mark_task
        .await
        .expect("mark task should not panic")
        .expect("marking read should succeed");
    writer
        .await
        .expect("writer task should not panic")
        .expect("preallocated update should commit after marking");

    assert_eq!(outcome.total_count, 1);
    assert_eq!(outcome.marked_count, 1);

    let state = runtime
        .get_thread_read_state(thread_id)
        .await
        .expect("read state should load")
        .expect("thread should exist");
    assert_eq!(state.updated_at, concurrent_update);
    assert_eq!(
        state.read_at,
        Some(metadata.updated_at),
        "the read marker must stop at the transactionally captured update"
    );
    assert!(state.has_unread());

    runtime.close().await;
    tokio::fs::remove_dir_all(&codex_home)
        .await
        .expect("temporary state directory should be removed");
}

#[tokio::test]
async fn a_failed_later_chunk_rolls_back_every_read_marker() {
    let codex_home = unique_temp_dir();
    let runtime = StateRuntime::init(codex_home.clone(), "test-provider".to_string())
        .await
        .expect("state db should initialize");
    let mut thread_ids = Vec::with_capacity(READ_MARK_CHUNK_SIZE + 1);
    for _ in 0..=READ_MARK_CHUNK_SIZE {
        let thread_id = ThreadId::new();
        let metadata = test_thread_metadata(&codex_home, thread_id, PathBuf::from("/tmp/project"));
        runtime
            .upsert_thread(&metadata)
            .await
            .expect("thread should insert");
        thread_ids.push(thread_id);
    }

    let mut sorted_ids = thread_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    sorted_ids.sort();
    let failing_id = &sorted_ids[READ_MARK_CHUNK_SIZE];
    let trigger_sql = format!(
        r#"
CREATE TRIGGER fail_read_marker
BEFORE UPDATE OF read_at_ms ON threads
WHEN NEW.id = '{failing_id}'
BEGIN
    SELECT RAISE(ABORT, 'injected read marker failure');
END
        "#
    );
    // SAFETY: the only interpolation is a canonical generated ThreadId whose
    // textual form contains only UUID hexadecimal digits and hyphens.
    sqlx::query(sqlx::AssertSqlSafe(trigger_sql.as_str()))
        .execute(runtime.pool.as_ref())
        .await
        .expect("failure trigger should install");

    runtime
        .mark_thread_ids_read(&thread_ids)
        .await
        .expect_err("the injected second-chunk failure should abort marking");

    let marked_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM threads WHERE read_at_ms != 0")
            .fetch_one(runtime.pool.as_ref())
            .await
            .expect("read markers should count");
    assert_eq!(
        marked_count, 0,
        "the first chunk must roll back when a later chunk fails"
    );

    runtime.close().await;
    tokio::fs::remove_dir_all(&codex_home)
        .await
        .expect("temporary state directory should be removed");
}
