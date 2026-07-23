use super::*;
use crate::runtime::test_support::test_thread_metadata;
use crate::runtime::test_support::unique_temp_dir;
use pretty_assertions::assert_eq;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::oneshot;

fn test_thread_id(index: u64) -> ThreadId {
    ThreadId::from_string(&format!("00000000-0000-7000-8000-{index:012}")).expect("valid thread id")
}

async fn test_runtime() -> (std::sync::Arc<StateRuntime>, PathBuf) {
    let codex_home = unique_temp_dir();
    let runtime = StateRuntime::init(codex_home.clone(), "test-provider".to_string())
        .await
        .expect("state db should initialize");
    (runtime, codex_home)
}

async fn insert_test_thread(
    runtime: &StateRuntime,
    codex_home: &Path,
    index: u64,
) -> (ThreadId, crate::ThreadMetadata) {
    let thread_id = test_thread_id(index);
    let metadata = test_thread_metadata(codex_home, thread_id, PathBuf::from("/tmp/project"));
    runtime
        .upsert_thread(&metadata)
        .await
        .expect("thread should insert");
    (thread_id, metadata)
}

async fn read_state(runtime: &StateRuntime, thread_id: ThreadId) -> crate::ThreadReadState {
    runtime
        .get_thread_read_state(thread_id)
        .await
        .expect("read state should load")
        .expect("thread should exist")
}

async fn mark_read(
    runtime: &StateRuntime,
    thread_ids: &[ThreadId],
) -> crate::ThreadReadStateMarkAllOutcome {
    runtime
        .mark_thread_ids_read(thread_ids)
        .await
        .expect("marking read should succeed")
}

fn assert_outcome(
    outcome: &crate::ThreadReadStateMarkAllOutcome,
    total_count: usize,
    marked_count: usize,
) {
    let expected = (
        total_count,
        marked_count,
        total_count - marked_count,
        total_count != 0,
    );
    assert_eq!(
        (
            outcome.total_count,
            outcome.marked_count,
            outcome.already_read_count,
            outcome.operation_at.is_some(),
        ),
        expected
    );
}

async fn close_runtime(runtime: &StateRuntime, codex_home: &Path) {
    runtime.close().await;
    tokio::fs::remove_dir_all(codex_home)
        .await
        .expect("temporary state directory should be removed");
}

#[tokio::test]
async fn marking_read_tracks_state_and_deduplicates_ids() {
    let (runtime, codex_home) = test_runtime().await;
    let (thread_id, metadata) = insert_test_thread(&runtime, &codex_home, 111).await;
    let missing_id = test_thread_id(112);
    let initial = read_state(&runtime, thread_id).await;
    assert_eq!(
        initial,
        crate::ThreadReadState {
            thread_id,
            updated_at: metadata.updated_at,
            read_at: None,
        }
    );
    assert!(initial.has_unread());

    let first = mark_read(&runtime, &[thread_id, thread_id, missing_id]).await;
    assert_outcome(&first, 1, 1);
    let read = read_state(&runtime, thread_id).await;
    assert_eq!(read.read_at, Some(read.updated_at));
    assert!(!read.has_unread());

    assert_outcome(&mark_read(&runtime, &[thread_id]).await, 1, 0);
    assert_eq!(
        mark_read(&runtime, &[missing_id]).await,
        crate::ThreadReadStateMarkAllOutcome {
            total_count: 0,
            marked_count: 0,
            already_read_count: 0,
            operation_at: None,
        }
    );
    close_runtime(&runtime, &codex_home).await;
}

#[tokio::test]
async fn reconciliation_preserves_idempotent_and_unknown_source_read_state() {
    let (runtime, codex_home) = test_runtime().await;
    let (thread_id, metadata) = insert_test_thread(&runtime, &codex_home, 113).await;
    mark_read(&runtime, &[thread_id]).await;
    let before = read_state(&runtime, thread_id).await;
    runtime
        .upsert_thread(&metadata)
        .await
        .expect("idempotent reconcile should succeed");
    assert_eq!(read_state(&runtime, thread_id).await, before);

    sqlx::query(
        "UPDATE threads SET source_updated_at_ms = NULL, updated_at_ms = updated_at_ms + 1 \
         WHERE id = ?",
    )
    .bind(thread_id.to_string())
    .execute(runtime.pool.as_ref())
    .await
    .expect("legacy state should be simulated");
    mark_read(&runtime, &[thread_id]).await;
    let before_unknown_source = read_state(&runtime, thread_id).await;
    runtime
        .upsert_thread(&metadata)
        .await
        .expect("unknown source timestamp should initialize");
    assert_eq!(
        read_state(&runtime, thread_id).await,
        before_unknown_source,
        "initializing an unknown raw source timestamp must not manufacture a snapshot"
    );
    let source_updated_at_ms =
        sqlx::query_scalar::<_, i64>("SELECT source_updated_at_ms FROM threads WHERE id = ?")
            .bind(thread_id.to_string())
            .fetch_one(runtime.pool.as_ref())
            .await
            .expect("source timestamp should load");
    assert_eq!(
        source_updated_at_ms,
        datetime_to_epoch_millis(metadata.updated_at)
    );

    let mut changed = metadata;
    changed.preview = Some("equal timestamp snapshot".to_string());
    runtime
        .upsert_thread(&changed)
        .await
        .expect("equal timestamp snapshot should persist");
    assert!(read_state(&runtime, thread_id).await.has_unread());
    mark_read(&runtime, &[thread_id]).await;
    changed.updated_at -= chrono::Duration::seconds(10);
    changed.preview = Some("regressive timestamp snapshot".to_string());
    runtime
        .upsert_thread(&changed)
        .await
        .expect("regressive snapshot should persist");
    assert!(read_state(&runtime, thread_id).await.has_unread());
    close_runtime(&runtime, &codex_home).await;
}

#[tokio::test]
async fn legacy_timestamp_writes_normalize_and_reset_markers() {
    let (runtime, codex_home) = test_runtime().await;
    let (thread_id, _) = insert_test_thread(&runtime, &codex_home, 116).await;
    let legacy_seconds = 1_700_000_000_i64;
    sqlx::query("UPDATE threads SET updated_at = ?, updated_at_ms = ? WHERE id = ?")
        .bind(legacy_seconds)
        .bind(legacy_seconds)
        .bind(thread_id.to_string())
        .execute(runtime.pool.as_ref())
        .await
        .expect("thread should use a legacy second-precision raw value");
    mark_read(&runtime, &[thread_id]).await;
    let read = read_state(&runtime, thread_id).await;
    assert_eq!(
        read.read_at,
        DateTime::<Utc>::from_timestamp_millis(1_700_000_000_000)
    );
    assert!(!read.has_unread());

    sqlx::query("UPDATE threads SET updated_at = updated_at, preview = ? WHERE id = ?")
        .bind("legacy equal-second snapshot")
        .bind(thread_id.to_string())
        .execute(runtime.pool.as_ref())
        .await
        .expect("equal-second legacy snapshot should persist");
    assert!(read_state(&runtime, thread_id).await.has_unread());
    close_runtime(&runtime, &codex_home).await;
}

#[tokio::test]
async fn unix_epoch_marker_does_not_hide_a_later_millisecond() {
    let (runtime, codex_home) = test_runtime().await;
    let (thread_id, _) = insert_test_thread(&runtime, &codex_home, 115).await;
    sqlx::query("UPDATE threads SET updated_at = 0, updated_at_ms = 0 WHERE id = ?")
        .bind(thread_id.to_string())
        .execute(runtime.pool.as_ref())
        .await
        .expect("thread should move to the Unix epoch");
    assert!(read_state(&runtime, thread_id).await.has_unread());
    assert_outcome(&mark_read(&runtime, &[thread_id]).await, 1, 1);
    let read = read_state(&runtime, thread_id).await;
    assert_eq!(read.read_at, DateTime::<Utc>::from_timestamp_millis(1));
    assert!(!read.has_unread());

    sqlx::query("UPDATE threads SET updated_at_ms = 1 WHERE id = ?")
        .bind(thread_id.to_string())
        .execute(runtime.pool.as_ref())
        .await
        .expect("post-read millisecond update should persist");
    let later = read_state(&runtime, thread_id).await;
    assert_eq!(later.read_at, None);
    assert!(later.has_unread());
    close_runtime(&runtime, &codex_home).await;
}

#[tokio::test]
async fn post_snapshot_writes_remain_unread_for_equal_and_regressive_timestamps() {
    let (runtime, codex_home) = test_runtime().await;
    for (index, timestamp_delta_ms) in [(121, 1_i64), (122, 0), (123, -1)] {
        let (thread_id, metadata) = insert_test_thread(&runtime, &codex_home, index).await;
        let concurrent_update =
            metadata.updated_at + chrono::Duration::milliseconds(timestamp_delta_ms);
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
        assert_outcome(
            &mark_task
                .await
                .expect("mark task should not panic")
                .expect("marking read should succeed"),
            1,
            1,
        );
        writer
            .await
            .expect("writer task should not panic")
            .expect("post-snapshot update should commit after marking");
        let state = read_state(&runtime, thread_id).await;
        assert_eq!(state.updated_at, concurrent_update);
        assert_eq!(state.read_at, None);
        assert!(state.has_unread());
    }
    close_runtime(&runtime, &codex_home).await;
}

#[tokio::test]
async fn a_failed_later_chunk_rolls_back_every_read_marker() {
    let (runtime, codex_home) = test_runtime().await;
    let mut thread_ids = Vec::with_capacity(READ_MARK_CHUNK_SIZE + 1);
    for index in 1..=(READ_MARK_CHUNK_SIZE + 1) {
        thread_ids.push(
            insert_test_thread(&runtime, &codex_home, 1_000 + index as u64)
                .await
                .0,
        );
    }
    let mut sorted_ids = thread_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    sorted_ids.sort();
    let failing_id = &sorted_ids[READ_MARK_CHUNK_SIZE];
    let trigger_sql = format!(
        "CREATE TRIGGER fail_read_marker BEFORE UPDATE OF read_at_ms ON threads \
         WHEN NEW.id = '{failing_id}' BEGIN \
         SELECT RAISE(ABORT, 'injected read marker failure'); END"
    );
    // SAFETY: a canonical generated ThreadId contains only UUID hex digits and hyphens.
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
    assert_eq!(marked_count, 0);
    close_runtime(&runtime, &codex_home).await;
}
