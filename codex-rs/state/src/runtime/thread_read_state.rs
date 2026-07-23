use super::*;

const READ_MARK_CHUNK_SIZE: usize = 500;

#[derive(Debug)]
struct ThreadReadSnapshotRow {
    thread_id: String,
    read_marker_ms: i64,
}

impl StateRuntime {
    /// Load the durable read high-water mark for one thread.
    pub async fn get_thread_read_state(
        &self,
        thread_id: ThreadId,
    ) -> anyhow::Result<Option<crate::ThreadReadState>> {
        let row = sqlx::query(
            r#"
SELECT updated_at_ms, read_at_ms
FROM threads
WHERE id = ?
            "#,
        )
        .bind(thread_id.to_string())
        .fetch_optional(self.pool.as_ref())
        .await?;

        row.map(|row| {
            let updated_at = epoch_millis_to_datetime(row.try_get("updated_at_ms")?)?;
            let read_at_ms: i64 = row.try_get("read_at_ms")?;
            let read_at = match read_at_ms {
                0 => None,
                read_at_ms => Some(
                    DateTime::<Utc>::from_timestamp_millis(read_at_ms).ok_or_else(|| {
                        anyhow::anyhow!("invalid read marker unix timestamp millis: {read_at_ms}")
                    })?,
                ),
            };
            Ok(crate::ThreadReadState {
                thread_id,
                updated_at,
                read_at,
            })
        })
        .transpose()
    }

    /// Mark the provided thread ids as read through a consistent snapshot.
    ///
    /// An immediate transaction captures each matched row's `updated_at_ms`
    /// and advances markers only through those captured values. Because the
    /// snapshot and writes share one writer-serialized transaction, an update
    /// that commits afterward remains unread even if its timestamp was
    /// allocated before this operation began. All chunks commit or roll back
    /// together.
    pub async fn mark_thread_ids_read(
        &self,
        thread_ids: &[ThreadId],
    ) -> anyhow::Result<crate::ThreadReadStateMarkAllOutcome> {
        self.mark_thread_ids_read_inner(thread_ids, None).await
    }

    async fn mark_thread_ids_read_inner(
        &self,
        thread_ids: &[ThreadId],
        snapshot_observer: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> anyhow::Result<crate::ThreadReadStateMarkAllOutcome> {
        let ids = dedup_thread_ids(thread_ids);
        if ids.is_empty() {
            return Ok(crate::ThreadReadStateMarkAllOutcome {
                total_count: 0,
                marked_count: 0,
                already_read_count: 0,
                operation_at: None,
            });
        }

        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let mut snapshot = Vec::with_capacity(ids.len());
        let mut marked_count = 0usize;
        for chunk in ids.chunks(READ_MARK_CHUNK_SIZE) {
            let mut query =
                QueryBuilder::<Sqlite>::new("SELECT id, updated_at_ms, read_at_ms FROM threads");
            push_id_filter(&mut query, chunk);
            let rows = query.build().fetch_all(&mut *transaction).await?;
            for row in rows {
                let thread_id: String = row.try_get("id")?;
                let updated_at_ms: i64 = row.try_get("updated_at_ms")?;
                let read_at_ms: i64 = row.try_get("read_at_ms")?;
                let read_marker_ms = read_marker_for_updated_at(updated_at_ms);
                if read_at_ms == 0 || read_marker_ms > read_at_ms {
                    marked_count += 1;
                }
                snapshot.push(ThreadReadSnapshotRow {
                    thread_id,
                    read_marker_ms,
                });
            }
        }

        if let Some(snapshot_observer) = snapshot_observer {
            let _ = snapshot_observer.send(());
        }

        let total_count = snapshot.len();
        if snapshot.is_empty() {
            transaction.commit().await?;
            return Ok(crate::ThreadReadStateMarkAllOutcome {
                total_count: 0,
                marked_count: 0,
                already_read_count: 0,
                operation_at: None,
            });
        }

        for chunk in snapshot.chunks(READ_MARK_CHUNK_SIZE) {
            let mut update = QueryBuilder::<Sqlite>::new(
                "WITH read_snapshot(thread_id, read_marker_ms) AS (VALUES ",
            );
            for (index, row) in chunk.iter().enumerate() {
                if index > 0 {
                    update.push(", ");
                }
                update
                    .push("(")
                    .push_bind(row.thread_id.as_str())
                    .push(", ")
                    .push_bind(row.read_marker_ms)
                    .push(")");
            }
            update.push(
                r#")
UPDATE threads AS target
SET read_at_ms = CASE
    WHEN target.read_at_ms = 0
      OR target.read_at_ms < read_snapshot.read_marker_ms
        THEN read_snapshot.read_marker_ms
    ELSE target.read_at_ms
END
FROM read_snapshot
WHERE target.id = read_snapshot.thread_id
                "#,
            );
            update.build().execute(&mut *transaction).await?;
        }
        transaction.commit().await?;

        Ok(crate::ThreadReadStateMarkAllOutcome {
            total_count,
            marked_count,
            already_read_count: total_count - marked_count,
            operation_at: Some(Utc::now()),
        })
    }
}

fn read_marker_for_updated_at(updated_at_ms: i64) -> i64 {
    let updated_at_ms = normalize_epoch_millis(updated_at_ms);
    if updated_at_ms == 0 { 1 } else { updated_at_ms }
}

fn dedup_thread_ids(thread_ids: &[ThreadId]) -> Vec<String> {
    thread_ids
        .iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn push_id_filter(builder: &mut QueryBuilder<Sqlite>, thread_ids: &[String]) {
    builder.push(" WHERE id IN (");
    let mut separated = builder.separated(", ");
    for thread_id in thread_ids {
        separated.push_bind(thread_id);
    }
    separated.push_unseparated(")");
}

#[cfg(test)]
#[path = "thread_read_state_tests.rs"]
mod tests;
