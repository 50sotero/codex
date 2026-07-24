use super::*;

/// Persisted read state for one thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadReadState {
    pub thread_id: ThreadId,
    pub updated_at: DateTime<Utc>,
    pub read_at: Option<DateTime<Utc>>,
}

impl ThreadReadState {
    pub fn has_unread(&self) -> bool {
        self.read_at
            .as_ref()
            .is_none_or(|read_at| &self.updated_at > read_at)
    }
}

/// Outcome of atomically marking a set of existing threads as read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadReadStateMarkAllOutcome {
    pub total_count: usize,
    pub marked_count: usize,
    pub already_read_count: usize,
    /// Observational completion time, absent when no requested thread existed.
    pub operation_at: Option<DateTime<Utc>>,
}

impl StateRuntime {
    /// Load the durable read high-water mark for one thread.
    pub async fn get_thread_read_state(
        &self,
        thread_id: ThreadId,
    ) -> anyhow::Result<Option<crate::ThreadReadState>> {
        let row = sqlx::query("SELECT updated_at_ms, read_at_ms FROM threads WHERE id = ?")
            .bind(thread_id.to_string())
            .fetch_optional(self.pool.as_ref())
            .await?;
        row.map(|row| {
            let updated_at = epoch_millis_to_datetime(row.try_get("updated_at_ms")?)?;
            let read_at_ms: i64 = row.try_get("read_at_ms")?;
            let read_at =
                match read_at_ms {
                    0 => None,
                    value => Some(DateTime::<Utc>::from_timestamp_millis(value).ok_or_else(
                        || anyhow::anyhow!("invalid read marker unix timestamp millis: {value}"),
                    )?),
                };
            Ok(crate::ThreadReadState {
                thread_id,
                updated_at,
                read_at,
            })
        })
        .transpose()
    }

    /// Mark existing requested threads read through one writer-serialized snapshot.
    pub async fn mark_thread_ids_read(
        &self,
        thread_ids: &[ThreadId],
    ) -> anyhow::Result<crate::ThreadReadStateMarkAllOutcome> {
        let thread_ids = thread_ids
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>();
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let mut total_count = 0;
        let mut marked_count = 0;

        for thread_id in thread_ids {
            let row = sqlx::query("SELECT updated_at_ms, read_at_ms FROM threads WHERE id = ?")
                .bind(&thread_id)
                .fetch_optional(&mut *transaction)
                .await?;
            let Some(row) = row else {
                continue;
            };
            let updated_at = epoch_millis_to_datetime(row.try_get::<i64, _>("updated_at_ms")?)?;
            let marker = match datetime_to_epoch_millis(updated_at) {
                0 => 1,
                value => value,
            };
            let previous: i64 = row.try_get("read_at_ms")?;
            total_count += 1;
            if previous == 0 || previous < marker {
                marked_count += 1;
            }
            sqlx::query("UPDATE threads SET read_at_ms = MAX(read_at_ms, ?) WHERE id = ?")
                .bind(marker)
                .bind(thread_id)
                .execute(&mut *transaction)
                .await?;
        }
        transaction.commit().await?;

        Ok(crate::ThreadReadStateMarkAllOutcome {
            total_count,
            marked_count,
            already_read_count: total_count - marked_count,
            operation_at: (total_count != 0).then(Utc::now),
        })
    }
}

#[cfg(test)]
#[path = "thread_read_state_tests.rs"]
mod tests;
