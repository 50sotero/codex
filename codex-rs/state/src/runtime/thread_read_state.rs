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
}

#[cfg(test)]
#[path = "thread_read_state_tests.rs"]
mod tests;
