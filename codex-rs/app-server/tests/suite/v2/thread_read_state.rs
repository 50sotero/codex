use anyhow::Result;
use app_test_support::TestAppServer;
use app_test_support::create_fake_rollout;
use app_test_support::create_mock_responses_server_repeating_assistant;
use app_test_support::rollout_path;
use app_test_support::to_response;
use app_test_support::write_mock_responses_config_toml;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadListParams;
use codex_app_server_protocol::ThreadListResponse;
use codex_app_server_protocol::ThreadReadStateMarkAllParams;
use codex_app_server_protocol::ThreadReadStateMarkAllResponse;
use codex_app_server_protocol::ThreadReadStateScope;
use codex_app_server_protocol::ThreadSideSummaryCreateParams;
use codex_app_server_protocol::ThreadSideSummaryCreateResponse;
use codex_app_server_protocol::ThreadSideSummaryLatestParams;
use codex_app_server_protocol::ThreadSideSummaryLatestResponse;
use codex_protocol::protocol::AgentMessageEvent;
use codex_protocol::protocol::EventMsg;
use serde_json::json;
use std::collections::BTreeMap;
use std::io::Write;
use tempfile::TempDir;
use tokio::time::timeout;

#[cfg(windows)]
const DEFAULT_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(25);
#[cfg(not(windows))]
const DEFAULT_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[tokio::test]
async fn thread_read_state_mark_all_marks_visible_threads_read() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    write_mock_responses_config_toml(
        codex_home.path(),
        &server.uri(),
        &BTreeMap::new(),
        200_000,
        None,
        "mock_provider",
        "compact",
    )?;
    let thread_id = create_fake_rollout(
        codex_home.path(),
        "2025-01-05T12-00-00",
        "2025-01-05T12:00:00Z",
        "Review unread sessions",
        Some("mock_provider"),
        None,
    )?;

    let mut mcp = TestAppServer::new(codex_home.path()).await?;
    timeout(DEFAULT_READ_TIMEOUT, mcp.initialize()).await??;

    let before = list_threads(&mut mcp).await?;
    let before_thread = before
        .data
        .iter()
        .find(|thread| thread.id == thread_id)
        .expect("created thread should be listed");
    assert!(before_thread.has_unread);

    let request_id = mcp
        .send_thread_read_state_mark_all_request(ThreadReadStateMarkAllParams {
            scope: ThreadReadStateScope::default(),
        })
        .await?;
    let response: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(request_id)),
    )
    .await??;
    let response = to_response::<ThreadReadStateMarkAllResponse>(response)?;
    assert_eq!(response.total_count, 1);
    assert_eq!(response.marked_count, 1);
    assert_eq!(response.already_read_count, 0);

    let after = list_threads(&mut mcp).await?;
    let after_thread = after
        .data
        .iter()
        .find(|thread| thread.id == thread_id)
        .expect("created thread should still be listed");
    assert!(!after_thread.has_unread);

    Ok(())
}

#[tokio::test]
async fn thread_side_summary_create_marks_read_and_persists_latest_summary() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    write_mock_responses_config_toml(
        codex_home.path(),
        &server.uri(),
        &BTreeMap::new(),
        200_000,
        None,
        "mock_provider",
        "compact",
    )?;
    let thread_id = create_fake_rollout(
        codex_home.path(),
        "2025-01-05T12-00-00",
        "2025-01-05T12:00:00Z",
        "Summarize finished automation sessions",
        Some("mock_provider"),
        None,
    )?;
    append_agent_message(
        codex_home.path(),
        "2025-01-05T12-00-00",
        &thread_id,
        "Finished the automation cleanup and verified the durable state.",
    )?;

    let mut mcp = TestAppServer::new(codex_home.path()).await?;
    timeout(DEFAULT_READ_TIMEOUT, mcp.initialize()).await??;

    let request_id = mcp
        .send_thread_side_summary_create_request(ThreadSideSummaryCreateParams {
            scope: ThreadReadStateScope::default(),
        })
        .await?;
    let response: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(request_id)),
    )
    .await??;
    let response = to_response::<ThreadSideSummaryCreateResponse>(response)?;
    assert_eq!(response.summary.thread_count, 1);
    assert_eq!(response.summary.unread_count, 1);
    assert_eq!(response.summary.entries.len(), 1);
    assert_eq!(response.summary.entries[0].thread_id, thread_id);
    assert!(
        response.summary.entries[0]
            .summary
            .contains("Finished the automation cleanup")
    );

    let latest_request_id = mcp
        .send_thread_side_summary_latest_request(ThreadSideSummaryLatestParams::default())
        .await?;
    let latest_response: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(latest_request_id)),
    )
    .await??;
    let latest_response = to_response::<ThreadSideSummaryLatestResponse>(latest_response)?;
    let latest_summary = latest_response
        .summary
        .expect("latest summary should exist");
    assert_eq!(latest_summary.id, response.summary.id);
    assert_eq!(latest_summary.markdown, response.summary.markdown);

    let after = list_threads(&mut mcp).await?;
    let after_thread = after
        .data
        .iter()
        .find(|thread| thread.id == thread_id)
        .expect("created thread should still be listed");
    assert!(!after_thread.has_unread);

    Ok(())
}

async fn list_threads(mcp: &mut TestAppServer) -> Result<ThreadListResponse> {
    let request_id = mcp
        .send_thread_list_request(ThreadListParams {
            cursor: None,
            limit: None,
            sort_key: None,
            sort_direction: None,
            model_providers: None,
            source_kinds: None,
            archived: None,
            cwd: None,
            use_state_db_only: false,
            search_term: None,
            parent_thread_id: None,
            ancestor_thread_id: None,
        })
        .await?;
    let response: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(request_id)),
    )
    .await??;
    to_response::<ThreadListResponse>(response)
}

fn append_agent_message(
    codex_home: &std::path::Path,
    filename_ts: &str,
    thread_id: &str,
    message: &str,
) -> Result<()> {
    let file_path = rollout_path(codex_home, filename_ts, thread_id);
    let payload = serde_json::to_value(EventMsg::AgentMessage(AgentMessageEvent {
        message: message.to_string(),
        phase: None,
        memory_citation: None,
    }))?;
    let line = json!({
        "timestamp": "2025-01-05T12:00:01Z",
        "type": "event_msg",
        "payload": payload
    })
    .to_string();
    let mut file = std::fs::OpenOptions::new().append(true).open(file_path)?;
    writeln!(file, "{line}")?;
    Ok(())
}
