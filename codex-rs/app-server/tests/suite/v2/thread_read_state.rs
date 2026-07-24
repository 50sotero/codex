use anyhow::Result;
use app_test_support::TestAppServer;
use app_test_support::create_fake_rollout;
use app_test_support::to_response;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadListParams;
use codex_app_server_protocol::ThreadListResponse;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadSourceKind;
use pretty_assertions::assert_eq;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::timeout;

const READ_TIMEOUT: Duration = Duration::from_secs(10);

async fn list_all_threads(
    mcp: &mut TestAppServer,
    use_state_db_only: bool,
) -> Result<ThreadListResponse> {
    let request_id = mcp
        .send_thread_list_request(ThreadListParams {
            cursor: None,
            limit: Some(10),
            sort_key: None,
            sort_direction: None,
            model_providers: Some(Vec::new()),
            source_kinds: Some(vec![ThreadSourceKind::Cli]),
            archived: None,
            cwd: None,
            use_state_db_only,
            search_term: None,
            parent_thread_id: None,
            ancestor_thread_id: None,
        })
        .await?;
    let response = timeout(
        READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(request_id)),
    )
    .await??;
    to_response(response)
}

async fn resume_thread(mcp: &mut TestAppServer, thread_id: &str) -> Result<ThreadResumeResponse> {
    let request_id = mcp
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.to_string(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let response: JSONRPCResponse = timeout(
        READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(request_id)),
    )
    .await??;
    to_response(response)
}

#[tokio::test]
async fn projects_persisted_read_state_across_list_and_resume() -> Result<()> {
    let codex_home = TempDir::new()?;
    std::fs::write(
        codex_home.path().join("config.toml"),
        "model = \"mock-model\"\napproval_policy = \"never\"\n",
    )?;
    let first_id = create_fake_rollout(
        codex_home.path(),
        "2025-01-05T12-00-00",
        "2025-01-05T12:00:00Z",
        "first",
        Some("openai"),
        None,
    )?;
    let mut mcp = TestAppServer::new(codex_home.path()).await?;
    timeout(READ_TIMEOUT, mcp.initialize()).await??;

    let initial = list_all_threads(&mut mcp, false).await?;
    assert_eq!(initial.data.len(), 1);
    assert_eq!(
        (initial.data[0].read_at, initial.data[0].has_unread),
        (None, true)
    );

    let current = list_all_threads(&mut mcp, true).await?;
    let first = current
        .data
        .iter()
        .find(|thread| thread.id == first_id)
        .expect("first thread");
    assert_eq!((first.read_at, first.has_unread), (None, true));

    let cold_resume = resume_thread(&mut mcp, &first_id).await?;
    assert_eq!(
        (cold_resume.thread.read_at, cold_resume.thread.has_unread),
        (None, true)
    );
    let loaded_resume = resume_thread(&mut mcp, &first_id).await?;
    assert_eq!(
        (
            loaded_resume.thread.read_at,
            loaded_resume.thread.has_unread
        ),
        (None, true)
    );
    Ok(())
}
