use anyhow::Result;
use app_test_support::TestAppServer;
use app_test_support::create_mock_responses_server_repeating_assistant;
use app_test_support::to_response;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadAutomationCompletedSummaryResponse;
use codex_app_server_protocol::ThreadGoalCompleteAllResponse;
use codex_app_server_protocol::ThreadGoalSetResponse;
use codex_app_server_protocol::ThreadGoalStatus;
use codex_app_server_protocol::ThreadSource;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::path::Path;
use tempfile::TempDir;
use tokio::time::timeout;

const DEFAULT_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[tokio::test]
async fn thread_goal_complete_all_marks_every_incomplete_goal_complete() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri())?;
    let sqlite_home = codex_home.path().to_string_lossy().to_string();
    let mut mcp = TestAppServer::new_without_managed_config_with_env(
        codex_home.path(),
        &[("CODEX_SQLITE_HOME", Some(sqlite_home.as_str()))],
    )
    .await?;
    timeout(DEFAULT_READ_TIMEOUT, mcp.initialize()).await??;

    let active_automation =
        start_thread(&mut mcp, Some(ThreadSource::Feature("automation".into()))).await?;
    let active_user = start_thread(&mut mcp, Some(ThreadSource::User)).await?;
    let complete_automation =
        start_thread(&mut mcp, Some(ThreadSource::Feature("automation".into()))).await?;

    set_goal(
        &mut mcp,
        &active_automation,
        "ship automation",
        ThreadGoalStatus::Active,
    )
    .await?;
    set_goal(
        &mut mcp,
        &active_user,
        "ship user work",
        ThreadGoalStatus::Blocked,
    )
    .await?;
    set_goal(
        &mut mcp,
        &complete_automation,
        "already shipped automation",
        ThreadGoalStatus::Complete,
    )
    .await?;

    let request_id = mcp
        .send_raw_request("thread/goal/completeAll", /*params*/ None)
        .await?;
    let response: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(request_id)),
    )
    .await??;
    let response: ThreadGoalCompleteAllResponse = to_response(response)?;

    assert_eq!(
        response,
        ThreadGoalCompleteAllResponse {
            updated_count: 2,
            already_complete_count: 1,
            total_count: 3,
        }
    );

    let summary = completed_automation_summary(&mut mcp).await?;
    let mut ids = summary
        .data
        .iter()
        .map(|item| item.thread.id.as_str())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    let mut expected_ids = vec![active_automation.as_str(), complete_automation.as_str()];
    expected_ids.sort_unstable();
    assert_eq!(ids, expected_ids);
    assert!(
        summary
            .data
            .iter()
            .all(|item| item.goal.status == ThreadGoalStatus::Complete && !item.archived)
    );

    Ok(())
}

#[tokio::test]
async fn thread_automation_completed_summary_paginates_complete_automation_goals() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri())?;
    let sqlite_home = codex_home.path().to_string_lossy().to_string();
    let mut mcp = TestAppServer::new_without_managed_config_with_env(
        codex_home.path(),
        &[("CODEX_SQLITE_HOME", Some(sqlite_home.as_str()))],
    )
    .await?;
    timeout(DEFAULT_READ_TIMEOUT, mcp.initialize()).await??;

    let first = start_thread(&mut mcp, Some(ThreadSource::Feature("automation".into()))).await?;
    let user = start_thread(&mut mcp, Some(ThreadSource::User)).await?;
    let second = start_thread(&mut mcp, Some(ThreadSource::Feature("automation".into()))).await?;

    set_goal(
        &mut mcp,
        &first,
        "first automation",
        ThreadGoalStatus::Complete,
    )
    .await?;
    set_goal(&mut mcp, &user, "user goal", ThreadGoalStatus::Complete).await?;
    set_goal(
        &mut mcp,
        &second,
        "second automation",
        ThreadGoalStatus::Complete,
    )
    .await?;

    let first_page = completed_automation_summary_with_limit(&mut mcp, None, 1).await?;
    assert_eq!(first_page.data.len(), 1);
    let cursor = first_page
        .next_cursor
        .clone()
        .expect("expected a cursor for the next automation summary page");

    let second_page = completed_automation_summary_with_limit(&mut mcp, Some(cursor), 10).await?;
    assert_eq!(second_page.next_cursor, None);
    assert_eq!(second_page.data.len(), 1);

    let ids = first_page
        .data
        .iter()
        .chain(second_page.data.iter())
        .map(|item| item.thread.id.as_str())
        .collect::<Vec<_>>();
    assert!(ids.contains(&first.as_str()));
    assert!(ids.contains(&second.as_str()));
    assert!(!ids.contains(&user.as_str()));

    Ok(())
}

async fn start_thread(
    mcp: &mut TestAppServer,
    thread_source: Option<ThreadSource>,
) -> Result<String> {
    let request_id = mcp
        .send_thread_start_request(ThreadStartParams {
            model: Some("mock-model".to_string()),
            thread_source,
            ..Default::default()
        })
        .await?;
    let response: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(request_id)),
    )
    .await??;
    let ThreadStartResponse { thread, .. } = to_response(response)?;
    Ok(thread.id)
}

async fn set_goal(
    mcp: &mut TestAppServer,
    thread_id: &str,
    objective: &str,
    status: ThreadGoalStatus,
) -> Result<()> {
    let request_id = mcp
        .send_raw_request(
            "thread/goal/set",
            Some(json!({
                "threadId": thread_id,
                "objective": objective,
                "status": status,
            })),
        )
        .await?;
    let response: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(request_id)),
    )
    .await??;
    let _: ThreadGoalSetResponse = to_response(response)?;
    timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("thread/goal/updated"),
    )
    .await??;
    Ok(())
}

async fn completed_automation_summary(
    mcp: &mut TestAppServer,
) -> Result<ThreadAutomationCompletedSummaryResponse> {
    completed_automation_summary_with_limit(mcp, /*cursor*/ None, 10).await
}

async fn completed_automation_summary_with_limit(
    mcp: &mut TestAppServer,
    cursor: Option<String>,
    limit: u32,
) -> Result<ThreadAutomationCompletedSummaryResponse> {
    let request_id = mcp
        .send_raw_request(
            "thread/automation/completedSummary",
            Some(json!({
                "cursor": cursor,
                "limit": limit,
            })),
        )
        .await?;
    let response: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(request_id)),
    )
    .await??;
    to_response(response)
}

fn create_config_toml(codex_home: &Path, server_uri: &str) -> std::io::Result<()> {
    let config_toml = codex_home.join("config.toml");
    std::fs::write(
        config_toml,
        format!(
            r#"model = "mock-model"
approval_policy = "never"
sandbox_mode = "read-only"

model_provider = "mock_provider"

[features]
goals = true

[model_providers.mock_provider]
name = "Mock provider for test"
base_url = "{server_uri}/v1"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
"#
        ),
    )
}
