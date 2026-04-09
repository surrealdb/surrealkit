use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;

use super::actors::ActorSession;
use super::assertions::{
	JsonAssertionContext, assert_header_value, assert_json_value_with_context,
};
use super::types::{ApiRequestCase, AssertionReport};

#[derive(Debug, Clone)]
pub struct ApiResult {
	pub status: u16,
	pub assertions: Vec<AssertionReport>,
}

pub async fn execute_api_case(
	base_url: &str,
	case: &ApiRequestCase,
	actor: &ActorSession,
	default_timeout_ms: u64,
) -> Result<ApiResult> {
	let client = reqwest::Client::builder()
		.timeout(Duration::from_millis(case.timeout_ms.unwrap_or(default_timeout_ms)))
		.build()
		.context("building API client")?;

	let path = case.path.trim();
	if path.is_empty() {
		bail!("api_request case path cannot be empty");
	}
	let url = format!(
		"{}{}{}",
		base_url.trim_end_matches('/'),
		if path.starts_with('/') {
			""
		} else {
			"/"
		},
		path
	);

	let method = reqwest::Method::from_bytes(case.method.to_uppercase().as_bytes())
		.with_context(|| format!("invalid HTTP method '{}'", case.method))?;

	let mut headers = HeaderMap::new();
	for (k, v) in &actor.headers {
		insert_header(&mut headers, k, v)?;
	}
	for (k, v) in &case.headers {
		insert_header(&mut headers, k, v)?;
	}

	let mut req = client.request(method, &url).headers(headers);
	if let Some(body) = &case.body {
		req = req.json(body);
	}

	let resp = req.send().await.with_context(|| format!("request to {} failed", url))?;
	let status = resp.status().as_u16();
	let headers = resp.headers().clone();
	let body_text = resp.text().await.context("reading response body")?;

	let mut assertions = Vec::new();
	let status_ok = status == case.expected_status;
	assertions.push(AssertionReport {
		name: "status".to_string(),
		passed: status_ok,
		message: format!("expected status {}, got {}", case.expected_status, status),
	});

	let body = if body_text.trim().is_empty() {
		None
	} else {
		serde_json::from_str::<Value>(&body_text).ok()
	};

	for (idx, assertion) in case.header_assertions.iter().enumerate() {
		assertions.push(assert_header_value(&headers, assertion, idx)?);
	}

	if !case.body_assertions.is_empty() {
		let parsed = body.as_ref().ok_or_else(|| {
			anyhow!("body assertions requested but response body is not valid JSON")
		})?;
		let ctx = JsonAssertionContext {
			actor_auth: actor.auth.clone(),
		};
		for (idx, assertion) in case.body_assertions.iter().enumerate() {
			assertions.push(assert_json_value_with_context(parsed, assertion, idx, &ctx)?);
		}
	}

	Ok(ApiResult {
		status,
		assertions,
	})
}

fn insert_header(headers: &mut HeaderMap, key: &str, value: &str) -> Result<()> {
	let name = HeaderName::from_bytes(key.as_bytes())
		.with_context(|| format!("invalid header name '{}'", key))?;
	let val = HeaderValue::from_str(value)
		.with_context(|| format!("invalid header value for '{}'", key))?;
	headers.insert(name, val);
	Ok(())
}
