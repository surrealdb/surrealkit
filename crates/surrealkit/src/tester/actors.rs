use std::collections::{BTreeMap, HashMap};
use std::env;

use anyhow::{Context, Result, anyhow, bail};
use serde_json::Value;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::{Database, Namespace, Record, Root};
use surrealdb_types::SurrealValue;

use super::types::{ActorKind, ActorSpec};
use crate::config::DbCfg;
use crate::core::create_surreal_client;

#[derive(Debug, Clone)]
pub struct ActorSession {
	pub db: Surreal<Any>,
	pub headers: BTreeMap<String, String>,
	pub auth: Option<Value>,
}

pub fn merged_actor_specs(
	global: &BTreeMap<String, ActorSpec>,
	suite: &BTreeMap<String, ActorSpec>,
) -> BTreeMap<String, ActorSpec> {
	let mut merged = global.clone();
	for (name, spec) in suite {
		merged.insert(name.clone(), spec.clone());
	}
	merged
}

pub async fn build_actor_sessions(
	cfg: &DbCfg,
	host: &str,
	namespace: &str,
	database: &str,
	specs: &BTreeMap<String, ActorSpec>,
) -> Result<HashMap<String, ActorSession>> {
	let mut out = HashMap::new();

	let root = build_default_root_session(cfg, host, namespace, database).await?;
	out.insert("root".to_string(), root);

	for (name, spec) in specs {
		let session = build_session(name, spec, cfg, host, namespace, database).await?;
		out.insert(name.clone(), session);
	}

	Ok(out)
}

async fn build_default_root_session(
	cfg: &DbCfg,
	host: &str,
	namespace: &str,
	database: &str,
) -> Result<ActorSession> {
	let db = create_surreal_client(&host.to_string())
		.await
		.with_context(|| format!("connecting root actor to {host}"))?;
	let _token = db
		.signin(Root {
			username: cfg.user().to_string(),
			password: cfg.pass().to_string(),
		})
		.await
		.context("root signin failed")?;
	db.use_ns(namespace)
		.use_db(database)
		.await
		.with_context(|| format!("switching root actor to ns={namespace} db={database}"))?;

	Ok(ActorSession {
		auth: fetch_auth(&db).await?,
		db,
		headers: BTreeMap::new(),
	})
}

async fn build_session(
	name: &str,
	spec: &ActorSpec,
	cfg: &DbCfg,
	host: &str,
	namespace: &str,
	database: &str,
) -> Result<ActorSession> {
	let mut session_headers = spec.headers.clone();
	let actor_ns = resolve_string(
		spec.namespace.as_deref(),
		spec.namespace_env.as_deref(),
		some_default(namespace),
	)?;
	let actor_db = resolve_string(
		spec.database.as_deref(),
		spec.database_env.as_deref(),
		some_default(database),
	)?;

	let db = create_surreal_client(&host.to_string())
		.await
		.with_context(|| format!("connecting actor '{name}' to {host}"))?;
	let access_token = match spec.kind {
		ActorKind::Root => {
			let username = resolve_string(
				spec.username.as_deref(),
				spec.username_env.as_deref(),
				some_default(cfg.user()),
			)?;
			let password = resolve_string(
				spec.password.as_deref(),
				spec.password_env.as_deref(),
				some_default(cfg.pass()),
			)?;
			let token = db
				.signin(Root {
					username,
					password,
				})
				.await
				.with_context(|| format!("actor '{name}' root signin failed"))?;
			Some(token.access.as_insecure_token().to_string())
		}
		ActorKind::Namespace => {
			let username = required_string(
				spec.username.as_deref(),
				spec.username_env.as_deref(),
				format!("actor '{name}' namespace username"),
			)?;
			let password = required_string(
				spec.password.as_deref(),
				spec.password_env.as_deref(),
				format!("actor '{name}' namespace password"),
			)?;
			let token = db
				.signin(Namespace {
					namespace: actor_ns.clone(),
					username,
					password,
				})
				.await
				.with_context(|| format!("actor '{name}' namespace signin failed"))?;
			Some(token.access.as_insecure_token().to_string())
		}
		ActorKind::Database => {
			let username = required_string(
				spec.username.as_deref(),
				spec.username_env.as_deref(),
				format!("actor '{name}' database username"),
			)?;
			let password = required_string(
				spec.password.as_deref(),
				spec.password_env.as_deref(),
				format!("actor '{name}' database password"),
			)?;
			let token = db
				.signin(Database {
					namespace: actor_ns.clone(),
					database: actor_db.clone(),
					username,
					password,
				})
				.await
				.with_context(|| format!("actor '{name}' database signin failed"))?;
			Some(token.access.as_insecure_token().to_string())
		}
		ActorKind::Record => {
			let access = required_string(
				spec.access.as_deref(),
				spec.access_env.as_deref(),
				format!("actor '{name}' access method"),
			)?;
			if let Some(params) = spec.signup_params.clone() {
				db.signup(Record {
					namespace: actor_ns.clone(),
					database: actor_db.clone(),
					access: access.clone(),
					params,
				})
				.await
				.with_context(|| format!("actor '{name}' record signup failed"))?;
			}
			let params = spec
				.signin_params
				.clone()
				.or_else(|| spec.params.clone())
				.unwrap_or_else(|| serde_json::json!({}));
			let token = db
				.signin(Record {
					namespace: actor_ns.clone(),
					database: actor_db.clone(),
					access,
					params,
				})
				.await
				.with_context(|| format!("actor '{name}' record signin failed"))?;
			Some(token.access.as_insecure_token().to_string())
		}
		ActorKind::Token => {
			let token = required_string(
				spec.token.as_deref(),
				spec.token_env.as_deref(),
				format!("actor '{name}' token"),
			)?;
			db.authenticate(token.clone())
				.await
				.with_context(|| format!("actor '{name}' token authentication failed"))?;
			Some(token)
		}
		ActorKind::Headers => {
			let token = db
				.signin(Root {
					username: cfg.user().to_string(),
					password: cfg.pass().to_string(),
				})
				.await
				.with_context(|| format!("actor '{name}' default root signin failed"))?;
			Some(token.access.as_insecure_token().to_string())
		}
	};

	db.use_ns(&actor_ns).use_db(&actor_db).await.with_context(|| {
		format!("actor '{name}' use_ns/use_db failed for {actor_ns}/{actor_db}")
	})?;

	if let Some(token) = &access_token {
		session_headers
			.entry("authorization".to_string())
			.or_insert_with(|| format!("Bearer {token}"));
	}

	Ok(ActorSession {
		auth: fetch_auth(&db).await?,
		db,
		headers: session_headers,
	})
}

async fn fetch_auth(db: &Surreal<Any>) -> Result<Option<Value>> {
	let mut response = db.query("RETURN $auth;").await?.check()?;
	let raw: surrealdb_types::Value = response.take(0)?;
	let json = Value::from_value(raw).unwrap_or(Value::Null);
	Ok((json != Value::Null).then_some(json))
}

pub fn actor_name_or_default(name: Option<&str>) -> &str {
	name.unwrap_or("root")
}

pub fn require_actor<'a>(
	actors: &'a HashMap<String, ActorSession>,
	name: &str,
) -> Result<&'a ActorSession> {
	actors.get(name).ok_or_else(|| anyhow!("actor '{}' not configured", name))
}

pub fn resolve_string(
	literal: Option<&str>,
	env_name: Option<&str>,
	default: Option<&str>,
) -> Result<String> {
	if let Some(value) = literal {
		if !value.trim().is_empty() {
			return Ok(value.to_string());
		}
	}

	if let Some(key) = env_name {
		let value = env::var(key).with_context(|| format!("reading env var {}", key))?;
		if !value.trim().is_empty() {
			return Ok(value);
		}
	}

	if let Some(value) = default {
		if !value.trim().is_empty() {
			return Ok(value.to_string());
		}
	}

	bail!("required value missing")
}

fn required_string(literal: Option<&str>, env_name: Option<&str>, label: String) -> Result<String> {
	resolve_string(literal, env_name, None).with_context(|| format!("missing {label}"))
}

fn some_default<'a>(value: &'a str) -> Option<&'a str> {
	Some(value)
}
