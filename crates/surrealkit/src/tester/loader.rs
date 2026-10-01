use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use walkdir::WalkDir;

use super::types::{
	CaseKind, GlobalTestConfig, LoadedSpecs, LoadedSuite, PermissionAction, SuiteSpec,
};

pub fn load_specs(folder: &str) -> Result<LoadedSpecs> {
	let tests_dir = PathBuf::from(folder).join("tests");
	let suites_dir = tests_dir.join("suites");
	let config_path = tests_dir.join("config.toml");

	let global = load_global_config(&config_path)?;
	let suites = load_suites(&suites_dir)?;

	if suites.is_empty() {
		return Err(anyhow!("No suite files found in {}", suites_dir.display()));
	}

	Ok(LoadedSpecs {
		global,
		suites,
	})
}

fn load_global_config(path: &Path) -> Result<GlobalTestConfig> {
	if !path.exists() {
		return Ok(GlobalTestConfig::default());
	}

	let raw = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
	let cfg: GlobalTestConfig =
		toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;
	Ok(cfg)
}

fn load_suites(suites_dir: &Path) -> Result<Vec<LoadedSuite>> {
	let mut suites = Vec::new();
	for entry in WalkDir::new(suites_dir)
		.follow_links(true)
		.into_iter()
		.filter_map(|e| e.ok())
		.filter(|e| e.file_type().is_file())
	{
		let path = entry.path();
		if path.extension().and_then(|x| x.to_str()) != Some("toml") {
			continue;
		}

		let raw = fs::read_to_string(path).with_context(|| format!("reading {}", display(path)))?;
		let spec: SuiteSpec =
			toml::from_str(&raw).with_context(|| format!("parsing {}", display(path)))?;
		validate_suite(&spec).with_context(|| format!("in {}", display(path)))?;
		suites.push(LoadedSuite {
			path: relative(path),
			spec,
		});
	}

	suites.sort_by(|a, b| a.path.cmp(&b.path));
	Ok(suites)
}

/// Reject what parses but can never run.
pub(crate) fn validate_suite(spec: &SuiteSpec) -> Result<()> {
	for case in &spec.cases {
		let CaseKind::PermissionsMatrix(matrix) = &case.kind else {
			continue;
		};
		if matrix.record_id.as_deref() == Some("$auth")
			&& matrix.rules.iter().any(|rule| matches!(rule.action, PermissionAction::Create))
		{
			bail!(
				"permissions_matrix case '{}' has a create rule with record_id = \"$auth\". The \
				 signed-in record already exists (signup created it), so there is nothing to \
				 create; test creation in a case with another record_id.",
				case.name
			);
		}
	}
	Ok(())
}

fn relative(path: &Path) -> PathBuf {
	let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
	path.strip_prefix(cwd).unwrap_or(path).to_path_buf()
}

fn display(path: &Path) -> String {
	path.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
	use super::*;

	fn suite(rules: &str) -> SuiteSpec {
		toml::from_str(&format!(
			"[[cases]]\nname = \"c\"\nkind = \"permissions_matrix\"\ntable = \"user\"\nrecord_id = \"$auth\"\n{rules}"
		))
		.unwrap()
	}

	#[test]
	fn a_create_rule_on_auth_is_rejected() {
		let err = validate_suite(&suite("[[cases.rules]]\naction = \"create\"\n"))
			.unwrap_err()
			.to_string();
		assert!(err.contains("case 'c'") && err.contains("nothing to create"), "{err}");
		validate_suite(&suite(
			"[[cases.rules]]\naction = \"select\"\n[[cases.rules]]\naction = \"delete\"\n",
		))
		.expect("select and delete on $auth are fine");
	}
}
