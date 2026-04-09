use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use walkdir::WalkDir;

use super::types::{GlobalTestConfig, LoadedSpecs, LoadedSuite, SuiteSpec};

pub const TEST_CONFIG_PATH: &str = "database/tests/config.toml";
pub const TEST_SUITES_DIR: &str = "database/tests/suites";

pub fn load_specs() -> Result<LoadedSpecs> {
	let global = load_global_config()?;
	let suites = load_suites()?;

	if suites.is_empty() {
		return Err(anyhow!("No suite files found in {}", TEST_SUITES_DIR));
	}

	Ok(LoadedSpecs {
		global,
		suites,
	})
}

fn load_global_config() -> Result<GlobalTestConfig> {
	let path = Path::new(TEST_CONFIG_PATH);
	if !path.exists() {
		return Ok(GlobalTestConfig::default());
	}

	let raw = fs::read_to_string(path).with_context(|| format!("reading {}", TEST_CONFIG_PATH))?;
	let cfg: GlobalTestConfig =
		toml::from_str(&raw).with_context(|| format!("parsing {}", TEST_CONFIG_PATH))?;
	Ok(cfg)
}

fn load_suites() -> Result<Vec<LoadedSuite>> {
	let mut suites = Vec::new();
	for entry in WalkDir::new(TEST_SUITES_DIR)
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
		suites.push(LoadedSuite {
			path: relative(path),
			spec,
		});
	}

	suites.sort_by(|a, b| a.path.cmp(&b.path));
	Ok(suites)
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

	#[test]
	fn loader_constants_are_in_database_tests() {
		assert!(TEST_CONFIG_PATH.starts_with("database/tests"));
		assert!(TEST_SUITES_DIR.starts_with("database/tests"));
	}
}
