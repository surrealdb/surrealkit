use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use walkdir::WalkDir;

use super::types::{GlobalTestConfig, LoadedSpecs, LoadedSuite, SuiteSpec};

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
