use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use tempfile::TempDir;

use super::manifest::TemplateManifest;

/// A single file embedded from the repo's `/templates` tree at compile time.
/// `path` is relative to the templates root, e.g. `default/template.toml`.
pub struct EmbeddedTemplateFile {
	pub path: &'static str,
	pub contents: &'static str,
}

// Embeds the entire `/templates` tree into the binary. `build.rs` generates the
// `TEMPLATES` static (one `EmbeddedTemplateFile` per file).
include!(concat!(env!("OUT_DIR"), "/embedded_templates.rs"));

/// Read access to a template's files, independent of where they live.
pub trait TemplateFiles {
	/// The raw `template.toml` for this template.
	fn read_manifest(&self) -> Result<String>;
	/// Read a file by its manifest-relative path (e.g. `schema/user.surql`).
	fn read_file(&self, rel: &str) -> Result<String>;
}

/// Where a template comes from.
#[derive(Debug)]
pub enum TemplateSource {
	/// A template bundled in the binary, by name (e.g. `default`).
	Bundled(String),
	/// A template on the local filesystem, rooted at this directory.
	Local(PathBuf),
	/// A git working copy, kept alive for the lifetime of the source.
	Git {
		_tmp: TempDir,
		root: PathBuf,
	},
}

impl TemplateSource {
	/// Resolve a `--from <url-or-path>` value (optionally `#rev`) into a source.
	pub fn from_arg(from: &str) -> Result<Self> {
		let looks_like_git = from.starts_with("http://")
			|| from.starts_with("https://")
			|| from.starts_with("git@")
			|| from.starts_with("ssh://")
			|| from.ends_with(".git");
		if looks_like_git {
			Self::from_git(from)
		} else {
			let path = PathBuf::from(from);
			if !path.exists() {
				bail!("template path '{}' does not exist", from);
			}
			Ok(TemplateSource::Local(path))
		}
	}

	/// Clone a git template (shallow) into a temp dir. Supports `<url>#<rev>`
	/// and `<url>#<rev>:<subdir>`.
	fn from_git(spec: &str) -> Result<Self> {
		let (url, rev, subdir) = parse_git_spec(spec);

		which_git()?;

		let tmp = TempDir::new().context("creating temp dir for git template")?;
		let dest = tmp.path().join("repo");

		let mut cmd = Command::new("git");
		cmd.args(["clone", "--depth", "1"]);
		if let Some(rev) = &rev {
			cmd.args(["--branch", rev]);
		}
		cmd.arg(url).arg(&dest);

		let status = cmd.status().context("running git clone")?;
		if !status.success() {
			bail!("git clone of '{}' failed", url);
		}

		let root = match &subdir {
			Some(sub) => dest.join(sub),
			None => dest,
		};
		if !root.exists() {
			bail!("subdirectory '{}' not found in cloned template", subdir.unwrap_or_default());
		}

		Ok(TemplateSource::Git {
			_tmp: tmp,
			root,
		})
	}

	/// The single bundled template name, or the default. Errors if `name` is
	/// given but no such bundled template exists.
	pub fn bundled(name: Option<&str>) -> Result<Self> {
		let available = bundled_template_names();
		let chosen = match name {
			Some(n) => {
				if !available.contains(n) {
					bail!(
						"no bundled template named '{}' (available: {})",
						n,
						available.into_iter().collect::<Vec<_>>().join(", ")
					);
				}
				n.to_string()
			}
			None => {
				if available.contains("default") {
					"default".to_string()
				} else if available.len() == 1 {
					available.into_iter().next().expect("len checked == 1")
				} else {
					bail!(
						"no default bundled template; pass --template (available: {})",
						available.into_iter().collect::<Vec<_>>().join(", ")
					);
				}
			}
		};
		Ok(TemplateSource::Bundled(chosen))
	}
}

impl TemplateFiles for TemplateSource {
	fn read_manifest(&self) -> Result<String> {
		match self {
			TemplateSource::Bundled(name) => {
				let key = format!("{name}/template.toml");
				bundled_file(&key)
					.map(str::to_string)
					.with_context(|| format!("bundled template '{name}' has no template.toml"))
			}
			TemplateSource::Local(root)
			| TemplateSource::Git {
				root,
				..
			} => read_local(root, "template.toml"),
		}
	}

	fn read_file(&self, rel: &str) -> Result<String> {
		match self {
			TemplateSource::Bundled(name) => {
				let key = format!("{name}/{rel}");
				bundled_file(&key)
					.map(str::to_string)
					.with_context(|| format!("template file '{rel}' missing from bundled '{name}'"))
			}
			TemplateSource::Local(root)
			| TemplateSource::Git {
				root,
				..
			} => read_local(root, rel),
		}
	}
}

/// Names of bundled templates (top-level dirs under the embedded tree).
pub fn bundled_template_names() -> BTreeSet<String> {
	TEMPLATES.iter().filter_map(|f| f.path.split('/').next()).map(str::to_string).collect()
}

/// Load and validate the manifest for a source.
pub fn load_manifest(source: &TemplateSource) -> Result<TemplateManifest> {
	let raw = source.read_manifest()?;
	TemplateManifest::parse(&raw)
}

fn bundled_file(key: &str) -> Option<&'static str> {
	TEMPLATES.iter().find(|f| f.path == key).map(|f| f.contents)
}

fn read_local(root: &Path, rel: &str) -> Result<String> {
	// Guard against traversal even though manifests are validated; `--from` may
	// point at an untrusted tree.
	if Path::new(rel).is_absolute() || rel.split(['/', '\\']).any(|c| c == "..") {
		bail!("unsafe template path '{rel}'");
	}
	let path = root.join(rel);
	std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))
}

fn which_git() -> Result<()> {
	let ok =
		Command::new("git").arg("--version").output().map(|o| o.status.success()).unwrap_or(false);
	if !ok {
		bail!("`git` was not found on PATH; install git or use a bundled template");
	}
	Ok(())
}

/// Parse `<url>[#<rev>[:<subdir>]]`.
fn parse_git_spec(spec: &str) -> (&str, Option<String>, Option<String>) {
	match spec.split_once('#') {
		None => (spec, None, None),
		Some((url, frag)) => match frag.split_once(':') {
			Some((rev, sub)) => (url, Some(rev.to_string()), Some(sub.to_string())),
			None => (url, Some(frag.to_string()), None),
		},
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn bundled_default_template_present() {
		let names = bundled_template_names();
		assert!(names.contains("default"), "expected a bundled 'default' template: {names:?}");
	}

	#[test]
	fn bundled_default_manifest_parses() {
		let src = TemplateSource::bundled(Some("default")).unwrap();
		let manifest = load_manifest(&src).unwrap();
		assert_eq!(manifest.name, "default");
		// Every file referenced by every feature must be present in the bundle.
		for feature in &manifest.features {
			for path in feature.all_paths() {
				src.read_file(path)
					.unwrap_or_else(|e| panic!("bundled file '{path}' unreadable: {e}"));
			}
		}
	}

	#[test]
	fn unknown_bundled_name_errors() {
		let err = TemplateSource::bundled(Some("ghost")).unwrap_err();
		assert!(err.to_string().contains("no bundled template named 'ghost'"));
	}

	#[test]
	fn parse_git_spec_variants() {
		assert_eq!(parse_git_spec("https://x/y.git"), ("https://x/y.git", None, None));
		assert_eq!(
			parse_git_spec("https://x/y.git#main"),
			("https://x/y.git", Some("main".into()), None)
		);
		assert_eq!(
			parse_git_spec("https://x/y.git#v1:sub/dir"),
			("https://x/y.git", Some("v1".into()), Some("sub/dir".into()))
		);
	}
}
