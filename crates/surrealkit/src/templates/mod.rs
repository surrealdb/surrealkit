//! Templates for `surrealkit init`.
//!
//! A template is a directory with a `template.toml` manifest plus `schema/`,
//! `tests/suites/`, and `tests/fixtures/` subtrees. Templates can be bundled in
//! the binary (under the repo's `/templates`), read from a local path, or cloned
//! from a git URL. During `init` the user selects optional *features*; each
//! feature contributes a set of files that are copied into the scaffolded project.

mod emit;
mod manifest;
mod select;
mod source;

use anyhow::Result;
use emit::EmitPlan;
pub use select::InitOpts;
use source::TemplateSource;
use surrealkit::scaffold;

/// Run `surrealkit init`: scaffold the base project and layer on the selected
/// template features.
pub fn run_init(folder: &str, opts: InitOpts) -> Result<()> {
	// 1. Resolve where the template comes from.
	let template_source = match &opts.from {
		Some(from) => TemplateSource::from_arg(from)?,
		None => TemplateSource::bundled(opts.template.as_deref())?,
	};

	// 2. Load + validate the manifest.
	let manifest = source::load_manifest(&template_source)?;
	let title = manifest.display_name.as_deref().unwrap_or(&manifest.name);
	println!("Using template: {title}");
	if let Some(desc) = &manifest.description {
		println!("  {desc}");
	}
	println!();

	// 3. Scaffold the base project tree (idempotent; skips existing files).
	scaffold::scaffold(folder)?;

	// 4. Resolve the dependency-closed feature set (interactive or from flags).
	let feature_ids = select::resolve_features(&manifest, &opts)?;
	if feature_ids.is_empty() {
		println!("\nNo features selected — scaffolded a bare project.");
		return Ok(());
	}

	// 5. Build the full emit plan (detects conflicts) and write it.
	let plan = EmitPlan::build(folder, &manifest, &feature_ids, &template_source)?;
	println!("\nAdding features: {}", feature_ids.join(", "));
	plan.write(opts.force)?;

	Ok(())
}
