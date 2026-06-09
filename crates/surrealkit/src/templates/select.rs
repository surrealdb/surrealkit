use std::io::IsTerminal;

use anyhow::Result;

use super::manifest::TemplateManifest;

/// Options driving `init`, parsed from the CLI.
#[derive(Debug, Clone, Default)]
pub struct InitOpts {
	/// Bundled template name. Ignored when `from` is set.
	pub template: Option<String>,
	/// Git URL or local path to an external template.
	pub from: Option<String>,
	/// Explicit feature ids to enable (implies non-interactive).
	pub feature: Vec<String>,
	/// Scaffold the base project only; add no features.
	pub minimal: bool,
	/// Accept default features without prompting (non-interactive).
	pub yes: bool,
	/// Overwrite files that already exist.
	pub force: bool,
}

/// Resolve the final, dependency-closed set of feature ids to emit.
/// Honors the flag precedence and falls back to defaults when not on a TTY.
pub fn resolve_features(manifest: &TemplateManifest, opts: &InitOpts) -> Result<Vec<String>> {
	// 1. --minimal wins: no features.
	if opts.minimal {
		return Ok(Vec::new());
	}

	// 2. Explicit --feature flags.
	if !opts.feature.is_empty() {
		let closed = manifest.resolve_closure(&opts.feature)?;
		announce_auto_added(manifest, &opts.feature, &closed);
		return Ok(closed);
	}

	// 3. -y, or no TTY: take the defaults without prompting.
	let interactive =
		!opts.yes && std::io::stdin().is_terminal() && std::io::stdout().is_terminal();

	if !interactive {
		if !opts.yes {
			eprintln!(
				"note: not a TTY; enabling default features (use --feature/--minimal/-y to control)"
			);
		}
		let defaults = manifest.default_feature_ids();
		return manifest.resolve_closure(&defaults);
	}

	// 4. Interactive multi-select.
	prompt_features(manifest)
}

fn announce_auto_added(manifest: &TemplateManifest, requested: &[String], closed: &[String]) {
	for id in closed {
		if !requested.contains(id) {
			// Find a requester that depends (directly) on this id, for a friendly note.
			let by = manifest
				.features
				.iter()
				.find(|f| requested.contains(&f.id) && f.requires.contains(id))
				.map(|f| f.id.as_str());
			match by {
				Some(req) => println!("  + {id} (required by {req})"),
				None => println!("  + {id} (dependency)"),
			}
		}
	}
}

#[cfg(not(test))]
fn prompt_features(manifest: &TemplateManifest) -> Result<Vec<String>> {
	use inquire::MultiSelect;
	use inquire::list_option::ListOption;

	if manifest.features.is_empty() {
		return Ok(Vec::new());
	}

	let labels: Vec<String> = manifest
		.features
		.iter()
		.map(|f| match &f.description {
			Some(d) => format!("{} — {}", f.name, d),
			None => f.name.clone(),
		})
		.collect();

	let defaults: Vec<usize> =
		manifest.features.iter().enumerate().filter_map(|(i, f)| f.default.then_some(i)).collect();

	let chosen: Vec<ListOption<String>> =
		MultiSelect::new("Select features to add:", labels).with_default(&defaults).raw_prompt()?;

	let selected: Vec<String> =
		chosen.into_iter().map(|opt| manifest.features[opt.index].id.clone()).collect();

	let closed = manifest.resolve_closure(&selected)?;
	announce_auto_added(manifest, &selected, &closed);
	Ok(closed)
}

// In tests there is no TTY; resolve_features never reaches the interactive path,
// but provide a stub so the crate builds with `cfg(test)`.
#[cfg(test)]
fn prompt_features(manifest: &TemplateManifest) -> Result<Vec<String>> {
	manifest.resolve_closure(&manifest.default_feature_ids())
}

#[cfg(test)]
mod tests {
	use super::*;

	// Mirrors the real template: two opt-in features, teams requires organizations.
	fn manifest() -> TemplateManifest {
		TemplateManifest::parse(
			r#"
schema_version = 1
name = "t"
[[features]]
id = "organizations"
name = "Organizations"
default = false
[[features]]
id = "teams"
name = "Teams"
requires = ["organizations"]
default = false
"#,
		)
		.unwrap()
	}

	#[test]
	fn minimal_yields_nothing() {
		let opts = InitOpts {
			minimal: true,
			..Default::default()
		};
		assert!(resolve_features(&manifest(), &opts).unwrap().is_empty());
	}

	#[test]
	fn explicit_feature_pulls_dependencies() {
		let opts = InitOpts {
			feature: vec!["teams".to_string()],
			..Default::default()
		};
		let got = resolve_features(&manifest(), &opts).unwrap();
		assert_eq!(got, vec!["organizations", "teams"]);
	}

	#[test]
	fn yes_with_no_defaults_yields_nothing() {
		// Features are opt-in: `-y` accepts the (empty) default set -> bare project.
		let opts = InitOpts {
			yes: true,
			..Default::default()
		};
		assert!(resolve_features(&manifest(), &opts).unwrap().is_empty());
	}

	#[test]
	fn yes_takes_default_features_when_present() {
		let m = TemplateManifest::parse(
			r#"
schema_version = 1
name = "t"
[[features]]
id = "organizations"
name = "Organizations"
default = true
"#,
		)
		.unwrap();
		let opts = InitOpts {
			yes: true,
			..Default::default()
		};
		assert_eq!(resolve_features(&m, &opts).unwrap(), vec!["organizations"]);
	}

	#[test]
	fn unknown_feature_is_rejected() {
		let opts = InitOpts {
			feature: vec!["ghost".to_string()],
			..Default::default()
		};
		assert!(resolve_features(&manifest(), &opts).is_err());
	}
}
