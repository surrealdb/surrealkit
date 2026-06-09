//! Embeds the repo's `/templates` tree into the binary at compile time.
//!
//! Generates `$OUT_DIR/embedded_templates.rs` defining a `TEMPLATES` static of
//! `EmbeddedTemplateFile` entries (one per file), which `templates::source`
//! `include!`s. Kept as a build script (rather than a proc-macro) so the
//! init-only template machinery stays out of the public `surrealkit-macros` and
//! `surrealkit` library surfaces.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

fn main() {
	let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
	let templates_dir = PathBuf::from(&manifest_dir)
		.join("../../templates")
		.canonicalize()
		.expect("templates directory not found");

	println!("cargo:rerun-if-changed={}", templates_dir.display());

	let mut files = Vec::new();
	collect_files(&templates_dir, &mut files);
	files.sort();

	let mut out = String::from("pub static TEMPLATES: &[EmbeddedTemplateFile] = &[\n");
	for file in &files {
		println!("cargo:rerun-if-changed={}", file.display());
		let rel = file
			.strip_prefix(&templates_dir)
			.expect("path under templates dir")
			.to_string_lossy()
			.replace('\\', "/");
		let abs = file.to_string_lossy();
		writeln!(
			out,
			"    EmbeddedTemplateFile {{ path: {rel:?}, contents: include_str!({abs:?}) }},"
		)
		.expect("write generated entry");
	}
	out.push_str("];\n");

	let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR");
	std::fs::write(Path::new(&out_dir).join("embedded_templates.rs"), out)
		.expect("writing embedded_templates.rs");
}

fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) {
	let entries = match std::fs::read_dir(dir) {
		Ok(e) => e,
		Err(_) => return,
	};
	for entry in entries.flatten() {
		let path = entry.path();
		if path.is_dir() {
			println!("cargo:rerun-if-changed={}", path.display());
			collect_files(&path, out);
		} else if path.is_file() {
			out.push(path);
		}
	}
}
