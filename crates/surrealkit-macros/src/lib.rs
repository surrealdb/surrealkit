use std::env;
use std::path::PathBuf;

use proc_macro::TokenStream;
use quote::quote;
use syn::LitStr;
use walkdir::WalkDir;

/// Resolve a macro argument to a directory relative to the caller's `Cargo.toml`,
/// returning `(rel_dir, abs_dir)`. `default` is used when the macro is invoked
/// with no argument.
fn resolve_dir(input: TokenStream, default: &str, macro_name: &str) -> (String, PathBuf) {
	let rel_dir = if input.is_empty() {
		default.to_string()
	} else {
		match syn::parse::<LitStr>(input) {
			Ok(lit) => lit.value(),
			Err(e) => panic!("{macro_name}: expected a string literal directory path: {e}"),
		}
	};

	let manifest_dir =
		env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set during macro expansion");
	let abs_dir = PathBuf::from(&manifest_dir).join(&rel_dir);

	if !abs_dir.exists() {
		panic!("{macro_name}: directory does not exist: {}", abs_dir.display());
	}

	(rel_dir, abs_dir)
}

/// Collect, sorted, the `(rel_display, abs_str)` of every `.surql` file under
/// `abs_dir`. `rel_display` is the stable tracking key (`<rel_dir>/<relpath>`).
fn collect_surql(rel_dir: &str, abs_dir: &PathBuf) -> Vec<(String, String)> {
	let mut file_paths: Vec<PathBuf> = WalkDir::new(abs_dir)
		.follow_links(true)
		.into_iter()
		.filter_map(|e| e.ok())
		.filter(|e| e.file_type().is_file())
		.map(|e| e.into_path())
		.filter(|p| p.extension().and_then(|s| s.to_str()) == Some("surql"))
		.collect();
	file_paths.sort();

	file_paths
		.iter()
		.map(|abs_path| {
			let abs_str = abs_path.to_str().expect("non-UTF8 path in surql dir").to_string();
			let rel = abs_path.strip_prefix(abs_dir).expect("path not under surql dir");
			let rel_str = rel.to_str().expect("non-UTF8 relative path in surql dir");
			let rel_display = format!("{rel_dir}/{rel_str}").replace('\\', "/");
			(rel_display, abs_str)
		})
		.collect()
}

/// Embeds `.surql` schema files at compile time.
///
/// Generates a `pub mod embedded_schema` with a `SCHEMA` static and
/// an async `sync(db)` function that applies all files to the database.
///
/// # Usage
///
/// ```rust,ignore
/// surrealkit::embed_schema!();
/// surrealkit::embed_schema!("database/schema");
///
/// embedded_schema::sync(&db).await?;
/// ```
#[proc_macro]
pub fn embed_schema(input: TokenStream) -> TokenStream {
	let (rel_dir, abs_dir) = resolve_dir(input, "database/schema", "embed_schema!");

	let file_entries: Vec<_> = collect_surql(&rel_dir, &abs_dir)
		.into_iter()
		.map(|(rel_display, abs_str)| {
			quote! {
				::surrealkit::EmbeddedSchemaFile {
					path: #rel_display,
					sql: include_str!(#abs_str),
				}
			}
		})
		.collect();

	let expanded = quote! {
		pub mod embedded_schema {
			pub static SCHEMA: &[::surrealkit::EmbeddedSchemaFile] = &[
				#(#file_entries),*
			];

			pub async fn sync(
				db: &::surrealkit::Surreal<::surrealkit::engine::any::Any>,
			) -> ::surrealkit::anyhow::Result<()> {
				::surrealkit::Sync::embedded(SCHEMA).run(db).await
			}
		}
	};

	expanded.into()
}

/// Embeds `.surql` seed files at compile time.
///
/// Generates a `pub mod embedded_seed` with a `SEEDS` static and an async
/// `seed(db)` function. Seeding is tracked in the `__seed` table, so each file
/// runs only on first boot or when its content changes.
///
/// # Usage
///
/// ```rust,ignore
/// surrealkit::embed_seed!();
/// surrealkit::embed_seed!("database/seed");
///
/// embedded_seed::seed(&db).await?;
/// ```
#[proc_macro]
pub fn embed_seed(input: TokenStream) -> TokenStream {
	let (rel_dir, abs_dir) = resolve_dir(input, "database/seed", "embed_seed!");

	let file_entries: Vec<_> = collect_surql(&rel_dir, &abs_dir)
		.into_iter()
		.map(|(rel_display, abs_str)| {
			quote! {
				::surrealkit::EmbeddedSeedFile {
					path: #rel_display,
					sql: include_str!(#abs_str),
				}
			}
		})
		.collect();

	let expanded = quote! {
		pub mod embedded_seed {
			pub static SEEDS: &[::surrealkit::EmbeddedSeedFile] = &[
				#(#file_entries),*
			];

			pub async fn seed(
				db: &::surrealkit::Surreal<::surrealkit::engine::any::Any>,
			) -> ::surrealkit::anyhow::Result<()> {
				::surrealkit::Seed::embedded(SEEDS).run(db).await
			}
		}
	};

	expanded.into()
}
