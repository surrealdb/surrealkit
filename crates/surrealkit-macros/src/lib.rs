use std::env;
use std::path::PathBuf;

use proc_macro::TokenStream;
use quote::quote;
use syn::{LitStr, parse_macro_input};
use walkdir::WalkDir;

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
	let schema_dir = if input.is_empty() {
		"database/schema".to_string()
	} else {
		let lit = parse_macro_input!(input as LitStr);
		lit.value()
	};

	let manifest_dir =
		env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set during macro expansion");
	let abs_schema_dir = PathBuf::from(&manifest_dir).join(&schema_dir);

	if !abs_schema_dir.exists() {
		panic!("embed_schema!: schema directory does not exist: {}", abs_schema_dir.display());
	}

	let mut file_paths: Vec<PathBuf> = WalkDir::new(&abs_schema_dir)
		.follow_links(true)
		.into_iter()
		.filter_map(|e| e.ok())
		.filter(|e| e.file_type().is_file())
		.map(|e| e.into_path())
		.filter(|p| p.extension().and_then(|s| s.to_str()) == Some("surql"))
		.collect();
	file_paths.sort();

	let file_entries: Vec<_> = file_paths
		.iter()
		.map(|abs_path| {
			let abs_str = abs_path.to_str().expect("non-UTF8 path in schema dir");
			let rel = abs_path.strip_prefix(&abs_schema_dir).expect("path not under schema dir");
			let rel_display =
				format!("{}/{}", schema_dir, rel.to_str().unwrap()).replace('\\', "/");

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
				::surrealkit::run_sync_embedded(db, SCHEMA).await
			}
		}
	};

	expanded.into()
}
