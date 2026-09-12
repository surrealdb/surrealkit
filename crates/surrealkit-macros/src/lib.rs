use std::env;
use std::path::PathBuf;

use proc_macro::TokenStream;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::{Ident, LitStr, Token};
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

/// The tracking key prefix for an embedded directory.
///
/// Keys must match the ones the CLI writes, or an app booting with
/// `embed_seed!` and a developer running `surrealkit seed` track the same file
/// under two names, each migration deleting the other's row, and the seed re-runs
/// on every alternation.
///
/// The CLI keys relative to the project folder: `<folder>/schema/a.surql` is
/// tracked as `schema/a.surql`, and `<folder>/modules/billing/schema/a.surql` as
/// `modules/billing/schema/a.surql`. The macro argument is folder-prefixed
/// (`database/schema`), so dropping its first segment lands on the same key.
///
/// A single-segment argument has no folder to drop and is used as-is.
fn tracking_prefix(rel_dir: &str) -> String {
	let normalised = rel_dir.replace('\\', "/");
	let trimmed = normalised.trim_matches('/');
	match trimmed.split_once('/') {
		Some((_folder, rest)) => rest.to_string(),
		None => trimmed.to_string(),
	}
}

/// Collect, sorted, the `(rel_display, abs_str)` of every `.surql` file under
/// `abs_dir`. `rel_display` is the stable tracking key, relative to the project
/// folder so that it matches what the CLI writes for the same file.
fn collect_surql(rel_dir: &str, abs_dir: &PathBuf) -> Vec<(String, String)> {
	let prefix = tracking_prefix(rel_dir);
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
			let rel_display = if prefix.is_empty() {
				rel_str.replace('\\', "/")
			} else {
				format!("{prefix}/{rel_str}").replace('\\', "/")
			};
			(rel_display, abs_str)
		})
		.collect()
}

/// One `name = "path"` arm of a named-module macro invocation.
struct ModuleArm {
	name: Ident,
	path: LitStr,
}

impl syn::parse::Parse for ModuleArm {
	fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
		let name: Ident = input.parse()?;
		input.parse::<Token![=]>()?;
		let path: LitStr = input.parse()?;
		Ok(Self {
			name,
			path,
		})
	}
}

/// What a macro invocation asked for.
enum Invocation {
	/// `embed_schema!()` or `embed_schema!("dir")` — one unnamed default module.
	Single(TokenStream),
	/// `embed_schema!(core = "dir", billing = "dir")` — several named modules.
	Named(Vec<ModuleArm>),
}

/// Distinguish the legacy single-directory form from the named-module form.
///
/// The legacy forms are tried first and are unchanged, so existing call sites
/// expand exactly as before.
fn parse_invocation(input: TokenStream, macro_name: &str) -> Invocation {
	if input.is_empty() {
		return Invocation::Single(input);
	}
	if syn::parse::<LitStr>(input.clone()).is_ok() {
		return Invocation::Single(input);
	}
	let parser = Punctuated::<ModuleArm, Token![,]>::parse_terminated;
	match syn::parse::Parser::parse(parser, input) {
		Ok(arms) if !arms.is_empty() => Invocation::Named(arms.into_iter().collect()),
		Ok(_) => panic!("{macro_name}: expected at least one `name = \"path\"` entry"),
		Err(e) => panic!(
			"{macro_name}: expected a string literal directory, or \
			 `name = \"path\"` entries separated by commas: {e}"
		),
	}
}

/// Embeds `.surql` schema files at compile time.
///
/// Generates a `pub mod embedded_schema` with a `SCHEMA` static and
/// an async `sync(db)` function that applies all files to the database.
///
/// # Usage
///
/// # Usage
///
/// ```rust,ignore
/// // One unnamed (default) module -- the pre-1.0 forms, unchanged.
/// surrealkit::embed_schema!();
/// surrealkit::embed_schema!("database/schema");
/// embedded_schema::sync(&db).await?;
/// ```
///
/// Several named modules, each tracked and pruned independently. Order the arms
/// so a module follows the ones it depends on:
///
/// ```rust,ignore
/// surrealkit::embed_schema!(
///     core    = "database/modules/core/schema",
///     billing = "database/modules/billing/schema",
/// );
///
/// embedded_schema::sync(&db).await?;          // every module, in order
/// embedded_schema::billing::sync(&db).await?; // just one
/// ```
///
/// `embedded_schema::sync` exists in both forms and means "all of it", so moving
/// from one module to several needs no call-site change.
///
/// # Rebuilds
///
/// `include_str!` makes cargo re-run when an embedded file *changes*, but the
/// directory listing is not tracked, so **adding** a new `.surql` file does not
/// trigger a rebuild. Add a `build.rs` with:
///
/// ```rust,ignore
/// println!("cargo:rerun-if-changed=database");
/// ```
#[proc_macro]
pub fn embed_schema(input: TokenStream) -> TokenStream {
	match parse_invocation(input, "embed_schema!") {
		Invocation::Single(input) => {
			let (rel_dir, abs_dir) = resolve_dir(input, "database/schema", "embed_schema!");
			let file_entries = schema_entries(&rel_dir, &abs_dir);
			quote! {
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
			}
			.into()
		}
		Invocation::Named(arms) => {
			let mut mods = Vec::new();
			let mut listing = Vec::new();
			let mut syncs = Vec::new();
			for arm in &arms {
				let ident = &arm.name;
				let name = ident.to_string();
				let (rel_dir, abs_dir) =
					resolve_named_dir(&arm.path.value(), &name, "embed_schema!");
				let file_entries = schema_entries(&rel_dir, &abs_dir);
				mods.push(quote! {
					pub mod #ident {
						/// This module's name, as used for metadata scoping.
						pub const NAME: &str = #name;

						pub static SCHEMA: &[::surrealkit::EmbeddedSchemaFile] = &[
							#(#file_entries),*
						];

						/// Apply just this module.
						pub async fn sync(
							db: &::surrealkit::Surreal<::surrealkit::engine::any::Any>,
						) -> ::surrealkit::anyhow::Result<()> {
							::surrealkit::Sync::embedded(SCHEMA).module(NAME)?.run(db).await
						}
					}
				});
				listing.push(quote! { (#name, #ident::SCHEMA) });
				syncs.push(quote! { #ident::sync(db).await?; });
			}
			quote! {
				pub mod embedded_schema {
					#(#mods)*

					/// Every embedded module, in declaration order.
					pub static MODULES:
						&[(&str, &[::surrealkit::EmbeddedSchemaFile])] = &[#(#listing),*];

					/// Apply every module, in declaration order. Order the macro
					/// arms so a module follows what it depends on.
					pub async fn sync(
						db: &::surrealkit::Surreal<::surrealkit::engine::any::Any>,
					) -> ::surrealkit::anyhow::Result<()> {
						#(#syncs)*
						Ok(())
					}
				}
			}
			.into()
		}
	}
}

/// Build the `EmbeddedSchemaFile` literals for one directory.
fn schema_entries(rel_dir: &str, abs_dir: &PathBuf) -> Vec<proc_macro2::TokenStream> {
	collect_surql(rel_dir, abs_dir)
		.into_iter()
		.map(|(rel_display, abs_str)| {
			quote! {
				::surrealkit::EmbeddedSchemaFile {
					path: #rel_display,
					sql: include_str!(#abs_str),
				}
			}
		})
		.collect()
}

/// Build the `EmbeddedSeedFile` literals for one directory.
fn seed_entries(rel_dir: &str, abs_dir: &PathBuf) -> Vec<proc_macro2::TokenStream> {
	collect_surql(rel_dir, abs_dir)
		.into_iter()
		.map(|(rel_display, abs_str)| {
			quote! {
				::surrealkit::EmbeddedSeedFile {
					path: #rel_display,
					sql: include_str!(#abs_str),
				}
			}
		})
		.collect()
}

/// Resolve a named module's directory, reporting which module failed.
fn resolve_named_dir(rel_dir: &str, module: &str, macro_name: &str) -> (String, PathBuf) {
	let manifest_dir =
		env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set during macro expansion");
	let abs_dir = PathBuf::from(&manifest_dir).join(rel_dir);
	if !abs_dir.exists() {
		panic!(
			"{macro_name}: directory for module `{module}` does not exist: {}",
			abs_dir.display()
		);
	}
	(rel_dir.to_string(), abs_dir)
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
	if let Invocation::Named(arms) = parse_invocation(input.clone(), "embed_seed!") {
		let mut mods = Vec::new();
		let mut listing = Vec::new();
		let mut seeds = Vec::new();
		for arm in &arms {
			let ident = &arm.name;
			let name = ident.to_string();
			let (rel_dir, abs_dir) = resolve_named_dir(&arm.path.value(), &name, "embed_seed!");
			let file_entries = seed_entries(&rel_dir, &abs_dir);
			mods.push(quote! {
				pub mod #ident {
					/// This module's name.
					pub const NAME: &str = #name;

					pub static SEEDS: &[::surrealkit::EmbeddedSeedFile] = &[
						#(#file_entries),*
					];

					/// Seed just this module.
					pub async fn seed(
						db: &::surrealkit::Surreal<::surrealkit::engine::any::Any>,
					) -> ::surrealkit::anyhow::Result<()> {
						::surrealkit::Seed::embedded(SEEDS).run(db).await
					}
				}
			});
			listing.push(quote! { (#name, #ident::SEEDS) });
			seeds.push(quote! { #ident::seed(db).await?; });
		}
		return quote! {
			pub mod embedded_seed {
				#(#mods)*

				/// Every embedded seed module, in declaration order.
				pub static MODULES: &[(&str, &[::surrealkit::EmbeddedSeedFile])] =
					&[#(#listing),*];

				/// Seed every module, in declaration order.
				pub async fn seed(
					db: &::surrealkit::Surreal<::surrealkit::engine::any::Any>,
				) -> ::surrealkit::anyhow::Result<()> {
					#(#seeds)*
					Ok(())
				}
			}
		}
		.into();
	}

	let (rel_dir, abs_dir) = resolve_dir(input, "database/seed", "embed_seed!");
	let file_entries = seed_entries(&rel_dir, &abs_dir);

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

#[cfg(test)]
mod tests {
	use super::tracking_prefix;

	#[test]
	fn tracking_prefix_drops_the_project_folder() {
		// What the CLI writes for the same files.
		assert_eq!(tracking_prefix("database/schema"), "schema");
		assert_eq!(tracking_prefix("database/seed"), "seed");
		assert_eq!(tracking_prefix("database/modules/billing/schema"), "modules/billing/schema");
	}

	#[test]
	fn tracking_prefix_tolerates_other_spellings() {
		assert_eq!(tracking_prefix("db/schema"), "schema");
		assert_eq!(tracking_prefix("/database/schema/"), "schema");
		assert_eq!(tracking_prefix("database\\schema"), "schema");
		// Nothing to drop: used as-is rather than emptied.
		assert_eq!(tracking_prefix("schema"), "schema");
	}
}
