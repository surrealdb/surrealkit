use std::fs;
use std::path::{Path, PathBuf};

use proc_macro::TokenStream;
use quote::quote;
use sha2::{Digest, Sha256};
use syn::parse::{Parse, ParseStream};
use syn::{LitStr, Token};
use walkdir::WalkDir;

#[derive(Clone, Copy, PartialEq, Eq)]
enum EmbedMode {
    Schema,
    DataMigration,
}

struct EmbedInput {
    paths: Vec<LitStr>,
}

impl Parse for EmbedInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut paths = Vec::new();
        while !input.is_empty() {
            paths.push(input.parse::<LitStr>()?);
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(EmbedInput { paths })
    }
}

#[proc_macro]
pub fn embed_migrations(input: TokenStream) -> TokenStream {
    let parsed = syn::parse_macro_input!(input as EmbedInput);
    match embed_files(parsed, EmbedMode::Schema) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

#[proc_macro]
pub fn embed_data_migrations(input: TokenStream) -> TokenStream {
    let parsed = syn::parse_macro_input!(input as EmbedInput);
    match embed_files(parsed, EmbedMode::DataMigration) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn embed_files(input: EmbedInput, mode: EmbedMode) -> syn::Result<proc_macro2::TokenStream> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").map_err(|_| {
        syn::Error::new(proc_macro2::Span::call_site(), "CARGO_MANIFEST_DIR not set")
    })?;
    let base = PathBuf::from(&manifest_dir);

    if input.paths.is_empty() {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "expected at least one argument",
        ));
    }

    let files = if input.paths.len() == 1 && is_directory(&base, &input.paths[0].value()) {
        discover_directory(&base, &input.paths[0], mode)?
    } else {
        resolve_explicit_files(&base, &input.paths, mode)?
    };

    let entries = files
        .iter()
        .map(|f| {
            let rel_path = &f.rel_path;
            let abs_path = &f.abs_path;
            let hash = &f.hash;
            quote! {
                surrealkit::schema_state::SchemaFile {
                    path: #rel_path.to_string(),
                    sql: include_str!(#abs_path).to_string(),
                    hash: #hash.to_string(),
                }
            }
        })
        .collect::<Vec<_>>();

    Ok(quote! {
        vec![ #(#entries),* ]
    })
}

struct EmbeddedFile {
    rel_path: String,
    abs_path: String,
    hash: String,
}

fn is_directory(base: &Path, path: &str) -> bool {
    base.join(path).is_dir()
}

fn discover_directory(
    base: &Path,
    dir_lit: &LitStr,
    mode: EmbedMode,
) -> syn::Result<Vec<EmbeddedFile>> {
    let dir_str = dir_lit.value();
    let dir_path = base.join(&dir_str);

    if !dir_path.is_dir() {
        return Err(syn::Error::new(
            dir_lit.span(),
            format!("directory not found: {}", dir_path.display()),
        ));
    }

    let mut files: Vec<PathBuf> = WalkDir::new(&dir_path)
        .follow_links(true)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.into_path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("surql"))
        .collect();

    files.sort();

    let mut out = Vec::with_capacity(files.len());
    for path in &files {
        let rel = path
            .strip_prefix(base)
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/");
        let abs = path.to_string_lossy().to_string();
        let content = fs::read_to_string(path).map_err(|e| {
            syn::Error::new(dir_lit.span(), format!("reading {}: {e}", path.display()))
        })?;
        if mode == EmbedMode::Schema {
            validate_schema_content(&rel, &content, dir_lit.span())?;
        }
        if mode == EmbedMode::DataMigration {
            validate_migration_prefix(path, dir_lit.span())?;
        }
        let hash = sha256_hex(content.as_bytes());
        out.push(EmbeddedFile {
            rel_path: rel,
            abs_path: abs,
            hash,
        });
    }

    if out.is_empty() {
        return Err(syn::Error::new(
            dir_lit.span(),
            format!("no .surql files found in {}", dir_path.display()),
        ));
    }

    Ok(out)
}

fn resolve_explicit_files(
    base: &Path,
    paths: &[LitStr],
    mode: EmbedMode,
) -> syn::Result<Vec<EmbeddedFile>> {
    let mut out = Vec::with_capacity(paths.len());
    for lit in paths {
        let rel = lit.value();
        let full = base.join(&rel);
        if !full.is_file() {
            return Err(syn::Error::new(
                lit.span(),
                format!("file not found: {}", full.display()),
            ));
        }
        let abs = full.to_string_lossy().to_string();
        let content = fs::read_to_string(&full).map_err(|e| {
            syn::Error::new(lit.span(), format!("reading {rel}: {e}"))
        })?;
        if mode == EmbedMode::Schema {
            validate_schema_content(&rel, &content, lit.span())?;
        }
        if mode == EmbedMode::DataMigration {
            validate_migration_prefix(&full, lit.span())?;
        }
        let hash = sha256_hex(content.as_bytes());
        out.push(EmbeddedFile {
            rel_path: rel,
            abs_path: abs,
            hash,
        });
    }
    Ok(out)
}

fn validate_migration_prefix(path: &Path, span: proc_macro2::Span) -> syn::Result<()> {
    let filename = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("");

    let has_numeric_prefix = filename
        .find('_')
        .is_some_and(|pos| pos > 0 && filename[..pos].chars().all(|c| c.is_ascii_digit()));

    if !has_numeric_prefix {
        return Err(syn::Error::new(
            span,
            format!(
                "{}: migration files must start with a numeric prefix (e.g. 001_name.surql)",
                path.file_name().unwrap_or_default().to_string_lossy()
            ),
        ));
    }
    Ok(())
}

fn validate_schema_content(
    path: &str,
    content: &str,
    span: proc_macro2::Span,
) -> syn::Result<()> {
    for stmt in split_statements(&strip_line_comments(content)) {
        let trimmed = stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        let upper = trimmed.to_ascii_uppercase();
        if upper.starts_with("REMOVE ") {
            return Err(syn::Error::new(
                span,
                format!(
                    "{path}: REMOVE statements are not allowed in schema files; use rollout steps for destructive changes"
                ),
            ));
        }
        if !upper.starts_with("DEFINE ") {
            return Err(syn::Error::new(
                span,
                format!(
                    "{path}: schema files must contain only DEFINE statements, found: '{}'",
                    truncate(trimmed, 96)
                ),
            ));
        }
    }
    Ok(())
}

fn strip_line_comments(sql: &str) -> String {
    sql.lines()
        .filter(|line| {
            let t = line.trim_start();
            !(t.starts_with("--") || t.starts_with("//"))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn split_statements(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let mut prev_escape = false;
    let mut brace_depth = 0usize;

    for ch in sql.chars() {
        match ch {
            '\'' if !in_double && !in_backtick && !prev_escape => in_single = !in_single,
            '"' if !in_single && !in_backtick && !prev_escape => in_double = !in_double,
            '`' if !in_single && !in_double && !prev_escape => in_backtick = !in_backtick,
            '{' if !in_single && !in_double && !in_backtick => brace_depth += 1,
            '}' if !in_single && !in_double && !in_backtick && brace_depth > 0 => brace_depth -= 1,
            ';' if !in_single && !in_double && !in_backtick && brace_depth == 0 => {
                let stmt = buf.trim();
                if !stmt.is_empty() {
                    out.push(stmt.to_string());
                }
                buf.clear();
                prev_escape = false;
                continue;
            }
            _ => {}
        }
        prev_escape = ch == '\\' && !prev_escape;
        buf.push(ch);
    }

    let tail = buf.trim();
    if !tail.is_empty() {
        out.push(tail.to_string());
    }

    out
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max])
    }
}
