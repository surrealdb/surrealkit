//! Diagnostics: everything typegen could not represent structurally, said out
//! loud instead of being dropped or silently typed as unknown.
//!
//! The shape and the codes are those of schema document v2
//! (`docs/typegen/schema-document-v2.md`, section 14), so the v1 generator and
//! the v2 document report problems the same way. Codes are stable identifiers;
//! reasons are prose and may change.
//!
//! Severity decides what a strict run does: `surrealkit typegen --strict` (and
//! `[typegen] strict`) fails on any `error`, and logs warnings and infos.

use std::fmt;

use serde::{Deserialize, Serialize};

/// The stable diagnostic codes typegen raises.
pub mod codes {
	/// A kind string could not be parsed; the kind became `unknown`. Error.
	pub const KIND_UNRECOGNIZED: &str = "kind-unrecognized";
	/// `INFO` returned an entry in a category the document does not model. Warning.
	pub const DEFINITION_UNSUPPORTED: &str = "definition-unsupported";
	/// `COMPUTED` without `TYPE`: the value can be anything. Warning.
	pub const FIELD_COMPUTED_KIND_UNDECLARED: &str = "field-computed-kind-undeclared";
	/// A non-computed field has no `TYPE`: it holds any value. Info.
	pub const FIELD_KIND_UNDECLARED: &str = "field-kind-undeclared";

	// Schema document v2 only.

	/// The engine minor has no `INFO … STRUCTURE` adapter. Error.
	pub const STRUCTURE_UNSUPPORTED_ENGINE: &str = "structure-unsupported-engine";
	/// `INFO … STRUCTURE` returned a key the adapter does not map. Error.
	pub const STRUCTURE_UNKNOWN_KEY: &str = "structure-unknown-key";
	/// A `REFERENCE` whose `ON DELETE` action is not one the contract knows. Error.
	pub const FIELD_REFERENCE_UNRECOGNIZED: &str = "field-reference-unrecognized";
	/// `DEFINE FIELD id` carries clauses other than `TYPE`. Warning.
	pub const FIELD_ID_CLAUSES_IGNORED: &str = "field-id-clauses-ignored";
	/// `VALUE` reads its input and has a fallback; the conservative contract applies. Warning.
	pub const FIELD_VALUE_INPUT_CONDITIONAL: &str = "field-value-input-conditional";
	/// An optional function argument precedes a required one. Warning.
	pub const FUNCTION_OPTIONAL_ARG_NOT_TRAILING: &str = "function-optional-arg-not-trailing";
	/// The compiled modules define the same entity more than once. Warning.
	pub const DEFINITION_DUPLICATE: &str = "definition-duplicate";
	/// A compiled `DEFINE PARAM` stores a different value each time it is defined. Error.
	pub const PARAM_VALUE_NONDETERMINISTIC: &str = "param-value-nondeterministic";
	/// The project deploys to an engine minor other than the one compile embeds. Error.
	pub const COMPILE_ENGINE_MISMATCH: &str = "compile-engine-mismatch";
	/// An object of a SCHEMAFULL table is not FLEXIBLE and has no declared fields, so it
	/// can hold only `{}`. Error.
	pub const OBJECT_WITHOUT_FIELDS: &str = "object-without-fields";
	/// A top-level field name begins with an object field's name (`envelopealgorithm`
	/// beside `envelope`): perhaps a child path missing its dot. Warning.
	pub const FIELD_NAME_RUN_TOGETHER: &str = "field-name-run-together";
	/// A nested field's parent path is not declared. Info.
	pub const FIELD_PARENT_IMPLICIT: &str = "field-parent-implicit";
	/// `ASSERT` is outside the recognised subset. Info.
	pub const ASSERT_UNRECOGNIZED: &str = "assert-unrecognized";
	/// A function declares no return kind. Info.
	pub const FUNCTION_RETURN_UNDECLARED: &str = "function-return-undeclared";
	/// A view: its row shape is not derived. Info.
	pub const TABLE_VIEW_SHAPE: &str = "table-view-shape";
	/// A lightweight relation: no content schemas, `relate` only. Info.
	pub const TABLE_LIGHTWEIGHT_RELATION: &str = "table-lightweight-relation";
	/// More than one middleware; their order is not preserved. Info.
	pub const API_MIDDLEWARE_ORDER: &str = "api-middleware-order";
}

/// How serious a [`Diagnostic`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
	/// The output does not describe the schema; strict runs fail.
	Error,
	/// The output is correct but weaker than the schema deserves.
	Warning,
	/// Worth knowing, not worth fixing.
	Info,
}

impl fmt::Display for Severity {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(match self {
			Severity::Error => "error",
			Severity::Warning => "warning",
			Severity::Info => "info",
		})
	}
}

/// A UTF-8 byte range within a diagnostic's `source`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Span {
	pub start: usize,
	pub end: usize,
}

/// One thing typegen could not represent structurally.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Diagnostic {
	pub severity: Severity,
	/// A stable identifier from [`codes`].
	pub code: String,
	/// The table the diagnostic is about, if any.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub table: Option<String>,
	/// The field path, as segments (`["address", "zip"]`, `["tags", "*"]`).
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub field: Option<Vec<String>>,
	/// The function (with its `fn::` prefix) the diagnostic is about, if any.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub function: Option<String>,
	/// The exact text the diagnostic is about.
	pub source: String,
	/// The part of `source` at fault, when narrower than all of it.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub span: Option<Span>,
	/// A human-readable explanation.
	pub reason: String,
}

impl Diagnostic {
	pub(crate) fn new(
		severity: Severity,
		code: &str,
		source: impl Into<String>,
		reason: impl Into<String>,
	) -> Self {
		Self {
			severity,
			code: code.to_string(),
			table: None,
			field: None,
			function: None,
			source: source.into(),
			span: None,
			reason: reason.into(),
		}
	}

	pub(crate) fn on_table(mut self, table: &str) -> Self {
		self.table = Some(table.to_string());
		self
	}

	pub(crate) fn on_field(mut self, path: Vec<String>) -> Self {
		self.field = Some(path);
		self
	}

	pub(crate) fn on_function(mut self, name: &str) -> Self {
		self.function = Some(name.to_string());
		self
	}

	pub(crate) fn with_span(mut self, span: std::ops::Range<usize>) -> Self {
		self.span = Some(Span {
			start: span.start,
			end: span.end,
		});
		self
	}

	/// Whether a strict run must fail on this diagnostic.
	pub fn is_error(&self) -> bool {
		self.severity == Severity::Error
	}
}

impl fmt::Display for Diagnostic {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{} {}", self.severity, self.code)?;
		match (&self.table, &self.field, &self.function) {
			(Some(table), Some(field), _) => write!(f, " at {table}.{}", field.join("."))?,
			(Some(table), None, _) => write!(f, " at {table}")?,
			(None, _, Some(function)) => write!(f, " at {function}")?,
			_ => {}
		}
		write!(f, ": {} (`{}`)", self.reason, self.source)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn serialises_in_the_document_v2_shape() {
		let d = Diagnostic::new(Severity::Error, codes::KIND_UNRECOGNIZED, "range<int>", "nope")
			.on_table("k")
			.on_field(vec!["r".into()])
			.with_span(5..10);
		let json = serde_json::to_value(&d).unwrap();
		assert_eq!(
			json,
			serde_json::json!({
				"severity": "error", "code": "kind-unrecognized", "table": "k", "field": ["r"],
				"source": "range<int>", "span": { "start": 5, "end": 10 }, "reason": "nope"
			})
		);
		assert_eq!(serde_json::from_value::<Diagnostic>(json).unwrap(), d);
	}

	#[test]
	fn displays_where_and_why() {
		let d = Diagnostic::new(Severity::Warning, codes::DEFINITION_UNSUPPORTED, "X", "why")
			.on_function("fn::f");
		assert_eq!(d.to_string(), "warning definition-unsupported at fn::f: why (`X`)");
		let d = Diagnostic::new(Severity::Info, codes::FIELD_KIND_UNDECLARED, "Y", "any value")
			.on_table("person")
			.on_field(vec!["prefs".into(), "locale".into()]);
		assert_eq!(
			d.to_string(),
			"info field-kind-undeclared at person.prefs.locale: any value (`Y`)"
		);
	}
}
