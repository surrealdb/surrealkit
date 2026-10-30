/**
 * Shared pieces of the `toCode` metadata every codec carries, so that
 * `SchemaRepresentation.toCodeDocument` can print a schema built from these
 * codecs back to source.
 *
 * Runtime code refers to this package as `SurrealSchema` and SDK types as
 * `Surreal`, the same names the emitter uses in generated modules.
 */
import { type Schema, SchemaRepresentation } from 'effect';

/** The package name, as written in generated imports. */
export const PACKAGE_NAME = 'effect-surrealkit';

/** Namespace import for runtime references to this package. */
export const RUNTIME_IMPORT = `import * as SurrealSchema from "${PACKAGE_NAME}"`;

/** Namespace type import for SurrealDB SDK value classes. */
export const SDK_TYPE_IMPORT = 'import type * as Surreal from "surrealdb"';

/** `effect` namespace import for generated code that names `Schema`. */
export const EFFECT_IMPORT = 'import { Schema } from "effect"';

/** Renders a string literal the way generated code spells it. */
export const quote = (value: string): string => JSON.stringify(value);

/** Renders a readonly string-literal tuple: `["a", "b"]`. */
export const quoteList = (values: ReadonlyArray<string>): string =>
	`[${values.map(quote).join(', ')}]`;

/** Renders a string-literal union type: `"a" | "b"`, or `string` when empty. */
export const literalUnion = (values: ReadonlyArray<string>): string =>
	values.length === 0 ? 'string' : values.map(quote).join(' | ');

export interface CodeOutput {
	readonly runtime: string;
	readonly Type: string;
	readonly importDeclarations: ReadonlyArray<string>;
}

/**
 * Source for an arbitrary schema, through Effect's own code generator. Used
 * by codecs that close over schemas rather than taking type parameters.
 * TypeScript-only distinctions such as brands do not survive (see
 * `SchemaRepresentation.toRepresentation`).
 */
export const codeOf = (
	schema: Schema.Top,
): { readonly runtime: string; readonly Type: string } => {
	const document = SchemaRepresentation.toCodeDocument(
		SchemaRepresentation.toMultiDocument(
			SchemaRepresentation.toRepresentation(schema.ast),
		),
	);
	const [code] = document.codes;
	if (code === undefined) throw new Error('no code generated');
	return code;
};

/** `toCode` output for a value of this package referenced by name. */
export const runtimeCode = (runtime: string, Type: string): CodeOutput => ({
	runtime: `SurrealSchema.${runtime}`,
	Type,
	importDeclarations: [RUNTIME_IMPORT, SDK_TYPE_IMPORT],
});
