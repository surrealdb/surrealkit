/**
 * Shared pieces of the `toCode` metadata Effect uses to generate source for
 * schemas built from these codecs.
 *
 * Runtime code refers to this package as `SurrealSchema` and SDK types as
 * `Surreal`, the same names the emitter uses in generated modules.
 */
/** The package name, as written in generated imports. */
export const PACKAGE_NAME = 'effect-surrealkit';

/** Namespace import for runtime references to this package. */
export const RUNTIME_IMPORT = `import * as SurrealSchema from "${PACKAGE_NAME}"`;

/** Namespace type import for SurrealDB SDK value classes. */
export const SDK_TYPE_IMPORT = 'import type * as Surreal from "surrealdb"';

/** Type import for the Effect datetime used by the datetime profiles. */
export const DATETIME_TYPE_IMPORT = 'import type { DateTime } from "effect"';

/** Renders a string literal the way generated code spells it. */
export const quote = (value: string): string => JSON.stringify(value);

/** Renders a readonly string-literal tuple: `["a", "b"]`. */
export const quoteList = (values: ReadonlyArray<string>): string =>
	`[${values.map(quote).join(', ')}]`;

/** Renders a string-literal union type: `"a" | "b"`, or `string` when empty. */
export const literalUnion = (values: ReadonlyArray<string>): string =>
	values.length === 0 ? 'string' : values.map(quote).join(' | ');

export interface SchemaCode {
	readonly runtime: string;
	readonly Type: string;
}

export interface CodeOutput extends SchemaCode {
	readonly importDeclarations: ReadonlyArray<string>;
}

/** `toCode` output for a value of this package referenced by name. */
export const runtimeCode = (
	runtime: string,
	Type: string,
	extraImports: ReadonlyArray<string> = [],
): CodeOutput => ({
	runtime: `SurrealSchema.${runtime}`,
	Type,
	importDeclarations: [RUNTIME_IMPORT, SDK_TYPE_IMPORT, ...extraImports],
});
