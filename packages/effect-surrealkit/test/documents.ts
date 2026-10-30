/** Schema documents the emitter tests run on. */
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { semanticDigest } from '../src/emitter/index.js';

const corpus = join(
	import.meta.dirname,
	'..',
	'..',
	'..',
	'crates',
	'surrealkit',
	'tests',
	'fixtures',
	'typegen_corpus',
);

export const readJson = (path: string): Record<string, unknown> =>
	JSON.parse(readFileSync(path, 'utf8')) as Record<string, unknown>;

export const corpusPath = (...segments: string[]) => join(corpus, ...segments);

/** The contract's hand-checked example (crates/…/typegen_corpus/example.v2.json). */
export const examplePath = corpusPath('example.v2.json');

/** The JSON Schema for schema document v2. */
export const jsonSchemaPath = join(
	import.meta.dirname,
	'..',
	'..',
	'..',
	'crates',
	'surrealkit',
	'schema',
	'schema-document-v2.schema.json',
);

/**
 * Documents `surrealkit compile` produced from the corpus, pinned byte for
 * byte by SurrealKit's own test suite (`compile::corpus::the_compiled_*`):
 * `corpus` holds every corpus module (3.3, with the experimental ones) and
 * `field-presence` the tables of `semantics/field_presence.surql`.
 */
export const compiledPath = (name: 'corpus' | 'field-presence') =>
	corpusPath('compiled', `${name}.v2.json`);

export const sha256 = (text: string): string =>
	createHash('sha256').update(text, 'utf8').digest('hex');

/** The compiled document for the presence-evidence tables (`e`, `ip`, `m`, `oa`, `p`, `up`). */
export const presenceDocument = (): Record<string, unknown> =>
	readJson(compiledPath('field-presence'));

/**
 * `document` with only the named tables and functions (and the diagnostics
 * about them), its `semanticDigest` recomputed.
 */
export const subsetOf = (
	document: Record<string, unknown>,
	keep: { tables: string[]; functions: string[] },
): Record<string, unknown> => {
	const named =
		(names: string[]) =>
		(entry: { name?: unknown }): boolean =>
			typeof entry.name === 'string' && names.includes(entry.name);
	const subset: Record<string, unknown> = {
		...document,
		tables: (document.tables as Array<{ name?: unknown }>).filter(
			named(keep.tables),
		),
		functions: (document.functions as Array<{ name?: unknown }>).filter(
			named(keep.functions),
		),
		diagnostics: (
			document.diagnostics as Array<{
				table?: unknown;
				function?: unknown;
			}>
		).filter(
			(d) =>
				(typeof d.table === 'string' &&
					keep.tables.includes(d.table)) ||
				(typeof d.function === 'string' &&
					keep.functions.includes(d.function)),
		),
	};
	return { ...subset, semanticDigest: semanticDigest(subset, sha256) };
};

/**
 * The error codes the corpus acknowledges, with their reasons (`corpus.json`
 * `acknowledge`): generating from the compiled corpus passes them, as a
 * project's `[typegen] acknowledge` would.
 */
export const corpusAcknowledge = readJson(corpusPath('corpus.json'))
	.acknowledge as Readonly<Record<string, string>>;

/** Emitter options for documents compiled from the corpus. */
export const corpusOptions = {
	store: 'corpus',
	acknowledge: corpusAcknowledge,
} as const;

/** Golden variants: an input document, emitter options, and an output directory. */
export const goldens = [
	{
		name: 'example',
		document: () => readJson(examplePath),
		options: { store: 'corpus' },
	},
	{
		name: 'example-bigint',
		document: () => readJson(examplePath),
		options: {
			store: 'corpus',
			int: 'bigint',
			datetime: 'effect-datetime',
			patch: true,
		},
	},
	{
		name: 'corpus',
		document: () => readJson(compiledPath('corpus')),
		options: corpusOptions,
	},
	{
		// The integer-keyed tables and the int64 function the round trips use in
		// the bigint profile. The whole corpus in that profile differs from the
		// `corpus` golden only in its integers, which emitter.test.ts checks.
		name: 'ints-bigint',
		document: () =>
			subsetOf(readJson(compiledPath('corpus')), {
				tables: ['id_int', 'id_union'],
				functions: ['fn::optional_first'],
			}),
		options: { ...corpusOptions, int: 'bigint' },
	},
	{
		name: 'presence',
		document: presenceDocument,
		options: { store: 'presence' },
	},
] as const;

export const goldenDir = (name: string) =>
	join(import.meta.dirname, 'golden', name);

/**
 * Generated text with the versions of the tools that wrote it replaced by `*`:
 * the emitter's own version, and the SurrealKit version, commit and engine
 * version `manifest.ts` copies from the document's provenance. They change with
 * every release, `surrealdb` bump or re-bless of the pinned documents without
 * changing what is generated, so goldens pin the generated modules, not which
 * release wrote them. The adapter (it changes with the engine minor, and with
 * it what is read) stays pinned, as does everything else.
 */
export const withoutToolVersions = (text: string): string =>
	text
		.replace(
			/(emitter: \{ name: "effect-surrealkit", version: )"[^"]*"/,
			'$1"*"',
		)
		.replace(/^\t\tcommit: "[^"]*",\n/m, '')
		.replace(/^(\t\t(?:surrealkit|engine): )"[^"]*",$/gm, '$1"*",');

/** {@link withoutToolVersions} over every file of a generated module set. */
export const withoutToolVersionsIn = (
	files: Record<string, string>,
): Record<string, string> =>
	Object.fromEntries(
		Object.entries(files).map(([path, text]) => [
			path,
			withoutToolVersions(text),
		]),
	);

/** The element at `index`, or a thrown error (tests mutate fixtures through it). */
export const at = <T>(items: ReadonlyArray<T>, index: number): T => {
	const item = items[index];
	if (item === undefined) throw new Error(`no element ${index}`);
	return item;
};
