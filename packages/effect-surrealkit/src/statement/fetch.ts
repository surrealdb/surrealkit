/**
 * `FETCH`: a select's record links replaced by the records they point at.
 *
 * A path names a field of what the select returns (the stored row, or the
 * projection's aliases), dot-separated through objects and fetched records
 * (`manager`, `manager.team`). Its type changes from a record id to the
 * target's row; through a list of links to a list of rows. SurrealDB 3.3
 * and 4.0 answer NONE for a link whose record is missing or the session
 * may not read, so a fetched link may be absent and each record in a
 * fetched list may be NONE.
 */
import { Schema } from 'effect';
import { escapeIdent, type RecordId } from 'surrealdb';

import {
	type GraphStore,
	isOptionalField,
	type LinkTables,
	linkShape,
	selectFields,
	shapeTables,
	type TableIn,
	union,
	unwrapField,
} from './graph.js';
import { type Fields, fieldsOf, projectedCodec, rowCodec } from './row.js';
import type { Codec, Simplify } from './statement.js';
import type { PureTable } from './table.js';

// ---- types

type Split<P extends string> = P extends `${infer H}.${infer R}`
	? readonly [H, ...Split<R>]
	: readonly [P];

type Defined<V> = Exclude<V, undefined>;

/** The link type at a value: one id, or the id of each element of a list. */
type LinkOf<V> = [Defined<V>] extends [never]
	? never
	: [Defined<V>] extends [RecordId]
		? Defined<V>
		: Defined<V> extends ReadonlyArray<infer I>
			? [Defined<I>] extends [never]
				? never
				: [Defined<I>] extends [RecordId]
					? Defined<I>
					: never
			: never;

type TableNames<S> = S extends { readonly tables: infer M } ? keyof M : never;

/** The row type of every table a link of type `V` can point at. */
export type FetchTarget<S, V> =
	TableIn<S, LinkTables<V>> extends infer T
		? T extends { readonly select: { readonly Type: infer X } }
			? X
			: never
		: never;

/** The object a path walks into: through lists of objects too. */
type Container<V> =
	Defined<V> extends ReadonlyArray<infer I> ? Container<I> : Defined<V>;

type Problem<V, Segs, Path extends string, S> = Segs extends readonly [
	infer H extends string,
	...infer R extends ReadonlyArray<string>,
]
	? Container<V> extends infer O
		? [O] extends [object]
			? H extends keyof O
				? ProblemAt<O[H], R, Path, S>
				: NoField<Path, H>
			: NoField<Path, H>
		: never
	: never;

type NoField<Path extends string, H extends string> = Path extends H
	? `\`${H}\` is not a field of what the select returns`
	: `\`${Path}\`: \`${H}\` is not a field of what the select returns`;

type ProblemAt<X, R, Path extends string, S> = [LinkOf<X>] extends [never]
	? R extends readonly []
		? `\`${Path}\` is not a record link, so FETCH has nothing to replace`
		: Problem<X, R, Path, S>
	: string extends LinkTables<LinkOf<X>>
		? `\`${Path}\` can point at any table (\`record\`), so FETCH has no row type for it`
		: [Exclude<LinkTables<LinkOf<X>>, TableNames<S>>] extends [never]
			? R extends readonly []
				? never
				: Problem<FetchTarget<S, LinkOf<X>>, R, Path, S>
			: `\`${Path}\` points at \`${Exclude<LinkTables<LinkOf<X>>, TableNames<S>> & string}\`, which is not a table of this store`;

/** Why `Path` cannot be fetched from rows of type `T`, or `never`. */
export type FetchProblem<T, Path extends string, S> = Problem<
	T,
	Split<Path>,
	Path,
	S
>;

/** The paths as the caller wrote them, each checked: a wrong one is a message. */
export type CheckedFetch<T, S, Paths extends ReadonlyArray<string>> = {
	readonly [I in keyof Paths]: Paths[I] extends string
		? [FetchProblem<T, Paths[I], S>] extends [never]
			? Paths[I]
			: FetchProblem<T, Paths[I], S>
		: Paths[I];
};

type FetchIn<T, Segs, S> = Segs extends readonly [
	infer H extends string,
	...infer R extends ReadonlyArray<string>,
]
	? T extends ReadonlyArray<infer I>
		? ReadonlyArray<FetchIn<I, Segs, S>>
		: T extends RecordId
			? T
			: T extends object
				? H extends keyof T
					? FetchKey<T, H, R, S>
					: T
				: T
	: T;

type IsLink<V> = [Defined<V>] extends [never]
	? false
	: [Defined<V>] extends [RecordId]
		? true
		: false;

type FetchKey<T, H extends keyof T, R, S> =
	IsLink<T[H]> extends true
		? Simplify<
				{ readonly [K in keyof T as K extends H ? never : K]: T[K] } & {
					readonly [K in H]?:
						| FetchLink<Defined<T[H]>, R, S>
						| undefined;
				}
			>
		: {
				readonly [K in keyof T]: K extends H
					? FetchValue<T[K], R, S>
					: T[K];
			};

type FetchValue<V, R, S> = V extends undefined
	? V
	: V extends ReadonlyArray<infer I>
		? IsLink<I> extends true
			? ReadonlyArray<FetchLink<Defined<I>, R, S> | undefined>
			: ReadonlyArray<FetchIn<I, R, S>>
		: FetchIn<V, R, S>;

type FetchLink<V, R, S> = R extends readonly []
	? FetchTarget<S, V>
	: FetchIn<FetchTarget<S, V>, R, S>;

type FetchAll<T, Paths, S> = Paths extends readonly [
	infer P extends string,
	...infer Rest,
]
	? FetchAll<FetchIn<T, Split<P>, S>, Rest, S>
	: T;

/** The row codec of a select after `FETCH` of `Paths`. */
export type Fetched<Row extends Codec, S, Paths> = Schema.Codec<
	FetchAll<Row['Type'], Paths, S>,
	unknown,
	never,
	never
>;

// ---- run time

/** What a select returns, as FETCH rewrites it. @internal */
export type FetchBase =
	| { readonly kind: 'row'; readonly table: PureTable }
	| { readonly kind: 'projection'; readonly fields: Fields }
	| { readonly kind: 'value' };

interface Tree {
	readonly [segment: string]: Tree;
}

const treeOf = (paths: ReadonlyArray<string>): Tree => {
	const root: Record<string, Record<string, unknown>> = {};
	for (const path of paths) {
		let node: Record<string, unknown> = root;
		for (const segment of path.split('.')) {
			node[segment] ??= {};
			node = node[segment] as Record<string, unknown>;
		}
	}
	return root as Tree;
};

/** A path's text: each segment quoted where SurrealQL needs it. @internal */
export const fetchPath = (path: string): string => {
	const segments = path.split('.');
	if (segments.some((segment) => segment.length === 0)) {
		throw new RangeError(
			`\`${path}\` is not a FETCH path: name fields separated by dots`,
		);
	}
	return segments.map((segment) => escapeIdent(segment)).join('.');
};

const rebuildRow = (row: Codec, fields: Fields): Codec =>
	'records' in row && 'schema' in row
		? (Schema.StructWithRest(
				Schema.Struct(fields),
				row.records as never,
			) as unknown as Codec)
		: (Schema.Struct(fields) as unknown as Codec);

const targetsOf = (
	store: GraphStore,
	tables: ReadonlyArray<string> | 'any',
	path: string,
): ReadonlyArray<PureTable> => {
	if (tables === 'any') {
		throw new TypeError(
			`\`${path}\` can point at any table (\`record\`), so FETCH has no row type for it`,
		);
	}
	return tables.map((name) => {
		const table = store.tables[name];
		if (table === undefined) {
			throw new RangeError(
				`\`${path}\` points at \`${name}\`, which is not a table of this store`,
			);
		}
		return table;
	});
};

const fetchedRow = (
	store: GraphStore,
	tables: ReadonlyArray<string> | 'any',
	tree: Tree,
	path: string,
): Codec =>
	union(
		targetsOf(store, tables, path).map((table) =>
			Object.keys(tree).length === 0
				? (rowCodec(table.select) as Codec)
				: (rowCodec(
						rebuildRow(
							table.select,
							rewrite(store, selectFields(table), tree, path),
						),
					) as Codec),
		),
	);

const rewriteValue = (
	store: GraphStore,
	codec: Codec,
	tree: Tree,
	path: string,
): { readonly codec: Codec; readonly absent: boolean } => {
	const shape = linkShape(codec);
	if (shape !== undefined) {
		const inner = shape.kind === 'optional' ? shape.of : shape;
		if (inner.kind === 'id') {
			// A missing or unreadable record: NONE, which reads as an absent key.
			return {
				codec: fetchedRow(store, inner.tables, tree, path),
				absent: true,
			};
		}
		if (inner.kind === 'list' && inner.of.kind !== 'list') {
			const row = fetchedRow(store, shapeTables(inner.of), tree, path);
			const list = Schema.Array(
				Schema.UndefinedOr(row),
			) as unknown as Codec;
			return {
				codec:
					shape.kind === 'optional'
						? (Schema.UndefinedOr(list) as unknown as Codec)
						: list,
				absent: false,
			};
		}
	}
	const nested = fieldsOf(codec);
	if (nested !== undefined && Object.keys(tree).length > 0) {
		return {
			codec: rebuildRow(codec, rewrite(store, nested, tree, path)),
			absent: false,
		};
	}
	if (
		'value' in codec &&
		Schema.isSchema(codec.value) &&
		Object.keys(tree).length > 0
	) {
		const item = fieldsOf(codec.value as Codec);
		if (item !== undefined) {
			return {
				codec: Schema.Array(
					rebuildRow(
						codec.value as Codec,
						rewrite(store, item, tree, path),
					),
				) as unknown as Codec,
				absent: false,
			};
		}
	}
	throw new TypeError(
		`\`${path}\` is not a record link, so FETCH has nothing to replace`,
	);
};

const rewrite = (
	store: GraphStore,
	fields: Fields,
	tree: Tree,
	prefix: string,
): Fields => {
	const next: Record<string, Codec> = { ...fields };
	for (const [key, subtree] of Object.entries(tree)) {
		const path = prefix === '' ? key : `${prefix}.${key}`;
		const declared = fields[key];
		if (declared === undefined) {
			throw new RangeError(
				path === key
					? `\`${key}\` is not a field of what the select returns`
					: `\`${path}\`: \`${key}\` is not a field of what the select returns`,
			);
		}
		const optional = isOptionalField(declared);
		const { codec, absent } = rewriteValue(
			store,
			unwrapField(declared),
			subtree,
			path,
		);
		next[key] = (
			absent
				? Schema.optional(codec)
				: optional
					? Schema.optionalKey(codec)
					: codec
		) as Codec;
	}
	return next;
};

/**
 * The codec of what a select returns after `FETCH` of `paths`: the stored
 * row or the projection, with each fetched link replaced by its target's
 * row.
 *
 * @internal
 */
export const fetchedCodec = (
	store: GraphStore,
	base: FetchBase,
	paths: ReadonlyArray<string>,
): Codec => {
	for (const path of paths) fetchPath(path);
	const tree = treeOf(paths);
	switch (base.kind) {
		case 'row':
			return rowCodec(
				rebuildRow(
					base.table.select,
					rewrite(store, selectFields(base.table), tree, ''),
				),
			) as Codec;
		case 'projection':
			return projectedCodec(
				rewrite(store, base.fields, tree, '') as never,
			) as Codec;
		case 'value':
			throw new TypeError(
				'FETCH replaces fields of the row or of a projection; `SELECT VALUE` has none: project the value instead',
			);
	}
};
