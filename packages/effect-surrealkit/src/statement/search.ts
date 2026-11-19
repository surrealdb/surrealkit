/**
 * Full-text and nearest-neighbour search, and fused rankings of their
 * results, typed from the generated descriptors' indexes.
 *
 * A match or a nearest-neighbour search names its table and field, so the
 * compiler can check the index the engine needs: a full-text match needs a
 * `FULLTEXT` index on the field (SurrealDB 3.3 and 4.0 fail the query
 * without one) and the indexed nearest-neighbour form an `HNSW` or `DISKANN`
 * index (4.0.1 fails without one, 3.3 answers no rows). What the engine
 * accepts only as a literal (the match reference, the neighbour count, the
 * search effort, the distance) is a validated literal; the search terms and
 * the query vector are bindings.
 */
import { Schema, SchemaAST } from 'effect';

import { Int } from '../int.js';
import { NoneAsAbsent } from '../row.js';
import { Float } from '../scalars.js';
import {
	admit,
	type CodecOf,
	codecOf,
	derive,
	type Expr,
	isExpr,
	isPath,
	Level,
	literal,
	makeExpr,
	mergeParams,
	type OperandParams,
	operand,
	type ParamsOfAll,
	param,
	paramsOf,
	sqlOf,
} from './expr.js';
import { withRefs } from './refs.js';
import type { Count } from './select.js';
import {
	type Codec,
	make,
	type NoParams,
	type Params,
	type Simplify,
	type Statement,
} from './statement.js';
import { type PureTable, type RowExprs, rowExprs } from './table.js';

// ---- the descriptor's indexes, at the type level

/** A table descriptor with the indexes the emitter generates. */
export type SearchTable = PureTable & {
	readonly indexes?: {
		readonly [name: string]: {
			readonly type: string;
			readonly columns: ReadonlyArray<ReadonlyArray<string>>;
			readonly fulltext?: { readonly highlights: boolean };
			readonly vector?: { readonly dimension: number };
		};
	};
};

type IndexesOf<T> = T extends { readonly indexes?: infer I }
	? NonNullable<I>
	: never;

/** An index's field path, as `a.b`. */
type Joined<C> = C extends readonly [infer H extends string]
	? H
	: C extends readonly [infer H extends string, ...infer Rest]
		? `${H}.${Joined<Rest>}`
		: never;

/** The indexes of `T` of kind `Kind` on exactly the field `F`. */
type IndexOn<T, F extends string, Kind> = {
	readonly [K in keyof IndexesOf<T>]: IndexesOf<T>[K] extends {
		readonly type: Kind;
		readonly columns: readonly [infer C];
	}
		? [Joined<C>] extends [F]
			? [F] extends [Joined<C>]
				? IndexesOf<T>[K]
				: never
			: never
		: never;
}[keyof IndexesOf<T>];

type NameOf<T> = T extends { readonly name: infer N extends string }
	? N
	: string;

/** The expression of the field path `F` of a row object. */
type At<R, F extends string> = F extends `${infer H}.${infer Rest}`
	? H extends keyof R
		? At<R[H], Rest>
		: never
	: F extends keyof R
		? R[F]
		: never;

/** The codec of the field path `F` of `T`'s stored row. */
export type FieldCodec<T extends PureTable, F extends string> = CodecOf<
	At<RowExprs<T>, F>
>;

// ---- literals the engine parses only as literals

type Enumerate<
	N extends number,
	Acc extends ReadonlyArray<number> = [],
> = Acc['length'] extends N
	? Acc[number]
	: Enumerate<N, [...Acc, Acc['length']]>;

/** A match reference number: SurrealDB parses an integer from 0 to 255. */
export type MatchRef = Enumerate<256>;

/** `R`, or a message when it is not a match reference literal. */
export type CheckedRef<R> = [R] extends [undefined]
	? R
	: number extends R
		? 'a match reference is an integer literal from 0 to 255, written in the code: SurrealDB parses no binding there'
		: R extends MatchRef
			? R
			: 'a match reference is an integer from 0 to 255';

/**
 * `K`, or a message when it is not a positive integer literal: the
 * neighbour count and the search effort of `<|k, ef|>`, which SurrealDB
 * parses only as literals.
 */
export type CheckedCount<K, What extends string> = number extends K
	? `${What} is an integer literal written in the code: SurrealDB parses no binding there`
	: `${K & number}` extends `${string}.${string}` | `-${string}` | '0'
		? `${What} is a positive integer`
		: K;

const U32 = 4_294_967_295;

const assertRef = (ref: number): void => {
	if (!Number.isInteger(ref) || ref < 0 || ref > 255) {
		throw new RangeError(
			`${ref} is not a match reference: use an integer from 0 to 255`,
		);
	}
};

const assertCount = (value: number, what: string): string => {
	if (!Number.isInteger(value) || value < 1 || value > U32) {
		throw new RangeError(
			`${what} takes a positive integer up to ${U32}, not ${value}`,
		);
	}
	return String(value);
};

// ---- finding the index and the field at run time

interface IndexEntry {
	readonly name: string;
	readonly type: string;
	readonly fulltext?: { readonly highlights: boolean };
	readonly vector?: { readonly dimension: number };
}

const indexesOn = (
	table: SearchTable,
	field: string,
	kinds: ReadonlyArray<string>,
): ReadonlyArray<IndexEntry> =>
	Object.entries(table.indexes ?? {}).flatMap(([name, index]) =>
		kinds.includes(index.type) &&
		index.columns.length === 1 &&
		index.columns[0]?.join('.') === field
			? [{ name, ...index }]
			: [],
	);

const fieldExpr = (table: SearchTable, field: string): Expr => {
	let node: unknown = rowExprs(table);
	for (const part of field.split('.')) {
		node =
			typeof node === 'object' &&
			node !== null &&
			Object.hasOwn(node, part)
				? (node as Record<string, unknown>)[part]
				: undefined;
	}
	if (!isPath(node)) {
		throw new TypeError(
			`\`${table.name}\` has no field \`${field}\` in its select schema`,
		);
	}
	return node;
};

// ---- full-text match

export const MatchTypeId = '~effect-surrealkit/Match';

/**
 * A full-text match, `field @ref@ terms`: a condition, carrying its
 * reference number, the codec of the field it matches and whether its index
 * keeps highlights, for {@link score}, {@link highlight} and
 * {@link offsets}.
 */
export interface Match<
	R extends number | undefined,
	F extends Codec,
	H extends boolean,
	P extends Params,
> extends Expr<Schema.Boolean, P> {
	readonly [MatchTypeId]: {
		readonly ref: R;
		readonly field: F;
		readonly highlights: H;
		readonly sql: string;
	};
}

type AnyMatch = Match<number | undefined, Codec, boolean, Params>;

/** The field `F`, or a message when no `FULLTEXT` index of `T` covers it. */
export type FulltextField<T, F extends string> = [
	IndexOn<T, F, 'fulltext'>,
] extends [never]
	? `\`${NameOf<T>}\` has no FULLTEXT index on \`${F}\``
	: F;

type HighlightsOf<T, F extends string> =
	IndexOn<T, F, 'fulltext'> extends {
		readonly fulltext: { readonly highlights: infer H extends boolean };
	}
		? H
		: false;

/** The search terms: a string expression, or a string to inline. */
export type Terms =
	| Expr<Schema.Codec<unknown, string, never, never>, Params>
	| string;

/**
 * `field @ref@ terms`: the rows whose `field` matches the search terms,
 * through the `FULLTEXT` index on it. The field is a path of `table`'s
 * stored row (`"body"`, `"address.city"`) that a `FULLTEXT` index covers,
 * or the call is a compile error. `ref` (0 to 255) names the match for
 * {@link score}, {@link highlight} and {@link offsets}; `operator` makes
 * every term required (`AND`, `@1,AND@`) or any one enough (`OR`).
 *
 * @example
 * const hit = match(DocTable, 'body', param('terms', SurrealSchema.String), { ref: 1 });
 * select(DocTable)
 *   .where(() => hit)
 *   .project((row) => ({ id: row.id, score: score(hit) }));
 * // SELECT id AS id, search::score(1) AS score FROM ⟨doc⟩ WHERE body @1@ $terms
 */
export const match = <
	T extends SearchTable,
	const F extends string,
	const Q extends Terms,
	const R extends number | undefined = undefined,
>(
	table: T,
	field: FulltextField<T, F>,
	terms: Q,
	options?: {
		readonly ref?: CheckedRef<R>;
		readonly operator?: 'AND' | 'OR';
	},
): Match<R, FieldCodec<T, F>, HighlightsOf<T, F>, OperandParams<Q>> => {
	const path = field as string;
	const [index] = indexesOn(table, path, ['fulltext']);
	if (index === undefined) {
		throw new TypeError(
			`\`${table.name}\` has no FULLTEXT index on \`${path}\``,
		);
	}
	const target = fieldExpr(table, path);
	const ref = options?.ref as number | undefined;
	if (ref !== undefined) assertRef(ref);
	const operator = options?.operator;
	if (operator !== undefined && operator !== 'AND' && operator !== 'OR') {
		throw new RangeError(
			`a match operator is \`AND\` or \`OR\`, not ${String(operator)}`,
		);
	}
	const query: Expr = isExpr(terms) ? terms : literal(terms as string);
	const between = [ref, operator].filter((part) => part !== undefined);
	const sql = `${sqlOf(target)} @${between.join(',')}@ ${operand(query)}`;
	const params = withRefs(
		mergeParams(paramsOf(query)),
		ref === undefined
			? undefined
			: { matches: new Map([[ref, sqlOf(target)]]) },
	);
	return {
		...derive(
			Schema.Boolean,
			sql,
			Level.comparison,
			[target, query],
			params,
		),
		[MatchTypeId]: {
			ref,
			field: codecOf(target),
			highlights: index.fulltext?.highlights === true,
			sql,
		},
	} as unknown as Match<
		R,
		FieldCodec<T, F>,
		HighlightsOf<T, F>,
		OperandParams<Q>
	>;
};

/** `M`, or a message when the match has no reference number. */
export type Referenced<M, What extends string> =
	M extends Match<infer R, Codec, boolean, Params>
		? [R] extends [number]
			? M
			: `this match has no reference number, so ${What} cannot name it: give it one, as in \`match(table, field, terms, { ref: 1 })\``
		: never;

const refOf = (target: unknown): number => {
	const data = (target as Partial<AnyMatch>)[MatchTypeId];
	if (data?.ref === undefined) {
		throw new TypeError(
			'this match has no reference number: give it one, as in `match(table, field, terms, { ref: 1 })`',
		);
	}
	return data.ref;
};

const reading = <S extends Codec>(codec: S, sql: string, ref: number) =>
	makeExpr(
		codec,
		sql,
		withRefs({} as NoParams, { reads: new Set([ref]) }),
		Level.atom,
	);

/**
 * `search::score(ref)`: the relevance of the row to the match (BM25). On
 * SurrealDB 4 every matching row scores above 0; SurrealDB 3 scores 0 for a
 * term in at least half the rows. A row the condition admits through
 * another branch (`OR`) scores 0.
 */
export const score = <M extends AnyMatch>(
	target: Referenced<M, 'search::score'>,
): Expr<typeof Float, NoParams> => {
	const ref = refOf(target);
	return reading(Float, `search::score(${ref})`, ref);
};

/** `M`, or a message when it has no reference or its index no highlights. */
export type Highlighted<M> =
	M extends Match<infer R, Codec, infer H, Params>
		? [R] extends [number]
			? [H] extends [true]
				? M
				: 'the FULLTEXT index of this match keeps no HIGHLIGHTS, so `search::highlight` would return the text unmarked: define the index with HIGHLIGHTS'
			: 'this match has no reference number, so search::highlight cannot name it: give it one, as in `match(table, field, terms, { ref: 1 })`'
		: never;

/**
 * `search::highlight(open, close, ref)`: the matched field with every
 * matched term between `open` and `close` (each element of an array field).
 * A row the condition admits through another branch is unmarked. The index
 * must keep highlights (`HIGHLIGHTS`).
 */
export const highlight = <
	M extends AnyMatch,
	const O extends Terms,
	const C extends Terms,
>(
	target: Highlighted<M>,
	open: O,
	close: C,
): Expr<
	M[typeof MatchTypeId]['field'],
	OperandParams<O> & OperandParams<C>
> => {
	const ref = refOf(target);
	const data = (target as unknown as AnyMatch)[MatchTypeId];
	if (!data.highlights) {
		throw new TypeError(
			'the FULLTEXT index of this match keeps no HIGHLIGHTS: define the index with HIGHLIGHTS',
		);
	}
	const marks = [open, close].map((mark) =>
		isExpr(mark) ? mark : literal(mark as string),
	);
	return derive(
		data.field,
		`search::highlight(${marks.map(operand).join(', ')}, ${ref})`,
		Level.atom,
		marks,
		withRefs(mergeParams(...marks.map(paramsOf)), {
			reads: new Set([ref]),
		}),
	) as Expr<
		M[typeof MatchTypeId]['field'],
		OperandParams<O> & OperandParams<C>
	>;
};

/** Where each matched term is: start and end offsets per field element. */
export const Offsets = Schema.UndefinedOr(
	Schema.Record(
		Schema.String,
		Schema.Array(Schema.Struct({ s: Int, e: Int })),
	),
);

/**
 * `search::offsets(ref)`: for each element of the matched field (`"0"` for
 * a string), the start (`s`) and end (`e`) of every matched term; NONE for
 * a row the condition admits through another branch.
 */
export const offsets = <M extends AnyMatch>(
	target: Referenced<M, 'search::offsets'>,
): Expr<typeof Offsets, NoParams> => {
	const ref = refOf(target);
	return reading(Offsets, `search::offsets(${ref})`, ref);
};

// ---- nearest neighbours

export const KnnTypeId = '~effect-surrealkit/Knn';

/** A nearest-neighbour search, `field <|k, ef|> $vector`: a condition. */
export interface Knn<P extends Params> extends Expr<Schema.Boolean, P> {
	readonly [KnnTypeId]: { readonly sql: string };
}

/**
 * The distances SurrealDB computes without an index (`<|k, DISTANCE|>`), each
 * the same on SurrealDB 3.3 and 4.0 (`test/search-engine.test.ts` checks them
 * against their formulas on both). `JACCARD` and `PEARSON` are not among
 * them: the two majors compute them differently and find other rows.
 */
export type Distance =
	| 'CHEBYSHEV'
	| 'COSINE'
	| 'COSINE_NORMALIZED'
	| 'EUCLIDEAN'
	| 'HAMMING'
	| 'INNER_PRODUCT'
	| 'MANHATTAN'
	| readonly ['MINKOWSKI', number];

const distances: ReadonlySet<string> = new Set([
	'CHEBYSHEV',
	'COSINE',
	'COSINE_NORMALIZED',
	'EUCLIDEAN',
	'HAMMING',
	'INNER_PRODUCT',
	'MANHATTAN',
]);

/** Distances SurrealDB parses but 3.3 and 4.0 compute differently. */
const differing: Readonly<Record<string, string>> = {
	JACCARD:
		'SurrealDB 3.3 answers a measure of the sets of values (0 for most vectors of floats) and 4.0 one minus the weighted Jaccard similarity',
	PEARSON:
		'SurrealDB 3.3 answers the Pearson correlation itself as the distance, so its nearest rows are the least correlated, and 4.0 answers one minus it',
};

const distanceText = (distance: Distance): string => {
	if (typeof distance === 'string' && distances.has(distance)) {
		return distance;
	}
	if (typeof distance === 'string' && Object.hasOwn(differing, distance)) {
		throw new RangeError(
			`${distance} is refused: ${differing[distance]}; the same search finds other rows on each`,
		);
	}
	if (Array.isArray(distance) && distance[0] === 'MINKOWSKI') {
		const order = distance[1];
		if (typeof order === 'number' && Number.isFinite(order) && order > 0) {
			return `MINKOWSKI ${order}`;
		}
	}
	throw new RangeError(
		`${JSON.stringify(distance)} is not a distance: use one of ${[...distances].join(', ')} or ['MINKOWSKI', p] with p > 0`,
	);
};

/** How the search runs: through the vector index, or by brute force. */
export type KnnOptions =
	| { readonly k: number; readonly ef: number }
	| { readonly k: number; readonly distance: Distance };

/**
 * The options of a search with neighbour count `K` and search effort `EF`
 * (none for a brute-force search), each count checked.
 */
export type CheckedKnnOptions<K, EF> = [EF] extends [undefined]
	? {
			readonly k: CheckedCount<K, 'the neighbour count'>;
			readonly distance: Distance;
		}
	: {
			readonly k: CheckedCount<K, 'the neighbour count'>;
			readonly ef: CheckedCount<EF, 'the search effort'>;
		};

type DimensionOf<T, F extends string> =
	IndexOn<T, F, 'hnsw' | 'diskann'> extends {
		readonly vector: { readonly dimension: infer D extends number };
	}
		? D
		: never;

/**
 * The field `F` for a search with options `O`: the indexed form (`ef`)
 * needs an `HNSW` or `DISKANN` index on it; the brute-force form
 * (`distance`) an array of numbers.
 */
export type VectorField<T extends PureTable, F extends string, EF> = [
	EF,
] extends [number]
	? [IndexOn<T, F, 'hnsw' | 'diskann'>] extends [never]
		? `\`${NameOf<T>}\` has no HNSW or DISKANN index on \`${F}\`: search without one by naming a distance, as in \`{ k, distance: 'COSINE' }\``
		: F
	: [FieldCodec<T, F>] extends [never]
		? `\`${NameOf<T>}\` has no field \`${F}\``
		: NonNullable<FieldCodec<T, F>['Encoded']> extends ReadonlyArray<
					number | bigint
				>
			? F
			: `\`${NameOf<T>}.${F}\` is not an array of numbers, so it has no nearest neighbours`;

/** A query vector bound by name, as an array of floats. */
export type VectorCodec = Schema.$Array<typeof Float>;

/**
 * The query vector: a binding name (its value is an array of floats), or a
 * binding of your own whose stored type is an array of numbers. A tuple
 * codec's length must be the index dimension.
 */
export type QueryVector = string | Expr<Codec, Params>;

/** `V`, or a message when it cannot be the query vector of `T.F`. */
export type CheckedVector<T, F extends string, V> = V extends string
	? V
	: V extends Expr<infer S, Params>
		? S['Encoded'] extends ReadonlyArray<number | bigint>
			? number extends S['Encoded']['length']
				? V
				: [DimensionOf<T, F>] extends [never]
					? V
					: S['Encoded']['length'] extends DimensionOf<T, F>
						? V
						: `the query vector has ${S['Encoded']['length']} elements and the vector index on \`${NameOf<T>}.${F}\` has dimension ${DimensionOf<T, F>}`
			: 'the query vector is a binding whose stored type is an array of numbers'
		: never;

type VectorParams<V> = V extends string
	? { readonly [N in V]: VectorCodec }
	: V extends Expr<Codec, infer P>
		? P
		: never;

const lengthChecked = new WeakMap<Codec, Map<string, Codec>>();

/**
 * `codec` with the length checked against every dimension in `dimensions`,
 * one codec per pair, so the same binding used twice is still one codec.
 */
const withDimension = (
	codec: Codec,
	dimensions: ReadonlyArray<number>,
): Codec => {
	if (dimensions.length === 0) return codec;
	const id = dimensions.join(',');
	const known = lengthChecked.get(codec) ?? new Map<string, Codec>();
	lengthChecked.set(codec, known);
	const existing = known.get(id);
	if (existing !== undefined) return existing;
	const [first, ...rest] = dimensions.map((dimension) =>
		Schema.isBetweenLength(dimension, dimension, {
			expected: `a vector of ${dimension} numbers (the index dimension)`,
		}),
	);
	if (first === undefined) return codec;
	const checked = (
		codec as unknown as Schema.Codec<ReadonlyArray<unknown>>
	).check(first, ...rest) as unknown as Codec;
	known.set(id, checked);
	return checked;
};

/**
 * `field <|k, ef|> $vector` through the field's `HNSW` or `DISKANN` index,
 * or `field <|k, DISTANCE|> $vector` by brute force: the `k` rows nearest to
 * the query vector. `k` and `ef` are positive integer literals and the
 * distance one of {@link Distance}, which SurrealDB parses only as
 * literals; the vector is a binding, its length checked against the index
 * dimension when it is encoded (and by the compiler for a tuple codec).
 * SurrealDB takes one search per `SELECT`, at the top level of its `WHERE`
 * (joined with `AND`): under `OR` or `NOT` it is refused when built.
 *
 * @example
 * const near = knn(DocTable, 'embedding', 'query', { k: 5, ef: 40 });
 * select(DocTable)
 *   .where(() => near)
 *   .project((row) => ({ id: row.id, distance: knnDistance(near) }));
 * // SELECT id AS id, vector::distance::knn() AS distance FROM ⟨doc⟩
 * //   WHERE embedding <|5,40|> $query
 */
export const knn = <
	T extends SearchTable,
	const F extends string,
	const V extends QueryVector,
	const K extends number,
	const EF extends number | undefined = undefined,
>(
	table: T,
	field: VectorField<T, F, EF>,
	vector: CheckedVector<T, F, V>,
	options: CheckedKnnOptions<K, EF>,
): Knn<VectorParams<V>> => {
	const path = field as string;
	const given = options as KnnOptions;
	const indexed = 'ef' in given;
	const indexes = indexesOn(table, path, ['hnsw', 'diskann']);
	if (indexed && indexes.length === 0) {
		throw new TypeError(
			`\`${table.name}\` has no HNSW or DISKANN index on \`${path}\`: search without one by naming a distance, as in \`{ k, distance: 'COSINE' }\``,
		);
	}
	const target = fieldExpr(table, path);
	const k = assertCount(given.k, 'the neighbour count');
	const how =
		'ef' in given
			? assertCount(given.ef, 'the search effort')
			: distanceText(given.distance);
	const dimensions = [
		...new Set(indexes.flatMap((index) => index.vector?.dimension ?? [])),
	];
	let bound: Expr;
	if (typeof vector === 'string') {
		bound = param(vector, withDimension(Schema.Array(Float), dimensions));
	} else {
		const own = vector as Expr;
		const names = Object.keys(paramsOf(own));
		const name = names[0];
		if (
			names.length !== 1 ||
			name === undefined ||
			sqlOf(own) !== `$${name}`
		) {
			throw new TypeError(
				'the query vector is a binding (`param(name, codec)`) or a binding name: SurrealDB compares it as a value, and its length is checked when it is encoded',
			);
		}
		const codec = withDimension(codecOf(own), dimensions);
		bound = makeExpr(codec, sqlOf(own), { [name]: codec }, Level.atom);
	}
	const sql = `${sqlOf(target)} <|${k},${how}|> ${operand(bound)}`;
	return {
		...derive(
			Schema.Boolean,
			sql,
			Level.comparison,
			[target, bound],
			withRefs(mergeParams(paramsOf(bound)), { knn: new Set([sql]) }),
		),
		[KnnTypeId]: { sql },
	} as unknown as Knn<VectorParams<V>>;
};

/**
 * `vector::distance::knn()`: the distance of the row from the query vector
 * of the statement's nearest-neighbour search, in its index's distance (or
 * the one named).
 */
export const knnDistance = (
	_search: Knn<Params>,
): Expr<typeof Float, NoParams> =>
	makeExpr(
		Float,
		'vector::distance::knn()',
		withRefs({} as NoParams, { distance: true }),
		Level.atom,
	);

// ---- fused rankings

/** A list of rows to fuse: a subquery whose rows have an `id`. */
export type RankedList = Expr<Codec, Params>;

type RowOfList<L> =
	CodecOf<L>['Type'] extends ReadonlyArray<infer R> ? R : never;

/** Each list, or a message when its rows have no `id`. */
export type CheckedLists<Ls> = {
	readonly [I in keyof Ls]: RowOfList<Ls[I]> extends { readonly id: unknown }
		? Ls[I]
		: 'a fused list is a list of rows with an `id`, such as `select(…).project((row) => ({ id: row.id, … })).toExpr()`';
};

type KeysOf<U> = U extends unknown ? keyof U : never;
type ValueOf<U, K extends PropertyKey> = U extends unknown
	? K extends keyof U
		? U[K]
		: never
	: never;

/**
 * A fused row: the `id` every list has, every other field of any list
 * (absent when the row was not in that list), and the fused score.
 */
export type FusedRow<Ls extends ReadonlyArray<unknown>, Score extends string> =
	RowOfList<Ls[number]> extends infer Rows
		? Simplify<
				{ readonly id: ValueOf<Rows, 'id'> } & {
					readonly [K in Exclude<
						KeysOf<Rows>,
						'id' | Score
					>]?: Exclude<ValueOf<Rows, K>, undefined>;
				} & { readonly [S in Score]: number }
			>
		: never;

/** The codec of a fused ranking. */
export type FusedCodec<
	Ls extends ReadonlyArray<unknown>,
	Score extends string,
> = Schema.Codec<ReadonlyArray<FusedRow<Ls, Score>>, unknown, never, never>;

type Lists = readonly [RankedList, RankedList, ...ReadonlyArray<RankedList>];

const rowAst = (list: Codec): SchemaAST.Objects => {
	let node: SchemaAST.AST = list.ast;
	if (SchemaAST.isArrays(node) && node.rest[0] !== undefined) {
		node = node.rest[0];
		while (SchemaAST.isDeclaration(node) && node.typeParameters[0]) {
			node = node.typeParameters[0];
		}
		if (SchemaAST.isObjects(node)) return node;
	}
	throw new TypeError(
		'a fused list is a list of rows, such as `select(…).project(…).toExpr()`',
	);
};

const union = (codecs: ReadonlyArray<Schema.Top>): Schema.Top => {
	const distinct = [...new Set(codecs)];
	return distinct.length === 1 && distinct[0] !== undefined
		? distinct[0]
		: Schema.Union(distinct);
};

/** The codec of the fused rows of `lists`, with the score field `score`. */
const fusedCodec = (lists: ReadonlyArray<Expr>, scoreField: string): Codec => {
	const fields = new Map<string, Array<Schema.Top>>();
	const asts = new Map<SchemaAST.AST, Schema.Top>();
	for (const list of lists) {
		const row = rowAst(codecOf(list));
		if (!row.propertySignatures.some((field) => field.name === 'id')) {
			throw new TypeError(
				`the fused list \`${sqlOf(list)}\` has rows without an \`id\`; SurrealDB leaves such rows out`,
			);
		}
		for (const { name, type } of row.propertySignatures) {
			if (typeof name !== 'string') continue;
			if (name === scoreField) {
				throw new RangeError(
					`a fused list has a field \`${scoreField}\`, which the fused score replaces; rename it`,
				);
			}
			const codec = asts.get(type) ?? (Schema.make(type) as Schema.Top);
			asts.set(type, codec);
			fields.set(name, [...(fields.get(name) ?? []), codec]);
		}
	}
	const struct: Record<string, Schema.Top> = {};
	for (const [name, codecs] of fields) {
		struct[name] =
			name === 'id' ? union(codecs) : Schema.optionalKey(union(codecs));
	}
	struct[scoreField] = Float;
	return Schema.Array(
		NoneAsAbsent(Schema.Struct(struct as Schema.Struct.Fields)),
	) as unknown as Codec;
};

const countText = (value: Count, what: string, minimum: number): Expr => {
	if (isExpr(value)) return value;
	if (!Number.isInteger(value) || value < minimum) {
		throw new RangeError(
			`${what} takes an integer of at least ${minimum}, not ${value}`,
		);
	}
	return literal(value);
};

const listsText = (lists: ReadonlyArray<Expr>): string => {
	if (lists.length < 2) {
		throw new RangeError('a fused ranking takes two or more lists');
	}
	return `[${lists.map(sqlOf).join(', ')}]`;
};

/**
 * `search::rrf(lists, limit, k)`: reciprocal rank fusion of two or more
 * ranked lists. Rows with the same `id` are merged (fields of every list
 * they appear in), scored `rrf_score` (the sum of `1 / (k + rank)`), and
 * the best `limit` returned, best first. `k` defaults to 60.
 *
 * @example
 * evaluate(rrf([byMeaning.toExpr(), byWords.toExpr()], { limit: 10 }));
 * // RETURN search::rrf([(SELECT …), (SELECT …)], 10, 60);
 */
export const rrf = <
	const Ls extends Lists,
	const L extends Count,
	const K extends Count = 60,
>(
	lists: CheckedLists<Ls>,
	options: { readonly limit: L; readonly k?: K },
): Expr<
	FusedCodec<Ls, 'rrf_score'>,
	ParamsOfAll<Ls[number]> & OperandParams<L> & OperandParams<K>
> => {
	const all = lists as unknown as ReadonlyArray<Expr>;
	const limit = countText(options.limit, 'the fused limit', 1);
	const k = countText(options.k ?? 60, 'the rank constant', 0);
	return derive(
		fusedCodec(all, 'rrf_score'),
		`search::rrf(${listsText(all)}, ${operand(limit)}, ${operand(k)})`,
		Level.atom,
		[...all, limit, k],
	) as never;
};

/** `W`, or a message when a tuple of weights does not match the lists. */
export type CheckedWeights<Ls extends ReadonlyArray<unknown>, W> =
	W extends ReadonlyArray<number>
		? number extends W['length']
			? W
			: W['length'] extends Ls['length']
				? W
				: `there are ${Ls['length']} lists and ${W['length']} weights; give one weight per list`
		: W;

/**
 * `search::linear(lists, weights, limit, normalisation)`: a weighted sum of
 * each list's scores (`distance`, else `ft_score`, else `score`, else the
 * rank), normalised per list (`minmax` or `zscore`), as `linear_score`.
 * Rows with the same `id` are merged, as for {@link rrf}.
 */
export const linear = <
	const Ls extends Lists,
	const W extends
		| ReadonlyArray<number>
		| Expr<
				Schema.Codec<unknown, ReadonlyArray<number>, never, never>,
				Params
		  >,
	const L extends Count,
>(
	lists: CheckedLists<Ls>,
	options: {
		readonly weights: W & CheckedWeights<Ls, W>;
		readonly limit: L;
		readonly normalisation: 'minmax' | 'zscore';
	},
): Expr<
	FusedCodec<Ls, 'linear_score'>,
	ParamsOfAll<Ls[number]> & OperandParams<W> & OperandParams<L>
> => {
	const all = lists as unknown as ReadonlyArray<Expr>;
	const given = options.weights as ReadonlyArray<number> | Expr;
	let weights: Expr;
	if (isExpr(given)) {
		weights = given;
	} else {
		if (given.length !== all.length) {
			throw new RangeError(
				`there are ${all.length} lists and ${given.length} weights; give one weight per list`,
			);
		}
		weights = literal([...given], Schema.Array(Schema.Finite));
	}
	if (
		options.normalisation !== 'minmax' &&
		options.normalisation !== 'zscore'
	) {
		throw new RangeError(
			`normalisation is \`minmax\` or \`zscore\`, not ${String(options.normalisation)}`,
		);
	}
	const limit = countText(options.limit, 'the fused limit', 1);
	return derive(
		fusedCodec(all, 'linear_score'),
		`search::linear(${listsText(all)}, ${operand(weights)}, ${operand(limit)}, ${sqlOf(literal(options.normalisation))})`,
		Level.atom,
		[...all, weights, limit],
	) as never;
};

/**
 * `RETURN expr`: a statement whose result is one expression, such as a
 * fused ranking. What it writes is unknown (its subqueries may call
 * functions that write) unless `options.writes` says.
 */
export const evaluate = <S extends Codec, P extends Params>(
	expr: Expr<S, P>,
	name = 'evaluate',
	options?: { readonly writes?: boolean },
): Statement<Simplify<P>, S> => {
	admit(
		{
			own: undefined,
			what: 'a bare `RETURN`',
			parents: 'refuse',
			refused:
				'SurrealDB 3.3 and 4.0 bind `$parent` differently under a bare `RETURN`, which has no enclosing select',
		},
		[expr],
	);
	return make({
		...(options?.writes !== undefined && { writes: options.writes }),
		name,
		sql: `RETURN ${sqlOf(expr)};`,
		params: paramsOf(expr) as Simplify<P>,
		result: codecOf(expr),
	});
};
