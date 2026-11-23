/**
 * `SELECT` from a generated table descriptor.
 *
 * The builder is immutable: every method returns a new builder, so a base
 * query can be shared. The result codec is derived from the descriptor's
 * select schema, or from the projection's own expressions.
 */
import { Schema, SchemaAST } from 'effect';
import { escapeIdent } from 'surrealdb';

import {
	admit,
	assertReader,
	type CodecOf,
	type Comparable,
	type Context,
	codecOf,
	contextOf,
	derive,
	type Expr,
	eq,
	type ItemOf,
	isExpr,
	isPath,
	joinContexts,
	Level,
	mergeParams,
	type Operand,
	type OperandEncoded,
	type OperandParams,
	type ParamsOf,
	type ParamsOfAll,
	type Path,
	type PathData,
	paramsOf,
	pathOf,
	RowScope,
	type Site,
	settle,
	sqlOf,
} from './expr.js';
import {
	type CheckedFetch,
	type FetchBase,
	type Fetched,
	fetchedCodec,
	fetchPath,
} from './fetch.js';
import {
	type GraphStore,
	type LinkTables,
	linkShape,
	shapeTables,
	type TableIn,
	TraversalTypeId,
	traversalOf,
	viewOf,
} from './graph.js';
import {
	type Base,
	type CheckedKeys,
	type GroupAll,
	type Grouped,
	groupAll,
	groupBy,
} from './group.js';
import {
	type IdRange,
	IdRangeTypeId,
	isBuiltRecordId,
	isIdRange,
} from './record.js';
import {
	type CheckedProjection,
	type FieldsOf,
	fieldsOf,
	type IsOptional,
	isOptional,
	type ProjectedCodec,
	type Projection,
	type RowCodec,
	type RowCodecIn,
	rowCodec,
	type Unwrapped,
	unwrap,
} from './row.js';
import {
	type Codec,
	make,
	type NoParams,
	type Params,
	type Simplify,
	type Statement,
} from './statement.js';
import {
	andAll,
	type Condition,
	type IdOf,
	type PureTable,
	project,
	type RowExprs,
	readerOf,
	rowExprs,
	type StoredRow,
	storedRow,
	subquery,
	tableName,
} from './table.js';

/** A row count: a constant, or an expression yielding a number. */
export type Count =
	| number
	| Expr<Schema.Codec<unknown, number, never, never>, Params>;

/**
 * A sort key as its callback returned it, `K`: a field path, or else a
 * message saying that only a field path can be one.
 */
export type OrderKey<K> = K extends Path
	? K
	: 'ORDER BY takes a field path of the row, like `row.name` or `at(row.pins, 0)`';

/** The names a row of `Row` holds, which ORDER BY can name as aliases. */
export type OrderAlias<Row extends Codec> = Row extends SelectValueRow
	? never
	: Row['Type'] extends ReadonlyArray<unknown> | RecordIdLike
		? never
		: Row['Type'] extends object
			? keyof Row['Type'] & string
			: never;

type RecordIdLike = { readonly table: unknown; readonly id: unknown };

/**
 * Marks, for the compiler only, the row codec of a `SELECT VALUE`: its value
 * may be an object, but it has no names `orderByAlias` could sort by.
 */
export interface SelectValueRow {
	readonly '~effect-surrealkit/SelectValue': true;
}

/** The indexes a descriptor lists, by name. */
type IndexesOf<T> = T extends { readonly indexes: infer I }
	? I
	: Record<never, never>;

/** A column path as a lookup key: its segments joined by `.`. */
type ColumnKey<C> = C extends readonly [infer H extends string]
	? H
	: C extends readonly [
				infer H extends string,
				...infer R extends ReadonlyArray<string>,
			]
		? `${H}.${ColumnKey<R>}`
		: never;

/** Each column of an index, by its lookup key. */
type ColumnsOf<I> = I extends {
	readonly columns: infer C extends ReadonlyArray<ReadonlyArray<string>>;
}
	? { readonly [P in C[number] as ColumnKey<P>]: P }
	: never;

/** The expression at a column path of the row. */
type ExprAt<R, P> = P extends readonly [
	infer H extends string,
	...infer Rest extends ReadonlyArray<string>,
]
	? H extends keyof R
		? Rest extends readonly []
			? R[H]
			: ExprAt<R[H], Rest>
		: never
	: never;

/**
 * The index fields whose path the row of `T` does not expose: a segment
 * under a field that is not one object (a union of objects, a nullable
 * object), which the run-time lookup cannot resolve either.
 */
type Unaddressable<T extends PureTable, C> = {
	[K in keyof C]-?: [ExprAt<RowExprs<T>, C[K]>] extends [never] ? K : never;
}[keyof C] &
	string;

/**
 * `N`, when it names a unique index of `T` over field paths; otherwise a
 * message saying why it cannot be looked up by.
 */
export type UniqueIndex<T extends PureTable, N> = N extends keyof IndexesOf<T>
	? IndexesOf<T>[N] extends { readonly type: 'unique' }
		? [
				Extract<
					keyof ColumnsOf<IndexesOf<T>[N]>,
					`${string}*${string}`
				>,
			] extends [never]
			? [Unaddressable<T, ColumnsOf<IndexesOf<T>[N]>>] extends [never]
				? N
				: `\`${Unaddressable<T, ColumnsOf<IndexesOf<T>[N]>>}\` of the unique index \`${N & string}\` is no field path of \`${T['name']}\` a lookup can read: a field on it is not one object (a union or a nullable object has no fields to address)`
			: `\`${N & string}\` indexes array elements, which a unique lookup cannot address`
		: `\`${N & string}\` is a ${IndexesOf<T>[N] extends { readonly type: infer K extends string } ? K : 'non-unique'} index of \`${T['name']}\`, not a unique one`
	: `\`${T['name']}\` has no index \`${N & string}\``;

/** The index fields whose value never compares with what `V` gives. */
type Mismatched<T extends PureTable, C, V> = {
	[K in keyof C & keyof V]-?: Comparable<
		CodecOf<ExprAt<RowExprs<T>, C[K]>>['Encoded'],
		OperandEncoded<V[K]>,
		true
	> extends true
		? never
		: K;
}[keyof C & keyof V] &
	string;

/**
 * Whether a value of stored type `E` may be an array (`true`), may be
 * another kind (`false`), or either (`boolean`); NONE and NULL count as
 * neither. A set is not an array.
 */
type MayBeArray<E> = unknown extends E
	? boolean
	: Exclude<E, undefined | null> extends infer X
		? [X] extends [never]
			? never
			: [X] extends [ReadonlyArray<unknown>]
				? true
				: [Extract<X, ReadonlyArray<unknown>>] extends [never]
					? false
					: boolean
		: never;

/**
 * The index fields that may hold an array or another kind and are looked up
 * by a value that may be an array: SurrealDB 3.3 finds no array through a
 * unique index on such a field, and 4.0 does.
 */
type AmbiguousArray<T extends PureTable, C, V> = {
	[K in keyof C & keyof V]-?: boolean extends MayBeArray<
		CodecOf<ExprAt<RowExprs<T>, C[K]>>['Encoded']
	>
		? true extends MayBeArray<OperandEncoded<V[K]>>
			? K
			: never
		: never;
}[keyof C & keyof V] &
	string;

/**
 * The values `V` of a lookup by the unique index `N`, when they are exactly
 * one operand per field of the index (its path joined by `.`, like
 * `profile.bio`) and each compares with its field; otherwise a message
 * naming the first field that does not fit. An unknown index is reported
 * on the index itself.
 */
export type UniqueValues<
	T extends PureTable,
	N,
	V,
> = N extends keyof IndexesOf<T>
	? [Exclude<keyof ColumnsOf<IndexesOf<T>[N]>, keyof V>] extends [never]
		? [Exclude<keyof V, keyof ColumnsOf<IndexesOf<T>[N]>>] extends [never]
			? [Mismatched<T, ColumnsOf<IndexesOf<T>[N]>, V>] extends [never]
				? [AmbiguousArray<T, ColumnsOf<IndexesOf<T>[N]>, V>] extends [
						never,
					]
					? V
					: `\`${AmbiguousArray<T, ColumnsOf<IndexesOf<T>[N]>, V>}\` may hold an array or another kind, and SurrealDB 3.3 finds no array through its unique index (4.0 does): look it up by a value that is not an array, or declare it an array kind only`
				: `\`${Mismatched<T, ColumnsOf<IndexesOf<T>[N]>, V>}\` never compares with this value: their stored types do not overlap`
			: `\`${Exclude<keyof V, keyof ColumnsOf<IndexesOf<T>[N]>> & string}\` is not a field of the unique index \`${N & string}\``
		: `the unique index \`${N & string}\` also needs \`${Exclude<keyof ColumnsOf<IndexesOf<T>[N]>, keyof V> & string}\``
	: V;

/** The fields of the stored row of `T`. */
type SelectFields<T extends PureTable> = FieldsOf<T['select']>;

type AsFields<F> = F extends Schema.Struct.Fields ? F : never;

/** A row schema like `S` (closed or open) over the fields `F`. */
type RebuiltRow<S, F> =
	S extends Schema.StructWithRest<
		Schema.Struct<Schema.Struct.Fields>,
		infer R
	>
		? Schema.StructWithRest<Schema.Struct<AsFields<F>>, R>
		: Schema.Struct<AsFields<F>>;

/** Field names as the caller wrote them, `K`, each a field of `T`, or a message. */
export type CheckedOmit<
	T extends PureTable,
	K extends ReadonlyArray<string>,
> = {
	readonly [I in keyof K]: K[I] extends keyof SelectFields<T>
		? K[I]
		: `\`${K[I] & string}\` is not a field of \`${T['name']}\``;
};

/** The row schema `S` without the top-level fields `K`. */
export type OmittedRow<
	S extends RowCodecIn,
	K extends PropertyKey,
> = RebuiltRow<S, Omit<FieldsOf<S>, K>>;

/** The array fields of the stored row of `T`: what SPLIT takes. */
export type ArrayFields<T extends PureTable> = {
	[K in keyof SelectFields<T>]: [
		ItemOf<Unwrapped<SelectFields<T>[K]> & Codec>,
	] extends [never]
		? never
		: K;
}[keyof SelectFields<T>] &
	string;

/** `K` when it is an array field of `T`; otherwise a message. */
export type SplitField<T extends PureTable, K> =
	K extends ArrayFields<T>
		? K
		: `\`${K & string}\` is not an array field of \`${T['name']}\`: SPLIT takes one`;

/**
 * A split field: one element per row, or the array itself when it is empty
 * (SurrealDB 3.3 and 4.0 keep a record whose array is empty, unsplit).
 */
export type SplitCodec<F> = Schema.Union<
	readonly [ItemOf<Unwrapped<F> & Codec>, Unwrapped<F> & Codec]
>;

/** The row schema `S` with the array field `K` split. */
export type SplitRow<S extends RowCodecIn, K extends string> = RebuiltRow<
	S,
	{
		readonly [F in keyof FieldsOf<S>]: F extends K
			? IsOptional<FieldsOf<S>[F]> extends true
				? Schema.optionalKey<SplitCodec<FieldsOf<S>[F]>>
				: SplitCodec<FieldsOf<S>[F]>
			: FieldsOf<S>[F];
	}
>;

/** `T` read with its array field `K` split. */
export type SplitTable<T extends PureTable, K extends string> = AsTable<
	Omit<T, 'select'> & { readonly select: SplitRow<T['select'], K> }
>;

/** `X` as a table descriptor, which it is once `T` is known. */
type AsTable<X> = X extends PureTable ? X : never;

/** A projection after SPLIT must hold the split field under its own name. */
type KeepsSplit<F, K extends string> = K extends keyof F
	? unknown
	: {
			readonly [P in K]: `the SPLIT field \`${K}\` must be projected under its own name`;
		};

/**
 * A `SELECT … SPLIT field` under construction: one row per element of the
 * array field `K`. Conditions come before SPLIT, so there is no `where`.
 */
export interface Split<
	T extends PureTable,
	P extends Params,
	Row extends Codec,
	K extends string,
> {
	/** `OMIT`: top-level fields `SELECT *` leaves out. */
	readonly omit: <const O extends ReadonlyArray<string>>(
		...fields: CheckedOmit<T, O>
	) => Split<T, P, RowCodec<OmittedRow<T['select'], O[number]>>, K>;
	/** Adds a sort key, a field path of the split row. */
	readonly orderBy: <O>(
		key: (row: RowExprs<T>) => OrderKey<O>,
		direction?: 'ASC' | 'DESC',
	) => Split<T, P, Row, K>;
	/** `LIMIT`: at most this many rows. */
	readonly limit: <const L extends Count>(
		rows: L,
	) => Split<T, P & OperandParams<L>, Row, K>;
	/** `START`: skip this many rows. */
	readonly start: <const L extends Count>(
		rows: L,
	) => Split<T, P & OperandParams<L>, Row, K>;
	/** Selects named expressions; the split field among them, under its name. */
	readonly project: <const F>(
		fields: (row: RowExprs<T>) => CheckedProjection<F> & KeepsSplit<F, K>,
	) => Split<T, P & ParamsOfAll<F[keyof F]>, ProjectedCodec<F>, K>;
	/** The finished statement. */
	readonly build: (
		name?: string,
		options?: { readonly writes?: boolean },
	) => Statement<Simplify<P>, Schema.$Array<Row>, never, never>;
}

/** What a `SELECT` returns: every row, or with `ONLY` one row or NONE. */
export type SelectResult<
	Row extends Codec,
	Only extends boolean,
> = Only extends true ? Schema.UndefinedOr<Row> : Schema.$Array<Row>;

interface State {
	readonly table: PureTable;
	/** The row the select's callbacks read. */
	readonly scope: RowScope;
	readonly row: RowExprs<PureTable>;
	readonly source: Expr | undefined;
	/** The source is an id range, which reads many rows as a table does. */
	readonly range?: boolean;
	readonly out: Codec;
	readonly fields: string;
	readonly where: ReadonlyArray<Expr>;
	readonly order: ReadonlyArray<Order>;
	/** The projection's aliases and the text each stands for. */
	readonly aliases: ReadonlyMap<string, string>;
	/**
	 * Only stored fields and constants are read, so the statement certainly
	 * does not write; an expression can call a function that writes.
	 */
	readonly pure: boolean;
	/** `OMIT`: top-level fields `SELECT *` leaves out. */
	readonly omit?: ReadonlyArray<string>;
	/** `SPLIT`: the array field each element of which is a row. */
	readonly split?: string;
	readonly limit: string | undefined;
	readonly start: string | undefined;
	readonly only: boolean;
	/** The index a lookup by a unique index names in `WITH INDEX`. */
	readonly index?: string;
	/** `FOR UPDATE`: the record read conflicts with a concurrent write. */
	readonly lock?: boolean;
	readonly params: Params;
	/** What the select returns, which FETCH paths name fields of. */
	readonly base: FetchBase;
	readonly fetch: ReadonlyArray<string>;
	/**
	 * What its expressions read besides their bindings and its own row:
	 * what a subquery reads of the rows around it.
	 */
	readonly context?: Context;
}

/**
 * Refuses a select that reads its table as one reader and records through a
 * store view of the other; returns what it reads of the rows around it,
 * which a subquery carries out without its readers (it checked them).
 */
const ownReader = (state: State, what: string): Context | undefined => {
	assertReader(readerOf(state.table), state.context?.readers, what);
	if (state.context?.readers === undefined) return state.context;
	const { readers: _, ...rest } = state.context;
	return Object.keys(rest).length === 0 ? undefined : rest;
};

/** Where a select's callbacks place their results. */
const siteOf = (state: State): Site => ({
	own: state.scope,
	what: `a select of \`${state.table.name}\``,
	parents: 'select',
});

/** `state`, reading what `exprs` read too; `callback` when a callback returned them. */
const reading = (
	state: State,
	exprs: ReadonlyArray<Expr>,
	callback = true,
): State => {
	const context = joinContexts([
		state.context,
		callback
			? admit(siteOf(state), exprs)
			: joinContexts(exprs.map(contextOf)),
	]);
	return {
		...state,
		...(context !== undefined && { context }),
	};
};

interface Order {
	readonly sql: string;
	readonly direction: 'ASC' | 'DESC';
	/** The field path sorted by, or none for an alias of the projection. */
	readonly path?: PathData;
	/** The name of the result row sorted by (`orderByAlias`). */
	readonly alias?: string;
}

const valueHasNoNames =
	'SELECT VALUE returns one value per row, with no names to order by; order by a field path of the stored row with `orderBy`';

/** Why the rows `state` returns hold no field `name`, or `undefined`. */
const missingName = (state: State, name: string): string | undefined => {
	const missing = `\`${name}\` is not a name the result row holds`;
	if (state.base.kind === 'value') return `${missing}: ${valueHasNoNames}`;
	const holds =
		state.fields === '*'
			? Object.hasOwn(fieldsOf(state.table.select) ?? {}, name) &&
				!(state.omit ?? []).includes(name)
			: state.aliases.has(name);
	return holds ? undefined : missing;
};

/**
 * SurrealDB resolves an `ORDER BY` path against the projection before the
 * stored row, so an alias named like the field the path starts from, over
 * anything but that field, would change what is sorted.
 */
const assertOrder = (state: State): void => {
	for (const { alias, path, sql } of state.order) {
		if (alias !== undefined) {
			// A projection, SELECT VALUE or OMIT after `orderByAlias` changes
			// the names the row holds: the sort key must still be one of them.
			const missing = missingName(state, alias);
			if (missing !== undefined) {
				throw new RangeError(`ORDER BY \`${alias}\`: ${missing}`);
			}
		}
		if (path === undefined) continue;
		const source = state.aliases.get(path.root);
		if (source !== undefined && source !== path.rootSql) {
			throw new RangeError(
				`the projection alias \`${path.root}\` stands for \`${source}\` and shadows the stored field \`${path.root}\` that ORDER BY ${sql} reads, so SurrealDB would order by the alias; rename the alias`,
			);
		}
	}
};

const count = (value: Count, clause: string): string => {
	if (isExpr(value)) return sqlOf(value);
	if (!Number.isInteger(value) || value < 0) {
		throw new RangeError(
			`${clause} takes a non-negative integer, not ${value}`,
		);
	}
	return String(value);
};

const render = (state: State): string => {
	assertOrder(state);
	const limit = state.only && readsMany(state) ? '1' : state.limit;
	return [
		`SELECT ${state.fields}${
			state.omit === undefined || state.omit.length === 0
				? ''
				: ` OMIT ${state.omit.map((field) => escapeIdent(field)).join(', ')}`
		} FROM ${state.only ? 'ONLY ' : ''}${
			state.source === undefined
				? tableName(state.table)
				: sqlOf(state.source)
		}`,
		...(state.index === undefined
			? []
			: [`WITH INDEX ${escapeIdent(state.index)}`]),
		...(state.where.length > 0 ? [`WHERE ${andAll(state.where)}`] : []),
		...(state.split === undefined
			? []
			: [`SPLIT ${splitTarget(state, state.split)}`]),
		...(state.order.length > 0
			? [
					`ORDER BY ${state.order
						.map(({ direction, sql }) => `${sql} ${direction}`)
						.join(', ')}`,
				]
			: []),
		...(limit === undefined ? [] : [`LIMIT ${limit}`]),
		...(state.start === undefined ? [] : [`START ${state.start}`]),
		...(state.fetch.length > 0
			? [`FETCH ${state.fetch.map(fetchPath).join(', ')}`]
			: []),
		...(state.lock === true ? ['FOR UPDATE'] : []),
	].join(' ');
};

/** FETCH paths name fields of the projection, so they come after it. */
const assertNoFetch = (state: State, method: string): void => {
	if (state.fetch.length > 0) {
		throw new RangeError(
			`FETCH ${state.fetch.join(', ')} names fields of what the select returns; call \`${method}\` before \`fetch\``,
		);
	}
};

/**
 * The `SPLIT` target: the field, which a projection must hold under its own
 * name. SurrealDB 4 resolves the target as a projection alias and 3.3 as the
 * stored field, and both refuse a target the projection leaves out, so only
 * the field under its own name means the same on both.
 */
const splitTarget = (state: State, field: string): string => {
	const sql = escapeIdent(field);
	if (state.fields !== '*' && state.aliases.get(field) !== sql) {
		throw new RangeError(
			`the SPLIT field \`${field}\` must be projected under its own name (\`${field}: row.${field}\`)`,
		);
	}
	return sql;
};

/** What a grouped SELECT keeps of a select: its subject and conditions. */
const baseOf = (state: State): Base => {
	if (
		state.split !== undefined ||
		state.omit !== undefined ||
		state.fetch.length > 0 ||
		state.fields !== '*' ||
		state.only ||
		state.order.length > 0 ||
		state.limit !== undefined ||
		state.start !== undefined
	) {
		throw new RangeError(
			'group a SELECT before projecting, ordering, limiting, fetching it or reading ONLY one row',
		);
	}
	return {
		table: state.table,
		scope: state.scope,
		...(state.context !== undefined && { context: state.context }),
		row: state.row,
		from:
			state.source === undefined
				? tableName(state.table)
				: sqlOf(state.source),
		where: state.where,
		params: state.params,
		pure: state.pure,
	};
};

/** A row schema like `row` (closed or open) over other fields. */
const rebuild = (
	row: RowCodecIn,
	fields: { readonly [field: string]: Codec },
): RowCodecIn =>
	'records' in row && 'schema' in row
		? (Schema.StructWithRest(
				Schema.Struct(fields),
				row.records as Schema.StructWithRest.Records,
			) as unknown as RowCodecIn)
		: (Schema.Struct(fields) as unknown as RowCodecIn);

/** A table or an id range: what `FROM ONLY` reads one row of with `LIMIT 1`. */
const readsMany = (state: State): boolean =>
	state.source === undefined || state.range === true;

const resultOf = (state: State): Codec =>
	state.only ? Schema.UndefinedOr(state.out) : Schema.Array(state.out);

/**
 * A `SELECT` under construction. `T` is the table, `P` the bindings read so
 * far, `Row` the codec of one result row and `Only` whether it reads a
 * single record.
 */
export interface Select<
	T extends PureTable,
	P extends Params,
	Row extends Codec,
	Only extends boolean,
> {
	/** Adds a condition. Several `where` calls must all hold. */
	readonly where: <P2 extends Params>(
		predicate: (row: RowExprs<T>) => Condition<P2>,
	) => Select<T, P & P2, Row, Only>;

	/**
	 * Adds a sort key, after the ones already given: a field path of the
	 * stored row (`row.name`, `row.address.city`, `at(row.pins, 0)`), the
	 * only thing SurrealDB parses there. A projection alias named like the
	 * field the path starts from must be that field (`.project((row) =>
	 * ({ age: row.age }))`), or the statement is refused when it is built.
	 */
	readonly orderBy: <K>(
		key: (row: RowExprs<T>) => OrderKey<K>,
		direction?: 'ASC' | 'DESC',
	) => Select<T, P, Row, Only>;

	/**
	 * Adds a sort key that is a name the result row holds: an alias of the
	 * projection, such as a full-text `score(…)` or a nearest-neighbour
	 * `knnDistance(…)` (`.orderByAlias('score', 'DESC')`), or a field of
	 * `SELECT *`. SurrealDB 3.3 parses no function call after ORDER BY, so
	 * a computed value is sorted by its alias. The name is checked again
	 * when the statement is built, against what it then returns: a later
	 * `project`, `value` or `omit` that drops it is refused, because
	 * SurrealDB would sort by a stored field or by NONE instead.
	 */
	readonly orderByAlias: <const A extends string>(
		alias: A extends OrderAlias<Row>
			? A
			: `\`${A}\` is not a name the result row holds`,
		direction?: 'ASC' | 'DESC',
	) => Select<T, P, Row, Only>;

	/** `LIMIT`: at most this many rows. */
	readonly limit: <const L extends Count>(
		rows: L,
	) => Select<T, P & OperandParams<L>, Row, Only>;

	/** `START`: skip this many rows. */
	readonly start: <const L extends Count>(
		rows: L,
	) => Select<T, P & OperandParams<L>, Row, Only>;

	/**
	 * Selects named expressions instead of the stored row. Each key of the
	 * returned object is a field of the result, decoded by its expression's
	 * codec.
	 */
	readonly project: <const F>(
		fields: (row: RowExprs<T>) => CheckedProjection<F>,
	) => Select<T, P & ParamsOfAll<F[keyof F]>, ProjectedCodec<F>, Only>;

	/**
	 * `SELECT VALUE`: one expression per row instead of an object. Its rows
	 * hold no names, so `orderByAlias` after it is a compile error.
	 */
	readonly value: <S extends Codec, P2 extends Params>(
		pick: (row: RowExprs<T>) => Expr<S, P2>,
	) => Select<T, P & P2, RowCodec<S> & SelectValueRow, Only>;

	/**
	 * `FROM ONLY`: one row, or NONE when there is none. Reading from a table
	 * adds `LIMIT 1`, because the engine fails `ONLY` on a second row.
	 */
	readonly only: () => Select<T, P, Row, true>;

	/**
	 * `OMIT`: top-level fields `SELECT *` leaves out of each row (conditions
	 * and sort keys still read them).
	 */
	readonly omit: <const K extends ReadonlyArray<string>>(
		...fields: CheckedOmit<T, K>
	) => Select<T, P, RowCodec<OmittedRow<T['select'], K[number]>>, Only>;

	/**
	 * `SPLIT`: one row per element of an array field, which holds that
	 * element in the row (the array itself when it is empty). Add conditions
	 * before it.
	 */
	readonly split: <const K extends string>(
		field: SplitField<T, K>,
	) => Split<SplitTable<T, K>, P, StoredRow<SplitTable<T, K>>, K>;

	/**
	 * `GROUP BY`: one row per distinct combination of the keys, named field
	 * paths of the row (`{ role: row.role, city: row.address.city }`), which
	 * every row of the result holds. Add aggregates with `.aggregate(…)`.
	 */
	readonly groupBy: <const K>(
		keys: (row: RowExprs<T>) => CheckedKeys<K>,
	) => Grouped<T, P, K>;

	/** `GROUP ALL`: one row of aggregates over every record. */
	readonly groupAll: () => GroupAll<T, P>;

	/**
	 * `FETCH`: replaces each record link `paths` names with the record it
	 * points at, found in `store` by its table. A path names a field of
	 * what the select returns (the stored row, or the projection's
	 * aliases), dot-separated through objects and fetched records
	 * (`manager.team`). A fetched link may be absent (SurrealDB answers
	 * NONE for a missing or unreadable record), and so may each record of
	 * a fetched list. Call it after `project`.
	 */
	readonly fetch: <
		S extends GraphStore,
		const Paths extends readonly [string, ...ReadonlyArray<string>],
	>(
		store: S,
		...paths: CheckedFetch<Row['Type'], S, Paths>
	) => FetchedSelect<T, P, Fetched<Row, S, Paths>, Only>;

	/** This query as a parenthesised subquery of another expression. */
	readonly toExpr: () => Expr<SelectResult<Row, Only>, P>;

	/**
	 * The finished statement. It is known not to write (`writes: false`)
	 * when it reads only stored fields and constants; with a condition, a
	 * projected expression or a bound count it may call a function that
	 * writes, so what it writes is unknown unless `options.writes` says.
	 */
	readonly build: (
		name?: string,
		options?: { readonly writes?: boolean },
	) => Statement<Simplify<P>, SelectResult<Row, Only>, never, never>;
}

/** Why a method that changes what a select returns cannot follow `fetch`. */
export type AfterFetch<M extends string> =
	`call \`${M}\` before \`fetch\`: FETCH names fields of what the select returns`;

/**
 * A `SELECT … FETCH` under construction. FETCH paths name fields of what the
 * select returns, so what changes that (a projection, `SELECT VALUE`, OMIT,
 * SPLIT, grouping) comes before `fetch`: here each is a compile error whose
 * message says so.
 */
export interface FetchedSelect<
	T extends PureTable,
	P extends Params,
	Row extends Codec,
	Only extends boolean,
> {
	/** Adds a condition. Several `where` calls must all hold. */
	readonly where: <P2 extends Params>(
		predicate: (row: RowExprs<T>) => Condition<P2>,
	) => FetchedSelect<T, P & P2, Row, Only>;
	/** Adds a sort key, a field path of the stored row. */
	readonly orderBy: <K>(
		key: (row: RowExprs<T>) => OrderKey<K>,
		direction?: 'ASC' | 'DESC',
	) => FetchedSelect<T, P, Row, Only>;
	/** Adds a sort key that is a name the result row holds. */
	readonly orderByAlias: <const A extends string>(
		alias: A extends OrderAlias<Row>
			? A
			: `\`${A}\` is not a name the result row holds`,
		direction?: 'ASC' | 'DESC',
	) => FetchedSelect<T, P, Row, Only>;
	/** `LIMIT`: at most this many rows. */
	readonly limit: <const L extends Count>(
		rows: L,
	) => FetchedSelect<T, P & OperandParams<L>, Row, Only>;
	/** `START`: skip this many rows. */
	readonly start: <const L extends Count>(
		rows: L,
	) => FetchedSelect<T, P & OperandParams<L>, Row, Only>;
	/** `FROM ONLY`: one row, or NONE when there is none. */
	readonly only: () => FetchedSelect<T, P, Row, true>;
	/** `FETCH` more paths. */
	readonly fetch: <
		S extends GraphStore,
		const Paths extends readonly [string, ...ReadonlyArray<string>],
	>(
		store: S,
		...paths: CheckedFetch<Row['Type'], S, Paths>
	) => FetchedSelect<T, P, Fetched<Row, S, Paths>, Only>;
	/** Refused: a projection comes before `fetch`. */
	readonly project: (fields: AfterFetch<'project'>) => never;
	/** Refused: `SELECT VALUE` takes no FETCH. */
	readonly value: (pick: AfterFetch<'value'>) => never;
	/** Refused: OMIT comes before `fetch`. */
	readonly omit: (field: AfterFetch<'omit'>) => never;
	/** Refused: SPLIT comes before `fetch`. */
	readonly split: (field: AfterFetch<'split'>) => never;
	/** Refused: grouping comes before `fetch`. */
	readonly groupBy: (keys: AfterFetch<'groupBy'>) => never;
	/** Refused: grouping comes before `fetch`. */
	readonly groupAll: (this: AfterFetch<'groupAll'>) => never;
	/** This query as a parenthesised subquery of another expression. */
	readonly toExpr: () => Expr<SelectResult<Row, Only>, P>;
	/** The finished statement, as {@link Select.build}. */
	readonly build: (
		name?: string,
		options?: { readonly writes?: boolean },
	) => Statement<Simplify<P>, SelectResult<Row, Only>, never, never>;
}

/**
 * A `SELECT` of one record by id (or of an id range), before any other
 * method is called: it can also read the record `FOR UPDATE`.
 */
export interface RecordSelect<T extends PureTable, P extends Params>
	extends Select<T, P, StoredRow<T>, false> {
	/**
	 * `FOR UPDATE`: a transaction that reads the record this way fails with
	 * a conflict (`StatementConflict`) when a concurrent write changes the
	 * record before it commits; no lock is held (SurrealDB 3.3 and 4.0). The
	 * id must be a binding or a block's local (`$name`), a literal record id
	 * or a {@link recordId} expression (`type::record(…)`): SurrealDB refuses
	 * a table, a field path, a subquery or any other expression as the
	 * target, so those and an {@link idRange} are refused here.
	 */
	readonly forUpdate: () => Select<T, P, StoredRow<T>, false>;
}

const parameter = /^\$[A-Za-z_][A-Za-z0-9_]*$/u;

/** A record id literal, as {@link literal} renders one (`r"invite:a"`). */
const recordLiteral = /^r"(?:[^"\\]|\\.)*"$/u;

/**
 * Whether SurrealDB 3.3 and 4.0 take `id` as a `FOR UPDATE` target: a
 * binding or a local, a literal record id, or a {@link recordId} call. They
 * refuse a field path, a subquery, a table and a range.
 */
const lockable = (id: Expr): boolean =>
	parameter.test(sqlOf(id)) ||
	recordLiteral.test(sqlOf(id)) ||
	isBuiltRecordId(id);

const builder = <
	T extends PureTable,
	P extends Params,
	Row extends Codec,
	Only extends boolean,
>(
	state: State,
): Select<T, P, Row, Only> => {
	const row = state.row as RowExprs<T>;
	const bounded = (rows: Count): Params =>
		isExpr(rows) ? mergeParams(state.params, paramsOf(rows)) : state.params;
	return {
		where: (predicate) => {
			if (state.split !== undefined) {
				throw new RangeError(
					'WHERE reads the stored row, before SPLIT; add conditions before `.split(…)`',
				);
			}
			const condition = predicate(row);
			return builder({
				...reading(state, [condition]),
				where: [...state.where, condition],
				pure: false,
				params: mergeParams(state.params, paramsOf(condition)),
			});
		},
		orderBy: (key, direction = 'ASC') => {
			const by: unknown = key(row);
			const path = isPath(by) ? pathOf(by) : undefined;
			if (path === undefined) {
				throw new TypeError(
					'ORDER BY takes a field path of the row (`row.name`, `row.address.city`, `at(row.pins, 0)`); SurrealDB parses nothing else there',
				);
			}
			return builder({
				...reading(state, [by as Path]),
				order: [
					...state.order,
					{ sql: sqlOf(by as Path), direction, path },
				],
			});
		},
		orderByAlias: (alias, direction = 'ASC') => {
			const name = alias as string;
			const missing = missingName(state, name);
			if (missing !== undefined) throw new RangeError(missing);
			return builder({
				...state,
				order: [
					...state.order,
					{ sql: escapeIdent(name), direction, alias: name },
				],
			});
		},
		limit: (rows) => {
			if (state.only && readsMany(state)) {
				throw new RangeError(
					'a SELECT … FROM ONLY a table already reads at most one row',
				);
			}
			return builder({
				...reading(state, isExpr(rows) ? [rows] : [], false),
				limit: count(rows, 'LIMIT'),
				params: bounded(rows),
				pure: state.pure && !isExpr(rows),
			});
		},
		start: (rows) =>
			builder({
				...reading(state, isExpr(rows) ? [rows] : [], false),
				start: count(rows, 'START'),
				params: bounded(rows),
				pure: state.pure && !isExpr(rows),
			}),
		project: (fields) => {
			if (state.omit !== undefined) {
				throw new RangeError(
					'OMIT leaves fields out of `SELECT *`; a projection names the fields it keeps instead',
				);
			}
			const projection = fields(row) as Projection;
			const projected = project(projection);
			assertNoFetch(state, 'project');
			return builder({
				...reading(state, Object.values(projection)),
				base: {
					kind: 'projection',
					fields: Object.fromEntries(
						Object.entries(projection).map(([alias, value]) => [
							alias,
							codecOf(value),
						]),
					),
				},
				fields: projected.sql,
				aliases: projected.aliases,
				pure: state.pure && Object.values(projection).every(isPath),
				out: projected.codec,
				params: mergeParams(state.params, projected.params),
			});
		},
		value: (pick) => {
			if (state.omit !== undefined || state.split !== undefined) {
				throw new RangeError(
					'`SELECT VALUE` takes no OMIT or SPLIT here; project the field instead',
				);
			}
			const picked = pick(row);
			assertNoFetch(state, 'value');
			return builder({
				...reading(state, [picked]),
				base: { kind: 'value' },
				fields: `VALUE ${sqlOf(picked)}`,
				aliases: new Map(),
				pure: state.pure && isPath(picked),
				out: rowCodec(codecOf(picked)),
				params: mergeParams(state.params, paramsOf(picked)),
			});
		},
		fetch: (store, ...paths) => {
			const fetch = [...state.fetch, ...(paths as ReadonlyArray<string>)];
			const context = joinContexts([state.context, viewOf(store)]);
			return builder({
				...state,
				...(context !== undefined && { context }),
				fetch,
				out: fetchedCodec(store, state.base, fetch),
			}) as never;
		},
		only: () => {
			if (readsMany(state) && state.limit !== undefined) {
				throw new RangeError(
					'a SELECT … FROM ONLY a table reads at most one row; drop the LIMIT',
				);
			}
			return builder({ ...state, only: true });
		},
		omit: (...names) => {
			assertNoFetch(state, 'omit');
			if (state.fields !== '*') {
				throw new RangeError(
					'OMIT leaves fields out of `SELECT *`; a projection names the fields it keeps instead',
				);
			}
			const stored = fieldsOf(state.table.select);
			if (stored === undefined) {
				throw new TypeError('OMIT takes a generated row schema');
			}
			const omitted = [
				...(state.omit ?? []),
				...(names as ReadonlyArray<string>),
			];
			for (const name of omitted) {
				if (!Object.hasOwn(stored, name)) {
					throw new RangeError(
						`\`${name}\` is not a field of \`${state.table.name}\``,
					);
				}
				if (name === state.split) {
					throw new RangeError(
						`\`${name}\` is the SPLIT field, which the row must hold`,
					);
				}
			}
			const kept = Object.fromEntries(
				Object.entries(stored).filter(
					([name]) => !omitted.includes(name),
				),
			);
			const select = rebuild(state.table.select, kept);
			return builder({
				...state,
				omit: omitted,
				out: rowCodec(select),
				base: { kind: 'row', table: { ...state.table, select } },
			}) as never;
		},
		split: (field) => {
			assertNoFetch(state, 'split');
			const name = field as string;
			if (
				state.fields !== '*' ||
				state.only ||
				state.omit !== undefined
			) {
				throw new RangeError(
					'split a SELECT before projecting it, omitting fields or reading ONLY one row',
				);
			}
			if (state.split !== undefined) {
				throw new RangeError('this SELECT splits one field');
			}
			const stored = fieldsOf(state.table.select);
			const declared = stored?.[name];
			const codec = declared === undefined ? undefined : unwrap(declared);
			if (
				stored === undefined ||
				declared === undefined ||
				codec === undefined ||
				!('value' in codec) ||
				!Schema.isSchema(codec.value)
			) {
				throw new RangeError(
					`\`${name}\` is not an array field of \`${state.table.name}\`: SPLIT takes one`,
				);
			}
			const element = Schema.Union([codec.value as Codec, codec]);
			const select = rebuild(state.table.select, {
				...stored,
				[name]: isOptional(declared)
					? Schema.optionalKey(element)
					: element,
			});
			const table = { ...state.table, select };
			return builder({
				...state,
				table,
				row: rowExprs(table, state.scope),
				out: storedRow(table),
				base: { kind: 'row', table },
				split: name,
			}) as never;
		},
		groupBy: (keys) =>
			groupBy(
				baseOf(state),
				keys as (row: RowExprs<PureTable>) => unknown,
			) as never,
		groupAll: () => groupAll(baseOf(state)) as never,
		toExpr: () =>
			subquery(
				resultOf(state) as SelectResult<Row, Only>,
				render(state),
				state.params as P,
				ownReader(state, `the subquery of \`${state.table.name}\``),
			),
		build: (name, options) => {
			const writes = options?.writes ?? (state.pure ? false : undefined);
			const label = name ?? `select ${state.table.name}`;
			settle(
				ownReader(state, `statement \`${label}\``),
				`statement \`${label}\``,
			);
			return make({
				...(writes !== undefined && { writes }),
				name: label,
				sql: `${render(state)};`,
				params: state.params as Simplify<P>,
				result: resultOf(state) as SelectResult<Row, Only>,
			});
		},
	};
};

/**
 * `SELECT * FROM table`; `SELECT * FROM $id` with a record id expression of
 * that table (a binding, or {@link recordId}); or the records an
 * {@link idRange} of that table spans. Rows decode with the descriptor's
 * select schema.
 *
 * @example
 * const recent = select(PersonTable)
 *   .where((row) => gte(row.age, param('minimum', Int)))
 *   .orderBy((row) => row.name)
 *   .project((row) => ({ name: row.name, city: row.address.city }))
 *   .build('recent adults');
 */
export const select: {
	<T extends PureTable>(table: T): Select<T, NoParams, StoredRow<T>, false>;
	<T extends PureTable, P extends Params>(
		table: T,
		subject: IdOf<T, P> | IdRange<T['name'], P>,
	): RecordSelect<T, P>;
	/**
	 * `SELECT * FROM ONLY table WITH INDEX index WHERE … LIMIT 1` by a
	 * unique index: one operand per field of the index (`{ email:
	 * param('email', String) }`; a nested field by its path,
	 * `'profile.bio'`), so the result is the row or NONE. `WITH INDEX`
	 * makes the engine read that index: with `LIMIT 1` it otherwise reads
	 * any index on a prefix of the fields whose name sorts first. An index
	 * that is not unique, a missing or extra field, a value that never
	 * compares with its field and a path through a field that is not one
	 * object (a union of objects, a nullable object) are compile errors, as
	 * is a value that may be
	 * an array on a field that may hold an array or another kind: SurrealDB
	 * 3.3 finds no array through such an index, and 4.0 does.
	 */
	<
		T extends PureTable,
		const N extends string,
		const V extends { readonly [field: string]: Operand },
	>(
		table: T,
		index: UniqueIndex<T, N>,
		values: UniqueValues<T, N, V>,
	): Select<T, ParamsOfAll<V[keyof V]>, StoredRow<T>, true>;
} = <T extends PureTable>(
	table: T,
	id?: Expr | IdRange | string,
	values?: { readonly [field: string]: Operand },
): never => {
	const scope = new RowScope('select', table.name);
	const state: State = {
		table,
		scope,
		row: rowExprs(table, scope),
		source: undefined,
		out: storedRow(table),
		fields: '*',
		where: [],
		pure: id === undefined,
		order: [],
		aliases: new Map(),
		limit: undefined,
		start: undefined,
		only: false,
		params: {},
		base: { kind: 'row', table },
		fetch: [],
	};
	if (typeof id === 'string') {
		const where = uniqueLookup(table, state.row, id, values ?? {});
		return builder<T, Params, StoredRow<T>, true>({
			...reading(state, where),
			where,
			pure: false,
			only: true,
			index: id,
			params: mergeParams(...where.map(paramsOf)),
		}) as never;
	}
	if (isIdRange(id)) {
		const { sql, params, parts = [] } = id[IdRangeTypeId];
		if (id[IdRangeTypeId].table !== table.name) {
			throw new RangeError(
				`a range of \`${id[IdRangeTypeId].table}\` is not a subject of \`${table.name}\``,
			);
		}
		const source = derive(Schema.Unknown, sql, Level.atom, parts, params);
		return {
			...builder<T, Params, StoredRow<T>, false>({
				...reading(state, [source], false),
				source,
				range: true,
				pure: false,
				params,
			}),
			forUpdate: () => {
				throw new RangeError(
					`FOR UPDATE reads one record, and an id range of \`${table.name}\` reads many; select the record by its id`,
				);
			},
		} as never;
	}
	if (id === undefined) {
		return builder<T, Params, StoredRow<T>, false>(state) as never;
	}
	const record: State = {
		...reading(state, [id], false),
		source: id,
		params: paramsOf(id),
	};
	return {
		...builder<T, Params, StoredRow<T>, false>(record),
		forUpdate: () => {
			if (!lockable(id)) {
				throw new RangeError(
					`FOR UPDATE reads one record from a binding or a local (\`$name\`), a literal record id or a \`recordId(…)\`, and SurrealDB refuses \`${sqlOf(id)}\` as its target; LET the id in a block first`,
				);
			}
			return builder<T, Params, StoredRow<T>, false>({
				...record,
				lock: true,
			});
		},
	} as never;
};

type IsUnion<U, All = U> = U extends unknown
	? [All] extends [U]
		? false
		: true
	: never;

/**
 * The table whose records the traversal `R` of store `S` yields, when it
 * is one table of the store; otherwise a message saying why `R` is no
 * subject.
 */
export type ReachedTable<S, R> = R extends {
	readonly [TraversalTypeId]: unknown;
}
	? LinkTables<CodecOf<R>['Encoded']> extends infer Tb
		? [Tb] extends [never]
			? 'a select reads the records of a graph step or a recursive path'
			: string extends Tb
				? 'the records of this step can be of any table, so a select has no row to read them with'
				: true extends IsUnion<Tb>
					? 'the records of this step are of several tables, and a select reads the rows of one'
					: Extract<TableIn<S, Tb>, PureTable>
		: never
	: "a select reads the records of a graph step or a recursive path, like `outgoing(store, param('who', PersonId), 'knows')`";

/** `R`, when it is a subject {@link selectFrom} takes; otherwise the message. */
export type ReachedSubject<S, R> =
	ReachedTable<S, R> extends PureTable ? R : ReachedTable<S, R>;

/**
 * `SELECT * FROM <records>`: the records a graph step (`outgoing`,
 * `incoming`, `both`), the edges of one (`edges`) or a recursive path
 * reaches, read as rows of their table, so a condition, a sort key, a
 * limit, a projection, FETCH and grouping apply to them as to any select.
 * The records must be of one table of `store`, and decode with its select
 * row (its `visible` row through `readAs(store)`). SurrealDB 3.3 and 4.0
 * read a step from a record that does not exist as no rows, keep the
 * duplicates of chained steps, and read a step from the row of an outer
 * select in a subquery from that row.
 *
 * @example
 * selectFrom(store, outgoing(store, param('who', PersonId), 'knows'))
 *   .where((peer) => gte(peer.age, 18))
 *   .orderBy((peer) => peer.name)
 *   .limit(10)
 *   .build();
 * // SELECT * FROM $who->knows->? WHERE age >= 18 ORDER BY name ASC LIMIT 10;
 */
export const selectFrom = <S extends GraphStore, const R extends Expr>(
	store: S,
	records: ReachedSubject<S, R>,
): Select<
	Extract<ReachedTable<S, R>, PureTable>,
	ParamsOf<R>,
	StoredRow<Extract<ReachedTable<S, R>, PureTable>>,
	false
> => {
	const source = records as Expr;
	const data = traversalOf(source);
	if (data === undefined) {
		throw new TypeError(
			'a select reads the records of a graph step or a recursive path',
		);
	}
	const shape = linkShape(data.item);
	const names = shape === undefined ? 'any' : shapeTables(shape);
	if (names === 'any') {
		throw new TypeError(
			'the records of this step can be of any table, so a select has no row to read them with',
		);
	}
	if (names.length !== 1) {
		throw new RangeError(
			`the records of this step are of several tables (${names.map((name) => `\`${name}\``).join(', ')}), and a select reads the rows of one`,
		);
	}
	const [name] = names as readonly [string];
	const table = store.tables[name];
	if (table === undefined) {
		throw new RangeError(`\`${name}\` is not a table of this store`);
	}
	const scope = new RowScope('select', table.name);
	const state: State = {
		table,
		scope,
		row: rowExprs(table, scope),
		source,
		range: true,
		out: storedRow(table),
		fields: '*',
		where: [],
		pure: false,
		order: [],
		aliases: new Map(),
		limit: undefined,
		start: undefined,
		only: false,
		params: paramsOf(source),
		base: { kind: 'row', table },
		fetch: [],
	};
	return builder(reading(state, [source], false)) as never;
};

/** The conditions of a lookup by a unique index, checked as the types are. */
/** Whether a value `ast` describes may be an array, and may be another kind. */
const arrayShape = (
	ast: SchemaAST.AST,
	seen: Set<SchemaAST.AST> = new Set(),
): { readonly array: boolean; readonly other: boolean } => {
	if (seen.has(ast)) return { array: false, other: false };
	seen.add(ast);
	if (SchemaAST.isUndefined(ast) || SchemaAST.isNull(ast)) {
		return { array: false, other: false };
	}
	if (SchemaAST.isUnknown(ast) || SchemaAST.isAny(ast)) {
		return { array: true, other: true };
	}
	if (SchemaAST.isArrays(ast)) return { array: true, other: false };
	if (SchemaAST.isSuspend(ast)) return arrayShape(ast.thunk(), seen);
	if (SchemaAST.isUnion(ast)) {
		const members = ast.types.map((member) => arrayShape(member, seen));
		return {
			array: members.some((member) => member.array),
			other: members.some((member) => member.other),
		};
	}
	return { array: false, other: true };
};

/**
 * Refuses an array lookup SurrealDB 3.3 and 4.0 answer differently: 3.3
 * finds no array through a unique index on a field that may also hold
 * another kind (`any`, `array<int> | string`); 4.0 does.
 */
const assertSameOnBoth = (
	field: Expr,
	value: Operand,
	key: string,
	index: string,
): void => {
	const stored = arrayShape(codecOf(field).ast);
	if (!stored.array || !stored.other || !isExpr(value)) return;
	if (!arrayShape(codecOf(value).ast).array) return;
	throw new RangeError(
		`\`${key}\` of the unique index \`${index}\` may hold an array or another kind, and SurrealDB 3.3 finds no array through such an index (4.0 does): look it up by a value that is not an array, or declare \`${key}\` an array kind only`,
	);
};

const uniqueLookup = (
	table: PureTable,
	row: RowExprs<PureTable>,
	name: string,
	values: { readonly [field: string]: Operand },
): ReadonlyArray<Expr> => {
	const index = table.indexes?.[name];
	if (index === undefined) {
		throw new RangeError(`\`${table.name}\` has no index \`${name}\``);
	}
	if (index.type !== 'unique') {
		throw new RangeError(
			`\`${name}\` is a ${index.type} index of \`${table.name}\`, not a unique one`,
		);
	}
	const keys = index.columns.map((path) => path.join('.'));
	if (index.columns.some((path) => path.includes('*'))) {
		throw new RangeError(
			`\`${name}\` indexes array elements, which a unique lookup cannot address`,
		);
	}
	for (const key of keys) {
		if (!Object.hasOwn(values, key)) {
			throw new RangeError(
				`the unique index \`${name}\` also needs \`${key}\``,
			);
		}
	}
	for (const key of Object.keys(values)) {
		if (!keys.includes(key)) {
			throw new RangeError(
				`\`${key}\` is not a field of the unique index \`${name}\``,
			);
		}
	}
	return index.columns.map((path, at) => {
		let field: unknown = row;
		for (const segment of path) {
			field =
				typeof field === 'object' &&
				field !== null &&
				Object.hasOwn(field, segment)
					? (field as Record<string, unknown>)[segment]
					: undefined;
		}
		if (!isPath(field)) {
			throw new RangeError(
				`\`${keys[at]}\` of the unique index \`${name}\` is not a declared field of \`${table.name}\``,
			);
		}
		const value = values[keys[at] as string] as Operand;
		assertSameOnBoth(field, value, keys[at] as string, name);
		const compare = eq as (left: Expr, right: Operand) => Expr;
		return compare(field, value);
	});
};
