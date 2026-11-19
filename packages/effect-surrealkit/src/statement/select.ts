/**
 * `SELECT` from a generated table descriptor.
 *
 * The builder is immutable: every method returns a new builder, so a base
 * query can be shared. The result codec is derived from the descriptor's
 * select schema, or from the projection's own expressions.
 */
import { Schema } from 'effect';
import { escapeIdent } from 'surrealdb';

import {
	type CodecOf,
	type Comparable,
	codecOf,
	type Expr,
	eq,
	isExpr,
	isPath,
	mergeParams,
	type Operand,
	type OperandEncoded,
	type OperandParams,
	type ParamsOfAll,
	type Path,
	type PathData,
	paramsOf,
	pathOf,
	sqlOf,
} from './expr.js';
import {
	type CheckedProjection,
	type ProjectedCodec,
	type Projection,
	type RowCodec,
	rowCodec,
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
			? N
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
				? V
				: `\`${Mismatched<T, ColumnsOf<IndexesOf<T>[N]>, V>}\` never compares with this value: their stored types do not overlap`
			: `\`${Exclude<keyof V, keyof ColumnsOf<IndexesOf<T>[N]>> & string}\` is not a field of the unique index \`${N & string}\``
		: `the unique index \`${N & string}\` also needs \`${Exclude<keyof ColumnsOf<IndexesOf<T>[N]>, keyof V> & string}\``
	: V;

/** What a `SELECT` returns: every row, or with `ONLY` one row or NONE. */
export type SelectResult<
	Row extends Codec,
	Only extends boolean,
> = Only extends true ? Schema.UndefinedOr<Row> : Schema.$Array<Row>;

interface State {
	readonly table: PureTable;
	readonly row: RowExprs<PureTable>;
	readonly source: Expr | undefined;
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
	readonly limit: string | undefined;
	readonly start: string | undefined;
	readonly only: boolean;
	/** The index a lookup by a unique index names in `WITH INDEX`. */
	readonly index?: string;
	readonly params: Params;
}

interface Order {
	readonly sql: string;
	readonly direction: 'ASC' | 'DESC';
	readonly path: PathData;
}

/**
 * SurrealDB resolves an `ORDER BY` path against the projection before the
 * stored row, so an alias named like the field the path starts from, over
 * anything but that field, would change what is sorted.
 */
const assertOrder = (state: State): void => {
	for (const { path, sql } of state.order) {
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
	const limit = state.only && state.source === undefined ? '1' : state.limit;
	return [
		`SELECT ${state.fields} FROM ${state.only ? 'ONLY ' : ''}${
			state.source === undefined
				? tableName(state.table)
				: sqlOf(state.source)
		}`,
		...(state.index === undefined
			? []
			: [`WITH INDEX ${escapeIdent(state.index)}`]),
		...(state.where.length > 0 ? [`WHERE ${andAll(state.where)}`] : []),
		...(state.order.length > 0
			? [
					`ORDER BY ${state.order
						.map(({ direction, sql }) => `${sql} ${direction}`)
						.join(', ')}`,
				]
			: []),
		...(limit === undefined ? [] : [`LIMIT ${limit}`]),
		...(state.start === undefined ? [] : [`START ${state.start}`]),
	].join(' ');
};

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

	/** `SELECT VALUE`: one expression per row instead of an object. */
	readonly value: <S extends Codec, P2 extends Params>(
		pick: (row: RowExprs<T>) => Expr<S, P2>,
	) => Select<T, P & P2, RowCodec<S>, Only>;

	/**
	 * `FROM ONLY`: one row, or NONE when there is none. Reading from a table
	 * adds `LIMIT 1`, because the engine fails `ONLY` on a second row.
	 */
	readonly only: () => Select<T, P, Row, true>;

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
	) => Statement<Simplify<P>, SelectResult<Row, Only>>;
}

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
			const condition = predicate(row);
			return builder({
				...state,
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
				...state,
				order: [
					...state.order,
					{ sql: sqlOf(by as Path), direction, path },
				],
			});
		},
		limit: (rows) => {
			if (state.only && state.source === undefined) {
				throw new RangeError(
					'a SELECT … FROM ONLY a table already reads at most one row',
				);
			}
			return builder({
				...state,
				limit: count(rows, 'LIMIT'),
				params: bounded(rows),
				pure: state.pure && !isExpr(rows),
			});
		},
		start: (rows) =>
			builder({
				...state,
				start: count(rows, 'START'),
				params: bounded(rows),
				pure: state.pure && !isExpr(rows),
			}),
		project: (fields) => {
			const projection = fields(row) as Projection;
			const projected = project(projection);
			return builder({
				...state,
				fields: projected.sql,
				aliases: projected.aliases,
				pure: state.pure && Object.values(projection).every(isPath),
				out: projected.codec,
				params: mergeParams(state.params, projected.params),
			});
		},
		value: (pick) => {
			const picked = pick(row);
			return builder({
				...state,
				fields: `VALUE ${sqlOf(picked)}`,
				aliases: new Map(),
				pure: state.pure && isPath(picked),
				out: rowCodec(codecOf(picked)),
				params: mergeParams(state.params, paramsOf(picked)),
			});
		},
		only: () => {
			if (state.source === undefined && state.limit !== undefined) {
				throw new RangeError(
					'a SELECT … FROM ONLY a table reads at most one row; drop the LIMIT',
				);
			}
			return builder({ ...state, only: true });
		},
		toExpr: () =>
			subquery(
				resultOf(state) as SelectResult<Row, Only>,
				render(state),
				state.params as P,
			),
		build: (name, options) => {
			const writes = options?.writes ?? (state.pure ? false : undefined);
			return make({
				...(writes !== undefined && { writes }),
				name: name ?? `select ${state.table.name}`,
				sql: `${render(state)};`,
				params: state.params as Simplify<P>,
				result: resultOf(state) as SelectResult<Row, Only>,
			});
		},
	};
};

/**
 * `SELECT * FROM table`, or `SELECT * FROM $id` with a record id expression
 * of that table. Rows decode with the descriptor's select schema.
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
		id: IdOf<T, P>,
	): Select<T, P, StoredRow<T>, false>;
	/**
	 * `SELECT * FROM ONLY table WITH INDEX index WHERE … LIMIT 1` by a
	 * unique index: one operand per field of the index (`{ email:
	 * param('email', String) }`; a nested field by its path,
	 * `'profile.bio'`), so the result is the row or NONE. `WITH INDEX`
	 * makes the engine read that index: with `LIMIT 1` it otherwise reads
	 * any index on a prefix of the fields whose name sorts first. An index
	 * that is not unique, a missing or extra field and a value that never
	 * compares with its field are compile errors.
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
	id?: Expr | string,
	values?: { readonly [field: string]: Operand },
): never => {
	const state: State = {
		table,
		row: rowExprs(table),
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
	};
	if (typeof id === 'string') {
		const where = uniqueLookup(table, state.row, id, values ?? {});
		return builder<T, Params, StoredRow<T>, true>({
			...state,
			where,
			pure: false,
			only: true,
			index: id,
			params: mergeParams(...where.map(paramsOf)),
		}) as never;
	}
	return builder<T, Params, StoredRow<T>, false>(
		id === undefined
			? state
			: { ...state, source: id, params: paramsOf(id) },
	) as never;
};

/** The conditions of a lookup by a unique index, checked as the types are. */
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
		const compare = eq as (left: Expr, right: Operand) => Expr;
		return compare(field, values[keys[at] as string] as Operand);
	});
};
