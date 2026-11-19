/**
 * `SELECT` from a generated table descriptor.
 *
 * The builder is immutable: every method returns a new builder, so a base
 * query can be shared. The result codec is derived from the descriptor's
 * select schema, or from the projection's own expressions.
 */
import { Schema } from 'effect';

import {
	codecOf,
	type Expr,
	isExpr,
	mergeParams,
	type OperandParams,
	type ParamsOfAll,
	paramsOf,
	sqlOf,
} from './expr.js';
import {
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
	readonly order: ReadonlyArray<string>;
	readonly limit: string | undefined;
	readonly start: string | undefined;
	readonly only: boolean;
	readonly params: Params;
}

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
	const limit = state.only && state.source === undefined ? '1' : state.limit;
	return [
		`SELECT ${state.fields} FROM ${state.only ? 'ONLY ' : ''}${
			state.source === undefined
				? tableName(state.table)
				: sqlOf(state.source)
		}`,
		...(state.where.length > 0 ? [`WHERE ${andAll(state.where)}`] : []),
		...(state.order.length > 0
			? [`ORDER BY ${state.order.join(', ')}`]
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

	/** Adds a sort key, after the ones already given. */
	readonly orderBy: <P2 extends Params = NoParams>(
		key: (row: RowExprs<T>) => Expr<Codec, P2>,
		direction?: 'ASC' | 'DESC',
	) => Select<T, P & P2, Row, Only>;

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
	readonly project: <const F extends Projection>(
		fields: (row: RowExprs<T>) => F,
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

	/** The finished statement. */
	readonly build: (name?: string) => Statement<P, SelectResult<Row, Only>>;
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
				params: mergeParams(state.params, paramsOf(condition)),
			});
		},
		orderBy: (key, direction = 'ASC') => {
			const by = key(row);
			return builder({
				...state,
				order: [...state.order, `${sqlOf(by)} ${direction}`],
				params: mergeParams(state.params, paramsOf(by)),
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
			});
		},
		start: (rows) =>
			builder({
				...state,
				start: count(rows, 'START'),
				params: bounded(rows),
			}),
		project: (fields) => {
			const projected = project(fields(row));
			return builder({
				...state,
				fields: projected.sql,
				out: projected.codec,
				params: mergeParams(state.params, projected.params),
			});
		},
		value: (pick) => {
			const picked = pick(row);
			return builder({
				...state,
				fields: `VALUE ${sqlOf(picked)}`,
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
		build: (name) =>
			make({
				name: name ?? `select ${state.table.name}`,
				sql: `${render(state)};`,
				params: state.params as P,
				result: resultOf(state) as SelectResult<Row, Only>,
			}),
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
} = <T extends PureTable>(table: T, id?: Expr) =>
	builder<T, Params, StoredRow<T>, false>({
		table,
		row: rowExprs(table),
		source: id,
		out: storedRow(table),
		fields: '*',
		where: [],
		order: [],
		limit: undefined,
		start: undefined,
		only: false,
		params: id === undefined ? {} : paramsOf(id),
	});
