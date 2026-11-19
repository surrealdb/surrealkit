/**
 * `GROUP BY` and `GROUP ALL` with typed aggregates.
 *
 * In a grouped `SELECT` a stored field yields one value per record of the
 * group, so SurrealDB answers a field that is neither grouped nor
 * aggregated with the array of the group's values (3.3 and 4.0), and
 * refuses a group key the projection leaves out. Here the group keys are
 * always projected, and the aggregate callback gets the stored row as
 * {@link RowValue}s: values only aggregates take, so projecting a field
 * that is neither grouped nor aggregated is a compile error.
 *
 * Each aggregate's codec follows what SurrealDB 3.3 and 4.0 return,
 * including for a group with no value (every value NONE, or `GROUP ALL`
 * over no record): `math::min` and `math::max` are then ±Infinity,
 * `math::mean`, `math::stddev` and `math::variance` NaN, `math::sum` the
 * int 0, and `math::median`, `time::min` and `time::max` NONE. Over a field
 * that may hold a decimal, `math::stddev` and `math::variance` are a decimal
 * or a number (one record spreads by the int 0) and `math::median` the
 * middle decimal or a float of the two middle values; `math::mean` is a
 * float whatever the field (`test/conformance.test.ts` and
 * `test/aggregates-engine.test.ts` pin every case).
 */
import { Schema } from 'effect';
import {
	escapeIdent,
	type DateTime as SdkDateTime,
	type Decimal as SdkDecimal,
} from 'surrealdb';

import { Decimal, SurrealNumber } from '../decimal.js';
import { Int, Int64 } from '../int.js';
import { Float } from '../scalars.js';
import {
	FloatOrDecimal,
	type FloatOrDecimalOf,
	holdsDecimal,
} from './arithmetic.js';
import {
	admit,
	assertReader,
	type Context,
	type Expr,
	isExpr,
	isPath,
	joinContexts,
	Level,
	makeExpr,
	mergeParams,
	type OrNone,
	orNone,
	type ParamsOfAll,
	type Path,
	paramsOf,
	type RowScope,
	settle,
	sqlOf,
} from './expr.js';
import {
	type FieldsOf,
	fieldsOf,
	type IsOptional,
	isOptional,
	type NestedFields,
	type ProjectedCodec,
	type Projection,
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
	type PureTable,
	project,
	type RowExprs,
	readerOf,
} from './table.js';

export const RowValueTypeId = '~effect-surrealkit/RowValue';

/**
 * A stored field of a grouped row: what an aggregate reads. It is not an
 * expression, so it cannot be projected or compared, only aggregated. `S` is
 * the field's codec, `O` whether a record may hold NONE there, and `A`
 * whether the group may be empty (`GROUP ALL`).
 */
export interface RowValue<
	S extends Codec = Codec,
	O extends boolean = boolean,
	A extends boolean = boolean,
> {
	readonly [RowValueTypeId]: {
		readonly codec: S;
		readonly sql: string;
		readonly optional: O;
		readonly all: A;
	};
}

/** One {@link RowValue} per field of a row schema, with its children. */
export type GroupRowOf<F, O extends boolean, A extends boolean> = {
	readonly [K in keyof F & string]: RowValue<
		Unwrapped<F[K]> & Codec,
		O extends true ? true : IsOptional<F[K]>,
		A
	> &
		([NestedFields<Unwrapped<F[K]>>] extends [never]
			? unknown
			: GroupRowOf<
					NestedFields<Unwrapped<F[K]>>,
					O extends true ? true : IsOptional<F[K]>,
					A
				>);
};

/** The grouped row of table `T`: its stored fields as aggregates read them. */
export type GroupRow<T extends PureTable, A extends boolean> = GroupRowOf<
	FieldsOf<T['select']>,
	false,
	A
>;

const groupRow = (
	fields: { readonly [field: string]: Codec },
	prefix: string,
	optional: boolean,
	all: boolean,
): Record<string, unknown> => {
	const row: Record<string, unknown> = {};
	for (const [key, declared] of Object.entries(fields)) {
		const mayBeNone = optional || isOptional(declared);
		const codec = unwrap(declared);
		const sql = `${prefix}${escapeIdent(key)}`;
		const nested = fieldsOf(codec);
		row[key] = Object.assign(
			nested === undefined
				? {}
				: groupRow(nested, `${sql}.`, mayBeNone, all),
			{ [RowValueTypeId]: { codec, sql, optional: mayBeNone, all } },
		);
	}
	return row;
};

const isRowValue = (value: unknown): value is RowValue =>
	typeof value === 'object' && value !== null && RowValueTypeId in value;

/** The codec of a field's value, from its row value. */
type CodecOfValue<V> =
	V extends RowValue<infer S, boolean, boolean> ? S : never;

/** The group may have no value of `V`: it may be NONE, or the group empty. */
type MayBeEmpty<V> =
	V extends RowValue<Codec, infer O, infer A>
		? O extends true
			? true
			: A
		: never;

/** `V` when its stored type is `Ok`; otherwise `Message`. */
type Taking<V, Ok, Message extends string> = V extends RowValue
	? [CodecOfValue<V>['Encoded']] extends [Ok]
		? V
		: Message
	: 'an aggregate takes a field of the grouped row';

type Aggregate<S extends Codec> = Expr<S, NoParams>;

const aggregate = (fn: string, value: RowValue | undefined, codec: Codec) =>
	makeExpr(
		codec,
		`${fn}(${value === undefined ? '' : value[RowValueTypeId].sql})`,
		{},
		Level.atom,
	);

const dataOf = (value: unknown) => {
	if (!isRowValue(value)) {
		throw new TypeError('an aggregate takes a field of the grouped row');
	}
	return value[RowValueTypeId];
};

const empty = (value: RowValue): boolean =>
	value[RowValueTypeId].optional || value[RowValueTypeId].all;

/** `count()`: the records of the group; `count(field)`: those where a bool field holds. */
export function count(): Aggregate<typeof Int>;
export function count<V extends RowValue>(
	value: Taking<V, boolean, '`count` takes a bool field, or nothing'>,
): Aggregate<typeof Int>;
export function count(value?: unknown): Aggregate<typeof Int> {
	if (value !== undefined) dataOf(value);
	return aggregate('count', value as RowValue | undefined, Int) as Aggregate<
		typeof Int
	>;
}

/** What `math::sum` returns for values of codec `S`. */
export type SumCodec<S extends Codec, E> = [S['Encoded']] extends [number]
	? typeof Float
	: [S['Encoded']] extends [number | bigint]
		? typeof Int64
		: [S['Encoded']] extends [SdkDecimal]
			? E extends true
				? Schema.Union<readonly [typeof Decimal, typeof Int]>
				: typeof Decimal
			: typeof SurrealNumber;

type NumberField = number | bigint | SdkDecimal;

const sumCodec = (value: RowValue): Codec => {
	const annotations = value[RowValueTypeId].codec.ast.annotations as
		| { readonly representation?: { readonly id?: unknown } }
		| undefined;
	switch (annotations?.representation?.id) {
		case 'effect-surrealkit/Int':
			return Int;
		case 'effect-surrealkit/Int64':
			return Int64;
		case 'effect-surrealkit/Decimal':
			// The sum of no value is the int 0, also for decimals.
			return empty(value) ? Schema.Union([Decimal, Int]) : Decimal;
		case 'effect-surrealkit/SurrealNumber':
			return SurrealNumber;
		default:
			return Float;
	}
};

/** `math::sum(field)`: the sum of a number field over the group (0 for none). */
export const sum = <V extends RowValue>(
	value: Taking<V, NumberField, '`math::sum` takes a number field'>,
): Aggregate<SumCodec<CodecOfValue<V>, MayBeEmpty<V>>> => {
	dataOf(value);
	return aggregate(
		'math::sum',
		value as RowValue,
		sumCodec(value as RowValue),
	) as Aggregate<SumCodec<CodecOfValue<V>, MayBeEmpty<V>>>;
};

/** `math::mean(field)`: a float whatever the field, NaN for a group with no value. */
export const mean = <V extends RowValue>(
	value: Taking<V, NumberField, '`math::mean` takes a number field'>,
): Aggregate<typeof Float> => {
	dataOf(value);
	return aggregate('math::mean', value as RowValue, Float) as Aggregate<
		typeof Float
	>;
};

/**
 * What `math::stddev` and `math::variance` return over values of codec `S`:
 * a float, or a float or a decimal when `S` may hold a decimal.
 */
export type SpreadCodec<S extends Codec> = FloatOrDecimalOf<S['Encoded']>;

const spreadCodec = (value: RowValue): Codec =>
	holdsDecimal(value[RowValueTypeId].codec)
		? (FloatOrDecimal as unknown as Codec)
		: Float;

const spread =
	<const Fn extends string>(fn: Fn) =>
	<V extends RowValue>(
		value: Taking<V, NumberField, `\`${Fn}\` takes a number field`>,
	): Aggregate<SpreadCodec<CodecOfValue<V>>> => {
		dataOf(value);
		return aggregate(
			fn,
			value as RowValue,
			spreadCodec(value as RowValue),
		) as Aggregate<SpreadCodec<CodecOfValue<V>>>;
	};

/**
 * `math::stddev(field)`: a float, or for a field that may hold a decimal a
 * decimal or a number; NaN for a group with no value.
 */
export const stddev = spread('math::stddev');
/** `math::variance(field)`: as {@link stddev}. */
export const variance = spread('math::variance');

/**
 * What `math::median` returns over values of codec `S`: as
 * {@link SpreadCodec}, or NONE for a group with no value when `E`.
 */
export type MedianCodec<S extends Codec, E> = E extends true
	? Schema.UndefinedOr<SpreadCodec<S>>
	: SpreadCodec<S>;

/**
 * `math::median(field)`: the middle value, or the mean of the two middle
 * ones (a float); for a field that may hold a decimal a decimal or a
 * number. NONE for a group with no value.
 */
export const median = <V extends RowValue>(
	value: Taking<V, NumberField, '`math::median` takes a number field'>,
): Aggregate<MedianCodec<CodecOfValue<V>, MayBeEmpty<V>>> => {
	dataOf(value);
	const codec = spreadCodec(value as RowValue);
	return aggregate(
		'math::median',
		value as RowValue,
		empty(value as RowValue) ? Schema.UndefinedOr(codec) : codec,
	) as Aggregate<MedianCodec<CodecOfValue<V>, MayBeEmpty<V>>>;
};

/** What `math::min` and `math::max` return: the field's value, or ±Infinity for none. */
export type ExtremeCodec<S extends Codec, E> = E extends true
	? Schema.Union<readonly [S, typeof Float]>
	: S;

const extreme =
	<const Fn extends string>(fn: Fn) =>
	<V extends RowValue>(
		value: Taking<V, NumberField, `\`${Fn}\` takes a number field`>,
	): Aggregate<ExtremeCodec<CodecOfValue<V>, MayBeEmpty<V>>> => {
		const data = dataOf(value);
		return aggregate(
			fn,
			value as RowValue,
			empty(value as RowValue)
				? Schema.Union([data.codec, Float])
				: data.codec,
		) as Aggregate<ExtremeCodec<CodecOfValue<V>, MayBeEmpty<V>>>;
	};

/** `math::min(field)`: the least value, Infinity for a group with no value. */
export const min = extreme('math::min');
/** `math::max(field)`: the greatest value, -Infinity for a group with no value. */
export const max = extreme('math::max');

/** What `time::min` and `time::max` return: the field's value, or NONE for none. */
export type TimeCodec<S extends Codec, E> = E extends true
	? Schema.UndefinedOr<S>
	: S;

const time =
	<const Fn extends string>(fn: Fn) =>
	<V extends RowValue>(
		value: Taking<V, SdkDateTime, `\`${Fn}\` takes a datetime field`>,
	): Aggregate<TimeCodec<CodecOfValue<V>, MayBeEmpty<V>>> => {
		const data = dataOf(value);
		return aggregate(
			fn,
			value as RowValue,
			empty(value as RowValue)
				? Schema.UndefinedOr(data.codec)
				: data.codec,
		) as Aggregate<TimeCodec<CodecOfValue<V>, MayBeEmpty<V>>>;
	};

/** `time::min(field)`: the earliest datetime, NONE for a group with no value. */
export const timeMin = time('time::min');
/** `time::max(field)`: the latest datetime, NONE for a group with no value. */
export const timeMax = time('time::max');

/** What `array::group` and `array::distinct` return: the distinct values. */
export type DistinctCodec<V> = Schema.$Array<
	V extends RowValue<infer S, infer O, boolean>
		? O extends true
			? OrNone<S>
			: S
		: never
>;

type ScalarValue<V> = V extends RowValue
	? [CodecOfValue<V>['Encoded']] extends [
			ReadonlyArray<unknown> | ReadonlySet<unknown>,
		]
		? 'this aggregate takes a field that is not an array: SurrealDB 4 flattens the arrays of a group and 3.3 does not'
		: V
	: 'an aggregate takes a field of the grouped row';

const distinct =
	(fn: string) =>
	<V extends RowValue>(
		value: ScalarValue<V>,
	): Aggregate<DistinctCodec<V>> => {
		const data = dataOf(value);
		return aggregate(
			fn,
			value as RowValue,
			Schema.Array(data.optional ? orNone(data.codec) : data.codec),
		) as Aggregate<DistinctCodec<V>>;
	};

/**
 * `array::group(field)`: the distinct values of a field that is not an
 * array. A record that holds NONE contributes NONE (once on SurrealDB 4, once
 * per record on 3.3).
 */
export const arrayGroup = distinct('array::group');
/** `array::distinct(field)`: the distinct values, as {@link arrayGroup}. */
export const arrayDistinct = distinct('array::distinct');

/** A group key as its callback returned it: a field path, or a message. */
export type CheckedKeys<K> = {
	readonly [A in keyof K]: K[A] extends Path
		? K[A]
		: 'GROUP BY takes a field path of the row, like `row.role` or `row.address.city`';
};

/**
 * Aggregates as their callback returned them, `F`, checked against the group
 * keys `K`: each an expression (an aggregate, or a constant), with an alias
 * no key has. A field of the grouped row is neither grouped nor aggregated.
 */
export type CheckedAggregates<F, K> = {
	readonly [A in keyof F]: A extends keyof K
		? `\`${A & string}\` is already a key of this GROUP BY`
		: F[A] extends Expr
			? F[A]
			: F[A] extends RowValue
				? `\`${A & string}\` is a field that is neither grouped nor aggregated: group by it, or aggregate it (count, sum, …)`
				: 'an aggregate is an expression: inline a constant with `literal`';
};

/** What a grouped `SELECT` returns: a row per group, or one for `GROUP ALL`. */
export type GroupResult<
	Row extends Codec,
	All extends boolean,
> = All extends true ? Schema.Tuple<readonly [Row]> : Schema.$Array<Row>;

/** @internal */
export interface Base {
	readonly table: PureTable;
	/** The row the select's callbacks read. */
	readonly scope: RowScope;
	/** What its conditions read besides their bindings and its own row. */
	readonly context?: Context;
	readonly row: RowExprs<PureTable>;
	/** The text after `FROM`. */
	readonly from: string;
	readonly where: ReadonlyArray<Expr>;
	readonly params: Params;
	/** Only stored fields and constants are read (see `select`). */
	readonly pure: boolean;
}

interface State extends Base {
	readonly keys: Projection;
	readonly aggregates: Projection;
	readonly order: ReadonlyArray<string>;
	readonly limit: string | undefined;
	readonly start: string | undefined;
}

/** A grouped `SELECT` with its output row `O` decided. */
export interface GroupedSelect<
	T extends PureTable,
	P extends Params,
	O,
	All extends boolean,
> {
	/**
	 * `ORDER BY` an alias of the output: a group key or an aggregate (the
	 * stored row is gone once records are grouped).
	 */
	readonly orderBy: <const A extends string>(
		alias: A extends keyof O
			? A
			: `\`${A}\` is not a group key or an aggregate of this SELECT`,
		direction?: 'ASC' | 'DESC',
	) => GroupedSelect<T, P, O, All>;
	/** `LIMIT`: at most this many groups. */
	readonly limit: (rows: number) => GroupedSelect<T, P, O, All>;
	/** `START`: skip this many groups. */
	readonly start: (rows: number) => GroupedSelect<T, P, O, All>;
	/** The finished statement. */
	readonly build: (
		name?: string,
		options?: { readonly writes?: boolean },
	) => Statement<Simplify<P>, GroupResult<ProjectedCodec<O>, All>>;
}

/** A `SELECT … GROUP BY` whose keys are set; add aggregates, or build. */
export interface Grouped<T extends PureTable, P extends Params, K>
	extends GroupedSelect<T, P, K, false> {
	/** The aggregates of each group, beside its keys. */
	readonly aggregate: <const F>(
		fields: (row: GroupRow<T, false>) => CheckedAggregates<F, K>,
	) => GroupedSelect<T, P & ParamsOfAll<F[keyof F]>, K & F, false>;
}

/** A `SELECT … GROUP ALL`: one group of every record; add its aggregates. */
export interface GroupAll<T extends PureTable, P extends Params> {
	/** The aggregates of the one group. */
	readonly aggregate: <const F>(
		fields: (row: GroupRow<T, true>) => CheckedAggregates<F, NoParams>,
	) => GroupedSelect<T, P & ParamsOfAll<F[keyof F]>, F, true>;
}

const rowCount = (rows: number, clause: string): string => {
	if (!Number.isInteger(rows) || rows < 0) {
		throw new RangeError(
			`${clause} takes a non-negative integer, not ${rows}`,
		);
	}
	return String(rows);
};

const outputOf = (state: State) =>
	project({ ...state.keys, ...state.aggregates });

const render = (state: State): string => {
	const keys = Object.values(state.keys);
	return [
		`SELECT ${outputOf(state).sql} FROM ${state.from}`,
		...(state.where.length > 0 ? [`WHERE ${andAll(state.where)}`] : []),
		keys.length === 0
			? 'GROUP ALL'
			: `GROUP BY ${keys.map((key) => sqlOf(key)).join(', ')}`,
		...(state.order.length > 0
			? [`ORDER BY ${state.order.join(', ')}`]
			: []),
		...(state.limit === undefined ? [] : [`LIMIT ${state.limit}`]),
		...(state.start === undefined ? [] : [`START ${state.start}`]),
	].join(' ');
};

const finishing = <
	T extends PureTable,
	P extends Params,
	O,
	All extends boolean,
>(
	state: State,
): GroupedSelect<T, P, O, All> => ({
	orderBy: (alias, direction = 'ASC') => {
		if (
			!Object.hasOwn(state.keys, alias) &&
			!Object.hasOwn(state.aggregates, alias)
		) {
			throw new RangeError(
				`\`${alias}\` is not a group key or an aggregate of this SELECT`,
			);
		}
		return finishing({
			...state,
			order: [...state.order, `${escapeIdent(alias)} ${direction}`],
		});
	},
	limit: (rows) => finishing({ ...state, limit: rowCount(rows, 'LIMIT') }),
	start: (rows) => finishing({ ...state, start: rowCount(rows, 'START') }),
	build: (name, options) => {
		const output = outputOf(state);
		const all = Object.keys(state.keys).length === 0;
		const writes = options?.writes ?? (state.pure ? false : undefined);
		const label = name ?? `select ${state.table.name} grouped`;
		const readers = settle(
			joinContexts([
				state.context,
				admit(
					{
						own: state.scope,
						what: `a grouped select of \`${state.table.name}\``,
						parents: 'refuse',
						refused:
							"it reads the row of a select from a step's `where` directly under that select, and a grouped select's rows are groups",
					},
					[
						...Object.values(state.keys),
						...Object.values(state.aggregates),
					],
				),
			]),
			`statement \`${label}\``,
		);
		assertReader(readerOf(state.table), readers, `statement \`${label}\``);
		return make({
			...(writes !== undefined && { writes }),
			name: label,
			sql: `${render(state)};`,
			params: mergeParams(state.params, output.params) as Simplify<P>,
			result: (all
				? Schema.Tuple([output.codec])
				: Schema.Array(output.codec)) as GroupResult<
				ProjectedCodec<O>,
				All
			>,
		});
	},
});

const withAggregates = (
	state: State,
	fields: (row: never) => unknown,
): State => {
	const fieldsOfRow = fieldsOf(state.table.select);
	if (fieldsOfRow === undefined) {
		throw new TypeError('a grouped SELECT takes a generated row schema');
	}
	const row = groupRow(
		fieldsOfRow,
		'',
		false,
		Object.keys(state.keys).length === 0,
	);
	const returned = (fields as (row: unknown) => Record<string, unknown>)(row);
	const aggregates: Record<string, Expr> = {};
	for (const [alias, value] of Object.entries(returned)) {
		if (Object.hasOwn(state.keys, alias)) {
			throw new RangeError(
				`\`${alias}\` is already a key of this GROUP BY`,
			);
		}
		if (!isExpr(value)) {
			throw new TypeError(
				isRowValue(value)
					? `\`${alias}\` is a field that is neither grouped nor aggregated: group by it, or aggregate it (count, sum, …)`
					: 'an aggregate is an expression: inline a constant with `literal`',
			);
		}
		aggregates[alias] = value;
	}
	if (Object.keys(state.keys).length + Object.keys(aggregates).length === 0) {
		throw new RangeError('a grouped SELECT needs at least one aggregate');
	}
	return {
		...state,
		aggregates,
		pure: false,
		params: mergeParams(
			state.params,
			...Object.values(aggregates).map(paramsOf),
		),
	};
};

/** @internal */
export const groupBy = (
	base: Base,
	keys: (row: RowExprs<PureTable>) => unknown,
): Grouped<PureTable, Params, Record<string, Expr>> => {
	const returned = keys(base.row) as Record<string, unknown>;
	const entries = Object.entries(returned);
	if (entries.length === 0) {
		throw new RangeError('GROUP BY needs at least one key; use groupAll()');
	}
	for (const [, key] of entries) {
		if (!isPath(key)) {
			throw new TypeError(
				'GROUP BY takes a field path of the row, like `row.role` or `row.address.city`',
			);
		}
	}
	const state: State = {
		...base,
		keys: returned as Projection,
		aggregates: {},
		order: [],
		limit: undefined,
		start: undefined,
	};
	return {
		...finishing(state),
		aggregate: (fields) => finishing(withAggregates(state, fields)),
	};
};

/** @internal */
export const groupAll = (base: Base): GroupAll<PureTable, Params> => {
	const state: State = {
		...base,
		keys: {},
		aggregates: {},
		order: [],
		limit: undefined,
		start: undefined,
	};
	return {
		aggregate: (fields) => finishing(withAggregates(state, fields)),
	};
};
