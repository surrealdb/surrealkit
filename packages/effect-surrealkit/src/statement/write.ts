/**
 * `CREATE`, `UPDATE`, `DELETE` and `RELATE` from generated table descriptors.
 *
 * Data is bound, never inlined, and must fit the schema the descriptor
 * declares for the operation (`create`, `merge`, `content`). The result
 * codec follows from the target and the `RETURN` clause:
 *
 * - one record that the statement makes (`CREATE ONLY`, `RELATE ONLY`): the
 *   row;
 * - one record by id (`UPDATE ONLY $id`, `DELETE ONLY $id`): the row, or
 *   NONE when the record does not exist or the condition does not hold;
 * - a table: an array of rows.
 */
import { Schema } from 'effect';

import {
	type Expr,
	mergeParams,
	operand,
	type ParamsOfAll,
	paramsOf,
} from './expr.js';
import type { CheckedProjection, ProjectedCodec, Projection } from './row.js';
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
	tableName,
} from './table.js';

/** How many records a write addresses. */
export type Shape = 'one' | 'maybe' | 'many';

/** The result of a write returning rows of codec `Row`. */
export type WriteResult<Row extends Codec, S extends Shape> = S extends 'one'
	? Row
	: S extends 'maybe'
		? Schema.UndefinedOr<Row>
		: Schema.$Array<Row>;

/** The result of a write with `RETURN NONE`. */
export type NoResult<S extends Shape> = S extends 'many'
	? Schema.Tuple<readonly []>
	: Schema.Undefined;

/** The schema a table declares for an operation, or `never` without one. */
export type OperationOf<
	T extends PureTable,
	O extends 'create' | 'merge' | 'content',
> = T extends { readonly [K in O]: infer C extends Codec } ? C : never;

/** The top-level fields `S` sends that `Target` does not have. */
type ExtraFields<S, Target> = S extends object
	? Exclude<keyof S, keyof Target> & string
	: never;

/** The top-level fields an object of type `A` always has. */
type RequiredFields<A> = {
	[K in keyof A]-?: Record<never, never> extends Pick<A, K> ? never : K;
}[keyof A];

/** The top-level fields both send, where `S` sends what `Target` refuses. */
type WrongFields<S, Target> = {
	[K in keyof S & keyof Target]-?: [S[K]] extends [Target[K]] ? never : K;
}[keyof S & keyof Target] &
	string;

/**
 * Data of encoded type `S` for a schema of encoded type `Target`: `V` when
 * it fits, otherwise a message naming what does not, read in this order: a
 * value that is not an object, a field `Target` requires that `S` may leave
 * out, a field whose value `Target` refuses, any other mismatch, and a field
 * `Target` does not have.
 */
type FitsEncoded<S, Target, V, Table extends string, Clause extends string> = [
	S,
] extends [Target]
	? [ExtraFields<S, Target>] extends [never]
		? V
		: `\`${ExtraFields<S, Target>}\` is not a field \`${Table}\` accepts in ${Clause}`
	: [S] extends [object]
		? [Exclude<RequiredFields<Target>, RequiredFields<S>>] extends [never]
			? [WrongFields<S, Target>] extends [never]
				? `this value is not what \`${Table}\` accepts in ${Clause}`
				: `\`${WrongFields<S, Target>}\` is not a value \`${Table}\` accepts in ${Clause}`
			: `\`${Exclude<RequiredFields<Target>, RequiredFields<S>> & string}\` is required by \`${Table}\` in ${Clause}`
		: `this value is not an object, which \`${Table}\` takes in ${Clause}`;

/**
 * `V`, the data expression of codec `S`, when `S` may be bound as the data
 * of an operation whose schema is `Target`: what it sends is what the
 * schema sends, with no field the schema does not have (SurrealDB refuses
 * one, and assignability alone would let it through). An open schema (a
 * `FLEXIBLE` object, a schemaless table) accepts any field. Otherwise the
 * data's expected type is a message naming the first field that does not
 * fit, with the table and the clause (`CREATE`, `MERGE`, …).
 */
export type Fits<
	Target extends Codec,
	S extends Codec,
	V,
	Table extends string,
	Clause extends string,
> = [Target] extends [never]
	? `\`${Table}\` takes no data in ${Clause}`
	: FitsEncoded<S['Encoded'], Target['Encoded'], V, Table, Clause>;

/** A table that is a relation with declared endpoints. */
export type RelationTable = PureTable & {
	readonly edge: NonNullable<PureTable['edge']>;
};

/**
 * `V`, the endpoint expression of codec `S`, when its decoded id is one of
 * the relation's declared endpoint ids (brands included) or the relation
 * declares none; otherwise the expected type is `Message`.
 */
export type Endpoint<
	Accepted extends ReadonlyArray<Codec>,
	S extends Codec,
	V,
	Message extends string,
> = [Accepted[number]] extends [never]
	? V
	: [S['Type']] extends [Accepted[number]['Type']]
		? V
		: Message;

interface State {
	readonly verb: 'CREATE' | 'UPDATE' | 'DELETE' | 'RELATE';
	readonly table: PureTable;
	readonly row: RowExprs<PureTable>;
	/** The text after `VERB [ONLY]`. */
	readonly target: string;
	readonly shape: Shape;
	readonly data: string | undefined;
	readonly where: ReadonlyArray<Expr>;
	/** The `RETURN` clause and one row's codec, or `undefined` for NONE. */
	readonly returning:
		| { readonly sql: string; readonly codec: Codec }
		| undefined;
	readonly params: Params;
}

const render = (state: State): string =>
	[
		`${state.verb} ${state.shape === 'many' ? '' : 'ONLY '}${state.target}`,
		...(state.data === undefined ? [] : [state.data]),
		...(state.where.length > 0 ? [`WHERE ${andAll(state.where)}`] : []),
		`RETURN ${state.returning?.sql ?? 'NONE'}`,
	].join(' ');

const resultOf = (state: State): Codec => {
	const { returning, shape } = state;
	if (returning === undefined) {
		return shape === 'many' ? Schema.Tuple([]) : Schema.Undefined;
	}
	if (shape === 'one') return returning.codec;
	return shape === 'maybe'
		? Schema.UndefinedOr(returning.codec)
		: Schema.Array(returning.codec);
};

const finish = <P extends Params, R extends Codec>(
	state: State,
	name: string | undefined,
): Statement<P, R> =>
	make({
		name: name ?? `${state.verb.toLowerCase()} ${state.table.name}`,
		sql: `${render(state)};`,
		params: state.params as P,
		result: resultOf(state) as R,
		writes: true,
		// CREATE ONLY and RELATE ONLY make one record whenever they apply;
		// a denied one returns NONE without an error.
		nonEmpty:
			(state.verb === 'CREATE' || state.verb === 'RELATE') &&
			state.returning !== undefined &&
			state.returning.sql !== 'BEFORE',
	});

const start = (
	verb: State['verb'],
	table: PureTable,
	options: {
		readonly target: string;
		readonly shape: Shape;
		readonly params: Params;
		readonly returning: 'AFTER' | 'NONE';
	},
): State => ({
	verb,
	table,
	row: rowExprs(table),
	target: options.target,
	shape: options.shape,
	data: undefined,
	where: [],
	returning:
		options.returning === 'NONE'
			? undefined
			: { sql: 'AFTER', codec: storedRow(table) },
	params: options.params,
});

const withData = (
	state: State,
	keyword: 'CONTENT' | 'MERGE',
	data: Expr,
): State => ({
	...state,
	data: `${keyword} ${operand(data)}`,
	params: mergeParams(state.params, paramsOf(data)),
});

const withWhere = (
	state: State,
	predicate: (row: RowExprs<PureTable>) => Expr,
): State => {
	const condition = predicate(state.row);
	return {
		...state,
		where: [...state.where, condition],
		params: mergeParams(state.params, paramsOf(condition)),
	};
};

const withReturning = (
	state: State,
	returning: 'after' | 'before' | 'none' | ((row: never) => Projection),
): State => {
	if (returning === 'none') return { ...state, returning: undefined };
	if (typeof returning === 'string') {
		return {
			...state,
			returning: {
				sql: returning.toUpperCase(),
				codec: storedRow(state.table),
			},
		};
	}
	const projected = project(
		(returning as (row: RowExprs<PureTable>) => Projection)(state.row),
	);
	return {
		...state,
		returning: { sql: projected.sql, codec: projected.codec },
		params: mergeParams(state.params, projected.params),
	};
};

/** A `CREATE ONLY` under construction; `R` is the codec of its result. */
export interface Create<
	T extends PureTable,
	P extends Params,
	R extends Codec,
> {
	/** `CONTENT`: the new record's data, as the table's create schema. */
	readonly content: <S extends Codec, P2 extends Params>(
		data: Fits<
			OperationOf<T, 'create'>,
			S,
			Expr<S, P2>,
			T['name'],
			'CREATE'
		>,
	) => Create<T, P & P2, R>;

	/** `RETURN NONE`, `RETURN AFTER` (the default) or chosen expressions. */
	readonly returning: {
		(mode: 'none'): Create<T, P, Schema.Undefined>;
		(mode: 'after'): Create<T, P, StoredRow<T>>;
		<const F>(
			fields: (row: RowExprs<T>) => CheckedProjection<F>,
		): Create<T, P & ParamsOfAll<F[keyof F]>, ProjectedCodec<F>>;
	};

	/** The finished statement. */
	readonly build: (name?: string) => Statement<P, R>;
}

type Returning = 'none' | 'after' | 'before' | ((row: never) => Projection);

const creating = <T extends PureTable, P extends Params, R extends Codec>(
	state: State,
): Create<T, P, R> => ({
	content: (data) => creating(withData(state, 'CONTENT', data as Expr)),
	returning: ((returning: Returning) =>
		creating(withReturning(state, returning))) as unknown as Create<
		T,
		P,
		R
	>['returning'],
	build: (name) => finish(state, name),
});

/**
 * `CREATE ONLY table`, or `CREATE ONLY $id` with a record id expression of
 * that table. Returns the created row unless told otherwise.
 */
export const create: {
	<T extends PureTable>(table: T): Create<T, NoParams, StoredRow<T>>;
	<T extends PureTable, P extends Params>(
		table: T,
		id: IdOf<T, P>,
	): Create<T, P, StoredRow<T>>;
} = <T extends PureTable>(table: T, id?: Expr) =>
	creating<T, Params, StoredRow<T>>(
		start('CREATE', table, {
			target: id === undefined ? tableName(table) : operand(id),
			shape: 'one',
			params: id === undefined ? {} : paramsOf(id),
			returning: 'AFTER',
		}),
	);

/** An `UPDATE` under construction; `R` is the codec of its result. */
export interface Update<
	T extends PureTable,
	P extends Params,
	R extends Codec,
	S extends Shape,
> {
	/** Adds a condition. Several `where` calls must all hold. */
	readonly where: <P2 extends Params>(
		predicate: (row: RowExprs<T>) => Condition<P2>,
	) => Update<T, P & P2, R, S>;

	/** `RETURN NONE`, `AFTER` (the default), `BEFORE` or chosen expressions. */
	readonly returning: {
		(mode: 'none'): Update<T, P, NoResult<S>, S>;
		(
			mode: 'after' | 'before',
		): Update<T, P, WriteResult<StoredRow<T>, S>, S>;
		<const F>(
			fields: (row: RowExprs<T>) => CheckedProjection<F>,
		): Update<
			T,
			P & ParamsOfAll<F[keyof F]>,
			WriteResult<ProjectedCodec<F>, S>,
			S
		>;
	};

	/** The finished statement. */
	readonly build: (name?: string) => Statement<P, R>;
}

const updating = <
	T extends PureTable,
	P extends Params,
	R extends Codec,
	S extends Shape,
>(
	state: State,
): Update<T, P, R, S> => ({
	where: (predicate) =>
		updating(
			withWhere(state, predicate as (row: RowExprs<PureTable>) => Expr),
		),
	returning: ((returning: Returning) =>
		updating(withReturning(state, returning))) as Update<
		T,
		P,
		R,
		S
	>['returning'],
	build: (name) => finish(state, name),
});

/** An `UPDATE` that still needs its data. */
export interface UpdateTarget<
	T extends PureTable,
	P extends Params,
	S extends Shape,
> {
	/**
	 * `MERGE`: omitted fields keep their value; `undefined` (NONE) removes a
	 * removable field. The data is the table's merge schema.
	 */
	readonly merge: <D extends Codec, P2 extends Params>(
		data: Fits<OperationOf<T, 'merge'>, D, Expr<D, P2>, T['name'], 'MERGE'>,
	) => Update<T, P & P2, WriteResult<StoredRow<T>, S>, S>;
	/** `CONTENT`: replaces the writable fields, as the table's content schema. */
	readonly content: <D extends Codec, P2 extends Params>(
		data: Fits<
			OperationOf<T, 'content'>,
			D,
			Expr<D, P2>,
			T['name'],
			'CONTENT'
		>,
	) => Update<T, P & P2, WriteResult<StoredRow<T>, S>, S>;
}

/**
 * `UPDATE table … WHERE`, returning every row it changed, or
 * `UPDATE ONLY $id` with a record id expression, returning the row or NONE
 * when the record does not exist. `UPDATE` never creates a record.
 */
export const update: {
	<T extends PureTable>(table: T): UpdateTarget<T, NoParams, 'many'>;
	<T extends PureTable, P extends Params>(
		table: T,
		id: IdOf<T, P>,
	): UpdateTarget<T, P, 'maybe'>;
} = <T extends PureTable>(
	table: T,
	id?: Expr,
): UpdateTarget<T, Params, Shape> => {
	const state = start('UPDATE', table, {
		target: id === undefined ? tableName(table) : operand(id),
		shape: id === undefined ? 'many' : 'maybe',
		params: id === undefined ? {} : paramsOf(id),
		returning: 'AFTER',
	});
	return {
		merge: (data) => updating(withData(state, 'MERGE', data as Expr)),
		content: (data) => updating(withData(state, 'CONTENT', data as Expr)),
	};
};

/** A `DELETE` under construction; `R` is the codec of its result. */
export interface Delete<
	T extends PureTable,
	P extends Params,
	R extends Codec,
	S extends Shape,
> {
	/** Adds a condition. Several `where` calls must all hold. */
	readonly where: <P2 extends Params>(
		predicate: (row: RowExprs<T>) => Condition<P2>,
	) => Delete<T, P & P2, R, S>;

	/** `RETURN NONE` (the default) or `RETURN BEFORE`, the deleted rows. */
	readonly returning: {
		(mode: 'none'): Delete<T, P, NoResult<S>, S>;
		(mode: 'before'): Delete<T, P, WriteResult<StoredRow<T>, S>, S>;
	};

	/** The finished statement. */
	readonly build: (name?: string) => Statement<P, R>;
}

const deleting = <
	T extends PureTable,
	P extends Params,
	R extends Codec,
	S extends Shape,
>(
	state: State,
): Delete<T, P, R, S> => ({
	where: (predicate) =>
		deleting(
			withWhere(state, predicate as (row: RowExprs<PureTable>) => Expr),
		),
	returning: ((returning: 'none' | 'before') =>
		deleting(withReturning(state, returning))) as Delete<
		T,
		P,
		R,
		S
	>['returning'],
	build: (name) => finish(state, name),
});

/**
 * `DELETE table … WHERE`, or `DELETE ONLY $id` with a record id expression.
 * Returns nothing unless asked for the rows as they were (`'before'`).
 */
export const deleteFrom: {
	<T extends PureTable>(
		table: T,
	): Delete<T, NoParams, NoResult<'many'>, 'many'>;
	<T extends PureTable, P extends Params>(
		table: T,
		id: IdOf<T, P>,
	): Delete<T, P, NoResult<'maybe'>, 'maybe'>;
} = <T extends PureTable>(table: T, id?: Expr) =>
	deleting<T, Params, never, never>(
		start('DELETE', table, {
			target: id === undefined ? tableName(table) : operand(id),
			shape: id === undefined ? 'many' : 'maybe',
			params: id === undefined ? {} : paramsOf(id),
			returning: 'NONE',
		}),
	);

/** A `RELATE ONLY` under construction; `R` is the codec of its result. */
export interface Relate<
	T extends PureTable,
	P extends Params,
	R extends Codec,
> {
	/** `CONTENT`: the edge's own fields, as the table's create schema. */
	readonly content: <S extends Codec, P2 extends Params>(
		data: Fits<
			OperationOf<T, 'create'>,
			S,
			Expr<S, P2>,
			T['name'],
			'RELATE'
		>,
	) => Relate<T, P & P2, R>;

	/** `RETURN NONE`, `RETURN AFTER` (the default) or chosen expressions. */
	readonly returning: {
		(mode: 'none'): Relate<T, P, Schema.Undefined>;
		(mode: 'after'): Relate<T, P, StoredRow<T>>;
		<const F>(
			fields: (row: RowExprs<T>) => CheckedProjection<F>,
		): Relate<T, P & ParamsOfAll<F[keyof F]>, ProjectedCodec<F>>;
	};

	/** The finished statement. */
	readonly build: (name?: string) => Statement<P, R>;
}

const relating = <T extends PureTable, P extends Params, R extends Codec>(
	state: State,
): Relate<T, P, R> => ({
	content: (data) => relating(withData(state, 'CONTENT', data as Expr)),
	returning: ((returning: Returning) =>
		relating(withReturning(state, returning))) as unknown as Relate<
		T,
		P,
		R
	>['returning'],
	build: (name) => finish(state, name),
});

/**
 * `RELATE ONLY $from->table->$to`: one edge between two records. Each
 * endpoint must be a record id the relation declares for that side.
 */
export const relate = <
	T extends RelationTable,
	From extends Codec,
	PF extends Params,
	To extends Codec,
	PT extends Params,
>(
	table: T,
	from: Endpoint<
		T['edge']['in'],
		From,
		Expr<From, PF>,
		`this record id is not an \`in\` endpoint of \`${T['name']}\``
	>,
	to: Endpoint<
		T['edge']['out'],
		To,
		Expr<To, PT>,
		`this record id is not an \`out\` endpoint of \`${T['name']}\``
	>,
): Relate<T, PF & PT, StoredRow<T>> => {
	const source = from as Expr;
	const target = to as Expr;
	return relating(
		start('RELATE', table, {
			target: `${operand(source)}->${tableName(table)}->${operand(target)}`,
			shape: 'one',
			params: mergeParams(paramsOf(source), paramsOf(target)),
			returning: 'AFTER',
		}),
	);
};
