/**
 * What the builders need from a generated table descriptor: its name and
 * row schemas, with codecs that need no services. Every table the emitter
 * generates fits.
 */
import type { Schema } from 'effect';
import { escapeIdent } from 'surrealdb';

import {
	codecOf,
	conjunct,
	type Expr,
	Level,
	makeExpr,
	mergeParams,
	type ParamsOfAll,
	paramsOf,
	sqlOf,
} from './expr.js';
import {
	type FieldsOf,
	type ProjectedCodec,
	type Projection,
	projectedCodec,
	type RowCodec,
	type RowCodecIn,
	type RowOf,
	rowCodec,
	rowOf,
} from './row.js';
import type { Codec, Params } from './statement.js';

/** A generated table descriptor, as far as the builders read it. */
export interface PureTable {
	readonly name: string;
	readonly id: Codec;
	readonly select: RowCodecIn;
	readonly create?: RowCodecIn;
	readonly merge?: RowCodecIn;
	readonly content?: RowCodecIn;
	readonly edge?: {
		readonly table: string;
		readonly in: ReadonlyArray<Codec>;
		readonly out: ReadonlyArray<Codec>;
	};
}

/** The row object a table's callbacks receive. */
export type RowExprs<T extends PureTable> = RowOf<FieldsOf<T['select']>>;

/** The codec a table's stored rows decode with. */
export type StoredRow<T extends PureTable> = RowCodec<T['select']>;

/** A boolean expression. */
export type Condition<P extends Params = Params> = Expr<
	Schema.Codec<unknown, boolean, never, never>,
	P
>;

/** A record id expression of table `T`. */
export type IdOf<T extends PureTable, P extends Params = Params> = Expr<
	Schema.Codec<unknown, T['id']['Encoded'], never, never>,
	P
>;

/**
 * `table` as a session with narrower permissions reads it.
 *
 * A generated `select` schema describes a privileged reader. SurrealDB does
 * not report what permissions hide: a field the session may not read comes
 * back as NONE, and a row it may not read is simply absent. Declare the row
 * such a session really gets (usually the same fields, optional where a
 * permission can hide them) and build statements from the result, so the
 * rows decode instead of failing on the missing fields.
 */
export const readAs = <T extends PureTable, S extends RowCodecIn>(
	table: T,
	select: S,
): Omit<T, 'select'> & { readonly select: S } => ({ ...table, select });

/** @internal */
export const tableName = (table: PureTable): string => escapeIdent(table.name);

/** @internal */
export const storedRow = <T extends PureTable>(table: T): StoredRow<T> =>
	rowCodec<T['select']>(table.select);

/** @internal */
export const rowExprs = <T extends PureTable>(table: T): RowExprs<T> =>
	rowOf<T['select']>(table.select);

/** @internal */
export interface Projected {
	readonly sql: string;
	readonly codec: Codec;
	readonly params: Params;
}

/**
 * The field list and codec of a projection: `expr AS alias, …`.
 *
 * @internal
 */
export const project = <F extends Projection>(
	fields: F,
): {
	readonly sql: string;
	readonly codec: ProjectedCodec<F>;
	readonly params: ParamsOfAll<F[keyof F]>;
} => {
	const entries = Object.entries(fields);
	if (entries.length === 0) {
		throw new RangeError('a projection needs at least one field');
	}
	const codecs: Record<string, Codec> = {};
	for (const [alias, value] of entries) {
		codecs[alias] = codecOf(value);
	}
	return {
		sql: entries
			.map(([alias, value]) => `${sqlOf(value)} AS ${escapeIdent(alias)}`)
			.join(', '),
		codec: projectedCodec<F>(
			codecs as Parameters<typeof projectedCodec<F>>[0],
		),
		params: mergeParams(
			...entries.map(([, value]) => paramsOf(value)),
		) as ParamsOfAll<F[keyof F]>,
	};
};

/** @internal */
export const andAll = (conditions: ReadonlyArray<Expr>): string =>
	conditions.map((condition) => conjunct(condition, Level.and)).join(' AND ');

/** @internal */
export const subquery = <S extends Codec, P extends Params>(
	codec: S,
	sql: string,
	params: P,
): Expr<S, P> => makeExpr(codec, `(${sql})`, params, Level.atom);
