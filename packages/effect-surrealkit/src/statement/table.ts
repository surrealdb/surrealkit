/**
 * What the builders need from a generated table descriptor: its name and
 * row schemas, with codecs that need no services. Every table the emitter
 * generates fits.
 */
import type { Schema } from 'effect';
import { escapeIdent, type RecordId } from 'surrealdb';

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
	fieldsOf,
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
	readonly schemafull?: boolean;
	readonly id: Codec;
	readonly select: RowCodecIn;
	/** The row as a session reads it when field permissions may hide fields. */
	readonly visible?: RowCodecIn;
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

type ExtraReadFields<T extends PureTable, S extends RowCodecIn> = Exclude<
	keyof FieldsOf<S>,
	keyof FieldsOf<T['select']>
>;
type Encoded<F> = F extends Codec ? F['Encoded'] : never;

// A view may decode a link to a domain brand. Its encoded value still addresses
// the stored table and uses an accepted key, allowing NONE for a hidden field.
type IncompatibleReadLinks<Stored, View> = {
	[K in keyof Stored & keyof View]: [
		NonNullable<Encoded<Stored[K]>>,
	] extends [never]
		? never
		: [NonNullable<Encoded<Stored[K]>>] extends [RecordId]
			? [Exclude<Encoded<View[K]>, undefined>] extends [
					Exclude<Encoded<Stored[K]>, undefined>,
				]
				? never
				: K
			: never;
}[keyof Stored & keyof View];

type KnownReadFields<T extends PureTable, S extends RowCodecIn> = T extends {
	readonly schemafull: true;
}
	? [ExtraReadFields<T, S>] extends [never]
		? [IncompatibleReadLinks<FieldsOf<T['select']>, FieldsOf<S>>] extends [
				never,
			]
			? unknown
			: {
					readonly incompatibleRecordLinks: IncompatibleReadLinks<
						FieldsOf<T['select']>,
						FieldsOf<S>
					>;
				}
		: { readonly unknownReadFields: ExtraReadFields<T, S> }
	: unknown;

/**
 * `T`, when it is a descriptor with a generated `visible` row; otherwise a
 * message saying what `readAs(table)` needs.
 */
export type WithVisible<T> = T extends { readonly visible: RowCodecIn }
	? T
	: 'this descriptor has no generated `visible` row: regenerate it, or pass the row the session reads as the second argument';

/**
 * `table` as a session with narrower permissions reads it.
 *
 * A generated `select` schema describes a privileged reader. SurrealDB does
 * not report what permissions hide: a field the session may not read comes
 * back as NONE, and a row it may not read is simply absent.
 *
 * With one argument the statements read the generated `visible` row, in
 * which every field whose `select` permission is not FULL may be absent.
 * With two, they read the row given: declare the row such a session really
 * gets (usually the same fields, optional where a permission can hide them),
 * so the rows decode instead of failing on the missing fields.
 *
 * @example
 * const visible = select(readAs(MemberTable)).build();
 * // ReadonlyArray<{ …; email?: string; salary?: number }>
 */
export function readAs<T extends PureTable>(
	table: WithVisible<T>,
): Omit<T, 'select'> & { readonly select: NonNullable<T['visible']> };
export function readAs<T extends PureTable, S extends RowCodecIn>(
	table: T,
	select: S & KnownReadFields<NoInfer<T>, NoInfer<S>>,
): Omit<T, 'select'> & { readonly select: S };
export function readAs(table: PureTable, declared?: RowCodecIn): PureTable {
	if (declared === undefined) {
		if (table.visible === undefined) {
			throw new TypeError(
				`readAs: the descriptor of \`${table.name}\` has no generated \`visible\` row; regenerate it, or pass the row the session reads`,
			);
		}
		return { ...table, select: table.visible };
	}
	const select = declared;
	if (table.schemafull) {
		const stored = fieldsOf(table.select);
		const view = fieldsOf(select);
		if (!stored || !view)
			throw new TypeError(
				'readAs requires row schemas with declared fields',
			);
		for (const key of Object.keys(view)) {
			if (!Object.hasOwn(stored, key))
				throw new TypeError(
					`readAs: ${table.name} has no declared field ${key}`,
				);
		}
	}
	return { ...table, select };
}

/**
 * A table name as SurrealQL reads it in subject position (`FROM`,
 * `FROM ONLY`, `CREATE`, `UPDATE`, `DELETE`, `RELATE`), whatever the word:
 * always delimited, because a bare `from`, `ONLY` or `select` there is a
 * keyword, `none` is NONE and `a.b` a field path. `⟨…⟩` cannot escape a `⟩`,
 * so a name with `⟩` or `\` takes backticks, escaping `\` and `` ` ``.
 *
 * @internal
 */
export const tableName = (table: PureTable): string =>
	/[⟩\\]/u.test(table.name)
		? `\`${table.name.replace(/[\\`]/gu, (char) => `\\${char}`)}\``
		: `⟨${table.name}⟩`;

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
	/** Each alias and the text of its expression. */
	readonly aliases: ReadonlyMap<string, string>;
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
		aliases: new Map(
			entries.map(([alias, value]) => [alias, sqlOf(value)]),
		),
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
