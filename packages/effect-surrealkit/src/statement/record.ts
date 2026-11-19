/**
 * Record ids and id ranges built from their key parts.
 *
 * `recordId(table, key)` is `type::record("table", key)`, the key a single
 * part, an array of parts (an array key) or an object of parts (an object
 * key), every part a binding or a constant. `idRange(table, bounds)` is
 * `type::record("table", from..=to)`: the rows whose ids fall between two
 * keys, either of which may be left open. SurrealDB 3.3 and 4.0 parse no
 * binding as a scalar key in a record id or range literal (`t:$k`,
 * `t:$a..$b`), and address the records by key through `type::record`
 * (`test/conformance.test.ts` pins both). Parts are checked against the
 * key the table's id codec declares, so a returned id decodes with it.
 */
import {
	derive,
	type Expr,
	isExpr,
	Level,
	literal,
	mergeParams,
	type OperandEncoded,
	operand,
	type ParamsOfAll,
	type Primitive,
	paramsOf,
	sqlOf,
} from './expr.js';
import type { Codec, Params } from './statement.js';
import type { PureTable } from './table.js';

/** The codec of the key a table's id codec declares (`RecordIdFor(t, { key })`). */
type KeyCodecOf<T extends PureTable> = T['id'] extends {
	readonly '~type.parameters': readonly [infer K extends Codec];
}
	? K
	: Codec;

/** The stored (encoded) key of a record of `T`. */
export type RecordKeyOf<T extends PureTable> = KeyCodecOf<T>['Encoded'];

/** One part of a key: an expression, or a constant to inline. */
export type KeyPart = Expr | Primitive;

/** A key as its parts: one part, an array of parts or an object of parts. */
export type KeyParts =
	| KeyPart
	| readonly [KeyPart, ...ReadonlyArray<KeyPart>]
	| { readonly [part: string]: KeyPart };

type IsPart<V> = V extends Expr ? true : V extends Primitive ? true : false;

type PartFits<V, K> = [OperandEncoded<V>] extends [K] ? true : false;

type ElementAt<KA, I> = I extends keyof KA
	? KA[I]
	: KA extends ReadonlyArray<infer E>
		? E
		: never;

/** The positions of `V` whose part is not what the array key holds there. */
type BadPositions<V extends ReadonlyArray<unknown>, KA> = {
	[I in keyof V]: I extends `${number}`
		? IsPart<V[I]> extends true
			? PartFits<V[I], ElementAt<KA, I>> extends true
				? never
				: I
			: I
		: never;
}[number];

type CheckArray<KA, V extends ReadonlyArray<unknown>, Table extends string> = [
	KA,
] extends [never]
	? `the key of \`${Table}\` is not an array`
	: KA extends ReadonlyArray<unknown>
		? number extends KA['length']
			? [BadPositions<V, KA>] extends [never]
				? V
				: `the part at index ${BadPositions<V, KA> & string} of this key is not what a key of \`${Table}\` holds there`
			: V['length'] extends KA['length']
				? [BadPositions<V, KA>] extends [never]
					? V
					: `the part at index ${BadPositions<V, KA> & string} of this key is not what a key of \`${Table}\` holds there`
				: `a key of \`${Table}\` has ${KA['length']} parts, not ${V['length']}`
		: never;

type RequiredKeys<A> = {
	[K in keyof A]-?: Record<never, never> extends Pick<A, K> ? never : K;
}[keyof A];

type CheckObject<KO, V, Table extends string> = [KO] extends [never]
	? `the key of \`${Table}\` is not an object`
	: string extends keyof KO
		? [
				{
					[P in keyof V]-?: PartFits<V[P], KO[keyof KO]> extends true
						? never
						: P;
				}[keyof V],
			] extends [never]
			? V
			: `this key holds a part a key of \`${Table}\` does not`
		: [Exclude<RequiredKeys<KO>, keyof V>] extends [never]
			? [Exclude<keyof V, keyof KO>] extends [never]
				? [
						{
							[P in keyof V & keyof KO]-?: IsPart<
								V[P]
							> extends true
								? PartFits<V[P], KO[P]> extends true
									? never
									: P
								: P;
						}[keyof V & keyof KO],
					] extends [never]
					? V
					: `\`${{
							[P in keyof V & keyof KO]-?: IsPart<
								V[P]
							> extends true
								? PartFits<V[P], KO[P]> extends true
									? never
									: P
								: P;
						}[keyof V & keyof KO] &
							string}\` is not what a key of \`${Table}\` holds there`
				: `\`${Exclude<keyof V, keyof KO> & string}\` is not part of a key of \`${Table}\``
			: `a key of \`${Table}\` also needs \`${Exclude<RequiredKeys<KO>, keyof V> & string}\``;

/**
 * `V`, the key of a record of `T` as its parts, when every part is what the
 * table's key holds there; otherwise a message naming what does not fit.
 */
export type CheckedKey<T extends PureTable, V> =
	IsPart<V> extends true
		? PartFits<V, RecordKeyOf<T>> extends true
			? V
			: `this is not a key of \`${T['name']}\``
		: V extends ReadonlyArray<unknown>
			? CheckArray<
					Extract<RecordKeyOf<T>, ReadonlyArray<unknown>>,
					V,
					T['name']
				>
			: CheckObject<
					Extract<RecordKeyOf<T>, Record<string, unknown>>,
					V,
					T['name']
				>;

/** The bindings the parts of a key read. */
export type KeyParams<V> = [V] extends [never]
	? Record<never, never>
	: V extends Expr
		? ParamsOfAll<V>
		: V extends ReadonlyArray<unknown>
			? ParamsOfAll<V[number]>
			: V extends Primitive
				? Record<never, never>
				: ParamsOfAll<V[keyof V]>;

const part = (value: unknown): Expr => {
	if (isExpr(value)) return value;
	if (typeof value === 'object' && value !== null) {
		throw new TypeError(
			'a key part is an expression or a constant; nest no array or object inside a key',
		);
	}
	return literal(value as Primitive);
};

/** The text and bindings of a key built from its parts. */
const keyOf = (
	key: KeyParts,
): {
	readonly sql: string;
	readonly params: Params;
	readonly parts: ReadonlyArray<Expr>;
} => {
	if (isExpr(key) || typeof key !== 'object' || key === null) {
		const value = part(key);
		return { sql: operand(value), params: paramsOf(value), parts: [value] };
	}
	if (Array.isArray(key)) {
		if (key.length === 0) {
			throw new RangeError('an array key needs at least one part');
		}
		const parts = key.map(part);
		return {
			sql: `[${parts.map(operand).join(', ')}]`,
			params: mergeParams(...parts.map(paramsOf)),
			parts,
		};
	}
	const entries = Object.entries(key).map(
		([name, value]) => [name, part(value)] as const,
	);
	if (entries.length === 0) {
		throw new RangeError('an object key needs at least one part');
	}
	return {
		sql: `{ ${entries
			.map(
				([name, value]) => `${JSON.stringify(name)}: ${operand(value)}`,
			)
			.join(', ')} }`,
		params: mergeParams(...entries.map(([, value]) => paramsOf(value))),
		parts: entries.map(([, value]) => value),
	};
};

/** `type::record(s"table", …)` over a key or range text. */
const typeRecord = (table: PureTable, inner: string): string =>
	`type::record(${sqlOf(literal(table.name))}, ${inner})`;

/**
 * A record id of `table` built from its key: one part, an array of parts
 * (an array key, `[param('city', String), param('at', DateTime)]`) or an
 * object of parts (an object key), each a binding or a constant. Every part
 * must be what the table's id codec declares for that position, and the id
 * decodes with that codec. It is a record id expression of the table, so it
 * is the subject `select`, `create`, `update` and `deleteFrom` take, and a
 * `relate` endpoint.
 *
 * @example
 * const reading = select(ReadingTable, recordId(ReadingTable, [
 *   param('city', SurrealSchema.String),
 *   param('at', SurrealSchema.DateTime),
 * ])).only().build();
 * // SELECT * FROM ONLY type::record(s"reading", [$city, $at]);
 */
export const recordId = <T extends PureTable, const V extends KeyParts>(
	table: T,
	key: CheckedKey<T, V>,
): Expr<T['id'], KeyParams<V>> => {
	const built = keyOf(key as KeyParts);
	return derive(
		table.id,
		typeRecord(table, built.sql),
		Level.atom,
		built.parts,
		built.params,
	) as Expr<T['id'], KeyParams<V>>;
};

export const IdRangeTypeId = '~effect-surrealkit/IdRange';

/**
 * The records of table `Name` whose ids fall between two keys, reading the
 * bindings `P`: a `select` subject.
 */
export interface IdRange<
	Name extends string = string,
	P extends Params = Params,
> {
	readonly [IdRangeTypeId]: {
		readonly table: Name;
		readonly sql: string;
		readonly params: P;
		/** The expressions its bounds are built from. @internal */
		readonly parts?: ReadonlyArray<Expr>;
	};
}

/** Whether `value` is an id range. */
export const isIdRange = (value: unknown): value is IdRange =>
	typeof value === 'object' && value !== null && IdRangeTypeId in value;

/**
 * The bounds of an id range: a start (`from`, included, or `after`,
 * excluded) and an end (`to`, excluded as SurrealQL's `..` excludes it, or
 * `through`, included), each optional; a range with neither is every record.
 */
export interface RangeBounds {
	readonly from?: KeyParts;
	readonly after?: KeyParts;
	readonly to?: KeyParts;
	readonly through?: KeyParts;
}

type BoundOf<R, K extends keyof RangeBounds> = K extends keyof R ? R[K] : never;

/** What is wrong with the bound `K` of `R`, or `never`. */
type BoundProblem<T extends PureTable, R, K extends keyof RangeBounds> = [
	BoundOf<R, K>,
] extends [never]
	? never
	: [BoundOf<R, K>] extends [CheckedKey<T, BoundOf<R, K>>]
		? never
		: `\`${K}\`: ${CheckedKey<T, BoundOf<R, K>> & string}`;

/**
 * `R` when it is the bounds of a range of `T`: at most one start and one
 * end, each a key of the table as its parts; otherwise a message.
 */
export type CheckedRange<T extends PureTable, R> = 'from' extends keyof R
	? 'after' extends keyof R
		? 'a range starts `from` a key (included) or `after` one (excluded), not both'
		: CheckedEnd<T, R>
	: CheckedEnd<T, R>;

type CheckedEnd<T extends PureTable, R> = 'to' extends keyof R
	? 'through' extends keyof R
		? 'a range ends `to` a key (excluded) or `through` one (included), not both'
		: CheckedBounds<T, R>
	: CheckedBounds<T, R>;

type CheckedBounds<T extends PureTable, R> = [
	BoundProblem<T, R, 'from'>,
] extends [never]
	? [BoundProblem<T, R, 'after'>] extends [never]
		? [BoundProblem<T, R, 'to'>] extends [never]
			? [BoundProblem<T, R, 'through'>] extends [never]
				? R
				: BoundProblem<T, R, 'through'>
			: BoundProblem<T, R, 'to'>
		: BoundProblem<T, R, 'after'>
	: BoundProblem<T, R, 'from'>;

/** The bindings the bounds of a range read. */
export type RangeParams<R> = KeyParams<BoundOf<R, 'from'>> &
	KeyParams<BoundOf<R, 'after'>> &
	KeyParams<BoundOf<R, 'to'>> &
	KeyParams<BoundOf<R, 'through'>>;

/**
 * The records of `table` whose ids fall between two keys, as a `select`
 * subject that reads many rows: `{ from, through }` is `from..=through`,
 * `{ from, to }` is `from..to`, `{ after }` is `after>..`, and a side left
 * out is open. Each bound is a key of the table as its parts, as
 * {@link recordId} takes it.
 *
 * @example
 * const week = select(ReadingTable, idRange(ReadingTable, {
 *   from: [param('city', SurrealSchema.String), param('start', SurrealSchema.DateTime)],
 *   to: [param('city', SurrealSchema.String), param('end', SurrealSchema.DateTime)],
 * })).build();
 * // SELECT * FROM type::record(s"reading", [$city, $start]..[$city, $end]);
 */
export const idRange = <T extends PureTable, const R extends RangeBounds>(
	table: T,
	bounds: CheckedRange<T, R>,
): IdRange<T['name'], RangeParams<R>> => {
	const given = bounds as RangeBounds;
	if (given.from !== undefined && given.after !== undefined) {
		throw new RangeError(
			'a range starts `from` a key (included) or `after` one (excluded), not both',
		);
	}
	if (given.to !== undefined && given.through !== undefined) {
		throw new RangeError(
			'a range ends `to` a key (excluded) or `through` one (included), not both',
		);
	}
	const start = given.from ?? given.after;
	const end = given.to ?? given.through;
	const low = start === undefined ? undefined : keyOf(start);
	const high = end === undefined ? undefined : keyOf(end);
	const sql = `${low?.sql ?? ''}${given.after === undefined ? '' : '>'}..${
		given.through === undefined ? '' : '='
	}${high?.sql ?? ''}`;
	return {
		[IdRangeTypeId]: {
			table: table.name as T['name'],
			sql: typeRecord(table, sql),
			params: mergeParams(
				low?.params ?? {},
				high?.params ?? {},
			) as RangeParams<R>,
			parts: [...(low?.parts ?? []), ...(high?.parts ?? [])],
		},
	};
};
