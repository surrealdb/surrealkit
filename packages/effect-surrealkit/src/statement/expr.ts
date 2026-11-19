/**
 * Expressions: SurrealQL fragments as plain values.
 *
 * An expression carries its static text, the codec of the value it yields
 * (in the native world, so a returned value decodes with it) and the codec of
 * every binding it reads. Combining expressions combines their bindings, so
 * a statement cannot read a binding it did not declare.
 */
import { Schema } from 'effect';
import {
	FileRef,
	Future,
	Geometry,
	Range,
	RecordId,
	RecordIdRange,
	DateTime as SdkDateTime,
	Decimal as SdkDecimal,
	Duration as SdkDuration,
	Uuid as SdkUuid,
	StringRecordId,
	Table,
	toSurqlString,
} from 'surrealdb';

import { Decimal, SurrealNumber } from '../decimal.js';
import { Int, int64 } from '../int.js';
import { isPlainObject } from '../internal/declare.js';
import { SurrealString, Uuid } from '../scalars.js';
import type { FunctionDescriptor } from '../table.js';
import { DateTime, Duration, dateTimeToString } from '../temporal.js';
import {
	assertBindingName,
	type Codec,
	type NoParams,
	type Params,
} from './statement.js';

export const ExprTypeId = '~effect-surrealkit/Expr';

interface ExprData<S extends Codec, P extends Params> {
	readonly codec: S;
	readonly sql: string;
	readonly params: P;
	/** How loosely the text binds, which decides where it needs parentheses. */
	readonly level: Level;
	/** Set when the text is a field path of the row. */
	readonly path?: PathData;
}

/**
 * Where a field path starts: the top-level field it reads, as the row
 * schema names it and as the text spells it.
 *
 * @internal
 */
export interface PathData {
	readonly root: string;
	readonly rootSql: string;
}

/**
 * Binding strength, loosest last: a field, binding, literal or call; a
 * comparison; a conjunction; a disjunction; raw text of unknown shape.
 *
 * @internal
 */
export const Level = {
	atom: 0,
	comparison: 1,
	and: 2,
	or: 3,
	raw: 4,
} as const;
/** @internal */
export type Level = (typeof Level)[keyof typeof Level];

/**
 * A SurrealQL expression yielding a value of codec `S`, reading the bindings
 * `P`. Its data sits under one key so that a row's nested fields can be
 * plain properties without ever colliding with it.
 */
export interface Expr<S extends Codec = Codec, P extends Params = Params> {
	readonly [ExprTypeId]: ExprData<S, P>;
}

/**
 * A field path of the row (`name`, `address.city`, `phones[0]`): what
 * `ORDER BY` accepts. The row's field expressions are paths, and so is an
 * element of one ({@link at}); nothing else is.
 */
export interface Path<S extends Codec = Codec> extends Expr<S, NoParams> {
	readonly [ExprTypeId]: ExprData<S, NoParams> & { readonly path: PathData };
}

/** The codec of the value an expression yields. */
export type CodecOf<E> = E extends Expr<infer S, Params> ? S : never;

/** The bindings an expression reads. */
export type ParamsOf<E> = E extends Expr<Codec, infer P> ? P : never;

type UnionToIntersection<U> = (
	U extends unknown
		? (value: U) => void
		: never
) extends (value: infer I) => void
	? I
	: never;

/** The bindings several expressions read together. */
export type ParamsOfAll<E> = [ParamsOf<E>] extends [never]
	? NoParams
	: UnionToIntersection<ParamsOf<E>> extends infer I extends Params
		? I
		: NoParams;

export const isExpr = (value: unknown): value is Expr =>
	typeof value === 'object' && value !== null && ExprTypeId in value;

/** Whether `value` is a field path of a row. */
export const isPath = (value: unknown): value is Path =>
	isExpr(value) && value[ExprTypeId].path !== undefined;

/** The codec of the value `expr` yields. */
export const codecOf = <S extends Codec>(expr: Expr<S, Params>): S =>
	expr[ExprTypeId].codec;

/** The SurrealQL text of `expr`. */
export const sqlOf = (expr: Expr): string => expr[ExprTypeId].sql;

/** The codec of each binding `expr` reads. */
export const paramsOf = <P extends Params>(expr: Expr<Codec, P>): P =>
	expr[ExprTypeId].params;

/** @internal */
export const makeExpr = <S extends Codec, P extends Params>(
	codec: S,
	sql: string,
	params: P,
	level: Level,
): Expr<S, P> => ({ [ExprTypeId]: { codec, sql, params, level } });

/** @internal */
export const makePath = <S extends Codec>(
	codec: S,
	sql: string,
	path: PathData,
): Path<S> => ({
	[ExprTypeId]: { codec, sql, params: {}, level: Level.atom, path },
});

/** The path data of `expr`, when it is a field path. @internal */
export const pathOf = (expr: Expr): PathData | undefined =>
	expr[ExprTypeId].path;

/**
 * The text of `expr` as an operand of a comparison, call or clause: anything
 * that is not a single term is parenthesised.
 *
 * @internal
 */
export const operand = (expr: Expr): string => {
	const { level, sql } = expr[ExprTypeId];
	return level === Level.atom ? sql : `(${sql})`;
};

/**
 * The text of `expr` as an operand of `AND` or `OR`. Comparisons and the
 * same connective need no parentheses; the other connective gets them even
 * where precedence would allow it, so no reader has to recall precedence.
 *
 * @internal
 */
export const conjunct = (
	expr: Expr,
	within: typeof Level.and | typeof Level.or,
): string => {
	const { level, sql } = expr[ExprTypeId];
	return level <= Level.comparison || level === within ? sql : `(${sql})`;
};

/**
 * One set of bindings from several. The same name may appear twice only with
 * the same codec: two codecs would encode one value two ways.
 *
 * @internal
 */
export const mergeParams = (...sets: ReadonlyArray<Params>): Params => {
	const merged: Record<string, Codec> = {};
	for (const set of sets) {
		for (const [name, codec] of Object.entries(set)) {
			const existing = merged[name];
			if (existing !== undefined && existing !== codec) {
				throw new RangeError(
					`binding \`$${name}\` is declared with two different codecs; declare it once and reuse the expression`,
				);
			}
			merged[name] = codec;
		}
	}
	return merged;
};

/**
 * A named binding. The value is supplied when the statement runs and is
 * encoded through `codec` first, so a value the codec refuses never reaches
 * the engine.
 */
export const param = <const Name extends string, S extends Codec>(
	name: Name,
	codec: S,
): Expr<S, { readonly [K in Name]: S }> => {
	assertBindingName(name);
	return makeExpr(
		codec,
		`$${name}`,
		{ [name]: codec } as { readonly [K in Name]: S },
		Level.atom,
	);
};

/** A value SurrealQL can spell as a literal without a codec. */
export type Primitive = string | number | boolean | bigint | null | undefined;

/** The codec of an inline primitive. */
export type LiteralCodec<V> = [V] extends [null]
	? Schema.Null
	: [V] extends [undefined]
		? Schema.Undefined
		: V extends string | number | boolean | bigint
			? Schema.Literal<V>
			: never;

const noLiteral = (what: string): RangeError =>
	new RangeError(`${what} has no SurrealQL literal; bind it with \`param\``);

const describe = (value: object): string => {
	const name = (value as { constructor?: { name?: unknown } }).constructor
		?.name;
	return typeof name === 'string' && name.length > 0
		? `a ${name}`
		: 'this value';
};

/** A string as a constant: well-formed, as the string codec requires. */
const stringLiteral = (value: string): string =>
	JSON.stringify(Schema.encodeUnknownSync(SurrealString)(value));

/**
 * Checks what a record id or range spells inside its `r"…"` text, which the
 * SDK formats: its strings and integers as their codecs require, and no
 * datetime, which the SDK formats wrongly before 1970.
 */
const assertKey = (value: unknown, seen: Set<object>): void => {
	if (typeof value === 'string') {
		stringLiteral(value);
	} else if (typeof value === 'bigint') {
		int64(value);
	} else if (typeof value === 'object' && value !== null) {
		if (seen.has(value)) throw noLiteral('a cyclic value');
		seen.add(value);
		if (value instanceof SdkDateTime) {
			throw new RangeError(
				'a record id or range whose key holds a datetime has no faithful literal (the SDK spells the datetime); bind it with `param`',
			);
		}
		if (value instanceof RecordId) assertKey(value.id, seen);
		else if (value instanceof Range || value instanceof RecordIdRange) {
			assertKey(value.begin?.value, seen);
			assertKey(value.end?.value, seen);
		} else if (Array.isArray(value)) {
			for (const item of value) assertKey(item, seen);
		} else if (isPlainObject(value)) {
			for (const [key, item] of Object.entries(value)) {
				stringLiteral(key);
				assertKey(item, seen);
			}
		}
		seen.delete(value);
	}
};

/**
 * The SurrealQL literal of an encoded constant. Every part is checked as
 * its codec checks a binding (a string must be well-formed, an integer an
 * int64), datetimes are written with {@link dateTimeToString}, and what has
 * no faithful literal (a `Set`, a `Map`, bytes, a class instance, a
 * non-finite number) is refused.
 */
const inline = (value: unknown, seen: Set<object>): string => {
	switch (typeof value) {
		case 'string':
			return `s${stringLiteral(value)}`;
		case 'number':
			if (!Number.isFinite(value)) {
				throw new RangeError(
					`${String(value)} has no portable SurrealQL literal; bind it with \`param\``,
				);
			}
			return toSurqlString(value);
		case 'bigint':
			return String(int64(value));
		case 'boolean':
			return String(value);
		case 'undefined':
			return 'NONE';
		case 'object':
			break;
		default:
			throw noLiteral(`a ${typeof value}`);
	}
	if (value === null) return 'NULL';
	if (value instanceof Set || value instanceof Map) {
		throw new RangeError(
			'a Set or Map has no SurrealQL literal; bind it with `param`',
		);
	}
	if (seen.has(value)) throw noLiteral('a cyclic value');
	seen.add(value);
	try {
		if (Array.isArray(value)) {
			return `[ ${value.map((item) => inline(item, seen)).join(', ')} ]`;
		}
		if (isPlainObject(value)) {
			const entries = Object.entries(value).map(
				([key, item]) => `${stringLiteral(key)}: ${inline(item, seen)}`,
			);
			return `{ ${entries.join(', ')} }`;
		}
		if (value instanceof SdkDateTime) {
			return `d${JSON.stringify(dateTimeToString(value))}`;
		}
		if (
			value instanceof RecordId ||
			value instanceof StringRecordId ||
			value instanceof RecordIdRange ||
			value instanceof Range
		) {
			if (value instanceof StringRecordId)
				stringLiteral(value.toString());
			else assertKey(value, new Set());
			return toSurqlString(value);
		}
		if (
			value instanceof SdkUuid ||
			value instanceof SdkDecimal ||
			value instanceof SdkDuration ||
			value instanceof Table ||
			value instanceof FileRef ||
			value instanceof Future ||
			value instanceof Geometry ||
			value instanceof Date
		) {
			return toSurqlString(value);
		}
		throw noLiteral(describe(value));
	} finally {
		seen.delete(value);
	}
};

const primitiveCodec = (value: Primitive): Codec => {
	if (value === null) return Schema.Null;
	if (value === undefined) return Schema.Undefined;
	return Schema.Literal(value);
};

/**
 * A constant written into the statement text. The constant is part of the
 * statement, not input: it is fixed when the statement is defined, and is
 * spelt as SurrealQL spells it (`s"…"`, `d"…"`, `r"…"`).
 *
 * With a codec, the value is encoded through it first, so SDK values
 * (`DateTime`, `RecordId`, `Duration`, …) and nested data can be inlined.
 * Every part is then checked as a binding of its kind would be: a string
 * must be well-formed and an integer must fit a signed 64-bit int. A
 * datetime is written with `dateTimeToString`. A `Set`, a `Map`, bytes and
 * other values without a faithful literal are refused.
 * Never pass request input here: bind it with {@link param}.
 */
export function literal<const V extends Primitive>(
	value: V,
): Expr<LiteralCodec<V>, NoParams>;
export function literal<S extends Codec>(
	value: S['Type'],
	codec: S,
): Expr<S, NoParams>;
export function literal(value: unknown, codec?: Codec): Expr<Codec, NoParams> {
	if (codec === undefined && typeof value === 'object' && value !== null) {
		throw new TypeError(
			'only a primitive can be inlined without a codec; pass the codec, or bind the value with `param`',
		);
	}
	const encoded =
		codec === undefined ? value : Schema.encodeUnknownSync(codec)(value);
	const sql = inline(encoded, new Set());
	return makeExpr(
		codec ?? primitiveCodec(value as Primitive),
		sql,
		{},
		Level.atom,
	);
}

/** An expression, or a primitive to inline with {@link literal}. */
export type Operand = Expr | Primitive;

/** The stored type an operand compares as. */
export type OperandEncoded<V> =
	V extends Expr<infer S, Params> ? S['Encoded'] : V;

/** The bindings an operand reads. */
export type OperandParams<V> = V extends Expr<Codec, infer P> ? P : NoParams;

const lift = (value: Operand): Expr => (isExpr(value) ? value : literal(value));

/**
 * Raw SurrealQL with a declared codec and bindings: the escape hatch for
 * what the typed functions do not cover yet. Nothing checks the text, its
 * bindings or its codec, and the text is used as written, so it must never
 * be built from input.
 */
export const unsafeExpr = <
	S extends Codec,
	P extends Params = NoParams,
>(options: {
	readonly sql: string;
	readonly codec: S;
	readonly params?: P;
	/** Set when the text needs no parentheses as an operand. */
	readonly atomic?: boolean;
}): Expr<S, P> => {
	const params = (options.params ?? {}) as P;
	for (const name of Object.keys(params)) assertBindingName(name);
	return makeExpr(
		options.codec,
		options.sql,
		params,
		options.atomic === true ? Level.atom : Level.raw,
	);
};

/**
 * The right operand `V`, when its stored type `B` compares with the left
 * operand's stored type `A`: one is assignable to the other. Refinements
 * and brands sit on the decoded type, so a branded or narrowed binding
 * still compares with its column, while a string never compares with an
 * int. Otherwise the operand's expected type is a message saying so.
 */
export type Comparable<A, B, V> = [A] extends [B]
	? V
	: [B] extends [A]
		? V
		: 'these operands never compare: their stored types do not overlap';

type BooleanCodec = Schema.Codec<unknown, boolean, never, never>;

const binary =
	(operator: string) =>
	<A extends Codec, PA extends Params, const B extends Operand>(
		left: Expr<A, PA>,
		right: Comparable<A['Encoded'], OperandEncoded<B>, B>,
	): Expr<Schema.Boolean, PA & OperandParams<B>> => {
		const other = lift(right as Operand);
		return makeExpr(
			Schema.Boolean,
			`${operand(left)} ${operator} ${operand(other)}`,
			mergeParams(paramsOf(left), paramsOf(other)) as PA &
				OperandParams<B>,
			Level.comparison,
		);
	};

/** `left = right` */
export const eq = binary('=');
/** `left != right` */
export const ne = binary('!=');
/** `left < right` */
export const lt = binary('<');
/** `left <= right` */
export const lte = binary('<=');
/** `left > right` */
export const gt = binary('>');
/** `left >= right` */
export const gte = binary('>=');

const logical =
	(operator: 'AND' | 'OR') =>
	<
		const A extends readonly [
			Expr<BooleanCodec, Params>,
			...ReadonlyArray<Expr<BooleanCodec, Params>>,
		],
	>(
		...operands: A
	): Expr<Schema.Boolean, ParamsOfAll<A[number]>> => {
		const level = operator === 'AND' ? Level.and : Level.or;
		return makeExpr(
			Schema.Boolean,
			operands.map((item) => conjunct(item, level)).join(` ${operator} `),
			mergeParams(...operands.map(paramsOf)) as ParamsOfAll<A[number]>,
			operands.length === 1 ? operands[0][ExprTypeId].level : level,
		);
	};

/** Every operand holds. */
export const and = logical('AND');
/** At least one operand holds. */
export const or = logical('OR');

/** `!value` */
export const not = <P extends Params>(
	value: Expr<BooleanCodec, P>,
): Expr<Schema.Boolean, P> =>
	makeExpr(Schema.Boolean, `!${operand(value)}`, paramsOf(value), Level.atom);

/** `value = NONE`: the field is absent, or the expression yields nothing. */
export const isNone = <P extends Params>(
	value: Expr<Codec, P>,
): Expr<Schema.Boolean, P> =>
	makeExpr(
		Schema.Boolean,
		`${operand(value)} = NONE`,
		paramsOf(value),
		Level.comparison,
	);

/** `value != NONE` */
export const isNotNone = <P extends Params>(
	value: Expr<Codec, P>,
): Expr<Schema.Boolean, P> =>
	makeExpr(
		Schema.Boolean,
		`${operand(value)} != NONE`,
		paramsOf(value),
		Level.comparison,
	);

/** The element type of a stored array or set. */
export type ElementOf<E> =
	E extends ReadonlyArray<infer I>
		? I
		: E extends ReadonlySet<infer I>
			? I
			: never;

type ListItem = string | number | boolean | bigint | null;

/** An array or set expression, or primitives to inline as an array. */
export type Collection = Expr | ReadonlyArray<ListItem>;

type CollectionEncoded<V> = V extends Expr<infer S, Params> ? S['Encoded'] : V;

/** An inline list: every item goes through its own {@link literal}. */
const liftCollection = (value: Collection): Expr =>
	isExpr(value)
		? value
		: makeExpr(
				Schema.Array(Schema.Unknown),
				`[ ${value.map((item) => sqlOf(literal(item))).join(', ')} ]`,
				{},
				Level.atom,
			);

/** `needle INSIDE haystack`: the value is an element of the array or set. */
export const inside = <
	A extends Codec,
	PA extends Params,
	const B extends Collection,
>(
	needle: Expr<A, PA>,
	haystack: Comparable<A['Encoded'], ElementOf<CollectionEncoded<B>>, B>,
): Expr<Schema.Boolean, PA & OperandParams<B>> => {
	const collection = liftCollection(haystack as Collection);
	return makeExpr(
		Schema.Boolean,
		`${operand(needle)} INSIDE ${operand(collection)}`,
		mergeParams(paramsOf(needle), paramsOf(collection)) as PA &
			OperandParams<B>,
		Level.comparison,
	);
};

/** `haystack CONTAINS needle`: the array or set holds the value. */
export const contains = <
	A extends Codec,
	PA extends Params,
	const B extends Operand,
>(
	haystack: Expr<A, PA>,
	needle: Comparable<ElementOf<A['Encoded']>, OperandEncoded<B>, B>,
): Expr<Schema.Boolean, PA & OperandParams<B>> => {
	const value = lift(needle as Operand);
	return makeExpr(
		Schema.Boolean,
		`${operand(haystack)} CONTAINS ${operand(value)}`,
		mergeParams(paramsOf(haystack), paramsOf(value)) as PA &
			OperandParams<B>,
		Level.comparison,
	);
};

/** The codec an array's element decodes with. */
export type ItemOf<S extends Codec> =
	S['Encoded'] extends ReadonlyArray<unknown>
		? S extends { readonly value: infer I extends Codec }
			? I
			: never
		: never;

/** A codec that also accepts NONE, unless it already does. */
export type OrNone<S extends Codec> = undefined extends S['Type']
	? S
	: Schema.UndefinedOr<S>;

/** @internal */
export const orNone = <S extends Codec>(codec: S): OrNone<S> =>
	(Schema.is(codec)(undefined)
		? codec
		: Schema.UndefinedOr(codec)) as OrNone<S>;

type ArrayOnly<S extends Codec> = [ItemOf<S>] extends [never]
	? {
			readonly 'this expression is not an array, so it has no elements to index': S['Encoded'];
		}
	: unknown;

/**
 * `array[index]`: one element, or NONE past the end. A negative index is
 * not an offset from the end in SurrealQL, so it is refused. An element of
 * a field path is a field path.
 */
export function at<S extends Codec>(
	array: Path<S> & ArrayOnly<S>,
	index: number,
): Path<OrNone<ItemOf<S>>>;
export function at<S extends Codec, P extends Params>(
	array: Expr<S, P> & ArrayOnly<S>,
	index: number,
): Expr<OrNone<ItemOf<S>>, P>;
export function at(array: Expr, index: number): Expr {
	if (!Number.isInteger(index) || index < 0) {
		throw new RangeError(
			`${index} is not an array index: use a non-negative integer`,
		);
	}
	const codec: Codec = codecOf(array);
	if (!('value' in codec) || !Schema.isSchema(codec.value)) {
		throw new TypeError(
			`\`${sqlOf(array)}\` is not an array expression, so it has no elements to index`,
		);
	}
	const element = orNone(codec.value as Codec);
	const sql = `${operand(array)}[${index}]`;
	const path = pathOf(array);
	return path === undefined
		? makeExpr(element, sql, paramsOf(array), Level.atom)
		: makePath(element, sql, path);
}

const casts = {
	bool: Schema.Boolean,
	datetime: DateTime,
	decimal: Decimal,
	duration: Duration,
	float: Schema.Number,
	int: Int,
	number: SurrealNumber,
	string: SurrealString,
	uuid: Uuid,
} as const;

/** The kinds {@link cast} converts to, and the codec each yields. */
export type CastKind = keyof typeof casts;

/**
 * `<kind>value`: SurrealQL's cast. The engine fails the statement when the
 * value cannot be converted.
 */
export const cast = <K extends CastKind, P extends Params>(
	kind: K,
	value: Expr<Codec, P>,
): Expr<(typeof casts)[K], P> =>
	makeExpr(
		casts[kind],
		`<${kind}>${operand(value)}`,
		paramsOf(value),
		Level.atom,
	);

/** A function descriptor whose codecs need no services. */
export type PureFunction = FunctionDescriptor & {
	readonly args: Codec;
	readonly returns: Codec;
};

type ArgumentOf<E> =
	| Expr<Schema.Codec<unknown, E, never, never>, Params>
	| Extract<E, Primitive>;

type ArgumentsOf<T extends ReadonlyArray<unknown>> = {
	readonly [I in keyof T]: ArgumentOf<T[I]>;
};

/** The arguments a generated function accepts, as expressions. */
export type CallArguments<F extends PureFunction> =
	F['args']['Encoded'] extends infer T extends ReadonlyArray<unknown>
		? ArgumentsOf<T>
		: never;

type Ordinals = readonly [
	'first',
	'second',
	'third',
	'fourth',
	'fifth',
	'sixth',
	'seventh',
	'eighth',
	'ninth',
	'tenth',
];

type OrdinalOf<I> = I extends keyof Ordinals ? Ordinals[I] : 'next';

/**
 * The function `F`, or a message when `A` leaves out an argument it needs.
 * An argument past the last is refused where it is ({@link CheckedArguments}).
 */
export type CallTarget<
	F extends PureFunction,
	A extends ReadonlyArray<unknown>,
> = A['length'] extends CallArguments<F>['length']
	? F
	: [Exclude<keyof A & `${number}`, keyof CallArguments<F>>] extends [never]
		? `\`${F['name']}\` needs more than ${A['length']} ${A['length'] extends 1 ? 'argument' : 'arguments'}`
		: F;

/**
 * The arguments `A` as the caller wrote them, each checked against the
 * parameter in its position: one that does not fit, or one past the last
 * parameter, is typed as a message naming the position.
 */
export type CheckedArguments<
	F extends PureFunction,
	A extends ReadonlyArray<unknown>,
> = {
	readonly [I in keyof A]: I extends keyof CallArguments<F>
		? A[I] extends CallArguments<F>[I]
			? A[I]
			: `\`${F['name']}\` does not accept this as its ${OrdinalOf<I>} argument`
		: `\`${F['name']}\` takes no ${OrdinalOf<I>} argument`;
};

/**
 * Calls a `DEFINE FUNCTION` through its generated descriptor: the arguments
 * are checked against the declared parameter kinds, trailing optional ones
 * may be left out, and the result decodes with the declared return kind.
 */
export const call = <
	F extends PureFunction,
	const A extends ReadonlyArray<unknown>,
>(
	fn: CallTarget<F, A>,
	...args: CheckedArguments<F, A>
): Expr<F['returns'], ParamsOfAll<A[number]>> => {
	const values = (args as ReadonlyArray<Operand>).map(lift);
	const target = fn as F;
	return makeExpr(
		target.returns as F['returns'],
		`${target.name}(${values.map(sqlOf).join(', ')})`,
		mergeParams(...values.map(paramsOf)) as ParamsOfAll<A[number]>,
		Level.atom,
	);
};
