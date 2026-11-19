/**
 * What the generated built-in functions share: the argument types each
 * argument kind accepts, the codecs of the open kinds, and the one builder
 * every generated function calls.
 *
 * An argument is an expression whose stored (encoded) type fits the kind,
 * or a primitive (or an array of primitives) inlined with `literal`, so every
 * value still enters as a typed binding or a validated constant.
 */
import { Schema } from 'effect';
import type {
	RecordId,
	DateTime as SdkDateTime,
	Decimal as SdkDecimal,
	Duration as SdkDuration,
	Geometry as SdkGeometry,
	Range as SdkRange,
	Uuid as SdkUuid,
} from 'surrealdb';
import { Float as FloatCodec } from '../scalars.js';
import {
	FloatOrDecimal,
	type FloatOrDecimalOf,
	holdsDecimal,
} from '../statement/arithmetic.js';
import {
	codecOf,
	derive,
	type Expr,
	isExpr,
	Level,
	literal,
	makeExpr,
	type OrNone,
	operand,
	orNone,
	type Primitive,
	sqlOf,
} from '../statement/expr.js';
import type { Codec, NoParams, Params } from '../statement/statement.js';
import { SurrealValue } from '../value.js';

/** `any`: any value, or NONE. */
export const AnyCodec = /*#__PURE__*/ Schema.UndefinedOr(SurrealValue);

/** `object`: an open object. */
export const ObjectCodec = /*#__PURE__*/ Schema.Record(
	Schema.String,
	SurrealValue,
);

/** `array`: an array of any values. */
export const ArrayCodec = /*#__PURE__*/ Schema.Array(AnyCodec);

/**
 * The stored type an argument of each kind accepts. `int` and `number`
 * accept a `bigint` too: the engine reads an int64 as an int.
 */
export interface In {
	readonly any: unknown;
	readonly string: string;
	readonly int: number | bigint;
	readonly int64: number | bigint;
	readonly float: number;
	readonly number: number | bigint | SdkDecimal;
	readonly decimal: SdkDecimal;
	readonly bool: boolean;
	readonly datetime: SdkDateTime;
	readonly duration: SdkDuration;
	readonly uuid: SdkUuid;
	readonly bytes: Uint8Array | ArrayBuffer;
	readonly record: RecordId;
	readonly object: { readonly [key: string]: unknown };
	readonly geometry: SdkGeometry;
	readonly range: SdkRange<unknown, unknown>;
	readonly array: ReadonlyArray<unknown>;
}

type InlineItem<I> = unknown extends I ? Primitive : Extract<I, Primitive>;

/** The constants an argument of stored type `E` may be written as. */
export type Inline<E> = unknown extends E
	? Primitive | ReadonlyArray<Primitive>
	:
			| Extract<E, Primitive>
			| (E extends ReadonlyArray<infer I>
					? ReadonlyArray<InlineItem<I>>
					: never);

/**
 * An argument whose stored type is `E`: an expression whose codec encodes
 * to it, or a constant to inline.
 */
export type Arg<E> =
	| Expr<Schema.Codec<unknown, E, never, never>, Params>
	| Inline<E>;

/** The codec of argument `A`, or `D` when it is inlined. */
export type CodecOfArg<A, D extends Codec> =
	A extends Expr<infer S, Params> ? S : D;

/** The codec of an element of an array codec, or NONE; `any` if unknown. */
export type ItemCodec<S extends Codec> =
	S['Encoded'] extends ReadonlyArray<unknown>
		? S extends { readonly value: infer I extends Codec }
			? OrNone<I>
			: typeof AnyCodec
		: typeof AnyCodec;

/** A result codec derived from the arguments as given. */
class Derived {
	constructor(readonly derive: (args: ReadonlyArray<unknown>) => Codec) {}
}

/** A result codec, or how to derive it from the arguments. */
export type ResultOf = Codec | Derived;

const argCodec = (
	args: ReadonlyArray<unknown>,
	index: number,
	fallback: Codec,
): Codec => {
	const arg = args[index];
	return isExpr(arg) ? codecOf(arg) : fallback;
};

/** The codec of argument `index`, or `fallback` when it is inlined. */
export const same = (index: number, fallback: Codec): ResultOf =>
	new Derived((args) => argCodec(args, index, fallback));

/** The codec of an element of argument `index` (an array), or NONE. */
export const item = (index: number, fallback: Codec): ResultOf =>
	new Derived((args) => {
		const codec = argCodec(args, index, fallback);
		return 'value' in codec && Schema.isSchema(codec.value)
			? orNone(codec.value as Codec)
			: AnyCodec;
	});

/** The stored type argument `A` holds: its codec's, or the constant's own. */
type EncodedOfArg<A> = A extends Expr<infer S, Params> ? S['Encoded'] : A;

/**
 * The codec of a `float|decimal:N` result over the arguments it names, `A`
 * (a union of their types): a float, or a float or a decimal when one of
 * them may hold a decimal.
 */
export type FloatOrDecimalCodec<A> = FloatOrDecimalOf<EncodedOfArg<A>>;

const mayHoldDecimal = (arg: unknown): boolean =>
	isExpr(arg)
		? holdsDecimal(codecOf(arg))
		: Array.isArray(arg) && arg.some(mayHoldDecimal);

/**
 * A float, or a float or a decimal when one of the arguments at `indices`
 * may hold a decimal (`math::sqrt`, `math::mean`): SurrealDB computes those
 * in the kind of their input.
 */
export const floatOrDecimal = (indices: ReadonlyArray<number>): ResultOf =>
	new Derived((args) =>
		indices.some((index) => mayHoldDecimal(args[index]))
			? (FloatOrDecimal as unknown as Codec)
			: FloatCodec,
	);

const lift = (value: unknown): Expr => {
	if (isExpr(value)) return value;
	if (Array.isArray(value)) {
		const items = value.map((element) => sqlOf(lift(element)));
		return makeExpr(ArrayCodec, `[ ${items.join(', ')} ]`, {}, Level.atom);
	}
	return literal(value as Primitive);
};

/**
 * `name(args…, rest…)`. Trailing optional arguments left out (or given as
 * `undefined`) are not written; an optional one before a given one is NONE.
 */
export const apply = <R>(
	name: string,
	result: ResultOf,
	required: number,
	args: ReadonlyArray<unknown>,
	rest: ReadonlyArray<unknown> = [],
): R => {
	let count = args.length;
	while (count > required && args[count - 1] === undefined) count--;
	const given = [...args.slice(0, count), ...rest];
	const values = given.map(lift);
	return derive(
		result instanceof Derived ? result.derive(given) : result,
		`${name}(${values.map(operand).join(', ')})`,
		Level.atom,
		values,
	) as R;
};

/** A constant, written without parentheses (`math::pi`). */
export const constant = <S extends Codec>(
	name: string,
	codec: S,
): Expr<S, NoParams> => makeExpr(codec, name, {}, Level.atom);
