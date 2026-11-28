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

/**
 * The element codec of argument `A` when it is an expression typed as an
 * array of a closed element type; `never` for an inlined constant, an open
 * value or an array of open values (any `SurrealValue`).
 */
type TypedItem<A> = A extends Expr<infer S, Params> ? ClosedItem<S> : never;

/** `true` when codec `S` may hold any value (`unknown` or a `SurrealValue`). */
type IsOpen<S extends Codec> = unknown extends S['Type']
	? true
	: SurrealValue extends S['Type']
		? true
		: false;

/** The element codec of array codec `S` when it is closed; else `never`. */
type ClosedItem<S> = S extends Codec
	? S['Encoded'] extends ReadonlyArray<unknown>
		? S extends { readonly value: infer I extends Codec }
			? IsOpen<I> extends true
				? never
				: I
			: never
		: never
	: never;

/** The codec of argument `A` when it is a closed expression; else `never`. */
type TypedValue<A> =
	A extends Expr<infer S, Params>
		? IsOpen<S> extends true
			? never
			: S
		: never;

/** `R`, or `D` when one of the codecs `T` it is built from is `never`. */
type Built<T extends ReadonlyArray<unknown>, R, D> = true extends {
	[K in keyof T]: [T[K]] extends [never] ? true : false;
}[number]
	? D
	: R;

/** `elements:N,M`: an array of the union of the arguments' element codecs. */
export type ElementsCodec<A extends ReadonlyArray<unknown>, D extends Codec> = [
	SomeOpen<A[number]>,
] extends [never]
	? Schema.$Array<TypedItem<A[number]>>
	: D;

/** `with:N,M`: the elements of array `N` and the value `M` itself. */
export type WithCodec<
	A extends readonly [unknown, unknown],
	D extends Codec,
> = Built<
	[TypedItem<A[0]>, TypedValue<A[1]>],
	Schema.$Array<TypedItem<A[0]> | TypedValue<A[1]>>,
	D
>;

/** The elements value `V` adds: its own for an array, else `V` itself. */
type Added<V> =
	TypedValue<V> extends infer S extends Codec
		? S['Encoded'] extends ReadonlyArray<unknown>
			? ClosedItem<S>
			: [Extract<S['Encoded'], ReadonlyArray<unknown>>] extends [never]
				? S
				: never
		: never;

/** `adds:N,M`: the elements of array `N` and those value `M` adds. */
export type AddsCodec<
	A extends readonly [unknown, unknown],
	D extends Codec,
> = Built<
	[TypedItem<A[0]>, Added<A[1]>],
	Schema.$Array<TypedItem<A[0]> | Added<A[1]>>,
	D
>;

/** `padded:N,M`: an element of either array, or NULL past the shorter. */
export type PaddedCodec<
	A extends readonly [unknown, unknown],
	D extends Codec,
> = Built<
	[TypedItem<A[0]>, TypedItem<A[1]>],
	Schema.$Array<TypedItem<A[0]> | TypedItem<A[1]> | Schema.Null>,
	D
>;

/** `pairs:N,M`: every pair of an element of `N` and one of `M`. */
export type PairsCodec<
	A extends readonly [unknown, unknown],
	D extends Codec,
> = Built<
	[TypedItem<A[0]>, TypedItem<A[1]>],
	Schema.$Array<Schema.Tuple<readonly [TypedItem<A[0]>, TypedItem<A[1]>]>>,
	D
>;

/** `flat:N`: the elements of the arrays array `N` holds. */
export type FlatCodec<A extends readonly [unknown], D extends Codec> = Built<
	[ClosedItem<TypedItem<A[0]>>],
	Schema.$Array<ClosedItem<TypedItem<A[0]>>>,
	D
>;

/** `chunks:N`: arrays of the elements of array `N`. */
export type ChunksCodec<A extends readonly [unknown], D extends Codec> = Built<
	[TypedItem<A[0]>],
	Schema.$Array<Schema.$Array<TypedItem<A[0]>>>,
	D
>;

/** `transposed:N`: arrays of the inner elements of `N`, or NONE (ragged). */
export type TransposedCodec<
	A extends readonly [unknown],
	D extends Codec,
> = Built<
	[ClosedItem<TypedItem<A[0]>>],
	Schema.$Array<Schema.$Array<OrNone<ClosedItem<TypedItem<A[0]>>>>>,
	D
>;

/** `repeats:N`: an array of the value `N`. */
export type RepeatsCodec<A extends readonly [unknown], D extends Codec> = Built<
	[TypedValue<A[0]>],
	Schema.$Array<TypedValue<A[0]>>,
	D
>;

/** `true` when some member of the union `A` is not a typed array. */
type SomeOpen<A> = A extends unknown
	? [TypedItem<A>] extends [never]
		? true
		: never
	: never;

/**
 * The codec of an `items:N` result over the variadic arguments `A`: the
 * argument's own codec when it is the only one, an array of the union of
 * the element codecs when every argument is a typed array, else `D`.
 */
export type ItemsCodec<A extends ReadonlyArray<unknown>, D extends Codec> = [
	A[number],
] extends [never]
	? D
	: [SomeOpen<A[number]>] extends [never]
		? A extends readonly [infer Only]
			? CodecOfArg<Only, D>
			: Schema.$Array<TypedItem<A[number]>>
		: D;

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

/** Whether a codec may hold any value (`unknown` or a `SurrealValue`). */
const isOpen = ({ ast }: Codec): boolean =>
	ast._tag === 'Unknown' ||
	ast._tag === 'Any' ||
	ast === SurrealValue.ast ||
	(ast._tag === 'Union' && ast.types.includes(SurrealValue.ast));

/** The element codec of a typed array codec; `undefined` when open. */
const typedItem = (codec: Codec): Codec | undefined => {
	if (codec.ast._tag !== 'Arrays' || !('value' in codec)) return undefined;
	const element = codec.value;
	if (!Schema.isSchema(element)) return undefined;
	return isOpen(element as Codec) ? undefined : (element as Codec);
};

/**
 * An array of the elements of every argument from `index` on (`array::concat`):
 * the argument's own codec when it is the only one, an array of the union of
 * the element codecs when every argument is a typed array, else `fallback`.
 */
export const items = (index: number, fallback: Codec): ResultOf =>
	new Derived((args) => {
		const given = args.slice(index);
		const elements: Array<Codec> = [];
		for (const arg of given) {
			const element = isExpr(arg) ? typedItem(codecOf(arg)) : undefined;
			if (element === undefined) return fallback;
			if (!elements.includes(element)) elements.push(element);
		}
		const [only, ...others] = elements;
		if (only === undefined) return fallback;
		if (given.length === 1) return codecOf(given[0] as Expr);
		return Schema.Array(
			others.length === 0 ? only : Schema.Union(elements),
		) as unknown as Codec;
	});

/** The element codec of an argument typed as a closed array, if it is one. */
const elementOf = (arg: unknown): Codec | undefined =>
	isExpr(arg) ? typedItem(codecOf(arg)) : undefined;

/** The codec of an argument that is a closed expression, if it is one. */
const typedValue = (arg: unknown): Codec | undefined =>
	isExpr(arg) && !isOpen(codecOf(arg)) ? codecOf(arg) : undefined;

/** One codec for every value of the given ones: the only one, or a union. */
const unionOf = (codecs: ReadonlyArray<Codec>): Codec => {
	const distinct = [...new Set(codecs)];
	const [only] = distinct;
	return (
		distinct.length === 1 && only !== undefined
			? only
			: Schema.Union(distinct)
	) as Codec;
};

const arrayOf = (codec: Codec): Codec =>
	Schema.Array(codec) as unknown as Codec;

/**
 * A result built from the arguments at `indices` by `build`, or `fallback`
 * when `build` finds one of them untyped (it returns `undefined`).
 */
const shaped =
	(
		build: (args: ReadonlyArray<unknown>) => Codec | undefined,
	): ((indices: ReadonlyArray<number>, fallback: Codec) => ResultOf) =>
	(indices, fallback) =>
		new Derived(
			(args) => build(indices.map((index) => args[index])) ?? fallback,
		);

/** Every codec given, or `undefined` when one is missing. */
const all = (
	codecs: ReadonlyArray<Codec | undefined>,
): ReadonlyArray<Codec> | undefined =>
	codecs.every((codec) => codec !== undefined) ? codecs : undefined;

/** `elements:N,M` (`array::union`): an array of the arguments' elements. */
export const elementsOf = shaped((args) => {
	const elements = all(args.map(elementOf));
	return elements && arrayOf(unionOf(elements));
});

/** `with:N,M` (`array::append`): the elements of `N` and the value `M`. */
export const withValue = shaped(([array, value]) => {
	const codecs = all([elementOf(array), typedValue(value)]);
	return codecs && arrayOf(unionOf(codecs));
});

/** The elements value `codec` adds: its own for an array, else itself. */
const added = (codec: Codec | undefined): Codec | undefined => {
	if (codec === undefined) return undefined;
	if (codec.ast._tag === 'Arrays') return typedItem(codec);
	const mayBeArray =
		codec.ast._tag === 'Union' &&
		codec.ast.types.some((member) => member._tag === 'Arrays');
	return mayBeArray ? undefined : codec;
};

/**
 * `adds:N,M` (`array::add`): the elements of `N` and those `M` adds, which
 * are its elements when it is an array.
 */
export const addsValue = shaped(([array, value]) => {
	const codecs = all([elementOf(array), added(typedValue(value))]);
	return codecs && arrayOf(unionOf(codecs));
});

/**
 * `padded:N,M` (`array::logical_and`): an element of either array, or NULL
 * where the shorter one has none.
 */
export const nullPadded = shaped(([left, right]) => {
	const codecs = all([elementOf(left), elementOf(right)]);
	return codecs && arrayOf(unionOf([...codecs, Schema.Null as Codec]));
});

/** `pairs:N,M` (`array::combine`): pairs of an element of each array. */
export const pairsOf = shaped(([left, right]) => {
	const codecs = all([elementOf(left), elementOf(right)]);
	return codecs && arrayOf(Schema.Tuple(codecs) as unknown as Codec);
});

/** The closed inner element codec of an array of closed arrays, if any. */
const innerOf = (arg: unknown): Codec | undefined => {
	const element = elementOf(arg);
	return element && typedItem(element);
};

/** `flat:N` (`array::flatten`): the elements of the arrays `N` holds. */
export const flattened = shaped(([array]) => {
	const inner = innerOf(array);
	return inner && arrayOf(inner);
});

/** `chunks:N` (`array::windows`): arrays of the elements of `N`. */
export const chunksOf = shaped(([array]) => {
	const element = elementOf(array);
	return element && arrayOf(arrayOf(element));
});

/**
 * `transposed:N` (`array::transpose`): arrays of the inner elements of `N`,
 * each of which may be NONE where an inner array is shorter.
 */
export const transposed = shaped(([array]) => {
	const inner = innerOf(array);
	return inner && arrayOf(arrayOf(orNone(inner)));
});

/** `repeats:N` (`array::repeat`): an array of the value `N`. */
export const repeated = shaped(([value]) => {
	const codec = typedValue(value);
	return codec && arrayOf(codec);
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
