import { Schema, SchemaAST } from 'effect';
import * as Surreal from 'surrealdb';

import { Int } from '../int.js';
import { Float, SurrealString } from '../scalars.js';
import { DateTime, Duration } from '../temporal.js';

// Over cbor.2, which SDK 2.2.2 negotiates, SurrealDB 4 returns dates, times,
// timestamps, intervals and sized numbers as their own SDK classes. Over
// cbor, and from the embedded engine, the same results arrive as a datetime,
// duration, string, float or int. Each codec here converts the new value to
// the old one and is otherwise its base codec: the same type, annotations,
// errors and encoding. The generated functions therefore keep their result
// types. Raw queries still return the SDK values.

/** An instance of an SDK class that older supported SDKs do not export. */
const isSdkValue = <T>(input: unknown, name: string): input is T => {
	const sdkClass: unknown = Reflect.get(Surreal, name);
	return typeof sdkClass === 'function' && input instanceof sdkClass;
};

/** `codec`, accepting `convert(input)` when decoding. */
const accepting = <
	S extends Schema.Top & { readonly ast: SchemaAST.Declaration },
>(
	codec: S,
	convert: (input: unknown) => unknown,
): S => {
	const { ast } = codec;
	return Schema.make<S>(
		new SchemaAST.Declaration(
			ast.typeParameters,
			(typeParameters) => {
				const run = ast.run(typeParameters);
				return (input, self, options) =>
					run(convert(input), self, options);
			},
			ast.annotations,
			ast.checks,
			ast.encoding,
			ast.context,
			ast.encodingChecks,
			ast.encodingRun ?? ast.run,
		),
	);
};

interface Compact<T> {
	toCompact(): T;
}

// The engine's datetime bounds, which cbor sends for an infinite date.
const infiniteDate = (positive: boolean) => {
	const timestamp = Reflect.get(Surreal, 'Timestamp') as unknown as {
		readonly INFINITY: Compact<[number, number]>;
		readonly NEG_INFINITY: Compact<[number, number]>;
	};
	return (positive ? timestamp.INFINITY : timestamp.NEG_INFINITY).toCompact();
};

/** `time::make_date`, `type::date`: a `PlainDate` becomes midnight UTC. */
export const DateResult = /*#__PURE__*/ accepting(DateTime, (input) => {
	if (
		!isSdkValue<{ readonly days: number; readonly isInfinite: boolean }>(
			input,
			'PlainDate',
		)
	)
		return input;
	return new Surreal.DateTime(
		input.isInfinite
			? infiniteDate(input.days > 0)
			: [BigInt(input.days) * 86_400n, 0n],
	);
});

/** `time::make_time`, `type::time`: a `PlainTime` becomes its duration since midnight. */
export const TimeResult = /*#__PURE__*/ accepting(Duration, (input) => {
	if (!isSdkValue<{ readonly nanos: number }>(input, 'PlainTime'))
		return input;
	const nanos = BigInt(input.nanos);
	return new Surreal.Duration([
		nanos / 1_000_000_000n,
		nanos % 1_000_000_000n,
	]);
});

/** `time::make_timestamp`, `type::timestamp`: a `Timestamp` becomes a datetime. */
export const TimestampResult = /*#__PURE__*/ accepting(DateTime, (input) =>
	isSdkValue<Compact<[number, number]>>(input, 'Timestamp')
		? new Surreal.DateTime(input.toCompact())
		: input,
);

/** `time::age`, `type::interval`: an `Interval` becomes the engine's ISO 8601 text. */
export const IntervalResult = /*#__PURE__*/ accepting(SurrealString, (input) =>
	isSdkValue<{ toString(): string }>(input, 'Interval')
		? input.toString()
		: input,
);

interface SizedNumber {
	readonly value: number | bigint;
	readonly kind: string;
}

const sized = (input: unknown, kinds: ReadonlyArray<string>) =>
	isSdkValue<SizedNumber>(input, 'SizedNumber') && kinds.includes(input.kind)
		? input
		: undefined;

/** `type::f16`, `type::f32`: the float. */
export const SizedFloatResult = /*#__PURE__*/ accepting(
	Float,
	(input) => sized(input, ['f16', 'f32'])?.value ?? input,
);

/**
 * `type::i8` to `type::u64`: the integer. A `u64` arrives as a bigint; one
 * in the safe range becomes a number, and `Int` refuses the rest as before.
 */
export const SizedIntResult = /*#__PURE__*/ accepting(Int, (input) => {
	const value = sized(input, [
		'i8',
		'i16',
		'i32',
		'u8',
		'u16',
		'u32',
		'u64',
	])?.value;
	if (value === undefined) return input;
	return typeof value === 'bigint' &&
		value >= BigInt(Number.MIN_SAFE_INTEGER) &&
		value <= BigInt(Number.MAX_SAFE_INTEGER)
		? Number(value)
		: value;
});
