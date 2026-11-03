/**
 * SurrealQL `int` is a signed 64-bit integer. The SDK's CBOR decoder hands it
 * back as a JS `number` when it fits and as a `bigint` otherwise, with one
 * lossy edge (measured against surrealdb.js 2.1.1 and SurrealDB 3.2.4):
 *
 * - `0 … 2^53` decode as exact numbers; `2^53 + 1 … 2^63 - 1` as bigints.
 * - `-1 … -2^53` decode as exact numbers; `-(2^53 + 2) … -2^63` as bigints.
 * - `-(2^53 + 1)` decodes as the number `-2^53`: already rounded.
 * - Encoding a bigint outside int64 is not rejected by the SDK; the server
 *   wraps it (`2^63` arrives as `-2^63`).
 *
 * Two profiles follow from that:
 *
 * - `Int` (`int: "safe-number"`, the default): a `number` that is a safe
 *   integer. Anything beyond `±(2^53 - 1)` fails loudly in both directions.
 * - `Int64` (`int: "bigint"`): a branded `bigint` checked against the int64
 *   range in both directions. Numbers are accepted only where the decoder is
 *   exact, so `-2^53` (ambiguous with a rounded `-(2^53 + 1)`) fails loudly.
 */
import { type Brand, Schema, SchemaGetter } from 'effect';

import { runtimeCode } from './internal/code.js';
import {
	constrained,
	invalid,
	leaf,
	ok,
	type Parsed,
	wrongType,
} from './internal/declare.js';

export const INT64_MIN = -(2n ** 63n);
export const INT64_MAX = 2n ** 63n - 1n;

/** The integer profile of a generated module or field. */
export type IntProfile = 'safe-number' | 'bigint';

const beyondSafe = (value: number | bigint): string =>
	`integer ${value} is outside the safe JavaScript range ±(2^53 - 1); SurrealDB int is 64-bit, so use the "bigint" int profile (Int64) for this field`;

const parseSafeInt = (input: unknown): Parsed<number> => {
	if (typeof input === 'number') {
		if (Number.isSafeInteger(input)) return ok(input);
		if (Number.isInteger(input)) return invalid(beyondSafe(input));
		return invalid(`${input} is not an integer`);
	}
	if (typeof input === 'bigint') return invalid(beyondSafe(input));
	return wrongType;
};

/**
 * `int`, `int: "safe-number"` profile: a JS `number` that must be a safe
 * integer. Values beyond `±(2^53 - 1)` fail instead of losing precision.
 */
export const Int = leaf<number>({
	id: 'effect-surrealkit/Int',
	expected: 'a safe integer (SurrealDB int)',
	code: runtimeCode('Int', 'number'),
	parse: parseSafeInt,
	arbitrary: (constraint) =>
		Schema.link<number>()(
			constrained(
				Schema.Number.check(
					Schema.isInt(),
					Schema.isBetween({
						minimum: Number.MIN_SAFE_INTEGER,
						maximum: Number.MAX_SAFE_INTEGER,
					}),
				),
				constraint,
			),
			{
				decode: SchemaGetter.transform((n) => n),
				encode: SchemaGetter.transform((n) => n),
			},
		),
});
export type Int = typeof Int.Type;

/** A signed 64-bit integer. */
export type Int64 = bigint & Brand.Brand<'Int64'>;

const outsideInt64 = (value: bigint): string =>
	`integer ${value} is outside the SurrealDB int range [-2^63, 2^63 - 1]`;

/**
 * Checks a bigint against the int64 range and brands it.
 *
 * @throws RangeError when the value is outside int64.
 */
export const int64 = (value: bigint): Int64 => {
	if (value < INT64_MIN || value > INT64_MAX) {
		throw new RangeError(outsideInt64(value));
	}
	return value as Int64;
};

const parseInt64 = (input: unknown): Parsed<Int64> => {
	if (typeof input === 'bigint') {
		if (input < INT64_MIN || input > INT64_MAX) {
			return invalid(outsideInt64(input));
		}
		return ok(input as Int64);
	}
	if (typeof input === 'number') {
		if (!Number.isInteger(input))
			return invalid(`${input} is not an integer`);
		// The SDK decodes 2^53 exactly; -2^53 may be a rounded -(2^53 + 1).
		if (Number.isSafeInteger(input) || input === 2 ** 53) {
			return ok(BigInt(input) as Int64);
		}
		return invalid(
			`integer ${input} arrived as a JS number beyond ±(2^53 - 1), so its exact value is unknown (surrealdb.js 2.1.1 decodes -(2^53 + 1) as -2^53); select it as a bigint-producing value or use a string projection`,
		);
	}
	return wrongType;
};

/**
 * `int`, `int: "bigint"` profile: a branded signed 64-bit `bigint`. Decodes
 * the SDK's `number | bigint`, always encodes a `bigint` within int64.
 */
export const Int64 = leaf<Int64, number | bigint>({
	id: 'effect-surrealkit/Int64',
	expected: 'a signed 64-bit integer (SurrealDB int)',
	code: runtimeCode('Int64', 'SurrealSchema.Int64'),
	parse: parseInt64,
	arbitrary: (constraint) =>
		Schema.link<Int64>()(
			constrained(
				Schema.BigInt.check(
					Schema.isBetweenBigInt({
						minimum: INT64_MIN,
						maximum: INT64_MAX,
					}),
				),
				constraint,
			),
			{
				decode: SchemaGetter.transform((n) => n as Int64),
				encode: SchemaGetter.transform((n) => n),
			},
		),
});

/**
 * An `int` literal kind under the `bigint` profile, e.g. `-5` in
 * `-5 | 0 | 1`. Accepts the SDK's `number | bigint` spelling of exactly that
 * value. (Under the `safe-number` profile, use `Schema.Literal(-5)`.)
 */
export const Int64Literal = <const V extends string>(value: V) => {
	const expectedValue = int64(BigInt(value));
	return leaf<Int64, number | bigint>({
		id: 'effect-surrealkit/Int64Literal',
		payload: value,
		expected: `the int ${value}`,
		code: runtimeCode(
			`Int64Literal(${JSON.stringify(value)})`,
			'SurrealSchema.Int64',
		),
		parse: (input) => {
			const parsed = parseInt64(input);
			if (!parsed.ok) return parsed;
			return parsed.value === expectedValue ? parsed : wrongType;
		},
		arbitrary: () =>
			Schema.link<Int64>()(Schema.Literal(value), {
				decode: SchemaGetter.transform(() => expectedValue),
				encode: SchemaGetter.transform(() => value),
			}),
	});
};

/** Picks the integer codec for a profile. */
export const intFor = (profile: IntProfile) =>
	profile === 'bigint' ? Int64 : Int;
