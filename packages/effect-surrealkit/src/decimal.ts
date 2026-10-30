/**
 * `decimal` and the `number` kind.
 *
 * SurrealDB decimals are 96-bit-mantissa decimals with at most 28 fractional
 * digits (measured on 3.2.4): a larger mantissa is a server error the SDK
 * never resolves (the query hangs), and extra fractional digits are rounded
 * away silently (`0.00000000000000000000000000001` reads back as `0`). The
 * codec rejects both before a value reaches the wire.
 *
 * The SDK's `Decimal` string constructor is permissive (`new Decimal("abc")`
 * is zero, and `new Decimal("+1.5")` is 0.5), so text is never handed to it:
 * {@link decimalFromString} parses the sign, digits and exponent itself and
 * builds the SDK value from exact integers. A decimal never passes through a
 * JS number.
 */
import { Schema, SchemaGetter } from 'effect';
import { equals, Decimal as SdkDecimal } from 'surrealdb';
import { INT64_MAX, INT64_MIN } from './int.js';
import { quote, runtimeCode } from './internal/code.js';
import {
	invalid,
	leaf,
	ok,
	type Parsed,
	wrongType,
} from './internal/declare.js';

/** Largest decimal mantissa: `2^96 - 1`. */
export const DECIMAL_MAX_MANTISSA = 2n ** 96n - 1n;
/** Most fractional digits a decimal keeps. */
export const DECIMAL_MAX_SCALE = 28;

/** Sign, integer digits, fraction digits, exponent. */
const DECIMAL_GRAMMAR = /^([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?$/;
/** The most significant digits a decimal within range can have (`2^96 - 1` has 29). */
const DECIMAL_MAX_DIGITS = DECIMAL_MAX_MANTISSA.toString().length;
const CANONICAL = /^(-?)(\d+)(?:\.(\d+))?$/;

const decimalRangeError = (value: SdkDecimal): string | undefined => {
	const text = value.toString();
	const match = CANONICAL.exec(text);
	if (match === null) return `decimal ${text} is not a plain decimal`;
	const fraction = match[3] ?? '';
	if (fraction.length > DECIMAL_MAX_SCALE) {
		return `decimal ${text} has ${fraction.length} fractional digits; SurrealDB keeps at most ${DECIMAL_MAX_SCALE} and would round the rest away`;
	}
	if (BigInt(`${match[2]}${fraction}`) > DECIMAL_MAX_MANTISSA) {
		return `decimal ${text} exceeds the SurrealDB decimal range (96-bit mantissa)`;
	}
	return undefined;
};

const parseDecimal = (input: unknown): Parsed<SdkDecimal> => {
	if (!(input instanceof SdkDecimal)) return wrongType;
	const error = decimalRangeError(input);
	return error === undefined ? ok(input) : invalid(error);
};

/**
 * Parses decimal text (`-1.50`, `+2`, `1e10`, `.5e1`) into an SDK `Decimal`.
 * The sign, digits and exponent are read here and the value is built from
 * exact integers (`new Decimal([int, frac, scale])`), never from the SDK's
 * permissive string parser; then the SurrealDB range is checked.
 *
 * @throws RangeError for malformed or out-of-range text.
 */
export const decimalFromString = (text: string): SdkDecimal => {
	const match = DECIMAL_GRAMMAR.exec(text);
	const intDigits = match?.[2] ?? '';
	const fracDigits = match?.[3] ?? '';
	if (match === null || (intDigits === '' && fracDigits === '')) {
		throw new RangeError(`${quote(text)} is not a decimal number`);
	}
	const negative = match[1] === '-';
	const exponent = Number(match[4] ?? '0');
	const outOfRange = () =>
		new RangeError(
			`decimal ${quote(text)} exceeds the SurrealDB decimal range (96-bit mantissa, at most ${DECIMAL_MAX_SCALE} fractional digits)`,
		);
	if (!Number.isSafeInteger(exponent)) throw outOfRange();
	// value = digits * 10^-scale, with no leading zeros and no trailing zero
	// after the point.
	let digits = `${intDigits}${fracDigits}`.replace(/^0+/, '');
	let scale = fracDigits.length - exponent;
	if (digits === '') return new SdkDecimal([0n, 0n, 0]);
	while (scale > 0 && digits.endsWith('0')) {
		digits = digits.slice(0, -1);
		scale -= 1;
	}
	if (scale > DECIMAL_MAX_SCALE) {
		throw new RangeError(
			`decimal ${quote(text)} has ${scale} fractional digits; SurrealDB keeps at most ${DECIMAL_MAX_SCALE} and would round the rest away`,
		);
	}
	if (digits.length - Math.min(scale, 0) > DECIMAL_MAX_DIGITS)
		throw outOfRange();
	if (scale < 0) {
		digits = `${digits}${'0'.repeat(-scale)}`;
		scale = 0;
	}
	const mantissa = BigInt(digits);
	if (mantissa > DECIMAL_MAX_MANTISSA) throw outOfRange();
	const unit = 10n ** BigInt(scale);
	const sign = negative ? -1n : 1n;
	const value = new SdkDecimal([
		sign * (mantissa / unit),
		sign * (mantissa % unit),
		scale,
	]);
	const error = decimalRangeError(value);
	if (error !== undefined) throw new RangeError(error);
	return value;
};

const decimalArbitrary = () =>
	Schema.link<SdkDecimal>()(
		Schema.Tuple([
			Schema.BigInt.check(
				Schema.isBetweenBigInt({
					minimum: -DECIMAL_MAX_MANTISSA,
					maximum: DECIMAL_MAX_MANTISSA,
				}),
			),
			Schema.Number.check(
				Schema.isInt(),
				Schema.isBetween({ minimum: 0, maximum: DECIMAL_MAX_SCALE }),
			),
		]),
		{
			decode: SchemaGetter.transform(([mantissa, scale]) => {
				const negative = mantissa < 0n;
				const digits = (negative ? -mantissa : mantissa)
					.toString()
					.padStart(scale + 1, '0');
				const cut = digits.length - scale;
				const text =
					scale === 0
						? digits
						: `${digits.slice(0, cut)}.${digits.slice(cut)}`;
				return new SdkDecimal(`${negative ? '-' : ''}${text}`);
			}),
			encode: SchemaGetter.transform((value) => {
				const match = CANONICAL.exec(value.toString());
				const fraction = match?.[3] ?? '';
				const mantissa = BigInt(
					`${match?.[1] ?? ''}${match?.[2] ?? '0'}${fraction}`,
				);
				return [mantissa, fraction.length] as const;
			}),
		},
	);

/** `decimal`: the SDK `Decimal` class, range-checked; never a JS number. */
export const Decimal = leaf<SdkDecimal>({
	id: 'effect-surrealkit/Decimal',
	expected: 'a SurrealDB Decimal',
	code: runtimeCode('Decimal', 'Surreal.Decimal'),
	parse: parseDecimal,
	equivalence: (self, that) => self.equals(that),
	arbitrary: decimalArbitrary,
});

/** A `decimal` literal kind such as `10dec`: exactly that value. */
export const DecimalLiteral = <const V extends string>(value: V) => {
	const expected = decimalFromString(value);
	return leaf<SdkDecimal>({
		id: 'effect-surrealkit/DecimalLiteral',
		payload: value,
		expected: `the decimal ${value}`,
		code: runtimeCode(`DecimalLiteral(${quote(value)})`, 'Surreal.Decimal'),
		parse: (input) => {
			const parsed = parseDecimal(input);
			if (!parsed.ok) return parsed;
			return parsed.value.equals(expected) ? parsed : wrongType;
		},
		equivalence: (self, that) => self.equals(that),
		arbitrary: () =>
			Schema.link<SdkDecimal>()(Schema.Literal(value), {
				decode: SchemaGetter.transform(() => decimalFromString(value)),
				encode: SchemaGetter.transform(() => value),
			}),
	});
};

/**
 * The `number` kind: any SurrealQL number. The SDK returns ints as
 * `number | bigint`, floats as `number` and decimals as `Decimal`; the
 * original numeric tag of an integral value is not recoverable, so none is
 * claimed.
 */
export const SurrealNumber = leaf<number | bigint | SdkDecimal>({
	id: 'effect-surrealkit/SurrealNumber',
	expected: 'a SurrealDB number (int, float or decimal)',
	code: runtimeCode('SurrealNumber', 'number | bigint | Surreal.Decimal'),
	parse: (input) => {
		if (typeof input === 'number') return ok(input);
		if (typeof input === 'bigint') {
			return input < INT64_MIN || input > INT64_MAX
				? invalid(`integer ${input} is outside the SurrealDB int range`)
				: ok(input);
		}
		return parseDecimal(input);
	},
	// SDK equality: an integral value may come back as `number` or `bigint`.
	equivalence: (self, that) => equals(self, that),
	arbitrary: () =>
		Schema.link<number | bigint | SdkDecimal>()(
			Schema.Union([
				Schema.Finite,
				Schema.BigInt.check(
					Schema.isBetweenBigInt({
						minimum: INT64_MIN,
						maximum: INT64_MAX,
					}),
				),
			]),
			{
				decode: SchemaGetter.transform((n) => n),
				encode: SchemaGetter.transform((n) =>
					n instanceof SdkDecimal ? n.toFloat() : n,
				),
			},
		),
});
