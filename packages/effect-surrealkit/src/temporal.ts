/**
 * `datetime` and `duration`.
 *
 * Out-of-range values are rejected here because the engine does not reject
 * them usefully (measured on SurrealDB 3.2.4 with surrealdb.js 2.1.1): a
 * datetime outside the engine's range makes the server answer with an error
 * the SDK never resolves, so the query hangs; a negative SDK `Duration` is
 * silently sent (and stored) as `0ns`.
 */
import {
	DateTime as EffectDateTime,
	Schema,
	SchemaGetter,
	SchemaTransformation,
} from 'effect';
import { DateTime as SdkDateTime, Duration as SdkDuration } from 'surrealdb';

import { quote, runtimeCode } from './internal/code.js';
import {
	invalid,
	leaf,
	ok,
	type Parsed,
	wrap,
	wrongType,
} from './internal/declare.js';

/** Earliest datetime the engine stores: `-262143-01-01T00:00:00Z`. */
export const DATETIME_MIN_SECONDS = -8_334_601_228_800n;
/** Latest whole second the engine stores: `+262142-12-31T23:59:59Z`. */
export const DATETIME_MAX_SECONDS = 8_210_266_876_799n;

const NANOS_PER_SECOND = 1_000_000_000n;

const compactDateTime = (value: SdkDateTime): readonly [bigint, bigint] => {
	const [seconds = 0n, nanos = 0n] = value.toCompact();
	return [seconds, nanos];
};

const parseDateTime = (input: unknown): Parsed<SdkDateTime> => {
	if (!(input instanceof SdkDateTime)) return wrongType;
	const [seconds, nanos] = compactDateTime(input);
	if (nanos < 0n || nanos >= NANOS_PER_SECOND) {
		return invalid(`datetime has an invalid nanosecond part ${nanos}`);
	}
	if (seconds < DATETIME_MIN_SECONDS || seconds > DATETIME_MAX_SECONDS) {
		return invalid(
			`datetime ${seconds}s since the epoch is outside the SurrealDB datetime range (-262143-01-01 to +262142-12-31)`,
		);
	}
	return ok(input);
};

const datetimeArbitrarySource = Schema.Tuple([
	// 1900-01-01 … 2100-01-01, plus the full nanosecond range.
	Schema.BigInt.check(
		Schema.isBetweenBigInt({
			minimum: -2_208_988_800n,
			maximum: 4_102_444_800n,
		}),
	),
	Schema.BigInt.check(
		Schema.isBetweenBigInt({ minimum: 0n, maximum: 999_999_999n }),
	),
]);

/**
 * `datetime`, `datetime: "sdk"` profile (default): the SDK `DateTime` class,
 * nanosecond-preserving. Requires the SDK's default `useNativeDates: false`.
 */
export const DateTime = leaf<SdkDateTime>({
	id: 'effect-surrealkit/DateTime',
	expected: 'a SurrealDB DateTime',
	code: runtimeCode('DateTime', 'Surreal.DateTime'),
	parse: parseDateTime,
	equivalence: (self, that) => self.equals(that),
	arbitrary: () =>
		Schema.link<SdkDateTime>()(datetimeArbitrarySource, {
			decode: SchemaGetter.transform(
				([s, ns]) => new SdkDateTime([s, ns]),
			),
			encode: SchemaGetter.transform((dt) => compactDateTime(dt)),
		}),
});

const DATETIME_TEXT =
	/^([+-]\d{6}|\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$/;

/**
 * Parses RFC 3339 datetime text (with the `±YYYYYY` extended year SurrealDB
 * prints for years outside 0000–9999) into an SDK `DateTime`, keeping every
 * nanosecond digit.
 *
 * Not delegated to the SDK: surrealdb.js 2.1.1 formats pre-1970 datetimes
 * with a fractional second one second late (`[-522127542, 999999991]` prints
 * as `…20:34:19.999999991Z`; SurrealDB prints `…20:34:18.999999991Z`).
 *
 * @throws RangeError for malformed text or impossible calendar values.
 */
export const dateTimeFromString = (text: string): SdkDateTime => {
	const match = DATETIME_TEXT.exec(text);
	if (match === null) {
		throw new RangeError(`${quote(text)} is not an RFC 3339 datetime`);
	}
	const [
		,
		year,
		month,
		day,
		hour,
		minute,
		second,
		fraction = '',
		zone = 'Z',
	] = match;
	const date = new Date(0);
	date.setUTCFullYear(Number(year), Number(month) - 1, Number(day));
	date.setUTCHours(Number(hour), Number(minute), Number(second), 0);
	if (
		Number.isNaN(date.getTime()) ||
		date.getUTCDate() !== Number(day) ||
		date.getUTCMonth() !== Number(month) - 1 ||
		date.getUTCHours() !== Number(hour) ||
		date.getUTCMinutes() !== Number(minute) ||
		date.getUTCSeconds() !== Number(second)
	) {
		throw new RangeError(`${quote(text)} is not a valid calendar datetime`);
	}
	let seconds = BigInt(date.getTime() / 1000);
	if (zone !== 'Z') {
		const sign = zone.startsWith('-') ? -1n : 1n;
		const offset = BigInt(
			Number(zone.slice(1, 3)) * 3600 + Number(zone.slice(4, 6)) * 60,
		);
		seconds -= sign * offset;
	}
	return new SdkDateTime([seconds, BigInt(fraction.padEnd(9, '0'))]);
};

const pad = (value: number, width: number): string =>
	String(value).padStart(width, '0');

/**
 * Formats an SDK `DateTime` as RFC 3339 UTC text with every significant
 * nanosecond digit (see {@link dateTimeFromString} for why the SDK's own
 * `toString` is not used).
 */
export const dateTimeToString = (value: SdkDateTime): string => {
	const [seconds, nanos] = compactDateTime(value);
	const date = new Date(Number(seconds) * 1000);
	const year = date.getUTCFullYear();
	const yearText =
		year >= 0 && year <= 9999
			? pad(year, 4)
			: `${year < 0 ? '-' : '+'}${pad(Math.abs(year), 6)}`;
	const fraction =
		nanos === 0n ? '' : `.${pad(Number(nanos), 9).replace(/0+$/, '')}`;
	return `${yearText}-${pad(date.getUTCMonth() + 1, 2)}-${pad(date.getUTCDate(), 2)}T${pad(date.getUTCHours(), 2)}:${pad(date.getUTCMinutes(), 2)}:${pad(date.getUTCSeconds(), 2)}${fraction}Z`;
};

const floorDiv = (n: bigint, d: bigint): bigint => {
	const q = n / d;
	return n % d !== 0n && n < 0n !== d < 0n ? q - 1n : q;
};

/**
 * `datetime`, `datetime: "effect-datetime"` profile: Effect's `DateTime.Utc`.
 *
 * **Lossy by design**: Effect datetimes have millisecond precision, so
 * decoding truncates sub-millisecond digits (towards negative infinity).
 * Encoding produces an SDK `DateTime` on a whole millisecond.
 */
export const DateTimeEffect = wrap(
	DateTime.pipe(
		Schema.decodeTo(
			Schema.DateTimeUtc,
			SchemaTransformation.transform({
				decode: (dt: SdkDateTime) =>
					EffectDateTime.makeUnsafe(
						Number(floorDiv(dt.nanoseconds, 1_000_000n)),
					),
				// Built from a normalised tuple: the SDK's own millisecond
				// constructors produce a negative nanosecond part before 1970.
				encode: (utc: EffectDateTime.Utc) => {
					const millis = BigInt(EffectDateTime.toEpochMillis(utc));
					const seconds = floorDiv(millis, 1000n);
					return new SdkDateTime([
						seconds,
						(millis - seconds * 1000n) * 1_000_000n,
					]);
				},
			}),
		),
	),
	{
		id: 'effect-surrealkit/DateTimeEffect',
		expected:
			'a SurrealDB DateTime (as Effect DateTime.Utc, millisecond precision)',
		code: () => ({
			...runtimeCode('DateTimeEffect', 'DateTime.Utc'),
			importDeclarations: [
				...runtimeCode('', '').importDeclarations,
				'import type { DateTime } from "effect"',
			],
		}),
		equivalence: (self, that) =>
			EffectDateTime.toEpochMillis(self) ===
			EffectDateTime.toEpochMillis(that),
		// Effect's own arbitrary spans ±275,760 years, beyond SurrealDB's range.
		arbitrary: () =>
			Schema.link<EffectDateTime.Utc>()(
				Schema.Number.check(
					Schema.isInt(),
					Schema.isBetween({
						minimum: -2_208_988_800_000,
						maximum: 4_102_444_800_000,
					}),
				),
				{
					decode: SchemaGetter.transform((millis) =>
						EffectDateTime.makeUnsafe(millis),
					),
					encode: SchemaGetter.transform((utc) =>
						EffectDateTime.toEpochMillis(utc),
					),
				},
			),
	},
);

/** Largest duration the engine stores: `2^64 - 1` seconds plus 999,999,999ns. */
export const DURATION_MAX_SECONDS = 2n ** 64n - 1n;

const compactDuration = (value: SdkDuration): readonly [bigint, bigint] => {
	const [seconds = 0n, nanos = 0n] = value.toCompact();
	return [seconds, nanos];
};

const parseDuration = (input: unknown): Parsed<SdkDuration> => {
	if (!(input instanceof SdkDuration)) return wrongType;
	// `toCompact()` (what the SDK puts on the wire) drops negative parts, so
	// read the signed total instead.
	const total = input.nanoseconds;
	if (total < 0n) {
		return invalid(
			`duration of ${total}ns is negative; SurrealDB durations are non-negative and surrealdb.js 2.1.1 silently sends a negative one as 0ns`,
		);
	}
	if (total / NANOS_PER_SECOND > DURATION_MAX_SECONDS) {
		return invalid(
			`duration ${input} exceeds the SurrealDB duration range`,
		);
	}
	return ok(input);
};

/** `duration`: the SDK `Duration` class, nanosecond-preserving, non-negative. */
export const Duration = leaf<SdkDuration>({
	id: 'effect-surrealkit/Duration',
	expected: 'a SurrealDB Duration',
	code: runtimeCode('Duration', 'Surreal.Duration'),
	parse: parseDuration,
	equivalence: (self, that) => self.equals(that),
	arbitrary: () =>
		Schema.link<SdkDuration>()(
			Schema.Tuple([
				Schema.BigInt.check(
					Schema.isBetweenBigInt({
						minimum: 0n,
						maximum: 10n ** 12n,
					}),
				),
				Schema.BigInt.check(
					Schema.isBetweenBigInt({
						minimum: 0n,
						maximum: 999_999_999n,
					}),
				),
			]),
			{
				decode: SchemaGetter.transform(
					([s, ns]) => new SdkDuration([s, ns]),
				),
				encode: SchemaGetter.transform((d) => compactDuration(d)),
			},
		),
});

/** A `duration` literal kind such as `1d`: exactly that duration. */
export const DurationLiteral = <const V extends string>(value: V) => {
	const expected = new SdkDuration(value);
	return leaf<SdkDuration>({
		id: 'effect-surrealkit/DurationLiteral',
		payload: value,
		expected: `the duration ${value}`,
		code: runtimeCode(
			`DurationLiteral(${quote(value)})`,
			'Surreal.Duration',
		),
		parse: (input) => {
			const parsed = parseDuration(input);
			if (!parsed.ok) return parsed;
			return parsed.value.equals(expected) ? parsed : wrongType;
		},
		equivalence: (self, that) => self.equals(that),
		arbitrary: () =>
			Schema.link<SdkDuration>()(Schema.Literal(value), {
				decode: SchemaGetter.transform(() => new SdkDuration(value)),
				encode: SchemaGetter.transform(() => value),
			}),
	});
};
