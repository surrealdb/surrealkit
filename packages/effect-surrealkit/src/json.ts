/**
 * The JSON codec world, kept apart from the native (SDK value) world.
 *
 * Ordinary SurrealDB JSON (HTTP `/sql`, `JSON` exports) is lossy: NONE and
 * NULL collapse, decimals become strings, sets become arrays, bytes become
 * arrays of numbers, record ids become `table:key` text, and an int beyond
 * `±(2^53 - 1)` cannot survive `JSON.parse`. These codecs read and write that
 * representation explicitly; they are never the default, and their names
 * (`<Name>FromJson`, `<Name>JsonEncoded`) say so.
 */
import { Effect, Schema, SchemaIssue, SchemaTransformation } from 'effect';
import {
	equals,
	type Decimal as SdkDecimal,
	Duration as SdkDuration,
	Geometry as SdkGeometry,
	type RecordId as SdkRecordId,
	Table as SdkTable,
	Uuid as SdkUuid,
	TextCodec,
	toSurrealqlString,
} from 'surrealdb';

import { Range, SetOf } from './collections.js';
import { Decimal, decimalFromString, SurrealNumber } from './decimal.js';
import { INT64_MAX, INT64_MIN, Int64 } from './int.js';
import {
	type CodeOutput,
	codeOf,
	literalUnion,
	quote,
	quoteList,
	runtimeCode,
} from './internal/code.js';
import { wrap } from './internal/declare.js';
import { FileRefFor, RecordIdAny, RecordIdFor, TableFor } from './record-id.js';
import { Bytes, Uuid } from './scalars.js';
import {
	DateTime,
	DateTimeEffect,
	Duration,
	dateTimeFromString,
	dateTimeToString,
} from './temporal.js';

/** The GeoJSON value `Geometry.fromJSON` takes. */
type GeoJsonValue = Parameters<typeof SdkGeometry.fromJSON>[0];

const failWith = (message: string) =>
	Effect.fail(new SchemaIssue.InvalidValue({ message }));

const attempt = <T>(f: () => T): Effect.Effect<T, SchemaIssue.Issue> =>
	Effect.suspend(() => {
		try {
			return Effect.succeed(f());
		} catch (error) {
			return failWith(
				error instanceof Error ? error.message : String(error),
			);
		}
	});

const fromString = <T>(
	target: Schema.Codec<T, T>,
	name: string,
	options: {
		readonly decode: (text: string) => T;
		readonly encode: (value: T) => string;
		readonly expected: string;
		readonly Type: string;
	},
) =>
	wrap(
		Schema.String.pipe(
			Schema.decodeTo(
				target,
				SchemaTransformation.transformEffect({
					decode: (text: string) =>
						attempt(() => options.decode(text)),
					encode: (value: T) => Effect.succeed(options.encode(value)),
				}),
			),
		),
		{
			id: `effect-surrealkit/${name}`,
			expected: options.expected,
			code: () => runtimeCode(name, options.Type),
		},
	);

/** `datetime` as RFC 3339 text, nanosecond-preserving. */
export const DateTimeFromJson = fromString(DateTime, 'DateTimeFromJson', {
	expected: 'an RFC 3339 datetime string',
	Type: 'Surreal.DateTime',
	decode: dateTimeFromString,
	encode: dateTimeToString,
});
export type DateTimeJsonEncoded = typeof DateTimeFromJson.Encoded;

/** `decimal` as decimal text (`"1.5"`), never through a JS number. */
export const DecimalFromJson = fromString(Decimal, 'DecimalFromJson', {
	expected: 'a decimal string',
	Type: 'Surreal.Decimal',
	decode: decimalFromString,
	encode: (value) => value.toString(),
});
export type DecimalJsonEncoded = typeof DecimalFromJson.Encoded;

const DURATION_TEXT = /^(?:\d+(?:y|w|d|h|m|s|ms|us|µs|ns))+$/;

/** `duration` as SurrealQL duration text (`"1h30m"`). */
export const DurationFromJson = fromString(Duration, 'DurationFromJson', {
	expected: 'a duration string',
	Type: 'Surreal.Duration',
	decode: (text) => {
		if (!DURATION_TEXT.test(text))
			throw new RangeError(`${quote(text)} is not a duration`);
		return new SdkDuration(text);
	},
	encode: (value) => value.toString(),
});
export type DurationJsonEncoded = typeof DurationFromJson.Encoded;

const UUID_TEXT =
	/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** `uuid` as canonical text. */
export const UuidFromJson = fromString(Uuid, 'UuidFromJson', {
	expected: 'a UUID string',
	Type: 'Surreal.Uuid',
	decode: (text) => {
		if (!UUID_TEXT.test(text))
			throw new RangeError(`${quote(text)} is not a UUID`);
		return new SdkUuid(text);
	},
	encode: (value) => value.toString(),
});
export type UuidJsonEncoded = typeof UuidFromJson.Encoded;

/** The length of the table part of record-id text: an identifier, `` `quoted` `` or `⟨quoted⟩`. */
const tableLength = (text: string): number => {
	const close = text.startsWith('`') ? '`' : text.startsWith('⟨') ? '⟩' : '';
	if (close === '') {
		const name = /^[A-Za-z0-9_]+/.exec(text);
		if (name === null)
			throw new RangeError(`${quote(text)} has no table name`);
		return name[0].length;
	}
	for (let i = 1; i < text.length; i++) {
		if (text[i] === '\\') i++;
		else if (text[i] === close) return i + 1;
	}
	throw new RangeError(`${quote(text)} has an unterminated table name`);
};

/**
 * Record-id text as SurrealDB JSON renders it. A key of letters, digits and
 * underscores that is not all digits is a string key the server leaves
 * unquoted (`p:0G`, `p:1s`, `p:1e5`), which SurrealQL would read as a
 * number, duration or decimal; everything else is SurrealQL the SDK parses.
 */
const recordIdFromText = (text: string): SdkRecordId => {
	const length = tableLength(text);
	if (text[length] !== ':')
		throw new RangeError(`${quote(text)} is not a record id`);
	const key = text.slice(length + 1);
	return /^[A-Za-z0-9_]+$/.test(key) && !/^\d+$/.test(key)
		? TextCodec.parseRecordId(`${text.slice(0, length)}:⟨${key}⟩`)
		: TextCodec.parseRecordId(text);
};

/**
 * `record<table>` as the record-id text SurrealDB JSON renders
 * (`person:ann`, `` person:`a b` ``), parsed with the SDK's SurrealQL value
 * grammar after the bare-key rule of {@link recordIdFromText}, never by
 * splitting on `:`.
 */
export const RecordIdFromJson = <const Tb extends string>(table: Tb) =>
	wrap(
		Schema.String.pipe(
			Schema.decodeTo(
				RecordIdFor(table),
				SchemaTransformation.transformEffect({
					decode: (text: string) =>
						attempt(
							() => recordIdFromText(text) as SdkRecordId<Tb>,
						),
					encode: (value: SdkRecordId<Tb>) =>
						Effect.succeed(value.toString()),
				}),
			),
		),
		{
			id: 'effect-surrealkit/RecordIdFromJson',
			payload: { table },
			expected: `a record id string of table ${table}`,
			code: () =>
				runtimeCode(
					`RecordIdFromJson(${quote(table)})`,
					`Surreal.RecordId<${quote(table)}>`,
				),
		},
	);

const INT_TEXT = /^-?\d+$/;

/**
 * `int` (bigint profile) from JSON: a safe-integer JSON number or decimal
 * integer text. A JSON number beyond `±(2^53 - 1)` was already rounded by
 * `JSON.parse` and fails. Encodes safe values as numbers, others as text.
 */
export const Int64FromJson = wrap(
	Schema.Union([Schema.Number, Schema.String]).pipe(
		Schema.decodeTo(
			Int64,
			SchemaTransformation.transformEffect({
				decode: (input: number | string) => {
					if (typeof input === 'number') {
						return Number.isSafeInteger(input)
							? Effect.succeed(input)
							: failWith(
									`JSON number ${input} is not a safe integer; send ints beyond ±(2^53 - 1) as text`,
								);
					}
					if (!INT_TEXT.test(input))
						return failWith(`${quote(input)} is not an integer`);
					const value = BigInt(input);
					return value < INT64_MIN || value > INT64_MAX
						? failWith(
								`integer ${input} is outside the SurrealDB int range`,
							)
						: Effect.succeed(value);
				},
				encode: (value: number | bigint) =>
					Effect.succeed(
						typeof value === 'bigint' &&
							value >= BigInt(Number.MIN_SAFE_INTEGER) &&
							value <= BigInt(Number.MAX_SAFE_INTEGER)
							? Number(value)
							: String(value),
					),
			}),
		),
	),
	{
		id: 'effect-surrealkit/Int64FromJson',
		expected: 'an integer (JSON number or text)',
		code: () => runtimeCode('Int64FromJson', 'SurrealSchema.Int64'),
	},
);
export type Int64JsonEncoded = typeof Int64FromJson.Encoded;

const byte = Schema.Number.check(
	Schema.isInt(),
	Schema.isBetween({ minimum: 0, maximum: 255 }),
);

/** `bytes` as SurrealDB JSON renders them: an array of byte numbers. */
export const BytesFromJson = wrap(
	Schema.Array(byte).pipe(
		Schema.decodeTo(
			Bytes,
			SchemaTransformation.transform<
				Uint8Array | ArrayBuffer,
				ReadonlyArray<number>
			>({
				decode: (bytes) => Uint8Array.from(bytes),
				encode: (bytes) => [
					...(bytes instanceof Uint8Array
						? bytes
						: new Uint8Array(bytes)),
				],
			}),
		),
	),
	{
		id: 'effect-surrealkit/BytesFromJson',
		expected: 'an array of bytes',
		code: () => runtimeCode('BytesFromJson', 'Uint8Array'),
	},
);
export type BytesJsonEncoded = typeof BytesFromJson.Encoded;

/**
 * Text that `decode` turns into a value `target` accepts, so the result is
 * exactly `target`'s type (brands and checks included).
 */
const textTo = <S extends Schema.Top>(
	target: S,
	options: {
		readonly id: string;
		readonly expected: string;
		readonly code: () => CodeOutput;
		readonly decode: (text: string) => S['Encoded'];
		readonly encode: (value: S['Encoded']) => string;
	},
) =>
	wrap(
		Schema.String.pipe(
			Schema.decodeTo(
				target,
				SchemaTransformation.transformEffect<S['Encoded'], string>({
					decode: (text) => attempt(() => options.decode(text)),
					encode: (value) => Effect.succeed(options.encode(value)),
				}),
			),
		),
		{
			id: options.id,
			expected: options.expected,
			code: options.code,
		},
	);

/** Any `record` as the SurrealQL record-id text SurrealDB JSON renders. */
export const RecordIdAnyFromJson = textTo(RecordIdAny, {
	id: 'effect-surrealkit/RecordIdAnyFromJson',
	expected: 'a record id string',
	code: () => runtimeCode('RecordIdAnyFromJson', 'Surreal.RecordId'),
	decode: recordIdFromText,
	encode: (value) => value.toString(),
});

/**
 * A record-id schema (a generated `PersonId`, a union of them, or
 * `RecordIdAny`) read from SurrealQL record-id text. The result is that
 * schema's type, brand included.
 */
export const RecordIdFromJsonAs = <
	S extends Schema.Codec<unknown, SdkRecordId>,
>(
	id: S,
) =>
	wrap(RecordIdAnyFromJson.pipe(Schema.decodeTo(id)), {
		id: 'effect-surrealkit/RecordIdFromJsonAs',
		expected: 'a record id string',
		code: () => {
			const inner = codeOf(id);
			return runtimeCode(
				`RecordIdFromJsonAs(${inner.runtime})`,
				inner.Type,
			);
		},
	});

/** `table` / `table<a | b>` from the bare table name SurrealDB JSON renders. */
export const TableFromJson = <const Tbs extends ReadonlyArray<string>>(
	tables: Tbs,
) =>
	textTo(TableFor(tables), {
		id: 'effect-surrealkit/TableFromJson',
		expected:
			tables.length === 0
				? 'a table name'
				: `one of the table names ${tables.join(', ')}`,
		code: () =>
			runtimeCode(
				`TableFromJson(${quoteList(tables)})`,
				`Surreal.Table<${literalUnion(tables)}>`,
			),
		// The table leaf checks the name against `tables`.
		decode: (text) =>
			new SdkTable(text) as ReturnType<typeof TableFor<Tbs>>['Encoded'],
		encode: (table) => table.name,
	});

/** `file` / `file<a | b>` from the `f"bucket:/key"` text SurrealDB JSON renders. */
export const FileRefFromJson = <const Bs extends ReadonlyArray<string>>(
	buckets: Bs,
) =>
	textTo(FileRefFor(buckets), {
		id: 'effect-surrealkit/FileRefFromJson',
		expected: 'a file reference string (f"bucket:/key")',
		code: () =>
			runtimeCode(
				`FileRefFromJson(${quoteList(buckets)})`,
				'Surreal.FileRef',
			),
		decode: (text) => TextCodec.parseFile(text),
		encode: (file) => toSurrealqlString(file),
	});

/**
 * `range` from the SurrealQL range text SurrealDB JSON renders (`1..5`,
 * `'a'..'z'`). Encoding prints SurrealQL through the SDK, which does not
 * quote string bounds.
 */
export const RangeFromJson = textTo(Range, {
	id: 'effect-surrealkit/RangeFromJson',
	expected: 'a range string',
	code: () => runtimeCode('RangeFromJson', 'Surreal.Range<unknown, unknown>'),
	decode: (text) => TextCodec.parseRange(text),
	encode: (range) => toSurrealqlString(range),
});

const GeoJson = Schema.Record(Schema.String, Schema.Unknown);

/**
 * A geometry schema (`GeometryPoint`, `GeometryOf([...])`, `Geometry`) read
 * from the GeoJSON object SurrealDB JSON renders.
 */
export const GeometryFromJson = <S extends Schema.Codec<unknown, SdkGeometry>>(
	geometry: S,
) =>
	wrap(
		GeoJson.pipe(
			Schema.decodeTo(
				geometry,
				SchemaTransformation.transformEffect<
					SdkGeometry,
					{ readonly [key: string]: unknown }
				>({
					decode: (json) =>
						attempt(() =>
							SdkGeometry.fromJSON(
								json as unknown as GeoJsonValue,
							),
						),
					encode: (value) =>
						Effect.succeed(
							value.toJSON() as unknown as {
								readonly [key: string]: unknown;
							},
						),
				}),
			),
		),
		{
			id: 'effect-surrealkit/GeometryFromJson',
			expected: 'a GeoJSON geometry',
			code: () => {
				const inner = codeOf(geometry);
				return runtimeCode(
					`GeometryFromJson(${inner.runtime})`,
					inner.Type,
				);
			},
		},
	);

/**
 * `number` from JSON: a JSON number, or the decimal text SurrealDB JSON
 * renders for a decimal. An int beyond `±(2^53 - 1)` was already rounded by
 * `JSON.parse`.
 */
export const SurrealNumberFromJson = wrap(
	Schema.Union([Schema.Number, Schema.String]).pipe(
		Schema.decodeTo(
			SurrealNumber,
			SchemaTransformation.transformEffect({
				decode: (input: number | string) =>
					typeof input === 'number'
						? Effect.succeed(input)
						: attempt(() => decimalFromString(input)),
				encode: (value: number | bigint | SdkDecimal) =>
					Effect.succeed(
						typeof value === 'number' ? value : value.toString(),
					),
			}),
		),
	),
	{
		id: 'effect-surrealkit/SurrealNumberFromJson',
		expected: 'a number, or a decimal string',
		code: () =>
			runtimeCode(
				'SurrealNumberFromJson',
				'number | bigint | Surreal.Decimal',
			),
	},
);

/**
 * `datetime` under the effect-datetime profile, from RFC 3339 text:
 * millisecond precision, like {@link DateTimeEffect}.
 */
export const DateTimeEffectFromJson = wrap(
	DateTimeFromJson.pipe(Schema.decodeTo(DateTimeEffect)),
	{
		id: 'effect-surrealkit/DateTimeEffectFromJson',
		expected: 'an RFC 3339 datetime string',
		code: () => runtimeCode('DateTimeEffectFromJson', 'DateTime.Utc'),
	},
);

/** `set<T>` as SurrealDB JSON renders it: an array of distinct elements. */
export const SetFromJson = <S extends Schema.Top>(
	item: S,
	options: { readonly length?: number } = {},
) =>
	wrap(
		Schema.Array(item).pipe(
			Schema.decodeTo(
				SetOf(Schema.toType(item), options),
				SchemaTransformation.transformEffect<
					ReadonlySet<S['Type']>,
					ReadonlyArray<S['Type']>
				>({
					// Check before `new Set` silently drops primitive duplicates.
					decode: (values) => {
						const duplicate = values.findIndex((value, index) =>
							values
								.slice(0, index)
								.some((earlier) => equals(earlier, value)),
						);
						return duplicate === -1
							? Effect.succeed(new Set(values))
							: failWith(
									`set element ${duplicate} duplicates an earlier element; SurrealDB sets hold distinct values`,
								);
					},
					encode: (set) => Effect.succeed([...set]),
				}),
			),
		),
		{
			id: 'effect-surrealkit/SetFromJson',
			expected: 'an array of distinct elements',
			// The wrapped array only shows the encoded side, so print the item.
			code: () => {
				const itemCode = codeOf(item);
				const lengthCode =
					options.length === undefined
						? ''
						: `, { length: ${options.length} }`;
				return runtimeCode(
					`SetFromJson(${itemCode.runtime}${lengthCode})`,
					`ReadonlySet<${itemCode.Type}>`,
				);
			},
		},
	);
