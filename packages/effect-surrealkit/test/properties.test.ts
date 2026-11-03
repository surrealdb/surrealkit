/**
 * Property round trips through the SDK's own CBOR codec (the wire format of
 * surrealdb.js 2.1.1), in process. The engine suite repeats them through a
 * real server.
 */
import { Arbitrary, Effect, Schema } from 'effect';
import { CborCodec } from 'surrealdb';
import { describe, expect, it } from 'vitest';

import * as S from '../src/index.js';
import { expectRoundTrip } from './helpers.js';

const cbor = CborCodec.DEFAULT;
const viaCbor = (value: unknown): unknown => cbor.decode(cbor.encode(value));
const viaJson = (value: unknown): unknown => JSON.parse(JSON.stringify(value));

const native: ReadonlyArray<readonly [string, Schema.Codec<unknown, unknown>]> =
	[
		['Int', S.Int],
		['Int64', S.Int64],
		['DateTime', S.DateTime],
		['DateTimeEffect', S.DateTimeEffect],
		['Duration', S.Duration],
		['Decimal', S.Decimal],
		['SurrealNumber', S.SurrealNumber],
		['Uuid', S.Uuid],
		['Bytes', S.Bytes],
		['RecordIdFor', S.RecordIdFor('person')],
		[
			'RecordIdFor (object key)',
			S.RecordIdFor('id_object', {
				key: Schema.Struct({ n: S.Int, region: S.String }),
			}),
		],
		[
			'RecordIdFor (array key)',
			S.RecordIdFor('id_array', {
				key: Schema.Tuple([S.Int64, S.String]),
			}),
		],
		['RecordIdFor (int64 key)', S.RecordIdFor('counter', { key: S.Int64 })],
		['RecordIdFor (uuid key)', S.RecordIdFor('ticket', { key: S.Uuid })],
		['RecordIdAny', S.RecordIdAny],
		[
			'taggedRecordId',
			S.taggedRecordId({
				org: S.RecordIdFor('org'),
				team: S.RecordIdFor('team'),
			}),
		],
		['TableFor', S.TableFor(['a', 'b'])],
		['FileRefFor', S.FileRefFor(['avatars'])],
		['GeometryPoint', S.GeometryPoint],
		['GeometryLine', S.GeometryLine],
		['GeometryPolygon', S.GeometryPolygon],
		['GeometryMultiPoint', S.GeometryMultiPoint],
		['GeometryMultiLine', S.GeometryMultiLine],
		['GeometryMultiPolygon', S.GeometryMultiPolygon],
		['GeometryCollection', S.GeometryCollection],
		['Range', S.Range],
		['SetOf(Int)', S.SetOf(S.Int)],
		['SetOf(String, length 3)', S.SetOf(S.String, { length: 3 })],
		['DecimalLiteral', S.DecimalLiteral('10')],
		['DurationLiteral', S.DurationLiteral('1d')],
		['Int64Literal', S.Int64Literal('-5')],
		[
			'row with NONE, NULL and nested values',
			S.NoneAsAbsent(
				Schema.Struct({
					id: S.RecordIdFor('person'),
					nickname: Schema.optionalKey(S.String),
					bio: Schema.NullOr(S.String),
					age: Schema.optionalKey(S.Int64),
					born: S.DateTime,
					balance: S.Decimal,
					tags: S.SetOf(S.String),
					address: Schema.Struct({
						city: S.String,
						zip: Schema.optionalKey(S.String),
					}),
					scores: Schema.Array(Schema.UndefinedOr(S.Int)),
				}),
			),
		],
	];

describe('checks on a codec narrow generation instead of filtering it', () => {
	// Generated modules put checks on the codecs (`S.Int.check(Schema.isBetween(…))`);
	// generation must honour them or a narrow range is never hit.
	for (const [name, schema] of [
		[
			'Int in [0, 150]',
			S.Int.check(Schema.isBetween({ minimum: 0, maximum: 150 })),
		],
		[
			'Int64 in [-5, 5]',
			S.Int64.check(
				Schema.isBetweenBigInt({ minimum: -5n, maximum: 5n }),
			),
		],
		[
			'String of 1 to 80 code points',
			S.String.check(Schema.isBetweenCodePoints(1, 80)),
		],
		[
			'String matching a pattern',
			S.String.check(Schema.isPattern(/^[a-z]{3}$/)),
		],
		[
			'RecordIdFor with a string key (never empty)',
			S.RecordIdFor('person', { key: S.String }),
		],
	] as const) {
		it(name, async () => {
			await expectRoundTrip(
				schema as Schema.Codec<unknown, unknown>,
				viaCbor,
			);
		});
	}
});

describe('native codecs round-trip through the SDK CBOR codec', () => {
	for (const [name, schema] of native) {
		it(name, async () => {
			await expectRoundTrip(schema, viaCbor);
		});
	}
});

const json: ReadonlyArray<readonly [string, Schema.Codec<unknown, unknown>]> = [
	['DateTimeFromJson', S.DateTimeFromJson],
	['DecimalFromJson', S.DecimalFromJson],
	['DurationFromJson', S.DurationFromJson],
	['UuidFromJson', S.UuidFromJson],
	['Int64FromJson', S.Int64FromJson],
	['BytesFromJson', S.BytesFromJson],
	['SetFromJson', S.SetFromJson(S.String)],
];

describe('JSON codecs round-trip through JSON text', () => {
	for (const [name, schema] of json) {
		it(name, async () => {
			await expectRoundTrip(schema, viaJson);
		});
	}
});

describe('decimalFromString reads every spelling of a decimal exactly', () => {
	it('signed, exponent and leading-dot spellings of arbitrary decimals', async () => {
		const equivalent = Schema.toEquivalence(S.Decimal);
		const result = await Effect.runPromise(
			Arbitrary.checkEffect(
				Arbitrary.schema(S.Decimal),
				(value) =>
					Effect.sync(() => {
						const text = value.toString();
						const negative = text.startsWith('-');
						const [int = '0', frac = ''] = (
							negative ? text.slice(1) : text
						).split('.');
						const sign = negative ? '-' : '+';
						const spellings = [
							text,
							`${sign}${int}.${frac}`,
							`${sign}${int}${frac}e-${frac.length}`,
							`${sign}${int}${frac}0E-${frac.length + 1}`,
							`${sign}.${int}${frac}e${int.length}`,
						];
						return spellings.every((spelling) =>
							equivalent(value, S.decimalFromString(spelling)),
						);
					}),
				{ runs: 200, seed: 20261025 },
			),
		);
		expect(Arbitrary.formatCheckFailure(result) ?? 'Passed').toBe('Passed');
	});
});
