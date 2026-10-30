/**
 * Round trips through a real, disposable SurrealDB engine with
 * surrealdb.js 2.1.1, including the integer edges the SDK's CBOR decoder
 * treats specially. See `engine.ts` for how the engine is provided.
 */
import { Arbitrary, Effect, Exit, Schema } from 'effect';
import { DateTime, Decimal, RecordId, toSurrealqlString } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import * as S from '../src/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';
import { expectRoundTrip } from './helpers.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] engine suite skipped: ${engineSkipReason}`,
	);
}

describe.skipIf(!engineConfigured)('disposable engine round trips', () => {
	let engine: Engine;
	const returnValue = async (value: unknown): Promise<unknown> => {
		const [result] = await within(
			engine.db.query<[unknown]>('RETURN $v', { v: value }),
		);
		return result;
	};

	beforeAll(async () => {
		engine = await startEngine();
		console.info(`[effect-surrealkit] engine ${engine.version}`);
	});
	afterAll(async () => {
		await engine?.stop();
	});

	const native: ReadonlyArray<
		readonly [string, Schema.Codec<unknown, unknown>]
	> = [
		['Int', S.Int],
		['Int64', S.Int64],
		['DateTime', S.DateTime],
		['DateTimeEffect', S.DateTimeEffect],
		['Duration', S.Duration],
		['Decimal', S.Decimal],
		['Uuid', S.Uuid],
		['Bytes', S.Bytes],
		['String', S.String],
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
		['TableFor', S.TableFor(['a', 'b'])],
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
	];

	for (const [name, schema] of native) {
		it(`${name} survives RETURN $v`, async () => {
			await expectRoundTrip(schema, returnValue, { runs: 25 });
		});
	}

	describe('the JSON world over HTTP /sql', () => {
		/** What JSON does to a value it cannot carry: a decode failure, or another value. */
		type Loss = (value: unknown) => 'fails' | 'changes' | undefined;
		const unsafeInt = (value: unknown) =>
			typeof value === 'bigint' &&
			(value > BigInt(Number.MAX_SAFE_INTEGER) ||
				value < -BigInt(Number.MAX_SAFE_INTEGER));
		/** NaN and ±Infinity render as null; -0 renders as 0. */
		const float: Loss = (value) =>
			typeof value !== 'number'
				? undefined
				: !Number.isFinite(value)
					? 'fails'
					: Object.is(value, -0)
						? 'changes'
						: undefined;
		/** A JSON number beyond ±(2^53 - 1) was already rounded by JSON.parse. */
		const int64: Loss = (value) => (unsafeInt(value) ? 'fails' : undefined);
		/** `number` cannot tell an int from a float: a rounded JSON number decodes as one. */
		const number: Loss = (value) =>
			unsafeInt(value) ? 'changes' : float(value);

		/**
		 * [name, native codec, JSON codec, what JSON loses]. A value is stored
		 * exactly through the SDK, read back as SurrealDB JSON, and must decode
		 * through the JSON codec to the same value; a value JSON cannot carry
		 * must fail loudly or, where the README says it changes, still decode.
		 */
		const pairs: ReadonlyArray<
			readonly [
				string,
				Schema.Codec<unknown, unknown>,
				Schema.Codec<unknown, unknown>,
				Loss | undefined,
			]
		> = [
			['Int', S.Int, S.Int],
			['Int64', S.Int64, S.Int64FromJson, int64],
			['float', Schema.Number, Schema.Number, float],
			['SurrealNumber', S.SurrealNumber, S.SurrealNumberFromJson, number],
			['DateTime', S.DateTime, S.DateTimeFromJson],
			['DateTimeEffect', S.DateTimeEffect, S.DateTimeEffectFromJson],
			['Duration', S.Duration, S.DurationFromJson],
			['Decimal', S.Decimal, S.DecimalFromJson],
			['Uuid', S.Uuid, S.UuidFromJson],
			['Bytes', S.Bytes, S.BytesFromJson],
			['String', S.String, S.String],
			[
				'RecordIdFor',
				S.RecordIdFor('person'),
				S.RecordIdFromJsonAs(S.RecordIdFor('person')),
			],
			...(
				[
					[
						'object key',
						S.RecordIdFor('id_object', {
							key: Schema.Struct({ n: S.Int, region: S.String }),
						}),
					],
					[
						'array key',
						S.RecordIdFor('id_array', {
							key: Schema.Tuple([S.Int64, S.String]),
						}),
					],
					['int64 key', S.RecordIdFor('counter', { key: S.Int64 })],
					['uuid key', S.RecordIdFor('ticket', { key: S.Uuid })],
					['any table', S.RecordIdAny],
				] as const
			).map(
				([label, id]) =>
					[
						`RecordId (${label})`,
						id as Schema.Codec<unknown, unknown>,
						S.RecordIdFromJsonAs(
							id as Schema.Codec<unknown, RecordId>,
						) as Schema.Codec<unknown, unknown>,
					] as const,
			),
			['TableFor', S.TableFor(['a', 'b']), S.TableFromJson(['a', 'b'])],
			...(
				[
					['GeometryPoint', S.GeometryPoint],
					['GeometryLine', S.GeometryLine],
					['GeometryPolygon', S.GeometryPolygon],
					['GeometryMultiPoint', S.GeometryMultiPoint],
					['GeometryMultiLine', S.GeometryMultiLine],
					['GeometryMultiPolygon', S.GeometryMultiPolygon],
					['GeometryCollection', S.GeometryCollection],
				] as const
			).map(
				([label, geometry]) =>
					[
						label,
						geometry as Schema.Codec<unknown, unknown>,
						S.GeometryFromJson(geometry) as Schema.Codec<
							unknown,
							unknown
						>,
					] as const,
			),
			['Range', S.Range, S.RangeFromJson],
			['SetOf(Int)', S.SetOf(S.Int), S.SetFromJson(S.Int)],
			[
				'SetOf(String, length 3)',
				S.SetOf(S.String, { length: 3 }),
				S.SetFromJson(S.String, { length: 3 }),
			],
			[
				'Array(DateTime)',
				Schema.Array(S.DateTime),
				Schema.Array(S.DateTimeFromJson),
			],
		].map(([name, n, j, lossy]) => [
			name as string,
			n as Schema.Codec<unknown, unknown>,
			j as Schema.Codec<unknown, unknown>,
			lossy as Loss | undefined,
		]);

		let row = 0;
		for (const [name, nativeCodec, jsonCodec, lossy] of pairs) {
			it(`${name}: stored through the SDK, read as JSON`, async () => {
				const encode = Schema.encodeSync(nativeCodec);
				const decodeJson = Schema.decodeUnknownExit(jsonCodec);
				const encodeJson = Schema.encodeSync(jsonCodec);
				const equivalent = Schema.toEquivalence(nativeCodec);
				const result = await Effect.runPromise(
					Arbitrary.checkEffect(
						Arbitrary.schema(nativeCodec),
						(value) =>
							Effect.promise(async () => {
								const id = new RecordId('json_rt', ++row);
								await within(
									engine.db.query('CREATE $id SET v = $v', {
										id,
										v: encode(value),
									}),
								);
								const json = await within(
									engine.http(
										`SELECT VALUE v FROM ONLY ${toSurrealqlString(id)}`,
									),
								);
								const decoded = decodeJson(json);
								const loss = lossy?.(value);
								if (loss === 'fails')
									return Exit.isFailure(decoded);
								if (loss === 'changes')
									return Exit.isSuccess(decoded);
								if (!Exit.isSuccess(decoded)) return false;
								// Decoding the JSON gives the stored value, and its JSON
								// encoding decodes to it again.
								const again = Schema.decodeUnknownSync(
									jsonCodec,
								)(
									JSON.parse(
										JSON.stringify(
											encodeJson(decoded.value),
										),
									),
								);
								return (
									equivalent(value, decoded.value) &&
									equivalent(value, again)
								);
							}),
						{ runs: 25, seed: 20261025 },
					),
				);
				expect(Arbitrary.formatCheckFailure(result) ?? 'Passed').toBe(
					'Passed',
				);
			});
		}

		it('NONE renders as an absent key in rows and objects, and as null in arrays', async () => {
			await within(
				engine.db.query(
					'CREATE json_rt:none CONTENT { a: NONE, b: NULL, c: [1, NONE, NULL], d: { x: NONE, y: 1 } }',
				),
			);
			const json = await within(
				engine.http('SELECT * FROM ONLY json_rt:none'),
			);
			expect(json).toEqual({
				b: null,
				c: [1, null, null],
				d: { y: 1 },
				id: 'json_rt:none',
			});
			const Row = Schema.Struct({
				id: S.RecordIdFromJsonAs(S.RecordIdFor('json_rt')),
				a: Schema.optionalKey(S.Int),
				b: Schema.NullOr(S.Int),
				c: Schema.Array(Schema.NullOr(S.Int)),
				d: Schema.Struct({ x: Schema.optionalKey(S.Int), y: S.Int }),
			});
			expect(Schema.decodeUnknownSync(Row)(json)).toEqual({
				id: new RecordId('json_rt', 'none'),
				b: null,
				c: [1, null, null],
				d: { y: 1 },
			});
		});

		it('int, decimal and duration literals read through their JSON codecs', async () => {
			const json = await within(
				engine.http(
					'RETURN { i: 9007199254740993, d: 1.50dec, u: 90m }',
				),
			);
			// A JSON number beyond ±(2^53 - 1) was rounded by JSON.parse: loud.
			expect(() =>
				Schema.decodeUnknownSync(S.Int64FromJson)(
					(json as { i: unknown }).i,
				),
			).toThrow(/not a safe integer/);
			const literals = Schema.Struct({
				d: S.DecimalFromJson.pipe(
					Schema.decodeTo(S.DecimalLiteral('1.5')),
				),
				u: S.DurationFromJson.pipe(
					Schema.decodeTo(S.DurationLiteral('1h30m')),
				),
			});
			expect(() =>
				Schema.decodeUnknownSync(literals)(json, {
					onExcessProperty: 'ignore',
				}),
			).not.toThrow();
		});
	});

	describe('integer edges (SDK CBOR number/bigint split)', () => {
		const roundTrip = async (literal: string) => {
			const [value] = await within(
				engine.db.query<[unknown]>(`RETURN ${literal}`),
			);
			return value;
		};

		it('±(2^53 - 1) decode under both profiles', async () => {
			for (const literal of ['9007199254740991', '-9007199254740991']) {
				const value = await roundTrip(literal);
				expect(Schema.decodeUnknownSync(S.Int)(value)).toBe(
					Number(literal),
				);
				expect(Schema.decodeUnknownSync(S.Int64)(value)).toBe(
					BigInt(literal),
				);
			}
		});

		it('2^53 arrives as an exact number: Int fails loudly, Int64 accepts it', async () => {
			const value = await roundTrip('9007199254740992');
			expect(value).toBe(2 ** 53);
			expect(() => Schema.decodeUnknownSync(S.Int)(value)).toThrow(
				/safe JavaScript range/,
			);
			expect(Schema.decodeUnknownSync(S.Int64)(value)).toBe(2n ** 53n);
		});

		it('2^53 + 1 arrives as a bigint: Int fails loudly, Int64 is exact', async () => {
			const value = await roundTrip('9007199254740993');
			expect(value).toBe(9007199254740993n);
			expect(() => Schema.decodeUnknownSync(S.Int)(value)).toThrow(
				/safe JavaScript range/,
			);
			expect(Schema.decodeUnknownSync(S.Int64)(value)).toBe(
				9007199254740993n,
			);
		});

		it('-(2^53 + 1) arrives rounded to -2^53: both profiles fail loudly', async () => {
			const value = await roundTrip('-9007199254740993');
			// The SDK decoder's rounding, recorded so a fix upstream is noticed.
			expect(value).toBe(-(2 ** 53));
			expect(() => Schema.decodeUnknownSync(S.Int)(value)).toThrow(
				/safe JavaScript range/,
			);
			expect(() => Schema.decodeUnknownSync(S.Int64)(value)).toThrow(
				/exact value is unknown/,
			);
		});

		it('int64 extremes are exact under Int64 and loud under Int', async () => {
			for (const literal of [
				'9223372036854775807',
				'-9223372036854775808',
			]) {
				const value = await roundTrip(literal);
				expect(Schema.decodeUnknownSync(S.Int64)(value)).toBe(
					BigInt(literal),
				);
				expect(() => Schema.decodeUnknownSync(S.Int)(value)).toThrow(
					/safe JavaScript range/,
				);
			}
		});

		it('Int64 encodes int64 extremes the engine stores as int', async () => {
			for (const value of [
				S.INT64_MAX,
				S.INT64_MIN,
				S.int64(-(2n ** 53n) - 2n),
			]) {
				const [type, back] = await within(
					engine.db.query<[string, unknown]>(
						'RETURN type::of($v); RETURN $v',
						{
							v: Schema.encodeSync(S.Int64)(S.int64(value)),
						},
					),
				);
				expect(type).toBe('int');
				expect(Schema.decodeUnknownSync(S.Int64)(back)).toBe(value);
			}
		});

		it('a bigint beyond int64 is refused before it can wrap around on the server', () => {
			expect(() => Schema.encodeUnknownSync(S.Int64)(2n ** 63n)).toThrow(
				/int range/,
			);
		});
	});

	describe('stored rows: NONE, NULL and absent', () => {
		beforeAll(async () => {
			await within(
				engine.db.query(`
					DEFINE TABLE presence SCHEMAFULL;
					DEFINE FIELD nickname ON presence TYPE option<string>;
					DEFINE FIELD bio ON presence TYPE string | null;
					DEFINE FIELD born ON presence TYPE datetime;
					DEFINE FIELD balance ON presence TYPE decimal;
					DEFINE FIELD tags ON presence TYPE set<string>;
					DEFINE FIELD meta ON presence TYPE object FLEXIBLE;
					DEFINE FIELD meta.kind ON presence TYPE string;
				`),
			);
		});

		const Row = Schema.Struct({
			id: S.RecordIdFor('presence'),
			nickname: Schema.optionalKey(S.String),
			bio: Schema.NullOr(S.String),
			born: S.DateTime,
			balance: S.Decimal,
			tags: S.SetOf(S.String),
			meta: Schema.StructWithRest(Schema.Struct({ kind: S.String }), [
				Schema.Record(Schema.String, Schema.Unknown),
			]),
		});

		it('create → select keeps nanoseconds, decimals, sets, NULL and FLEXIBLE keys', async () => {
			const id = new RecordId('presence', 'a');
			const content = {
				bio: null,
				born: new DateTime('1969-07-20T20:17:40.123456789Z'),
				balance: new Decimal('79228162514264337593543950335'),
				tags: new Set(['x', 'y']),
				meta: { kind: 'k', extra: 1 },
			};
			await within(
				engine.db.query('CREATE $id CONTENT $content', { id, content }),
			);
			const [rows] = await within(
				engine.db.query<[unknown[]]>('SELECT * FROM $id', { id }),
			);
			const row = Schema.decodeUnknownSync(S.NoneAsAbsent(Row))(rows[0]);
			expect('nickname' in row).toBe(false);
			expect(row.bio).toBeNull();
			expect(row.born.toCompact()).toEqual(content.born.toCompact());
			expect(row.balance.toString()).toBe(
				'79228162514264337593543950335',
			);
			expect([...row.tags].sort()).toEqual(['x', 'y']);
			expect(row.meta).toEqual({ kind: 'k', extra: 1 });
		});

		it('an option field set to NONE reads back absent', async () => {
			const id = new RecordId('presence', 'b');
			await within(
				engine.db.query(
					"CREATE $id CONTENT { nickname: 'n', bio: 'b', born: time::now(), balance: 1dec, tags: <set<string>> [], meta: { kind: 'k' } }",
					{ id },
				),
			);
			await within(
				engine.db.query('UPDATE $id MERGE { nickname: NONE }', { id }),
			);
			const [rows] = await within(
				engine.db.query<[unknown[]]>('SELECT * FROM $id', { id }),
			);
			const row = Schema.decodeUnknownSync(S.NoneAsAbsent(Row))(rows[0]);
			expect('nickname' in row).toBe(false);
		});

		it('a projection that yields NONE needs NoneAsAbsent', async () => {
			const [value] = await within(
				engine.db.query<[unknown]>(
					"RETURN { nickname: NONE, bio: 'b' }",
				),
			);
			const Projection = Schema.Struct({
				nickname: Schema.optionalKey(S.String),
				bio: S.String,
			});
			expect(() => Schema.decodeUnknownSync(Projection)(value)).toThrow();
			expect(
				Schema.decodeUnknownSync(S.NoneAsAbsent(Projection))(value),
			).toEqual({ bio: 'b' });
		});
	});
});
