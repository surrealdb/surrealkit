import { DateTime as EffectDateTime, Schema } from 'effect';
import {
	BoundIncluded,
	DateTime,
	Decimal,
	Duration,
	FileRef,
	GeometryLine,
	GeometryPoint,
	Range,
	RecordId,
	Table,
	Uuid,
} from 'surrealdb';
import { describe, expect, it } from 'vitest';

import * as S from '../src/index.js';

const decode = <T, E>(schema: Schema.Codec<T, E>, input: unknown): T =>
	Schema.decodeUnknownSync(schema)(input);
const encode = <T, E>(schema: Schema.Codec<T, E>, input: unknown): E =>
	Schema.encodeUnknownSync(schema)(input);

describe('Int (safe-number profile)', () => {
	it('accepts safe integers in both directions', () => {
		for (const n of [
			0,
			1,
			-1,
			Number.MAX_SAFE_INTEGER,
			Number.MIN_SAFE_INTEGER,
		]) {
			expect(decode(S.Int, n)).toBe(n);
			expect(encode(S.Int, n)).toBe(n);
		}
	});

	it('fails loudly beyond ±(2^53 - 1), whether number or bigint', () => {
		for (const input of [
			2 ** 53,
			-(2 ** 53),
			9007199254740993n,
			-(2n ** 63n),
		]) {
			expect(() => decode(S.Int, input)).toThrow(
				/outside the safe JavaScript range/,
			);
			expect(() => encode(S.Int, input)).toThrow(
				/outside the safe JavaScript range/,
			);
		}
	});

	it('rejects non-integers and other types', () => {
		expect(() => decode(S.Int, 1.5)).toThrow(/not an integer/);
		expect(() => decode(S.Int, '1')).toThrow(/safe integer/);
	});
});

describe('Int64 (bigint profile)', () => {
	it('normalises the SDK number | bigint to a bigint', () => {
		expect(decode(S.Int64, 5)).toBe(5n);
		expect(decode(S.Int64, 2 ** 53)).toBe(2n ** 53n);
		expect(decode(S.Int64, 9007199254740993n)).toBe(9007199254740993n);
		expect(decode(S.Int64, S.INT64_MAX)).toBe(S.INT64_MAX);
		expect(decode(S.Int64, S.INT64_MIN)).toBe(S.INT64_MIN);
	});

	it('encodes to a bigint within int64', () => {
		expect(encode(S.Int64, S.int64(7n))).toBe(7n);
		expect(() => encode(S.Int64, 2n ** 63n)).toThrow(
			/outside the SurrealDB int range/,
		);
		expect(() => encode(S.Int64, -(2n ** 63n) - 1n)).toThrow(
			/outside the SurrealDB int range/,
		);
	});

	it('rejects numbers whose exact value is unknown', () => {
		// surrealdb.js 2.1.1 decodes -(2^53 + 1) as -2^53.
		expect(() => decode(S.Int64, -(2 ** 53))).toThrow(
			/exact value is unknown/,
		);
		expect(() => decode(S.Int64, 2 ** 60)).toThrow(
			/exact value is unknown/,
		);
		expect(() => decode(S.Int64, 0.5)).toThrow(/not an integer/);
	});

	it('int64() checks its argument', () => {
		expect(() => S.int64(2n ** 63n)).toThrow(RangeError);
	});

	it('Int64Literal matches exactly one value in either spelling', () => {
		const five = S.Int64Literal('5');
		expect(decode(five, 5)).toBe(5n);
		expect(decode(five, 5n)).toBe(5n);
		expect(() => decode(five, 6)).toThrow();
	});
});

describe('DateTime', () => {
	it('keeps nanoseconds', () => {
		const dt = new DateTime('2024-01-02T03:04:05.123456789Z');
		expect(decode(S.DateTime, dt)).toBe(dt);
		expect(encode(S.DateTime, dt).toCompact()).toEqual([
			1704164645n,
			123456789n,
		]);
	});

	it('rejects JS Dates and out-of-range datetimes', () => {
		expect(() => decode(S.DateTime, new Date())).toThrow(
			/SurrealDB DateTime/,
		);
		expect(() =>
			encode(S.DateTime, new DateTime([S.DATETIME_MAX_SECONDS + 1n, 0n])),
		).toThrow(/outside the SurrealDB datetime range/);
		expect(() =>
			encode(S.DateTime, new DateTime([S.DATETIME_MIN_SECONDS - 1n, 0n])),
		).toThrow(/outside the SurrealDB datetime range/);
		expect(
			decode(
				S.DateTime,
				new DateTime([S.DATETIME_MAX_SECONDS, 999999999n]),
			),
		).toBeInstanceOf(DateTime);
	});

	it('DateTimeEffect truncates to milliseconds, towards negative infinity', () => {
		const utc = decode(S.DateTimeEffect, new DateTime([1n, 999999999n]));
		expect(EffectDateTime.toEpochMillis(utc)).toBe(1999);
		const before = decode(
			S.DateTimeEffect,
			new DateTime([-1n, 500000001n]),
		);
		expect(EffectDateTime.toEpochMillis(before)).toBe(-500);
		const back = encode(S.DateTimeEffect, EffectDateTime.makeUnsafe(1999));
		expect(back).toBeInstanceOf(DateTime);
		expect(back.toCompact()).toEqual([1n, 999000000n]);
	});
});

describe('Duration', () => {
	it('round-trips nanosecond durations', () => {
		const d = new Duration('1h30m5ns');
		expect(decode(S.Duration, d)).toBe(d);
	});

	it('rejects negative durations (the engine would store 0ns)', () => {
		expect(() => encode(S.Duration, new Duration([-1n, 0n]))).toThrow(
			/negative/,
		);
	});

	it('rejects durations beyond u64 seconds', () => {
		expect(() =>
			encode(S.Duration, new Duration([S.DURATION_MAX_SECONDS + 1n, 0n])),
		).toThrow(/exceeds/);
	});

	it('DurationLiteral matches exactly that duration', () => {
		const day = S.DurationLiteral('1d');
		expect(decode(day, new Duration('24h'))).toBeInstanceOf(Duration);
		expect(() => decode(day, new Duration('1h'))).toThrow();
	});
});

describe('Decimal', () => {
	it('accepts the full engine range', () => {
		for (const text of [
			'79228162514264337593543950335',
			'-79228162514264337593543950335',
			'0.0000000000000000000000000001',
			'7.9228162514264337593543950335',
		]) {
			expect(decode(S.Decimal, new Decimal(text)).toString()).toBe(text);
		}
	});

	it('rejects a mantissa beyond 96 bits (the SDK would hang on the server error)', () => {
		expect(() =>
			encode(S.Decimal, new Decimal('79228162514264337593543950336')),
		).toThrow(/96-bit mantissa/);
	});

	it('rejects more than 28 fractional digits (the engine would round them away)', () => {
		expect(() =>
			encode(S.Decimal, new Decimal('0.00000000000000000000000000001')),
		).toThrow(/fractional digits/);
	});

	it('decimalFromString checks the grammar before the permissive SDK constructor', () => {
		expect(decimalString('1.50')).toBe('1.5');
		expect(decimalString('1e10')).toBe('10000000000');
		expect(() => S.decimalFromString('abc')).toThrow(/not a decimal/);
		expect(() => S.decimalFromString('1.2.3')).toThrow(/not a decimal/);
		expect(() => S.decimalFromString('')).toThrow(/not a decimal/);
		for (const text of [
			'.',
			'+',
			'e5',
			'1e',
			'+-1',
			'1.5f',
			' 1',
			'0x10',
		]) {
			expect(() => S.decimalFromString(text), text).toThrow(
				/not a decimal/,
			);
		}
	});

	it('decimalFromString reads signs, leading dots and exponents exactly', () => {
		// The SDK's string constructor turns '+1.5' into 0.5 and '+2' into 0,
		// and throws on '.5e1'; none of that text reaches it.
		expect(new Decimal('+1.5').toString()).toBe('0.5');
		for (const [text, value] of [
			['+1.5', '1.5'],
			['+2', '2'],
			['+.5', '0.5'],
			['.5e1', '5'],
			['-.5e1', '-5'],
			['-0.0', '0'],
			['-0', '0'],
			['+0.000', '0'],
			['1.', '1'],
			['-1.5e-1', '-0.15'],
			['12.5E+2', '1250'],
			['0e-99', '0'],
			['1.0e-28', '0.0000000000000000000000000001'],
			[
				'7.9228162514264337593543950335e28',
				'79228162514264337593543950335',
			],
			[
				'-79228162514264337593543950335',
				'-79228162514264337593543950335',
			],
		] as const) {
			expect(decimalString(text), text).toBe(value);
		}
		expect(S.decimalFromString('-0.5').equals(new Decimal('-0.5'))).toBe(
			true,
		);
		expect(decode(S.DecimalFromJson, '+1.5').toString()).toBe('1.5');
	});

	it('decimalFromString refuses text beyond the engine range without building it', () => {
		for (const text of [
			'1e29',
			'-79228162514264337593543950336',
			'1e999999999999',
		]) {
			expect(() => S.decimalFromString(text), text).toThrow(
				/96-bit mantissa/,
			);
		}
		for (const text of ['1e-29', '0.00000000000000000000000000001']) {
			expect(() => S.decimalFromString(text), text).toThrow(
				/fractional digits/,
			);
		}
	});

	it('never accepts JS numbers', () => {
		expect(() => decode(S.Decimal, 1.5)).toThrow(/SurrealDB Decimal/);
	});

	it('DecimalLiteral matches exactly that value', () => {
		const ten = S.DecimalLiteral('10');
		expect(decode(ten, new Decimal('10.0')).toString()).toBe('10');
		expect(() => decode(ten, new Decimal('10.1'))).toThrow();
	});
});

const decimalString = (text: string) => S.decimalFromString(text).toString();

describe('SurrealNumber', () => {
	it('accepts every numeric spelling the SDK produces', () => {
		expect(decode(S.SurrealNumber, 1.5)).toBe(1.5);
		expect(decode(S.SurrealNumber, 2n ** 60n)).toBe(2n ** 60n);
		expect(decode(S.SurrealNumber, new Decimal('1.5'))).toBeInstanceOf(
			Decimal,
		);
		expect(() => decode(S.SurrealNumber, 2n ** 64n)).toThrow(/int range/);
	});
});

describe('Uuid and Bytes', () => {
	it('Uuid accepts SDK Uuids only', () => {
		const uuid = new Uuid('0189a8f6-0000-7000-8000-000000000000');
		expect(decode(S.Uuid, uuid)).toBe(uuid);
		expect(() => decode(S.Uuid, uuid.toString())).toThrow(/Uuid/);
	});

	it('Bytes accepts the ArrayBuffer the SDK decodes and yields a Uint8Array', () => {
		const buffer = new Uint8Array([1, 2, 3]).buffer;
		const bytes = decode(S.Bytes, buffer);
		expect(bytes).toBeInstanceOf(Uint8Array);
		expect([...bytes]).toEqual([1, 2, 3]);
		expect(encode(S.Bytes, bytes)).toBe(bytes);
	});
});

describe('RecordIdFor', () => {
	const PersonId = S.RecordIdFor('person');

	it('checks the table by SDK structure', () => {
		const id = new RecordId('person', 'tobie');
		expect(decode(PersonId, id)).toBe(id);
		expect(() => decode(PersonId, new RecordId('team', 'x'))).toThrow(
			/expected a record id of table `person`, got one of table `team`/,
		);
		// Text is never parsed into an id.
		expect(() => decode(PersonId, 'person:tobie')).toThrow(
			/record id of table person/,
		);
	});

	it('accepts every SDK key shape by default', () => {
		for (const key of [
			'a',
			1,
			2n ** 60n,
			new Uuid('0189a8f6-0000-7000-8000-000000000000'),
			[1, 'a'],
			{ n: 1 },
		]) {
			expect(
				decode(PersonId, new RecordId('person', key)),
			).toBeInstanceOf(RecordId);
		}
	});

	it('decodes and encodes the key through a key schema', () => {
		const ObjectId = S.RecordIdFor('id_object', {
			key: Schema.Struct({ n: S.Int, region: Schema.String }),
		});
		const id = decode(
			ObjectId,
			new RecordId('id_object', { n: 1, region: 'eu' }),
		);
		expect(id.id).toEqual({ n: 1, region: 'eu' });
		expect(() =>
			decode(
				ObjectId,
				new RecordId('id_object', { n: 'one', region: 'eu' }),
			),
		).toThrow(/id/);

		const BigKey = S.RecordIdFor('counter', { key: S.Int64 });
		const big = decode(BigKey, new RecordId('counter', 5));
		expect(big.id).toBe(5n);
		expect(() =>
			decode(
				S.RecordIdFor('counter', { key: S.Int }),
				new RecordId('counter', 2n ** 60n),
			),
		).toThrow(/outside the safe JavaScript range/);
	});

	it('rejects an empty string key, which SurrealDB refuses', () => {
		const empty = new RecordId('person', '');
		for (const schema of [
			PersonId,
			S.RecordIdFor('person', { key: S.String }),
			S.RecordIdFor('person', {
				key: Schema.Union([S.Int, S.String]),
			}),
			S.RecordIdAny,
		]) {
			expect(() => decode(schema, empty)).toThrow(/empty string/);
			expect(() => encode(schema, empty)).toThrow(/empty string/);
		}
	});

	it('RecordIdAny takes any table', () => {
		expect(
			decode(S.RecordIdAny, new RecordId('anything', 1)),
		).toBeInstanceOf(RecordId);
	});

	it('decodes a key exactly: an object key with extra properties is refused', () => {
		const ObjectId = S.RecordIdFor('t', {
			key: Schema.Struct({ n: S.Int }),
		});
		// Rebuilding `t:{ n: 1 }` from `t:{ n: 1, extra: 2 }` would name
		// another record.
		expect(() =>
			decode(ObjectId, new RecordId('t', { n: 1, extra: 2 })),
		).toThrow(/extra/);
		expect(() =>
			encode(ObjectId, new RecordId('t', { n: 1, extra: 2 })),
		).toThrow(/extra/);
		const id = new RecordId('t', { n: 1 });
		expect(decode(ObjectId, id).equals(id)).toBe(true);
	});

	it('numeric keys are integers', () => {
		const fractional = new RecordId('t', 1.5);
		for (const schema of [S.RecordIdFor('t'), S.RecordIdAny]) {
			expect(() => decode(schema, fractional)).toThrow();
		}
		expect(() => decode(S.RecordKey, 1.5)).toThrow(/record id key/);
		expect(() => decode(S.RecordKey, Number.NaN)).toThrow(/record id key/);
		expect(decode(S.RecordKey, 2)).toBe(2);
	});
});

describe('taggedRecordId', () => {
	const OrgId = S.RecordIdFor('org');
	const TeamId = S.RecordIdFor('team');
	const Owner = S.taggedRecordId({ team: TeamId, org: OrgId });

	it('tags a record id by its table', () => {
		const value = decode(Owner, new RecordId('team', 'a'));
		expect(value._tag).toBe('team');
		expect(value.id.table.name).toBe('team');
		expect(() => decode(Owner, new RecordId('person', 'a'))).toThrow();
	});

	it('encodes back to the bare id and rejects a tag/table mismatch', () => {
		const id = new RecordId('org', 'x');
		expect(encode(Owner, { _tag: 'org', id })).toBe(id);
		expect(() => encode(Owner, { _tag: 'team', id })).toThrow();
	});
});

describe('TableFor and FileRefFor', () => {
	it('TableFor checks the table name', () => {
		const T = S.TableFor(['a', 'b']);
		expect(decode(T, new Table('a')).name).toBe('a');
		expect(() => decode(T, new Table('c'))).toThrow(/not one of a, b/);
		expect(decode(S.TableFor([]), new Table('z')).name).toBe('z');
	});

	it('FileRefFor checks the bucket', () => {
		const F = S.FileRefFor(['avatars']);
		expect(decode(F, new FileRef('avatars', '/a.png'))).toBeInstanceOf(
			FileRef,
		);
		expect(() => decode(F, new FileRef('docs', '/a.png'))).toThrow(
			/bucket `docs`/,
		);
	});
});

describe('Geometry', () => {
	it('checks the geometry kind', () => {
		const point = new GeometryPoint([1, 2]);
		expect(decode(S.GeometryPoint, point)).toBe(point);
		expect(decode(S.Geometry, point)).toBe(point);
		const line = new GeometryLine([point, new GeometryPoint([3, 4])]);
		expect(() => decode(S.GeometryPoint, line)).toThrow(
			/line is not one of point/,
		);
		expect(decode(S.GeometryOf(['point', 'line']), line)).toBe(line);
	});
});

describe('Range', () => {
	it('accepts SDK ranges', () => {
		const range = new Range(new BoundIncluded(1), undefined);
		expect(decode(S.Range, range)).toBe(range);
		expect(() => decode(S.Range, [1, 2])).toThrow(/range/);
	});
});

describe('SetOf', () => {
	it('decodes elements and returns a ReadonlySet', () => {
		const set = decode(S.SetOf(S.Int64), new Set([1, 2n]));
		expect([...set]).toEqual([1n, 2n]);
	});

	it('rejects duplicates under SurrealDB equality', () => {
		const ids = S.SetOf(S.RecordIdAny);
		expect(() =>
			encode(ids, new Set([new RecordId('a', 1), new RecordId('a', 1)])),
		).toThrow(/duplicates/);
		const byReference = S.SetOf(S.RecordIdAny, { equality: 'reference' });
		expect(
			encode(
				byReference,
				new Set([new RecordId('a', 1), new RecordId('a', 1)]),
			).size,
		).toBe(2);
	});

	it('checks distinctness on its input, so a lossy item codec decodes a stored set', () => {
		const lossy = S.SetOf(S.DateTimeEffect);
		const stored = new Set([
			new DateTime('2024-01-01T00:00:00.000000001Z'),
			new DateTime('2024-01-01T00:00:00.000000002Z'),
		]);
		// Equal at millisecond precision once decoded, distinct as stored.
		expect(decode(lossy, stored).size).toBe(2);
		const same = EffectDateTime.makeUnsafe(Date.UTC(2024, 0, 1));
		expect(() =>
			encode(
				lossy,
				new Set([
					same,
					EffectDateTime.makeUnsafe(Date.UTC(2024, 0, 1)),
				]),
			),
		).toThrow(/duplicates/);
	});

	it('enforces an exact length', () => {
		const three = S.SetOf(S.Int, { length: 3 });
		expect(decode(three, new Set([1, 2, 3])).size).toBe(3);
		expect(() => encode(three, new Set([1, 2]))).toThrow(/exactly 3/);
		expect(() => encode(three, new Set([1, 2, 3, 4]))).toThrow(/exactly 3/);
	});

	it('rejects arrays', () => {
		expect(() => decode(S.SetOf(S.Int), [1, 2])).toThrow(/a set/);
	});
});

describe('NoneAsAbsent', () => {
	const Row = S.NoneAsAbsent(
		Schema.Struct({
			a: Schema.optionalKey(Schema.String),
			b: Schema.NullOr(Schema.String),
			nested: Schema.Struct({ c: Schema.optionalKey(S.Int) }),
			list: Schema.Array(Schema.UndefinedOr(S.Int)),
		}),
	);

	it('reads undefined (NONE) properties as absent keys, keeps null', () => {
		const value = decode(Row, {
			a: undefined,
			b: null,
			nested: { c: undefined },
			list: [1, undefined],
		});
		expect(value).toEqual({ b: null, nested: {}, list: [1, undefined] });
		expect('a' in value).toBe(false);
		expect('c' in value.nested).toBe(false);
	});

	it('leaves SDK values untouched', () => {
		expect(S.stripNone(new RecordId('a', 1))).toBeInstanceOf(RecordId);
	});

	it('reaches into set elements', () => {
		const Tagged = S.NoneAsAbsent(
			Schema.Struct({
				s: S.SetOf(
					Schema.Struct({ a: Schema.optionalKey(Schema.String) }),
				),
			}),
		);
		const value = decode(Tagged, {
			s: new Set([{ a: undefined }, { a: 'x' }]),
		});
		expect([...value.s]).toEqual([{}, { a: 'x' }]);
		expect('a' in ([...value.s][0] ?? { a: 1 })).toBe(false);
	});

	it('encodes exactly like the wrapped schema: undefined still removes', () => {
		const Merge = Schema.Struct({
			nickname: Schema.optional(Schema.String),
			name: Schema.optionalKey(Schema.String),
		});
		const wrapped = encode(S.NoneAsAbsent(Merge), { nickname: undefined });
		expect(wrapped).toEqual(encode(Merge, { nickname: undefined }));
		expect('nickname' in (wrapped as object)).toBe(true);
	});

	it('plain optionalKey schemas reject the undefined property', () => {
		expect(() =>
			decode(Schema.Struct({ a: Schema.optionalKey(Schema.String) }), {
				a: undefined,
			}),
		).toThrow();
	});
});

describe('Unsupported', () => {
	it('explains why regex has no value representation', () => {
		expect(() => decode(S.Unsupported('regex'), /a/)).toThrow(
			/no CBOR transport/,
		);
	});
});

describe('JSON codec world', () => {
	it('DateTimeFromJson keeps nanoseconds', () => {
		const dt = decode(S.DateTimeFromJson, '2024-01-02T03:04:05.123456789Z');
		expect(dt.toCompact()).toEqual([1704164645n, 123456789n]);
		expect(encode(S.DateTimeFromJson, dt)).toBe(
			'2024-01-02T03:04:05.123456789Z',
		);
		expect(() => decode(S.DateTimeFromJson, 'yesterday')).toThrow(
			/RFC 3339/,
		);
	});

	it('DecimalFromJson goes through text only', () => {
		expect(decode(S.DecimalFromJson, '1.50').toString()).toBe('1.5');
		expect(encode(S.DecimalFromJson, new Decimal('2.25'))).toBe('2.25');
		expect(() => decode(S.DecimalFromJson, 'abc')).toThrow(/not a decimal/);
		expect(() => decode(S.DecimalFromJson, 1.5)).toThrow();
	});

	it('DurationFromJson and UuidFromJson check their grammar', () => {
		expect(decode(S.DurationFromJson, '1h30m').toString()).toBe('1h30m');
		expect(() => decode(S.DurationFromJson, 'soon')).toThrow(
			/not a duration/,
		);
		const uuid = '0189a8f6-0000-7000-8000-000000000000';
		expect(encode(S.UuidFromJson, decode(S.UuidFromJson, uuid))).toBe(uuid);
		expect(() => decode(S.UuidFromJson, 'nope')).toThrow(/not a UUID/);
	});

	it('RecordIdFromJson parses SurrealQL record-id text', () => {
		const PersonFromJson = S.RecordIdFromJson('person');
		const id = decode(PersonFromJson, 'person:⟨tobie morgan⟩');
		expect(id.id).toBe('tobie morgan');
		const compound = decode(PersonFromJson, 'person:[1, "a"]');
		expect(compound.id).toEqual([1, 'a']);
		expect(() => decode(PersonFromJson, 'team:x')).toThrow(
			/table `person`/,
		);
		expect(encode(PersonFromJson, new RecordId('person', 'tobie'))).toBe(
			'person:tobie',
		);
	});

	it('Int64FromJson rejects JSON numbers JSON.parse already rounded', () => {
		expect(decode(S.Int64FromJson, 5)).toBe(5n);
		expect(decode(S.Int64FromJson, '9223372036854775807')).toBe(
			S.INT64_MAX,
		);
		expect(() => decode(S.Int64FromJson, 2 ** 60)).toThrow(
			/not a safe integer/,
		);
		expect(() => decode(S.Int64FromJson, '9223372036854775808')).toThrow(
			/int range/,
		);
		expect(encode(S.Int64FromJson, S.int64(5n))).toBe(5);
		expect(encode(S.Int64FromJson, S.INT64_MAX)).toBe(
			'9223372036854775807',
		);
	});

	it('BytesFromJson and SetFromJson use arrays', () => {
		expect([...decode(S.BytesFromJson, [1, 2])]).toEqual([1, 2]);
		expect(encode(S.BytesFromJson, new Uint8Array([3]))).toEqual([3]);
		const tags = S.SetFromJson(Schema.String);
		expect([...decode(tags, ['a', 'b'])]).toEqual(['a', 'b']);
		expect(() => decode(tags, ['a', 'a'])).toThrow(/duplicates/);
		expect(encode(tags, new Set(['x']))).toEqual(['x']);
	});

	// The texts below are what SurrealDB 3.2.4 and 3.3.0 render over HTTP /sql.
	it('reads record-id text, including string keys the server leaves unquoted', () => {
		const any = (text: string) => decode(S.RecordIdAnyFromJson, text);
		// Not numbers, durations or decimals: string keys.
		for (const key of ['0G', '1s', '1e5', '1dec', '9a', 'NaN', 'abc']) {
			expect(any(`p:${key}`).id).toBe(key);
		}
		expect(any('p:123').id).toBe(123);
		expect(any('p:`123`').id).toBe('123');
		expect(any('p:`-1`').id).toBe('-1');
		const odd = any('`odd name`:`k k`');
		expect([odd.table.name, odd.id]).toEqual(['odd name', 'k k']);
		expect(any("p:['x', d'2024-01-02T03:04:05.123456789Z']").id).toEqual([
			'x',
			new DateTime('2024-01-02T03:04:05.123456789Z'),
		]);
		expect(() => any('p')).toThrow(/not a record id/);
		const person = S.RecordIdFromJsonAs(S.RecordIdFor('person'));
		expect(decode(person, 'person:1s').id).toBe('1s');
		expect(() => decode(person, 'team:1')).toThrow();
		expect(decode(S.RecordIdFromJson('p'), 'p:0G').id).toBe('0G');
	});

	it('reads tables, files, ranges, geometry and numbers as the server renders them', () => {
		expect(decode(S.TableFromJson([]), 'odd name').name).toBe('odd name');
		expect(() => decode(S.TableFromJson(['a', 'b']), 'c')).toThrow();
		const file = decode(
			S.FileRefFromJson(['avatars']),
			'f"avatars:/dir/a.png"',
		);
		expect([file.bucket, file.key]).toEqual(['avatars', '/dir/a.png']);
		expect(encode(S.FileRefFromJson(['avatars']), file)).toBe(
			'f"avatars:/dir/a.png"',
		);
		expect(() =>
			decode(S.FileRefFromJson(['docs']), 'f"avatars:/dir/a.png"'),
		).toThrow();
		expect(String(decode(S.RangeFromJson, '1..=5'))).toBe('1..=5');
		const point = decode(S.GeometryFromJson(S.GeometryPoint), {
			type: 'Point',
			coordinates: [1.5, 2.5],
		});
		expect(point.toJSON()).toEqual({
			type: 'Point',
			coordinates: [1.5, 2.5],
		});
		expect(() =>
			decode(S.GeometryFromJson(S.GeometryLine), {
				type: 'Point',
				coordinates: [1.5, 2.5],
			}),
		).toThrow();
		expect(decode(S.SurrealNumberFromJson, 3)).toBe(3);
		expect(decode(S.SurrealNumberFromJson, '2.5').toString()).toBe('2.5');
		expect(() => decode(S.SurrealNumberFromJson, null)).toThrow();
		expect(
			decode(S.DateTimeEffectFromJson, '2024-01-02T03:04:05.123456789Z'),
		).toEqual(
			decode(S.DateTimeEffect, new DateTime('2024-01-02T03:04:05.123Z')),
		);
	});
});
