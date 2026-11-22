/**
 * The built-in function catalogue without an engine: the generated files
 * match the table, and the functions render, bind and type their arguments
 * and results as the table says. `functions-engine.test.ts` runs every
 * entry against SurrealDB.
 */
import { readdirSync, readFileSync } from 'node:fs';
import { join, relative } from 'node:path';

import { Schema } from 'effect';
import * as Surreal from 'surrealdb';
import { Decimal } from 'surrealdb';
import { describe, expect, it } from 'vitest';

import {
	exportName,
	generate,
	generatedDir,
	loadTable,
	packageRoot,
} from '../scripts/generate-functions.mjs';
import * as C from '../src/functions/generated/codecs.js';
import {
	array,
	math,
	object,
	parse,
	rand,
	string,
	time,
	type,
	vector,
} from '../src/functions/index.js';
import { ArrayCodec } from '../src/functions/internal.js';
import * as SurrealSchema from '../src/index.js';
import type { CodecOf, Expr } from '../src/statement/expr.js';
import {
	codecOf,
	eq,
	evaluate,
	param,
	paramsOf,
	sqlOf,
} from '../src/statement/index.js';

type Exact<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;

describe('the generated catalogue', () => {
	it('is what the generator makes of the table (run `npm run generate:functions`)', () => {
		const expected = generate(loadTable());
		const onDisk = readdirSync(generatedDir)
			.map((file) => relative(packageRoot, join(generatedDir, file)))
			.sort();
		expect(onDisk).toEqual([...expected.keys()].sort());
		for (const [path, text] of expected) {
			expect(
				readFileSync(join(packageRoot, path), 'utf8'),
				`${path} is out of date`,
			).toBe(text);
		}
	}, 60_000);

	it('names functions by family and camelCase', () => {
		expect(exportName('string::is_email')).toBe('isEmail');
		expect(exportName('vector::distance::euclidean')).toBe(
			'distanceEuclidean',
		);
		expect(exportName('array::sort::asc')).toBe('sortAsc');
		expect(exportName('math::log10_2')).toBe('log10_2');
		expect(exportName('rand::enum')).toBe('enum_');
	});
});

describe('built-in functions', () => {
	it('bind expressions and inline primitives as literals', () => {
		const text = param('text', SurrealSchema.String);
		const expr = string.slice(text, 1, 3n);
		expect(sqlOf(expr)).toBe('string::slice($text, 1, 3)');
		expect(paramsOf(expr)).toEqual({ text: SurrealSchema.String });
		expect(sqlOf(string.len('it"s'))).toBe('string::len(s"it\\"s")');
		expect(sqlOf(array.len([1, 'a', null]))).toBe(
			'array::len([ 1, s"a", NULL ])',
		);
	});

	it('leave out trailing optional arguments, and write NONE before a given one', () => {
		expect(sqlOf(string.slice('abc'))).toBe('string::slice(s"abc")');
		expect(sqlOf(string.slice('abc', undefined, 2))).toBe(
			'string::slice(s"abc", NONE, 2)',
		);
		expect(sqlOf(time.now())).toBe('time::now()');
	});

	it('take any number of values for a variadic argument', () => {
		expect(sqlOf(string.join('-'))).toBe('string::join(s"-")');
		expect(
			sqlOf(string.join('-', 'a', param('b', SurrealSchema.String))),
		).toBe('string::join(s"-", s"a", $b)');
		expect(sqlOf(rand.enum_('a', 'b'))).toBe('rand::enum(s"a", s"b")');
	});

	it('write constants without parentheses', () => {
		expect(sqlOf(math.pi)).toBe('math::pi');
		expect(codecOf(math.pi)).toBe(SurrealSchema.Float);
	});

	it('parenthesise an argument that is not a single term', () => {
		const flag = param('flag', Schema.Boolean);
		expect(sqlOf(type.isBool(eq(flag, true)))).toBe(
			'type::is_bool(($flag = true))',
		);
	});

	it('refuse a constant with no faithful literal', () => {
		expect(() => string.len('\ud800')).toThrow();
		expect(() => math.abs(Number.NaN)).toThrow(
			/no portable SurrealQL literal/,
		);
		expect(() => math.abs(2n ** 63n)).toThrow();
	});

	it('type results by the table, deriving them from an argument where it says so', () => {
		const names = param('names', Schema.Array(SurrealSchema.String));
		const first = array.first(names);
		const sorted = array.sort(names);
		const length = string.len(param('s', SurrealSchema.String));
		const distance = vector.distanceEuclidean([1, 2], [3, 4]);
		const firstType: Exact<
			CodecOf<typeof first>['Type'],
			string | undefined
		> = true;
		const sortedType: Exact<
			CodecOf<typeof sorted>['Type'],
			ReadonlyArray<string>
		> = true;
		const lengthType: Exact<CodecOf<typeof length>['Type'], number> = true;
		expect([firstType, sortedType, lengthType]).toEqual([true, true, true]);
		expect(Schema.decodeUnknownSync(codecOf(first))(undefined)).toBe(
			undefined,
		);
		expect(codecOf(sorted)).toBe(codecOf(names));
		expect(sqlOf(distance)).toBe(
			'vector::distance::euclidean([ 1, 2 ], [ 3, 4 ])',
		);
	});

	it('type a float|decimal result as a float unless an argument it names may hold a decimal', () => {
		const decimal = param('d', SurrealSchema.Decimal);
		const int = param('i', SurrealSchema.Int);
		const number = param('n', SurrealSchema.SurrealNumber);
		const decimals = param('ds', Schema.Array(SurrealSchema.Decimal));
		const floats = param('fs', Schema.Array(SurrealSchema.Float));
		const rootOfDecimal = math.sqrt(decimal);
		const rootOfNumber = math.sqrt(number);
		const rootOfInt = math.sqrt(int);
		const rootOfConstant = math.sqrt(2);
		const meanOfDecimals = math.mean(decimals);
		const meanOfFloats = math.mean(floats);
		const lerpByDecimal = math.lerp(0, 10, decimal);
		const cosine = vector.similarityCosine(floats, decimals);
		const types: ReadonlyArray<true> = [
			true satisfies Exact<
				CodecOf<typeof rootOfDecimal>['Type'],
				number | Decimal
			>,
			true satisfies Exact<
				CodecOf<typeof rootOfNumber>['Type'],
				number | Decimal
			>,
			true satisfies Exact<CodecOf<typeof rootOfInt>['Type'], number>,
			true satisfies Exact<
				CodecOf<typeof rootOfConstant>['Type'],
				number
			>,
			true satisfies Exact<
				CodecOf<typeof meanOfDecimals>['Type'],
				number | Decimal
			>,
			true satisfies Exact<CodecOf<typeof meanOfFloats>['Type'], number>,
			true satisfies Exact<
				CodecOf<typeof lerpByDecimal>['Type'],
				number | Decimal
			>,
			true satisfies Exact<
				CodecOf<typeof cosine>['Type'],
				number | Decimal
			>,
		];
		expect(types).toHaveLength(8);
		const decodes = (expr: Parameters<typeof codecOf>[0], value: unknown) =>
			Schema.is(codecOf(expr))(value);
		const half = new Decimal('0.5');
		expect(decodes(rootOfDecimal, half)).toBe(true);
		expect(decodes(rootOfNumber, half)).toBe(true);
		expect(decodes(meanOfDecimals, half)).toBe(true);
		expect(decodes(lerpByDecimal, half)).toBe(true);
		expect(decodes(cosine, half)).toBe(true);
		expect(decodes(rootOfDecimal, 0)).toBe(true);
		expect(decodes(rootOfInt, half)).toBe(false);
		expect(decodes(rootOfConstant, half)).toBe(false);
		expect(decodes(meanOfFloats, half)).toBe(false);
		expect(decodes(math.lerp(int, 10, 0.5), half)).toBe(false);
	});

	it('type a function that may answer NONE as optional', () => {
		const host = parse.emailHost(param('e', SurrealSchema.String));
		const median = math.median([]);
		const total = vector.sum([]);
		const types: ReadonlyArray<true> = [
			true satisfies Exact<
				CodecOf<typeof host>['Type'],
				string | undefined
			>,
			true satisfies Exact<
				CodecOf<typeof median>['Type'],
				number | bigint | Decimal | undefined
			>,
		];
		expect(types).toHaveLength(2);
		expect(Schema.is(codecOf(host))(undefined)).toBe(true);
		expect(Schema.is(codecOf(median))(undefined)).toBe(true);
		expect(Schema.is(codecOf(total))(undefined)).toBe(true);
	});

	it('refuse arguments of the wrong kind at compile time', () => {
		const count = param('count', SurrealSchema.Int);
		// @ts-expect-error an int is not a string
		string.len(count);
		// @ts-expect-error a string is not a number
		math.abs('one');
		// @ts-expect-error string::len takes one argument
		string.len('a', 'b');
		// @ts-expect-error string::repeat needs its count
		string.repeat('a');
		expect(() =>
			// @ts-expect-error an object is inlined only through `literal` with a codec
			object.keys({ a: 1 }),
		).toThrow(/only a primitive can be inlined/);
		expect(sqlOf(string.repeat('a', count))).toBe(
			'string::repeat(s"a", $count)',
		);
	});

	it('type array.concat by the elements of its arguments when each is a typed array', () => {
		const Note = Schema.Struct({
			kind: SurrealSchema.String,
			payload: SurrealSchema.String,
		});
		const Tally = Schema.Struct({
			kind: SurrealSchema.String,
			n: SurrealSchema.Int,
		});
		const early = param('early', Schema.Array(Note));
		const late = param('late', Schema.Array(Note));
		const tallies = param('tallies', Schema.Array(Tally));
		const loose = param('loose', Schema.Array(SurrealSchema.SurrealValue));
		const notes = evaluate(array.concat(early, late), 'notes');
		const mixed = evaluate(array.concat(early, tallies), 'mixed');
		const open = evaluate(array.concat(early, loose), 'open');
		const inlined = array.concat(early, [1]);
		const alone = array.concat(early);
		type NoteRow = typeof Note.Type;
		type TallyRow = typeof Tally.Type;
		const types: ReadonlyArray<true> = [
			true satisfies Exact<
				(typeof notes)['result']['Type'],
				ReadonlyArray<NoteRow>
			>,
			true satisfies Exact<
				(typeof mixed)['result']['Type'],
				ReadonlyArray<NoteRow | TallyRow>
			>,
			true satisfies Exact<(typeof open)['result'], typeof ArrayCodec>,
			true satisfies Exact<CodecOf<typeof inlined>, typeof ArrayCodec>,
			true satisfies Exact<CodecOf<typeof alone>, CodecOf<typeof early>>,
		];
		expect(types.length).toBe(5);
		expect(notes.sql).toBe('RETURN array::concat($early, $late);');
		const rows = [
			{ kind: 'a', payload: 'x' },
			{ kind: 'b', payload: 'y' },
		];
		expect(Schema.decodeUnknownSync(notes.result)(rows)).toEqual(rows);
		expect(() =>
			Schema.decodeUnknownSync(notes.result)([{ kind: 'a' }]),
		).toThrow();
		expect(
			Schema.decodeUnknownSync(mixed.result)([
				...rows,
				{ kind: 'c', n: 2 },
			]),
		).toEqual([...rows, { kind: 'c', n: 2 }]);
		expect(codecOf(alone)).toBe(codecOf(early));
		expect(open.result).toBe(ArrayCodec);
		expect(codecOf(inlined)).toBe(ArrayCodec);
	});
	it('type the array functions that build arrays from their arguments by those arguments', () => {
		const Tag = Schema.Struct({ name: SurrealSchema.String });
		const tags = param('tags', Schema.Array(Tag));
		const scores = param('scores', Schema.Array(SurrealSchema.Int));
		const groups = param(
			'groups',
			Schema.Array(Schema.Array(SurrealSchema.Int)),
		);
		const tag = param('tag', Tag);
		const loose = param('loose', Schema.Array(SurrealSchema.SurrealValue));
		const anything = param('anything', SurrealSchema.SurrealValue);
		type TagRow = typeof Tag.Type;
		type TypeOf<E> = CodecOf<E>['Type'];
		const appended = array.append(tags, tag);
		const mixed = array.push(scores, tag);
		const added = array.add(tags, scores);
		const union = array.union(tags, scores);
		const and = array.logicalAnd(tags, scores);
		const flat = array.flatten(groups);
		const windows = array.windows(tags, 2);
		const pairs = array.combine(tags, scores);
		const columns = array.transpose(groups);
		const repeated = array.repeat(tag, 3);
		const types: ReadonlyArray<true> = [
			true satisfies Exact<
				TypeOf<typeof appended>,
				ReadonlyArray<TagRow>
			>,
			true satisfies Exact<
				TypeOf<typeof mixed>,
				ReadonlyArray<number | TagRow>
			>,
			true satisfies Exact<
				TypeOf<
					ReturnType<typeof array.prepend<typeof tags, typeof tag>>
				>,
				ReadonlyArray<TagRow>
			>,
			true satisfies Exact<
				TypeOf<
					ReturnType<typeof array.insert<typeof tags, typeof tag>>
				>,
				ReadonlyArray<TagRow>
			>,
			true satisfies Exact<
				TypeOf<ReturnType<typeof array.fill<typeof tags, typeof tag>>>,
				ReadonlyArray<TagRow>
			>,
			true satisfies Exact<
				TypeOf<typeof added>,
				ReadonlyArray<number | TagRow>
			>,
			true satisfies Exact<
				TypeOf<ReturnType<typeof array.add<typeof tags, typeof tag>>>,
				ReadonlyArray<TagRow>
			>,
			true satisfies Exact<
				TypeOf<typeof union>,
				ReadonlyArray<number | TagRow>
			>,
			true satisfies Exact<
				TypeOf<
					ReturnType<
						typeof array.difference<typeof tags, typeof tags>
					>
				>,
				ReadonlyArray<TagRow>
			>,
			true satisfies Exact<
				TypeOf<typeof and>,
				ReadonlyArray<number | TagRow | null>
			>,
			true satisfies Exact<
				TypeOf<
					ReturnType<typeof array.logicalOr<typeof tags, typeof tags>>
				>,
				ReadonlyArray<TagRow | null>
			>,
			true satisfies Exact<
				TypeOf<
					ReturnType<
						typeof array.logicalXor<typeof tags, typeof tags>
					>
				>,
				ReadonlyArray<TagRow | null>
			>,
			true satisfies Exact<TypeOf<typeof flat>, ReadonlyArray<number>>,
			true satisfies Exact<
				TypeOf<ReturnType<typeof array.group<typeof groups>>>,
				ReadonlyArray<number>
			>,
			true satisfies Exact<
				TypeOf<typeof windows>,
				ReadonlyArray<ReadonlyArray<TagRow>>
			>,
			true satisfies Exact<
				TypeOf<ReturnType<typeof array.clump<typeof tags, 2>>>,
				ReadonlyArray<ReadonlyArray<TagRow>>
			>,
			true satisfies Exact<
				TypeOf<typeof pairs>,
				ReadonlyArray<readonly [TagRow, number]>
			>,
			true satisfies Exact<
				TypeOf<typeof columns>,
				ReadonlyArray<ReadonlyArray<number | undefined>>
			>,
			true satisfies Exact<
				TypeOf<typeof repeated>,
				ReadonlyArray<TagRow>
			>,
		];
		expect(types.length).toBe(19);
		const fallbacks = {
			append: array.append(loose, tag),
			appendOpen: array.append(tags, anything),
			appendInlined: array.append(tags, 1),
			add: array.add(tags, loose),
			union: array.union(tags, loose),
			and: array.logicalAnd(tags, [1]),
			flat: array.flatten(tags),
			flatOpen: array.flatten(loose),
			windows: array.windows(loose, 2),
			pairs: array.combine(loose, tags),
			columns: array.transpose(tags),
			repeated: array.repeat(anything, 3),
			repeatedInlined: array.repeat('a', 3),
		};
		type F = typeof fallbacks;
		const untyped: ReadonlyArray<true> = [
			true satisfies Exact<CodecOf<F['append']>, typeof ArrayCodec>,
			true satisfies Exact<CodecOf<F['appendOpen']>, typeof ArrayCodec>,
			true satisfies Exact<
				CodecOf<F['appendInlined']>,
				typeof ArrayCodec
			>,
			true satisfies Exact<CodecOf<F['add']>, typeof ArrayCodec>,
			true satisfies Exact<CodecOf<F['union']>, typeof ArrayCodec>,
			true satisfies Exact<CodecOf<F['and']>, typeof ArrayCodec>,
			true satisfies Exact<CodecOf<F['flat']>, typeof ArrayCodec>,
			true satisfies Exact<CodecOf<F['flatOpen']>, typeof ArrayCodec>,
			true satisfies Exact<
				CodecOf<F['windows']>,
				typeof C.ArrayOfArrayCodec
			>,
			true satisfies Exact<
				CodecOf<F['pairs']>,
				typeof C.ArrayOfArrayCodec
			>,
			true satisfies Exact<
				CodecOf<F['columns']>,
				typeof C.ArrayOfArrayCodec
			>,
			true satisfies Exact<CodecOf<F['repeated']>, typeof ArrayCodec>,
			true satisfies Exact<
				CodecOf<F['repeatedInlined']>,
				typeof ArrayCodec
			>,
		];
		expect(untyped.length).toBe(13);
		for (const [name, expr] of Object.entries(fallbacks) as ReadonlyArray<
			[string, Expr]
		>) {
			const expected = ['windows', 'pairs', 'columns'].includes(name)
				? C.ArrayOfArrayCodec
				: ArrayCodec;
			expect(codecOf(expr), name).toBe(expected);
		}
		expect(sqlOf(appended)).toBe('array::append($tags, $tag)');
		expect(sqlOf(and)).toBe('array::logical_and($tags, $scores)');
		expect(sqlOf(fallbacks.appendInlined)).toBe('array::append($tags, 1)');
		const decode = (expr: Expr) =>
			Schema.decodeUnknownSync(codecOf(expr) as Schema.Codec<unknown>);
		expect(decode(mixed)([1, { name: 'a' }])).toEqual([1, { name: 'a' }]);
		expect(() => decode(mixed)(['a'])).toThrow();
		expect(() => decode(appended)([1])).toThrow();
		expect(decode(and)([{ name: 'a' }, null])).toEqual([
			{ name: 'a' },
			null,
		]);
		expect(() => decode(and)([undefined])).toThrow();
		expect(decode(columns)([[1, undefined]])).toEqual([[1, undefined]]);
		expect(() => decode(flat)([[1]])).toThrow();
		expect(decode(pairs)([[{ name: 'a' }, 1]])).toEqual([
			[{ name: 'a' }, 1],
		]);
		expect(() => decode(pairs)([[1, { name: 'a' }]])).toThrow();
		expect(decode(repeated)([])).toEqual([]);
	});
});

// SDK 2.2.2 negotiates cbor.2, over which SurrealDB 4 returns these results as
// their own SDK classes. Each pair is what SDK 2.2.2 received from SurrealDB
// 4.0.0 and what SDK 2.1.1 received for the same query over cbor.
describe('results SurrealDB 4 returns as cbor.2 values', () => {
	const at = param('at', SurrealSchema.DateTime);
	const n = param('n', SurrealSchema.Int);
	const v = param('v', Schema.Unknown);
	const results = {
		age: time.age(at, at),
		makeDate: time.makeDate(n, n, n),
		makeTime: time.makeTime(n, n, n),
		makeTimestamp: time.makeTimestamp(n, n, n, n, n, n),
		date: type.date(v),
		time: type.time(v),
		timestamp: type.timestamp(v),
		interval: type.interval(v),
		f16: type.f16(v),
		f32: type.f32(v),
		i8: type.i8(v),
		i16: type.i16(v),
		i32: type.i32(v),
		u8: type.u8(v),
		u16: type.u16(v),
		u32: type.u32(v),
		u64: type.u64(v),
	};
	type R = typeof results;
	const decode = (expr: Expr) =>
		Schema.decodeUnknownSync(codecOf(expr) as Schema.Codec<unknown>);
	const dateTime = (seconds: number, nanos: number) =>
		new Surreal.DateTime([BigInt(seconds), BigInt(nanos)]);
	const duration = (seconds: number, nanos: number) =>
		new Surreal.Duration([BigInt(seconds), BigInt(nanos)]);

	it('keep the result types of the table', () => {
		const exact: ReadonlyArray<true> = [
			true satisfies Exact<CodecOf<R['age']>, typeof C.StringCodec>,
			true satisfies Exact<
				CodecOf<R['makeDate']>,
				typeof C.DateTimeCodec
			>,
			true satisfies Exact<
				CodecOf<R['makeTime']>,
				typeof C.DurationCodec
			>,
			true satisfies Exact<
				CodecOf<R['makeTimestamp']>,
				typeof C.DateTimeCodec
			>,
			true satisfies Exact<CodecOf<R['date']>, typeof C.DateTimeCodec>,
			true satisfies Exact<CodecOf<R['time']>, typeof C.DurationCodec>,
			true satisfies Exact<
				CodecOf<R['timestamp']>,
				typeof C.DateTimeCodec
			>,
			true satisfies Exact<CodecOf<R['interval']>, typeof C.StringCodec>,
			true satisfies Exact<CodecOf<R['f16']>, typeof C.FloatCodec>,
			true satisfies Exact<CodecOf<R['f32']>, typeof C.FloatCodec>,
			true satisfies Exact<CodecOf<R['i8']>, typeof C.IntCodec>,
			true satisfies Exact<CodecOf<R['i16']>, typeof C.IntCodec>,
			true satisfies Exact<CodecOf<R['i32']>, typeof C.IntCodec>,
			true satisfies Exact<CodecOf<R['u8']>, typeof C.IntCodec>,
			true satisfies Exact<CodecOf<R['u16']>, typeof C.IntCodec>,
			true satisfies Exact<CodecOf<R['u32']>, typeof C.IntCodec>,
			true satisfies Exact<CodecOf<R['u64']>, typeof C.IntCodec>,
		];
		expect(exact).toHaveLength(Object.keys(results).length);
	});

	it('keep the annotations and encoding of the base codecs', () => {
		const bases: Record<keyof R, Schema.Top> = {
			age: C.StringCodec,
			makeDate: C.DateTimeCodec,
			makeTime: C.DurationCodec,
			makeTimestamp: C.DateTimeCodec,
			date: C.DateTimeCodec,
			time: C.DurationCodec,
			timestamp: C.DateTimeCodec,
			interval: C.StringCodec,
			f16: C.FloatCodec,
			f32: C.FloatCodec,
			i8: C.IntCodec,
			i16: C.IntCodec,
			i32: C.IntCodec,
			u8: C.IntCodec,
			u16: C.IntCodec,
			u32: C.IntCodec,
			u64: C.IntCodec,
		};
		for (const [name, expr] of Object.entries(results) as ReadonlyArray<
			[keyof R, Expr]
		>) {
			const base = bases[name];
			expect(codecOf(expr), name).not.toBe(base);
			expect(codecOf(expr).ast.annotations, name).toBe(
				base.ast.annotations,
			);
		}
		const encode = (codec: Schema.Top, value: unknown) =>
			Schema.encodeUnknownSync(codec as Schema.Codec<unknown>)(value);
		const now = dateTime(1_704_190_272, 5);
		expect(encode(codecOf(results.date), now)).toBe(
			encode(C.DateTimeCodec, now),
		);
		expect(encode(codecOf(results.u64), 5)).toBe(5);
		expect(() =>
			encode(codecOf(results.u64), new Surreal.U64(5n)),
		).toThrow();
	});

	it('read an interval as the text cbor sends', () => {
		const cases: ReadonlyArray<[[number, number, bigint], string]> = [
			// type::interval('1 year 2 months 3 days 4 hours 5 minutes 6.789 seconds')
			[[14, 3, 14_706_789_000n], 'P1Y2M3DT4H5M6.789S'],
			// type::interval('-1 month 3 days')
			[[-1, 3, 0n], 'P-1M3D'],
			// type::interval('1 microsecond')
			[[0, 0, 1n], 'PT0.000001S'],
			// type::interval('0 days')
			[[0, 0, 0n], 'PT0S'],
			// time::age(d'2020-01-01T00:00:00Z', d'2024-06-01T00:00:00Z')
			[[-53, 0, 0n], 'P-4Y-5M'],
			// time::age(d'2024-01-31T10:00:00Z', d'2024-03-01T09:30:15.123456Z')
			[[-1, 0, -84_615_123_456n], 'P-1MT-23H-30M-15.123456S'],
		];
		for (const [parts, text] of cases) {
			for (const expr of [results.age, results.interval])
				expect(decode(expr)(new Surreal.Interval(parts))).toBe(text);
		}
		expect(decode(results.interval)('P1D')).toBe('P1D');
	});

	it('read dates, times and timestamps as the datetime or duration cbor sends', () => {
		for (const expr of [results.date, results.makeDate]) {
			expect(decode(expr)(new Surreal.PlainDate(19_724))).toEqual(
				dateTime(1_704_153_600, 0),
			);
			expect(decode(expr)(new Surreal.PlainDate(-1))).toEqual(
				dateTime(-86_400, 0),
			);
			// type::date('infinity') and type::date('-infinity')
			expect(decode(expr)(Surreal.PlainDate.INFINITY)).toEqual(
				dateTime(8_210_266_876_799, 999_999_999),
			);
			expect(decode(expr)(Surreal.PlainDate.NEG_INFINITY)).toEqual(
				dateTime(-8_334_601_228_800, 0),
			);
			expect(decode(expr)(dateTime(0, 0))).toEqual(dateTime(0, 0));
		}
		for (const expr of [results.time, results.makeTime]) {
			expect(
				decode(expr)(new Surreal.PlainTime(86_399_999_999_999)),
			).toEqual(duration(86_399, 999_999_999));
			expect(
				decode(expr)(new Surreal.PlainTime(86_400_000_000_000)),
			).toEqual(duration(86_400, 0));
			expect(decode(expr)(duration(1, 0))).toEqual(duration(1, 0));
		}
		for (const expr of [results.timestamp, results.makeTimestamp]) {
			expect(
				decode(expr)(new Surreal.Timestamp([-1, 500_000_000])),
			).toEqual(dateTime(-1, 500_000_000));
			expect(decode(expr)(Surreal.Timestamp.INFINITY)).toEqual(
				dateTime(8_210_266_876_799, 999_999_999),
			);
		}
		expect(() => decode(results.date)(new Surreal.PlainTime(0))).toThrow();
		expect(() => decode(results.time)(new Surreal.PlainDate(0))).toThrow();
	});

	it('read sized numbers as the float or safe integer cbor sends', () => {
		expect(decode(results.f16)(new Surreal.F16(0.0999755859375))).toBe(
			0.0999755859375,
		);
		expect(Object.is(decode(results.f32)(new Surreal.F32(-0)), -0)).toBe(
			true,
		);
		expect(decode(results.f32)(new Surreal.F32(Number.NaN))).toBeNaN();
		expect(decode(results.f32)(1.5)).toBe(1.5);
		const ints: ReadonlyArray<[Expr, Surreal.SizedNumber, number]> = [
			[results.i8, new Surreal.I8(-128), -128],
			[results.i16, new Surreal.I16(32_767), 32_767],
			[results.i32, new Surreal.I32(-2_147_483_648), -2_147_483_648],
			[results.u8, new Surreal.U8(255), 255],
			[results.u16, new Surreal.U16(65_535), 65_535],
			[results.u32, new Surreal.U32(4_294_967_295), 4_294_967_295],
			[results.u64, new Surreal.U64(0n), 0],
			[
				results.u64,
				new Surreal.U64(BigInt(Number.MAX_SAFE_INTEGER)),
				Number.MAX_SAFE_INTEGER,
			],
		];
		for (const [expr, value, expected] of ints)
			expect(decode(expr)(value)).toBe(expected);
		// Past the safe range a u64 stays a bigint, which Int refuses as before.
		const unsafe = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
		expect(() => decode(results.u64)(new Surreal.U64(unsafe))).toThrow(
			/safe JavaScript range/u,
		);
		expect(() => decode(results.u64)(unsafe)).toThrow(
			/safe JavaScript range/u,
		);
		expect(() => decode(results.i8)(new Surreal.F16(1))).toThrow();
		expect(() => decode(results.f16)(new Surreal.I8(1))).toThrow();
	});
});
