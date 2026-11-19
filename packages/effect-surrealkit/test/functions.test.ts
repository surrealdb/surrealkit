/**
 * The built-in function catalogue without an engine: the generated files
 * match the table, and the functions render, bind and type their arguments
 * and results as the table says. `functions-engine.test.ts` runs every
 * entry against SurrealDB.
 */
import { readdirSync, readFileSync } from 'node:fs';
import { join, relative } from 'node:path';

import { Schema } from 'effect';
import { Decimal } from 'surrealdb';
import { describe, expect, it } from 'vitest';

import {
	exportName,
	generate,
	generatedDir,
	loadTable,
	packageRoot,
} from '../scripts/generate-functions.mjs';
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
import * as SurrealSchema from '../src/index.js';
import type { CodecOf } from '../src/statement/expr.js';
import { codecOf, eq, param, paramsOf, sqlOf } from '../src/statement/index.js';

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
});
