/**
 * Arithmetic without an engine: the SurrealQL `add`, `sub`, `mul` and `div`
 * render, the codec each derives from its operands' kinds, and what is
 * refused. `test/arithmetic-engine.test.ts` decodes the engines' answers
 * with those codecs, and `test/conformance.test.ts` pins them.
 */
import { Schema } from 'effect';
import { Decimal as SdkDecimal } from 'surrealdb';
import { describe, expect, it } from 'vitest';

import * as SurrealSchema from '../src/index.js';
import {
	add,
	cast,
	codecOf,
	div,
	eq,
	gt,
	literal,
	mul,
	param,
	rowOf,
	select,
	sqlOf,
	sub,
	unsafeExpr,
} from '../src/statement/index.js';
import { Person, PersonTable } from './golden/corpus/index.js';

/** A builder with its types switched off, to reach its run-time checks. */
const untyped = <F>(f: F) =>
	f as unknown as (...args: ReadonlyArray<unknown>) => unknown;

const decode = (codec: unknown, value: unknown): unknown =>
	Schema.decodeUnknownSync(codec as Schema.Codec<unknown, unknown>)(value);

const i = param('i', SurrealSchema.Int);
const j = param('j', SurrealSchema.Int);
const f = param('f', SurrealSchema.Float);
const d = param('d', SurrealSchema.Decimal);
const n = param('n', SurrealSchema.SurrealNumber);
const big = param('big', SurrealSchema.Int64);

describe('arithmetic', () => {
	it('renders each operator, parenthesising what is not a term', () => {
		expect(sqlOf(add(i, j))).toBe('$i + $j');
		expect(sqlOf(sub(i, 1))).toBe('$i - 1');
		// A float binding is cast: the SDK sends a whole number as an int.
		expect(sqlOf(mul(add(i, j), f))).toBe('($i + $j) * <float>$f');
		expect(sqlOf(div(f, i))).toBe('<float>$f / $i');
		expect(sqlOf(div(i, sub(j, -2)))).toBe('$i / ($j - -2)');
		expect(sqlOf(gt(add(i, j), 3))).toBe('($i + $j) > 3');
		expect(
			select(PersonTable)
				.where((row) =>
					gt(mul(row.score, 2), param('min', SurrealSchema.Int)),
				)
				.value((row) => add(row.score, i))
				.build().sql,
		).toBe(
			'SELECT VALUE score + $i FROM ⟨person⟩ WHERE (score * 2) > $min;',
		);
	});

	it('collects the bindings of both operands', () => {
		expect(
			Object.keys(add(i, mul(f, d))[`~effect-surrealkit/Expr`].params),
		).toEqual(['i', 'f', 'd']);
	});

	it('derives the codec from the kinds: int, float, decimal, int64, any number', () => {
		expect(codecOf(add(i, j))).toBe(SurrealSchema.Int);
		expect(codecOf(add(i, 1))).toBe(SurrealSchema.Int);
		expect(codecOf(add(i, 1.5))).toBe(SurrealSchema.Float);
		expect(codecOf(add(i, f))).toBe(SurrealSchema.Float);
		expect(codecOf(mul(f, f))).toBe(SurrealSchema.Float);
		expect(codecOf(sub(i, d))).toBe(SurrealSchema.Decimal);
		expect(codecOf(add(f, d))).toBe(SurrealSchema.Decimal);
		expect(codecOf(add(big, i))).toBe(SurrealSchema.Int64);
		expect(codecOf(add(big, f))).toBe(SurrealSchema.Float);
		expect(codecOf(add(n, i))).toBe(SurrealSchema.SurrealNumber);
		expect(codecOf(add(cast('float', i), 1))).toBe(SurrealSchema.Float);
		expect(codecOf(add(cast('decimal', i), 1))).toBe(SurrealSchema.Decimal);
		// A refined field keeps its kind.
		const positive = param(
			'p',
			SurrealSchema.Int.check(Schema.isGreaterThan(0)),
		);
		expect(codecOf(add(positive, 1))).toBe(SurrealSchema.Int);
	});

	it('decodes a quotient as its kind or a float, which division by zero gives', () => {
		expect(decode(codecOf(div(i, j)), 3)).toBe(3);
		expect(decode(codecOf(div(i, j)), Number.NaN)).toBeNaN();
		expect(() => decode(codecOf(add(i, j)), Number.NaN)).toThrow();
		expect(decode(codecOf(div(f, f)), Number.POSITIVE_INFINITY)).toBe(
			Number.POSITIVE_INFINITY,
		);
		const third = new SdkDecimal('0.3333333333333333333333333333');
		expect(decode(codecOf(div(d, 3)), third)).toEqual(third);
		expect(decode(codecOf(div(d, 0)), Number.NaN)).toBeNaN();
		expect(decode(codecOf(div(big, i)), 3)).toBe(3n);
		expect(decode(codecOf(div(big, i)), Number.NaN)).toBeNaN();
	});

	it('refuses an operand SurrealDB refuses: possibly NONE, a string, or no number', () => {
		const person = rowOf(Person);
		expect(() => untyped(add)(person.age, 1)).toThrow(
			'`age` may be NONE, and SurrealDB refuses NONE in arithmetic',
		);
		expect(() => untyped(add)(person.name, 1)).toThrow(
			'`name` is not a number: arithmetic takes an int, a float, a decimal or a number',
		);
		expect(() => untyped(mul)(literal(true), 1)).toThrow(/is not a number/);
		expect(() => untyped(add)('1', 1)).toThrow(/arithmetic takes numbers/);
		expect(() =>
			untyped(add)(unsafeExpr({ sql: 'x', codec: Schema.Unknown }), 1),
		).toThrow(/`x` is not a number/);
		expect(eq(add(i, 1), 2)).toBeDefined();
	});
});
