/**
 * Arithmetic through a real engine: each statement is evaluated with bound
 * operands and its result decoded strictly by the codec `add`, `sub`, `mul`
 * or `div` derived, so a kind the engine answers that the codec does not
 * expect fails here. `test/conformance.test.ts` pins the engine's rules.
 */
import { Effect, Schema } from 'effect';
import { Decimal } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
	type Client,
	make as makeClient,
	StatementRequestError,
} from '../src/client/index.js';
import * as SurrealSchema from '../src/index.js';
import {
	add,
	div,
	evaluate,
	mul,
	param,
	select,
	sub,
} from '../src/statement/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] arithmetic engine suite skipped: ${engineSkipReason}`,
	);
}

const i = param('i', SurrealSchema.Int);
const j = param('j', SurrealSchema.Int);
const f = param('f', SurrealSchema.Float);
const d = param('d', SurrealSchema.Decimal);
const big = param('big', SurrealSchema.Int64);

const ItemId = SurrealSchema.RecordIdFor('ar_item');
const Item = Schema.Struct({
	id: ItemId,
	count: SurrealSchema.Int,
	price: SurrealSchema.Decimal,
	weight: SurrealSchema.Float,
});
const ItemTable = {
	name: 'ar_item',
	schemafull: true,
	id: ItemId,
	select: Item,
	visible: Item,
} as const;

describe.skipIf(!engineConfigured)('arithmetic on an engine', () => {
	let engine: Engine;
	let client: Client;
	const ok = <A, E>(effect: Effect.Effect<A, E>): Promise<A> =>
		within(Effect.runPromise(effect), 20_000);

	beforeAll(async () => {
		engine = await startEngine();
		await within(
			engine.db.query(`
				DEFINE TABLE ar_item SCHEMAFULL;
				DEFINE FIELD count ON ar_item TYPE int;
				DEFINE FIELD price ON ar_item TYPE decimal;
				DEFINE FIELD weight ON ar_item TYPE float;
				CREATE ar_item:a SET count = 3, price = 2.50dec, weight = 1.5;
			`),
			20_000,
		);
		client = makeClient(engine.db);
	});
	afterAll(async () => {
		await engine?.stop();
	});

	const value = <A>(
		expr: Parameters<typeof evaluate>[0],
		input: Record<string, unknown>,
	) =>
		ok(
			client.run(
				evaluate(expr, 'arithmetic', { writes: false }) as never,
				input as never,
			),
		) as Promise<A>;

	it('an int with an int is an int, with a float a float, with a decimal a decimal', async () => {
		expect(await value(add(i, j), { i: 2, j: 3 })).toBe(5);
		expect(await value(sub(i, j), { i: 2, j: 3 })).toBe(-1);
		expect(await value(mul(i, f), { i: 2, f: 1.25 })).toBe(2.5);
		expect(await value(add(i, 1), { i: 2 })).toBe(3);
		expect(await value(add(i, 0.5), { i: 2 })).toBe(2.5);
		const sum = await value<Decimal>(add(d, i), {
			d: new Decimal('0.1'),
			i: 2,
		});
		expect(sum).toBeInstanceOf(Decimal);
		expect(String(sum)).toBe('2.1');
		expect(
			await value(add(big, i), {
				big: SurrealSchema.int64(2n ** 60n),
				i: 1,
			}),
		).toBe(2n ** 60n + 1n);
	});

	it('an int quotient truncates toward zero; division by zero is NaN or Infinity', async () => {
		expect(await value(div(i, j), { i: 7, j: 2 })).toBe(3);
		expect(await value(div(i, j), { i: -7, j: 2 })).toBe(-3);
		expect(await value(div(i, j), { i: 7, j: 0 })).toBeNaN();
		expect(await value(div(f, 0), { f: 1.5 })).toBe(
			Number.POSITIVE_INFINITY,
		);
		// A whole number bound as a float divides as a float.
		expect(await value(div(f, 2), { f: 7 })).toBe(3.5);
		expect(await value(div(i, f), { i: 7, f: 2 })).toBe(3.5);
		expect(
			(
				await value<Decimal>(div(d, i), { d: new Decimal('7'), i: 2 })
			).equals(new Decimal('3.5')),
		).toBe(true);
		expect(await value(div(d, i), { d: new Decimal('7'), i: 0 })).toBeNaN();
	});

	it('an int that overflows fails the statement as a request', async () => {
		const error = await within(
			Effect.runPromise(
				Effect.flip(
					client.run(
						evaluate(add(big, big), 'overflow', { writes: false }),
						{ big: SurrealSchema.int64(2n ** 62n) },
					),
				),
			),
		);
		expect(error).toBeInstanceOf(StatementRequestError);
		expect(error.message).toMatch(/Cannot perform addition/);
	});

	it('reads fields of the row in a projection', async () => {
		const priced = select(ItemTable)
			.project((row) => ({
				total: mul(row.price, row.count),
				heavy: mul(row.weight, row.count),
				each: div(row.count, 2),
			}))
			.build('totals', { writes: false });
		expect(priced.sql).toBe(
			'SELECT price * count AS total, weight * count AS heavy, count / 2 AS each FROM ⟨ar_item⟩;',
		);
		const [row] = await ok(client.run(priced));
		expect(row?.total?.equals(new Decimal('7.5'))).toBe(true);
		expect(row?.heavy).toBe(4.5);
		expect(row?.each).toBe(1);
	});
});
