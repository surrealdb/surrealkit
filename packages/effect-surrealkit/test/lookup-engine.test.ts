/**
 * Lookups by a unique index whose field may hold an array, on whichever
 * engine the run uses (see `engine.ts`). SurrealDB 3.3 finds an array
 * through such an index only when the field is declared an array kind, and
 * 4.0 always does (`test/conformance.test.ts` pins both), so the builder
 * refuses an array lookup on any other field and every lookup it builds
 * finds the same row on both majors.
 */
import { Effect, Schema } from 'effect';
import { RecordId } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { type Client, make as makeClient } from '../src/client/index.js';
import * as SurrealSchema from '../src/index.js';
import { param, select } from '../src/statement/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';
import {
	LookupAnyTable,
	LookupArrayTable,
	LookupSetTable,
	LookupUnionTable,
	lookupSchema,
} from './lookups.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] lookup engine suite skipped: ${engineSkipReason}`,
	);
}

describe.skipIf(!engineConfigured)('unique lookups on the engine', () => {
	let engine: Engine;
	let client: Client;

	const ok = <A, E>(effect: Effect.Effect<A, E>): Promise<A> =>
		within(Effect.runPromise(effect), 20_000);

	beforeAll(async () => {
		engine = await startEngine();
		client = makeClient(engine.db);
		await engine.db.query(lookupSchema);
	});
	afterAll(async () => {
		await engine?.stop();
	});

	it('finds an array through an index on an array-only field, and a value that is not an array on any field', async () => {
		const ints = Schema.Array(SurrealSchema.Int);
		const byArray = await ok(
			client.run(
				select(LookupArrayTable, 'lk_array_f', {
					f: param('f', ints),
				}).build(),
				{ f: [1, 2] },
			),
		);
		expect(byArray?.id).toEqual(new RecordId('lk_array', 1));
		const byString = await ok(
			client.run(select(LookupAnyTable, 'lk_any_f', { f: 'x' }).build()),
		);
		expect(byString?.id).toEqual(new RecordId('lk_any', 2));
		const byStringParam = await ok(
			client.run(
				select(LookupUnionTable, 'lk_union_f', {
					f: param('f', SurrealSchema.String),
				}).build(),
				{ f: 'x' },
			),
		);
		expect(byStringParam?.id).toEqual(new RecordId('lk_union', 2));
		const bySet = await ok(
			client.run(
				select(LookupSetTable, 'lk_set_f', {
					f: param('f', SurrealSchema.SetOf(SurrealSchema.String)),
				}).build(),
				{ f: new Set(['x']) },
			),
		);
		expect(bySet?.id).toEqual(new RecordId('lk_set', 2));
	});

	it('refuses, when built, an array lookup that SurrealDB 3.3 answers with NONE', async () => {
		const untyped = select as unknown as (
			table: unknown,
			index: string,
			values: Record<string, unknown>,
		) => unknown;
		const ints = param('f', Schema.Array(SurrealSchema.Int));
		const refusal = (table: string) =>
			`\`f\` of the unique index \`${table}_f\` may hold an array or another kind, and SurrealDB 3.3 finds no array through such an index (4.0 does): look it up by a value that is not an array, or declare \`f\` an array kind only`;
		expect(() => untyped(LookupAnyTable, 'lk_any_f', { f: ints })).toThrow(
			refusal('lk_any'),
		);
		expect(() =>
			untyped(LookupAnyTable, 'lk_any_f', {
				f: param('f', SurrealSchema.SurrealValue),
			}),
		).toThrow(refusal('lk_any'));
		expect(() =>
			untyped(LookupUnionTable, 'lk_union_f', { f: ints }),
		).toThrow(refusal('lk_union'));
		expect(() => untyped(LookupSetTable, 'lk_set_f', { f: ints })).toThrow(
			refusal('lk_set'),
		);
		// What the refused lookup would have read: the row on 4.0, NONE on 3.3.
		const [raw] = await engine.db.query<[unknown]>(
			'SELECT VALUE id FROM ONLY lk_any WITH INDEX lk_any_f WHERE f = $f LIMIT 1;',
			{ f: [1, 2] },
		);
		expect(raw).toEqual(
			engine.majorVersion >= 4 ? new RecordId('lk_any', 1) : undefined,
		);
	});
});
