/**
 * Why projected fields are checked when modules are generated (lane L04):
 * on a SCHEMAFULL table the engine answers a SELECT of a field the table
 * does not declare without an error, the field missing from each row.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { analyzeSync } from '../src/analyze/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] projected-field engine suite skipped: ${engineSkipReason}`,
	);
}

describe.skipIf(!engineConfigured)(
	'a projected field a SCHEMAFULL table does not declare',
	() => {
		let engine: Engine;

		beforeAll(async () => {
			engine = await startEngine();
			await within(
				engine.db
					.query(
						"DEFINE TABLE person SCHEMAFULL; DEFINE FIELD name ON person TYPE string; CREATE person:1 SET name = 'Ada';",
					)
					.collect(),
			);
		});
		afterAll(async () => {
			await engine?.stop();
		});

		const first = async (sql: string): Promise<unknown> =>
			(await within(engine.db.query(sql).collect()))[0];

		it('comes back missing, without an error', async () => {
			expect(await first('SELECT nmae FROM person;')).toEqual([{}]);
			expect(await first('SELECT name, nmae FROM person;')).toEqual([
				{ name: 'Ada' },
			]);
			expect(await first('SELECT nmae FROM ONLY person:1;')).toEqual({});
		});

		it('is what the analyzer records for the schema check', () => {
			expect(
				analyzeSync('SELECT name, nmae FROM person;').projected,
			).toEqual([
				{ table: 'person', field: 'name', line: 1 },
				{ table: 'person', field: 'nmae', line: 1 },
			]);
		});
	},
);
