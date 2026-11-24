/**
 * Where a NONE binding assigned to a field that rejects NONE is guarded:
 * the text runs without an error when the binding is NONE, so a
 * `rejectsNone` entry, which refuses a codec that can hold NONE, would
 * refuse valid SurrealQL. The analyzer takes an assignment for a binding
 * use only where nothing in the text can skip it; these are the engine
 * facts on each lane (SurrealDB 4.0 embedded, 3.3 with `SURREAL_BIN`).
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
		`[effect-surrealkit] guarded assignment engine suite skipped: ${engineSkipReason}`,
	);
}

/** The fields `$x` is recorded as assigned to, as `table.field`. */
const assigned = (sql: string): ReadonlyArray<string> =>
	(analyzeSync(sql).uses.x ?? []).flatMap((use) =>
		use.kind === 'field' && use.role === 'assign'
			? [`${use.table}.${use.field}`]
			: [],
	);

describe.skipIf(!engineConfigured)(
	'a NONE binding assigned to a field that rejects NONE',
	() => {
		let engine: Engine;

		beforeAll(async () => {
			engine = await startEngine();
			await within(
				engine.db
					.query(
						"DEFINE TABLE gauge SCHEMAFULL; DEFINE FIELD station ON gauge TYPE string; CREATE gauge:1 SET station = 'a'; DEFINE TABLE site SCHEMAFULL; DEFINE FIELD label ON site TYPE string; CREATE site:1 SET label = 'L';",
					)
					.collect(),
			);
		});
		afterAll(async () => {
			await engine?.stop();
		});

		/** Runs `sql` with `$x` NONE: the error's message, or undefined. */
		const withNone = async (sql: string): Promise<string | undefined> => {
			try {
				await within(engine.db.query(sql, { x: undefined }).collect());
				return undefined;
			} catch (error) {
				return error instanceof Error ? error.message : String(error);
			}
		};

		const station = async (): Promise<unknown> =>
			(
				await within(
					engine.db
						.query('SELECT VALUE station FROM gauge:1;')
						.collect(),
				)
			)[0];

		it('runs without an error where a condition can skip it, and is not a binding use there', async () => {
			for (const sql of [
				'IF $x != NONE { (UPDATE gauge SET station = $x) };',
				'IF $x != NONE { LET $r = (UPDATE gauge SET station = $x); RETURN $r; };',
				'IF $x = NONE { 1 } ELSE { (UPDATE gauge SET station = $x) };',
				'RETURN IF $x != NONE THEN (UPDATE gauge SET station = $x) END;',
				'RETURN IF $x = NONE THEN 1 ELSE (UPDATE gauge SET station = $x) END;',
				'RETURN $x != NONE AND (UPDATE gauge SET station = $x);',
				'RETURN $x != NONE && (UPDATE gauge SET station = $x);',
				'RETURN $x = NONE OR (UPDATE gauge SET station = $x);',
				'SELECT (UPDATE gauge SET station = $x) AS u FROM site WHERE $x != NONE;',
				'RETURN [$x].filter(|$v| $v != NONE).map(|$v| (UPDATE gauge SET station = $x));',
			]) {
				expect(await withNone(sql), sql).toBeUndefined();
				expect(await station(), sql).toEqual(['a']);
				expect(assigned(sql), sql).toEqual([]);
			}
		});

		it('is refused where nothing can skip it, and is a binding use there', async () => {
			for (const sql of [
				'UPDATE gauge SET station = $x;',
				'(UPDATE gauge SET station = $x);',
				'LET $r = (UPDATE gauge SET station = $x);',
				'RETURN (UPDATE gauge SET station = $x);',
				'LET $r = UPDATE gauge SET station = $x;',
			]) {
				expect(await withNone(sql), sql).toMatch(/Expected `string`/u);
				expect(assigned(sql), sql).toEqual(['gauge.station']);
			}
		});

		it('is refused in operands that do not skip it, which the analyzer cannot tell apart', async () => {
			// `??` and `?:` evaluate their right side when the left is NONE,
			// and an early RETURN in a top-level IF block does not end the
			// text; the first two are not read, the last is a statement.
			for (const sql of [
				'RETURN $x ?? (UPDATE gauge SET station = $x);',
				'RETURN $x ?: (UPDATE gauge SET station = $x);',
			]) {
				expect(await withNone(sql), sql).toMatch(/Expected `string`/u);
				expect(assigned(sql), sql).toEqual([]);
			}
			const early =
				'IF $x = NONE { RETURN 1 }; UPDATE gauge SET station = $x;';
			expect(await withNone(early)).toMatch(/Expected `string`/u);
			expect(assigned(early)).toEqual(['gauge.station']);
		});
	},
);
