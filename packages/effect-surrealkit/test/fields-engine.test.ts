/**
 * Compared fields against an engine: why a field a SCHEMAFULL table does
 * not declare is checked when it is compared in a `WHERE`, and the reading
 * of each rule the analyzer uses to record one (`test/fields.test.ts`).
 * Every fact is pinned on the lane it runs on: SurrealDB 4.0 embedded, and
 * 3.3 with `SURREAL_BIN`.
 */
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

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
		`[effect-surrealkit] compared-field engine suite skipped: ${engineSkipReason}`,
	);
}

const compared = (sql: string) =>
	analyzeSync(sql)
		.fields.filter(({ role }) => role === 'compare')
		.map(({ table, field }) => `${table}.${field}`);

describe.skipIf(!engineConfigured)(
	'a compared field a SCHEMAFULL table does not declare',
	() => {
		let engine: Engine;

		beforeAll(async () => {
			engine = await startEngine();
			await within(
				engine.db
					.query(
						'DEFINE TABLE person SCHEMAFULL; DEFINE FIELD name ON person TYPE string; DEFINE FIELD age ON person TYPE int; CREATE person:1 SET name = "Ada", age = 36; DEFINE TABLE pet SCHEMAFULL; DEFINE FIELD owner ON pet TYPE string; CREATE pet:1 SET owner = "Ada";',
					)
					.collect(),
			);
		});
		afterAll(async () => {
			await engine?.stop();
		});

		const first = async (sql: string): Promise<unknown> =>
			(await within(engine.db.query(sql).collect()))[0];

		it('matches nothing, without an error, on either side', async () => {
			for (const sql of [
				"SELECT VALUE id FROM person WHERE nmae = 'Ada';",
				"SELECT VALUE id FROM person WHERE 'Ada' = nmae;",
				'SELECT VALUE id FROM person WHERE nmae = name;',
				"IF true { SELECT VALUE id FROM person WHERE nmae = 'Ada' };",
			]) {
				expect(await first(sql), sql).toEqual([]);
				expect(compared(sql), sql).toContain('person.nmae');
			}
			// The declared field, for contrast.
			expect(
				await first("SELECT VALUE id FROM person WHERE name = 'Ada';"),
			).toHaveLength(1);
		});

		it('names a field of the subquery’s own row in a subquery, never the outer row’s', async () => {
			// `name` is a field of person; in the subquery on pet it is pet's
			// own (absent) field, so nothing matches.
			const sql =
				'SELECT VALUE id FROM person WHERE name IN (SELECT VALUE owner FROM pet WHERE owner = name);';
			expect(await first(sql)).toEqual([]);
			expect(
				await first(
					'SELECT VALUE id FROM person WHERE name IN (SELECT VALUE owner FROM pet WHERE owner = $parent.name);',
				),
			).toHaveLength(1);
			expect(compared(sql)).toEqual(['pet.name', 'pet.owner']);
		});

		it('names the row’s field in a parenthesised closure body and in a cast', async () => {
			for (const sql of [
				'SELECT VALUE id FROM person WHERE array::any([36], |$v| (age = $v));',
				"SELECT VALUE id FROM person WHERE '36' = <string> age;",
			]) {
				expect(await first(sql), sql).toHaveLength(1);
				expect(compared(sql), sql).toEqual(['person.age']);
			}
		});

		it('names a field, never a projection alias, in WHERE', async () => {
			const sql = 'SELECT name AS label FROM person WHERE label = "Ada";';
			expect(await first(sql)).toEqual([]);
			expect(compared(sql)).toEqual(['person.label']);
		});

		it('a row written before its table became SCHEMAFULL still matches: the stated limit', async () => {
			await within(
				engine.db
					.query(
						"DEFINE TABLE loose SCHEMALESS; DEFINE FIELD name ON loose TYPE string; CREATE loose:1 SET name = 'Ada', extra = 'old'; DEFINE TABLE OVERWRITE loose SCHEMAFULL;",
					)
					.collect(),
			);
			const sql = "SELECT VALUE id FROM loose WHERE extra = 'old';";
			expect(await first(sql)).toHaveLength(1);
			expect(compared(sql)).toEqual(['loose.extra']);
		});
	},
);

const keywords = readFileSync(
	join(import.meta.dirname, 'fixtures', 'surql-keywords.txt'),
	'utf8',
)
	.split('\n')
	.filter((word) => word !== '');

describe.skipIf(!engineConfigured)('a lone bare word compared in WHERE', () => {
	let engine: Engine;
	const words = [
		...new Set([
			...keywords,
			...keywords.map((word) => word.toLowerCase()),
			...['NaN', 'nan', 'NAN', 'Nan', 'Infinity', 'infinity', 'INFINITY'],
			...['inf', 'Inf', 'True', 'False', 'None', 'Null'],
		]),
	];

	beforeAll(async () => {
		engine = await startEngine();
		await within(
			engine.db
				.query('DEFINE TABLE w SCHEMALESS; CREATE w:1 CONTENT $row;', {
					row: Object.fromEntries(
						words.map((word) => [word, 'stored']),
					),
				})
				.collect(),
		);
	});
	afterAll(async () => {
		await engine?.stop();
	});

	it.each([
		['on the left', (word: string) => `${word} = 'stored'`],
		['on the right', (word: string) => `'stored' = ${word}`],
	])(
		'is recorded %s only when the engine reads the field of that name',
		async (_, condition) => {
			let fields = 0;
			const refused: string[] = [];
			for (const word of words) {
				const sql = `SELECT VALUE id FROM w WHERE ${condition(word)};`;
				let rows: unknown;
				try {
					rows = (await within(engine.db.query(sql).collect()))[0];
				} catch {
					// Not an expression there: a parse error, so not valid text.
					continue;
				}
				const reads = Array.isArray(rows) && rows.length === 1;
				const recorded = compared(sql);
				// The analyzer may refuse a field, never record what is not one.
				if (!reads) expect(recorded, sql).toEqual([]);
				else if (recorded.length === 0) refused.push(word);
				else expect(recorded, sql).toEqual([`w.${word}`]);
				if (reads) fields++;
			}
			// Fields the analyzer does not read: words that end a comparison or
			// start a clause, which it never takes for an operand.
			const ending = [
				'AND',
				'CONTENT',
				'FETCH',
				'FROM',
				'GROUP',
				'LIMIT',
				'MERGE',
				'OMIT',
				'OR',
				'ORDER',
				'PARALLEL',
				'PATCH',
				'REPLACE',
				'SET',
				'SPLIT',
				'START',
				'TEMPFILES',
				'TIMEOUT',
				'UNSET',
				'VERSION',
				'WHERE',
				'WITH',
			];
			expect(refused.sort()).toEqual(
				[...ending, ...ending.map((word) => word.toLowerCase())].sort(),
			);
			expect(fields).toBeGreaterThan(600);
		},
		120_000,
	);
});
