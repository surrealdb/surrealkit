/**
 * Projected fields against an engine (lane L04). Why they are checked when
 * modules are generated: on a SCHEMAFULL table the engine answers a SELECT
 * of a field the table does not declare without an error, the field missing
 * from each row. Which bare words are values and not fields: every keyword
 * of the SurrealDB 3.3 and 4.0 parsers (`test/fixtures/surql-keywords.txt`,
 * the union of `surrealdb-syn` 3.3.0 `lexer/keywords.rs` and
 * `surrealdb-parser-next` 4.0.0 `kw.rs`), in capitals and in lower case,
 * with the spellings of `NaN` and `Infinity`, is projected alone from a
 * record that holds a field of that name, and the analyzer records it
 * exactly when the engine reads that field. And the stated limit: a row
 * written before its table became SCHEMAFULL still returns an undeclared
 * field.
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

		it('comes back missing through VALUE and an alias too', async () => {
			expect(await first('SELECT VALUE nmae FROM person;')).toEqual([
				undefined,
			]);
			expect(await first('SELECT nmae AS n FROM person;')).toEqual([{}]);
			expect(
				analyzeSync('SELECT VALUE nmae FROM person;').projected,
			).toEqual([{ table: 'person', field: 'nmae', line: 1 }]);
			expect(
				analyzeSync('SELECT nmae AS n FROM person;').projected,
			).toEqual([{ table: 'person', field: 'nmae', line: 1 }]);
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

const keywords = readFileSync(
	join(import.meta.dirname, 'fixtures', 'surql-keywords.txt'),
	'utf8',
)
	.split('\n')
	.filter((word) => word !== '');

/** The bare words both engines read as a value, not as a field. */
const VALUE_WORDS = [
	'FALSE',
	'False',
	'Infinity',
	'NONE',
	'NULL',
	'NaN',
	'None',
	'Null',
	'TRUE',
	'True',
	'false',
	'none',
	'null',
	'true',
];

describe.skipIf(!engineConfigured)('a lone bare word in a projection', () => {
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

	it('is recorded exactly when the engine reads the field of that name', async () => {
		const values: string[] = [];
		const refused: string[] = [];
		let fields = 0;
		for (const word of words) {
			const sql = `SELECT ${word} FROM w:1;`;
			let row: unknown;
			try {
				row = (
					(await within(engine.db.query(sql).collect()))[0] as [
						unknown,
					]
				)[0];
			} catch {
				// A statement word, which is not an expression on its own.
				continue;
			}
			const reads =
				typeof row === 'object' &&
				row !== null &&
				Object.keys(row).length === 1 &&
				(row as Record<string, unknown>)[word] === 'stored';
			const recorded = analyzeSync(sql).projected;
			// The analyzer may refuse a field (it records nothing), never
			// record what is not one.
			expect(recorded, sql).toEqual(
				reads && recorded.length > 0
					? [{ table: 'w', field: word, line: 1 }]
					: [],
			);
			if (reads && recorded.length === 0) refused.push(word);
			if (reads) fields++;
			else values.push(word);
		}
		expect(values.sort()).toEqual(VALUE_WORDS);
		// Fields the analyzer does not read: a field named `FROM` or `OMIT`
		// makes it uncertain where the projection ends.
		expect(refused.sort()).toEqual(['FROM', 'OMIT', 'from', 'omit']);
		expect(fields).toBeGreaterThan(700);
	}, 60_000);
});

describe.skipIf(!engineConfigured)(
	'a row written before its table became SCHEMAFULL',
	() => {
		let engine: Engine;

		beforeAll(async () => {
			engine = await startEngine();
			for (const table of ['loose', 'unset', 'declare'])
				await within(
					engine.db
						.query(
							`DEFINE TABLE ${table} SCHEMALESS; DEFINE FIELD name ON ${table} TYPE string; CREATE ${table}:1 SET name = 'Ada', extra = 'old'; DEFINE TABLE OVERWRITE ${table} SCHEMAFULL;`,
						)
						.collect(),
				);
		});
		afterAll(async () => {
			await engine?.stop();
		});

		const last = async (sql: string): Promise<unknown> =>
			(await within(engine.db.query(sql).collect())).at(-1);

		it('still returns a field the table does not declare: the stated limit', async () => {
			expect(await last('SELECT extra FROM loose;')).toEqual([
				{ extra: 'old' },
			]);
		});

		it('stops once the field is cleared or declared', async () => {
			expect(
				await last(
					'UPDATE unset UNSET extra; SELECT extra FROM unset;',
				),
			).toEqual([{}]);
			expect(
				await last(
					'DEFINE FIELD extra ON declare TYPE option<string>; SELECT extra FROM declare;',
				),
			).toEqual([{ extra: 'old' }]);
		});
	},
);
