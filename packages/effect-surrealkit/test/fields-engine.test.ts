/**
 * Compared fields against an engine: why a comparison in a `WHERE` of a
 * field a SCHEMAFULL table does not declare is worth recording, and the reading
 * of each rule the analyzer uses to record one (`test/fields.test.ts`).
 * Every fact is pinned on the engine `SURREAL_BIN` or `SURREAL_URL` names
 * (checked on SurrealDB 3.3 and 4.0).
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

		it('reads the subquery’s rows, never the table’s, where FROM has a subquery', async () => {
			// `n` and `owner` are not fields of person: the outer rows are the
			// subquery's results (records it returns are fetched).
			for (const [sql, rows] of [
				[
					"SELECT VALUE n FROM (SELECT name AS n FROM person) WHERE n = 'Ada';",
					['Ada'],
				],
				[
					"SELECT VALUE owner FROM (SELECT VALUE id FROM pet) WHERE owner = 'Ada';",
					['Ada'],
				],
				[
					"SELECT VALUE n FROM person, (SELECT name AS n FROM person) WHERE n = 'Ada';",
					['Ada'],
				],
				[
					"SELECT VALUE n FROM ONLY (SELECT name AS n FROM ONLY person:1) WHERE n = 'Ada';",
					'Ada',
				],
			] as const) {
				expect(await first(sql), sql).toEqual(rows);
				expect(compared(sql), sql).toEqual([]);
			}
			const bound =
				'SELECT VALUE n FROM (SELECT name AS n FROM person) WHERE n = $x;';
			expect(
				(
					await within(engine.db.query(bound, { x: 'Ada' }).collect())
				)[0],
			).toEqual(['Ada']);
			expect(analyzeSync(bound).uses).toEqual({});
		});

		it('parses a statement word before `:` only as a key of an object literal', async () => {
			// What the readers' statement split rests on (`statements.ts`):
			// a record id of a table named like a statement word is a parse
			// error, so such a word at a statement's start is an object key.
			for (const word of [
				'select',
				'CREATE',
				'update',
				'UPSERT',
				'delete',
				'INSERT',
				'relate',
			]) {
				await expect(
					within(engine.db.query(`RETURN ${word}:1;`).collect()),
				).rejects.toThrow(/Parse error/u);
				expect(await first(`RETURN { ${word}: 1 };`)).toEqual({
					[word]: 1,
				});
			}
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

const assigned = (sql: string) =>
	analyzeSync(sql)
		.fields.filter(({ role }) => role === 'assign')
		.map(({ table, field }) => `${table}.${field}`);

describe.skipIf(!engineConfigured)(
	'an assigned field a SCHEMAFULL table does not declare',
	() => {
		let engine: Engine;

		beforeAll(async () => {
			engine = await startEngine();
			await within(
				engine.db
					.query(
						"DEFINE TABLE person SCHEMAFULL; DEFINE FIELD name ON person TYPE string; CREATE person:1 SET name = 'Ada'; DEFINE TABLE pet SCHEMAFULL; DEFINE FIELD owner ON pet TYPE string; CREATE pet:1 SET owner = 'Ada';",
					)
					.collect(),
			);
		});
		afterAll(async () => {
			await engine?.stop();
		});

		const run = (sql: string) => within(engine.db.query(sql).collect());

		it.each([
			"'x'",
			'"x"',
			"d'2020-01-01T00:00:00Z'",
			'1',
			'1.5',
			'-1',
			'1d',
			'10dec',
			'true',
			'FALSE',
			'NULL',
			'[]',
			"[1, 'a']",
			'pet:1',
			"pet:[1, 'a']",
		])('fails on every record it writes when the value is %s, which is recorded', async (value) => {
			for (const sql of [
				`UPDATE person:1 SET nmae = ${value};`,
				`UPSERT person:1 SET name = 'Ada', nmae = ${value};`,
				`CREATE person:2 SET name = 'Bo', nmae = ${value};`,
				`IF true { UPDATE person:1 SET nmae = ${value} };`,
			]) {
				await expect(run(sql), sql).rejects.toThrow(
					/no such field exists for table 'person'/u,
				);
				expect(assigned(sql), sql).toContain('person.nmae');
			}
		});

		it.each([
			'NONE',
			'none',
			'$none',
			'1 AND NONE',
			'[1][5]',
			"'a'.nope",
		])('succeeds when the value %s is NONE, which is not recorded', async (value) => {
			const sql = `LET $none = NONE; UPDATE person:1 SET nmae = ${value};`;
			const results = await run(sql);
			expect(results.at(-1), sql).toEqual([
				{ id: expect.anything(), name: 'Ada' },
			]);
			expect(assigned(sql), sql).toEqual([]);
		});

		it('succeeds on UNSET of an undeclared field, which is not recorded', async () => {
			const sql = 'UPDATE person:1 UNSET nmae;';
			expect((await run(sql))[0]).toEqual([
				{ id: expect.anything(), name: 'Ada' },
			]);
			expect(analyzeSync(sql).fields).toEqual([]);
		});

		it('clears a field a row kept from before its table became SCHEMAFULL, with NONE or UNSET', async () => {
			for (const [table, clear] of [
				['legacy_none', 'SET extra = NONE'],
				['legacy_unset', 'UNSET extra'],
			] as const) {
				await run(
					`DEFINE TABLE ${table} SCHEMALESS; DEFINE FIELD name ON ${table} TYPE string; CREATE ${table}:1 SET name = 'Ada', extra = 'old'; DEFINE TABLE OVERWRITE ${table} SCHEMAFULL;`,
				);
				const sql = `UPDATE ${table}:1 ${clear}; SELECT * FROM ${table}:1;`;
				expect((await run(sql)).at(-1), sql).toEqual([
					{ id: expect.anything(), name: 'Ada' },
				]);
				expect(analyzeSync(sql).fields, sql).toEqual([]);
			}
		});
	},
);

describe.skipIf(!engineConfigured)('a lone bare word assigned in SET', () => {
	let engine: Engine;
	const words = [
		...new Set([
			...keywords,
			...keywords.map((word) => word.toLowerCase()),
			...['NaN', 'Infinity', 'True', 'False', 'None', 'Null'],
		]),
	];

	beforeAll(async () => {
		engine = await startEngine();
		await within(engine.db.query('DEFINE TABLE w SCHEMALESS;').collect());
	});
	afterAll(async () => {
		await engine?.stop();
	});

	it('is recorded only when the engine writes the field of that name', async () => {
		let fields = 0;
		const refused: string[] = [];
		for (const [n, word] of words.entries()) {
			const sql = `UPSERT w:${n} SET ${word} = 'stored' RETURN AFTER;`;
			let rows: unknown;
			try {
				rows = (await within(engine.db.query(sql).collect()))[0];
			} catch {
				continue;
			}
			const row = Array.isArray(rows) ? rows[0] : undefined;
			const writes =
				typeof row === 'object' &&
				row !== null &&
				(row as Record<string, unknown>)[word] === 'stored' &&
				Object.keys(row).length === 2;
			const recorded = assigned(sql);
			if (!writes) expect(recorded, sql).toEqual([]);
			else if (recorded.length === 0) refused.push(word);
			else expect(recorded, sql).toEqual([`w.${word}`]);
			if (writes) fields++;
		}
		// Fields the analyzer does not read: words that end an assignment or
		// start a clause, the words it reads as values elsewhere, and SELECT,
		// which makes the target uncertain.
		const kept = [
			'AND',
			'CONTENT',
			'EXPLAIN',
			'FETCH',
			'GROUP',
			'LIMIT',
			'MERGE',
			'OMIT',
			'OR',
			'ORDER',
			'PARALLEL',
			'PATCH',
			'REPLACE',
			'RETURN',
			'SELECT',
			'SET',
			'SPLIT',
			'START',
			'TEMPFILES',
			'TIMEOUT',
			'UNSET',
			'VERSION',
			'WHERE',
			'WITH',
			'NONE',
			'NULL',
			'TRUE',
			'FALSE',
		];
		expect(refused.sort()).toEqual(
			[
				...kept,
				...kept.map((word) => word.toLowerCase()),
				...['None', 'Null', 'True', 'False'],
			].sort(),
		);
		expect(fields).toBeGreaterThan(600);
	}, 120_000);
});

describe.skipIf(!engineConfigured)(
	'a local the text fixes to one table with type::record',
	() => {
		let engine: Engine;

		beforeAll(async () => {
			engine = await startEngine();
			await within(
				engine.db
					.query(
						"DEFINE TABLE person SCHEMAFULL; DEFINE FIELD name ON person TYPE string; CREATE person:1 SET name = 'Ada'; DEFINE TABLE pet SCHEMAFULL; DEFINE FIELD owner ON pet TYPE string; CREATE pet:1 SET owner = 'Ada';",
					)
					.collect(),
			);
		});
		afterAll(async () => {
			await engine?.stop();
		});

		const run = (sql: string) => within(engine.db.query(sql).collect());
		const last = async (sql: string) => (await run(sql)).at(-1);
		const fix = "LET $x = type::record('person', 1);\n";

		it('is read, written and deleted from as that table, an undeclared field as elsewhere', async () => {
			let sql = `${fix}SELECT name, nmae FROM ONLY $x;`;
			expect(await last(sql)).toEqual({ name: 'Ada' });
			expect(projected(sql)).toEqual(['person.name', 'person.nmae']);
			sql = `${fix}SELECT name FROM ONLY $x FOR UPDATE;`;
			expect(await last(sql)).toEqual({ name: 'Ada' });
			expect(projected(sql)).toEqual(['person.name']);
			sql = `${fix}UPDATE $x SET nmae = 'a';`;
			await expect(run(sql)).rejects.toThrow(
				/no such field exists for table 'person'/u,
			);
			expect(assigned(sql)).toEqual(['person.nmae']);
			sql = `${fix}DELETE $x WHERE nmae = 'Ada' RETURN BEFORE;`;
			expect(await last(sql)).toEqual([]);
			expect(compared(sql)).toEqual(['person.nmae']);
		});

		it('is a record of that table whatever the key', async () => {
			for (const key of [
				'pet:1',
				"[1, 'a']",
				"'pet:1'",
				'NONE',
				'{ a: 1 }',
			])
				expect(
					await last(
						`RETURN record::tb(type::record('person', ${key}));`,
					),
					key,
				).toBe('person');
		});

		it('holds what its one top-level LET gives it, and nothing else can set it', async () => {
			// A LET in a block is the block's own.
			expect(
				await last(
					`${fix}IF true { LET $x = type::record('pet', 1); }; RETURN record::tb($x);`,
				),
			).toBe('person');
			expect(
				await last(
					"IF true { LET $y = type::record('pet', 1); }; RETURN $y;",
				),
			).toBeUndefined();
			// A second LET replaces it, which is why one LET is required.
			expect(
				await last(
					`${fix}LET $x = type::record('pet', 1); RETURN record::tb($x);`,
				),
			).toBe('pet');
			// There is no assignment without LET.
			await expect(
				run(`${fix}$x = type::record('pet', 1);`),
			).rejects.toThrow(/Parse error/u);
			// A closure reads it.
			expect(
				await last(
					`${fix}RETURN array::map([1], |$v| record::tb($x));`,
				),
			).toEqual(['person']);
		});

		it('is not read in a function body, which the versions resolve differently', async () => {
			const { version } = await engine.db.version();
			const seen = await last(
				`${fix}DEFINE FUNCTION fn::seen() { RETURN $x; }; RETURN fn::seen();`,
			);
			// 3.3 gives a function the caller's LETs; 4.0 does not.
			if (version.includes('-3.')) expect(String(seen)).toBe('person:1');
			else expect(seen).toBeUndefined();
			expect(
				projected(
					`${fix}DEFINE FUNCTION fn::f() { RETURN SELECT nmae FROM ONLY $x; };`,
				),
			).toEqual([]);
		});

		it('type::thing is not a function on either version', async () => {
			await expect(
				run("RETURN type::thing('person', 1);"),
			).rejects.toThrow(/Parse error/u);
		});
	},
);

const projected = (sql: string) =>
	analyzeSync(sql).projected.map(({ table, field }) => `${table}.${field}`);
