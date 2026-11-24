/**
 * Field names a query compares in its `WHERE`, whatever the other operand,
 * and the check that fails generation when a `SCHEMAFULL` table does not
 * declare one. A comparison that is a binding use (`uses`) is not repeated
 * here. Every case the analyzer is not certain of records nothing; the
 * engine facts behind each rule are pinned in `test/fields-engine.test.ts`.
 */
import { join } from 'node:path';

import { Effect } from 'effect';
import { describe, expect, it } from 'vitest';

import {
	analyzeSync,
	type DeclaredNames,
	renderStatementModulesSync,
	StatementModulesError,
} from '../src/analyze/index.js';
import { declaredNames, decodeDocument } from '../src/emitter/index.js';
import { readJson } from './documents.js';

const fields = (sql: string) => analyzeSync(sql).fields;

const compare = (table: string, field: string, line = 1) => ({
	table,
	field,
	role: 'compare',
	line,
});

describe('analyze: compared fields', () => {
	it('a bare field on either side of each comparison, whatever the other operand', () => {
		expect(
			fields(
				"SELECT * FROM gauge WHERE station = 'a' AND 'b' != note OR reading <= 3 AND 4 == reading AND site > site:1 AND tags >= [] AND note < string::lowercase($x);",
			),
		).toEqual([
			compare('gauge', 'note'),
			compare('gauge', 'reading'),
			compare('gauge', 'site'),
			compare('gauge', 'station'),
			compare('gauge', 'tags'),
		]);
	});

	it('a field compared with another field, a call, a path, a subquery or $parent', () => {
		expect(fields('SELECT * FROM gauge WHERE reading > note;')).toEqual([
			compare('gauge', 'note'),
			compare('gauge', 'reading'),
		]);
		expect(
			fields('SELECT * FROM gauge WHERE string::len(note) > reading;'),
		).toEqual([compare('gauge', 'reading')]);
		expect(
			fields('SELECT * FROM gauge WHERE $row.reading = note;'),
		).toEqual([compare('gauge', 'note')]);
		expect(
			fields(
				'SELECT * FROM gauge WHERE station = (SELECT VALUE note FROM gauge LIMIT 1);',
			),
		).toEqual([compare('gauge', 'station')]);
		expect(
			fields(
				'SELECT * FROM gauge WHERE $x IN (SELECT VALUE id FROM site WHERE label = $parent.station);',
			),
		).toEqual([compare('site', 'label')]);
	});

	it('a bare word in a subquery against the subquery’s table, never the outer one', () => {
		// Both engines read `station` there as a field of `site`.
		expect(
			fields(
				'SELECT * FROM gauge WHERE $x IN (SELECT VALUE id FROM site WHERE label = station);',
			),
		).toEqual([compare('site', 'label'), compare('site', 'station')]);
	});

	it('a cast operand, a parenthesised comparison in a closure body, and an alias of the projection', () => {
		expect(
			fields("SELECT * FROM gauge WHERE 'a' = <string> note;"),
		).toEqual([compare('gauge', 'note')]);
		// Both engines read a bare word in a closure body as the row's field.
		expect(
			fields(
				'SELECT * FROM gauge WHERE array::any(tags, |$t| (note = $t));',
			),
		).toEqual([compare('gauge', 'note')]);
		// An alias is not visible in WHERE: `n` is a field of `gauge`.
		expect(fields('SELECT note AS n FROM gauge WHERE n = 1;')).toEqual([
			compare('gauge', 'n'),
		]);
	});

	it('in DELETE, UPDATE and UPSERT, before FOR UPDATE, and at the line of the field', () => {
		for (const verb of ['DELETE', 'UPDATE', 'UPSERT'])
			expect(fields(`${verb} gauge WHERE station = 'a';`)).toEqual([
				compare('gauge', 'station'),
			]);
		expect(
			fields("SELECT * FROM gauge:1 WHERE station = 'a' FOR UPDATE;"),
		).toEqual([compare('gauge', 'station')]);
		expect(
			fields(
				"SELECT * FROM gauge\nWHERE 'a'\n\t= station\nAND note = 1;",
			),
		).toEqual([
			compare('gauge', 'station', 3),
			compare('gauge', 'note', 4),
		]);
	});

	it('in a statement in a block, where a parameter is not taken for a binding use', () => {
		const sql = 'IF $x { DELETE gauge WHERE station = $s RETURN NONE; };';
		expect(fields(sql)).toEqual([compare('gauge', 'station')]);
		expect(analyzeSync(sql).uses).toEqual({});
	});

	it('not again for a comparison that is a binding use', () => {
		const sql = 'SELECT * FROM gauge WHERE station = $s AND $r < reading;';
		expect(fields(sql)).toEqual([]);
		expect(Object.keys(analyzeSync(sql).uses)).toEqual(['r', 's']);
		// A local is not a binding, so its comparison is a field name only.
		expect(
			fields("LET $s = 'a'; SELECT * FROM gauge WHERE station = $s;"),
		).toEqual([compare('gauge', 'station')]);
	});

	describe('records nothing', () => {
		it.each([
			['two targets', 'SELECT * FROM gauge, site WHERE station = 1;'],
			[
				'the outer query of a subquery of another table',
				'SELECT * FROM gauge WHERE station = (SELECT VALUE label FROM site LIMIT 1);',
			],
			['an open target', 'SELECT * FROM $g WHERE station = 1;'],
			[
				'a type::table of a parameter',
				'SELECT * FROM type::table($t) WHERE station = 1;',
			],
			[
				'a FOR variable',
				'FOR $g IN $gs { DELETE $g WHERE station = 1; };',
			],
			[
				'FOR UPDATE on a table',
				'SELECT * FROM gauge WHERE station = 1 FOR UPDATE;',
			],
			['NONE', 'SELECT * FROM gauge WHERE NONE = 1;'],
			['true', 'SELECT * FROM gauge WHERE 1 = true;'],
			['NULL', 'SELECT * FROM gauge WHERE NULL != 1;'],
			['NaN', 'SELECT * FROM gauge WHERE NaN = 1;'],
			['Infinity', 'SELECT * FROM gauge WHERE 1 < Infinity;'],
			['a path', 'SELECT * FROM gauge WHERE site.label = 1;'],
			[
				'a path on the right',
				'SELECT * FROM gauge WHERE 1 = site.label;',
			],
			['a parameter path', 'SELECT * FROM gauge WHERE $p.x = 1;'],
			[
				'$this and $parent',
				'SELECT * FROM gauge WHERE $this.note = $parent;',
			],
			['a call', 'SELECT * FROM gauge WHERE count() = 1;'],
			['a custom function', 'SELECT * FROM gauge WHERE fn::f(1) = 1;'],
			['an index', 'SELECT * FROM gauge WHERE tags[0] = 1;'],
			[
				'a record id on the right',
				'SELECT * FROM gauge WHERE 1 = site:x;',
			],
			['a graph step', 'SELECT * FROM gauge WHERE ->edge = 1;'],
			[
				'a graph step on the right',
				'SELECT * FROM gauge WHERE 1 = <-edge;',
			],
			[
				'an operand of arithmetic',
				'SELECT * FROM gauge WHERE reading + 1 = 2;',
			],
			[
				'arithmetic on the right',
				'SELECT * FROM gauge WHERE 2 = reading + 1;',
			],
			['a negated operand', 'SELECT * FROM gauge WHERE 2 = -reading;'],
			['?=', 'SELECT * FROM gauge WHERE tags ?= 1;'],
			['*=', 'SELECT * FROM gauge WHERE tags *= 1;'],
			['@@', "SELECT * FROM gauge WHERE note @@ 'a';"],
			['@1@', "SELECT * FROM gauge WHERE note @1@ 'a';"],
			['a knn operator', 'SELECT * FROM gauge WHERE tags <|2|> [1, 2];'],
			['~', "SELECT * FROM gauge WHERE note ~ 'a';"],
			['!~', "SELECT * FROM gauge WHERE note !~ 'a';"],
			['<- read as an arrow', 'SELECT * FROM gauge WHERE note <-edge;'],
			[
				'< before a negative number',
				'SELECT * FROM gauge WHERE reading < -1;',
			],
			[
				'a range end',
				'SELECT * FROM gauge WHERE reading IN 1..=reading;',
			],
			['a cast on the left', 'SELECT * FROM gauge WHERE <int> note = 1;'],
			[
				'a generic type',
				'SELECT * FROM gauge WHERE 1 = <array<string>> tags;',
			],
			[
				'a word operator',
				"SELECT * FROM gauge WHERE note CONTAINS 'a' AND note IN [1] AND note IS NONE;",
			],
			[
				'an unparenthesised closure body',
				'SELECT * FROM gauge WHERE array::any(tags, |$t| note = $t);',
			],
			['an object key', 'SELECT * FROM gauge WHERE { note: 1 } = $x;'],
			['a projection', 'SELECT note = 1 FROM gauge;'],
			[
				'ORDER BY, GROUP BY and SPLIT',
				'SELECT note FROM gauge SPLIT tags GROUP BY note ORDER BY note;',
			],
			[
				'a second clause',
				"UPDATE gauge SET note = $n WHERE station = 'a';",
			],
			[
				'a statement word as an object key',
				'RETURN { delete: 1, where: 2 };',
			],
			['a string', "SELECT * FROM gauge WHERE $x = 'station = 1';"],
			['a comment', 'SELECT * FROM gauge -- WHERE station = 1\n;'],
		])('for %s', (_, sql) => {
			expect(fields(sql)).toEqual([]);
		});
	});
});

const assign = (table: string, field: string, line = 1) => ({
	table,
	field,
	role: 'assign',
	line,
});

describe('analyze: assigned fields', () => {
	it('the target of SET in UPDATE, UPSERT and CREATE when the value is a literal that is never NONE', () => {
		for (const verb of ['UPDATE', 'UPSERT', 'CREATE'])
			expect(
				fields(
					`${verb} gauge SET station = 'a', reading = 1.5, note = d'2020-01-01T00:00:00Z', tags = [1, [2]], site = site:1;`,
				),
			).toEqual([
				assign('gauge', 'note'),
				assign('gauge', 'reading'),
				assign('gauge', 'site'),
				assign('gauge', 'station'),
				assign('gauge', 'tags'),
			]);
		expect(
			fields(
				'UPDATE gauge SET a = -1, b = 1d, c = 10dec, d = true, e = FALSE, f = NULL, g = site:[1, \'a\'], h = site:⟨x⟩, i = "s";',
			).map(({ field }) => field),
		).toEqual(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i']);
	});

	it('in a block, on its own line, and before WHERE or RETURN', () => {
		expect(
			fields(
				"IF $x {\n\tUPDATE gauge:1\n\t\tSET station = 'a',\n\t\t\tnote = 'b' WHERE reading = 1 RETURN NONE;\n};",
			),
		).toEqual([assign('gauge', 'station', 3), assign('gauge', 'note', 4)]);
	});

	describe('records nothing', () => {
		it.each([
			['NONE', 'UPDATE gauge SET station = NONE;'],
			['none', 'UPDATE gauge SET station = none;'],
			['a binding', 'UPDATE gauge SET station = $s;'],
			['a local', "LET $s = 'a'; UPDATE gauge SET station = $s;"],
			['a parameter path', 'UPDATE gauge SET station = $p.s;'],
			['a call', "UPDATE gauge SET station = string::lowercase('A');"],
			[
				'a subquery',
				'UPDATE gauge SET station = (SELECT VALUE label FROM site LIMIT 1);',
			],
			[
				'a boolean that can be NONE',
				'UPDATE gauge SET station = 1 AND NONE;',
			],
			['an index that can be NONE', 'UPDATE gauge SET station = [1][5];'],
			['arithmetic', "UPDATE gauge SET station = 'a' + $x;"],
			['a cast', 'UPDATE gauge SET station = <string> 1;'],
			[
				'an object or block',
				'UPDATE gauge SET station = {}, note = { RETURN 1 };',
			],
			['a range', 'UPDATE gauge SET reading = 1..5;'],
			['a path on a number', 'UPDATE gauge SET reading = 1.x;'],
			[
				'a compound assignment',
				'UPDATE gauge SET reading += 1, note -= 1;',
			],
			['a path target', 'UPDATE gauge SET pos.x = 1;'],
			['an index target', 'UPDATE gauge SET tags[0] = 1;'],
			['UNSET', 'UPDATE gauge UNSET station, note;'],
			[
				'a comparison after AND in the value',
				'UPDATE gauge SET station = true AND reading = 1;',
			],
			['RELATE', "RELATE site:1->gauge->site:2 SET station = 'a';"],
			[
				'INSERT … ON DUPLICATE KEY UPDATE',
				"INSERT INTO gauge { id: 1 } ON DUPLICATE KEY UPDATE station = 'a';",
			],
			['an open target', "UPDATE $g SET station = 'a';"],
			['two targets', "UPDATE gauge, site SET station = 'a';"],
			[
				'MERGE and CONTENT',
				"UPDATE gauge MERGE { station: 'a' }; CREATE gauge CONTENT { note: 'b' };",
			],
			[
				'a second clause',
				"UPDATE gauge WHERE reading = $r SET station = 'a';",
			],
		])('for %s', (_, sql) => {
			expect(fields(sql).filter(({ role }) => role === 'assign')).toEqual(
				[],
			);
		});
	});
});

const projected = (sql: string) =>
	analyzeSync(sql).projected.map(({ table, field }) => `${table}.${field}`);
const named = (sql: string) =>
	analyzeSync(sql).fields.map(
		({ table, field, role }) => `${role} ${table}.${field}`,
	);

describe('analyze: a local the text fixes to one table', () => {
	const fix = "LET $x = type::record('gauge', $key);\n";

	it('reads a SELECT from it, FROM ONLY and FOR UPDATE included', () => {
		expect(projected(`${fix}SELECT station, note FROM ONLY $x;`)).toEqual([
			'gauge.note',
			'gauge.station',
		]);
		expect(projected(`${fix}SELECT VALUE note FROM $x;`)).toEqual([
			'gauge.note',
		]);
		expect(
			projected(
				`${fix}LET $row = (SELECT note FROM ONLY $x FOR UPDATE);`,
			),
		).toEqual(['gauge.note']);
		expect(
			projected(
				"LET $x = type::thing('gauge', 1);\nSELECT note FROM ONLY $x;",
			),
		).toEqual(['gauge.note']);
	});

	it('reads comparisons and literal assignments of UPDATE, UPSERT and DELETE on it, in a block too', () => {
		expect(
			named(
				`${fix}UPDATE $x SET note = 'a' WHERE reading = 1;\nIF $c { UPSERT ONLY $x SET station = 'b' RETURN NONE } ELSE { DELETE $x WHERE station = $s };`,
			),
		).toEqual([
			'assign gauge.note',
			'assign gauge.station',
			'compare gauge.station',
		]);
	});

	it('never takes a binding on it for a use, so no binds or rejectsNone follow', () => {
		const sql = `${fix}UPSERT $x SET station = $s, reading = $r WHERE note = $n;`;
		expect(analyzeSync(sql).uses).toEqual({
			key: [{ kind: 'key', table: 'gauge', line: 1 }],
		});
		// The values can be NONE: not read as assignments either.
		expect(named(sql)).toEqual([]);
	});

	it('keeps the tables and open targets the text reports', () => {
		const sql = `${fix}SELECT note FROM ONLY $x;`;
		const analysis = analyzeSync(sql);
		expect(analysis.tables).toEqual({ read: ['gauge'], written: [] });
		expect(analysis.openTargets).toBe(true);
	});

	describe('records nothing', () => {
		it.each([
			[
				'a local LET twice',
				"LET $x = type::record('gauge', 1);\nLET $x = type::record('site', 1);\nSELECT note FROM $x;",
			],
			[
				'a LET in a block, read in it',
				"IF $c { LET $x = type::record('gauge', 1); SELECT note FROM $x; };",
			],
			[
				'a LET in a block, read after it',
				"IF $c { LET $x = type::record('gauge', 1) };\nSELECT note FROM $x;",
			],
			[
				'a LET shadowed in a block',
				"LET $x = type::record('gauge', 1);\nIF $c { LET $x = type::record('site', 1); SELECT label FROM $x; };",
			],
			[
				'a FOR variable of the same name',
				"LET $x = type::record('gauge', 1);\nFOR $x IN $xs { SELECT note FROM $x; };",
			],
			[
				'a read before the LET, of the parameter of that name',
				"SELECT note FROM $x;\nLET $x = type::record('gauge', 1);",
			],
			[
				'a table that is not a literal',
				'LET $x = type::record($t, 1);\nSELECT note FROM $x;',
			],
			[
				'a prefixed string',
				"LET $x = type::record(r'gauge', 1);\nSELECT note FROM $x;",
			],
			[
				'one argument',
				"LET $x = type::record('gauge:1');\nSELECT note FROM $x;",
			],
			[
				'a path after the call',
				"LET $x = type::record('gauge', 1).id;\nSELECT note FROM $x;",
			],
			[
				'an operator after the call',
				"LET $x = type::record('gauge', 1) ?? $y;\nSELECT note FROM $x;",
			],
			[
				'type::table',
				"LET $x = type::table('gauge');\nSELECT note FROM $x;",
			],
			['a parameter', 'LET $x = $y;\nSELECT note FROM $x;'],
			[
				'a subquery',
				'LET $x = (SELECT VALUE next FROM ONLY $y);\nSELECT note FROM $x;',
			],
			[
				'a closure in the text',
				"LET $x = type::record('gauge', 1);\nRETURN array::map([1], |$v| (SELECT note FROM ONLY $x));",
			],
			[
				'a function body in the text',
				"LET $x = type::record('gauge', 1);\nDEFINE FUNCTION fn::f() { RETURN SELECT note FROM ONLY $x; };",
			],
			[
				'a hole in the text',
				"LET $x = type::record('gauge', 1);\nSELECT note FROM $x WHERE __W__;",
			],
			[
				'a name the engine sets',
				"LET $parent = type::record('gauge', 1);\nSELECT note FROM $parent;",
			],
			[
				'a typed LET',
				"LET $x: record<gauge> = type::record('gauge', 1);\nSELECT note FROM $x;",
			],
			[
				'two targets',
				"LET $x = type::record('gauge', 1);\nSELECT note FROM $x, gauge;",
			],
			[
				'a path on the target',
				"LET $x = type::record('gauge', 1);\nSELECT note FROM $x.next;",
			],
		])('for %s', (_, sql) => {
			expect(analyzeSync(sql).projected, sql).toEqual([]);
			expect(analyzeSync(sql).fields, sql).toEqual([]);
		});
	});
});

const bindsDocument = join(import.meta.dirname, 'fixtures', 'binds.v2.json');
const declared = async (): Promise<DeclaredNames> =>
	declaredNames(
		await Effect.runPromise(decodeDocument(readJson(bindsDocument))),
	);

const render = async (sql: string, names?: DeclaredNames) => {
	try {
		const { files } = renderStatementModulesSync({
			scopes: [
				{
					name: 'shop',
					files: [{ name: 'q', sql }],
					declared: names ?? (await declared()),
				},
			],
		});
		return { problems: [], content: files[0]?.content ?? '' };
	} catch (error) {
		if (error instanceof StatementModulesError)
			return {
				problems: error.problems.map((problem) => problem.text),
				content: '',
			};
		throw error;
	}
};

const problems = async (sql: string, names?: DeclaredNames) =>
	(await render(sql, names)).problems;

describe('statements with schema detail: compared fields', () => {
	it('fails generation on a compared field a SCHEMAFULL table does not declare', async () => {
		expect(
			await problems("SELECT * FROM gauge\nWHERE staton = 'a';"),
		).toEqual([
			'shop/q.surql:2: compares `staton` of `gauge`, which declares no such field',
		]);
		expect(
			await problems(
				'IF $x {\n\tDELETE gauge WHERE $s = staton RETURN NONE;\n};',
			),
		).toEqual([
			'shop/q.surql:2: compares `staton` of `gauge`, which declares no such field',
		]);
	});

	it('reports a binding use once, as before', async () => {
		expect(
			await problems('SELECT * FROM gauge WHERE staton = $s;'),
		).toEqual([
			'shop/q.surql:1: compares `staton` of `gauge`, which declares no such field',
		]);
	});

	it('accepts declared fields, `id`, and a field whose nested fields alone are declared', async () => {
		expect(
			await problems(
				"SELECT * FROM gauge WHERE station = 'a' AND id = gauge:1 AND reading > 1 AND note != NONE AND site = $s AND tags = [];",
			),
		).toEqual([]);
		const names = await declared();
		const detail = names.detail;
		if (detail === undefined) throw new Error('no detail');
		const nested: DeclaredNames = {
			...names,
			detail: {
				...detail,
				tables: {
					...detail.tables,
					gauge: {
						fields: { 'pos.x': { kind: 'int' } },
						acceptsUndeclared: false,
					},
				},
			},
		};
		expect(
			await problems('SELECT * FROM gauge WHERE pos = {};', nested),
		).toEqual([]);
	});

	describe('checks nothing', () => {
		it.each([
			[
				'on a schemaless table',
				'SELECT * FROM scratch WHERE whatever = 1;',
			],
			[
				'in a text that defines a field itself',
				'DEFINE FIELD extra ON gauge TYPE int; SELECT * FROM gauge WHERE extra = 1;',
			],
			[
				'in a template where a hole starts a statement',
				'__SETUP__; SELECT * FROM gauge WHERE extra = 1;',
			],
		])('%s', async (_, sql) => {
			expect(await problems(sql)).toEqual([]);
		});

		it('on a table the schema does not declare, reported once by the names check', async () => {
			expect(
				await problems('SELECT * FROM shipment WHERE nope = 1;'),
			).toEqual([
				'shop/q.surql:1: reads table `shipment`, which the schema does not declare',
			]);
		});
	});

	it('fails generation on an assigned field a SCHEMAFULL table does not declare', async () => {
		expect(
			await problems("UPDATE gauge\n\tSET staton = 'a', note = 'b';"),
		).toEqual([
			'shop/q.surql:2: assigns `staton` of `gauge`, which declares no such field',
		]);
		expect(
			await problems(
				'IF $x { CREATE gauge SET reading = 1, staton = 2 RETURN NONE };',
			),
		).toEqual([
			'shop/q.surql:1: assigns `staton` of `gauge`, which declares no such field',
		]);
		// NONE clears a field, which both versions accept: not checked.
		expect(await problems('UPDATE gauge SET staton = NONE;')).toEqual([]);
		expect(await problems('UPDATE gauge UNSET staton;')).toEqual([]);
	});

	it('fails generation on a field of a local the text fixes to one table', async () => {
		expect(
			await problems(
				"LET $x = type::record('gauge', $k);\nSELECT staton FROM ONLY $x;\nUPDATE $x SET nte = 'a';\nDELETE $x WHERE readng = 1;",
			),
		).toEqual([
			'shop/q.surql:2: selects `staton` of `gauge`, which declares no such field',
			'shop/q.surql:3: assigns `nte` of `gauge`, which declares no such field',
			'shop/q.surql:4: compares `readng` of `gauge`, which declares no such field',
		]);
		// A binding assigned to it writes no rejectsNone entry.
		const { content, problems: none } = await render(
			"LET $x = type::record('gauge', $k);\nUPSERT $x SET station = $s;",
		);
		expect(none).toEqual([]);
		expect(content).not.toContain('rejectsNone');
		expect(content).not.toContain('binds: { s');
	});

	it('changes nothing in the generated module', async () => {
		const { content } = await render(
			"SELECT * FROM gauge WHERE station = 'a';",
		);
		expect(content).toContain('tables: { read: ["gauge"], written: [] }');
		expect(content).not.toContain('fields');
		expect(content).not.toContain('uses');
	});
});
