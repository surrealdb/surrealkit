/**
 * Projected fields: the bare fields and `*` a `SELECT` projects
 * from exactly one static table, and the check that fails generation when
 * a `SCHEMAFULL` table does not declare one. Every case the analyzer is not
 * certain of records nothing.
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

const projected = (sql: string) => analyzeSync(sql).projected;

const at = (table: string, field: string, line = 1) => ({
	table,
	field,
	line,
});

describe('analyze: projected fields', () => {
	it('records each bare field and `*` a SELECT projects, with its line', () => {
		expect(projected('SELECT name, age FROM person;')).toEqual([
			at('person', 'age'),
			at('person', 'name'),
		]);
		expect(projected('SELECT * FROM person;')).toEqual([at('person', '*')]);
		expect(projected('SELECT *, name FROM person;')).toEqual([
			at('person', '*'),
			at('person', 'name'),
		]);
		expect(
			projected('SELECT\n\tname,\n\t`full name`\nFROM person;'),
		).toEqual([at('person', 'name', 2), at('person', 'full name', 3)]);
	});

	it('reads the one target as a table, a record id, ONLY, or a literal type::record', () => {
		expect(projected('SELECT name FROM person:1;')).toEqual([
			at('person', 'name'),
		]);
		expect(projected('SELECT name FROM ONLY person:⟨a b⟩;')).toEqual([
			at('person', 'name'),
		]);
		expect(projected('SELECT name FROM person:[1, 2];')).toEqual([
			at('person', 'name'),
		]);
		expect(
			projected("SELECT name FROM ONLY type::record('person', $key);"),
		).toEqual([at('person', 'name')]);
		expect(projected("SELECT name FROM type::table('person');")).toEqual([
			at('person', 'name'),
		]);
	});

	it('keeps the clauses after the target out of the projection', () => {
		expect(
			projected(
				'SELECT name FROM person WHERE age > $min ORDER BY name LIMIT 10 FETCH team;',
			),
		).toEqual([at('person', 'name')]);
		expect(
			projected('SELECT name, count() FROM person GROUP BY name;'),
		).toEqual([at('person', 'name')]);
	});

	it('reads a SELECT after LET or RETURN, and each parenthesised SELECT as a query of its own', () => {
		expect(projected('LET $rows = SELECT name FROM person;')).toEqual([
			at('person', 'name'),
		]);
		expect(projected('RETURN SELECT name FROM person;')).toEqual([
			at('person', 'name'),
		]);
		expect(
			projected(
				'UPDATE team SET lead = (SELECT VALUE id FROM person WHERE name = $n);\nLET $t = (SELECT title FROM listing);',
			),
		).toEqual([at('person', 'id'), at('listing', 'title', 2)]);
		expect(
			projected(
				'SELECT name FROM person WHERE id IN (SELECT title FROM listing);',
			),
		).toEqual([at('listing', 'title'), at('person', 'name')]);
	});

	it('reads `VALUE` with one bare field', () => {
		expect(projected('SELECT VALUE name FROM person;')).toEqual([
			at('person', 'name'),
		]);
		expect(
			projected('SELECT VALUE `full name` FROM ONLY person:1;'),
		).toEqual([at('person', 'full name')]);
	});

	it('reads a bare field with an alias as the field, never the alias', () => {
		expect(projected('SELECT name AS label FROM person;')).toEqual([
			at('person', 'name'),
		]);
		expect(
			projected('SELECT name AS `the label`, age as years FROM person;'),
		).toEqual([at('person', 'age'), at('person', 'name')]);
		// A later item names a field, not an earlier alias.
		expect(projected('SELECT name AS label, label FROM person;')).toEqual([
			at('person', 'label'),
			at('person', 'name'),
		]);
	});

	it('reads nothing in strings and comments', () => {
		expect(
			projected(
				"-- SELECT nope FROM person;\nRETURN 'SELECT nope FROM person';",
			),
		).toEqual([]);
	});

	describe('records nothing', () => {
		it('for a function call', () => {
			expect(projected('SELECT count() FROM person GROUP ALL;')).toEqual(
				[],
			);
			expect(projected('SELECT string::len(name) FROM person;')).toEqual(
				[],
			);
		});

		it('for a path', () => {
			expect(projected('SELECT address.city FROM person;')).toEqual([]);
			expect(projected('SELECT tags[0] FROM person;')).toEqual([]);
		});

		it('for an expression', () => {
			expect(projected('SELECT age + 1 FROM person;')).toEqual([]);
			expect(projected("SELECT 'x' FROM person;")).toEqual([]);
			expect(projected('SELECT $x FROM person;')).toEqual([]);
			expect(projected('SELECT { a: name } FROM person;')).toEqual([]);
			expect(projected('SELECT true FROM person;')).toEqual([]);
		});

		it('for a bare word SurrealQL reads as a value', () => {
			// Pinned against both engines by test/projections-engine.test.ts.
			for (const word of [
				'NONE',
				'none',
				'NULL',
				'Null',
				'TRUE',
				'true',
				'FALSE',
				'False',
				'NaN',
				'Infinity',
			])
				expect(projected(`SELECT ${word} FROM person;`)).toEqual([]);
			expect(projected('SELECT NaN, Infinity FROM person;')).toEqual([]);
			// Spelt otherwise, they are field names.
			expect(
				projected(
					'SELECT nan, NAN, infinity, INFINITY, inf FROM person;',
				),
			).toEqual([
				at('person', 'INFINITY'),
				at('person', 'NAN'),
				at('person', 'inf'),
				at('person', 'infinity'),
				at('person', 'nan'),
			]);
		});

		it('for an aliased expression', () => {
			expect(projected('SELECT age + 1 AS next FROM person;')).toEqual(
				[],
			);
			expect(projected('SELECT name.first AS n FROM person;')).toEqual(
				[],
			);
			expect(projected('SELECT count() AS n FROM person;')).toEqual([]);
			expect(projected('SELECT $x AS n FROM person;')).toEqual([]);
			expect(projected('SELECT NaN AS n FROM person;')).toEqual([]);
			expect(projected('SELECT name AS FROM person;')).toEqual([]);
			expect(projected('SELECT name AS a.b FROM person;')).toEqual([]);
			expect(projected('SELECT name AS n AS m FROM person;')).toEqual([]);
		});

		it('for VALUE with anything but one bare field', () => {
			expect(projected('SELECT VALUE name.first FROM person;')).toEqual(
				[],
			);
			expect(projected('SELECT VALUE count() FROM person;')).toEqual([]);
			expect(projected('SELECT VALUE age + 1 FROM person;')).toEqual([]);
			expect(projected('SELECT VALUE name AS n FROM person;')).toEqual(
				[],
			);
			expect(projected('SELECT VALUE * FROM person;')).toEqual([]);
			expect(projected('SELECT VALUE NONE FROM person;')).toEqual([]);
			expect(projected('SELECT VALUE FROM person;')).toEqual([]);
			expect(projected('SELECT VALUE name FROM $owner;')).toEqual([]);
		});

		it('for OMIT', () => {
			expect(projected('SELECT * OMIT secret FROM person;')).toEqual([]);
			expect(projected('SELECT name OMIT secret FROM person;')).toEqual(
				[],
			);
		});

		it('for a graph step', () => {
			expect(
				projected('SELECT ->knows->person AS friends FROM person;'),
			).toEqual([]);
			expect(projected('SELECT <-knows<-person FROM person;')).toEqual(
				[],
			);
			expect(projected('SELECT name FROM person->knows;')).toEqual([]);
		});

		it('for a subquery in the projection, for the outer table', () => {
			expect(
				projected(
					'SELECT (SELECT VALUE title.en FROM listing) AS titles FROM person;',
				),
			).toEqual([]);
			// The subquery is a query of its own, of its own table.
			expect(
				projected(
					'SELECT (SELECT title FROM listing) AS t FROM person;',
				),
			).toEqual([at('listing', 'title')]);
		});

		it('for two targets', () => {
			expect(projected('SELECT name FROM person, team;')).toEqual([]);
			expect(projected('SELECT name FROM person, person;')).toEqual([]);
			expect(projected('SELECT name FROM person:1, person:2;')).toEqual(
				[],
			);
		});

		it('for an open target', () => {
			expect(projected('SELECT name FROM $owner;')).toEqual([]);
			expect(projected('SELECT name FROM ONLY $owner;')).toEqual([]);
			expect(projected('SELECT name FROM type::table($t);')).toEqual([]);
			expect(projected('SELECT name FROM type::record($t, $k);')).toEqual(
				[],
			);
			expect(
				projected('SELECT name FROM (SELECT * FROM person);'),
			).toEqual([at('person', '*')]);
			expect(projected('SELECT name FROM person:1..5;')).toEqual([]);
			expect(projected('SELECT name FROM person:ulid();')).toEqual([]);
			expect(projected('SELECT name FROM person.x;')).toEqual([]);
		});

		it('for a query whose FROM is not certain', () => {
			expect(projected('SELECT name;')).toEqual([]);
			// A field named `from`.
			expect(projected('SELECT name, from FROM person;')).toEqual([]);
		});

		it('for a template whose target is followed by a hole', () => {
			expect(projected('SELECT name FROM person __REST__;')).toEqual([]);
			expect(projected('SELECT __FIELDS__ FROM person;')).toEqual([]);
			expect(projected('SELECT name FROM __TABLE__;')).toEqual([]);
		});

		it('in a block for an object literal, a statement word as a key, or an open target', () => {
			expect(projected('RETURN { select: name, from: person };')).toEqual(
				[],
			);
			expect(projected('RETURN { SELECT: 1, a: 2 };')).toEqual([]);
			// Block statements whose target is not one static table: two targets,
			// a parameter, a FOR variable, a local bound to a parameter.
			expect(
				projected(
					'IF $x { SELECT name FROM person, team; SELECT name FROM $row; };',
				),
			).toEqual([]);
			expect(
				projected('FOR $row IN $rows { SELECT name FROM $row; };'),
			).toEqual([]);
			expect(
				projected(
					'LET $p = $owner; IF $x { SELECT name FROM ONLY $p; };',
				),
			).toEqual([]);
			// Two items separated by a comma, as a set literal holds them:
			// two targets, so nothing.
			expect(projected('RETURN { SELECT name FROM person, 2 };')).toEqual(
				[],
			);
		});
	});
});

describe('analyze: projected fields of a statement in a block', () => {
	it('reads the bodies of IF, ELSE IF and ELSE like top-level statements', () => {
		expect(
			projected(
				'IF $x { SELECT name FROM person } ELSE IF $y { SELECT age FROM person; } ELSE { [] };',
			),
		).toEqual([at('person', 'age'), at('person', 'name')]);
	});

	it('reads a FOR body, a bare block and a block nested in another', () => {
		expect(
			projected(
				'FOR $row IN $rows {\n\tLET $one = SELECT name FROM ONLY person:1;\n\tIF $one { RETURN SELECT VALUE title FROM listing };\n};',
			),
		).toEqual([at('person', 'name', 2), at('listing', 'title', 3)]);
		expect(
			projected('{ SELECT name FROM person; SELECT * FROM team }'),
		).toEqual([at('person', 'name'), at('team', '*')]);
	});

	it('reads a block in a function body, which runs against the same tables', () => {
		expect(
			projected(
				'DEFINE FUNCTION fn::names() { RETURN SELECT VALUE name FROM person; };',
			),
		).toEqual([at('person', 'name')]);
	});

	it('reads the clauses and FOR UPDATE of a block statement as at the top level', () => {
		expect(
			projected(
				'BEGIN; IF $x { LET $row = SELECT name FROM ONLY person:1 FOR UPDATE; SELECT age FROM person WHERE age > 1 LIMIT 2 } ; COMMIT;',
			),
		).toEqual([at('person', 'age'), at('person', 'name')]);
		expect(
			projected('IF $x { SELECT name FROM person FOR UPDATE };'),
		).toEqual([]);
	});
});

/**
 * The clauses that may follow a SELECT's target. Each is read through only
 * when both engines return the same fields with it as without it
 * (`test/projections-engine.test.ts`); `FOR UPDATE` only after record ids,
 * the one target both engines accept it with.
 */
describe('analyze: trailing clauses of a SELECT', () => {
	it.each([
		'SELECT name, age FROM person WHERE age > $min TIMEOUT 1s;',
		'SELECT name, age FROM person WHERE age > $min TEMPFILES;',
		'SELECT name, age FROM person WITH INDEX by_age WHERE age > $min;',
		'SELECT name, age FROM person WITH NOINDEX WHERE age > $min;',
		'SELECT name, age FROM person WHERE age > $min EXPLAIN;',
		'SELECT name, age FROM person WHERE age > $min EXPLAIN FULL;',
		'SELECT name, age FROM person VERSION d"2026-01-01T00:00:00Z";',
		'SELECT name, age FROM person:1 FOR UPDATE;',
		'SELECT name, age FROM person:1 for update;',
		'SELECT name, age FROM ONLY person:1 WHERE age > $min FOR UPDATE;',
		'SELECT name, age FROM person:1 ORDER BY age LIMIT 1 START 0 FOR UPDATE;',
		'SELECT name, age FROM ONLY person:1 TIMEOUT 1s FOR UPDATE;',
		'SELECT name, age FROM ONLY person:1 WITH NOINDEX FOR UPDATE;',
		'SELECT name, age FROM ONLY person:1 FETCH team FOR UPDATE;',
		'SELECT name, age FROM ONLY person:1 FOR UPDATE EXPLAIN;',
		'SELECT name, age FROM ONLY person:1 FOR UPDATE EXPLAIN FULL;',
		'SELECT name, age FROM ONLY person:1 FOR UPDATE TEMPFILES;',
		"SELECT name, age FROM ONLY type::record('person', $key) FOR UPDATE;",
		"LET $row = (SELECT name, age FROM ONLY type::record('person', $key) FOR UPDATE);",
	])('reads the projection through %s', (sql) => {
		expect(projected(sql)).toEqual([
			at('person', 'age'),
			at('person', 'name'),
		]);
	});

	it('reads `VALUE` through FOR UPDATE', () => {
		expect(
			projected(
				"LET $p = (SELECT VALUE enabled FROM ONLY type::record('principal', $id) FOR UPDATE);",
			),
		).toEqual([at('principal', 'enabled')]);
	});

	describe('records nothing', () => {
		it.each([
			// Both engines: "SELECT ... FOR UPDATE requires record ids as targets".
			'SELECT name FROM person FOR UPDATE;',
			'SELECT name FROM ONLY person WHERE age > $min FOR UPDATE;',
			"SELECT name FROM type::table('person') FOR UPDATE;",
			'SELECT name FROM person:1..5 FOR UPDATE;',
			// "cannot be combined with GROUP BY or SPLIT", "… with VERSION".
			'SELECT name FROM person:1 GROUP BY name FOR UPDATE;',
			'SELECT name FROM person:1 SPLIT name FOR UPDATE;',
			'SELECT name FROM person:1 VERSION d"2026-01-01T00:00:00Z" FOR UPDATE;',
			// Parse errors on both engines.
			'SELECT name FROM ONLY person:1 FOR UPDATE FETCH team;',
			'SELECT name FROM ONLY person:1 EXPLAIN FOR UPDATE;',
			'SELECT name FROM ONLY person:1 FOR UPDATE WHERE age > $min;',
			'SELECT name FROM person:1 FOR SHARE;',
			'SELECT name FROM person:1 FOR;',
			'SELECT name FROM person PARALLEL;',
			'SELECT name FROM person WHERE age > $min PARALLEL;',
		])('for %s', (sql) => {
			expect(projected(sql)).toEqual([]);
		});
	});
});

const bindsDocument = join(import.meta.dirname, 'fixtures', 'binds.v2.json');
const declared = async (): Promise<DeclaredNames> =>
	declaredNames(
		await Effect.runPromise(decodeDocument(readJson(bindsDocument))),
	);

/** The binds document with a SCHEMAFULL view, which declares no fields. */
const withView = async (): Promise<DeclaredNames> => {
	const document = readJson(bindsDocument);
	const tables = document.tables as ReadonlyArray<Record<string, unknown>>;
	return declaredNames(
		await Effect.runPromise(
			decodeDocument({
				...document,
				tables: [
					...tables,
					{
						name: 'tally',
						type: { kind: 'normal' },
						schemafull: true,
						drop: false,
						view: {
							select: 'SELECT count() AS n FROM gauge GROUP ALL',
						},
						permissions: {
							select: false,
							create: false,
							update: false,
							delete: false,
						},
						fields: [],
						indexes: [],
						events: [],
					},
				],
			}),
		),
	);
};

const problems = async (
	sql: string,
	names?: DeclaredNames,
): Promise<ReadonlyArray<string>> => {
	try {
		renderStatementModulesSync({
			scopes: [
				{
					name: 'shop',
					files: [{ name: 'q', sql }],
					declared: names ?? (await declared()),
				},
			],
		});
	} catch (error) {
		if (error instanceof StatementModulesError)
			return error.problems.map((problem) => problem.text);
		throw error;
	}
	return [];
};

describe('statements with schema detail: projected fields', () => {
	it('fails generation on a field a SCHEMAFULL table does not declare, with its file and line', async () => {
		expect(await problems('SELECT station,\n\tstaton FROM gauge;')).toEqual(
			[
				'shop/q.surql:2: selects `staton` of `gauge`, which declares no such field',
			],
		);
		expect(
			await problems("SELECT labl FROM ONLY type::record('site', $k);"),
		).toEqual([
			'shop/q.surql:1: selects `labl` of `site`, which declares no such field',
		]);
		// `VALUE` with one bare field, and a bare field with an alias.
		expect(await problems('SELECT VALUE staton FROM gauge;')).toEqual([
			'shop/q.surql:1: selects `staton` of `gauge`, which declares no such field',
		]);
		expect(await problems('SELECT staton AS station FROM gauge;')).toEqual([
			'shop/q.surql:1: selects `staton` of `gauge`, which declares no such field',
		]);
		expect(await problems('SELECT station AS s FROM gauge;')).toEqual([]);
		// In a template, as for the uses of its bindings.
		expect(
			await problems('SELECT nope FROM gauge WHERE __FILTER__;'),
		).toEqual([
			'shop/q.surql:1: selects `nope` of `gauge`, which declares no such field',
		]);
	});

	it('escapes a quoted name in the message', async () => {
		// The quoted names `lab\`el` and `a\\b`: `lab`el` and `a\b`.
		expect(await problems('SELECT `lab\\`el` FROM site;')).toEqual([
			'shop/q.surql:1: selects `lab\\`el` of `site`, which declares no such field',
		]);
		expect(await problems('SELECT `a\\\\b` FROM site;')).toEqual([
			'shop/q.surql:1: selects `a\\\\b` of `site`, which declares no such field',
		]);
	});

	it('accepts declared fields, `id` and `*`', async () => {
		expect(
			await problems(
				'SELECT id, station, reading, note, *, site, tags FROM gauge;',
			),
		).toEqual([]);
	});

	it('accepts a field whose nested fields alone are declared', async () => {
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
						fields: { 'pos.x': { kind: 'int' }, 'list[*]': {} },
						acceptsUndeclared: false,
					},
				},
			},
		};
		expect(await problems('SELECT pos, list FROM gauge;', nested)).toEqual(
			[],
		);
		expect(await problems('SELECT po FROM gauge;', nested)).toEqual([
			'shop/q.surql:1: selects `po` of `gauge`, which declares no such field',
		]);
	});

	describe('checks nothing', () => {
		it('on a schemaless table, whose fields the analyzer still records', async () => {
			expect(projected('SELECT whatever FROM scratch;')).toEqual([
				at('scratch', 'whatever'),
			]);
			expect(await problems('SELECT whatever FROM scratch;')).toEqual([]);
		});

		it('on a view, whose fields come from its SELECT', async () => {
			const names = await withView();
			expect(names.detail?.tables.tally?.acceptsUndeclared).toBe(true);
			expect(await problems('SELECT n FROM tally;', names)).toEqual([]);
			// The uses of a binding are not checked against it either.
			expect(
				await problems('SELECT * FROM tally WHERE n = $n;', names),
			).toEqual([]);
		});

		it('for a bare word SurrealQL reads as a value, in a projection or a comparison', async () => {
			expect(await problems('SELECT NaN, Infinity FROM gauge;')).toEqual(
				[],
			);
			expect(
				analyzeSync('SELECT * FROM gauge WHERE NaN = $x;').uses,
			).toEqual({});
			expect(
				analyzeSync('SELECT * FROM gauge WHERE $x < Infinity;').uses,
			).toEqual({});
			expect(
				await problems('SELECT * FROM gauge WHERE $x < Infinity;'),
			).toEqual([]);
		});

		it('in a template where a hole starts a statement, which a fill can make define a field', async () => {
			expect(
				await problems('__SETUP__;\nSELECT nope FROM gauge;'),
			).toEqual([]);
			expect(
				await problems(
					'{ __SETUP__; RETURN (SELECT nope FROM gauge); };',
				),
			).toEqual([]);
			expect(
				projected('{ __SETUP__; RETURN (SELECT nope FROM gauge); };'),
			).toEqual([at('gauge', 'nope')]);
			// A hole elsewhere does not start a statement: still checked.
			expect(
				await problems('LET $x = __VALUE__;\nSELECT nope FROM gauge;'),
			).toEqual([
				'shop/q.surql:2: selects `nope` of `gauge`, which declares no such field',
			]);
		});

		it('on a table the schema does not declare, reported once by the names check', async () => {
			expect(await problems('SELECT nope FROM shipment;')).toEqual([
				'shop/q.surql:1: reads table `shipment`, which the schema does not declare',
			]);
		});

		it('in a text that defines, alters or removes a table or field itself', async () => {
			expect(
				await problems(
					'DEFINE FIELD extra ON gauge TYPE string;\nSELECT extra FROM gauge;',
				),
			).toEqual([]);
			expect(
				await problems(
					'DEFINE TABLE fresh SCHEMAFULL;\nSELECT nope FROM fresh;',
				),
			).toEqual([]);
		});

		it('without schema detail', async () => {
			const { detail: _, ...names } = await declared();
			expect(await problems('SELECT nope FROM gauge;', names)).toEqual(
				[],
			);
		});

		it('for what the analyzer does not record', async () => {
			expect(await problems('SELECT VALUE nope.x FROM gauge;')).toEqual(
				[],
			);
			expect(await problems('SELECT nope + 1 AS x FROM gauge;')).toEqual(
				[],
			);
			expect(await problems('SELECT nope.x FROM gauge;')).toEqual([]);
		});
	});

	it('changes nothing in the generated module', async () => {
		const render = async (sql: string) =>
			renderStatementModulesSync({
				scopes: [
					{
						name: 'shop',
						files: [{ name: 'q', sql }],
						declared: await declared(),
					},
				],
			}).files;
		const files = await render('SELECT station, reading FROM gauge;');
		expect(files[0]?.content).not.toContain('projected');
		expect(files[0]?.content).not.toContain('station"');
	});
});
