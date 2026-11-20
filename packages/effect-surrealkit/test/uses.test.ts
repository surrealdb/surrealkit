import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

import { Effect } from 'effect';
import { describe, expect, it } from 'vitest';

import {
	analyzeSync,
	type DeclaredNames,
	renderStatementModulesSync,
	StatementModulesError,
	type StatementScope,
} from '../src/analyze/index.js';
import { run } from '../src/cli/run.js';
import { declaredNames, decodeDocument } from '../src/emitter/index.js';
import * as SurrealSchema from '../src/index.js';
import { part, sequence } from '../src/statement/index.js';
import { corpusPath, goldenDir, readJson } from './documents.js';

const uses = (sql: string) => analyzeSync(sql).uses;

const field = (
	table: string,
	name: string,
	line = 1,
	role: 'compare' | 'assign' = 'compare',
) => ({
	kind: 'field',
	table,
	field: name,
	role,
	line,
});

describe('analyze: argument uses', () => {
	it('a parameter that is a whole argument of a custom function', () => {
		expect(uses('RETURN fn::shop::price($item, 2, $count);')).toEqual({
			count: [
				{
					kind: 'argument',
					function: 'fn::shop::price',
					index: 2,
					line: 1,
				},
			],
			item: [
				{
					kind: 'argument',
					function: 'fn::shop::price',
					index: 0,
					line: 1,
				},
			],
		});
	});

	it('splits arguments at the call’s own level', () => {
		expect(uses('RETURN fn::pack([1, 2], { a: 1, b: 2 }, $tail);')).toEqual(
			{
				tail: [
					{
						kind: 'argument',
						function: 'fn::pack',
						index: 2,
						line: 1,
					},
				],
			},
		);
	});

	it.each([
		['part of a longer expression', 'RETURN fn::pack($a + 1);'],
		['a path', 'RETURN fn::pack($a.b);'],
		['an index', 'RETURN fn::pack($a[0]);'],
		['inside a nested bracket', 'RETURN fn::pack([$a]);'],
		['a built-in function', 'RETURN string::len($a);'],
		['inside a string', "RETURN fn::pack('$a'), $a;"],
		['inside a comment', 'RETURN $a; -- fn::pack($a)'],
	])('records nothing for %s', (_, sql) => {
		expect(uses(sql)).toEqual({});
	});
});

describe('analyze: key uses', () => {
	it.each([
		"SELECT * FROM type::record('gizmo', $key);",
		"SELECT * FROM type::thing('gizmo', $key);",
	])('%s', (sql) => {
		expect(uses(sql)).toEqual({
			key: [{ kind: 'key', table: 'gizmo', line: 1 }],
		});
	});

	it.each([
		[
			'a key that is an expression',
			"RETURN type::record('gizmo', $key + 1);",
		],
		['a table that is a parameter', 'RETURN type::record($table, $key);'],
		['a prefixed string', "RETURN type::record(r'gizmo', $key);"],
		['a path key', "RETURN type::record('gizmo', $key.id);"],
	])('records nothing for %s', (_, sql) => {
		expect(uses(sql)).toEqual({});
	});
});

describe('analyze: field uses', () => {
	it('either side of each comparison in WHERE', () => {
		expect(
			uses(
				'SELECT * FROM gizmo WHERE a = $a AND $b != b OR c <= $c AND d == $d AND $e > e AND f >= $f AND g < $g;',
			),
		).toEqual({
			a: [field('gizmo', 'a')],
			b: [field('gizmo', 'b')],
			c: [field('gizmo', 'c')],
			d: [field('gizmo', 'd')],
			e: [field('gizmo', 'e')],
			f: [field('gizmo', 'f')],
			g: [field('gizmo', 'g')],
		});
	});

	it('quoted identifiers, inside grouping parentheses', () => {
		expect(
			uses('DELETE gizmo WHERE (`odd name` = $a OR ⟨other⟩ = $b);'),
		).toEqual({
			a: [field('gizmo', 'odd name')],
			b: [field('gizmo', 'other')],
		});
	});

	it('assignments in SET of UPDATE, UPSERT and CREATE', () => {
		for (const verb of ['UPDATE', 'UPSERT', 'CREATE'])
			expect(uses(`${verb} gizmo SET a = $a, b = $b;`)).toEqual({
				a: [field('gizmo', 'a', 1, 'assign')],
				b: [field('gizmo', 'b', 1, 'assign')],
			});
	});

	it('tells a SET assignment from a WHERE comparison', () => {
		expect(
			uses(
				'UPDATE gizmo SET a = $a;\nSELECT * FROM gizmo WHERE b = $b AND $c < c;',
			),
		).toEqual({
			a: [field('gizmo', 'a', 1, 'assign')],
			b: [field('gizmo', 'b', 2, 'compare')],
			c: [field('gizmo', 'c', 2, 'compare')],
		});
	});

	it('a subquery is a query of its own table', () => {
		expect(
			uses(
				'SELECT * FROM gizmo WHERE a IN (SELECT VALUE id FROM sprocket WHERE b = $b);',
			),
		).toEqual({ b: [field('sprocket', 'b')] });
	});

	it('sorted by line and without duplicates', () => {
		expect(
			uses(
				'SELECT * FROM gizmo WHERE a = $x;\nSELECT * FROM gizmo WHERE a = $x;\nSELECT * FROM gizmo WHERE b = $x AND b = $x;',
			),
		).toEqual({
			x: [
				field('gizmo', 'a', 1),
				field('gizmo', 'a', 2),
				field('gizmo', 'b', 3),
			],
		});
	});

	it.each([
		[
			'part of a longer expression',
			'SELECT * FROM gizmo WHERE a = $a + 1;',
		],
		['an operand of arithmetic', 'SELECT * FROM gizmo WHERE b + a = $a;'],
		['a path', 'SELECT * FROM gizmo WHERE a.b = $a;'],
		['a path on the right', 'SELECT * FROM gizmo WHERE $a = b.c;'],
		['a parameter path', 'SELECT * FROM gizmo WHERE a = $a.b;'],
		['a graph step', 'SELECT * FROM gizmo WHERE ->a = $a;'],
		['a call', 'SELECT * FROM gizmo WHERE a() = $a;'],
		['two tables in FROM', 'SELECT * FROM gizmo, sprocket WHERE a = $a;'],
		['an open target', 'SELECT * FROM $where WHERE a = $a;'],
		['SET on a parameter target', 'UPDATE $target SET a = $a;'],
		['a compound assignment', 'UPDATE gizmo SET a += $a;'],
		['a projection', 'SELECT a = $a FROM gizmo;'],
		[
			'the outer query of another table',
			'SELECT * FROM gizmo WHERE a = $a AND b IN (SELECT * FROM sprocket);',
		],
		['inside a string', "SELECT * FROM gizmo WHERE a = '$a' OR b = $b.c;"],
		['inside a comment', 'SELECT * FROM gizmo -- WHERE a = $a\nWHERE $a;'],
		['a block', 'IF $c { UPDATE gizmo SET a = $a };'],
		['a non-query statement', 'IF a = $a THEN 1 END;'],
		['a value word', 'SELECT * FROM gizmo WHERE NONE = $a;'],
		[
			'a name the text also defines',
			'SELECT * FROM gizmo WHERE a = $a; LET $a = 1;',
		],
	])('records nothing for %s', (_, sql) => {
		expect(uses(sql)).toEqual({});
	});
});

describe('part and sequence: uses', () => {
	const read = part({
		sql: 'SELECT * FROM gizmo WHERE a = $a AND b = $b;',
		free: ['a', 'b'],
	});

	it('a part exposes the analysed uses', () => {
		expect(read.uses).toEqual({
			a: [field('gizmo', 'a')],
			b: [field('gizmo', 'b')],
		});
	});

	it('declared uses are checked against the text', () => {
		expect(() =>
			part({
				sql: 'SELECT * FROM gizmo WHERE a = $a;',
				free: ['a'],
				uses: { a: [field('gizmo', 'z')] as never },
			}),
		).toThrow(
			'uses: the text uses `$a` as field `gizmo.a` on line 1, which is not declared; uses: declared `$a` as field `gizmo.z` on line 1, which the text does not use it as',
		);
		expect(() =>
			part({
				sql: 'UPDATE gizmo SET a = $a;',
				free: ['a'],
				uses: { a: [field('gizmo', 'a', 1, 'compare')] as never },
			}),
		).toThrow(
			'uses: the text uses `$a` as field `gizmo.a` (assigned) on line 1, which is not declared; uses: declared `$a` as field `gizmo.a` on line 1, which the text does not use it as',
		);
		expect(
			part({
				sql: 'UPDATE gizmo SET a = $a;',
				free: ['a'],
				uses: {
					a: [{ kind: 'field', table: 'gizmo', field: 'a', line: 1 }],
				},
			}).uses,
		).toEqual({ a: [field('gizmo', 'a', 1, 'assign')] });
		expect(() =>
			part({
				sql: 'RETURN $a;',
				free: ['a'],
				binds: { a: 'string' },
			}),
		).toThrow(
			'binds: declared `$a`, which the text never uses as an argument, a key or a field',
		);
		expect(() =>
			part({
				sql: 'SELECT * FROM gizmo WHERE a = $a;',
				free: ['a'],
				rejectsNone: {
					a: { table: 'gizmo', field: 'a', kind: 'string' },
				},
			}),
		).toThrow(
			'rejectsNone: declared `$a` as assigned to `gizmo.a`, which the text never assigns it to',
		);
		expect(
			part({
				sql: 'UPDATE gizmo SET a = $a;',
				free: ['a'],
				rejectsNone: {
					a: { table: 'gizmo', field: 'a', kind: 'string' },
				},
			}).rejectsNone,
		).toEqual({ a: { table: 'gizmo', field: 'a', kind: 'string' } });
	});

	it('sequence merges the uses of the names that remain bindings', () => {
		const shadow = part({ sql: 'LET $b = 1;', defines: ['b'] });
		const call = part({ sql: 'RETURN fn::f($a);', free: ['a'] });
		const statement = sequence(call, shadow, read).define({
			params: { a: SurrealSchema.String },
			result: SurrealSchema.String,
		});
		expect(statement.uses).toEqual({
			a: [
				{ kind: 'argument', function: 'fn::f', index: 0, line: 1 },
				field('gizmo', 'a'),
			],
		});
		const plain = sequence(part({ sql: 'RETURN 1;' })).define({
			params: {},
			result: SurrealSchema.Int,
		});
		expect(plain.uses).toBeUndefined();
	});
});

const bindsDocument = join(import.meta.dirname, 'fixtures', 'binds.v2.json');
const declared = async (): Promise<DeclaredNames> =>
	declaredNames(
		await Effect.runPromise(decodeDocument(readJson(bindsDocument))),
	);

describe('declaredNames: detail', () => {
	it('fields with their kind text, undeclared fields, id keys and arguments', async () => {
		expect((await declared()).detail).toEqual({
			tables: {
				gauge: {
					fields: {
						note: { kind: 'option<string>' },
						reading: { kind: 'float', rejectsNone: true },
						site: { kind: 'record<site>', rejectsNone: true },
						station: { kind: 'string', rejectsNone: true },
						tags: { kind: 'array<string>', rejectsNone: true },
					},
					acceptsUndeclared: false,
				},
				scratch: { fields: { anything: {} }, acceptsUndeclared: true },
				site: {
					fields: { label: { kind: 'string', rejectsNone: true } },
					acceptsUndeclared: false,
					idKey: 'string',
				},
			},
			functions: { 'fn::scaled': ['float'] },
		});
	});
});

describe('declaredNames: fields that reject NONE', () => {
	it('marks a top-level field whose kind excludes NONE and that no clause supplies', async () => {
		// The engine's own document for the field-presence corpus.
		const document = await Effect.runPromise(
			decodeDocument(
				readJson(corpusPath('compiled', 'field-presence.v2.json')),
			),
		);
		const tables = declaredNames(document).detail?.tables ?? {};
		const marked = Object.entries(tables).flatMap(([table, detail]) =>
			Object.entries(detail.fields)
				.filter(([, entry]) => entry.rejectsNone === true)
				.map(([name]) => `${table}.${name}`),
		);
		// Not marked: `option<…>` fields (`opt`, `obj`, `flex`, `roopt`,
		// `oa.x`), every DEFAULT (`dflt`, `dalw`, `optd`, `ro`, `e.w`,
		// `ip.pref`), every VALUE (`vin`, `vfall`, `vout`), COMPUTED
		// (`comp`), a relation's `in` and `out`, and nested fields.
		expect(marked.sort()).toEqual(['ip.need', 'm.nul', 'm.req', 'up.obj']);
		expect(tables.m?.fields.nul).toEqual({
			kind: 'null | string',
			rejectsNone: true,
		});
	});
});

const render = async (sql: string) =>
	renderStatementModulesSync({
		scopes: [
			{
				name: 'shop',
				files: [{ name: 'q', sql }],
				declared: await declared(),
			},
		],
	});

const problems = async (sql: string): Promise<ReadonlyArray<string>> => {
	try {
		await render(sql);
	} catch (error) {
		if (error instanceof StatementModulesError)
			return error.problems.map((problem) => problem.text);
		throw error;
	}
	return [];
};

describe('statements with schema detail', () => {
	it('binds each binding whose uses resolve to one simple kind', async () => {
		const { files } = await render(
			"SELECT * FROM gauge WHERE station = $s AND note = $n AND tags = $t;\nSELECT * FROM type::record('site', $k) WHERE label = $s;\nRETURN fn::scaled($r);\nSELECT * FROM gauge WHERE id = $i;",
		);
		expect(files[0]?.content).toContain(
			'\tbinds: { "i": "option<record<gauge>>", "k": "string", "n": "option<string>", "r": "float", "s": "option<string>" },\n',
		);
	});

	it('binds option<K> only when every use accepts NONE', async () => {
		const binds = async (sql: string) =>
			/\tbinds: (.*),\n/u.exec(
				(await render(sql)).files[0]?.content ?? '',
			)?.[1];
		// Assigned to a plain field: NONE is refused there.
		expect(await binds('UPDATE gauge SET station = $s;')).toBe(
			'{ "s": "string" }',
		);
		// Assigned to an option field, or compared with it.
		expect(await binds('UPDATE gauge SET note = $n;')).toBe(
			'{ "n": "option<string>" }',
		);
		// One use that refuses NONE decides it.
		expect(
			await binds(
				'SELECT * FROM gauge WHERE station = $s;\nUPDATE gauge SET station = $s;',
			),
		).toBe('{ "s": "string" }');
		expect(
			await binds(
				'SELECT * FROM gauge WHERE note = $n;\nUPDATE gauge SET note = $n;',
			),
		).toBe('{ "n": "option<string>" }');
	});

	it('records the first field that rejects NONE each binding is assigned to', async () => {
		const rejects = async (sql: string) =>
			/\trejectsNone: (.*),\n/u.exec(
				(await render(sql)).files[0]?.content ?? '',
			)?.[1];
		expect(await rejects('UPDATE gauge SET station = $s, note = $n;')).toBe(
			'{ "s": { table: "gauge", field: "station", kind: "string" } }',
		);
		// Any kind that excludes NONE, simple or not.
		expect(await rejects('CREATE gauge SET tags = $t;')).toBe(
			'{ "t": { table: "gauge", field: "tags", kind: "array<string>" } }',
		);
		// A comparison records nothing, beside an assignment or alone.
		expect(
			await rejects(
				'SELECT * FROM gauge WHERE station = $s;\nUPSERT gauge SET reading = $s;',
			),
		).toBe('{ "s": { table: "gauge", field: "reading", kind: "float" } }');
		expect(
			await rejects('SELECT * FROM gauge WHERE station = $s;'),
		).toBeUndefined();
		// An option field, an undeclared field of a schemaless table.
		expect(await rejects('UPDATE gauge SET note = $n;')).toBeUndefined();
		expect(
			await rejects('UPDATE scratch SET odd = $o, anything = $a;'),
		).toBeUndefined();
		// A text that changes the schema itself may change the field.
		expect(
			await rejects(
				"DEFINE FIELD OVERWRITE station ON gauge TYPE string DEFAULT 'x';\nUPDATE gauge SET station = $s;",
			),
		).toBeUndefined();
		// The first such assignment, in the order of the uses.
		expect(
			await rejects(
				'UPDATE gauge SET station = $s;\nUPDATE gauge SET tags = $s;',
			),
		).toBe('{ "s": { table: "gauge", field: "station", kind: "string" } }');
	});

	it('an argument accepts NONE only when its kind is an option', () => {
		const { files } = renderStatementModulesSync({
			scopes: [
				{
					name: 'shop',
					files: [
						{
							name: 'q',
							sql: 'RETURN fn::label($a, $b);',
						},
					],
					declared: {
						tables: [],
						functions: ['fn::label'],
						detail: {
							tables: {},
							functions: {
								'fn::label': ['option<string>', 'int'],
							},
						},
					},
				},
			],
		});
		expect(files[0]?.content).toContain(
			'\tbinds: { "a": "option<string>", "b": "int" },\n',
		);
	});

	it('binds nothing when the uses disagree or do not resolve to a kind', async () => {
		const { files } = await render(
			'SELECT * FROM gauge WHERE station = $x AND reading = $x;\nSELECT * FROM scratch WHERE free = $y AND anything = $z;',
		);
		expect(files[0]?.content).not.toContain('binds:');
		expect(files[0]?.content).toContain('uses: {');
	});

	it('without detail, nothing is bound', () => {
		const { files } = renderStatementModulesSync({
			scopes: [
				{
					name: 'shop',
					files: [
						{
							name: 'q',
							sql: 'SELECT * FROM gauge WHERE station = $s;',
						},
					],
					declared: { tables: ['gauge'], functions: [] },
				},
			],
		});
		expect(files[0]?.content).not.toContain('binds:');
	});

	it('a field the table does not declare', async () => {
		expect(
			await problems('SELECT * FROM gauge\nWHERE statoin = $s;'),
		).toEqual([
			'shop/q.surql:2: compares `statoin` of `gauge`, which declares no such field',
		]);
	});

	it('an argument beyond the function’s arguments', async () => {
		expect(await problems('RETURN fn::scaled(1, $extra);')).toEqual([
			'shop/q.surql:1: passes argument 2 to `fn::scaled`, which takes 1 argument',
		]);
	});

	it('an undeclared table or function is reported once', async () => {
		expect(
			await problems(
				'SELECT * FROM gadget WHERE a = $a;\nRETURN fn::other($b);',
			),
		).toEqual([
			'shop/q.surql:1: reads table `gadget`, which the schema does not declare',
			'shop/q.surql:2: calls function `fn::other`, which the schema does not declare',
		]);
	});

	it('a schemaless table accepts any field', async () => {
		expect(await problems('SELECT * FROM scratch WHERE odd = $a;')).toEqual(
			[],
		);
	});
});

const fixtures = join(import.meta.dirname, 'fixtures', 'statements-binds');
const golden = goldenDir('statements-binds');

describe('statements with binds: goldens', () => {
	const scopes = async (): Promise<ReadonlyArray<StatementScope>> => {
		const names = await declared();
		return readdirSync(fixtures).map((scope) => ({
			name: scope,
			declared: names,
			files: readdirSync(join(fixtures, scope)).map((file) => ({
				name: file.slice(0, -'.surql'.length),
				sql: readFileSync(join(fixtures, scope, file), 'utf8'),
			})),
		}));
	};

	it('renders the golden modules in memory', async () => {
		const { files } = renderStatementModulesSync({
			scopes: await scopes(),
		});
		for (const file of files)
			expect(file.content).toBe(
				readFileSync(join(golden, file.path), 'utf8'),
			);
	});

	it('the command with --schema writes the same modules', async () => {
		let stderr = '';
		const code = await run(
			[
				'statements',
				'--input',
				fixtures,
				'--out',
				golden,
				'--schema',
				bindsDocument,
				'--check',
			],
			{
				cwd: import.meta.dirname,
				stdin: async () => '',
				stdout: () => {},
				stderr: (text) => {
					stderr += text;
				},
			},
		);
		expect(code, stderr).toBe(0);
	});
});
