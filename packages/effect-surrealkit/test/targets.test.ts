import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Effect, Schema } from 'effect';
import { afterEach, describe, expect, it } from 'vitest';

import {
	analyzeSync,
	renderStatementModulesSync,
	StatementModulesError,
	type StatementScope,
} from '../src/analyze/index.js';
import { run } from '../src/cli/run.js';
import { declaredNames, decodeDocument } from '../src/emitter/index.js';
import { part, sequence } from '../src/statement/index.js';
import { examplePath, readJson } from './documents.js';

/** The names a text uses, as the analyzer reports them. */
const names = (sql: string) => {
	const a = analyzeSync(sql);
	return {
		read: a.tables.read,
		written: a.tables.written,
		functions: a.functions,
		open: a.openTargets,
	};
};

const none = { read: [], written: [], functions: [], open: false };

describe('analyze: written tables', () => {
	it.each([
		['CREATE gizmo SET a = 1;', ['gizmo']],
		['CREATE ONLY gizmo CONTENT { a: 1 };', ['gizmo']],
		['UPDATE gizmo SET a = 1;', ['gizmo']],
		['UPDATE ONLY gizmo:one MERGE { a: 1 };', ['gizmo']],
		['UPSERT gizmo:[1, 2] SET a = 1;', ['gizmo']],
		['DELETE gizmo WHERE a = 1;', ['gizmo']],
		['DELETE FROM gizmo WHERE a = 1;', ['gizmo']],
		['DELETE FROM ONLY gizmo:one;', ['gizmo']],
		['INSERT INTO gizmo { a: 1 };', ['gizmo']],
		['INSERT IGNORE INTO gizmo [{ a: 1 }];', ['gizmo']],
		['INSERT RELATION INTO linkage { in: $a, out: $b };', ['linkage']],
		['INSERT IGNORE RELATION INTO linkage [];', ['linkage']],
		['RELATE $a->linkage->$b;', ['linkage']],
		[
			'RELATE ONLY gizmo:one->linkage->sprocket:two SET n = 1;',
			['linkage'],
		],
		['CREATE `odd name` SET a = 1;', ['odd name']],
		['CREATE ⟨odd name⟩:k SET a = 1;', ['odd name']],
		["CREATE type::record('gizmo', $key) SET a = 1;", ['gizmo']],
		["UPDATE type::thing('gizmo', 1) SET a = 1;", ['gizmo']],
		['UPDATE gizmo, sprocket SET a = 1;', ['gizmo', 'sprocket']],
	])('%s', (sql, written) => {
		expect(names(sql)).toEqual({ ...none, written });
	});

	it('a write inside a subquery or after LET, RETURN and THEN is a write', () => {
		expect(
			names(
				'LET $x = CREATE gizmo; RETURN (UPDATE sprocket SET a = 1); IF $x THEN DELETE cog END;',
			).written,
		).toEqual(['cog', 'gizmo', 'sprocket']);
	});
});

describe('analyze: read tables', () => {
	it('every target of a FROM list, after ONLY, up to the next clause', () => {
		expect(
			names(
				"SELECT * FROM ONLY gizmo, sprocket:one, type::record('cog', 1), type::thing('wheel', 2) WHERE a = 1 LIMIT 1;",
			),
		).toEqual({ ...none, read: ['cog', 'gizmo', 'sprocket', 'wheel'] });
	});

	it('a subquery in FROM is not open: its own FROM is read', () => {
		expect(names('SELECT * FROM (SELECT * FROM gizmo WHERE a);')).toEqual({
			...none,
			read: ['gizmo'],
		});
	});

	it('subqueries anywhere in a statement are read', () => {
		expect(
			names(
				'SELECT a, (SELECT VALUE b FROM sprocket) AS b FROM gizmo WHERE c IN (SELECT VALUE c FROM cog);',
			).read,
		).toEqual(['cog', 'gizmo', 'sprocket']);
	});

	it('graph steps name their edges and tables', () => {
		expect(
			names(
				'SELECT ->owns->gadget AS g, <-made<-maker AS m, <->pairs<->twin AS t FROM gizmo;',
			),
		).toEqual({
			...none,
			read: ['gadget', 'gizmo', 'made', 'maker', 'owns', 'pairs', 'twin'],
		});
	});

	it('a parenthesised graph step with a WHERE names its edge', () => {
		expect(
			names('SELECT VALUE ->(owns WHERE n > 1)->gadget FROM gizmo;').read,
		).toEqual(['gadget', 'gizmo', 'owns']);
	});

	it('a graph step on a parameter reads its names, the parameter target is open', () => {
		expect(names('SELECT VALUE ->owns->gadget FROM $start;')).toEqual({
			...none,
			read: ['gadget', 'owns'],
			open: true,
		});
	});

	it('type::record, type::thing and type::table with a literal elsewhere are reads', () => {
		expect(
			names(
				"LET $a = type::record('gizmo', 1); LET $b = type::thing('cog', 2); RETURN type::table('wheel');",
			),
		).toEqual({ ...none, read: ['cog', 'gizmo', 'wheel'] });
	});

	it('type::table with a literal in FROM is a read', () => {
		expect(names("SELECT * FROM type::table('gizmo');")).toEqual({
			...none,
			read: ['gizmo'],
		});
	});

	it('a record-id literal outside a target is no read', () => {
		expect(names('RETURN gizmo:one;')).toEqual(none);
	});

	it('a table both read and written is in both lists', () => {
		expect(
			names('UPDATE gizmo SET n = n + 1; SELECT * FROM gizmo;'),
		).toEqual({ ...none, read: ['gizmo'], written: ['gizmo'] });
	});
});

describe('analyze: open targets', () => {
	it.each([
		'SELECT * FROM $ids;',
		'SELECT * FROM type::table($name);',
		'SELECT * FROM [gizmo:one, gizmo:two];',
		'SELECT * FROM fn::pick();',
		"LET $r = type::record($t, 'k');",
		"LET $r = type::record('gizmo:one');",
		'CREATE $target SET a = 1;',
		'UPDATE type::table($t) SET a = 1;',
		"CREATE type::table('gizmo');",
		'INSERT INTO $t { a: 1 };',
		'RELATE $a->$edge->$b;',
		'SELECT * FROM gizmo:one->owns;',
		'SELECT * FROM gizmo WHERE x IN (SELECT * FROM $other);',
		'SELECT from FROM gizmo;',
		'SELECT VALUE ->?->gadget FROM gizmo;',
	])('%s', (sql) => {
		expect(analyzeSync(sql).openTargets).toBe(true);
	});

	it('a static target is not open', () => {
		expect(analyzeSync('SELECT * FROM gizmo;').openTargets).toBe(false);
	});
});

describe('analyze: functions', () => {
	it('custom functions called, as written, with nested paths, sorted, once each', () => {
		expect(
			names(
				'RETURN fn::b(1) + fn::a::deep::name(2) + fn::b(3) + math::max([1]);',
			).functions,
		).toEqual(['fn::a::deep::name', 'fn::b']);
	});
});

describe('analyze: what contributes nothing', () => {
	it('names inside strings and comments are ignored', () => {
		expect(
			names(
				"-- SELECT * FROM ghost\n/* CREATE phantom */ # fn::hidden()\nRETURN 'SELECT * FROM spectre; fn::shade()';",
			),
		).toEqual(none);
	});

	it.each([
		'DEFINE TABLE gizmo SCHEMAFULL;',
		'DEFINE FUNCTION fn::helper($x: int) { RETURN SELECT * FROM gizmo WHERE n = $x; };',
		'DEFINE EVENT changed ON gizmo THEN (CREATE log SET at = time::now());',
		'REMOVE TABLE gizmo;',
		'ALTER TABLE gizmo COMMENT "x";',
		'INFO FOR TABLE gizmo;',
		'USE NS a DB b;',
		'SHOW CHANGES FOR TABLE gizmo SINCE 1;',
		'REBUILD INDEX idx ON gizmo;',
	])('%s', (sql) => {
		expect(names(sql)).toEqual(none);
	});

	it('an object key named like a statement is not one', () => {
		expect(
			names('RETURN { info: 1, x: (SELECT * FROM gizmo) };').read,
		).toEqual(['gizmo']);
	});
});

describe('part: tables and functions', () => {
	it('exposes the analysed values', () => {
		const p = part({
			sql: 'UPDATE gizmo SET n = fn::next(n); SELECT * FROM $ids;',
			free: ['ids'],
			slots: 2,
		});
		expect(p.tables).toEqual({ read: [], written: ['gizmo'] });
		expect(p.functions).toEqual(['fn::next']);
		expect(p.openTargets).toBe(true);
	});

	it('accepts a declaration that matches', () => {
		expect(() =>
			part({
				sql: 'SELECT * FROM gizmo;',
				tables: { read: ['gizmo'], written: [] },
				functions: [],
				openTargets: false,
			}),
		).not.toThrow();
	});

	it('refuses a wrong declaration with the difference', () => {
		expect(() =>
			part({
				name: 'wrong',
				sql: 'SELECT * FROM gizmo WHERE n = fn::count();',
				tables: { read: ['sprocket'], written: ['gizmo'] },
				functions: ['fn::other'],
				openTargets: true,
			}),
		).toThrow(
			'part `wrong` does not declare what its text does: tables.read: the text reads `gizmo`, which is not declared; tables.read: declared `sprocket`, which the text never reads; tables.written: declared `gizmo`, which the text never writes; functions: the text calls `fn::count`, which is not declared; functions: declared `fn::other`, which the text never calls; openTargets: declared true, every target of the text is a static table name',
		);
	});

	it('refuses openTargets false for an open text', () => {
		expect(() =>
			part({ sql: 'SELECT * FROM $t;', free: ['t'], openTargets: false }),
		).toThrow(
			'openTargets: declared false, a target of the text is not a static table name',
		);
	});
});

describe('sequence: tables and functions', () => {
	it('the statement carries the union over its parts', () => {
		const a = part({ sql: 'UPDATE gizmo SET n = fn::a();' });
		const b = part({
			sql: 'SELECT * FROM sprocket, gizmo WHERE n = fn::b();',
		});
		const c = part({ sql: 'SELECT * FROM $t;', free: ['t'] });
		const closed = sequence(a, b).define({
			params: {},
			result: Schema.Unknown,
		});
		expect(closed.tables).toEqual({
			read: ['gizmo', 'sprocket'],
			written: ['gizmo'],
		});
		expect(closed.functions).toEqual(['fn::a', 'fn::b']);
		expect(closed.openTargets).toBe(false);
		const opened = sequence(a, c).define({
			params: { t: Schema.Unknown },
			result: Schema.Unknown,
		});
		expect(opened.openTargets).toBe(true);
	});
});

const scope = (
	sql: string,
	declared?: StatementScope['declared'],
): StatementScope => ({
	name: 'shop',
	files: [{ name: 'q', sql }],
	...(declared !== undefined && { declared }),
});

const problemsOf = (scopes: ReadonlyArray<StatementScope>) => {
	try {
		renderStatementModulesSync({ scopes });
	} catch (error) {
		if (error instanceof StatementModulesError) return error.problems;
		throw error;
	}
	return [];
};

describe('renderStatementModules: tables and functions', () => {
	it('renders tables, functions and, only when true, openTargets', () => {
		const { files } = renderStatementModulesSync({
			scopes: [
				{
					name: 'shop',
					files: [
						{
							name: 'closed',
							sql: 'UPDATE gizmo SET n = fn::next(n);',
						},
						{ name: 'opened', sql: 'SELECT * FROM $t;' },
					],
				},
			],
		});
		const content = files[0]?.content ?? '';
		expect(content).toContain(
			'\tthrowsAt: {},\n\ttables: { read: [], written: ["gizmo"] },\n\tfunctions: ["fn::next"],\n} as const);',
		);
		expect(content).toContain(
			'\ttables: { read: [], written: [] },\n\tfunctions: [],\n\topenTargets: true,\n} as const);',
		);
	});

	const declared = { tables: ['gizmo'], functions: ['fn::known'] };

	it('reports a table the schema does not declare, at its first use', () => {
		const problems = problemsOf([
			scope(
				'SELECT * FROM gizmo;\nSELECT * FROM shipmnt;\nCREATE crate;',
				declared,
			),
		]);
		expect(problems.map((p) => p.text)).toEqual([
			'shop/q.surql:2: reads table `shipmnt`, which the schema does not declare',
			'shop/q.surql:3: writes table `crate`, which the schema does not declare',
		]);
		expect(problems[0]).toMatchObject({
			kind: 'undeclared',
			scope: 'shop',
			file: 'q',
			line: 2,
		});
	});

	it('reports a function the schema does not declare', () => {
		expect(
			problemsOf([
				scope('RETURN fn::known() + fn::unknwn();', declared),
			]).map((p) => p.text),
		).toEqual([
			'shop/q.surql:1: calls function `fn::unknwn`, which the schema does not declare',
		]);
	});

	it('a name the same text defines counts as declared', () => {
		expect(
			problemsOf([
				scope(
					'DEFINE TABLE IF NOT EXISTS scratch; DEFINE FUNCTION OVERWRITE fn::local() { RETURN 1; };\nCREATE scratch SET n = fn::local();',
					declared,
				),
			]),
		).toEqual([]);
	});

	it('names behind an open target are not reported', () => {
		expect(
			problemsOf([scope('SELECT * FROM $anything;', declared)]),
		).toEqual([]);
	});

	it('checks nothing for a scope without declared names', () => {
		expect(problemsOf([scope('SELECT * FROM shipmnt;')])).toEqual([]);
	});
});

describe('declaredNames', () => {
	it('returns the tables, edge tables included, and functions of a document', async () => {
		const document = await Effect.runPromise(
			decodeDocument(readJson(examplePath)),
		);
		expect(declaredNames(document)).toEqual({
			tables: ['doc', 'id_object', 'member_of', 'person', 'team'],
			functions: ['fn::double', 'fn::greet', 'fn::optional_first'],
		});
	});
});

describe('statements --schema', () => {
	const dirs: string[] = [];
	afterEach(() => {
		for (const dir of dirs.splice(0))
			rmSync(dir, { recursive: true, force: true });
	});
	const cli = async (sql: string, extra: string[]) => {
		const dir = mkdtempSync(join(tmpdir(), 'effect-surrealkit-schema-'));
		dirs.push(dir);
		mkdirSync(join(dir, 'queries', 'shop'), { recursive: true });
		writeFileSync(join(dir, 'queries', 'shop', 'q.surql'), sql);
		let stderr = '';
		const code = await run(
			['statements', '--input', 'queries', '--out', 'gen', ...extra],
			{
				cwd: dir,
				stdin: async () => '',
				stdout: () => {},
				stderr: (text) => {
					stderr += text;
				},
			},
		);
		return { code, stderr };
	};

	it('passes names the document declares', async () => {
		const result = await cli(
			'SELECT ->member_of->team FROM person WHERE n = fn::double(1);',
			['--schema', examplePath],
		);
		expect(result).toMatchObject({ code: 0 });
	});

	it('fails on a name the document does not declare', async () => {
		const result = await cli('SELECT * FROM shipmnt;', [
			'--schema',
			examplePath,
		]);
		expect(result.code).toBe(1);
		expect(result.stderr).toContain(
			'shop/q.surql:1: reads table `shipmnt`, which the schema does not declare',
		);
	});

	it('without --schema checks nothing', async () => {
		expect((await cli('SELECT * FROM shipmnt;', [])).code).toBe(0);
	});

	it('refuses a file that is not a schema document', async () => {
		const result = await cli('SELECT * FROM gizmo;', [
			'--schema',
			'missing.json',
		]);
		expect(result.code).toBe(2);
		expect(result.stderr).toContain('--schema missing.json');
	});
});
