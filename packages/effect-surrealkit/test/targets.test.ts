import { describe, expect, it } from 'vitest';

import { analyzeSync } from '../src/analyze/index.js';

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

	it.each([
		'SELECT * FROM gizmo:1 FOR UPDATE;',
		'SELECT * FROM ONLY gizmo:1 WHERE a = $a FOR UPDATE;',
		'SELECT * FROM gizmo:1, gizmo:2 FOR UPDATE EXPLAIN;',
		"SELECT * FROM ONLY type::record('gizmo', $k) FOR UPDATE;",
	])('the record ids before FOR UPDATE: %s', (sql) => {
		expect(names(sql)).toEqual({ ...none, read: ['gizmo'] });
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
		// FOR UPDATE the engines refuse: on a table, a range, an array, with
		// GROUP, SPLIT or VERSION, or followed by another clause.
		'SELECT * FROM gizmo FOR UPDATE;',
		'SELECT * FROM gizmo WHERE a = 1 FOR UPDATE;',
		"SELECT * FROM type::table('gizmo') FOR UPDATE;",
		'SELECT * FROM gizmo:1..5 FOR UPDATE;',
		'SELECT * FROM [gizmo:1] FOR UPDATE;',
		'SELECT * FROM gizmo:1 GROUP BY a FOR UPDATE;',
		'SELECT * FROM gizmo:1 FOR UPDATE FETCH a;',
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
