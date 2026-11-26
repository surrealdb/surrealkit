import { describe, expect, it } from 'vitest';

import { analyzeSync } from '../src/analyze/index.js';

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

	it('nothing of an object literal whose key is a statement word, but its subqueries', () => {
		for (const word of [
			'select',
			'CREATE',
			'update',
			'UPSERT',
			'delete',
			'INSERT',
			'relate',
		]) {
			for (const sql of [
				`RETURN { ${word}: 1, where: station = $x, set: 2 };`,
				`RETURN { ${word}: gizmo, from: gizmo, a: 1 + from };`,
				`IF $c { RETURN { ${word}: 1 } };`,
			]) {
				const analysis = analyzeSync(sql);
				expect(analysis.uses, sql).toEqual({});
				expect(analysis.fields, sql).toEqual([]);
				expect(analysis.projected, sql).toEqual([]);
			}
			const inner = `RETURN { ${word}: 1, a: (SELECT b FROM gizmo WHERE c = $c) };`;
			expect(uses(inner), inner).toEqual({ c: [field('gizmo', 'c')] });
			expect(analyzeSync(inner).projected, inner).toEqual([
				{ table: 'gizmo', field: 'b', line: 1 },
			]);
		}
	});

	it('an assignment in a query whose only WHERE is inside a subquery', () => {
		for (const sql of [
			'UPDATE gizmo SET a = $a, b = (SELECT VALUE c FROM gizmo WHERE d = 1);',
			'UPDATE gizmo SET a = $a RETURN (SELECT * FROM gizmo WHERE d = 1);',
			'CREATE gizmo SET a = $a, b = { where: 1 };',
		])
			expect(uses(sql).a, sql).toEqual([
				field('gizmo', 'a', 1, 'assign'),
			]);
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

	it('a parenthesised query that is the whole value of a top-level statement', () => {
		for (const sql of [
			'(UPDATE gizmo SET a = $a);',
			'LET $r = (UPDATE gizmo SET a = $a);',
			'RETURN (UPDATE gizmo SET a = $a)',
		])
			expect(uses(sql), sql).toEqual({
				a: [field('gizmo', 'a', 1, 'assign')],
			});
		expect(uses('RETURN (SELECT * FROM gizmo WHERE b = $b);')).toEqual({
			b: [field('gizmo', 'b')],
		});
	});

	it('a subquery is a query of its own table', () => {
		expect(
			uses(
				'SELECT * FROM gizmo WHERE a IN (SELECT VALUE id FROM sprocket WHERE b = $b);',
			),
		).toEqual({ b: [field('sprocket', 'b')] });
	});

	it('a comparison in any subquery, but no assignment there', () => {
		// A condition around the subquery can keep NONE from an assignment;
		// a comparison accepts NONE, so a use there says the same either way.
		for (const sql of [
			'LET $r = IF $c THEN 1 ELSE (SELECT * FROM gizmo WHERE b = $b) END;',
			'RETURN array::len((SELECT * FROM gizmo WHERE b = $b));',
			'RETURN $c AND (SELECT * FROM gizmo WHERE b = $b);',
			'IF $c { RETURN (SELECT * FROM gizmo WHERE b = $b) };',
		])
			expect(uses(sql), sql).toEqual({ b: [field('gizmo', 'b')] });
		const both =
			'RETURN IF $c THEN (DELETE gizmo WHERE b = $b) ELSE (UPDATE gizmo SET a = $a) END;';
		expect(uses(both)).toEqual({ b: [field('gizmo', 'b')] });
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
		[
			'a subquery in FROM',
			'SELECT * FROM (SELECT a AS s FROM gizmo) WHERE s = $s AND $t < t;',
		],
		[
			'a subquery in FROM after a table',
			'SELECT * FROM gizmo, (SELECT a AS s FROM gizmo) WHERE s = $s;',
		],
		[
			'a subquery in FROM ONLY',
			'SELECT * FROM ONLY (SELECT VALUE b FROM ONLY gizmo:1) WHERE s = $s;',
		],
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
		[
			'a parenthesised query in a block',
			'IF $c { (UPDATE gizmo SET a = $a) };',
		],
		[
			'a LET of a parenthesised query in a block',
			'IF $c { LET $r = (UPDATE gizmo SET a = $a); RETURN $r; };',
		],
		[
			'a parenthesised query in ELSE',
			'IF $c { 1 } ELSE { (UPDATE gizmo SET a = $a) };',
		],
		['IF … THEN … END', 'RETURN IF $c THEN (UPDATE gizmo SET a = $a) END;'],
		[
			'IF … ELSE … END',
			'LET $r = IF $c THEN 1 ELSE (UPSERT gizmo SET a = $a) END;',
		],
		['an operand of AND', 'RETURN $c AND (UPDATE gizmo SET a = $a);'],
		['an operand of ||', 'RETURN $c || (UPDATE gizmo SET a = $a);'],
		[
			'a projection subquery',
			'SELECT (UPDATE gizmo SET a = $a) AS u FROM sprocket WHERE $c;',
		],
		['a closure body', 'RETURN $list.map(|$v| (UPDATE gizmo SET a = $a));'],
		[
			'a function argument',
			'RETURN array::len((CREATE gizmo SET a = $a));',
		],
		[
			'a parenthesised query with more after it',
			'LET $r = (UPDATE gizmo SET a = $a)[0];',
		],
		[
			'an assignment in a query with a WHERE on the binding',
			'UPDATE gizmo SET a = $a WHERE $a != NONE;',
		],
		[
			'an assignment in a query with any WHERE',
			'UPSERT gizmo SET a = $a WHERE b = 1;',
		],
		[
			'an assignment in a whole-value query with a WHERE',
			'LET $r = (UPDATE gizmo SET a = $a WHERE $ok);',
		],
		['a non-query statement', 'IF a = $a THEN 1 END;'],
		['a value word', 'SELECT * FROM gizmo WHERE NONE = $a;'],
		[
			'a name the text also defines',
			'SELECT * FROM gizmo WHERE a = $a; LET $a = 1;',
		],
	])('records nothing for %s', (_, sql) => {
		expect(uses(sql)).toEqual({});
	});

	it.each([
		'SELECT * FROM gizmo:1 WHERE a = $a AND $b < b FOR UPDATE;',
		'SELECT * FROM ONLY gizmo:1 WHERE a = $a AND $b < b for update;',
		'SELECT * FROM ONLY gizmo:1 WHERE a = $a AND $b < b FOR UPDATE EXPLAIN;',
		"LET $row = (SELECT * FROM ONLY type::record('gizmo', 1) WHERE a = $a AND $b < b FOR UPDATE);",
	])('comparisons before FOR UPDATE on a record id: %s', (sql) => {
		expect(uses(sql)).toEqual({
			a: [field('gizmo', 'a')],
			b: [field('gizmo', 'b')],
		});
	});

	it.each([
		// The engines refuse FOR UPDATE on a table: it needs record ids.
		'SELECT * FROM gizmo WHERE a = $a FOR UPDATE;',
		'SELECT * FROM gizmo:1 WHERE a = $a GROUP BY a FOR UPDATE;',
		'SELECT * FROM gizmo:1 WHERE a = $a FOR UPDATE FETCH b;',
	])('records nothing before FOR UPDATE the engines refuse: %s', (sql) => {
		expect(uses(sql)).toEqual({});
	});
});
