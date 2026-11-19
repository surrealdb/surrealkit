import { Effect } from 'effect';
import { describe, expect, it } from 'vitest';

import {
	type Analysis,
	AnalyzeError,
	analyze,
	analyzeSync,
	FillError,
	fill,
	text,
} from '../src/analyze/index.js';

const run = <A, E>(effect: Effect.Effect<A, E>) =>
	Effect.runSync(Effect.result(effect));

/** The facts a part declares, without the rest of the analysis. */
const facts = (sql: string) => {
	const a = analyzeSync(sql);
	return {
		kind: a.kind,
		free: a.free,
		defines: a.defines,
		slots: a.slots,
		throws: a.throws,
	};
};

const failure = (sql: string, options = {}): AnalyzeError => {
	try {
		analyzeSync(sql, options);
	} catch (error) {
		if (error instanceof AnalyzeError) return error;
		throw error;
	}
	throw new Error(`analysed without an error: ${sql}`);
};

const codes = (a: Analysis) => a.diagnostics.map((d) => d.code);

describe('analyze: variables', () => {
	it('free names are the reads nothing defined earlier, sorted, without `$`', () => {
		expect(facts('SELECT * FROM t WHERE b = $b AND a = $a;')).toEqual({
			kind: 'part',
			free: ['a', 'b'],
			defines: [],
			slots: 1,
			throws: [],
		});
	});

	it('a top-level LET defines a name later statements read', () => {
		expect(facts('LET $x = $input1; RETURN $x + $y;')).toMatchObject({
			free: ['input1', 'y'],
			defines: ['x'],
			slots: 2,
		});
	});

	it('a read before the LET of the same name is free, including on its own right side', () => {
		expect(facts('RETURN $x; LET $x = 1;')).toMatchObject({
			free: ['x'],
			defines: ['x'],
		});
		expect(facts('LET $x = $x + 1;')).toMatchObject({
			free: ['x'],
			defines: ['x'],
		});
	});

	it('a LET inside a block is local to the block, and shadows', () => {
		expect(
			facts('IF true { LET $t = 1; RETURN $t; }; RETURN $t;'),
		).toMatchObject({ free: ['t'], defines: [] });
		expect(
			facts(
				'LET $x = 1; IF $x { LET $x = 2; RETURN $x; }; RETURN { LET $y = $x; $y };',
			),
		).toMatchObject({ free: [], defines: ['x'], slots: 3 });
	});

	it('nested blocks see outer definitions and keep their own', () => {
		expect(
			facts(
				'LET $a = 1; IF $a { LET $b = $a; IF $b { LET $c = $b; RETURN $c + $d; }; RETURN $c; };',
			),
		).toMatchObject({ free: ['c', 'd'], defines: ['a'] });
	});

	it('closure parameters, typed or not, are local to the closure body', () => {
		expect(
			facts(
				'RETURN array::map($xs, |$x| $x + $k), $x; RETURN array::fold($xs, 0, |$a: int, $b: option<int>| -> int { $a + $b });',
			),
		).toMatchObject({ free: ['k', 'x', 'xs'] });
		expect(
			facts(
				'LET $f = |$a| array::map($a, |$b| $b + $a); RETURN $f([1]);',
			),
		).toMatchObject({ free: [], defines: ['f'] });
	});

	it('FOR binds its variable in the loop body only', () => {
		expect(
			facts('FOR $i IN $items { CREATE t SET n = $i; }; RETURN $i;'),
		).toMatchObject({ free: ['i', 'items'], slots: 2 });
		expect(facts('FOR $i IN $i { };')).toMatchObject({ free: ['i'] });
		// FOR without a variable is a clause, not a loop.
		expect(facts('SELECT * FROM ONLY $id FOR UPDATE;')).toMatchObject({
			free: ['id'],
		});
	});

	it('DEFINE FUNCTION parameters are local to its body', () => {
		expect(
			facts(
				'DEFINE FUNCTION fn::add($a: int, $b: option<int>) -> int { RETURN $a + $b + $c; };',
			),
		).toMatchObject({ free: ['c'], defines: [] });
	});

	it('engine names are never free, and a LET of a protected one is an error', () => {
		expect(
			facts(
				'RETURN [$auth.id, $access, $session, $token, $this, $self, $parent];',
			).free,
		).toEqual([]);
		const a = analyzeSync('LET $auth = 1; RETURN $auth;');
		expect(a.defines).toEqual([]);
		expect(a.diagnostics).toMatchObject([
			{ severity: 'error', code: 'protectedLet', line: 1, column: 5 },
		]);
	});

	it('$value, $input, $before, $after and $event are ordinary in a script, with a warning when free', () => {
		const a = analyzeSync(
			'RETURN [$value, $input, $before, $after, $event];',
		);
		expect(a.free).toEqual(['after', 'before', 'event', 'input', 'value']);
		expect(codes(a)).toEqual(Array(5).fill('contextualName'));
		const defined = analyzeSync('LET $event = $boundEvent; RETURN $event;');
		expect(defined.free).toEqual(['boundEvent']);
		expect(defined.diagnostics).toEqual([]);
	});

	it('inside a definition the engine sets them, so they are not free there', () => {
		expect(
			facts(
				'DEFINE FIELD n ON t VALUE $value ASSERT $value > $min; DEFINE EVENT e ON t WHEN $event = "CREATE" THEN { CREATE log SET after = $after };',
			).free,
		).toEqual(['min']);
	});

	it('parameters may be written with backticks or angle brackets', () => {
		expect(facts('LET $`a` = 1; RETURN [$`a`, $⟨b⟩];')).toMatchObject({
			free: ['b'],
			defines: ['a'],
		});
		expect(codes(analyzeSync('RETURN $`a b`;'))).toEqual([
			'unbindableName',
		]);
	});
});

describe('analyze: lexing', () => {
	it('`$`, `;` and comment markers inside strings are text', () => {
		expect(
			facts(
				`RETURN ['$no; -- x', "$no # /* y", 'it\\'s $no', "a\\"$no"];`,
			),
		).toMatchObject({ free: [], slots: 1 });
	});

	it('prefixed strings are strings', () => {
		expect(
			facts(
				`RETURN [r"t:$no;", d'2020-01-01', u'0190d0fa-0000-7000-8000-000000000000', s'$no', f"b:/$no", b'00'];`,
			).free,
		).toEqual([]);
	});

	it('backtick and ⟨⟩ identifiers are text', () => {
		expect(facts('SELECT `$no;` FROM ⟨$no; -- ⟩, t:⟨a\\⟩b⟩;').free).toEqual(
			[],
		);
	});

	it('--, //, # and /* */ comments are skipped', () => {
		expect(
			facts(
				'-- $a;\n// $b;\n# $c;\n/* $d;\n; */ RETURN 1 /* ; */; RETURN 6 / 2; -- trailing $e',
			),
		).toMatchObject({ free: [], slots: 2 });
	});

	it('a regex literal is opaque', () => {
		expect(facts("RETURN 'a' = /$no;'/;").free).toEqual([]);
	});

	it('only top-level `;` ends a statement', () => {
		expect(
			facts('IF true { LET $a = 1; LET $b = 2; RETURN [$a, $b]; };')
				.slots,
		).toBe(1);
	});

	it.each([
		["RETURN 'open;", 'unterminated', 1, 8],
		['RETURN "open;', 'unterminated', 1, 8],
		["RETURN r'open;", 'unterminated', 1, 8],
		['RETURN `open;', 'unterminated', 1, 8],
		['RETURN ⟨open;', 'unterminated', 1, 8],
		['RETURN 1;\n/* open', 'unterminated', 2, 1],
		['RETURN (1;', 'unbalanced', 1, 8],
		['RETURN 1);', 'unbalanced', 1, 9],
		['RETURN [1);', 'unbalanced', 1, 10],
		["RETURN '\\d';", 'invalidEscape', 1, 9],
		['RETURN $ x;', 'invalidParameter', 1, 8],
		['RETURN 1 \\ 2;', 'unexpectedCharacter', 1, 10],
		['RETURN 1;;', 'emptyStatement', 1, 10],
		['', 'empty', 1, 1],
		['-- nothing', 'empty', 1, 1],
		['RETURN 1;\nRETURN 2', 'unterminatedStatement', 2, 1],
		['LET $x = 1', 'notAFragment', 1, 1],
		[
			'RETURN array::map($x, |$a: int | string| $a);',
			'unexpectedToken',
			1,
			44,
		],
	] as const)('fails on %j with %s at %i:%i', (sql, code, line, column) => {
		expect(failure(sql)).toMatchObject({ code, line, column });
	});

	it('fails through the Effect error channel', () => {
		const result = run(analyze("RETURN 'x"));
		expect(result._tag).toBe('Failure');
	});
});

describe('analyze: results', () => {
	it('every terminated top-level statement is one result', () => {
		expect(
			facts(
				[
					'LET $n = 2;',
					'IF $n = 0 { 99 };',
					'IF $n > 0 { $n + 1 } ELSE { 99 };',
					'FOR $i IN [1, 2] { LET $local = $i; };',
					'$n * 3;',
					'CREATE slot:one RETURN NONE;',
					'{ LET $x = $n + 3; $x + 1 };',
				].join('\n'),
			).slots,
		).toBe(7);
	});

	it('BEGIN, COMMIT and CANCEL are results, and make the text transactional', () => {
		const a = analyzeSync('BEGIN; LET $x = 2; $x + 1; COMMIT; 2 + 2;');
		expect(a.slots).toBe(5);
		expect(a.transactional).toBe(true);
		expect(
			analyzeSync('BEGIN TRANSACTION; CANCEL TRANSACTION;'),
		).toMatchObject({ slots: 2, transactional: true });
		expect(analyzeSync('RETURN 1;').transactional).toBe(false);
	});

	it('a RETURN inside BEGIN … COMMIT followed by another statement is an error', () => {
		expect(
			analyzeSync('BEGIN; RETURN 1; 2; COMMIT;').diagnostics,
		).toMatchObject([
			{ severity: 'error', code: 'returnInTransaction', column: 8 },
		]);
		expect(
			codes(analyzeSync('BEGIN; IF true { RETURN 1 }; 2; COMMIT;')),
		).toEqual(['returnInTransaction']);
		// Last before COMMIT, outside a transaction, or a clause: fine.
		expect(codes(analyzeSync('BEGIN; RETURN 1; COMMIT; 3;'))).toEqual([]);
		expect(codes(analyzeSync('RETURN 1; 2;'))).toEqual([]);
		expect(
			codes(analyzeSync('BEGIN; CREATE t RETURN NONE; 2; COMMIT;')),
		).toEqual([]);
		expect(
			codes(
				analyzeSync(
					'BEGIN; LET $f = |$a| { RETURN $a }; $f(1); COMMIT;',
				),
			),
		).toEqual([]);
	});

	it('an unterminated single expression or block is a fragment with no results', () => {
		expect(facts('owner = $owner -- a note')).toMatchObject({
			kind: 'fragment',
			free: ['owner'],
			slots: 0,
		});
		expect(facts('{ LET $a = 1; $a + $b }')).toMatchObject({
			kind: 'fragment',
			free: ['b'],
			defines: [],
		});
		expect(analyzeSync('owner = $owner -- a note').sql).toBe(
			'owner = $owner',
		);
	});
});

describe('analyze: THROW', () => {
	it('collects literal messages at any depth, deduplicated and sorted, with their lines', () => {
		const a = analyzeSync(
			[
				"IF $a { THROW 'b' };",
				'IF $b { IF $c { THROW "a" } };',
				"THROW 'b';",
			].join('\n'),
		);
		expect(a.throws).toEqual(['a', 'b']);
		expect(a.throwsAt).toEqual({ a: [2], b: [1, 3] });
		expect(a.diagnostics).toEqual([]);
	});

	it('resolves escapes in a message', () => {
		expect(analyzeSync("THROW 'it\\'s \\u{41}';").throws).toEqual([
			"it's A",
		]);
	});

	it('a THROW of anything else is a dynamicThrow warning', () => {
		const a = analyzeSync(
			"THROW string::concat('x', $y); THROW 'a' + $b; THROW $c;",
		);
		expect(a.throws).toEqual([]);
		expect(a.diagnostics.map((d) => [d.severity, d.code])).toEqual([
			['warning', 'dynamicThrow'],
			['warning', 'dynamicThrow'],
			['warning', 'dynamicThrow'],
		]);
	});
});

describe('analyze: holes', () => {
	it('reports holes outside strings and comments, and marks the analysis provisional', () => {
		const a = analyzeSync(
			"LET $rows = SELECT * FROM t WHERE __FILTER__ LIMIT __LIMIT__; RETURN ['__NOT__', $rows]; -- __NOR__",
		);
		expect(a.holes).toEqual(['__FILTER__', '__LIMIT__']);
		expect(a.provisional).toBe(true);
		expect(a.defines).toEqual(['rows']);
		expect(analyzeSync('RETURN 1;').provisional).toBe(false);
		// Part of a longer name is not a hole.
		expect(analyzeSync('RETURN X__A__ + __A__x;').holes).toEqual([]);
	});

	it('takes a configurable pattern', () => {
		expect(
			analyzeSync('RETURN {{ limit }};', {
				holePattern: '\\{\\{ [a-z]+ \\}\\}',
			}).holes,
		).toEqual(['{{ limit }}']);
		expect(failure('RETURN 1;', { holePattern: 'a*' }).code).toBe(
			'invalidHolePattern',
		);
		expect(failure('RETURN 1;', { holePattern: '(' }).code).toBe(
			'invalidHolePattern',
		);
	});
});

describe('fill', () => {
	const template = text({
		kind: 'part',
		sql: 'SELECT * FROM t WHERE __FILTER__ LIMIT __LIMIT__;',
		holes: ['__FILTER__', '__LIMIT__'],
		free: [],
		defines: [],
		provisional: true,
		slots: 1,
		throws: [],
	});

	it('substitutes every hole, and the result analyses as a part', () => {
		const result = run(
			fill(template, {
				__FILTER__: '  -- lead\n a = $a -- trailing',
				__LIMIT__: text({
					kind: 'fragment',
					sql: '$n',
					holes: [],
					free: ['n'],
					defines: [],
					provisional: false,
					slots: 0,
					throws: [],
				}),
			}),
		);
		expect(result).toMatchObject({
			_tag: 'Success',
			success: 'SELECT * FROM t WHERE a = $a LIMIT $n;',
		});
		if (result._tag === 'Success') {
			expect(analyzeSync(result.success)).toMatchObject({
				free: ['a', 'n'],
				holes: [],
				provisional: false,
			});
		}
	});

	it('leaves holes in strings and comments alone', () => {
		expect(
			run(fill("RETURN ['__A__', __A__]; -- __A__", { __A__: '1' })),
		).toMatchObject({ success: "RETURN ['__A__', 1]; -- __A__" });
	});

	it.each([
		[{ __FILTER__: 'x', __LIMIT__: '1', __OTHER__: '2' }, 'unknownHole'],
		[{ __FILTER__: 'x' }, 'missingFill'],
		[{ __FILTER__: 'a = __INNER__', __LIMIT__: '1' }, 'nestedHole'],
		[{ __FILTER__: 'a = 1) OR (1', __LIMIT__: '1' }, 'unbalanced'],
		[{ __FILTER__: "'open", __LIMIT__: '1' }, 'unterminated'],
		[{ __FILTER__: '-- only a comment', __LIMIT__: '1' }, 'emptyFill'],
	] as const)('fails on %j with %s', (fills, code) => {
		const result = run(
			fill(
				template.sql,
				fills as unknown as Readonly<Record<string, string>>,
			),
		);
		expect(result._tag).toBe('Failure');
		if (result._tag === 'Failure') {
			expect(result.failure).toBeInstanceOf(FillError);
			expect(result.failure.code).toBe(code);
		}
	});

	it('accepts a fill with holes when allowed', () => {
		expect(
			run(
				fill(
					template,
					{ __FILTER__: 'a = __INNER__', __LIMIT__: '1' },
					{ allowHoles: true },
				),
			),
		).toMatchObject({
			success: 'SELECT * FROM t WHERE a = __INNER__ LIMIT 1;',
		});
	});
});
