/**
 * Engine and SDK behaviour the statement layer relies on and the SurrealDB
 * documentation does not state, pinned on whichever engine the run uses (see
 * `engine.ts`): what each statement contributes to the list of results, what
 * the stateless transaction does, how parameters, missing records and
 * permissions behave, and what each `RETURN` mode yields.
 *
 * Where SurrealDB 3 and 4 differ, the test branches on the engine's major
 * version and asserts both, so a run on either engine fails when the engine
 * stops behaving as recorded here.
 */
import {
	Duration,
	RecordId,
	type Surreal,
	type SurrealSession,
} from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] conformance suite skipped: ${engineSkipReason}`,
	);
}

describe.skipIf(!engineConfigured)('engine conformance', () => {
	let engine: Engine;
	let db: Surreal;

	/** The results of one request, one entry per SurrealQL statement. */
	const query = (
		sql: string,
		bindings?: Record<string, unknown>,
		session: Pick<SurrealSession, 'query'> = db,
	): Promise<unknown[]> =>
		within(Promise.resolve(session.query<unknown[]>(sql, bindings)));

	beforeAll(async () => {
		engine = await startEngine();
		db = engine.db;
		console.info(
			`[effect-surrealkit] conformance: ${engine.mode} engine ${engine.version}`,
		);
	});
	afterAll(async () => {
		await engine?.stop();
	});

	describe('results of a request: one per top-level statement', () => {
		it('LET, a guard that does not fire and FOR yield NONE; expressions, IF/ELSE and blocks yield their value', async () => {
			expect(
				await query(
					[
						'LET $n = 2;',
						'IF $n = 0 { 99 };',
						'IF $n > 0 { $n + 1 } ELSE { 99 };',
						'FOR $i IN [1, 2] { LET $local = $i; };',
						'$n * 3;',
						'CREATE slot:one RETURN NONE;',
						'{ LET $x = $n + 3; $x + 1 };',
					].join('\n'),
				),
			).toStrictEqual([undefined, undefined, 3, undefined, 6, [], 6]);
		});

		it('a guard that throws nothing is one NONE result', async () => {
			expect(
				await query(
					"LET $n = 2; IF $n = 0 { THROW 'never' }; RETURN 7;",
				),
			).toStrictEqual([undefined, undefined, 7]);
		});

		it('BEGIN and COMMIT written in the text each yield a NONE result', async () => {
			expect(
				await query('BEGIN; LET $x = 2; $x + 1; COMMIT; 2 + 2;'),
			).toStrictEqual([undefined, undefined, 3, undefined, 4]);
			expect(
				await query(
					'BEGIN TRANSACTION; CREATE slot:two RETURN NONE; COMMIT TRANSACTION;',
				),
			).toStrictEqual([undefined, [], undefined]);
		});

		it('a top-level RETURN does not end the request', async () => {
			expect(await query('RETURN 1; 2;')).toStrictEqual([1, 2]);
		});

		it('RETURN inside BEGIN … COMMIT ends the transaction on SurrealDB 3 and not on 4', async () => {
			const results = await query('BEGIN; RETURN 1; 2; COMMIT;');
			if (engine.majorVersion >= 4) {
				expect(results).toStrictEqual([undefined, 1, 2, undefined]);
			} else {
				// The statement after RETURN is skipped and has no result.
				expect(results).toStrictEqual([undefined, 1, undefined]);
			}
		});

		it('CREATE … RETURN NONE yields an empty list, and NONE with ONLY', async () => {
			expect(
				await query(
					'CREATE slot:three SET n = 1 RETURN NONE; CREATE ONLY slot:four SET n = 1 RETURN NONE;',
				),
			).toStrictEqual([[], undefined]);
		});
	});

	describe('the stateless transaction', () => {
		it('returns one result per statement, not one per input', async () => {
			expect(
				await within(
					db.transaction<unknown[]>([
						'LET $a = 1; $a + 1;',
						'CREATE tx:one SET n = 1 RETURN NONE; SELECT VALUE n FROM ONLY tx:one;',
					]),
				),
			).toStrictEqual([undefined, 2, [], 1]);
		});

		it('rejects with the thrown message and commits nothing', async () => {
			await expect(
				within(
					db.transaction([
						'CREATE tx:two SET n = 1;',
						"THROW 'stop here';",
					]),
				),
			).rejects.toMatchObject({
				kind: 'Thrown',
				message: 'An error occurred: stop here',
			});
			expect(await query('SELECT * FROM tx:two;')).toStrictEqual([[]]);
		});

		it('a THROW outside a transaction carries the same message', async () => {
			await expect(query("THROW 'plain';")).rejects.toMatchObject({
				kind: 'Thrown',
				message: 'An error occurred: plain',
			});
		});

		it('the SDK refuses a RETURN that another statement follows, before sending', async () => {
			const refused =
				/RETURN statement which is followed by another statement/;
			await expect(
				within(db.transaction(['RETURN 1;', '2;'])),
			).rejects.toThrow(refused);
			await expect(
				within(db.transaction(['RETURN 1; 2;'])),
			).rejects.toThrow(refused);
		});

		it('accepts a RETURN as the last statement', async () => {
			expect(
				await within(
					db.transaction<unknown[]>(['LET $q = 5;', 'RETURN $q;']),
				),
			).toStrictEqual([undefined, 5]);
		});
	});

	describe('parameters', () => {
		it('a name nothing bound evaluates to NONE, without an error', async () => {
			expect(
				await query('RETURN [$unbound = NONE, $unbound]; $unbound;'),
			).toStrictEqual([[true, undefined], undefined]);
		});

		it('a protected name cannot be bound or defined', async () => {
			const refused = {
				kind: 'Validation',
				message: "'auth' is a protected variable and cannot be set",
			};
			await expect(
				query('RETURN 1;', { auth: 'x' }),
			).rejects.toMatchObject(refused);
			await expect(
				query("LET $auth = 'spoofed'; RETURN 1;"),
			).rejects.toMatchObject(refused);
		});

		it('a name the engine sets only inside clauses can be bound at the top level', async () => {
			expect(await query('RETURN $this;', { this: 'x' })).toStrictEqual([
				'x',
			]);
		});
	});

	describe('missing records and tables', () => {
		it('ONLY on a missing record of a defined table is NONE; without ONLY it is an empty list', async () => {
			expect(
				await query(
					'DEFINE TABLE known; SELECT * FROM ONLY known:missing; SELECT * FROM known:missing; SELECT * FROM ONLY known LIMIT 1;',
				),
			).toStrictEqual([undefined, undefined, [], undefined]);
		});

		it('reading a table that does not exist is an error', async () => {
			const missing = {
				kind: 'NotFound',
				message: "The table 'no_such_table' does not exist",
			};
			await expect(
				query('SELECT * FROM no_such_table;'),
			).rejects.toMatchObject(missing);
			await expect(
				query('SELECT * FROM ONLY no_such_table:missing;'),
			).rejects.toMatchObject(missing);
		});
	});

	describe('clauses that take parameters', () => {
		beforeAll(async () => {
			await query(
				'CREATE lim:1 SET n = 1; CREATE lim:2 SET n = 2; CREATE lim:3 SET n = 3; CREATE lim:4 SET n = 4;',
			);
		});

		it('LIMIT and START take bound integers', async () => {
			expect(
				await query(
					'SELECT VALUE n FROM lim ORDER BY n LIMIT $limit START $start;',
					{ limit: 2, start: 1 },
				),
			).toStrictEqual([[2, 3]]);
		});

		it('LIMIT with a bound float is truncated on SurrealDB 3 and refused on 4', async () => {
			const limited = query(
				'SELECT VALUE n FROM lim ORDER BY n LIMIT $limit;',
				{ limit: 1.5 },
			);
			if (engine.majorVersion >= 4) {
				await expect(limited).rejects.toThrow(
					/LIMIT clause must evaluate to a positive integer/,
				);
			} else {
				expect(await limited).toStrictEqual([[1]]);
			}
		});

		it('TIMEOUT takes a bound duration', async () => {
			expect(
				await query(
					'SELECT VALUE n FROM lim ORDER BY n TIMEOUT $timeout;',
					{ timeout: new Duration('5s') },
				),
			).toStrictEqual([[1, 2, 3, 4]]);
		});

		it('TIMEOUT with a bound string is refused on SurrealDB 3 and read as a duration on 4', async () => {
			const timed = query(
				'SELECT VALUE n FROM lim ORDER BY n TIMEOUT $timeout;',
				{ timeout: '5s' },
			);
			if (engine.majorVersion >= 4) {
				expect(await timed).toStrictEqual([[1, 2, 3, 4]]);
			} else {
				await expect(timed).rejects.toThrow(/Invalid timeout value/);
			}
		});
	});

	describe('record ids and ranges from bound key parts', () => {
		beforeAll(async () => {
			await query(`
				DEFINE TABLE rk_n; DEFINE TABLE rk_a; DEFINE TABLE rk_o; DEFINE TABLE rk_s;
				DEFINE TABLE rk_edge TYPE RELATION;
				CREATE rk_n:1 SET v = 1; CREATE rk_n:2 SET v = 2; CREATE rk_n:3 SET v = 3; CREATE rk_n:4 SET v = 4;
				CREATE rk_a:['x', 1] SET v = 1; CREATE rk_a:['x', 2] SET v = 2; CREATE rk_a:['y', 1] SET v = 3;
				CREATE rk_o:{ r: 'eu', n: 1 } SET v = 1;
				CREATE rk_s:ann SET v = 1; CREATE rk_s:bob SET v = 2;
			`);
		});

		it('a binding is no scalar key in a record id or range literal', async () => {
			const unparsed =
				/Unexpected token `\$param` expected a record-id key/;
			await expect(
				query('SELECT * FROM rk_s:$k;', { k: 'ann' }),
			).rejects.toThrow(unparsed);
			await expect(
				query('SELECT * FROM rk_n:$a..$b;', { a: 1, b: 3 }),
			).rejects.toThrow(unparsed);
		});

		it('type::record builds an id from a scalar, an array or an object of bound parts', async () => {
			expect(
				await query(
					[
						'SELECT VALUE v FROM ONLY type::record(s"rk_s", $k);',
						'SELECT VALUE v FROM ONLY type::record(s"rk_a", [$p, $q]);',
						'SELECT VALUE v FROM ONLY type::record(s"rk_o", { "r": $r, "n": $n });',
						// A key is a value, never parsed as an id.
						'RETURN <string> type::record(s"rk_s", $odd);',
					].join('\n'),
					{ k: 'bob', p: 'x', q: 2, r: 'eu', n: 1, odd: 'a:b' },
				),
			).toStrictEqual([2, 2, 1, 'rk_s:`a:b`']);
		});

		it('CREATE, UPDATE and DELETE take it as their subject; RELATE takes it parenthesised', async () => {
			const [created, updated, deleted] = (await query(
				[
					'CREATE ONLY type::record(s"rk_s", $k) SET v = 9 RETURN VALUE v;',
					'UPDATE ONLY type::record(s"rk_s", $k) SET v = 10 RETURN VALUE v;',
					'DELETE ONLY type::record(s"rk_s", $k) RETURN VALUE $before.v;',
				].join('\n'),
				{ k: 'dee' },
			)) as unknown[];
			expect([created, updated, deleted]).toStrictEqual([9, 10, 10]);
			await expect(
				query(
					'RELATE ONLY type::record(s"rk_s", $k)->rk_edge->rk_s:bob;',
					{ k: 'ann' },
				),
			).rejects.toThrow(/Parse error/);
			const [edge] = (await query(
				'RELATE ONLY (type::record(s"rk_s", $k))->rk_edge->(r"rk_s:bob") RETURN VALUE [<string> in, <string> out];',
				{ k: 'ann' },
			)) as [unknown];
			expect(edge).toStrictEqual(['rk_s:ann', 'rk_s:bob']);
		});

		it('type::record spans a range between bound keys: open sides, an excluded start, an included end', async () => {
			expect(
				await query(
					[
						'SELECT VALUE v FROM type::record(s"rk_n", $lo..$hi);',
						'SELECT VALUE v FROM type::record(s"rk_n", $lo..=$hi);',
						'SELECT VALUE v FROM type::record(s"rk_n", $lo..);',
						'SELECT VALUE v FROM type::record(s"rk_n", ..=$hi);',
						'SELECT VALUE v FROM type::record(s"rk_n", ..);',
						'SELECT VALUE v FROM type::record(s"rk_n", $lo>..$hi);',
						'SELECT VALUE v FROM type::record(s"rk_a", [$p, $q]..=[$p, $r]);',
						'SELECT VALUE v FROM type::record(s"rk_n", $hi..$lo);',
					].join('\n'),
					{ lo: 1, hi: 3, p: 'x', q: 1, r: 2 },
				),
			).toStrictEqual([
				[1, 2],
				[1, 2, 3],
				[1, 2, 3, 4],
				[1, 2, 3],
				[1, 2, 3, 4],
				[2],
				[1, 2],
				[],
			]);
			await expect(
				query('SELECT * FROM type::record(s"rk_n", $lo..$hi);', {
					lo: undefined,
					hi: 3,
				}),
			).rejects.toThrow(/is not a valid id/);
		});

		it('reads the records by key, never by a table scan', async () => {
			const operators = async (sql: string) => {
				const [plan] = await query(sql, { k: 2, lo: 1, hi: 3 });
				return [
					...JSON.stringify(plan).matchAll(/"operator":"([^"]+)"/gu),
				].map((match) => match[1]);
			};
			const id = await operators(
				'SELECT * FROM type::record(s"rk_n", $k) EXPLAIN;',
			);
			const range = await operators(
				'SELECT * FROM type::record(s"rk_n", $lo..=$hi) EXPLAIN;',
			);
			if (engine.majorVersion >= 4) {
				expect([id, range]).toStrictEqual([
					['Values', 'SelectProject'],
					['Values', 'SelectProject'],
				]);
			} else {
				expect([id, range]).toStrictEqual([
					['RecordIdScan', 'SelectProject'],
					['DynamicScan', 'SelectProject'],
				]);
			}
		});
	});

	describe('the index a lookup reads', () => {
		/** The indexes the plan of `sql` names. */
		const planned = async (sql: string): Promise<string[]> => {
			const [plan] = await query(sql, { a: 'x', b: 1 });
			return [
				...JSON.stringify(plan).matchAll(/"index":"([^"]+)"/gu),
			].map((match) => match[1] as string);
		};

		beforeAll(async () => {
			await query(`
				DEFINE TABLE pick SCHEMAFULL;
				DEFINE FIELD a ON pick TYPE string;
				DEFINE FIELD b ON pick TYPE int;
				DEFINE INDEX pick_a ON pick FIELDS a;
				DEFINE INDEX pick_ab ON pick FIELDS a, b UNIQUE;
				CREATE pick:1 SET a = 'x', b = 1;
				CREATE pick:2 SET a = 'x', b = 2;
			`);
		});

		it('with LIMIT 1, equality on every field of a composite unique index reads a single-field index whose name sorts first', async () => {
			expect(
				await planned(
					'SELECT * FROM ONLY pick WHERE a = $a AND b = $b LIMIT 1 EXPLAIN;',
				),
			).toStrictEqual(['pick_a']);
		});

		it('WITH INDEX makes the plan read the named index, and the lookup still finds its row', async () => {
			expect(
				await planned(
					'SELECT * FROM ONLY pick WITH INDEX pick_ab WHERE a = $a AND b = $b LIMIT 1 EXPLAIN;',
				),
			).toStrictEqual(['pick_ab']);
			expect(
				await query(
					'SELECT VALUE id FROM ONLY pick WITH INDEX pick_ab WHERE a = $a AND b = $b LIMIT 1;',
					{ a: 'x', b: 2 },
				),
			).toMatchObject([{ id: 2 }]);
			expect(
				await query(
					'SELECT * FROM ONLY pick WITH INDEX pick_ab WHERE a = $a AND b = $b LIMIT 1;',
					{ a: 'x', b: 3 },
				),
			).toStrictEqual([undefined]);
		});
	});

	describe('RETURN modes on writes', () => {
		beforeAll(async () => {
			await query(
				'CREATE ret:a SET n = 1; CREATE ret:b SET n = 2; CREATE ret:c SET n = 3; CREATE ret:d SET n = 4; CREATE ret:e SET n = 5;',
			);
		});

		it('DELETE yields an empty list by default, and NONE with ONLY', async () => {
			expect(
				await query('DELETE ret:a; DELETE ONLY ret:never;'),
			).toStrictEqual([[], undefined]);
		});

		it('DELETE … RETURN AFTER yields one NONE per deleted record', async () => {
			expect(await query('DELETE ret:b RETURN AFTER;')).toStrictEqual([
				[undefined],
			]);
		});

		it('DELETE … RETURN BEFORE yields the deleted records', async () => {
			expect(
				await query('DELETE ret:c RETURN VALUE $before.n;'),
			).toStrictEqual([[3]]);
			const [rows] = await query('DELETE ret:d RETURN BEFORE;');
			expect(rows).toMatchObject([{ n: 4 }]);
		});

		it('RELATE … RETURN BEFORE yields NONE for a new edge', async () => {
			expect(
				await query(
					'CREATE node:a; CREATE node:b; RELATE node:a->linked:x->node:b RETURN BEFORE;',
				),
			).toMatchObject([
				expect.anything(),
				expect.anything(),
				[undefined],
			]);
		});

		it('UPDATE … RETURN DIFF yields a JSON Patch per record', async () => {
			expect(
				await query('UPDATE ret:e SET n = 6, extra = "x" RETURN DIFF;'),
			).toStrictEqual([
				[
					[
						{ op: 'add', path: '/extra', value: 'x' },
						{ op: 'replace', path: '/n', value: 6 },
					],
				],
			]);
		});

		it('UPDATE … RETURN BEFORE yields the records as they were', async () => {
			const [rows] = await query('UPDATE ret:e SET n = 7 RETURN BEFORE;');
			expect(rows).toMatchObject([{ n: 6, extra: 'x' }]);
		});
	});

	describe('RETURN modes of every write, UPSERT and INSERT', () => {
		beforeAll(async () => {
			await query(`
				DEFINE TABLE rw SCHEMAFULL;
				DEFINE FIELD name ON rw TYPE string;
				DEFINE FIELD n ON rw TYPE int DEFAULT 0;
				DEFINE FIELD opt ON rw TYPE option<string>;
				DEFINE INDEX rw_name ON rw FIELDS name UNIQUE;
				DEFINE TABLE rw_edge TYPE RELATION SCHEMAFULL;
				DEFINE FIELD k ON rw_edge TYPE int DEFAULT 1;
				CREATE rw:a CONTENT { name: 'a', n: 1, opt: 'o' };
				CREATE rw:b CONTENT { name: 'b', n: 1 };
				CREATE rw:c CONTENT { name: 'c', n: 1 };
				CREATE rw:d CONTENT { name: 'd', n: 1 };
			`);
		});

		it('CREATE: BEFORE is NONE, DIFF replaces the whole record, VALUE is one expression', async () => {
			const [before, diff, value] = (await query(
				[
					"CREATE ONLY rw:c1 CONTENT { name: 'c1' } RETURN BEFORE;",
					"CREATE ONLY rw:c2 CONTENT { name: 'c2' } RETURN DIFF;",
					"CREATE ONLY rw:c3 CONTENT { name: 'c3' } RETURN VALUE n;",
				].join('\n'),
			)) as [unknown, Array<Record<string, unknown>>, unknown];
			expect(before).toBeUndefined();
			expect(diff).toHaveLength(1);
			expect(diff[0]).toMatchObject({
				op: 'replace',
				path: '',
				value: { name: 'c2', n: 0 },
			});
			expect(value).toBe(0);
		});

		it('DIFF writes add, remove, replace and change (a text patch of a string), and nothing for no change', async () => {
			expect(
				await query(
					[
						'UPDATE ONLY rw:a MERGE { opt: NONE } RETURN DIFF;',
						'UPDATE ONLY rw:a SET n = 2 RETURN DIFF;',
						"UPDATE ONLY rw:a SET name = 'a longer name' RETURN DIFF;",
						"UPDATE ONLY rw:b SET opt = 'x' RETURN DIFF;",
						'UPDATE ONLY rw:b SET n = n RETURN DIFF;',
						'UPDATE ONLY rw:missing SET n = 1 RETURN DIFF;',
					].join('\n'),
				),
			).toStrictEqual([
				[{ op: 'remove', path: '/opt' }],
				[{ op: 'replace', path: '/n', value: 2 }],
				[
					{
						op: 'change',
						path: '/name',
						value: '@@ -1,1 +1,13 @@\n a\n+ longer name\n',
					},
				],
				[{ op: 'add', path: '/opt', value: 'x' }],
				[],
				undefined,
			]);
		});

		it('DELETE: AFTER is NONE per record, DIFF replaces the record with NONE, and a projection reads $before', async () => {
			await query(
				"CREATE rw:x1 CONTENT { name: 'x1', n: 5 }; CREATE rw:x2 CONTENT { name: 'x2', n: 6 }; CREATE rw:x3 CONTENT { name: 'x3', n: 7 }; CREATE rw:x4 CONTENT { name: 'x4', n: 8 };",
			);
			expect(
				await query(
					[
						'DELETE rw:x1 RETURN AFTER;',
						'DELETE ONLY rw:x2 RETURN DIFF;',
						'DELETE ONLY rw:x3 RETURN n, $before.n AS was;',
						'DELETE ONLY rw:x4 RETURN VALUE $before.name;',
						'DELETE ONLY rw:missing RETURN DIFF;',
					].join('\n'),
				),
			).toStrictEqual([
				[undefined],
				[{ op: 'replace', path: '', value: undefined }],
				{ n: undefined, was: 7 },
				'x4',
				undefined,
			]);
		});

		it('UPSERT of a record creates it or changes it; BEFORE is NONE for one it created', async () => {
			const [created, changed, beforeNew, beforeOld] = (await query(
				[
					"UPSERT ONLY rw:u1 MERGE { name: 'u1' } RETURN AFTER;",
					'UPSERT ONLY rw:u1 MERGE { n: 3 } RETURN AFTER;',
					"UPSERT ONLY rw:u2 MERGE { name: 'u2' } RETURN BEFORE;",
					'UPSERT ONLY rw:u2 MERGE { n: 4 } RETURN BEFORE;',
				].join('\n'),
			)) as Array<Record<string, unknown> | undefined>;
			expect(created).toMatchObject({ name: 'u1', n: 0 });
			expect(changed).toMatchObject({ name: 'u1', n: 3 });
			expect(beforeNew).toBeUndefined();
			expect(beforeOld).toMatchObject({ name: 'u2', n: 0 });
			// By condition: every match changes, or one record is created.
			expect(
				await query(
					[
						"UPSERT rw MERGE { n: 9 } WHERE name = 'c' RETURN VALUE [name, n];",
						"UPSERT rw MERGE { name: 'fresh', n: 1 } WHERE name = 'fresh' RETURN VALUE [name, n];",
					].join('\n'),
				),
			).toStrictEqual([[['c', 9]], [['fresh', 1]]]);
		});

		it('UPSERT of an existing record with RETURN NONE checks a partial MERGE as if the record were new on SurrealDB 3, not on 4; RETURN VALUE NONE does not on either', async () => {
			const none = query('UPSERT ONLY rw:d MERGE { n: 2 } RETURN NONE;');
			if (engine.majorVersion >= 4) {
				expect(await none).toStrictEqual([undefined]);
			} else {
				await expect(none).rejects.toThrow(
					/Couldn't coerce value for field `name` of `rw:d`/,
				);
			}
			expect(
				await query(
					'UPSERT ONLY rw:d MERGE { n: 3 } RETURN VALUE NONE; SELECT VALUE [name, n] FROM ONLY rw:d;',
				),
			).toStrictEqual([undefined, ['d', 3]]);
		});

		it('INSERT returns a list for one row or many, takes no ONLY, and fails on a taken unique value', async () => {
			expect(
				await query(
					[
						'INSERT INTO rw $row RETURN VALUE name;',
						'INSERT INTO rw $rows RETURN VALUE name;',
						'INSERT INTO rw $none RETURN VALUE name;',
					].join('\n'),
					{
						row: { name: 'i1' },
						rows: [{ name: 'i2' }, { name: 'i3' }],
						none: [],
					},
				),
			).toStrictEqual([['i1'], ['i2', 'i3'], []]);
			await expect(
				query('INSERT ONLY INTO rw $row;', { row: { name: 'i4' } }),
			).rejects.toThrow(/Parse error/);
			await expect(
				query('INSERT INTO rw $row;', { row: { name: 'i1' } }),
			).rejects.toThrow(/Database index `rw_name` already contains 'i1'/);
		});

		it('ON DUPLICATE KEY UPDATE reads the attempted row as $input; BEFORE is the stored record, or NONE for a new one', async () => {
			const [rows] = (await query(
				'INSERT INTO rw $rows ON DUPLICATE KEY UPDATE n = $input.n RETURN BEFORE;',
				{
					rows: [
						{ name: 'i1', n: 10 },
						{ name: 'i9', n: 1 },
					],
				},
			)) as [Array<Record<string, unknown> | undefined>];
			expect(rows).toHaveLength(2);
			expect(rows[0]).toMatchObject({ name: 'i1', n: 0 });
			expect(rows[1]).toBeUndefined();
			expect(
				await query(
					"SELECT VALUE n FROM ONLY rw WHERE name = 'i1' LIMIT 1;",
				),
			).toStrictEqual([10]);
		});

		it('ON DUPLICATE KEY UPDATE reads the stored record by field name on SurrealDB 4 and as NONE on 3; `+=` works on both', async () => {
			const bare = query(
				'INSERT INTO rw $row ON DUPLICATE KEY UPDATE n = $input.n + n RETURN VALUE n;',
				{ row: { name: 'i1', n: 1 } },
			);
			if (engine.majorVersion >= 4) {
				expect(await bare).toStrictEqual([[11]]);
			} else {
				await expect(bare).rejects.toThrow(
					/Cannot perform addition with 'int' and 'none'/,
				);
			}
			const before = (
				(await query(
					"SELECT VALUE n FROM ONLY rw WHERE name = 'i1' LIMIT 1;",
				)) as [number]
			)[0];
			expect(
				await query(
					'INSERT INTO rw $row ON DUPLICATE KEY UPDATE n += 1 RETURN VALUE n;',
					{ row: { name: 'i1', n: 1 } },
				),
			).toStrictEqual([[before + 1]]);
		});

		it('INSERT RELATION takes each edge with its endpoints, and refuses one without', async () => {
			expect(
				await query(
					'INSERT RELATION INTO rw_edge $edges RETURN VALUE [<string> in, k];',
					{
						edges: [
							{
								in: new RecordId('rw', 'a'),
								out: new RecordId('rw', 'b'),
								k: 5,
							},
							{
								in: new RecordId('rw', 'b'),
								out: new RecordId('rw', 'a'),
							},
						],
					},
				),
			).toStrictEqual([
				[
					['rw:a', 5],
					['rw:b', 1],
				],
			]);
			await expect(
				query('INSERT RELATION INTO rw_edge $edge;', {
					edge: { out: new RecordId('rw', 'b') },
				}),
			).rejects.toThrow(/property 'in' is: NONE/);
		});
	});

	describe('permissions, as a record user', () => {
		let user: SurrealSession;
		const asUser = (sql: string) => query(sql, undefined, user);

		beforeAll(async () => {
			await query(`
				DEFINE TABLE account SCHEMAFULL
					PERMISSIONS FOR select, update WHERE id = $auth.id FOR create, delete NONE;
				DEFINE FIELD name ON account TYPE string;
				DEFINE FIELD pass ON account TYPE string;
				DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNUP (CREATE account SET name = $name, pass = crypto::argon2::generate($pass))
					SIGNIN (SELECT * FROM account WHERE name = $name AND crypto::argon2::compare(pass, $pass));
				DEFINE TABLE secret SCHEMAFULL PERMISSIONS NONE;
				DEFINE FIELD v ON secret TYPE int;
				CREATE secret:one SET v = 1;
				DEFINE TABLE visible SCHEMAFULL
					PERMISSIONS FOR select FULL FOR create, update, delete NONE;
				DEFINE FIELD shown ON visible TYPE string;
				DEFINE FIELD hidden ON visible TYPE string PERMISSIONS FOR select NONE;
				CREATE visible:one SET shown = 's', hidden = 'h';
				DEFINE TABLE nested SCHEMAFULL
					PERMISSIONS FOR select FULL FOR create, update, delete NONE;
				DEFINE FIELD card ON nested TYPE object;
				DEFINE FIELD card.name ON nested TYPE string;
				DEFINE FIELD card.phone ON nested TYPE string
					PERMISSIONS FOR select WHERE $auth.id = id;
				DEFINE FIELD owned ON nested TYPE string
					PERMISSIONS FOR select WHERE $auth.id = id;
				CREATE nested:one SET card = { name: 'n', phone: 'p' }, owned = 'o';
				DEFINE FUNCTION fn::denied() { RETURN 1; } PERMISSIONS NONE;
				DEFINE FUNCTION fn::allowed() { RETURN 1; } PERMISSIONS FULL;
			`);
			user = await within(db.newSession());
			const scope = {
				namespace: engine.namespace,
				database: engine.namespace,
			};
			await within(user.use(scope));
			await within(
				user.signup({
					...scope,
					access: 'user',
					variables: { name: 'ann', pass: 'correct horse' },
				}),
			);
		});
		afterAll(async () => {
			await user?.closeSession();
		});

		it('the session is the record user', async () => {
			expect(
				await asUser('SELECT VALUE name FROM account;'),
			).toStrictEqual([['ann']]);
		});

		it('a table whose SELECT is denied reads as empty, without an error', async () => {
			expect(
				await asUser(
					'SELECT * FROM secret; SELECT * FROM ONLY secret:one;',
				),
			).toStrictEqual([[], undefined]);
		});

		it('a denied CREATE returns an empty list, or NONE with ONLY, without an error, and stores nothing', async () => {
			expect(
				await asUser(
					"CREATE visible:two SET shown = 'x', hidden = 'y'; CREATE ONLY visible:three SET shown = 'x', hidden = 'y';",
				),
			).toStrictEqual([[], undefined]);
			expect(
				await query('SELECT VALUE shown FROM visible;'),
			).toStrictEqual([['s']]);
		});

		it('a denied UPDATE or DELETE returns an empty list, without an error, and changes nothing', async () => {
			expect(
				await asUser(
					"UPDATE visible:one SET shown = 'changed'; DELETE visible:one RETURN BEFORE;",
				),
			).toStrictEqual([[], []]);
			expect(
				await query('SELECT VALUE shown FROM visible;'),
			).toStrictEqual([['s']]);
		});

		it('a field whose SELECT is denied is absent from `*`, and NONE when projected by name', async () => {
			const [star] = (await asUser('SELECT * OMIT id FROM visible;')) as [
				Array<Record<string, unknown>>,
			];
			expect(star).toStrictEqual([{ shown: 's' }]);
			const [named] = (await asUser(
				'SELECT shown, hidden FROM visible;',
			)) as [Array<Record<string, unknown>>];
			expect(named.map((row) => Object.keys(row).sort())).toStrictEqual([
				['hidden', 'shown'],
			]);
			expect(named[0]?.hidden).toBeUndefined();
			// A system user reads the field.
			expect(
				await query('SELECT VALUE hidden FROM visible;'),
			).toStrictEqual([['h']]);
		});

		it('a nested field whose SELECT is denied is absent from its object; a WHERE permission is checked per record', async () => {
			// `$auth.id = id` is false for this user on `nested:one`.
			expect(await asUser('SELECT * OMIT id FROM nested;')).toStrictEqual(
				[[{ card: { name: 'n' } }]],
			);
			expect(
				await asUser('SELECT VALUE card FROM ONLY nested:one;'),
			).toStrictEqual([{ name: 'n' }]);
			// A system user is not a record user: the permission does not apply.
			expect(await query('SELECT * OMIT id FROM nested;')).toStrictEqual([
				[{ card: { name: 'n', phone: 'p' }, owned: 'o' }],
			]);
		});

		it('calling a function the session may not run is an error', async () => {
			await expect(asUser('RETURN fn::denied();')).rejects.toThrow(
				/You don't have permission to run the fn::denied function/,
			);
			expect(await asUser('RETURN fn::allowed();')).toStrictEqual([1]);
		});
	});

	describe('a projected field that is NONE', () => {
		it('arrives as a present key holding undefined, where `*` leaves the key out', async () => {
			const [, rows, only, star] = (await query(
				'CREATE proj:one SET a = 1; SELECT a, b FROM proj; SELECT a, b FROM ONLY proj:one; SELECT * OMIT id FROM proj;',
			)) as [
				unknown,
				Array<Record<string, unknown>>,
				Record<string, unknown>,
				Array<Record<string, unknown>>,
			];
			expect(rows.map((row) => Object.keys(row).sort())).toStrictEqual([
				['a', 'b'],
			]);
			expect(rows[0]).toStrictEqual({ a: 1, b: undefined });
			expect(Object.keys(only).sort()).toStrictEqual(['a', 'b']);
			expect(star.map((row) => Object.keys(row))).toStrictEqual([['a']]);
		});
	});
});
