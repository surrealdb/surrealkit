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
	DateTime,
	Decimal,
	Duration,
	Features,
	RecordId,
	type Surreal,
	type SurrealSession,
	Table,
	UnsupportedFeatureError,
} from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
	conflictingIndex,
	isRecordExists,
} from '../src/client/internal/classify.js';
import * as S from '../src/index.js';
import { idRange, param, select } from '../src/statement/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	interactiveTransactions,
	startEngine,
	within,
} from './engine.js';
import { IdIntTable } from './golden/corpus/index.js';
import { openRecordSession } from './record-session.js';

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

	describe('paging: an id range after the last id read, not START', () => {
		// The statements the README's "Paging through a table" builds.
		const rows = param('rows', S.Int);
		const firstPage = select(IdIntTable, idRange(IdIntTable, {}))
			.limit(rows)
			.build();
		const nextPage = select(
			IdIntTable,
			idRange(IdIntTable, { after: param('last', S.Int) }),
		)
			.limit(rows)
			.build();
		const offsetPage = select(IdIntTable)
			.orderBy((row) => row.id)
			.limit(rows)
			.start(param('skip', S.Int))
			.build();
		const explained = (sql: string) =>
			`${sql.replace(/;\s*$/u, '')} EXPLAIN;`;
		const operators = async (
			sql: string,
			bindings: Record<string, unknown>,
		) => {
			const [plan] = await query(sql, bindings);
			return [
				...JSON.stringify(plan).matchAll(/"operator":"([^"]+)"/gu),
			].map((match) => match[1]);
		};
		const ids = async (sql: string, bindings: Record<string, unknown>) => {
			const [found] = (await query(sql, bindings)) as [
				{ id: RecordId }[],
			];
			return found.map((row) => row.id.id);
		};

		beforeAll(async () => {
			// Created out of key order, so key order is not insertion order.
			await query(`
				DEFINE TABLE id_int SCHEMAFULL;
				FOR $k IN [5, 1, 9, 3, 7, 2, 8, 4, 6, 10] { CREATE type::record("id_int", $k); };
			`);
		});

		it('the builders render the range pages and the START page', () => {
			expect([firstPage.sql, nextPage.sql, offsetPage.sql]).toStrictEqual(
				[
					'SELECT * FROM type::record(s"id_int", ..) LIMIT $rows;',
					'SELECT * FROM type::record(s"id_int", $last>..) LIMIT $rows;',
					'SELECT * FROM ⟨id_int⟩ ORDER BY id ASC LIMIT $rows START $skip;',
				],
			);
		});

		it('START plans as a table scan that reads the skipped rows; a range reads by key', async () => {
			const bindings = { rows: 3, skip: 3 };
			const [offset] = await query(explained(offsetPage.sql), bindings);
			expect(
				await operators(explained(offsetPage.sql), bindings),
			).toStrictEqual(['TableScan', 'SelectProject']);
			// The scan walks the table in key order and passes over `offset`
			// rows before it returns `limit`.
			expect(JSON.stringify(offset)).toMatch(
				/\{"attributes":\{"direction":"Forward","limit":"\$rows","offset":"\$skip","table":"id_int"[^}]*\},"context":"Db","operator":"TableScan"\}/u,
			);
			const plans = [
				await operators(explained(firstPage.sql), { rows: 3 }),
				await operators(explained(nextPage.sql), { rows: 3, last: 3 }),
				await operators(
					'SELECT * FROM id_int:3.. LIMIT 3 EXPLAIN;',
					{},
				),
			];
			if (engine.majorVersion >= 4) {
				expect(plans).toStrictEqual([
					['Values', 'Limit', 'SelectProject'],
					['Values', 'Limit', 'SelectProject'],
					['RecordIdScan', 'Limit', 'SelectProject'],
				]);
			} else {
				expect(plans).toStrictEqual([
					['DynamicScan', 'SelectProject'],
					['DynamicScan', 'SelectProject'],
					['RecordIdScan', 'Limit', 'Project'],
				]);
			}
		});

		it('a range returns its rows in key order with no ORDER BY, so walking it visits every row once', async () => {
			const seen: unknown[] = [];
			let page = await ids(firstPage.sql, { rows: 3 });
			while (page.length > 0) {
				seen.push(...page);
				page = await ids(nextPage.sql, { rows: 3, last: page.at(-1) });
			}
			expect(seen).toStrictEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
		});

		it('ORDER BY id on an open range sorts the whole range before LIMIT on SurrealDB 4', async () => {
			const plan = await operators(
				explained(
					'SELECT * FROM type::record(s"id_int", $last>..) ORDER BY id LIMIT $rows;',
				),
				{ rows: 3, last: 3 },
			);
			expect(plan).toStrictEqual(
				engine.majorVersion >= 4
					? ['Values', 'Sort', 'Limit', 'SelectProject']
					: ['DynamicScan', 'SortByKey', 'SelectProject'],
			);
		});

		it('after a row already read is deleted, START skips a row and the range does not', async () => {
			await query(`
				DEFINE TABLE pg_skip SCHEMAFULL;
				FOR $k IN [1, 2, 3, 4, 5] { CREATE type::record("pg_skip", $k); };
			`);
			const table = (sql: string) => sql.replaceAll('id_int', 'pg_skip');
			expect(
				await ids(table(offsetPage.sql), { rows: 3, skip: 0 }),
			).toStrictEqual([1, 2, 3]);
			await query('DELETE pg_skip:2;');
			expect([
				await ids(table(offsetPage.sql), { rows: 3, skip: 3 }),
				await ids(table(nextPage.sql), { rows: 3, last: 3 }),
			]).toStrictEqual([[5], [4, 5]]);
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

		it('an array is found through a unique index on SurrealDB 3.3 only when the field is declared an array kind; 4.0 finds it on any field', async () => {
			await query(`
				DEFINE TABLE uq_free SCHEMALESS;
				DEFINE INDEX uq_free_f ON uq_free FIELDS f UNIQUE;
				CREATE uq_free:1 CONTENT { f: [1, 2] };
				DEFINE TABLE uq_any SCHEMALESS;
				DEFINE FIELD f ON uq_any TYPE any;
				DEFINE INDEX uq_any_f ON uq_any FIELDS f UNIQUE;
				CREATE uq_any:1 CONTENT { f: [1, 2] };
				CREATE uq_any:2 CONTENT { f: 'x' };
				DEFINE TABLE uq_union SCHEMAFULL;
				DEFINE FIELD f ON uq_union TYPE array<int> | string;
				DEFINE INDEX uq_union_f ON uq_union FIELDS f UNIQUE;
				CREATE uq_union:1 CONTENT { f: [1, 2] };
				CREATE uq_union:2 CONTENT { f: 'x' };
				DEFINE TABLE uq_set SCHEMAFULL;
				DEFINE FIELD f ON uq_set TYPE array<int> | set<string>;
				DEFINE INDEX uq_set_f ON uq_set FIELDS f UNIQUE;
				CREATE uq_set:1 CONTENT { f: [1, 2] };
				DEFINE TABLE uq_array SCHEMALESS;
				DEFINE FIELD f ON uq_array TYPE option<array<int>>;
				DEFINE INDEX uq_array_f ON uq_array FIELDS f UNIQUE;
				CREATE uq_array:1 CONTENT { f: [1, 2] };
				DEFINE TABLE uq_arrays SCHEMAFULL;
				DEFINE FIELD f ON uq_arrays TYPE array<int> | array<string> | null;
				DEFINE INDEX uq_arrays_f ON uq_arrays FIELDS f UNIQUE;
				CREATE uq_arrays:1 CONTENT { f: [1, 2] };
			`);
			const lookup = (table: string, value: unknown) =>
				query(
					`SELECT VALUE id FROM ONLY ${table} WITH INDEX ${table}_f WHERE f = $v LIMIT 1;`,
					{ v: value },
				);
			const mixed = [
				await lookup('uq_free', [1, 2]),
				await lookup('uq_any', [1, 2]),
				await lookup('uq_union', [1, 2]),
				await lookup('uq_set', [1, 2]),
			];
			if (engine.majorVersion >= 4) {
				expect(mixed).toStrictEqual([
					[new RecordId('uq_free', 1)],
					[new RecordId('uq_any', 1)],
					[new RecordId('uq_union', 1)],
					[new RecordId('uq_set', 1)],
				]);
			} else {
				expect(mixed).toStrictEqual([
					[undefined],
					[undefined],
					[undefined],
					[undefined],
				]);
				// A scan of the table reads the index too, and misses as well.
				expect(
					await query(
						'SELECT VALUE id FROM uq_free WHERE f = [1, 2];',
					),
				).toStrictEqual([[]]);
			}
			// The same on both: an array-only field, and a value that is not an array.
			expect([
				await lookup('uq_array', [1, 2]),
				await lookup('uq_arrays', [1, 2]),
				await lookup('uq_any', 'x'),
				await lookup('uq_union', 'x'),
			]).toStrictEqual([
				[new RecordId('uq_array', 1)],
				[new RecordId('uq_arrays', 1)],
				[new RecordId('uq_any', 2)],
				[new RecordId('uq_union', 2)],
			]);
			// Both index an array by its elements for uniqueness.
			await expect(
				query('CREATE uq_free:2 CONTENT { f: [2, 3] };'),
			).rejects.toThrow(/Database index `uq_free_f` already contains 2/);
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

		it('UPSERT of a table by a condition that matches, with RETURN NONE, also creates a record on SurrealDB 3, not on 4; RETURN VALUE NONE does not on either, and answers one NONE per record', async () => {
			await query(
				[
					'DEFINE TABLE rwu SCHEMAFULL;',
					'DEFINE FIELD name ON rwu TYPE string;',
					'DEFINE FIELD n ON rwu TYPE int;',
					"CREATE rwu:a CONTENT { name: 'm', n: 1 };",
					"CREATE rwu:b CONTENT { name: 'm', n: 1 };",
				].join('\n'),
			);
			const matching = "SELECT VALUE n FROM rwu WHERE name = 'm';";
			expect(
				await query(
					`UPSERT rwu MERGE { name: 'm', n: 2 } WHERE name = 'm' RETURN VALUE NONE; ${matching}`,
				),
			).toStrictEqual([
				[undefined, undefined],
				[2, 2],
			]);
			// No record matches: one is created, and answered as one NONE.
			expect(
				await query(
					"UPSERT rwu MERGE { name: 'fresh', n: 1 } WHERE name = 'fresh' RETURN VALUE NONE; SELECT VALUE n FROM rwu WHERE name = 'fresh';",
				),
			).toStrictEqual([[undefined], [1]]);
			const none = await query(
				`UPSERT rwu MERGE { name: 'm', n: 3 } WHERE name = 'm' RETURN NONE; ${matching}`,
			);
			expect(none).toStrictEqual(
				engine.majorVersion >= 4
					? [[], [3, 3]]
					: // The two records changed, and a third was created.
						[[], [3, 3, 3]],
			);
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

	// What the client's constraint errors read (`src/client/internal/classify.ts`).
	// A record that exists is structured. A duplicate on a unique index is
	// `Internal` with no details, and the index is named only in the message,
	// which the SurrealDB protocol documentation says may change between
	// releases: these rows are what lets the client read it (a message pattern
	// classifies an engine error only when a test here pins it on both engine
	// lanes), so a release that changes it fails here.
	describe('constraint errors: a duplicate on a unique index and a record that exists', () => {
		interface Shape {
			readonly kind: unknown;
			readonly details: unknown;
			readonly message: string;
		}
		const shapeOf = (error: unknown): Shape => {
			if (!(error instanceof Error)) throw error;
			const { kind, details } = error as Error & {
				readonly kind?: unknown;
				readonly details?: unknown;
			};
			return { kind, details, message: error.message };
		};
		/** The error of a request that must fail, as the engine reports it. */
		const failureOf = async (
			sql: string,
			bindings?: Record<string, unknown>,
		): Promise<Shape> => {
			try {
				await query(sql, bindings);
			} catch (error) {
				return shapeOf(error);
			}
			throw new Error(`expected \`${sql}\` to fail`);
		};
		const declared = ['uq_email', 'uq_name', 'uq-nick', 'uq_edge_pair'];
		/** The engine's message for a duplicate value of `index`. */
		const duplicate = (index: string, value: string, record: string) => ({
			kind: 'Internal',
			details: undefined,
			message: `Database index \`${index}\` already contains ${value}, with record \`${record}\``,
		});
		const exists = (record: string) => ({
			kind: 'AlreadyExists',
			details: { kind: 'Record', details: { id: record } },
			message: `Database record \`${record}\` already exists`,
		});

		beforeAll(async () => {
			await query(`
				DEFINE TABLE uq SCHEMAFULL;
				DEFINE FIELD email ON uq TYPE string;
				DEFINE FIELD first ON uq TYPE option<string>;
				DEFINE FIELD last ON uq TYPE option<string>;
				DEFINE FIELD nick ON uq TYPE option<string>;
				DEFINE INDEX uq_email ON uq FIELDS email UNIQUE;
				DEFINE INDEX uq_name ON uq FIELDS first, last UNIQUE;
				DEFINE INDEX \`uq-nick\` ON uq FIELDS nick UNIQUE;
				DEFINE TABLE uq_edge TYPE RELATION IN uq OUT uq SCHEMAFULL;
				DEFINE INDEX uq_edge_pair ON uq_edge FIELDS in, out UNIQUE;
				DEFINE TABLE uq_audit SCHEMAFULL;
				DEFINE FIELD email ON uq_audit TYPE string;
				DEFINE INDEX uq_email ON uq_audit FIELDS email UNIQUE;
				DEFINE TABLE uq_member SCHEMAFULL;
				DEFINE FIELD email ON uq_member TYPE string;
				DEFINE EVENT copy ON uq_member WHEN $event = 'CREATE' THEN { CREATE uq_audit SET email = $after.email };
				DEFINE FUNCTION fn::uq_audit($email: string) { CREATE uq_audit SET email = $email };
				CREATE uq:1 SET email = 'a@x.io', first = 'Ada', last = 'L', nick = 'ada';
				CREATE uq:2 SET email = 'b@x.io';
				RELATE uq:1->uq_edge:1->uq:2;
				CREATE uq_audit:1 SET email = 'k@x.io';
			`);
		});

		it('a duplicate through CREATE, INSERT, UPDATE and UPSERT is Internal, with the index only in the message', async () => {
			const taken = duplicate('uq_email', "'a@x.io'", 'uq:1');
			for (const sql of [
				"CREATE uq:10 SET email = 'a@x.io';",
				"CREATE uq SET email = 'a@x.io';",
				'CREATE uq:11 SET email = $email;',
				"INSERT INTO uq { id: uq:12, email: 'a@x.io' };",
				"INSERT INTO uq [{ id: uq:13, email: 'c@x.io' }, { id: uq:14, email: 'a@x.io' }];",
				"UPDATE uq:2 SET email = 'a@x.io';",
				"UPSERT uq:15 SET email = 'a@x.io';",
				"UPSERT uq:2 SET email = 'a@x.io';",
			]) {
				const shape = await failureOf(sql, { email: 'a@x.io' });
				expect(shape, sql).toStrictEqual(taken);
				expect(conflictingIndex(shape, declared), sql).toBe('uq_email');
			}
			// Nothing was written by any of them.
			expect(
				await query("SELECT VALUE id FROM uq WHERE email = 'c@x.io';"),
			).toStrictEqual([[]]);
		});

		it('a duplicate through RELATE and INSERT RELATION names the index of the relation', async () => {
			const taken = duplicate(
				'uq_edge_pair',
				'[uq:1, uq:2]',
				'uq_edge:1',
			);
			for (const sql of [
				'RELATE uq:1->uq_edge->uq:2;',
				'INSERT RELATION INTO uq_edge { in: uq:1, out: uq:2 };',
			]) {
				const shape = await failureOf(sql);
				expect(shape, sql).toStrictEqual(taken);
				expect(conflictingIndex(shape, declared), sql).toBe(
					'uq_edge_pair',
				);
			}
		});

		it('a composite index lists its values, and a name that needs quoting is written in backticks as it is', async () => {
			const composite = await failureOf(
				"CREATE uq:16 SET email = 'd@x.io', first = 'Ada', last = 'L';",
			);
			expect(composite).toStrictEqual(
				duplicate('uq_name', "['Ada', 'L']", 'uq:1'),
			);
			expect(conflictingIndex(composite, declared)).toBe('uq_name');
			const quoted = await failureOf(
				"CREATE uq:17 SET email = 'e@x.io', nick = 'ada';",
			);
			expect(quoted).toStrictEqual(duplicate('uq-nick', "'ada'", 'uq:1'));
			expect(conflictingIndex(quoted, declared)).toBe('uq-nick');
		});

		it('only an index that is declared is read from the message', async () => {
			const shape = await failureOf("CREATE uq:18 SET email = 'a@x.io';");
			expect(conflictingIndex(shape, [])).toBeUndefined();
			expect(conflictingIndex(shape, ['uq'])).toBeUndefined();
			expect(conflictingIndex(shape, ['uq_email_2'])).toBeUndefined();
			expect(conflictingIndex(shape, ['email'])).toBeUndefined();
		});

		it('a value that imitates the message is stored, and a THROW of it is Thrown, not Internal', async () => {
			const imitation =
				"x', with record `uq:1`; Database index `uq_name` already contains 'y";
			expect(
				await query(
					'CREATE ONLY uq:19 SET email = $email RETURN VALUE email;',
					{
						email: imitation,
					},
				),
			).toStrictEqual([imitation]);
			const thrown = await failureOf(
				"THROW 'Database index `uq_email` already contains 1, with record `uq:1`';",
			);
			expect(thrown.kind).toBe('Thrown');
			expect(conflictingIndex(thrown, declared)).toBeUndefined();
		});

		it('a duplicate raised inside an event is prefixed with the event, so it is not read; inside a function it reads as the caller’s own', async () => {
			const inEvent = await failureOf(
				"CREATE uq_member:1 SET email = 'k@x.io';",
			);
			expect(inEvent).toStrictEqual({
				...duplicate('uq_email', "'k@x.io'", 'uq_audit:1'),
				message: `Error while processing event copy: ${duplicate('uq_email', "'k@x.io'", 'uq_audit:1').message}`,
			});
			expect(conflictingIndex(inEvent, declared)).toBeUndefined();
			// The index of another table with the same name: the message names
			// the index and not its table, so the client reports the name only.
			const inFunction = await failureOf(
				"RETURN fn::uq_audit('k@x.io');",
			);
			expect(inFunction).toStrictEqual(
				duplicate('uq_email', "'k@x.io'", 'uq_audit:1'),
			);
			expect(conflictingIndex(inFunction, declared)).toBe('uq_email');
		});

		it('when two unique indexes are violated, the engine reports one of them', async () => {
			const shape = await failureOf(
				"CREATE uq:20 SET email = 'a@x.io', nick = 'ada';",
			);
			expect(shape).toStrictEqual(duplicate('uq-nick', "'ada'", 'uq:1'));
		});

		it('a record that exists, through CREATE, INSERT and INSERT RELATION, is AlreadyExists of kind Record', async () => {
			for (const [sql, record] of [
				["CREATE uq:1 SET email = 'z1@x.io';", 'uq:1'],
				["INSERT INTO uq { id: uq:1, email: 'z2@x.io' };", 'uq:1'],
				[
					"INSERT INTO uq [{ id: uq:21, email: 'z3@x.io' }, { id: uq:1, email: 'z4@x.io' }];",
					'uq:1',
				],
				[
					'INSERT RELATION INTO uq_edge { id: uq_edge:1, in: uq:2, out: uq:1 };',
					'uq_edge:1',
				],
			] as const) {
				const shape = await failureOf(sql);
				expect(shape, sql).toStrictEqual(exists(record));
				expect(isRecordExists(shape), sql).toBe(true);
				expect(conflictingIndex(shape, declared), sql).toBeUndefined();
			}
			expect(
				await query("SELECT VALUE id FROM uq WHERE email = 'z3@x.io';"),
			).toStrictEqual([[]]);
		});

		it('RELATE onto an existing edge id is no AlreadyExists: a different Internal message on SurrealDB 3 and 4', async () => {
			const shape = await failureOf('RELATE uq:1->uq_edge:1->uq:1;');
			expect(shape.kind).toBe('Internal');
			expect(shape.message).toBe(
				engine.majorVersion >= 4
					? 'Cannot change the `out` endpoint of an existing edge'
					: 'Found uq:2 for the `out` field, which does not match the existing field value',
			);
			expect(isRecordExists(shape)).toBe(false);
			expect(conflictingIndex(shape, declared)).toBeUndefined();
		});

		it('INSERT IGNORE raises neither: it skips a row whose id exists or whose unique value is taken', async () => {
			expect(
				await query(
					[
						"INSERT IGNORE INTO uq { id: uq:1, email: 'z5@x.io' };",
						"INSERT IGNORE INTO uq { id: uq:22, email: 'a@x.io' };",
						"INSERT IGNORE INTO uq { email: 'a@x.io' };",
						"INSERT IGNORE INTO uq [{ id: uq:23, email: 'n@x.io' }, { id: uq:24, email: 'a@x.io' }, { id: uq:1, email: 'm@x.io' }] RETURN VALUE <string> id;",
						'INSERT RELATION IGNORE INTO uq_edge { in: uq:1, out: uq:2 };',
					].join('\n'),
				),
			).toStrictEqual([[], [], [], ['uq:23'], []]);
		});

		it('ON DUPLICATE KEY UPDATE updates a row whose id exists or whose unique value is taken, but a taken value under another explicit id, or one its update sets, is a duplicate', async () => {
			expect(
				await query(
					[
						"INSERT INTO uq { id: uq:1, email: 'a@x.io' } ON DUPLICATE KEY UPDATE first = 'Ada' RETURN VALUE <string> id;",
						"INSERT INTO uq { email: 'a@x.io' } ON DUPLICATE KEY UPDATE last = 'L' RETURN VALUE <string> id;",
						'INSERT RELATION INTO uq_edge { in: uq:1, out: uq:2 } ON DUPLICATE KEY UPDATE in = uq:1 RETURN VALUE <string> id;',
					].join('\n'),
				),
			).toStrictEqual([['uq:1'], ['uq:1'], ['uq_edge:1']]);
			const taken = duplicate('uq_email', "'a@x.io'", 'uq:1');
			for (const sql of [
				"INSERT INTO uq { id: uq:25, email: 'a@x.io' } ON DUPLICATE KEY UPDATE nick = 'x';",
				"INSERT INTO uq { id: uq:2, email: 'b@x.io' } ON DUPLICATE KEY UPDATE email = 'a@x.io';",
			]) {
				const shape = await failureOf(sql);
				expect(shape, sql).toStrictEqual(taken);
				expect(conflictingIndex(shape, declared), sql).toBe('uq_email');
			}
		});

		it('UPSERT by a condition on the unique field changes the record that holds the value', async () => {
			expect(
				await query(
					"UPSERT uq SET email = 'a@x.io' WHERE email = 'a@x.io' RETURN VALUE <string> id;",
				),
			).toStrictEqual([['uq:1']]);
		});

		it('in a transaction the duplicate is its statement’s own result, and the others were not executed', async () => {
			const responses = await within(
				db
					.query(
						"BEGIN; CREATE uq:30 SET email = 'q@x.io'; CREATE uq:31 SET email = 'a@x.io'; COMMIT;",
					)
					.responses(),
			);
			const errors = responses.map((response) =>
				response.success ? undefined : shapeOf(response.error),
			);
			expect(errors[0]).toBeUndefined();
			expect(errors[1]).toMatchObject({
				kind: 'Query',
				details: { kind: 'NotExecuted' },
			});
			expect(errors[2]).toStrictEqual(
				duplicate('uq_email', "'a@x.io'", 'uq:1'),
			);
			expect(errors[3]).toMatchObject({
				kind: 'Query',
				details: { kind: 'NotExecuted' },
			});
		});
	});

	describe('GROUP BY, GROUP ALL and aggregates', () => {
		beforeAll(async () => {
			await query(`
				DEFINE TABLE gr SCHEMAFULL;
				DEFINE FIELD role ON gr TYPE string;
				DEFINE FIELD city ON gr TYPE object;
				DEFINE FIELD city.name ON gr TYPE string;
				DEFINE FIELD n ON gr TYPE int;
				DEFINE FIELD d ON gr TYPE decimal;
				DEFINE FIELD o ON gr TYPE option<int>;
				DEFINE FIELD tags ON gr TYPE array<string>;
				DEFINE FIELD at ON gr TYPE datetime;
				DEFINE FIELD name ON gr TYPE string;
				CREATE gr:1 CONTENT { role: 'a', city: { name: 'x' }, n: 1, d: 1.5dec, tags: ['t1', 't2'], at: d'2024-01-01T00:00:00Z', name: 'p1' };
				CREATE gr:2 CONTENT { role: 'a', city: { name: 'y' }, n: 2, d: 2.5dec, tags: ['t2'], at: d'2024-01-02T00:00:00Z', name: 'p2' };
				CREATE gr:3 CONTENT { role: 'b', city: { name: 'x' }, n: 4, d: 4dec, o: 3, tags: [], at: d'2024-01-03T00:00:00Z', name: 'p3' };
				DEFINE TABLE gr_empty SCHEMAFULL;
				DEFINE FIELD n ON gr_empty TYPE int;
				DEFINE FIELD d ON gr_empty TYPE decimal;
				DEFINE FIELD at ON gr_empty TYPE datetime;
			`);
		});

		it('a field neither grouped nor aggregated yields the array of the group values, and a key left out of the projection is refused', async () => {
			expect(
				await query(
					'SELECT role AS r, name, count() AS c FROM gr GROUP BY role ORDER BY r;',
				),
			).toStrictEqual([
				[
					{ r: 'a', name: ['p1', 'p2'], c: 2 },
					{ r: 'b', name: ['p3'], c: 1 },
				],
			]);
			await expect(
				query('SELECT count() AS c FROM gr GROUP BY role;'),
			).rejects.toThrow(
				/Missing group idiom `role` in statement selection/,
			);
		});

		it('groups by a nested path under an alias, orders by an alias, and limits groups', async () => {
			expect(
				await query(
					'SELECT city.name AS city, count() AS c FROM gr GROUP BY city.name ORDER BY c DESC LIMIT 1;',
				),
			).toStrictEqual([[{ city: 'x', c: 2 }]]);
		});

		it('each aggregate per group: count, math::sum keeps the kind, mean and median are floats, min and max keep the field', async () => {
			const [rows] = (await query(
				'SELECT role AS role, count() AS c, math::sum(n) AS sum, math::sum(d) AS dsum, math::mean(n) AS mean, math::mean(d) AS dmean, math::median(n) AS med, math::min(d) AS dmin, math::max(n) AS max, math::stddev(n) AS sd, math::variance(n) AS var, time::max(at) AS last, array::group(name) AS names FROM gr GROUP BY role ORDER BY role;',
			)) as [Array<Record<string, unknown>>];
			const [a] = rows;
			expect(a).toMatchObject({
				role: 'a',
				c: 2,
				sum: 3,
				mean: 1.5,
				dmean: 2,
				med: 1.5,
				max: 2,
				sd: Math.SQRT1_2,
				var: 0.5,
				names: ['p1', 'p2'],
			});
			expect(a?.dsum).toBeInstanceOf(Decimal);
			expect(String(a?.dsum)).toBe('4');
			expect(a?.dmin).toBeInstanceOf(Decimal);
			expect(a?.last).toBeInstanceOf(DateTime);
		});

		it('over a decimal field, stddev and variance are decimals but the int 0 for one record, median the middle decimal or a float of two, mean a float', async () => {
			const [rows, all] = (await query(
				[
					'SELECT role AS role, math::stddev(d) AS sd, math::variance(d) AS var, math::median(d) AS med, math::mean(d) AS mean FROM gr GROUP BY role ORDER BY role;',
					'SELECT math::stddev(d) AS sd, math::variance(d) AS var, math::median(d) AS med, math::mean(d) AS mean FROM gr GROUP ALL;',
				].join('\n'),
			)) as [
				Array<Record<string, unknown>>,
				Array<Record<string, unknown>>,
			];
			const [a, b] = rows;
			expect(a?.sd).toBeInstanceOf(Decimal);
			expect(String(a?.sd)).toBe('0.7071067811865475244008443621');
			expect(a?.var).toBeInstanceOf(Decimal);
			expect(String(a?.var)).toBe('0.5');
			expect(a?.med).toBe(2);
			expect(a?.mean).toBe(2);
			expect(b?.sd).toBe(0);
			expect(b?.var).toBe(0);
			expect(b?.med).toBeInstanceOf(Decimal);
			expect(String(b?.med)).toBe('4');
			expect(b?.mean).toBe(4);
			// Three records: the middle one, and a mean that is not whole.
			const [three] = all;
			expect(three?.sd).toBeInstanceOf(Decimal);
			expect(three?.var).toBeInstanceOf(Decimal);
			expect(three?.med).toBeInstanceOf(Decimal);
			expect(String(three?.med)).toBe('2.5');
			expect(three?.mean).toBeCloseTo(8 / 3, 12);
			expect(typeof three?.mean).toBe('number');
		});

		it('GROUP ALL is one row, even over no record, where min and max are ±Infinity, mean NaN, sum the int 0 and median and time::min NONE', async () => {
			expect(
				await query(
					'SELECT count() AS c, math::sum(n) AS sum FROM gr GROUP ALL; SELECT count() AS c FROM gr WHERE role = "z" GROUP ALL;',
				),
			).toStrictEqual([[{ c: 3, sum: 7 }], [{ c: 0 }]]);
			expect(
				await query(
					'SELECT count() AS c, math::sum(n) AS sum, math::sum(d) AS dsum, math::min(n) AS min, math::max(n) AS max, math::mean(n) AS mean, math::median(n) AS med, time::min(at) AS first FROM gr_empty GROUP ALL;',
				),
			).toStrictEqual([
				[
					{
						c: 0,
						sum: 0,
						dsum: 0,
						min: Number.POSITIVE_INFINITY,
						max: Number.NEGATIVE_INFINITY,
						mean: Number.NaN,
						med: undefined,
						first: undefined,
					},
				],
			]);
		});

		it('a group whose values are all NONE aggregates as no value', async () => {
			const [rows] = (await query(
				"SELECT role AS role, math::sum(o) AS sum, math::min(o) AS min, math::mean(o) AS mean FROM gr WHERE role = 'a' GROUP BY role;",
			)) as [Array<Record<string, unknown>>];
			expect(rows).toStrictEqual([
				{
					role: 'a',
					sum: 0,
					min: Number.POSITIVE_INFINITY,
					mean: Number.NaN,
				},
			]);
		});

		it('another math function is no aggregate: it yields one value per record', async () => {
			expect(
				await query(
					'SELECT role AS role, math::product(n) AS p FROM gr GROUP BY role ORDER BY role;',
				),
			).toStrictEqual([
				[
					{ role: 'a', p: [undefined, undefined] },
					{ role: 'b', p: [undefined] },
				],
			]);
		});

		it('array::group flattens the arrays of a group on SurrealDB 4 and not on 3; NONE once on 4, once per record on 3', async () => {
			const [arrays, nones] = (await query(
				[
					"SELECT role AS role, array::group(tags) AS g FROM gr WHERE role = 'a' GROUP BY role;",
					"SELECT role AS role, array::group(o) AS g FROM gr WHERE role = 'a' GROUP BY role;",
				].join('\n'),
			)) as [unknown, unknown];
			if (engine.majorVersion >= 4) {
				expect(arrays).toStrictEqual([{ role: 'a', g: ['t1', 't2'] }]);
				expect(nones).toStrictEqual([{ role: 'a', g: [undefined] }]);
			} else {
				expect(arrays).toStrictEqual([
					{ role: 'a', g: [['t1', 't2'], ['t2']] },
				]);
				expect(nones).toStrictEqual([
					{ role: 'a', g: [undefined, undefined] },
				]);
			}
		});

		it('a nested aggregate is refused on SurrealDB 4 and yields 0 on 3', async () => {
			const nested = query(
				'SELECT role AS role, math::sum(math::max(n)) AS v FROM gr GROUP BY role ORDER BY role;',
			);
			if (engine.majorVersion >= 4) {
				await expect(nested).rejects.toThrow(
					/Nested aggregate functions are not supported/,
				);
			} else {
				expect(await nested).toStrictEqual([
					[
						{ role: 'a', v: 0 },
						{ role: 'b', v: 0 },
					],
				]);
			}
		});
	});

	describe('SPLIT and OMIT', () => {
		beforeAll(async () => {
			await query(`
				DEFINE TABLE sp SCHEMAFULL;
				DEFINE FIELD name ON sp TYPE string;
				DEFINE FIELD tags ON sp TYPE array<string>;
				DEFINE FIELD opt ON sp TYPE option<array<string>>;
				DEFINE FIELD ssn ON sp TYPE option<string>;
				DEFINE FIELD addr ON sp TYPE object;
				DEFINE FIELD addr.city ON sp TYPE string;
				DEFINE FIELD addr.zip ON sp TYPE string;
				CREATE sp:1 CONTENT { name: 'a', tags: ['x', 'y'], opt: ['o'], ssn: '1', addr: { city: 'c', zip: 'z' } };
				CREATE sp:2 CONTENT { name: 'b', tags: [], addr: { city: 'd', zip: 'w' } };
			`);
		});

		it('SPLIT makes a row per element, keeps a record whose array is empty unsplit, and leaves NONE as NONE', async () => {
			expect(
				await query(
					'SELECT name, tags FROM sp SPLIT tags ORDER BY name, tags; SELECT name, opt FROM sp SPLIT opt ORDER BY name;',
				),
			).toStrictEqual([
				[
					{ name: 'a', tags: 'x' },
					{ name: 'a', tags: 'y' },
					{ name: 'b', tags: [] },
				],
				[
					{ name: 'a', opt: 'o' },
					{ name: 'b', opt: undefined },
				],
			]);
		});

		it('the SPLIT target is a projection alias on SurrealDB 4 and the stored field on 3; a target the projection leaves out is refused on both', async () => {
			const byField = query(
				'SELECT name, tags AS tag FROM sp SPLIT tags ORDER BY name, tag;',
			);
			const byAlias = query(
				'SELECT name, tags AS tag FROM sp SPLIT tag ORDER BY name;',
			);
			if (engine.majorVersion >= 4) {
				await expect(byField).rejects.toThrow(
					/Missing split idiom `"tags"`/,
				);
				expect(await byAlias).toStrictEqual([
					[
						{ name: 'a', tag: 'x' },
						{ name: 'a', tag: 'y' },
						{ name: 'b', tag: [] },
					],
				]);
			} else {
				expect(await byField).toStrictEqual([
					[
						{ name: 'a', tag: 'x' },
						{ name: 'a', tag: 'y' },
						{ name: 'b', tag: [] },
					],
				]);
				expect(await byAlias).toStrictEqual([
					[
						{ name: 'a', tag: ['x', 'y'] },
						{ name: 'b', tag: [] },
					],
				]);
			}
			await expect(
				query('SELECT name FROM sp SPLIT tags;'),
			).rejects.toThrow(/Missing split idiom `"tags"`/);
		});

		it('SPLIT with ONLY fails on more than one row', async () => {
			await expect(
				query('SELECT * FROM ONLY sp:1 SPLIT tags;'),
			).rejects.toThrow(
				/Expected a single result output when using the ONLY keyword/,
			);
		});

		it('OMIT leaves top-level and nested fields out, and ORDER BY still reads an omitted field', async () => {
			expect(
				await query(
					'SELECT * OMIT id, ssn, addr.zip FROM sp ORDER BY name; SELECT * OMIT id, name, addr, opt, ssn FROM sp ORDER BY name DESC;',
				),
			).toStrictEqual([
				[
					{
						name: 'a',
						tags: ['x', 'y'],
						opt: ['o'],
						addr: { city: 'c' },
					},
					{ name: 'b', tags: [], addr: { city: 'd' } },
				],
				[{ tags: [] }, { tags: ['x', 'y'] }],
			]);
		});

		it('OMIT and SPLIT combine', async () => {
			expect(
				await query(
					'SELECT * OMIT id, ssn, addr, opt FROM sp SPLIT tags ORDER BY name, tags;',
				),
			).toStrictEqual([
				[
					{ name: 'a', tags: 'x' },
					{ name: 'a', tags: 'y' },
					{ name: 'b', tags: [] },
				],
			]);
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
			user = await within(openRecordSession(engine));
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

	describe('arithmetic', () => {
		it('int with int is an int, with a float a float, with a decimal a decimal; a float with a decimal is a decimal', async () => {
			expect(
				await query(
					'RETURN [type::of(1 + 2), type::of(1 + 2.5), type::of(1 + 2dec), type::of(1.5 + 2dec), type::of(1.5 + 2.5), type::of(5 - 7), type::of(3 * 4), type::of(2.5 * 2), type::of(2dec * 3)];',
				),
			).toStrictEqual([
				[
					'int',
					'float',
					'decimal',
					'decimal',
					'float',
					'int',
					'int',
					'float',
					'decimal',
				],
			]);
			const [values] = (await query(
				'RETURN [1 + 2, 1 + 2.5, 5 - 7, 3 * 4, 0.1 + 0.2, 1 + 2dec, 0.1dec + 0.2dec];',
			)) as [Array<unknown>];
			expect(values.slice(0, 5)).toStrictEqual([
				3,
				3.5,
				-2,
				12,
				0.1 + 0.2,
			]);
			expect(values.slice(5).map(String)).toStrictEqual(['3', '0.3']);
		});

		it('int division truncates toward zero; float and decimal division is exact', async () => {
			const [values] = (await query(
				'RETURN [7 / 2, 6 / 2, -7 / 2, 7 / -2, 1 / 3, type::of(7 / 2), 7.0 / 2, -7 / 2.0, type::of(7.0 / 2), <string>(7dec / 2), <string>(1dec / 3), type::of(7dec / 2)];',
			)) as [Array<unknown>];
			expect(values).toStrictEqual([
				3,
				3,
				-3,
				-3,
				0,
				'int',
				3.5,
				-3.5,
				'float',
				// A decimal quotient keeps a scale of its own.
				'3.50',
				'0.3333333333333333333333333333',
				'decimal',
			]);
		});

		it('division by zero or by NONE is the float NaN or ±Infinity, whatever the operands', async () => {
			const [kinds, texts] = (await query(
				'RETURN [type::of(1 / 0), type::of(1.0 / 0), type::of(0 / 0), type::of(1dec / 0), type::of(1 / NONE)]; RETURN [<string>(1 / 0), <string>(-1 / 0), <string>(1.0 / 0), <string>(-1.0 / 0), <string>(0.0 / 0), <string>(1dec / 0), <string>(1 / NONE)];',
			)) as [Array<unknown>, Array<unknown>];
			expect(kinds).toStrictEqual([
				'float',
				'float',
				'float',
				'float',
				'float',
			]);
			expect(texts).toStrictEqual([
				'NaN',
				'NaN',
				'Infinity',
				'-Infinity',
				'NaN',
				'NaN',
				'NaN',
			]);
		});

		it('an int that overflows 64 bits fails the statement; a float past its range is Infinity', async () => {
			await expect(
				query('RETURN 9223372036854775807 + 1;'),
			).rejects.toThrow(
				"Cannot perform addition with '9223372036854775807' and '1'",
			);
			await expect(
				query('RETURN 9223372036854775807 * 2;'),
			).rejects.toThrow(
				"Cannot perform multiplication with '9223372036854775807' and '2'",
			);
			await expect(
				query('RETURN -9223372036854775807 - 2;'),
			).rejects.toThrow(
				"Cannot perform subtraction with '-9223372036854775807' and '2'",
			);
			expect(
				await query(
					'RETURN [<string>(1e308 * 10), type::of(1e308 * 10)];',
				),
			).toStrictEqual([['Infinity', 'float']]);
			// An int past 2^53 comes back from the SDK as a bigint.
			expect(await query('RETURN 9007199254740993 + 0;')).toStrictEqual([
				9007199254740993n,
			]);
		});

		it('a whole number bound by the SDK arrives as an int, even for a float, and divides as one; <float> casts it; a float field stores it as a float', async () => {
			expect(
				await query('RETURN [type::of($f), $f / 2, <float>$f / 2];', {
					f: 7,
				}),
			).toStrictEqual([['int', 3, 3.5]]);
			const [, , stored] = (await query(
				'DEFINE TABLE ar_float SCHEMAFULL; DEFINE FIELD w ON ar_float TYPE float; SELECT VALUE [type::of(w), w / 2] FROM ONLY (CREATE ONLY ar_float:1 SET w = $f);',
				{ f: 7 },
			)) as Array<unknown>;
			expect(stored).toStrictEqual(['float', 3.5]);
		});

		it('NONE, NULL, a string, a boolean or an array is refused as an operand of + - *; strings concatenate with +', async () => {
			for (const [sql, message] of [
				[
					'RETURN 1 + NONE;',
					"Cannot perform addition with 'int' and 'none'",
				],
				[
					'RETURN 1 - NONE;',
					"Cannot perform subtraction with 'int' and 'none'",
				],
				[
					'RETURN 1 * NONE;',
					"Cannot perform multiplication with 'int' and 'none'",
				],
				[
					'RETURN 1 + NULL;',
					"Cannot perform addition with 'int' and 'null'",
				],
				[
					'RETURN 1 + "2";',
					"Cannot perform addition with 'int' and 'string'",
				],
				[
					'RETURN 1 + true;',
					"Cannot perform addition with 'int' and 'bool'",
				],
				[
					'RETURN 1 + [2];',
					"Cannot perform addition with 'int' and 'array'",
				],
			] as const) {
				await expect(query(sql)).rejects.toThrow(message);
			}
			expect(await query('RETURN "a" + "b";')).toStrictEqual(['ab']);
		});
	});

	describe('introspection: INFO … STRUCTURE, which the drift check reads', () => {
		beforeAll(async () => {
			await query(`
				DEFINE TABLE in_p SCHEMAFULL;
				DEFINE FIELD name ON in_p TYPE string;
				DEFINE FIELD email ON in_p TYPE string;
				DEFINE FIELD tags ON in_p TYPE array<string>;
				DEFINE FIELD body ON in_p TYPE string;
				DEFINE FIELD vec ON in_p TYPE array<float>;
				DEFINE TABLE in_any TYPE ANY SCHEMALESS;
				DEFINE TABLE in_plain SCHEMALESS;
				DEFINE TABLE in_g TYPE NORMAL SCHEMALESS;
				DEFINE TABLE in_r TYPE RELATION IN in_p OUT in_g | in_p ENFORCED;
				DEFINE TABLE in_l TYPE RELATION FROM in_p TO in_p;
				DEFINE TABLE in_u TYPE RELATION;
				DEFINE INDEX in_email ON in_p FIELDS email UNIQUE;
				DEFINE INDEX in_multi ON in_p FIELDS name, email;
				DEFINE INDEX in_tags ON in_p FIELDS tags.*;
				DEFINE INDEX in_odd ON in_plain FIELDS \`odd.name\`, profile.bio;
				DEFINE ANALYZER in_simple TOKENIZERS blank FILTERS lowercase;
				DEFINE INDEX in_ft ON in_p FIELDS body FULLTEXT ANALYZER in_simple BM25 HIGHLIGHTS;
				DEFINE INDEX in_vec ON in_p FIELDS vec HNSW DIMENSION 3 DIST COSINE;
				DEFINE INDEX in_dk ON in_p FIELDS vec DISKANN DIMENSION 3 DIST EUCLIDEAN;
				DEFINE INDEX in_count ON in_p COUNT;
				DEFINE INDEX in_open ON in_p COUNT WHERE name = 'open';
			`);
		});

		it('INFO FOR DB STRUCTURE lists each table with its kind: NORMAL, ANY (also without TYPE), or RELATION with its tables and ENFORCED', async () => {
			const [db] = (await query('INFO FOR DB STRUCTURE;')) as [
				{
					readonly tables: ReadonlyArray<{
						readonly name: string;
						readonly kind: unknown;
					}>;
				},
			];
			// A relation's tables, as names or as the SDK's `Table` values.
			const named = (kind: unknown) =>
				JSON.parse(
					JSON.stringify(kind, (_, value) =>
						value instanceof Table ? `table:${value.name}` : value,
					),
				);
			const kinds = Object.fromEntries(
				db.tables
					.filter((table) => table.name.startsWith('in_'))
					.map((table) => [table.name, named(table.kind)]),
			);
			expect(kinds).toStrictEqual({
				in_any: { kind: 'ANY' },
				in_g: { kind: 'NORMAL' },
				in_l: {
					kind: 'RELATION',
					in: ['in_p'],
					out: ['in_p'],
					enforced: false,
				},
				in_p: { kind: 'NORMAL' },
				in_plain: { kind: 'ANY' },
				// Written `IN … OUT …` the tables come back as names, and written
				// `FROM … TO …` as `Table` values, on 3.3 and 4.0 alike.
				in_r: {
					kind: 'RELATION',
					in: ['in_p'],
					out: ['in_g', 'in_p'],
					enforced: true,
				},
				in_u: { kind: 'RELATION', enforced: false },
			});
		});

		it('INFO FOR TABLE … STRUCTURE lists each index with its fields as SurrealQL spells them and a definition that starts with its kind, empty for a standard one', async () => {
			const [table, plain, missing] = (await query(
				'INFO FOR TABLE ⟨in_p⟩ STRUCTURE; INFO FOR TABLE in_plain STRUCTURE; INFO FOR TABLE in_nosuch STRUCTURE;',
			)) as Array<{
				readonly indexes: ReadonlyArray<{
					readonly name: string;
					readonly cols: ReadonlyArray<string>;
					readonly index: string;
				}>;
			}>;
			const indexes = Object.fromEntries(
				(table?.indexes ?? []).map((index) => [
					index.name,
					{ cols: index.cols, index: index.index },
				]),
			);
			expect(indexes.in_email).toStrictEqual({
				cols: ['email'],
				index: 'UNIQUE',
			});
			expect(indexes.in_multi).toStrictEqual({
				cols: ['name', 'email'],
				index: '',
			});
			expect(indexes.in_tags).toStrictEqual({
				cols: ['tags.*'],
				index: '',
			});
			expect(plain?.indexes).toMatchObject([
				{
					name: 'in_odd',
					cols: ['`odd.name`', 'profile.bio'],
					index: '',
				},
			]);
			expect(indexes.in_count).toStrictEqual({
				cols: [],
				index: 'COUNT',
			});
			expect(indexes.in_open).toStrictEqual({
				cols: [],
				index: "COUNT WHERE name = 'open'",
			});
			expect(indexes.in_ft?.index).toMatch(
				/^FULLTEXT ANALYZER in_simple BM25\(1\.2,0\.75\) HIGHLIGHTS$/,
			);
			expect(indexes.in_vec?.index).toMatch(
				/^HNSW DIMENSION 3 DIST COSINE /,
			);
			expect(indexes.in_dk?.index).toMatch(
				/^DISKANN DIMENSION 3 DIST EUCLIDEAN /,
			);
			// A table that does not exist has no definitions, without an error.
			expect(missing?.indexes).toStrictEqual([]);
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

	describe('what blocks rely on', () => {
		it('a branch yields its last expression, an object literal included, and NONE when no branch is taken', async () => {
			expect(
				await query(
					[
						'IF true { LET $x = 1; { a: $x, b: NONE } } ELSE { 2 };',
						'IF false { 1 };',
						'IF true { IF false { 1 } ELSE { 2 } } ELSE { 3 };',
						"IF false { 1 } ELSE IF true { THROW 'never' ; 2 } ELSE { 3 };",
					].join('\n'),
				).catch((error: unknown) => error),
			).toMatchObject({
				kind: 'Thrown',
				message: 'An error occurred: never',
			});
			expect(
				await query(
					[
						'IF true { LET $x = 1; { a: $x, b: NONE } } ELSE { 2 };',
						'IF false { 1 };',
						'IF true { IF false { 1 } ELSE { 2 } } ELSE { 3 };',
						'IF true { CREATE ONLY blk:one SET n = 1 RETURN NONE; 7 };',
						'IF true { NONE };',
					].join('\n'),
				),
			).toStrictEqual([
				{ a: 1, b: undefined },
				undefined,
				2,
				7,
				undefined,
			]);
		});

		it('outside a transaction, a THROW fails only its own statement: the statements after it run', async () => {
			await query('DEFINE TABLE after_throw;');
			await expect(
				query(
					"IF true { THROW 'stop' }; CREATE after_throw:one SET n = 1;",
				),
			).rejects.toMatchObject({ kind: 'Thrown' });
			expect(
				await query('SELECT VALUE id FROM after_throw;'),
			).toStrictEqual([[new RecordId('after_throw', 'one')]]);
		});

		it('a block statement is all or nothing: a THROW inside it stops the rest and undoes what it wrote, even outside a transaction', async () => {
			await query('DEFINE TABLE in_block;');
			await expect(
				query(
					"{ CREATE in_block:a SET n = 1; IF true { THROW 'stop' }; CREATE in_block:b SET n = 1; 7 }; CREATE in_block:c SET n = 1;",
				),
			).rejects.toMatchObject({
				kind: 'Thrown',
				message: 'An error occurred: stop',
			});
			expect(await query('SELECT VALUE id FROM in_block;')).toStrictEqual(
				[[new RecordId('in_block', 'c')]],
			);
		});

		it('a LET inside a top-level block is local to it, in a transaction too', async () => {
			expect(
				await query('{ LET $inner = 1; $inner + 1 }; $inner;'),
			).toStrictEqual([2, undefined]);
			expect(
				await within(
					db.transaction<unknown[]>([
						'{ LET $inner = 1; $inner + 1 };',
						'$inner;',
					]),
				),
			).toStrictEqual([2, undefined]);
		});

		it('a RETURN in the value of a LET: at the top level the LET yields it and binds nothing; in a block SurrealDB 4 ends the block with it and 3 binds it', async () => {
			expect(await query('LET $x = (RETURN 1); $x;')).toStrictEqual([
				1,
				undefined,
			]);
			const inBlock = await query(
				'{ LET $x = (RETURN 2); $x + 10 }; { LET $x = (IF true { RETURN 2 }); $x + 10 };',
			);
			if (engine.majorVersion >= 4) {
				expect(inBlock).toStrictEqual([2, 2]);
			} else {
				expect(inBlock).toStrictEqual([12, 12]);
			}
		});

		it('a block and an IF are values: either can be the value of a LET', async () => {
			expect(
				await query(
					'LET $x = ({\n\tLET $a = 1;\n\t$a + 1\n}); LET $y = (IF $x = 2 { 3 } ELSE { 4 }); [$x, $y];',
				),
			).toStrictEqual([undefined, undefined, [2, 3]]);
		});

		it('a LET inside a branch is local to it and shadows an outer name only there', async () => {
			expect(
				await query(
					'LET $x = 1; IF true { LET $x = 2; LET $y = 3; $x + $y }; $x; $y;',
				),
			).toStrictEqual([undefined, 5, 1, undefined]);
		});

		it('a RETURN inside an IF ends the request on neither engine, but ends a transaction on SurrealDB 3 and not on 4', async () => {
			expect(await query('IF true { RETURN 1 }; 2;')).toStrictEqual([
				1, 2,
			]);
			// The SDK refuses only a top-level RETURN, so it sends this.
			const results = await within(
				db.transaction<unknown[]>(['IF true { RETURN 1 };', '2;']),
			);
			if (engine.majorVersion >= 4) {
				expect(results).toStrictEqual([1, 2]);
			} else {
				// What follows is skipped, and its result is missing.
				expect(results).toStrictEqual([1]);
			}
			// A branch's last expression is its value on both, in a transaction too.
			expect(
				await within(
					db.transaction<unknown[]>(['IF true { 1 };', '2;']),
				),
			).toStrictEqual([1, 2]);
		});

		it('a LET inside a block of a name SurrealDB sets is refused on SurrealDB 4 and shadows it on 3', async () => {
			const shadowed = query('IF true { LET $value = 1; $value };');
			if (engine.majorVersion >= 4) {
				await expect(shadowed).rejects.toMatchObject({
					kind: 'Validation',
					message:
						"'value' is a protected variable and cannot be set",
				});
			} else {
				expect(await shadowed).toStrictEqual([1]);
			}
		});

		it('FOR UPDATE reads one record from a bound or LET id, and is refused for a table or a field path (SurrealDB 3.3 and 4)', async () => {
			await query('DEFINE TABLE lock; CREATE lock:one SET n = 1;');
			expect(
				await query('SELECT VALUE n FROM ONLY $id FOR UPDATE;', {
					id: new RecordId('lock', 'one'),
				}),
			).toStrictEqual([1]);
			expect(
				await query(
					'LET $id = lock:one; LET $row = (SELECT * FROM ONLY $id WHERE n = 1 FOR UPDATE); $row.n;',
				),
			).toStrictEqual([undefined, undefined, 1]);
			expect(
				await query('SELECT * FROM ONLY lock:missing FOR UPDATE;'),
			).toStrictEqual([undefined]);
			const refused =
				/SELECT \.\.\. FOR UPDATE requires record ids as targets/;
			await expect(
				query('SELECT * FROM lock WHERE n = 1 FOR UPDATE;'),
			).rejects.toThrow(refused);
			await expect(
				query(
					'LET $row = (SELECT * FROM ONLY lock:one); SELECT * FROM ONLY $row.id FOR UPDATE;',
				),
			).rejects.toThrow(refused);
		});

		it('pins FOR UPDATE conflicts at interactive commit and refusal on unsupported transports', async () => {
			if (!interactiveTransactions) {
				await expect(
					within(db.beginTransaction()),
				).rejects.toMatchObject({
					constructor: UnsupportedFeatureError,
					feature: Features.Transactions,
				});
				return;
			}
			await query(
				'DEFINE TABLE held; CREATE held:a SET n = 1; CREATE held:b SET n = 1;',
			);
			const attempt = async (lock: string, written: string) => {
				const tx = await within(db.beginTransaction());
				try {
					await within(
						Promise.resolve(
							tx.query(`SELECT * FROM ONLY held:a${lock};`),
						),
					);
					await query(`UPDATE ${written} SET n += 1;`);
					await within(
						Promise.resolve(
							tx.query('CREATE held_log SET at = time::now();'),
						),
					);
					await within(tx.commit());
					return 'committed';
				} catch (error) {
					return (error as { isTransactionConflict?: boolean })
						.isTransactionConflict === true
						? 'conflict'
						: error;
				}
			};
			expect(await attempt('', 'held:a')).toBe('committed');
			expect(await attempt(' FOR UPDATE', 'held:a')).toBe('conflict');
			expect(await attempt(' FOR UPDATE', 'held:b')).toBe('committed');
		});
	});

	describe('full-text and nearest-neighbour search', () => {
		const v = [1, 0, 0, 0];
		beforeAll(async () => {
			await query(`
				DEFINE ANALYZER words TOKENIZERS blank, class FILTERS lowercase;
				DEFINE TABLE ft_doc SCHEMAFULL;
				DEFINE FIELD title ON ft_doc TYPE string;
				DEFINE FIELD body ON ft_doc TYPE string;
				DEFINE FIELD embedding ON ft_doc TYPE array<float, 4>;
				DEFINE INDEX ft_doc_body ON ft_doc FIELDS body FULLTEXT ANALYZER words BM25 HIGHLIGHTS;
				DEFINE INDEX ft_doc_title ON ft_doc FIELDS title FULLTEXT ANALYZER words BM25;
				DEFINE INDEX ft_doc_embedding ON ft_doc FIELDS embedding HNSW DIMENSION 4 DIST COSINE;
				${[0, 1, 2, 3, 4, 5]
					.map(
						(i) =>
							`CREATE ft_doc:f${i} SET title = 'filler ${i}', body = 'common words only', embedding = [0.0, 0.0, 1.0, ${i}.0];`,
					)
					.join('\n')}
				CREATE ft_doc:fox SET title = 'fox', body = 'the quick fox and another fox', embedding = [1.0, 0.0, 0.0, 0.0];
				CREATE ft_doc:dog SET title = 'dog', body = 'a quick dog', embedding = [0.9, 0.1, 0.0, 0.0];
				DEFINE TABLE ft_plain SCHEMAFULL;
				DEFINE FIELD body ON ft_plain TYPE string;
				DEFINE FIELD embedding ON ft_plain TYPE array<float>;
				CREATE ft_plain:a SET body = 'the fox', embedding = [1.0, 0.0];
				CREATE ft_plain:b SET body = 'a dog', embedding = [0.0, 1.0];
			`);
		});

		it('a match reference, a neighbour count and a search effort are literals only', async () => {
			await expect(
				query("SELECT id FROM ft_doc WHERE body @$r@ 'fox';", { r: 1 }),
			).rejects.toThrow(/expected a match reference/);
			await expect(
				query("SELECT id FROM ft_doc WHERE body @256@ 'fox';"),
			).rejects.toThrow(/Parse error/);
			expect(
				await query(
					"SELECT VALUE id FROM ft_doc WHERE body @255@ 'fox';",
				),
			).toHaveLength(1);
			await expect(
				query('SELECT id FROM ft_doc WHERE embedding <|$k,40|> $v;', {
					k: 2,
					v,
				}),
			).rejects.toThrow(/expected an unsigned integer/);
			await expect(
				query('SELECT id FROM ft_doc WHERE embedding <|2,$ef|> $v;', {
					ef: 40,
					v,
				}),
			).rejects.toThrow(/expected a distance of an integer/);
		});

		it('the search terms and the query vector take bindings', async () => {
			expect(
				await query(
					'SELECT VALUE id FROM ft_doc WHERE body @1@ $terms; SELECT VALUE id FROM ft_doc WHERE embedding <|1,40|> $v;',
					{ terms: 'fox', v },
				),
			).toMatchObject([[{ id: 'fox' }], [{ id: 'fox' }]]);
		});

		it('brute-force JACCARD and PEARSON give other distances and other nearest rows on SurrealDB 3.3 than on 4.0', async () => {
			const nearest = async (distance: string) =>
				(
					(await query(
						`SELECT record::id(id) AS id, vector::distance::knn() AS d FROM ft_doc WHERE embedding <|3,${distance}|> $q ORDER BY d, id;`,
						{ q: [0.8, 0.15, 0.45, 1.3] },
					)) as [Array<{ id: string; d: number }>]
				)[0].map(({ id, d }) => [id, Number(d.toFixed(6))]);
			if (engine.majorVersion >= 4) {
				// One minus the similarity: a distance.
				expect(await nearest('JACCARD')).toStrictEqual([
					['f1', 0.553846],
					['f2', 0.556962],
					['f3', 0.646465],
				]);
				expect(await nearest('PEARSON')).toStrictEqual([
					['f5', 0.178193],
					['f4', 0.189275],
					['f3', 0.212946],
				]);
			} else {
				// JACCARD is 0 for every row, so the nearest are any three;
				// PEARSON is the correlation itself (4.0's one minus it), so
				// the least correlated rows come first.
				expect(
					(await nearest('JACCARD')).map(([, d]) => d),
				).toStrictEqual([0, 0, 0]);
				expect(await nearest('PEARSON')).toStrictEqual([
					['f0', -0.303562],
					['dog', 0.092856],
					['fox', 0.168646],
				]);
			}
		});

		it('a match without a FULLTEXT index is an error', async () => {
			await expect(
				query("SELECT id FROM ft_plain WHERE body @1@ 'fox';"),
			).rejects.toThrow(
				/There was no suitable index supporting the expression/,
			);
		});

		it('<|k, ef|> without a vector index is an error from SurrealDB 4.0.1 and no rows before', async () => {
			const unindexed = query(
				'SELECT id FROM ft_plain WHERE embedding <|1,40|> $v;',
				{ v: [1, 0] },
			);
			const [major = 0, minor = 0, patch = 0] = (
				/(\d+)\.(\d+)\.(\d+)/u.exec(engine.version) ?? []
			)
				.slice(1)
				.map(Number);
			const rejects =
				major !== 4 ? major > 4 : minor !== 0 ? minor > 0 : patch >= 1;
			if (rejects) {
				await expect(unindexed).rejects.toThrow(
					/needs an HNSW or DiskANN index on `embedding`/,
				);
			} else {
				expect(await unindexed).toStrictEqual([[]]);
			}
			// The brute-force form needs no index.
			expect(
				await query(
					'SELECT VALUE id FROM ft_plain WHERE embedding <|1,EUCLIDEAN|> $v;',
					{ v: [1, 0] },
				),
			).toMatchObject([[{ id: 'a' }]]);
		});

		it('scores a term in at least half the rows above 0 on SurrealDB 4 and 0 on 3; a rarer one above 0 on both', async () => {
			const [common] = (await query(
				"SELECT VALUE search::score(1) FROM ft_doc WHERE body @1@ 'common';",
			)) as [Array<number>];
			expect(common).toHaveLength(6);
			for (const value of common) {
				if (engine.majorVersion >= 4) expect(value).toBeGreaterThan(0);
				else expect(value).toBe(0);
			}
			const [rare] = (await query(
				"SELECT VALUE search::score(1) FROM ft_doc WHERE body @1@ 'quick';",
			)) as [Array<number>];
			expect(rare).toHaveLength(2);
			for (const value of rare) expect(value).toBeGreaterThan(0);
		});

		it('a score of a reference no match defines reads another match’s score, without an error', async () => {
			const [[row]] = (await query(
				"SELECT search::score(1) AS s, search::score(2) AS stray FROM ft_doc WHERE body @1@ 'fox';",
			)) as [[{ s: number; stray: number }]];
			expect(row.s).toBeGreaterThan(0);
			expect(row.stray).toBe(row.s);
		});

		it('a row admitted through OR scores 0, is unmarked and has no offsets', async () => {
			const [rows] = (await query(
				"SELECT id, search::score(1) AS s, search::highlight('<', '>', 1) AS h, search::offsets(1) AS o FROM ft_doc WHERE body @1@ 'fox' OR title = 'dog' ORDER BY id;",
			)) as [Array<Record<string, unknown>>];
			expect(rows.map(({ id, ...rest }) => rest)).toStrictEqual([
				{ s: 0, h: 'a quick dog', o: undefined },
				{
					s: expect.any(Number),
					h: 'the quick <fox> and another <fox>',
					o: {
						0: [
							{ s: 10, e: 13 },
							{ s: 26, e: 29 },
						],
					},
				},
			]);
		});

		it('two matches sharing a reference highlight both on SurrealDB 4 and the last on 3', async () => {
			const [marked] = await query(
				"SELECT VALUE search::highlight('<', '>', 1) FROM ft_doc WHERE body @1@ 'fox' AND body @1@ 'another';",
			);
			expect(marked).toStrictEqual(
				engine.majorVersion >= 4
					? ['the quick <fox> and <another> <fox>']
					: ['the quick fox and <another> fox'],
			);
		});

		it('an index without HIGHLIGHTS leaves the text unmarked', async () => {
			expect(
				await query(
					"SELECT VALUE search::highlight('<', '>', 1) FROM ft_doc WHERE title @1@ 'fox';",
				),
			).toStrictEqual([['fox']]);
		});

		it('vector::distance::knn() is the distance of each hit, and an error without a search', async () => {
			const [hits] = (await query(
				'SELECT VALUE vector::distance::knn() FROM ft_doc WHERE embedding <|2,40|> $v;',
				{ v },
			)) as [Array<number>];
			expect(hits[0]).toBe(0);
			expect(hits[1]).toBeGreaterThan(0);
			await expect(
				query('SELECT vector::distance::knn() FROM ft_doc;'),
			).rejects.toThrow(/no KNN operator found in WHERE condition/);
		});

		it('a WHERE takes one search on SurrealDB 4 (3 takes two), only at the top level on both', async () => {
			const two = query(
				'SELECT VALUE id FROM ft_doc WHERE embedding <|2,40|> $v AND embedding <|1,40|> $w;',
				{ v, w: [0, 1, 0, 0] },
			);
			if (engine.majorVersion >= 4) {
				await expect(two).rejects.toThrow(
					/A WHERE clause supports a single KNN operator/,
				);
			} else {
				expect(await two).toHaveLength(1);
			}
			await expect(
				query(
					"SELECT id FROM ft_doc WHERE (embedding <|2,40|> $v) OR title = 'dog';",
					{ v },
				),
			).rejects.toThrow(
				/must appear at the top level of the WHERE clause/,
			);
		});

		it('a query vector of another length than the index dimension is an error', async () => {
			await expect(
				query('SELECT id FROM ft_doc WHERE embedding <|2,40|> $v;', {
					v: [1, 0, 0],
				}),
			).rejects.toThrow(/Incorrect vector dimension \(3\)/);
		});

		it('rrf and linear merge rows by id, add their score, and leave out rows without an id', async () => {
			const lists =
				"[(SELECT id, title, vector::distance::knn() AS distance FROM ft_doc WHERE embedding <|2,40|> $v), (SELECT id, body, search::score(1) AS ft_score FROM ft_doc WHERE body @1@ 'quick')]";
			type Rows = [Array<Record<string, unknown>>];
			const [fused] = (await query(
				`RETURN search::rrf(${lists}, 10, 60);`,
				{ v },
			)) as Rows;
			const [weighted] = (await query(
				`RETURN search::linear(${lists}, [1, 1], 10, 'minmax');`,
				{ v },
			)) as Rows;
			const [noIds] = (await query(
				'RETURN search::rrf([(SELECT title FROM ft_doc), (SELECT title FROM ft_doc)], 10);',
			)) as Rows;
			expect(fused.map((row) => Object.keys(row).sort())).toStrictEqual([
				['body', 'distance', 'ft_score', 'id', 'rrf_score', 'title'],
				['body', 'distance', 'ft_score', 'id', 'rrf_score', 'title'],
			]);
			expect(
				weighted.every((row) => typeof row.linear_score === 'number'),
			).toBe(true);
			expect(noIds).toStrictEqual([]);
			await expect(
				query(`RETURN search::rrf(${lists}, 0, 60);`, { v }),
			).rejects.toThrow(/limit must be at least 1/);
		});

		it('plans a match through its FULLTEXT index, <|k, ef|> through its vector index, and the brute-force form through none', async () => {
			const plan = async (
				sql: string,
				bindings?: Record<string, unknown>,
			) => JSON.stringify(await query(`${sql} EXPLAIN;`, bindings));
			expect(
				await plan("SELECT id FROM ft_doc WHERE body @1@ 'fox'"),
			).toContain('"index":"ft_doc_body"');
			expect(
				await plan(
					'SELECT id FROM ft_doc WHERE embedding <|2,40|> $v',
					{
						v,
					},
				),
			).toContain('"index":"ft_doc_embedding"');
			const brute = await plan(
				'SELECT id FROM ft_doc WHERE embedding <|2,COSINE|> $v',
				{ v },
			);
			expect(brute).toContain('"operator":"KnnTopK"');
			expect(brute).not.toContain('"index"');
			// Without its index a match plans a table scan, and fails when it runs.
			expect(
				await plan("SELECT id FROM ft_plain WHERE body @1@ 'fox'"),
			).not.toContain('"index"');
		});
	});

	describe('record links, graph steps, FETCH and recursive paths', () => {
		let user: SurrealSession;
		const asUser = (sql: string) => query(sql, undefined, user);
		/** Record ids as text, sorted: the engine does not promise an order. */
		const ids = (value: unknown): unknown =>
			Array.isArray(value)
				? value
						.map((item) =>
							item instanceof RecordId
								? `${item.table.name}:${String(item.id)}`
								: ids(item),
						)
						.sort()
				: value;

		beforeAll(async () => {
			await query(`
				DEFINE TABLE g_person SCHEMAFULL PERMISSIONS FULL;
				DEFINE FIELD name ON g_person TYPE string;
				DEFINE FIELD boss ON g_person TYPE option<record<g_person>>;
				DEFINE FIELD watchers ON g_person TYPE array<record<g_person>> DEFAULT [];
				DEFINE TABLE g_team SCHEMAFULL PERMISSIONS FULL;
				DEFINE FIELD name ON g_team TYPE string;
				DEFINE FIELD size ON g_team TYPE int;
				DEFINE TABLE g_secret SCHEMAFULL PERMISSIONS NONE;
				DEFINE FIELD name ON g_secret TYPE string;
				DEFINE FIELD pin ON g_person TYPE option<record<g_person | g_team>>;
				DEFINE FIELD secret ON g_person TYPE option<record<g_secret>>;
				DEFINE TABLE g_knows TYPE RELATION IN g_person OUT g_person ENFORCED;
				DEFINE TABLE g_in TYPE RELATION IN g_person OUT g_team | g_secret ENFORCED SCHEMAFULL PERMISSIONS FULL;
				DEFINE FIELD role ON g_in TYPE string;
				DEFINE TABLE g_loose TYPE RELATION IN g_person OUT g_person;
				DEFINE ACCESS g_user ON DATABASE TYPE RECORD
					SIGNUP (CREATE g_person SET name = $name)
					SIGNIN (SELECT * FROM g_person WHERE name = $name);
				CREATE g_team:t SET name = 'T', size = 3;
				CREATE g_secret:s SET name = 'S';
				CREATE g_person:a SET name = 'A', pin = g_team:t, secret = g_secret:s;
				CREATE g_person:b SET name = 'B', boss = g_person:a, pin = g_person:a;
				CREATE g_person:c SET name = 'C', boss = g_person:ghost, watchers = [g_person:ghost, g_person:a];
				CREATE g_person:d SET name = 'D', boss = g_person:b;
				RELATE g_person:a->g_knows->g_person:b;
				RELATE g_person:a->g_knows->g_person:c;
				RELATE g_person:b->g_knows->g_person:d;
				RELATE g_person:c->g_knows->g_person:d;
				RELATE g_person:a->g_in->g_team:t SET role = 'lead';
				RELATE g_person:a->g_in->g_secret:s SET role = 'hidden';
				RELATE g_person:a->g_loose->g_person:ghost;
				CREATE g_holder:one SET l = r"g_nosuch:1";
			`);
			user = await within(openRecordSession(engine));
			const scope = {
				namespace: engine.namespace,
				database: engine.namespace,
			};
			await within(user.use(scope));
			await within(
				user.signup({
					...scope,
					access: 'g_user',
					variables: { name: 'u' },
				}),
			);
		});
		afterAll(async () => {
			await user?.closeSession();
		});

		it('a field read through a link is NONE when the link is absent or its record missing, in a list too', async () => {
			expect(
				await query(
					'RETURN [g_person:a.boss.name, g_person:b.boss.name, g_person:c.boss.name, g_person:c.watchers.name];',
				),
			).toStrictEqual([[undefined, 'A', undefined, [undefined, 'A']]]);
		});

		it('a link to several tables reads NONE for a field its record does not have', async () => {
			expect(
				await query(
					'RETURN [g_person:a.pin.size, g_person:b.pin.size];',
				),
			).toStrictEqual([[3, undefined]]);
		});

		it('a link to a record the session may not read is NONE followed or fetched; a step leaves the record out and keeps its edge', async () => {
			const [followed, fetched, roles] = (await asUser(
				'SELECT VALUE secret.name FROM ONLY g_person:a; SELECT secret FROM ONLY g_person:a FETCH secret; SELECT VALUE ->g_in.role FROM ONLY g_person:a;',
			)) as [unknown, unknown, Array<string>];
			expect(followed).toBeUndefined();
			expect(fetched).toStrictEqual({ secret: undefined });
			expect([...roles].sort()).toStrictEqual(['hidden', 'lead']);
			expect(
				ids(
					await asUser(
						'SELECT VALUE ->g_in->? FROM ONLY g_person:a;',
					),
				),
			).toStrictEqual([['g_team:t']]);
			expect(
				ids(
					await query('SELECT VALUE ->g_in->? FROM ONLY g_person:a;'),
				),
			).toStrictEqual([['g_secret:s', 'g_team:t']]);
		});

		it('chained steps flatten, duplicates kept; from a list of links or after a recursive path they nest; from NONE they are NONE', async () => {
			const [chained, fromList, afterRecursion, fromNone] = (await query(
				'RETURN g_person:a->g_knows->?->g_knows->?; RETURN g_person:c.watchers->g_knows->?; RETURN g_person:a.{..+collect}(->g_knows->?)->g_knows->?; RETURN g_person:a.boss->g_knows->?;',
			)) as [unknown, Array<unknown>, Array<unknown>, unknown];
			expect(ids(chained)).toStrictEqual(['g_person:d', 'g_person:d']);
			expect(fromList.map(ids)).toStrictEqual([
				[],
				['g_person:b', 'g_person:c'],
			]);
			expect(afterRecursion.every(Array.isArray)).toBe(true);
			expect(fromNone).toBeUndefined();
		});

		it('`<->` yields both endpoints of every edge, the start once per edge', async () => {
			expect(
				ids(await query('RETURN g_person:b<->g_knows<->?;')),
			).toStrictEqual([
				['g_person:a', 'g_person:b', 'g_person:b', 'g_person:d'],
			]);
		});

		it('a relation that is not enforced keeps an edge to a missing record, which reads as NONE; an enforced one refuses it, and deleting a record deletes its edges', async () => {
			expect(
				await query(
					'RETURN [g_person:a->g_loose->?.name, g_person:a->g_loose->?.*, g_person:a->g_loose->?.{ n: name }];',
				),
			).toStrictEqual([[[undefined], [undefined], [undefined]]]);
			await expect(
				query('RELATE g_person:a->g_knows->g_person:ghost;'),
			).rejects.toThrow("The record 'g_person:ghost' does not exist");
			const deleted = await query(
				'CREATE g_person:z SET name = "Z"; RELATE g_person:d->g_knows->g_person:z RETURN NONE; DELETE g_person:z; RETURN g_person:d->g_knows->?;',
			);
			expect(deleted.at(-1)).toStrictEqual([]);
		});

		it('an edge filter, destructuring and `.*` apply under a step', async () => {
			const [filtered, picked, materialised] = (await query(
				'RETURN g_person:a->(g_in WHERE role = $role)->?; RETURN g_person:a->g_knows->?.{ n: name }; RETURN g_person:a->g_knows->?.*;',
				{ role: 'lead' },
			)) as [
				unknown,
				Array<{ n: string }>,
				Array<Record<string, unknown>>,
			];
			expect(ids(filtered)).toStrictEqual(['g_team:t']);
			expect(picked.map((row) => row.n).sort()).toStrictEqual(['B', 'C']);
			expect(materialised.map((row) => row.name).sort()).toStrictEqual([
				'B',
				'C',
			]);
		});

		it('FETCH replaces a link with its record, NONE for a missing one and in a list, names the projected alias, and fetches the prefix of a nested path', async () => {
			const [found, missing, list, alias, unfetched, nested] =
				(await query(
					[
						'SELECT boss FROM ONLY g_person:b FETCH boss;',
						'SELECT boss FROM ONLY g_person:c FETCH boss;',
						'SELECT watchers FROM ONLY g_person:c FETCH watchers;',
						'SELECT boss AS chief FROM ONLY g_person:b FETCH chief;',
						'SELECT boss AS chief FROM ONLY g_person:b FETCH boss;',
						'SELECT boss FROM ONLY g_person:d FETCH boss.boss;',
					].join(' '),
				)) as Array<Record<string, unknown>>;
			expect(found).toMatchObject({ boss: { name: 'A' } });
			expect(missing).toStrictEqual({ boss: undefined });
			expect(list).toMatchObject({
				watchers: [undefined, { name: 'A' }],
			});
			expect(alias).toMatchObject({ chief: { name: 'A' } });
			expect(unfetched?.chief).toBeInstanceOf(RecordId);
			expect(nested).toMatchObject({
				boss: { name: 'B', boss: { name: 'A' } },
			});
		});

		it('a recursive path keeps the records of the last repetition that reached any, collects each once, and finds a shortest path or NONE', async () => {
			const results = await query(
				[
					'RETURN g_person:a.{1..3}(->g_knows->?);',
					'RETURN g_person:a.{1..1}(->g_knows->?);',
					'RETURN g_person:a.{3..4}(->g_knows->?);',
					'RETURN g_person:a.{..+collect}(->g_knows->?);',
					'RETURN g_person:a.{..+collect+inclusive}(->g_knows->?);',
					'RETURN g_person:a.{..+shortest=g_person:d}(->g_knows->?);',
					'RETURN g_person:d.{..+shortest=g_person:a}(->g_knows->?);',
					'RETURN g_person:a.{2..1}(->g_knows->?);',
				].join(' '),
			);
			expect(results.slice(0, 5).map(ids)).toStrictEqual([
				['g_person:d', 'g_person:d'],
				['g_person:b', 'g_person:c'],
				[],
				['g_person:b', 'g_person:c', 'g_person:d'],
				['g_person:a', 'g_person:b', 'g_person:c', 'g_person:d'],
			]);
			const [path, none, reversed] = results.slice(5) as [
				Array<RecordId>,
				unknown,
				unknown,
			];
			expect(path).toHaveLength(2);
			expect(ids(path.slice(1))).toStrictEqual(['g_person:d']);
			expect(none).toBeUndefined();
			expect(reversed).toBeUndefined();
		});

		it('a recursion depth outside 1 to 256 is refused', async () => {
			await expect(
				query('RETURN g_person:a.{0..2}(->g_knows->?);'),
			).rejects.toThrow('Found 0 for bound but expected at least 1.');
			await expect(
				query('RETURN g_person:a.{1..257}(->g_knows->?);'),
			).rejects.toThrow('Found 257 for bound but expected 256 at most.');
		});

		describe('$parent in a step’s where', () => {
			/** One `{ id, r }` row per record, `r` as sorted record ids. */
			const pairs = (value: unknown) =>
				(value as Array<{ id: RecordId; r: unknown }>).map((row) => [
					`${row.id.table.name}:${String(row.id.id)}`,
					ids(row.r),
				]);

			beforeAll(async () => {
				await query(`
					DEFINE TABLE pa_p SCHEMALESS;
					DEFINE TABLE pa_e TYPE RELATION IN pa_p OUT pa_p;
					DEFINE TABLE pa_a TYPE RELATION IN pa_p OUT pa_p;
					CREATE pa_p:1 SET y = 'k1', link = pa_p:2;
					CREATE pa_p:2 SET y = 'k2', link = pa_p:3;
					CREATE pa_p:3 SET y = 'k3';
					RELATE pa_p:1->pa_e->pa_p:2 SET x = 'k1';
					RELATE pa_p:1->pa_e->pa_p:3 SET x = 'k2';
					RELATE pa_p:2->pa_e->pa_p:3 SET x = 'k2';
					RELATE pa_p:2->pa_e->pa_p:1 SET x = 'k1';
					RELATE pa_p:3->pa_e->pa_p:1 SET x = 'k3';
					RELATE pa_p:1->pa_a->pa_p:2;
				`);
			});

			it('is the select’s row directly under it: from the row, from a binding, chained, in WHERE, and with the step destructured', async () => {
				const [direct, binding, second, first, where, nested, picked] =
					(await query(
						[
							'SELECT id, id->(pa_e WHERE x = $parent.y)->? AS r FROM pa_p ORDER BY id;',
							'SELECT id, $s->(pa_e WHERE x = $parent.y)->? AS r FROM pa_p ORDER BY id;',
							'SELECT id, id->pa_a->?->(pa_e WHERE x = $parent.y)->? AS r FROM pa_p ORDER BY id;',
							'SELECT id, id->(pa_e WHERE x = $parent.y)->?->pa_e->? AS r FROM pa_p ORDER BY id;',
							'SELECT VALUE id FROM pa_p WHERE id->(pa_e WHERE x = $parent.y)->? CONTAINS pa_p:1 ORDER BY id;',
							'SELECT id, id->(pa_e WHERE x = $parent.link.y)->? AS r FROM pa_p ORDER BY id;',
							'SELECT id, id->(pa_e WHERE x = $parent.y)->?.{ n: y } AS r FROM pa_p ORDER BY id;',
						].join(' '),
						{ s: new RecordId('pa_p', 1) },
					)) as Array<unknown>;
				expect(pairs(direct)).toStrictEqual([
					['pa_p:1', ['pa_p:2']],
					['pa_p:2', ['pa_p:3']],
					['pa_p:3', ['pa_p:1']],
				]);
				// From the binding `pa_p:1`, each row's own `y` picks the edge.
				expect(pairs(binding)).toStrictEqual([
					['pa_p:1', ['pa_p:2']],
					['pa_p:2', ['pa_p:3']],
					['pa_p:3', []],
				]);
				// pa_p:1->pa_a->pa_p:2, whose edges are filtered by pa_p:1's `y`.
				expect(pairs(second)).toStrictEqual([
					['pa_p:1', ['pa_p:1']],
					['pa_p:2', []],
					['pa_p:3', []],
				]);
				expect(pairs(first)).toStrictEqual([
					['pa_p:1', ['pa_p:1', 'pa_p:3']],
					['pa_p:2', ['pa_p:1']],
					['pa_p:3', ['pa_p:2', 'pa_p:3']],
				]);
				expect(ids(where)).toStrictEqual(['pa_p:3']);
				expect(pairs(nested)).toStrictEqual([
					['pa_p:1', ['pa_p:3']],
					['pa_p:2', []],
					['pa_p:3', []],
				]);
				expect(picked).toStrictEqual([
					{ id: new RecordId('pa_p', 1), r: [{ n: 'k2' }] },
					{ id: new RecordId('pa_p', 2), r: [{ n: 'k3' }] },
					{ id: new RecordId('pa_p', 3), r: [{ n: 'k1' }] },
				]);
			});

			it('inside a destructuring is the outer row on SurrealDB 3 and the destructured record on 4', async () => {
				const [ofLink, ofStep] = (await query(
					[
						'SELECT VALUE link.{ r: ->(pa_e WHERE x = $parent.y)->? }.r FROM ONLY pa_p:1;',
						'SELECT VALUE id->pa_a->?.{ r: ->(pa_e WHERE x = $parent.y)->? }.r FROM ONLY pa_p:1;',
					].join(' '),
				)) as Array<unknown>;
				// pa_p:1 (y = k1) links to pa_p:2 (y = k2): pa_p:2's edges are
				// pa_e x = k2 to pa_p:3 and x = k1 to pa_p:1.
				if (engine.majorVersion >= 4) {
					expect(ids(ofLink)).toStrictEqual(['pa_p:3']);
					expect((ofStep as Array<unknown>).map(ids)).toStrictEqual([
						['pa_p:3'],
					]);
				} else {
					expect(ids(ofLink)).toStrictEqual(['pa_p:1']);
					expect((ofStep as Array<unknown>).map(ids)).toStrictEqual([
						['pa_p:1'],
					]);
				}
			});

			it('under a recursive path is the select’s row on SurrealDB 4 and matches nothing on 3', async () => {
				const [recursive] = (await query(
					'SELECT id, id.{1..2}(->(pa_e WHERE x = $parent.y)->?) AS r FROM pa_p ORDER BY id;',
				)) as Array<unknown>;
				expect(pairs(recursive)).toStrictEqual(
					engine.majorVersion >= 4
						? [
								['pa_p:1', ['pa_p:2']],
								['pa_p:2', ['pa_p:3']],
								['pa_p:3', ['pa_p:1']],
							]
						: [
								['pa_p:1', []],
								['pa_p:2', []],
								['pa_p:3', []],
							],
				);
			});

			it('under a bare RETURN or a block, with no select, binds something on SurrealDB 4 and nothing on 3', async () => {
				const [bare, inBlock] = (await query(
					'RETURN pa_p:1->(pa_e WHERE x = $parent.y)->?; { LET $s = pa_p:1; $s->(pa_e WHERE x = $parent.y)->? };',
				)) as Array<unknown>;
				if (engine.majorVersion >= 4) {
					expect(ids(bare)).toStrictEqual(['pa_p:2']);
					expect(ids(inBlock)).toStrictEqual(['pa_p:2']);
				} else {
					expect(bare).toStrictEqual([]);
					expect(inBlock).toStrictEqual([]);
				}
			});
		});

		describe('a step as the subject of a select, and a filter on the records it reaches', () => {
			beforeAll(async () => {
				await query(`
					DEFINE TABLE ps_p SCHEMALESS;
					DEFINE TABLE ps_e TYPE RELATION IN ps_p OUT ps_p;
					CREATE ps_p:1 SET y = 'k1';
					CREATE ps_p:2 SET y = 'k2';
					CREATE ps_p:3 SET y = 'k3';
					RELATE ps_p:1->ps_e->ps_p:2 SET x = 'k1';
					RELATE ps_p:1->ps_e->ps_p:3 SET x = 'k2';
					RELATE ps_p:2->ps_e->ps_p:3 SET x = 'k2';
					RELATE ps_p:2->ps_e->ps_p:1 SET x = 'k1';
					RELATE ps_p:3->ps_e->ps_p:1 SET x = 'k3';
				`);
			});

			it('SELECT … FROM a step reads its records as rows: WHERE, ORDER BY, LIMIT, START and ONLY with LIMIT 1 apply; a missing start or NONE reads none', async () => {
				const [, rows, paged, only, missing, none] = (await query(
					[
						'LET $w = ps_p:1;',
						"SELECT VALUE y FROM $w->ps_e->? WHERE y != 'k9' ORDER BY y DESC LIMIT 2;",
						'SELECT VALUE y FROM $w->ps_e->? ORDER BY y ASC LIMIT 1 START 1;',
						'SELECT VALUE y FROM ONLY $w->ps_e->? ORDER BY y LIMIT 1;',
						'SELECT * FROM ps_p:9->ps_e->?;',
						'SELECT * FROM NONE->ps_e->?;',
					].join(' '),
				)) as Array<unknown>;
				expect(rows).toStrictEqual(['k3', 'k2']);
				expect(paged).toStrictEqual(['k3']);
				expect(only).toBe('k2');
				expect(missing).toStrictEqual([]);
				expect(none).toStrictEqual([]);
			});

			it('chained steps keep their duplicates as rows; the edges of a step and a recursive path are subjects too', async () => {
				const [chained, edgeRows, collected] = (await query(
					[
						'SELECT VALUE id FROM ps_p:1->ps_e->?->ps_e->?;',
						'SELECT VALUE x FROM ps_p:1->ps_e ORDER BY x;',
						'SELECT VALUE id FROM ps_p:1.{..+collect}(->ps_e->?);',
					].join(' '),
				)) as Array<unknown>;
				expect(ids(chained)).toStrictEqual([
					'ps_p:1',
					'ps_p:1',
					'ps_p:3',
				]);
				expect(edgeRows).toStrictEqual(['k1', 'k2']);
				expect(ids(collected)).toStrictEqual([
					'ps_p:1',
					'ps_p:2',
					'ps_p:3',
				]);
			});

			it('a subquery FROM a step of the outer row reads that row', async () => {
				const [rows] = (await query(
					'SELECT id, (SELECT VALUE y FROM id->ps_e->? ORDER BY y) AS r FROM ps_p ORDER BY id;',
				)) as Array<Array<{ r: unknown }>>;
				expect(rows?.map((row) => row.r)).toStrictEqual([
					['k2', 'k3'],
					['k1', 'k3'],
					['k1'],
				]);
			});

			it('`->(? WHERE …)` keeps the records reached that satisfy it, in each direction, after an edge filter, chained, followed and repeated', async () => {
				const results = (await query(
					[
						"RETURN ps_p:1->(ps_e WHERE x = 'k1')->(? WHERE y = 'k2');",
						"RETURN ps_p:1->(ps_e WHERE x = 'k1')->(? WHERE y = 'k3');",
						"RETURN ps_p:2<-ps_e<-(? WHERE y = 'k1');",
						"RETURN ps_p:2<->ps_e<->(? WHERE y = 'k1');",
						"RETURN ps_p:1->ps_e->(? WHERE y = 'k2')->ps_e->(? WHERE y != 'k1');",
						"RETURN ps_p:1->ps_e->(? WHERE y = 'k2').{ n: y };",
						"RETURN ps_p:1.{1..2}(->ps_e->(? WHERE y != 'k1'));",
						"RETURN ps_p:9->ps_e->(? WHERE y = 'k2');",
					].join(' '),
				)) as Array<unknown>;
				expect(
					results.map((value, at) => (at === 5 ? value : ids(value))),
				).toStrictEqual([
					['ps_p:2'],
					[],
					['ps_p:1'],
					// Both endpoints of each edge at ps_p:2 that pass.
					['ps_p:1', 'ps_p:1'],
					['ps_p:3'],
					[{ n: 'k2' }],
					['ps_p:3'],
					[],
				]);
			});
		});

		it('a link to a record of a table that does not exist reads NONE on SurrealDB 3 and is an error on 4', async () => {
			const read = query(
				'SELECT VALUE l.x FROM ONLY g_holder:one; SELECT l FROM ONLY g_holder:one FETCH l;',
			);
			if (engine.majorVersion >= 4) {
				await expect(read).rejects.toThrow(
					"The table 'g_nosuch' does not exist",
				);
			} else {
				expect(await read).toStrictEqual([undefined, { l: undefined }]);
			}
		});
	});
});
