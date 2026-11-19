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
import { Duration, type Surreal, type SurrealSession } from 'surrealdb';
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
