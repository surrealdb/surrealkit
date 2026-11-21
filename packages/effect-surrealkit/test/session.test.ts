import { Effect, Schedule, Schema } from 'effect';
import { BoundQuery, QueryError } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
	bind,
	make as makeClient,
	StatementConflict,
} from '../src/client/index.js';
import { make } from '../src/statement/index.js';
import { conflictingSession } from '../src/testing/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
} from './engine.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] conflicting session suite skipped: ${engineSkipReason}`,
	);
}

const marker = 'injected conflict';
const bump = make({
	name: 'bump',
	sql: '/* injected conflict */ UPDATE ONLY request_probe:counter SET n += 1 RETURN VALUE n;',
	params: {},
	result: Schema.Number,
	writes: true,
});

describe.skipIf(!engineConfigured)(
	'conflicting session on a disposable engine',
	() => {
		let engine: Engine;

		beforeAll(async () => {
			engine = await startEngine();
			console.log(
				`[effect-surrealkit] conflicting session: ${engine.mode} engine ${engine.version}`,
			);
			await engine.db.query(
				'CREATE ONLY request_probe:counter SET n = 0;',
			);
		});
		afterAll(async () => {
			await engine?.stop();
		});

		const reset = () =>
			engine.db.query(
				'UPDATE ONLY request_probe:counter SET n = 0 RETURN NONE;',
			);
		const count = () =>
			engine.db.query('SELECT VALUE n FROM ONLY request_probe:counter;');

		it('injects only the first matching run sends and counts later matches', async () => {
			await reset();
			const { session, matched } = conflictingSession(engine.db, {
				marker,
				times: 2,
			});
			const client = makeClient(session);
			for (let i = 0; i < 2; i++) {
				const failure = await Effect.runPromise(
					Effect.flip(client.run(bump)),
				);
				expect(failure).toBeInstanceOf(StatementConflict);
				expect(failure).toHaveProperty('cause', expect.any(QueryError));
			}
			expect(await count()).toEqual([0]);
			expect(await session.query('1;')).toEqual([1]);
			expect(matched.count).toBe(2);
			expect(await Effect.runPromise(client.run(bump))).toBe(1);
			expect(await Effect.runPromise(client.run(bump))).toBe(2);
			expect(matched.count).toBe(4);
			expect(await count()).toEqual([2]);
		});

		it('injects only the first matching atomic sends and counts later matches', async () => {
			await reset();
			const { session, matched } = conflictingSession(engine.db, {
				marker,
				times: 2,
			});
			const client = makeClient(session);
			for (let i = 0; i < 2; i++) {
				const failure = await Effect.runPromise(
					Effect.flip(client.transaction([bind(bump)])),
				);
				expect(failure).toBeInstanceOf(StatementConflict);
			}
			expect(await count()).toEqual([0]);
			expect(
				await Effect.runPromise(client.transaction([bind(bump)])),
			).toEqual([1]);
			expect(
				await session.transaction([new BoundQuery(bump.sql)]),
			).toEqual([2]);
			expect(matched.count).toBe(4);
			expect(await count()).toEqual([2]);
		});

		it('passes the same request through on its retry after the injection', async () => {
			await reset();
			const { session, matched } = conflictingSession(engine.db, {
				marker,
				times: 1,
			});
			expect(
				await Effect.runPromise(
					makeClient(session).transaction([bind(bump)], {
						retry: Schedule.recurs(1),
					}),
				),
			).toEqual([1]);
			expect(matched.count).toBe(2);
			expect(await count()).toEqual([1]);

			const repeat = conflictingSession(engine.db, { marker, times: 1 });
			expect(
				await Effect.runPromise(
					makeClient(repeat.session).run(
						bump,
						{},
						{ retry: Schedule.recurs(1) },
					),
				),
			).toBe(2);
			expect(repeat.matched.count).toBe(2);
			expect(await count()).toEqual([2]);
		});

		it('preserves a conflict in an interior slot without sending any part of the request', async () => {
			await reset();
			const sql = [
				'UPDATE ONLY request_probe:counter SET n += 1 RETURN NONE;',
				'BEGIN;',
				bump.sql,
				"'a literal with ; and brackets ]';",
				'COMMIT;',
				'UPDATE ONLY request_probe:counter SET n += 1 RETURN NONE;',
			].join('\n');
			const { session, matched } = conflictingSession(engine.db, {
				marker,
				times: 1,
			});
			const slots = await session
				.query(sql)
				.signal(undefined)
				.responses();
			expect(slots).toHaveLength(6);
			expect(
				slots.map((slot) =>
					slot.success
						? 'ok'
						: (slot.error as QueryError).details?.kind,
				),
			).toEqual([
				'NotExecuted',
				'NotExecuted',
				'TransactionConflict',
				'NotExecuted',
				'NotExecuted',
				'NotExecuted',
			]);
			expect(matched.count).toBe(1);
			expect(await count()).toEqual([0]);
			expect(await session.query(bump.sql).responses()).toMatchObject([
				{ success: true, result: 1 },
			]);
			expect(matched.count).toBe(2);
		});

		it('injects direct collector and view reads, and forwards later views and disposal', async () => {
			await reset();
			const { session, matched } = conflictingSession(engine.db, {
				marker,
				times: 3,
			});
			const lazy = session.query(new BoundQuery(bump.sql));
			const abandoned = lazy.rows();
			await abandoned[Symbol.asyncDispose]();
			expect(matched.count).toBe(0);
			await expect(lazy.collect()).rejects.toMatchObject({
				isTransactionConflict: true,
			});
			await expect(lazy.rows().next()).rejects.toMatchObject({
				isTransactionConflict: true,
			});
			await expect(lazy.statements().next()).rejects.toMatchObject({
				isTransactionConflict: true,
			});
			expect(await count()).toEqual([0]);
			const view = lazy.rows<number>((n) => Number(n));
			expect(await view.next()).toEqual({ value: 1, done: false });
			await view[Symbol.asyncDispose]();
			expect((await view.next()).done).toBe(true);
			expect(await count()).toEqual([1]);
			expect(matched.count).toBe(4);
		});

		it('only counts dispatched requests and injects a raw stream frame', async () => {
			await reset();
			const { session, matched } = conflictingSession(engine.db, {
				marker,
				times: 1,
			});
			const reason = new Error('already aborted');
			const aborted = new AbortController();
			aborted.abort(reason);
			await expect(
				session.query(bump.sql).signal(aborted.signal).collect(),
			).rejects.toBe(reason);
			await expect(
				session.transaction([new BoundQuery(bump.sql)], {
					signal: aborted.signal,
				}),
			).rejects.toBe(reason);
			expect(matched.count).toBe(0);
			const frames = [];
			for await (const frame of session.query(bump.sql).stream())
				frames.push(frame);
			expect(frames).toMatchObject([
				{ query: 0, error: { isTransactionConflict: true } },
			]);
			expect(await count()).toEqual([0]);
			expect(matched.count).toBe(1);
		});

		it('keeps additional session methods, getters and own properties', async () => {
			const original = engine.db;
			const { session } = conflictingSession(original, {
				marker,
				times: 0,
			});
			expect(session).not.toBe(original);
			expect(await session.version()).toEqual(await original.version());
			expect(session.namespace).toBe(original.namespace);
			expect(session.database).toBe(original.database);

			const key = Symbol('extra');
			const custom = {
				query: original.query.bind(original),
				transaction: original.transaction.bind(original),
				[key]: 8,
				hint: 'extra',
			};
			const { session: forwarded } = conflictingSession(custom, {
				marker,
				times: 0,
			});
			expect(forwarded.hint).toBe('extra');
			expect(forwarded[key]).toBe(8);
			expect(forwarded.query).not.toBe(custom.query);
			expect(await forwarded.query('1;')).toEqual([1]);
		});
	},
);
