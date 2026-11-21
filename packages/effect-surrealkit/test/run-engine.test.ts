import { Duration, Effect, Schema } from 'effect';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { make as makeClient, StatementThrown } from '../src/client/index.js';
import { requestPart } from '../src/client/internal/join.js';
import { sdkTransport } from '../src/client/internal/transport.js';
import { make } from '../src/statement/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
} from './engine.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] run engine suite skipped: ${engineSkipReason}`,
	);
}

describe.skipIf(!engineConfigured)(
	'run compatibility on a disposable engine',
	() => {
		let engine: Engine;
		let retryChecks = 0;

		beforeAll(async () => {
			engine = await startEngine({
				retry: {
					attempts: 2,
					retryDelay: 1,
					retryDelayJitter: 0,
					retryable: (error) => {
						if (
							!(error instanceof Error) ||
							error.message !== 'An error occurred: retry probe'
						) {
							return false;
						}
						retryChecks++;
						return true;
					},
				},
			});
			console.log(
				`[effect-surrealkit] run: ${engine.mode} engine ${engine.version}`,
			);
		});
		afterAll(async () => {
			await engine?.stop();
		});

		it('classifies the second slot when a three-slot query fails there', async () => {
			const statement = make({
				name: 'failure in the second slot',
				sql: "NONE; THROW 'slot two'; 3;",
				params: {},
				result: Schema.Number,
				throws: ['slot two'],
				slots: 3,
				writes: false,
			});
			const error = await Effect.runPromise(
				Effect.flip(makeClient(engine.db).run(statement)),
			);
			expect(error).toBeInstanceOf(StatementThrown);
			expect(error).toMatchObject({
				statement: statement.name,
				reason: 'slot two',
			});
		});

		it('keeps raw response order and surfaces the root error after a rollback', async () => {
			await engine.db.query(
				'CREATE ONLY request_test:rollback SET n = 0;',
			);
			const statement = make({
				name: 'rollback before throw',
				sql: "BEGIN; UPDATE ONLY request_test:rollback SET n += 1 RETURN NONE; THROW 'real failure'; COMMIT;",
				params: {},
				result: Schema.Undefined,
				throws: ['real failure'],
				slots: 4,
				valueSlot: 3,
				writes: true,
			});
			const bindings = {};
			const slots = await sdkTransport(engine.db).send(
				{
					mode: 'query',
					sql: statement.sql,
					bindings,
					parts: [
						requestPart(statement, bindings, 0, statement.slots),
					],
				},
				new AbortController().signal,
			);
			expect(slots.map((slot) => slot.ok)).toEqual([
				true,
				false,
				false,
				false,
			]);
			expect(slots[1]).toMatchObject({
				error: {
					message:
						'The query was not executed due to a failed transaction',
				},
			});
			expect(slots[2]).toMatchObject({
				error: {
					kind: 'Thrown',
					message: 'An error occurred: real failure',
				},
			});
			expect(
				slots.every(({ server }) => Duration.isDuration(server)),
			).toBe(true);

			const error = await Effect.runPromise(
				Effect.flip(makeClient(engine.db).run(statement)),
			);
			expect(error).toBeInstanceOf(StatementThrown);
			expect(error).toMatchObject({
				statement: statement.name,
				reason: 'real failure',
			});
			expect(
				await engine.db.query(
					'SELECT VALUE n FROM ONLY request_test:rollback;',
				),
			).toEqual([0]);
		});

		it('honors the SDK connection retry policy without a client retry option', async () => {
			await engine.db.query('CREATE ONLY request_test:retry SET n = 0;');
			const statement = make({
				name: 'connection retry',
				sql: "UPDATE ONLY request_test:retry SET n += 1 RETURN NONE; THROW 'retry probe';",
				params: {},
				result: Schema.Undefined,
				throws: ['retry probe'],
				slots: 2,
				writes: true,
			});
			const error = await Effect.runPromise(
				Effect.flip(makeClient(engine.db).run(statement)),
			);
			expect(error).toBeInstanceOf(StatementThrown);
			expect(error).toMatchObject({
				statement: statement.name,
				reason: 'retry probe',
			});
			expect(retryChecks).toBe(3);
			expect(
				await engine.db.query(
					'SELECT VALUE n FROM ONLY request_test:retry;',
				),
			).toEqual([3]);
		});
	},
);
