/**
 * The `members` golden through a real engine: the fixture's schema
 * (`test/fixtures/members`) is applied to a disposable database and the
 * statements are built from the modules generated from its compiled
 * document. A record user signed up with the testing subpath's
 * `recordUser` reads what field permissions leave it.
 */
import { readFileSync } from 'node:fs';

import { Effect, type Layer } from 'effect';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
	run,
	StatementDecodeError,
	type SurrealClient,
} from '../src/client/index.js';
import { readAs, select } from '../src/statement/index.js';
import {
	layerFrom,
	recordUser,
	type TestDatabase,
} from '../src/testing/index.js';
import { membersSchemaPath } from './documents.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';
import { MemberTable } from './golden/members/index.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] members engine suite skipped: ${engineSkipReason}`,
	);
}

describe.skipIf(!engineConfigured)('the members golden on an engine', () => {
	let engine: Engine;
	let database: Layer.Layer<TestDatabase | SurrealClient>;

	beforeAll(async () => {
		engine = await startEngine();
		await within(
			engine.db.query(readFileSync(membersSchemaPath, 'utf8')),
			20_000,
		);
		await within(
			engine.db.query(`
				CREATE member:ann CONTENT {
					handle: 'ann', region: 'north', number: 1, email: 'ann@example.com',
					salary: 10, nickname: 'an', profile: { bio: 'b1', phone: '1' },
				};
				CREATE member:bob CONTENT {
					handle: 'bob', region: 'north', number: 2, email: 'bob@example.com',
					salary: 20, profile: { bio: 'b2', phone: '2' },
				};
			`),
		);
		database = layerFrom(engine.db, {
			namespace: engine.namespace,
			database: engine.namespace,
		});
	});
	afterAll(async () => {
		await engine?.stop();
	});

	/** Runs `effect` as a fresh record user of the `viewer` access. */
	const asViewer = <A, E>(
		effect: (client: SurrealClient['Service']) => Effect.Effect<A, E>,
	) =>
		Effect.runPromise(
			Effect.scoped(
				Effect.gen(function* () {
					const user = yield* recordUser({
						access: 'viewer',
						variables: { name: `v${Date.now()}` },
					});
					return yield* effect(user.client);
				}),
			).pipe(Effect.provide(database)),
		);

	describe('readAs(table): the row a restricted session reads', () => {
		const privileged = select(MemberTable)
			.orderBy((row) => row.handle)
			.build('members as root reads them');
		const visible = select(readAs(MemberTable))
			.orderBy((row) => row.handle)
			.build('members as a session reads them');

		it('the privileged row refuses what a record user gets: the fields it may not select are NONE', async () => {
			const error = await asViewer((client) =>
				Effect.flip(client.run(privileged)),
			);
			expect(error).toBeInstanceOf(StatementDecodeError);
		});

		it('the visible row decodes it, without the hidden fields, at any depth', async () => {
			const rows = await asViewer((client) => client.run(visible));
			expect(rows).toEqual([
				{
					id: expect.anything(),
					handle: 'ann',
					region: 'north',
					number: 1,
					profile: { bio: 'b1' },
				},
				{
					id: expect.anything(),
					handle: 'bob',
					region: 'north',
					number: 2,
					profile: { bio: 'b2' },
				},
			]);
		});

		it('and a privileged session still reads every field through it', async () => {
			const rows = await Effect.runPromise(
				run(visible).pipe(Effect.provide(database)),
			);
			expect(rows.map((row) => [row.email, row.salary])).toEqual([
				['ann@example.com', 10],
				['bob@example.com', 20],
			]);
			expect(rows[0]?.profile.phone).toBe('1');
			expect(rows[0]?.nickname).toBe('an');
		});
	});
});
