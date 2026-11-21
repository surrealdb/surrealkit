import { Deferred, Effect, Fiber, Option, Schema, Stream } from 'effect';
import { RecordId, Surreal } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
	LiveQueryClosed,
	make as makeClient,
	StatementRequestError,
	StatementUnsupported,
} from '../src/client/index.js';
import * as S from '../src/index.js';
import { add, eq, gt, live, param, readAs } from '../src/statement/index.js';
import { layerFrom, recordUser } from '../src/testing/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	liveQueries,
	startEngine,
	transport,
	within,
} from './engine.js';

if (!engineConfigured)
	console.warn(`[effect-surrealkit] live suite skipped: ${engineSkipReason}`);

const ItemId = S.RecordIdFor('l7_item');
const Item = {
	name: 'l7_item',
	id: ItemId,
	select: Schema.Struct({ id: ItemId, n: S.Int, label: S.String }),
} as const;
const plain = live(Item)
	.where((row) => gt(row.n, param('minimum', S.Int)))
	.build('by minimum');
const projection = live(Item)
	.where((row) => gt(row.n, param('minimum', S.Int)))
	.project((row) => ({ count: row.n, adjusted: add(row.n, 10) }))
	.build('projected live');
const reg = { minimum: 0 } as const;

const OwnerId = S.RecordIdFor('l7_user');
const RestrictedId = S.RecordIdFor('l7_private');
const restrictedVisible = Schema.Struct({
	id: RestrictedId,
	owner: OwnerId,
	label: S.String,
});
const restricted = live(
	readAs({
		name: 'l7_private',
		id: RestrictedId,
		select: Schema.Struct({
			...restrictedVisible.fields,
			secret: S.String,
		}),
		visible: restrictedVisible,
	}),
).build('owned records');

const setup = `
	DEFINE TABLE l7_item SCHEMAFULL;
	DEFINE FIELD n ON l7_item TYPE int;
	DEFINE FIELD label ON l7_item TYPE string;
	DEFINE TABLE l7_removed SCHEMALESS;
	DEFINE TABLE l7_user SCHEMAFULL PERMISSIONS FOR select FULL FOR create, update, delete NONE;
	DEFINE FIELD name ON l7_user TYPE string;
	DEFINE ACCESS l7_access ON DATABASE TYPE RECORD
		SIGNUP (CREATE l7_user SET name = $name)
		SIGNIN (SELECT * FROM l7_user WHERE name = $name);
	DEFINE TABLE l7_private SCHEMAFULL PERMISSIONS FOR select WHERE owner = $auth.id FOR create, update, delete NONE;
	DEFINE FIELD owner ON l7_private TYPE record<l7_user>;
	DEFINE FIELD label ON l7_private TYPE string;
	DEFINE FIELD secret ON l7_private TYPE string PERMISSIONS FOR select NONE;
`;

const liveIds = async (db: Surreal, sql = 'INFO FOR TABLE l7_item;') => {
	const [info] = await db
		.query<[{ readonly lives: Record<string, string> }]>(sql)
		.collect();
	return Object.keys(info.lives);
};
const run = <A, E>(program: Effect.Effect<A, E>) =>
	Effect.runPromise(program.pipe(Effect.timeout('8 seconds')));

const id = (key: string) => new RecordId('l7_item', key);

describe.skipIf(!engineConfigured || !liveQueries)(
	'live query engine behavior',
	() => {
		let engine: Engine;
		beforeAll(async () => {
			engine = await startEngine();
			console.info(
				`[effect-surrealkit] live queries: ${engine.transport} ${engine.version}`,
			);
			await engine.db.query(setup).collect();
		});
		afterAll(async () => {
			await engine?.stop();
		});

		it('delivers buffered CREATE, UPDATE and DELETE for plain and projected rows', async () => {
			const db = engine.db;
			const client = makeClient(db);
			const mutations = [
				"CREATE l7_item:events SET n = 7, label = 'before';",
				"UPDATE l7_item:events SET n = 8, label = 'after';",
				'DELETE l7_item:events;',
			];
			const record = id('events');
			const actual = await run(
				Effect.scoped(
					Effect.gen(function* () {
						const full = yield* client.liveScoped(plain, reg);
						const projected = yield* client.liveScoped(
							projection,
							reg,
						);
						expect(
							yield* Effect.promise(() => liveIds(db)),
						).toHaveLength(2);
						const observed = [];
						for (const mutation of mutations) {
							// The CREATE is written before either stream is pulled. Each
							// action is observed before the engine receives the next write.
							yield* Effect.promise(() =>
								db.query(mutation).collect(),
							);
							const a = yield* full.pipe(
								Stream.take(1),
								Stream.runCollect,
							);
							const b = yield* projected.pipe(
								Stream.take(1),
								Stream.runCollect,
							);
							observed.push([a[0], b[0]] as const);
						}
						return [
							observed.map(([full]) => full),
							observed.map(([, projected]) => projected),
						] as const;
					}),
				),
			);
			expect(actual).toEqual([
				[
					{
						action: 'CREATE',
						id: record,
						value: { id: record, n: 7, label: 'before' },
					},
					{
						action: 'UPDATE',
						id: record,
						value: { id: record, n: 8, label: 'after' },
					},
					{
						action: 'DELETE',
						id: record,
						value: { id: record, n: 8, label: 'after' },
					},
				],
				[
					{
						action: 'CREATE',
						id: record,
						value: { count: 7, adjusted: 17 },
					},
					{
						action: 'UPDATE',
						id: record,
						value: { count: 8, adjusted: 18 },
					},
					{
						action: 'DELETE',
						id: record,
						value: { count: 8, adjusted: 18 },
					},
				],
			]);
			expect(await liveIds(db)).toEqual([]);
		});

		it('uses parameterized WHERE and does not notify when an update stops matching', async () => {
			const db = engine.db;
			const client = makeClient(db);
			const observed = await run(
				Effect.scoped(
					Effect.gen(function* () {
						const stream = yield* client.liveScoped(plain, {
							minimum: 5,
						});
						const next = stream.pipe(
							Stream.take(1),
							Stream.runCollect,
						);
						yield* Effect.promise(() =>
							db
								.query(
									"CREATE l7_item:ignored SET n = 5, label = 'outside';",
								)
								.collect(),
						);
						expect(
							yield* next.pipe(
								Effect.timeoutOption('100 millis'),
							),
						).toEqual(Option.none());
						yield* Effect.promise(() =>
							db
								.query(
									"CREATE l7_item:matches SET n = 7, label = 'inside';",
								)
								.collect(),
						);
						const created = yield* next;
						yield* Effect.promise(() =>
							db
								.query('UPDATE l7_item:matches SET n = 1;')
								.collect(),
						);
						expect(
							yield* next.pipe(
								Effect.timeoutOption('100 millis'),
							),
						).toEqual(Option.none());
						yield* Effect.promise(() =>
							db
								.query('UPDATE l7_item:matches SET n = 8;')
								.collect(),
						);
						return [...created, ...(yield* next)];
					}),
				),
			);
			expect(
				observed.map((event) => [
					event.action,
					event.id.toString(),
					event.value.n,
				]),
			).toEqual([
				['CREATE', 'l7_item:matches', 7],
				['UPDATE', 'l7_item:matches', 8],
			]);
		});

		it('keeps two live queries on one connection isolated by query ID', async () => {
			const db = engine.db;
			const client = makeClient(db);
			const watch = live(Item)
				.where((row) => eq(row.n, param('choice', S.Int)))
				.build('choice');
			const both = await run(
				Effect.scoped(
					Effect.gen(function* () {
						const first = yield* client.liveScoped(watch, {
							choice: 23,
						});
						const second = yield* client.liveScoped(watch, {
							choice: 24,
						});
						yield* Effect.promise(() =>
							db
								.query(
									"CREATE l7_item:second SET n = 24, label = 'second'; CREATE l7_item:first SET n = 23, label = 'first';",
								)
								.collect(),
						);
						const a = yield* first.pipe(
							Stream.take(1),
							Stream.runCollect,
						);
						const b = yield* second.pipe(
							Stream.take(1),
							Stream.runCollect,
						);
						return [a[0]?.id.toString(), b[0]?.id.toString()];
					}),
				),
			);
			expect(both).toEqual(['l7_item:first', 'l7_item:second']);
		});

		it('uses the record user permissions and decodes the readAs row for CREATE and DELETE', async () => {
			const db = engine.db;
			const database = layerFrom(db, {
				namespace: engine.namespace,
				database: engine.namespace,
			});
			const actual = await run(
				Effect.scoped(
					Effect.gen(function* () {
						const ann = yield* recordUser({
							access: 'l7_access',
							variables: { name: 'Ann' },
						});
						const bob = yield* recordUser({
							access: 'l7_access',
							variables: { name: 'Bob' },
						});
						const [owner] = yield* Effect.promise(() =>
							ann.session.query('RETURN $auth.id;').collect(),
						);
						const [other] = yield* Effect.promise(() =>
							bob.session.query('RETURN $auth.id;').collect(),
						);
						const info = () =>
							liveIds(db, 'INFO FOR TABLE l7_private;');
						const events = yield* Effect.scoped(
							Effect.gen(function* () {
								const listener =
									yield* ann.client.liveScoped(restricted);
								expect(
									yield* Effect.promise(info),
								).toHaveLength(1);
								const next = listener.pipe(
									Stream.take(1),
									Stream.runCollect,
								);
								yield* Effect.promise(() =>
									db
										.query(
											"CREATE l7_private:bob SET owner = $other, label = 'Bob only', secret = 'hidden-b';",
											{ other },
										)
										.collect(),
								);
								expect(
									yield* next.pipe(
										Effect.timeoutOption('100 millis'),
									),
								).toEqual(Option.none());
								yield* Effect.promise(() =>
									db
										.query(
											"CREATE l7_private:ann SET owner = $owner, label = 'Ann only', secret = 'hidden-a';",
											{ owner },
										)
										.collect(),
								);
								const created = yield* next;
								yield* Effect.promise(() =>
									db
										.query('DELETE l7_private:bob;')
										.collect(),
								);
								expect(
									yield* next.pipe(
										Effect.timeoutOption('100 millis'),
									),
								).toEqual(Option.none());
								yield* Effect.promise(() =>
									db
										.query('DELETE l7_private:ann;')
										.collect(),
								);
								return [...created, ...(yield* next)];
							}),
						);
						expect(yield* Effect.promise(info)).toEqual([]);
						return { owner, events };
					}),
				).pipe(Effect.provide(database)),
			);
			const record = new RecordId('l7_private', 'ann');
			expect(actual.events).toEqual([
				{
					action: 'CREATE',
					id: record,
					value: {
						id: record,
						owner: actual.owner,
						label: 'Ann only',
					},
				},
				{
					action: 'DELETE',
					id: record,
					value: {
						id: record,
						owner: actual.owner,
						label: 'Ann only',
					},
				},
			]);
		});

		it('kills the query after interruption while a consumer is waiting', async () => {
			const db = engine.db;
			const client = makeClient(db);
			const ready = Effect.runSync(Deferred.make<void>());
			const fiber = Effect.runFork(
				Effect.scoped(
					Effect.gen(function* () {
						const listener = yield* client.liveScoped(plain, reg);
						yield* Deferred.succeed(ready, undefined);
						return yield* Stream.runDrain(listener);
					}),
				),
			);
			try {
				await within(Effect.runPromise(Deferred.await(ready)));
				expect(await liveIds(db)).toHaveLength(1);
			} finally {
				await run(Fiber.interrupt(fiber));
			}
			expect(await liveIds(db)).toEqual([]);
		});

		it('kills the server registration if SDK attachment fails', async () => {
			const db = engine.db;
			const cause = new Error('test attachment refusal');
			const client = makeClient({
				query: db.query.bind(db),
				transaction: db.transaction.bind(db),
				liveOf: () => {
					throw cause;
				},
			});
			const error = await run(
				Effect.flip(Effect.scoped(client.liveScoped(plain, reg))),
			);
			expect(error).toBeInstanceOf(StatementRequestError);
			expect(error).toHaveProperty('cause', cause);
			expect(await liveIds(db)).toEqual([]);
		});

		it('reports server KILLED even when the table is removed before the first pull', async () => {
			const db = engine.db;
			const Table = {
				name: 'l7_removed',
				id: S.RecordIdFor('l7_removed'),
				select: Schema.Struct({}),
			} as const;
			const listener = live(Table).build('removed');
			const error = await run(
				Effect.flip(
					Effect.scoped(
						Effect.gen(function* () {
							const stream =
								yield* makeClient(db).liveScoped(listener);
							yield* Effect.promise(() =>
								db.query('REMOVE TABLE l7_removed;').collect(),
							);
							return yield* Stream.runDrain(stream);
						}),
					),
				),
			);
			expect(error).toBeInstanceOf(LiveQueryClosed);
			expect(error).toMatchObject({ reason: 'killed' });
		});

		it('closes a stream with a typed disconnected error when its connection is lost', async () => {
			const isolated = await startEngine();
			try {
				await isolated.db.query(setup).collect();
				const error = await run(
					Effect.flip(
						Effect.scoped(
							Effect.gen(function* () {
								const stream = yield* makeClient(
									isolated.db,
								).liveScoped(plain, reg);
								yield* Effect.promise(() =>
									isolated.db.close(),
								);
								return yield* Stream.runDrain(stream);
							}),
						),
					),
				);
				expect(error).toBeInstanceOf(LiveQueryClosed);
				expect(error).toMatchObject({ reason: 'disconnected' });
			} finally {
				await isolated.stop();
			}
		});
	},
);

it.skipIf(!engineConfigured || transport !== 'http')(
	'refuses HTTP live queries without leaving a server registration',
	async () => {
		const engine = await startEngine();
		try {
			await engine.db.query(setup).collect();
			const error = await run(
				Effect.flip(
					Effect.scoped(makeClient(engine.db).liveScoped(plain, reg)),
				),
			);
			expect(error).toBeInstanceOf(StatementUnsupported);
			expect(error).toMatchObject({ feature: 'live query' });
			expect(await liveIds(engine.db)).toEqual([]);
		} finally {
			await engine.stop();
		}
	},
);

const disabledConfigurations = [
	{
		capabilities: {
			scripting: false,
			network_targets: false,
			live_query_notifications: false,
		},
	},
	{ capabilities: false },
] as const;

it
	.skipIf(!engineConfigured || transport !== 'embedded')
	.each(disabledConfigurations)(
	'embedded 4.0.1 still delivers notifications with %j',
	async (options) => {
		const { createNodeEngines } = await import('@surrealdb/node');
		const db = new Surreal({ engines: createNodeEngines(options) });
		try {
			await db.connect('mem://', {
				namespace: 'live_disabled',
				database: 'live_disabled',
			});
			await db
				.query(
					'DEFINE TABLE l7_item SCHEMAFULL; DEFINE FIELD n ON l7_item TYPE int; DEFINE FIELD label ON l7_item TYPE string;',
				)
				.collect();
			const received = await run(
				Effect.scoped(
					Effect.gen(function* () {
						const stream = yield* makeClient(db).liveScoped(
							plain,
							reg,
						);
						yield* Effect.promise(() =>
							db
								.query(
									"CREATE l7_item:seen SET n = 7, label = 'seen';",
								)
								.collect(),
						);
						return yield* stream.pipe(
							Stream.take(1),
							Stream.runCollect,
						);
					}),
				),
			);
			expect(
				received.map(({ action, value }) => [action, value.n]),
			).toEqual([['CREATE', 7]]);
			expect(await liveIds(db)).toEqual([]);
		} finally {
			await db.close();
		}
	},
);
