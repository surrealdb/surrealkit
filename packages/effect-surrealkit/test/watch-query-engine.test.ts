import { Effect, Schema, Stream } from 'effect';
import { RecordId, Surreal } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import {
	bindLive,
	make as makeClient,
	StatementScopeError,
	StatementUnsupported,
} from '../src/client/index.js';
import { Int, RecordIdFor, String as SurrealString } from '../src/index.js';
import {
	gt,
	live,
	make as makeStatement,
	param,
	readAs,
	select,
} from '../src/statement/index.js';
import { layerFrom, recordUser } from '../src/testing/index.js';
import {
	type Engine,
	engineConfigured,
	startEngine,
	transport,
	within,
} from './engine.js';

const EntryId = RecordIdFor('watch_entry');
const UserId = RecordIdFor('watch_user');
const visible = Schema.Struct({
	id: EntryId,
	label: SurrealString,
	score: Int,
	owner: UserId,
});
const entry = readAs({
	name: 'watch_entry',
	id: EntryId,
	select: Schema.Struct({ ...visible.fields, secret: SurrealString }),
	visible,
});
const topEntry = select(entry)
	.where((row) => gt(row.score, param('minimum', Int)))
	.orderBy((row) => row.score, 'DESC')
	.limit(1)
	.build('top authorized entry', { writes: false });
const entries = bindLive(
	live(entry)
		.project((row) => ({ id: row.id }))
		.build('entry changes'),
);
const VertexId = RecordIdFor('watch_vertex');
const EdgeId = RecordIdFor('watch_edge');
const vertices = bindLive(
	live({
		name: 'watch_vertex',
		id: VertexId,
		select: Schema.Struct({ id: VertexId }),
	}).build('vertex changes'),
);
const edges = bindLive(
	live({
		name: 'watch_edge',
		id: EdgeId,
		select: Schema.Struct({ id: EdgeId }),
	})
		.project((row) => ({ id: row.id }))
		.build('edge changes'),
);
const connected = makeStatement({
	name: 'connected vertices',
	sql: 'SELECT ->watch_edge->watch_vertex AS linked FROM ONLY $vertex;',
	params: { vertex: VertexId },
	result: Schema.Struct({ linked: Schema.Array(VertexId) }),
	writes: false,
});
const vertex = (key: string) =>
	Schema.decodeSync(VertexId)(new RecordId('watch_vertex', key));
const source = { vertex: vertex('source') };
const reconnect = { vertex: vertex('reconnect') };

const setup = `
	DEFINE TABLE watch_user SCHEMAFULL PERMISSIONS FOR select FULL FOR create, update, delete NONE;
	DEFINE FIELD name ON watch_user TYPE string;
	DEFINE FIELD enabled ON watch_user TYPE bool;
	DEFINE ACCESS watch_access ON DATABASE TYPE RECORD
		SIGNUP (CREATE watch_user SET name = $name, enabled = true)
		SIGNIN (SELECT * FROM watch_user WHERE name = $name);
	DEFINE TABLE watch_entry SCHEMAFULL
		PERMISSIONS FOR select WHERE owner = $auth.id AND owner.enabled FOR create, update, delete NONE;
	DEFINE FIELD owner ON watch_entry TYPE record<watch_user>;
	DEFINE FIELD label ON watch_entry TYPE string;
	DEFINE FIELD score ON watch_entry TYPE int;
	DEFINE FIELD secret ON watch_entry TYPE string PERMISSIONS FOR select NONE;
	DEFINE TABLE watch_vertex SCHEMAFULL;
	DEFINE TABLE watch_edge TYPE RELATION IN watch_vertex OUT watch_vertex SCHEMAFULL;
	CREATE watch_vertex:source, watch_vertex:reconnect, watch_vertex:target, watch_vertex:later;
	RELATE watch_vertex:reconnect->watch_edge->watch_vertex:target;
`;

const query = (db: Surreal, sql: string, bindings?: Record<string, unknown>) =>
	Effect.promise(() => db.query(sql, bindings).collect());
const liveIds = (
	db: Surreal,
	table: 'watch_entry' | 'watch_vertex' | 'watch_edge',
) =>
	Effect.promise(async () => {
		const [info] = await db
			.query<[{ readonly lives: Record<string, string> }]>(
				`INFO FOR TABLE ${table};`,
			)
			.collect();
		return Object.keys(info.lives);
	});
const run = <A, E>(program: Effect.Effect<A, E>) =>
	Effect.runPromise(program.pipe(Effect.timeout('10 seconds')));
const nextFrom = <A, E, R>(stream: Stream.Stream<A, E, R>) =>
	stream.pipe(
		Stream.rechunk(1),
		Stream.toPull,
		Effect.map(Effect.map(([state]) => state)),
	);
const recordScope = (engine: Engine) =>
	layerFrom(engine.db, {
		namespace: engine.namespace,
		database: engine.namespace,
	});
const user = (name: string) =>
	Effect.gen(function* () {
		const { session, client } = yield* recordUser({
			access: 'watch_access',
			variables: { name },
		});
		const [id] = yield* Effect.promise(() =>
			session.query<[RecordId]>('RETURN $auth.id;').collect(),
		);
		return { session, client, id };
	});
const graphStates = (db: Surreal, input: typeof source) =>
	nextFrom(
		makeClient(db).watchQuery(
			connected,
			{ sources: [vertices, edges], reconcileEvery: '1 hour' },
			input,
		),
	);
const currentEntry = (
	key: string,
	owner: RecordId,
	label: string,
	score: number,
) => ({
	_tag: 'Current',
	value: [{ id: new RecordId('watch_entry', key), owner, label, score }],
});
const currentGraph = (...keys: string[]) => ({
	_tag: 'Current',
	value: { linked: keys.map(vertex) },
});

// Let the writer commit during the gap before the SDK observes the new socket.
const reconnectableSocket = () => {
	const sockets: WebSocket[] = [];
	let ready = () => {};
	const replacement = new Promise<void>((resolve) => {
		ready = resolve;
	});
	class TrackedWebSocket extends WebSocket {
		constructor(...args: ConstructorParameters<typeof WebSocket>) {
			super(...args);
			if (sockets.length > 0) {
				this.addEventListener(
					'open',
					(event) => {
						event.stopImmediatePropagation();
						void replacement.then(() =>
							this.dispatchEvent(new Event('open')),
						);
					},
					{ once: true },
				);
			}
			sockets.push(this);
		}
	}
	return {
		sockets,
		ready,
		db: new Surreal({ websocketImpl: TrackedWebSocket }),
	};
};

const connectSocket = async (engine: Engine, db: Surreal) => {
	if (engine.rpcUrl === undefined)
		throw new Error('WebSocket fixture requires an RPC URL');
	await db.connect(engine.rpcUrl, {
		namespace: engine.namespace,
		database: engine.namespace,
		authentication: { username: 'root', password: 'root' },
		reconnect: { attempts: 2, retryDelay: 1, retryDelayJitter: 0 },
	});
};
const closeSocket = (sockets: WebSocket[]) => {
	const socket = sockets.at(-1);
	if (socket === undefined) throw new Error('no active subscriber socket');
	socket.close();
};

describe.skipIf(!engineConfigured || transport === 'http')(
	'watchQuery on a disposable engine',
	() => {
		let engine: Engine;
		beforeAll(async () => {
			engine = await startEngine();
			await engine.db.query(setup).collect();
		});
		afterAll(async () => {
			await engine?.stop();
		});

		it('publishes the authorized top result and refreshes when its winner exits', () => {
			const db = engine.db;
			const program = Effect.gen(function* () {
				const alice = yield* user('Alice top');
				const bob = yield* user('Bob top');
				const owner = alice.id;
				yield* query(
					db,
					`
					CREATE watch_entry:a SET owner = $owner, score = 2, label = 'A', secret = 'private';
					CREATE watch_entry:b SET owner = $owner, score = 3, label = 'B', secret = 'private';
					CREATE watch_entry:c SET owner = $other, score = 99, label = 'C', secret = 'private';
				`,
					{ owner, other: bob.id },
				);
				yield* Effect.scoped(
					Effect.gen(function* () {
						const next = yield* nextFrom(
							alice.client.watchQuery(
								topEntry,
								{
									sources: [entries],
									reconcileEvery: '1 hour',
								},
								{ minimum: 0 },
							),
						);
						expect(yield* next).toEqual({ _tag: 'Loading' });
						expect(yield* next).toEqual(
							currentEntry('b', owner, 'B', 3),
						);
						expect(yield* liveIds(db, 'watch_entry')).toHaveLength(
							1,
						);
						yield* query(db, 'UPDATE watch_entry:b SET score = 0;');
						expect(yield* next).toEqual(
							currentEntry('a', owner, 'A', 2),
						);
					}),
				);
				expect(yield* liveIds(db, 'watch_entry')).toEqual([]);
			});
			return run(
				Effect.scoped(program).pipe(
					Effect.provide(recordScope(engine)),
				),
			);
		});

		it('refreshes for an edge-only change and releases every registration', () => {
			const db = engine.db;
			const observe = Effect.gen(function* () {
				const next = yield* graphStates(db, source);
				expect(yield* next).toEqual({ _tag: 'Loading' });
				expect(yield* next).toEqual(currentGraph());
				expect(yield* liveIds(db, 'watch_vertex')).toHaveLength(1);
				expect(yield* liveIds(db, 'watch_edge')).toHaveLength(1);
				yield* query(
					db,
					'RELATE watch_vertex:source->watch_edge->watch_vertex:target;',
				);
				expect(yield* next).toEqual(currentGraph('target'));
			});
			return run(
				Effect.gen(function* () {
					yield* Effect.scoped(observe);
					expect(yield* liveIds(db, 'watch_vertex')).toEqual([]);
					expect(yield* liveIds(db, 'watch_edge')).toEqual([]);
				}),
			);
		});

		it('reconciles permission loss and clears the old identity on invalidate', () => {
			const db = engine.db;
			const program = Effect.gen(function* () {
				const { session, id: owner } = yield* user('Alice entitlement');
				yield* query(
					db,
					"CREATE watch_entry:entitled SET owner = $owner, score = 1, label = 'Entitled', secret = 'private';",
					{ owner },
				);
				const client = makeClient(session, { connection: db });
				const next = yield* nextFrom(
					client.watchQuery(
						topEntry,
						{ sources: [entries], reconcileEvery: '100 millis' },
						{ minimum: 0 },
					),
				);
				const granted = currentEntry('entitled', owner, 'Entitled', 1);
				expect(yield* next).toEqual({ _tag: 'Loading' });
				expect(yield* next).toEqual(granted);
				// Permission changes without mutating the observed watch_entry table.
				yield* query(db, 'UPDATE $owner SET enabled = false;', {
					owner,
				});
				const empty = yield* Effect.repeat(next, {
					until: (state) =>
						state._tag === 'Current' && state.value.length === 0,
				});
				expect(empty).toEqual({ _tag: 'Current', value: [] });
				yield* query(db, 'UPDATE $owner SET enabled = true;', {
					owner,
				});
				const visible = yield* Effect.repeat(next, {
					until: (state) =>
						state._tag === 'Current' && state.value.length > 0,
				});
				expect(visible).toEqual(granted);
				yield* Effect.promise(() => session.invalidate());
				expect(yield* next).toEqual({ _tag: 'Stale' });
				expect(yield* Effect.flip(next)).toMatchObject({
					_tag: 'WatchQueryClosed',
					reason: 'authentication',
				});
			});
			return run(
				Effect.gen(function* () {
					yield* Effect.scoped(program).pipe(
						Effect.provide(recordScope(engine)),
					);
					expect(yield* liveIds(db, 'watch_entry')).toEqual([]);
				}),
			);
		});

		it('discovers a closed child before delivering a queued snapshot', async () => {
			const db = engine.db;
			const child = await db.forkSession();
			const requests = vi.spyOn(child, 'query');
			try {
				await run(
					Effect.scoped(
						Effect.gen(function* () {
							const client = makeClient(child, {
								connection: db,
							});
							const next = yield* nextFrom(
								client.watchQuery(
									connected,
									{
										sources: [vertices, edges],
										reconcileEvery: '100 millis',
									},
									reconnect,
								),
							);
							expect(yield* next).toEqual({ _tag: 'Loading' });
							// Starting the second read proves the first result is already queued.
							yield* Effect.promise(() =>
								vi.waitFor(() => {
									const reads = requests.mock.calls.filter(
										([sql]) =>
											Object.is(sql, connected.sql),
									);
									expect(reads.length).toBeGreaterThanOrEqual(
										2,
									);
								}),
							);
							yield* Effect.promise(() => child.closeSession());
							expect(db.status).toBe('connected');
							expect(yield* next).toEqual({ _tag: 'Stale' });
							expect(yield* Effect.flip(next)).toMatchObject({
								_tag: 'WatchQueryClosed',
								reason: 'session-closed',
							});
						}),
					),
				);
				expect(await run(liveIds(db, 'watch_vertex'))).toEqual([]);
				expect(await run(liveIds(db, 'watch_edge'))).toEqual([]);
			} finally {
				requests.mockRestore();
				if (child.isValid) await child.closeSession();
			}
		});

		it('rejects a mismatched session owner before registering or reading', async () => {
			const db = engine.db;
			const otherEngine =
				engine.rpcUrl === undefined ? await startEngine() : undefined;
			const other = otherEngine?.db ?? new Surreal();
			try {
				if (otherEngine === undefined)
					await connectSocket(engine, other);
				const child = await db.forkSession();
				const requests = vi.spyOn(child, 'query');
				try {
					const client = makeClient(child, { connection: other });
					const stream = client.watchQuery(
						connected,
						{ sources: [vertices, edges] },
						source,
					);
					expect(
						await run(Effect.flip(Stream.runDrain(stream))),
					).toBeInstanceOf(StatementScopeError);
					expect(requests).not.toHaveBeenCalled();
					expect(await run(liveIds(db, 'watch_vertex'))).toEqual([]);
					expect(await run(liveIds(db, 'watch_edge'))).toEqual([]);
				} finally {
					requests.mockRestore();
					await child.closeSession();
				}
			} finally {
				if (otherEngine === undefined) await other.close();
				else await otherEngine.stop();
			}
		});

		it.skipIf(transport !== 'ws')(
			'marks a WebSocket stale and reacquires sources on reconnect',
			async () => {
				const { db, ready, sockets } = reconnectableSocket();
				try {
					await connectSocket(engine, db);
					const writer = engine.db;
					const observe = Effect.gen(function* () {
						const next = yield* graphStates(db, reconnect);
						expect(yield* next).toEqual({ _tag: 'Loading' });
						expect(yield* next).toEqual(currentGraph('target'));
						const ids = yield* liveIds(writer, 'watch_edge');
						expect(ids).toHaveLength(1);
						closeSocket(sockets);
						expect(yield* next).toEqual({ _tag: 'Stale' });
						yield* query(
							writer,
							'RELATE watch_vertex:reconnect->watch_edge->watch_vertex:later;',
						);
						ready();
						const fresh = yield* next;
						expect(
							fresh._tag === 'Current' &&
								fresh.value.linked.map(String).sort(),
						).toEqual([
							'watch_vertex:later',
							'watch_vertex:target',
						]);
						expect(sockets.length).toBeGreaterThan(1);
						const freshIds = yield* liveIds(writer, 'watch_edge');
						expect(freshIds).toHaveLength(1);
						expect(freshIds).not.toEqual(ids);
					});
					await run(
						Effect.gen(function* () {
							yield* Effect.scoped(observe);
							expect(
								yield* liveIds(writer, 'watch_vertex'),
							).toEqual([]);
							expect(
								yield* liveIds(writer, 'watch_edge'),
							).toEqual([]);
						}),
					);
				} finally {
					ready();
					await db.close();
				}
			},
		);

		it.skipIf(transport !== 'ws')(
			'closes a scope while its WebSocket is still reconnecting',
			async () => {
				const { db, ready, sockets } = reconnectableSocket();
				try {
					await connectSocket(engine, db);
					const observe = Effect.gen(function* () {
						const next = yield* graphStates(db, source);
						expect(yield* next).toEqual({ _tag: 'Loading' });
						expect(yield* next).toMatchObject({ _tag: 'Current' });
						closeSocket(sockets);
						expect(yield* next).toEqual({ _tag: 'Stale' });
					});
					await within(run(Effect.scoped(observe)), 3_000);
					expect(db.status).toBe('reconnecting');
					expect(
						await run(liveIds(engine.db, 'watch_vertex')),
					).toEqual([]);
					expect(await run(liveIds(engine.db, 'watch_edge'))).toEqual(
						[],
					);
				} finally {
					ready();
					await db.close();
				}
			},
		);
	},
);

it.skipIf(!engineConfigured || transport !== 'http')(
	'watchQuery rejects HTTP without leaving a registration',
	async () => {
		const engine = await startEngine();
		try {
			await engine.db.query(setup).collect();
			const stream = makeClient(engine.db).watchQuery(
				connected,
				{ sources: [vertices] },
				source,
			);
			const error = await run(Effect.flip(Stream.runDrain(stream)));
			expect(error).toBeInstanceOf(StatementUnsupported);
			expect(await run(liveIds(engine.db, 'watch_vertex'))).toEqual([]);
		} finally {
			await engine.stop();
		}
	},
);
