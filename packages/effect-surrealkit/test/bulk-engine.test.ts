import { setTimeout as delay } from 'node:timers/promises';
import { Effect, Schedule, Schema, Stream } from 'effect';
import { CallTerminatedError, RecordId, ServerError } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { makeWith } from '../src/client/client.js';
import {
	BulkWriteError,
	make as makeClient,
	StatementThrown,
	TransactionOutcomeUnknown,
} from '../src/client/index.js';
import { sdkTransport } from '../src/client/internal/transport.js';
import { Int, RecordIdFor } from '../src/index.js';
import {
	insert,
	insertRelation,
	make,
	param,
	updateEach,
} from '../src/statement/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	interactiveTransactions,
	startEngine,
} from './engine.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] bulk engine suite skipped: ${engineSkipReason}`,
	);
}
if (!interactiveTransactions) {
	console.warn(
		'[effect-surrealkit] HTTP uses the single-request atomic bulk fallback; scoped bulk requires interactive transactions',
	);
}

const id = RecordIdFor('bulk_row');
const Row = {
	name: 'bulk_row',
	schemafull: true,
	id,
	select: Schema.Struct({ id, n: Int }),
	create: Schema.Struct({ n: Int }),
	merge: Schema.Struct({ n: Schema.optionalKey(Int) }),
} as const;
const edgeId = RecordIdFor('bulk_edge');
const Edge = {
	name: 'bulk_edge',
	schemafull: true,
	id: edgeId,
	select: Schema.Struct({ id: edgeId, in: id, out: id, weight: Int }),
	create: Schema.Struct({ weight: Int }),
	edge: { table: 'bulk_edge', in: [id], out: [id] },
} as const;

const rowId = (key: string | number) =>
	Schema.decodeUnknownSync(id)(new RecordId('bulk_row', key));
const createRows = Schema.Array(Schema.Struct({ id, n: Int }));
const write = insert(Row, param('rows', createRows))
	.returning('none')
	.build('insert bulk rows');
const replace = insert(Row, param('rows', createRows))
	.onDuplicate((input) => ({ n: input.n }))
	.returning('none')
	.build('upsert bulk rows');
const patches = Schema.Array(
	Schema.Struct({ id, set: Schema.Struct({ n: Int }) }),
);
const applyPatches = updateEach(Row, param('rows', patches)).build();
const skipMissing = updateEach(Row, param('rows', patches), {
	missing: 'ignore',
}).build();
const addEdges = insertRelation(
	Edge,
	param(
		'edges',
		Schema.Array(
			Schema.Struct({
				id: edgeId,
				in: id,
				out: id,
				weight: Int,
			}),
		),
	),
)
	.returning('none')
	.build();

const bump = make({
	name: 'bump counter',
	sql: 'FOR $amount IN $increments { UPDATE ONLY bulk_row:counter SET n += $amount RETURN NONE; };',
	params: { increments: Schema.Array(Int) },
	result: Schema.Undefined,
	writes: true,
});
const failAfterWrite = make({
	name: 'write then fail',
	sql: "FOR $amount IN $increments { UPDATE ONLY bulk_row:counter SET n += $amount RETURN NONE; }; THROW 'after write';",
	params: bump.params,
	result: Schema.Undefined,
	throws: ['after write'],
	slots: 2,
	writes: true,
});

const bumpWithLock = make({
	name: 'bump counter while holding a lock',
	sql: `
		LET $locked = (SELECT * FROM ONLY bulk_row:locked FOR UPDATE);
		FOR $amount IN $increments { UPDATE ONLY bulk_row:counter SET n += $amount RETURN NONE; };
		SLEEP 1s;
	`,
	params: bump.params,
	result: bump.result,
	defines: ['locked'],
	slots: 3,
	writes: true,
});

const failed = <A, E>(effect: Effect.Effect<A, E>) =>
	Effect.runPromise(Effect.flip(effect));

describe.skipIf(!engineConfigured)('bulk writes on a disposable engine', () => {
	let engine: Engine;
	let sdkRetryChecks = 0;

	beforeAll(async () => {
		engine = await startEngine({
			retry: {
				attempts: 2,
				retryDelay: 1,
				retryDelayJitter: 0,
				retryable: (error) => {
					if (
						error instanceof Error &&
						error.message === 'An error occurred: after write'
					) {
						sdkRetryChecks++;
						return true;
					}
					return false;
				},
			},
		});
		console.log(
			`[effect-surrealkit] bulk: ${engine.transport} ${engine.version}`,
		);
		await engine.db.query(`
			DEFINE TABLE bulk_row SCHEMAFULL;
			DEFINE FIELD n ON bulk_row TYPE int ASSERT $value >= 0;
			DEFINE TABLE bulk_edge TYPE RELATION IN bulk_row OUT bulk_row SCHEMAFULL;
			DEFINE FIELD weight ON bulk_edge TYPE int;
		`);
	});
	afterAll(async () => {
		await engine?.stop();
	});

	const reset = () => engine.db.query('DELETE bulk_edge; DELETE bulk_row;');
	const counter = () =>
		engine.db.query('SELECT VALUE n FROM ONLY bulk_row:counter;');
	const counts = () =>
		engine.db.query('count((SELECT VALUE id FROM bulk_row));');

	it('does not create a missing record and returns one empty-list result for updateEach', async () => {
		await reset();
		await engine.db.query('CREATE ONLY bulk_row:before SET n = 0;');
		const raw = await engine.db.query(
			'UPDATE bulk_row:absent MERGE { n: 9 } RETURN NONE;',
		);
		expect(raw).toEqual([[]]);
		expect(
			await engine.db.query('SELECT * FROM ONLY bulk_row:absent;'),
		).toEqual([undefined]);

		const client = makeClient(engine.db);
		const rows = [
			{ id: rowId('before'), set: { n: 7 } },
			{ id: rowId('absent'), set: { n: 9 } },
		];
		expect(
			await Effect.runPromise(client.run(skipMissing, { rows })),
		).toEqual([]);
		expect(
			await engine.db.query('SELECT VALUE n FROM ONLY bulk_row:before;'),
		).toEqual([7]);
		expect(
			await engine.db.query('SELECT * FROM ONLY bulk_row:absent;'),
		).toEqual([undefined]);
		expect(applyPatches.slots).toBe(1);
		expect(applyPatches.sql).toContain('record::exists($row.id)');
		expect(() => updateEach(Row, param('row', patches)).build()).toThrow(
			/reserves the binding name `row`/u,
		);
	});

	it('rolls back earlier rows when a later row throws inside BEGIN and COMMIT', async () => {
		await reset();
		await engine.db.query('CREATE ONLY bulk_row:before SET n = 0;');
		const rows = [
			{ id: rowId('before'), set: { n: 7 } },
			{ id: rowId('absent'), set: { n: 9 } },
		];
		const client = makeClient(engine.db);
		const controlBlock = make({
			name: 'explicit atomic update each',
			sql: `BEGIN; ${applyPatches.sql} COMMIT;`,
			params: applyPatches.params,
			result: applyPatches.result,
			throws: ['Record not found'],
			slots: 3,
			valueSlot: 2,
			transactional: true,
			writes: true,
		});
		const error = await failed(client.run(controlBlock, { rows }));
		expect(error).toBeInstanceOf(StatementThrown);
		expect(error).toMatchObject({ reason: 'Record not found' });
		expect(
			await engine.db.query('SELECT VALUE n FROM ONLY bulk_row:before;'),
		).toEqual([0]);
	});

	it.skipIf(!interactiveTransactions)(
		'uses the existing interactive transaction and rolls back a failed later row',
		async () => {
			await reset();
			await engine.db.query('CREATE ONLY bulk_row:before SET n = 0;');
			const client = makeClient(engine.db);
			const rows = [
				{ id: rowId('before'), set: { n: 7 } },
				{ id: rowId('absent'), set: { n: 9 } },
			];
			const error = await failed(
				client.withTransaction((tx) =>
					tx.bulk(applyPatches, { rows }, { over: 'rows', chunk: 2 }),
				),
			);
			expect(error).toBeInstanceOf(StatementThrown);
			expect(error).toMatchObject({ reason: 'Record not found' });
			expect(
				await engine.db.query(
					'SELECT VALUE n FROM ONLY bulk_row:before;',
				),
			).toEqual([0]);

			const ok = client.withTransaction((tx) =>
				tx.bulk(
					applyPatches,
					{
						rows: [
							{ id: rowId('before'), set: { n: 6 } },
							{ id: rowId('before'), set: { n: 8 } },
						],
					},
					{ over: 'rows', chunk: 1 },
				),
			);
			expect(await Effect.runPromise(ok)).toEqual({
				chunks: 2,
				rows: 2,
				results: [[], []],
			});
			expect(
				await engine.db.query(
					'SELECT VALUE n FROM ONLY bulk_row:before;',
				),
			).toEqual([8]);
		},
	);

	it('rolls back all chunks in atomic mode and reports the preceding committed chunks otherwise', async () => {
		await reset();
		const rows = [1, 2, 3, 4, -5].map((n, index) => ({
			id: rowId(index),
			n,
		}));
		const client = makeClient(engine.db);
		const refused = await failed(
			client.bulk(write, { rows }, { over: 'rows', chunk: 2 }),
		);
		expect(refused).not.toBeInstanceOf(BulkWriteError);
		expect(await counts()).toEqual([0]);

		const nonAtomic = await failed(
			client.bulk(
				write,
				{ rows },
				{ over: 'rows', chunk: 2, atomic: false },
			),
		);
		expect(nonAtomic).toBeInstanceOf(BulkWriteError);
		expect(nonAtomic).toMatchObject({
			chunk: 2,
			chunksCommitted: 2,
			rowsCommitted: 4,
		});
		expect(
			await engine.db.query('SELECT VALUE n FROM bulk_row ORDER BY n;'),
		).toEqual([[1, 2, 3, 4]]);
	});

	it('sends exactly 10 non-atomic requests, 12 interactive or 1 HTTP atomic for 5,000 rows', async () => {
		const rows = Array.from({ length: 5_000 }, (_, n) => ({
			id: rowId(n),
			n,
		}));
		const sdk = sdkTransport(engine.db);
		const begin = sdk.begin;
		for (const atomic of [false, true]) {
			await reset();
			const requests: Array<string> = [];
			let beginCalls = 0;
			const client = makeWith({
				...sdk,
				collect: (request, signal) => {
					requests.push(request.mode);
					return sdk.collect(request, signal);
				},
				...(begin !== undefined && {
					begin: async (signal) => {
						beginCalls++;
						const handle = await begin(signal);
						requests.push('begin');
						return {
							send: (request, signal) => {
								requests.push('scoped query');
								return handle.send(request, signal);
							},
							commit: () => {
								requests.push('commit');
								return handle.commit();
							},
							cancel: () => {
								requests.push('cancel');
								return handle.cancel();
							},
						};
					},
				}),
			});
			const input = atomic
				? Stream.fromIterable(rows.values(), { chunkSize: 500 })
				: rows;
			const results = await Effect.runPromise(
				client.bulk(
					write,
					{ rows: input },
					{ over: 'rows', chunk: 500, atomic },
				),
			);
			expect(results).toEqual({
				chunks: 10,
				rows: 5_000,
				results: Array.from({ length: 10 }, () => []),
			});
			expect(await counts()).toEqual([5_000]);
			if (!atomic) {
				expect(beginCalls).toBe(0);
				expect(requests).toEqual(
					Array.from({ length: 10 }, () => 'query'),
				);
			} else {
				expect(beginCalls).toBe(1);
				expect(requests).toEqual(
					interactiveTransactions
						? [
								'begin',
								...Array.from(
									{ length: 10 },
									() => 'scoped query',
								),
								'commit',
							]
						: ['atomic'],
				);
			}
			console.log(
				`[effect-surrealkit] bulk 5000 ${engine.transport} atomic=${atomic}: ${requests.length} wire requests`,
			);
		}
	});

	it('updates duplicate ids and inserts new ids together, and can insert relations', async () => {
		await reset();
		await engine.db.query('CREATE ONLY bulk_row:existing SET n = 1;');
		const client = makeClient(engine.db);
		const rows = [
			{ id: rowId('existing'), n: 2 },
			{ id: rowId('new'), n: 3 },
		];
		expect(
			await Effect.runPromise(
				client.bulk(replace, { rows }, { over: 'rows' }),
			),
		).toEqual({
			chunks: 1,
			rows: 2,
			results: [[]],
		});
		expect(
			await engine.db.query('SELECT VALUE n FROM bulk_row ORDER BY n;'),
		).toEqual([[2, 3]]);

		const edges = [
			{
				id: Schema.decodeUnknownSync(edgeId)(
					new RecordId('bulk_edge', 'joined'),
				),
				in: rowId('existing'),
				out: rowId('new'),
				weight: 4,
			},
		];
		expect(
			await Effect.runPromise(
				client.bulk(addEdges, { edges }, { over: 'edges' }),
			),
		).toEqual({
			chunks: 1,
			rows: 1,
			results: [[]],
		});
		const [actual] = await engine.db.query(
			'SELECT in, out, weight FROM ONLY bulk_edge:joined;',
		);
		expect(actual).toEqual({
			in: rowId('existing'),
			out: rowId('new'),
			weight: 4,
		});
	});

	it('rolls back an earlier slot when a later slot fails and disables SDK retries', async () => {
		await reset();
		await engine.db.query('CREATE ONLY bulk_row:counter SET n = 0;');
		const error = await failed(
			makeClient(engine.db).bulk(
				failAfterWrite,
				{ increments: [1] },
				{
					over: 'increments',
					atomic: false,
					retry: Schedule.recurs(2),
				},
			),
		);
		expect(error).toBeInstanceOf(BulkWriteError);
		expect(error).toMatchObject({
			cause: { _tag: 'StatementThrown', reason: 'after write' },
		});
		expect(sdkRetryChecks).toBe(0);
		expect(await counter()).toEqual([0]);
	});

	it('keeps a server parse refusal distinct from an unknown atomic outcome', async () => {
		await reset();
		await engine.db.query('CREATE ONLY bulk_row:counter SET n = 0;');
		const invalid = make({
			...bump,
			sql: `${bump.sql}\nSELECT FROM;`,
			slots: 2,
		});
		const error = await failed(
			makeClient(engine.db).bulk(
				invalid,
				{ increments: [1] },
				{
					over: 'increments',
					atomic: false,
					retry: Schedule.recurs(1),
				},
			),
		);
		expect(error).toBeInstanceOf(BulkWriteError);
		expect(error).toMatchObject({
			cause: {
				_tag: 'StatementRequestError',
				cause: expect.any(ServerError),
			},
		});
		expect(await counter()).toEqual([0]);
	});

	it('retries a real chunk conflict once and applies the earlier increment once', async () => {
		await reset();
		await engine.db.query(`
			CREATE ONLY bulk_row:counter SET n = 0;
			CREATE ONLY bulk_row:locked SET n = 0;
		`);
		const sdk = sdkTransport(engine.db);
		let sends = 0;
		const failedAttempts: Array<unknown> = [];
		const observed: Array<unknown> = [];
		const client = makeWith({
			...sdk,
			collect: async (request, signal) => {
				sends++;
				const pending = Promise.resolve(sdk.collect(request, signal));
				if (sends !== 1) return pending;
				const [, result] = await Promise.all([
					(async () => {
						// Change the locked record while the first request sleeps before commit.
						await delay(100);
						await engine.db.query(
							'UPDATE bulk_row:locked SET n += 1;',
						);
					})(),
					pending.catch(async (error: unknown) => {
						failedAttempts.push(error);
						observed.push(await counter());
						throw error;
					}),
				]);
				return result;
			},
		});
		expect(
			await Effect.runPromise(
				client.bulk(
					bumpWithLock,
					{ increments: [2] },
					{
						over: 'increments',
						atomic: false,
						retry: Schedule.recurs(1),
					},
				),
			),
		).toEqual({ chunks: 1, rows: 1, results: [undefined] });
		expect(sends).toBe(2);
		expect(failedAttempts).toEqual([
			expect.objectContaining({ isTransactionConflict: true }),
		]);
		expect(observed).toEqual([[0]]);
		expect(await counter()).toEqual([2]);
	});

	it('never resends a write after dropping the real engine response', async () => {
		const sdk = sdkTransport(engine.db);
		const multiSlotBump = make({
			...bump,
			sql: `${bump.sql}\nNONE;`,
			slots: 2,
			valueSlot: 2,
		});
		for (const [statement, atomic, expected] of [
			[bump, false, BulkWriteError],
			[bump, true, TransactionOutcomeUnknown],
			[multiSlotBump, false, TransactionOutcomeUnknown],
		] as const) {
			await reset();
			await engine.db.query('CREATE ONLY bulk_row:counter SET n = 0;');
			let sends = 0;
			const client = makeWith({
				stream: sdk.stream,
				send: sdk.send,
				collect: async (request, signal) => {
					expect(request.sdkRetry).toBe(false);
					sends++;
					await sdk.collect(request, signal);
					throw new CallTerminatedError();
				},
			});
			const error = await failed(
				client.bulk(
					statement,
					{ increments: [1] },
					{
						over: 'increments',
						atomic,
						retry: Schedule.recurs(2),
					},
				),
			);
			expect(error).toBeInstanceOf(expected);
			expect(sends).toBe(1);
			expect(await counter()).toEqual([1]);
		}

		if (!interactiveTransactions) return;
		const begin = sdk.begin;
		if (begin === undefined) throw new Error('missing interactive begin');
		await reset();
		await engine.db.query('CREATE ONLY bulk_row:counter SET n = 0;');
		let begins = 0;
		let commits = 0;
		let cancels = 0;
		const client = makeWith({
			...sdk,
			begin: async (signal) => {
				begins++;
				const handle = await begin(signal);
				return {
					send: handle.send,
					commit: async () => {
						commits++;
						await handle.commit();
						throw new CallTerminatedError();
					},
					cancel: async () => {
						cancels++;
						await handle.cancel();
					},
				};
			},
		});
		expect(
			await failed(
				client.bulk(
					bump,
					{ increments: [1] },
					{ over: 'increments', retry: Schedule.recurs(2) },
				),
			),
		).toBeInstanceOf(TransactionOutcomeUnknown);
		expect({ begins, commits, cancels }).toEqual({
			begins: 1,
			commits: 1,
			cancels: 0,
		});
		expect(await counter()).toEqual([1]);
	});
});
