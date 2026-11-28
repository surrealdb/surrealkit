import {
	Cause,
	Deferred,
	Effect,
	Exit,
	Fiber,
	Schedule,
	Schema,
	Stream,
} from 'effect';
import {
	CallTerminatedError,
	CborCodec,
	ConnectionUnavailableError,
	ExpressionError,
	Surreal,
} from 'surrealdb';
import { describe, expect, it } from 'vitest';

import { makeWith } from '../src/client/client.js';
import {
	BulkWriteError,
	make as makeClient,
	StatementConflict,
	StatementRequestError,
	StatementScopeError,
	StatementUnsupported,
	StatementWriteOutcomeError,
	SurrealClient,
	TransactionOutcomeUnknown,
} from '../src/client/index.js';
import type { Request, Transport } from '../src/client/internal/transport.js';
import { make } from '../src/statement/index.js';
import { scriptedTransport } from '../src/testing/fake.js';
import { conflict, failure, fake, unsupported } from '../src/testing/index.js';

const list = make({
	name: 'numbers',
	sql: '$rows;',
	params: { rows: Schema.Array(Schema.Number), label: Schema.String },
	result: Schema.Array(Schema.Number),
	writes: true,
});

const multiSlot = make({
	...list,
	sql: 'LET $copy = $rows; $copy;',
	defines: ['copy'],
	slots: 2,
	valueSlot: 2,
});

const scopeViolation = make({
	name: 'nested control',
	sql: 'BEGIN; $rows; COMMIT;',
	params: list.params,
	result: list.result,
	slots: 3,
	valueSlot: 2,
	transactional: true,
});

const collectRows = (vars: Readonly<Record<string, unknown>>) => vars.rows;
const flip = <A, E>(effect: Effect.Effect<A, E>) =>
	Effect.runPromise(Effect.flip(effect));
const values = [1, 2, 3, 4, 5];
const input = { rows: values, label: 'all' };

const cborResponse = (body: unknown) =>
	new Response(Uint8Array.from(CborCodec.DEFAULT.encode(body)));

const httpClient = async (reply: (id: string) => Response) => {
	const queries: Array<string> = [];
	const db = new Surreal({
		fetchImpl: async (_url, init) => {
			if (!(init?.body instanceof Uint8Array))
				throw new Error('expected an HTTP RPC body');
			const rpc = CborCodec.DEFAULT.decode<{
				id: string;
				method: string;
				params: [string];
			}>(init.body);
			if (rpc.method === 'version') {
				return cborResponse({ id: rpc.id, result: 'surrealdb-3.3.4' });
			}
			expect(rpc.method).toBe('query');
			queries.push(rpc.params[0]);
			return reply(rpc.id);
		},
	});
	await db.connect('http://surreal.invalid/rpc', {
		namespace: 'bulk',
		database: 'bulk',
		reconnect: false,
	});
	return { client: makeClient(db), queries, close: () => db.close() };
};

describe('bulk writes', () => {
	it('keeps binding names, row order, the short chunk and confirmed progress through the Client service', async () => {
		const client = fake({ numbers: collectRows });
		const callbacks: Array<unknown> = [];
		const program = Effect.gen(function* () {
			const service = yield* SurrealClient;
			return yield* service.bulk(list, input, {
				over: 'rows',
				chunk: 2,
				atomic: false,
				beforeChunk: (chunk) =>
					Effect.sync(() => {
						callbacks.push(chunk);
					}),
				onProgress: (progress) =>
					Effect.sync(() => {
						callbacks.push(progress);
					}),
			});
		});
		expect(
			await Effect.runPromise(
				program.pipe(Effect.provideService(SurrealClient, client)),
			),
		).toEqual({
			chunks: 3,
			rows: 5,
			results: [[1, 2], [3, 4], [5]],
		});
		expect(client.calls.map(({ bindings }) => bindings)).toEqual([
			{ rows: [1, 2], label: 'all' },
			{ rows: [3, 4], label: 'all' },
			{ rows: [5], label: 'all' },
		]);
		expect(callbacks).toEqual([
			{ index: 0, rows: [1, 2] },
			{ chunks: 1, rows: 2, total: 5 },
			{ index: 1, rows: [3, 4] },
			{ chunks: 2, rows: 4, total: 5 },
			{ index: 2, rows: [5] },
			{ chunks: 3, rows: 5, total: 5 },
		]);
	});

	it('sends nothing for empty arrays and streams, even by default atomic', async () => {
		const client = fake({});
		for (const rows of [[], Stream.empty]) {
			expect(
				await Effect.runPromise(
					client.bulk(
						list,
						{ rows, label: 'empty' },
						{
							over: 'rows',
							beforeChunk: () => Effect.die('must not prepare'),
							onProgress: () => Effect.die('must not report'),
						},
					),
				),
			).toEqual({ chunks: 0, rows: 0, results: [] });
		}
		expect(client.calls).toEqual([]);
	});

	it('refuses transaction controls and invalid chunk sizes without pulling the source', async () => {
		const client = fake({});
		const source = Stream.fromEffect(Effect.die('must not pull'));
		const supplied = { rows: source, label: 'invalid' };
		expect(
			await flip(client.bulk(scopeViolation, supplied, { over: 'rows' })),
		).toBeInstanceOf(StatementScopeError);
		for (const chunk of [
			0,
			-1,
			1.5,
			Number.NaN,
			Number.POSITIVE_INFINITY,
		]) {
			const exit = await Effect.runPromise(
				Effect.exit(
					client.bulk(list, supplied, { over: 'rows', chunk }),
				),
			);
			expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(
				true,
			);
			if (Exit.isFailure(exit))
				expect(Cause.findDefect(exit.cause)).toMatchObject({
					success: expect.objectContaining({
						message: 'bulk chunk size must be a positive integer',
					}),
				});
		}
		expect(client.calls).toEqual([]);
	});

	it('closes a one-shot iterable when a beforeChunk callback stops the write', async () => {
		const client = fake({});
		let closed = false;
		function* produce() {
			try {
				yield* values;
			} finally {
				closed = true;
			}
		}
		const stopped = new Error('beforeChunk stopped');
		expect(
			await flip(
				client.bulk(
					list,
					{ rows: produce(), label: 'all' },
					{
						over: 'rows',
						chunk: 2,
						atomic: false,
						beforeChunk: () => Effect.fail(stopped),
					},
				),
			),
		).toBe(stopped);
		expect(closed).toBe(true);
		expect(client.calls).toEqual([]);
	});

	it('returns a beforeChunk failure unchanged and does not retry it as a database conflict', async () => {
		const client = fake({ numbers: collectRows });
		const stopped = new StatementConflict({
			statement: 'user hook',
			message: 'stop',
			cause: undefined,
		});
		const error = await flip(
			client.bulk(list, input, {
				over: 'rows',
				chunk: 2,
				retry: Schedule.recurs(3),
				beforeChunk: ({ index }) =>
					index === 1 ? Effect.fail(stopped) : Effect.void,
			}),
		);
		expect(error).toBe(stopped);
		expect(client.calls.map(({ statement }) => statement)).toEqual([
			'begin',
			'numbers',
			'cancel',
		]);
	});

	it('returns a producer failure unchanged even if it has the database conflict tag', async () => {
		const client = fake({ numbers: collectRows });
		const stopped = new StatementConflict({
			statement: 'producer',
			message: 'stop',
			cause: undefined,
		});
		const rows = Stream.concat(Stream.make(1, 2), Stream.fail(stopped));
		expect(
			await flip(
				client.bulk(
					list,
					{ rows, label: 'all' },
					{ over: 'rows', chunk: 2, retry: Schedule.recurs(3) },
				),
			),
		).toBe(stopped);
		expect(client.calls.map(({ statement }) => statement)).toEqual([
			'begin',
			'numbers',
			'cancel',
		]);
	});

	it('pulls the next stream chunk only after the previous one is acknowledged', async () => {
		const seen: Array<number> = [];
		const progress: Array<number> = [];
		await Effect.runPromise(
			Effect.gen(function* () {
				const waiting = yield* Deferred.make<void>();
				const reply = yield* Deferred.make<void>();
				const { transport } = scriptedTransport({
					numbers: collectRows,
				});
				let calls = 0;
				const client = makeWith({
					...transport,
					collect: async (request, signal) => {
						if (++calls === 1)
							await Effect.runPromise(
								Effect.andThen(
									Deferred.succeed(waiting, undefined),
									Deferred.await(reply),
								),
							);
						return transport.collect(request, signal);
					},
				});
				const rows = Stream.fromIterable(values, { chunkSize: 1 }).pipe(
					Stream.mapEffect((row) =>
						Effect.sync(() => {
							seen.push(row);
							return row;
						}),
					),
				);
				const fiber = yield* Effect.forkChild(
					client.bulk(
						list,
						{ rows, label: 'all' },
						{
							over: 'rows',
							chunk: 2,
							atomic: false,
							onProgress: ({ rows, total }) =>
								Effect.sync(() => {
									expect(total).toBeUndefined();
									progress.push(rows);
								}),
						},
					),
				);
				yield* Deferred.await(waiting);
				expect(seen).toEqual([1, 2]);
				expect(progress).toEqual([]);
				yield* Deferred.succeed(reply, undefined);
				expect(yield* Fiber.join(fiber)).toEqual({
					chunks: 3,
					rows: 5,
					results: [[1, 2], [3, 4], [5]],
				});
			}),
		);
		expect(seen).toEqual(values);
		expect(progress).toEqual([2, 4, 5]);
	});

	it('retries only the conflicting non-atomic chunk and never re-pulls its rows', async () => {
		let sends = 0;
		const client = fake({
			numbers: (vars: Readonly<Record<string, unknown>>) =>
				++sends === 2 ? conflict() : vars.rows,
		});
		const rows = values.values();
		expect(
			await Effect.runPromise(
				client.bulk(
					list,
					{ rows, label: 'all' },
					{
						over: 'rows',
						chunk: 2,
						atomic: false,
						retry: Schedule.recurs(1),
					},
				),
			),
		).toEqual({ chunks: 3, rows: 5, results: [[1, 2], [3, 4], [5]] });
		expect(client.calls.map(({ bindings }) => bindings.rows)).toEqual([
			[1, 2],
			[3, 4],
			[3, 4],
			[5],
		]);
	});

	it('retries a multi-slot chunk in one atomic request using the same encoded chunk', async () => {
		let sends = 0;
		const { transport } = scriptedTransport({
			numbers: (vars: Readonly<Record<string, unknown>>) =>
				++sends === 2 ? conflict() : vars.rows,
		});
		const requests: Array<Request> = [];
		const client = makeWith({
			...transport,
			collect: (request, signal) => {
				requests.push(request);
				return transport.collect(request, signal);
			},
		});
		expect(
			await Effect.runPromise(
				client.bulk(multiSlot, input, {
					over: 'rows',
					chunk: 2,
					atomic: false,
					retry: Schedule.recurs(1),
				}),
			),
		).toEqual({ chunks: 3, rows: 5, results: [[1, 2], [3, 4], [5]] });
		expect(requests.map(({ mode }) => mode)).toEqual([
			'atomic',
			'atomic',
			'atomic',
			'atomic',
		]);
		expect(requests[2]).toBe(requests[1]);
		expect(requests.map(({ bindings }) => bindings.rows)).toEqual([
			[1, 2],
			[3, 4],
			[3, 4],
			[5],
		]);
	});

	it('replays all original rows in a fresh transaction without re-acquiring a one-shot stream', async () => {
		let commits = 0;
		const client = fake({
			numbers: collectRows,
			commit: () => (++commits === 1 ? conflict() : undefined),
		});
		const reads: Array<number> = [];
		const oneShot = values.values();
		const rows = Stream.fromIterable(oneShot, { chunkSize: 1 }).pipe(
			Stream.mapEffect((value) =>
				Effect.sync(() => {
					reads.push(value);
					return value;
				}),
			),
		);
		expect(
			await Effect.runPromise(
				client.bulk(
					list,
					{ rows, label: 'all' },
					{
						over: 'rows',
						chunk: 2,
						retry: Schedule.recurs(1),
					},
				),
			),
		).toEqual({ chunks: 3, rows: 5, results: [[1, 2], [3, 4], [5]] });
		expect(reads).toEqual(values);
		expect(
			client.calls
				.filter(({ statement }) => statement === 'numbers')
				.map(({ interactive, bindings }) => [
					interactive,
					bindings.rows,
				]),
		).toEqual([
			['transaction 1', [1, 2]],
			['transaction 1', [3, 4]],
			['transaction 1', [5]],
			['transaction 2', [1, 2]],
			['transaction 2', [3, 4]],
			['transaction 2', [5]],
		]);
	});

	it('replays a prefix then consumes the unread tail when a transaction fails early', async () => {
		let sends = 0;
		const client = fake({
			numbers: (vars: Readonly<Record<string, unknown>>) =>
				++sends === 2 ? conflict() : vars.rows,
		});
		const rows = values.values();
		expect(
			await Effect.runPromise(
				client.bulk(
					list,
					{ rows, label: 'all' },
					{
						over: 'rows',
						chunk: 2,
						retry: Schedule.recurs(1),
					},
				),
			),
		).toEqual({ chunks: 3, rows: 5, results: [[1, 2], [3, 4], [5]] });
		expect(
			client.calls
				.filter(({ statement }) => statement === 'numbers')
				.map(({ bindings }) => bindings.rows),
		).toEqual([[1, 2], [3, 4], [1, 2], [3, 4], [5]]);
	});

	it('falls back only when begin itself was refused, keeping the peeked prefix', async () => {
		const callbacks: Array<string> = [];
		const client = fake({
			begin: unsupported('interactive transaction'),
			numbers: collectRows,
		});
		const oneShot = values.values();
		const rows = Stream.fromIterable(oneShot, { chunkSize: 1 }).pipe(
			Stream.mapEffect((value) =>
				Effect.sync(() => {
					callbacks.push(`read ${value}`);
					return value;
				}),
			),
		);
		expect(
			await Effect.runPromise(
				client.bulk(
					list,
					{ rows, label: 'all' },
					{
						over: 'rows',
						chunk: 2,
						beforeChunk: ({ index }) =>
							Effect.sync(() => {
								expect(client.calls[0]?.operation).toBe(
									'begin',
								);
								callbacks.push(`before ${index}`);
							}),
					},
				),
			),
		).toEqual({ chunks: 3, rows: 5, results: [[1, 2], [3, 4], [5]] });
		expect(callbacks).toEqual([
			'read 1',
			'read 2',
			'before 0',
			'read 3',
			'read 4',
			'before 1',
			'read 5',
			'before 2',
		]);
		expect(
			client.calls
				.filter(({ statement }) => statement === 'numbers')
				.map(({ transaction }) => transaction),
		).toEqual([
			'transaction(numbers, numbers, numbers)',
			'transaction(numbers, numbers, numbers)',
			'transaction(numbers, numbers, numbers)',
		]);

		const unexpected = new StatementUnsupported({
			feature: 'interactive transaction',
			message: 'user hook',
		});
		const running = fake({ numbers: collectRows });
		expect(
			await flip(
				running.bulk(list, input, {
					over: 'rows',
					beforeChunk: () => Effect.fail(unexpected),
				}),
			),
		).toBe(unexpected);
		expect(running.calls.map(({ statement }) => statement)).toEqual([
			'begin',
			'cancel',
		]);
	});

	it('classifies a confirmed decode failure separately from a dropped non-atomic reply without replay', async () => {
		for (const lost of [false, true]) {
			const { transport } = scriptedTransport({ numbers: collectRows });
			const sent: Array<Request> = [];
			const client = makeWith({
				...transport,
				collect: async (request, signal) => {
					sent.push(request);
					if (sent.length === 3) {
						if (lost) throw new CallTerminatedError();
						return [false];
					}
					return transport.collect(request, signal);
				},
			});
			const error = await flip(
				client.bulk(list, input, {
					over: 'rows',
					chunk: 2,
					atomic: false,
					retry: Schedule.recurs(2),
				}),
			);
			expect(error).toBeInstanceOf(BulkWriteError);
			expect(error).toMatchObject({
				chunk: 2,
				chunksCommitted: 2,
				rowsCommitted: 4,
				cause: lost
					? { cause: expect.any(CallTerminatedError) }
					: expect.any(StatementWriteOutcomeError),
			});
			expect(sent).toHaveLength(3);
			expect(sent.map(({ sdkRetry }) => sdkRetry)).toEqual([
				false,
				false,
				false,
			]);
		}
	});

	it('reports unknown interactive and atomic-batch commits without replay', async () => {
		const lost = new CallTerminatedError();
		const lifecycle: Array<string> = [];
		let batches = 0;
		const scripted = scriptedTransport({ numbers: collectRows }).transport;
		const begin = scripted.begin;
		if (begin === undefined) throw new Error('missing scripted begin');
		const interactive = makeWith({
			...scripted,
			begin: async (signal) => {
				lifecycle.push('begin');
				const handle = await begin(signal);
				return {
					send: handle.send,
					cancel: async () => {
						lifecycle.push('cancel');
					},
					commit: async () => {
						lifecycle.push('commit');
						throw lost;
					},
				};
			},
		});
		const batch = makeWith({
			stream: scripted.stream,
			send: scripted.send,
			collect: async (request: Request) => {
				expect(request.mode).toBe('atomic');
				expect(request.sdkRetry).toBe(false);
				batches++;
				throw lost;
			},
		} satisfies Transport);
		for (const client of [interactive, batch]) {
			const error = await flip(
				client.bulk(list, input, {
					over: 'rows',
					chunk: 2,
					retry: Schedule.recurs(2),
				}),
			);
			expect(error).toBeInstanceOf(TransactionOutcomeUnknown);
			expect(error).toMatchObject({ cause: lost });
		}
		expect(lifecycle).toEqual(['begin', 'commit']);
		expect(batches).toBe(1);
	});

	it.each([
		{
			scenario: 'a terminated response body',
			cause: new TypeError('terminated'),
			atomic: true,
		},
		{
			scenario: 'browser fetch',
			cause: new TypeError(
				'NetworkError when attempting to fetch resource.',
			),
			beforeResponse: true,
			atomic: false,
		},
		{
			scenario: 'a response body with a network DOMException',
			cause: new DOMException('transport failed', 'NetworkError'),
			atomic: true,
		},
		{
			scenario: 'a response body with a socket reset',
			cause: Object.assign(new Error('socket hang up'), {
				code: 'ECONNRESET',
			}),
			atomic: false,
		},
	])('reports an unknown HTTP atomic acknowledgement when $scenario fails', async ({
		cause,
		beforeResponse,
		atomic,
	}) => {
		const { client, queries, close } = await httpClient(() => {
			if (beforeResponse) throw cause;
			return new Response(
				new ReadableStream({
					pull: (controller) => controller.error(cause),
				}),
			);
		});
		try {
			const error = await flip(
				client.bulk(multiSlot, input, {
					over: 'rows',
					atomic,
					retry: Schedule.recurs(2),
				}),
			);
			expect(queries).toEqual([
				expect.stringMatching(/^BEGIN;[\s\S]+COMMIT;$/u),
			]);
			expect(error).toBeInstanceOf(TransactionOutcomeUnknown);
			expect(error).toMatchObject({ cause });
		} finally {
			await close();
		}
	});

	it('keeps HTTP atomic rejections distinct from unknown outcomes', async () => {
		const { client, queries, close } = await httpClient((id) =>
			cborResponse({
				id,
				error: { code: -32002, message: 'query denied' },
			}),
		);
		try {
			const scripted = fake({ numbers: collectRows });
			for (const [sql, keyword] of [
				['RETURN $rows; NONE;', 'RETURN'],
				['BEGIN; $rows;', 'BEGIN'],
			] as const) {
				const statement = make({ ...multiSlot, sql, defines: [] });
				for (const candidate of [client, scripted]) {
					const preflight = await flip(
						candidate.bulk(statement, input, {
							over: 'rows',
							atomic: false,
						}),
					);
					expect(preflight).toBeInstanceOf(BulkWriteError);
					expect(preflight).toMatchObject({
						cause: {
							_tag: 'StatementRequestError',
							cause: expect.any(ExpressionError),
							message: expect.stringContaining(
								`contains a ${keyword} statement`,
							),
						},
					});
				}
			}
			expect(scripted.calls).toHaveLength(0);
			expect(queries).toHaveLength(0);
			const rejected = await flip(
				client.bulk(multiSlot, input, { over: 'rows' }),
			);
			expect(rejected).toBeInstanceOf(StatementRequestError);
			expect(rejected).toMatchObject({ cause: { kind: 'NotAllowed' } });
			expect(queries).toHaveLength(1);
		} finally {
			await close();
		}
	});

	it('keeps an SDK refusal before connecting distinct from an unknown atomic outcome', async () => {
		const client = makeClient(new Surreal());
		const rejected = await flip(
			client.bulk(multiSlot, input, { over: 'rows', atomic: false }),
		);
		expect(rejected).toBeInstanceOf(BulkWriteError);
		expect(rejected).toMatchObject({
			cause: {
				_tag: 'StatementRequestError',
				cause: expect.any(ConnectionUnavailableError),
			},
		});
	});

	it('uses the current transaction for tx.bulk without another lifecycle or chunk retry', async () => {
		const client = fake({ numbers: collectRows });
		expect(
			await Effect.runPromise(
				client.withTransaction((tx) =>
					tx.bulk(multiSlot, input, {
						over: 'rows',
						chunk: 2,
					}),
				),
			),
		).toEqual({ chunks: 3, rows: 5, results: [[1, 2], [3, 4], [5]] });
		expect(
			client.calls.map(({ statement, interactive }) => [
				statement,
				interactive,
			]),
		).toEqual([
			['begin', 'transaction 1'],
			['numbers', 'transaction 1'],
			['numbers', 'transaction 1'],
			['numbers', 'transaction 1'],
			['commit', 'transaction 1'],
		]);

		const conflicted = fake({ numbers: conflict() });
		expect(
			await flip(
				conflicted.withTransaction((tx) =>
					tx.bulk(multiSlot, input, { over: 'rows', chunk: 2 }),
				),
			),
		).toBeInstanceOf(StatementConflict);
		expect(conflicted.calls.map(({ statement }) => statement)).toEqual([
			'begin',
			'numbers',
			'cancel',
		]);
	});

	it('keeps confirmed counts when an ordinary non-atomic request fails', async () => {
		let sent = 0;
		const client = fake({
			numbers: (bindings: Readonly<Record<string, unknown>>) =>
				++sent === 3 ? failure('write refused') : bindings.rows,
		});
		const error = await flip(
			client.bulk(list, input, { over: 'rows', chunk: 2, atomic: false }),
		);
		expect(error).toBeInstanceOf(BulkWriteError);
		expect(error).toMatchObject({
			chunk: 2,
			rowsCommitted: 4,
			chunksCommitted: 2,
			cause: {
				message: 'SurrealDB request failed',
				cause: { message: 'write refused' },
			},
		});
		expect(JSON.stringify(error)).not.toContain('write refused');
	});
});
