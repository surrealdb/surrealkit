import { Effect, Result, Schema, Stream } from 'effect';
import { DoneFrame, ValueFrame } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { makeWith } from '../src/client/client.js';
import {
	make as makeClient,
	StatementDecodeError,
	StatementThrown,
	timed,
} from '../src/client/index.js';
import { type Signal, sdkTransport } from '../src/client/internal/transport.js';
import { make } from '../src/statement/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] stream engine suite skipped: ${engineSkipReason}`,
	);
}

const read = make({
	name: 'ordered stream rows',
	sql: 'SELECT n FROM stream_rows ORDER BY n;',
	params: {},
	result: Schema.Array(Schema.Struct({ n: Schema.Number })),
	writes: false,
});
const ordered = [{ n: 1 }, { n: 2 }, { n: 3 }];
const run = <A, E>(effect: Effect.Effect<A, E>) =>
	within(Effect.runPromise(effect), 20_000);

describe.skipIf(!engineConfigured)('streams on a disposable engine', () => {
	let engine: Engine;
	beforeAll(async () => {
		engine = await startEngine();
		await engine.db.query(`
			DEFINE TABLE stream_rows;
			CREATE stream_rows:one SET n = 1, pair = [1, 2];
			CREATE stream_rows:two SET n = 2, pair = [3, 4];
			CREATE stream_rows:three SET n = 3, pair = [5, 6];
			DEFINE TABLE stream_writes;
			CREATE stream_writes:one SET n = 7;
		`);
		console.info(
			`[effect-surrealkit] stream: ${engine.transport} engine ${engine.version}`,
		);
	});
	afterAll(async () => engine?.stop());

	it('V6.1: the SDK delivers one value frame per record and one terminal slot with statistics', async () => {
		const rows: Array<unknown> = [];
		const done: Array<DoneFrame<unknown, false>> = [];
		for await (const frame of engine.db.query(read.sql).stream()) {
			expect(frame.query).toBe(0);
			if (frame instanceof ValueFrame) {
				expect(frame.isSingle).toBe(false);
				rows.push(frame.value);
			} else {
				expect(frame).toBeInstanceOf(DoneFrame);
				if (frame instanceof DoneFrame) done.push(frame);
			}
		}
		expect(rows).toEqual(ordered);
		expect(done).toHaveLength(1);
		expect(done[0]?.stats?.duration).toBeDefined();
		const client = makeClient(engine.db);
		const [value, timings] = await run(
			timed(Stream.runCollect(client.stream(read))),
		);
		expect(value).toEqual(ordered);
		expect(timings).toMatchObject([
			{
				kind: 'stream',
				name: read.name,
				sends: 1,
				server: expect.anything(),
				statements: [
					{
						statement: read.name,
						rows: 3,
						server: expect.anything(),
					},
				],
			},
		]);
	});

	it('V6.2: taking the first row aborts and consumes no more frames; the connection serves the next query', async () => {
		const transport = sdkTransport(engine.db);
		let sent = 0;
		let consumed = 0;
		let closed = 0;
		let signal: Signal | undefined;
		const client = makeWith({
			...transport,
			stream: async function* (request, abort) {
				sent++;
				signal = abort;
				try {
					for await (const frame of transport.stream(
						request,
						abort,
					)) {
						consumed++;
						yield frame;
					}
				} finally {
					closed++;
				}
			},
		});
		const [first, timings] = await run(
			timed(Stream.runCollect(client.stream(read).pipe(Stream.take(1)))),
		);
		expect(first).toEqual([ordered[0]]);
		expect(signal?.aborted).toBe(true);
		expect({ sent, consumed, closed }).toEqual({
			sent: 1,
			consumed: 1,
			closed: 1,
		});
		expect(timings).toMatchObject([
			{
				kind: 'stream',
				sends: 1,
				statements: [{ statement: read.name, rows: 1 }],
			},
		]);
		expect(await run(client.run(read))).toEqual(ordered);
	});

	it('streams array-valued rows as whole values and unpacks an array-valued scalar result', async () => {
		const client = makeClient(engine.db);
		const pairs = Schema.Array(
			Schema.Tuple([Schema.Number, Schema.Number]),
		);
		const listOfRows = make({
			...read,
			sql: 'SELECT VALUE pair FROM stream_rows ORDER BY pair;',
			result: pairs,
		});
		const scalarList = make({
			...read,
			sql: 'RETURN [[1, 2], [3, 4], [5, 6]];',
			result: pairs,
		});
		for (const statement of [listOfRows, scalarList]) {
			expect(
				await run(Stream.runCollect(client.stream(statement))),
			).toEqual(await run(client.run(statement)));
		}
	});

	it('uses the declared value slot and accepts NONE before and after it', async () => {
		const statement = make({
			...read,
			sql: `NONE; ${read.sql} NONE;`,
			slots: 3,
			valueSlot: 2,
		});
		expect(
			await run(
				Stream.runCollect(makeClient(engine.db).stream(statement)),
			),
		).toEqual(ordered);
	});

	it.each([
		'[];',
		'SELECT n FROM stream_rows WHERE false;',
		'NULL;',
	])('refuses a non-value slot containing %s, with the same error as run', async (sql) => {
		const statement = make({
			...read,
			sql: `${sql} ${read.sql}`,
			slots: 2,
			valueSlot: 2,
		});
		const client = makeClient(engine.db);
		const failure = await run(
			Effect.flip(Stream.runCollect(client.stream(statement))),
		);
		const collected = await run(Effect.flip(client.run(statement)));
		expect(failure).toBeInstanceOf(StatementDecodeError);
		expect(failure).toMatchObject({
			_tag: collected._tag,
			message: collected.message,
			slot: 1,
			committed: false,
		});
	});

	it('keeps previously emitted rows when a later slot throws', async () => {
		const statement = make({
			...read,
			sql: `${read.sql} THROW 'stream ended';`,
			slots: 2,
			valueSlot: 1,
			throws: ['stream ended'],
		});
		const seen: Array<{ readonly n: number }> = [];
		const result = await run(
			Effect.result(
				Stream.runForEach(
					makeClient(engine.db).stream(statement),
					(row) =>
						Effect.sync(() => {
							seen.push(row);
						}),
				),
			),
		);
		expect(seen).toEqual(ordered);
		expect(Result.isFailure(result) && result.failure).toBeInstanceOf(
			StatementThrown,
		);
	});

	it('allows an empty row set and returns rows from a write', async () => {
		const client = makeClient(engine.db);
		const empty = make({
			...read,
			sql: 'SELECT n FROM stream_rows WHERE false;',
		});
		const write = make({
			...read,
			sql: 'UPDATE stream_writes:one SET n += 1 RETURN n;',
			writes: true,
			nonEmpty: true,
		});
		expect(await run(Stream.runCollect(client.stream(empty)))).toEqual([]);
		expect(await run(Stream.runCollect(client.stream(write)))).toEqual([
			{ n: 8 },
		]);
		expect(
			await engine.db.query(
				'SELECT VALUE n FROM ONLY stream_writes:one;',
			),
		).toEqual([8]);
	});
});
