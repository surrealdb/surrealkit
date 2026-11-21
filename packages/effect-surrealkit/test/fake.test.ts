import { Effect, Schema } from 'effect';
import { UnsupportedFeatureError } from 'surrealdb';
import { describe, expect, it } from 'vitest';

import { bind } from '../src/client/index.js';
import { join, requestPart } from '../src/client/internal/join.js';
import type { Request } from '../src/client/internal/transport.js';
import { make } from '../src/statement/index.js';
import { scriptedTransport } from '../src/testing/fake.js';
import {
	events,
	failure,
	fake,
	thrown,
	unsupported,
} from '../src/testing/index.js';

const signal = new AbortController().signal;
const read = make({
	name: 'read',
	sql: '$n;',
	params: { n: Schema.Number },
	result: Schema.Number,
});
const fail = make({
	name: 'fail',
	sql: "THROW 'denied';",
	params: {},
	result: Schema.Number,
});

const requests = (mode: Request['mode']): Request => ({
	mode,
	sql: '$n__1; $n__2;',
	bindings: { n__1: 1, n__2: 2 },
	parts: [
		{ ...requestPart(read, { n: 1 }, 0, 1), sent: { n: 'n__1' } },
		{ ...requestPart(read, { n: 2 }, 1, 1), sent: { n: 'n__2' } },
	],
});

const requestFor = (
	statement: typeof read | typeof fail,
	mode: Request['mode'] = 'query',
): Request => {
	const bindings = statement === read ? { n: 3 } : {};
	return {
		mode,
		sql: statement.sql,
		bindings,
		parts: [requestPart(statement, bindings, 0, statement.slots)],
	};
};

const all = async <A>(
	iterable: AsyncIterable<A>,
): Promise<ReadonlyArray<A>> => {
	const values: Array<A> = [];
	for await (const value of iterable) values.push(value);
	return values;
};

describe('scripted execution transport', () => {
	it('uses original bindings for every part even when wire names differ', async () => {
		const { calls, transport } = scriptedTransport({
			read: (bindings: Readonly<Record<string, unknown>>) => bindings.n,
		});
		await expect(
			transport.send(requests('query'), signal),
		).resolves.toEqual([
			{ ok: true, value: 1 },
			{ ok: true, value: 2 },
		]);
		await expect(
			transport.send(requests('atomic'), signal),
		).resolves.toEqual([
			{ ok: true, value: 1 },
			{ ok: true, value: 2 },
		]);
		expect(calls).toEqual([
			{
				statement: 'read',
				bindings: { n: 1 },
				pipeline: 'pipeline(read, read)',
			},
			{
				statement: 'read',
				bindings: { n: 2 },
				pipeline: 'pipeline(read, read)',
			},
			{
				statement: 'read',
				bindings: { n: 1 },
				transaction: 'transaction(read, read)',
			},
			{
				statement: 'read',
				bindings: { n: 2 },
				transaction: 'transaction(read, read)',
			},
		]);
	});

	it('keeps wrapper slots and answers later query parts after a failure', async () => {
		const middle = make({
			name: 'middle',
			sql: 'NONE; $n;',
			params: { n: Schema.Number },
			result: Schema.Number,
			slots: 2,
		});
		const wrapped = await Effect.runPromise(
			join(
				[
					{ statement: middle, bindings: { n: 1 } },
					{ statement: fail, bindings: {} },
					{ statement: read, bindings: { n: 1 } },
				],
				{
					mode: 'query',
					refuseTransactional: true,
					wrap: { before: 'BEGIN;', after: 'COMMIT;' },
				},
			),
		);
		const { transport } = scriptedTransport({
			middle: 1,
			fail: thrown('denied'),
			read: 3,
		});
		const slots = await transport.send(wrapped, signal);
		expect(slots).toHaveLength(10);
		expect(
			wrapped.parts.map(({ offset, slots }) => [offset, slots]),
		).toEqual([
			[0, 4],
			[4, 3],
			[7, 3],
		]);
		expect(slots[2]).toEqual({ ok: true, value: 1 });
		expect(slots[4]).toEqual({ ok: true, value: undefined });
		expect(slots[5]).toMatchObject({
			ok: false,
			error: { kind: 'Thrown', message: 'An error occurred: denied' },
		});
		expect(slots[8]).toEqual({ ok: true, value: 3 });
		expect(slots[9]).toEqual({ ok: true, value: undefined });
	});

	it('rejects an atomic request on the first failure and preserves SDK refusals', async () => {
		const { calls, transport } = scriptedTransport({
			fail: failure('denied'),
			read: 3,
		});
		const failed = requestFor(fail, 'atomic');
		const later = requestFor(read, 'atomic');
		await expect(
			transport.send(
				{
					mode: 'atomic',
					sql: 'THROW "denied"; $n;',
					bindings: later.bindings,
					parts: [
						...failed.parts,
						...later.parts.map((part) => ({ ...part, offset: 1 })),
					],
				},
				signal,
			),
		).rejects.toThrow('denied');
		expect(calls.map(({ statement }) => statement)).toEqual(['fail']);
		await expect(
			transport.send({ ...later, sql: 'RETURN $n; 2;' }, signal),
		).rejects.toThrow(
			/RETURN statement which is followed by another statement/u,
		);
		expect(calls).toHaveLength(1);
	});

	it('uses the collector via the exported fake client', async () => {
		const client = fake({
			read: (bindings: Readonly<Record<string, unknown>>) => bindings.n,
		});
		expect(await Effect.runPromise(client.run(read, { n: 5 }))).toBe(5);
		expect(
			await Effect.runPromise(client.transaction([bind(read, { n: 6 })])),
		).toEqual([6]);
		expect(client.calls).toEqual([
			{ statement: 'read', bindings: { n: 5 } },
			{
				statement: 'read',
				bindings: { n: 6 },
				transaction: 'transaction(read)',
			},
		]);
	});

	it('streams rows from the value slot and preserves the other slots', async () => {
		const statement = make({
			name: 'rows',
			sql: 'NONE; [1, 2]; NONE;',
			params: {},
			result: Schema.Array(Schema.Number),
			slots: 3,
			valueSlot: 2,
		});
		const { transport } = scriptedTransport({ rows: [1, 2] });
		const request = {
			mode: 'query',
			sql: statement.sql,
			bindings: {},
			parts: [requestPart(statement, {}, 0, 3)],
		} satisfies Request;
		expect(await all(transport.stream(request, signal))).toEqual([
			{ kind: 'value', slot: 0, value: undefined },
			{ kind: 'done', slot: 0 },
			{ kind: 'row', slot: 1, value: 1 },
			{ kind: 'row', slot: 1, value: 2 },
			{ kind: 'done', slot: 1 },
			{ kind: 'value', slot: 2, value: undefined },
			{ kind: 'done', slot: 2 },
		]);
	});

	it('records interactive sends, commits and cancellations', async () => {
		const { calls, transport } = scriptedTransport({ read: 3 });
		const first = await transport.begin?.(signal);
		await expect(first?.send(requestFor(read), signal)).resolves.toEqual([
			{ ok: true, value: 3 },
		]);
		await first?.commit();
		const second = await transport.begin?.(signal);
		await second?.cancel();
		expect(calls).toEqual([
			{
				statement: 'begin',
				bindings: {},
				operation: 'begin',
				interactive: 'transaction 1',
			},
			{
				statement: 'read',
				bindings: { n: 3 },
				interactive: 'transaction 1',
			},
			{
				statement: 'commit',
				bindings: {},
				operation: 'commit',
				interactive: 'transaction 1',
			},
			{
				statement: 'begin',
				bindings: {},
				operation: 'begin',
				interactive: 'transaction 2',
			},
			{
				statement: 'cancel',
				bindings: {},
				operation: 'cancel',
				interactive: 'transaction 2',
			},
		]);
	});

	it('delivers scripted events and records kill', async () => {
		const messages = [
			{ action: 'CREATE', id: 1, value: { n: 1 } },
			{ action: 'UPDATE', id: 1, value: { n: 2 } },
			{ action: 'DELETE', id: 1, value: { n: 2 } },
			{ action: 'KILLED' },
		] as const;
		const { calls, transport } = scriptedTransport({
			read: events(messages),
		});
		const handle = await transport.live?.(requestFor(read), signal);
		if (handle === undefined)
			throw new Error('missing scripted live query');
		expect(await all(handle.messages)).toEqual(messages);
		await handle.kill();
		expect(calls.at(-1)).toEqual({
			statement: 'read',
			bindings: {},
			operation: 'kill',
		});
	});

	it('scripts SDK feature refusals for begin, live and stream', async () => {
		const { transport: interactive } = scriptedTransport({
			begin: unsupported('interactive transaction'),
		});
		await expect(interactive.begin?.(signal)).rejects.toBeInstanceOf(
			UnsupportedFeatureError,
		);
		const { transport: live } = scriptedTransport({
			read: unsupported('live query'),
		});
		await expect(
			live.live?.(requestFor(read), signal),
		).rejects.toBeInstanceOf(UnsupportedFeatureError);
		const { transport: streamed } = scriptedTransport({
			read: unsupported('stream'),
		});
		await expect(
			all(streamed.stream(requestFor(read), signal)),
		).rejects.toBeInstanceOf(UnsupportedFeatureError);
	});
});
