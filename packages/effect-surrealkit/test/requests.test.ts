import { Effect, Schema, Stream } from 'effect';
import { describe, expect, it } from 'vitest';

import { makeWith } from '../src/client/client.js';
import { bind } from '../src/client/index.js';
import type { Transport } from '../src/client/internal/transport.js';
import { LiveStatementTypeId, make } from '../src/statement/index.js';
import { scriptedTransport } from '../src/testing/fake.js';
import { events } from '../src/testing/index.js';

const read = make({
	name: 'read value',
	sql: '$reading;',
	params: { reading: Schema.Number },
	result: Schema.Number,
	writes: false,
});
const readMany = make({
	name: 'read rows',
	sql: '[1, 2];',
	params: {},
	result: Schema.Array(Schema.Number),
	writes: false,
});
const write = make({
	name: 'insert items',
	sql: 'INSERT INTO item $rows RETURN NONE;',
	params: { rows: Schema.Array(Schema.Struct({ n: Schema.Number })) },
	result: Schema.Tuple([]),
	writes: true,
});
const live = {
	[LiveStatementTypeId]: LiveStatementTypeId,
	name: 'item changes',
	sql: 'LIVE SELECT * FROM item;',
	params: {},
	row: Schema.Struct({ n: Schema.Number }),
	id: Schema.String,
} as const;

const counted = (interactive = true) => {
	const { transport } = scriptedTransport({
		[read.name]: (input: Readonly<Record<string, unknown>>) =>
			input.reading,
		[readMany.name]: [1, 2],
		[write.name]: [],
		[live.name]: events([]),
	});
	const { begin, live: open, ...methods } = transport;
	if (begin === undefined || open === undefined) {
		throw new Error('scripted test transport lacks its lifecycle methods');
	}
	const requests: Array<string> = [];
	const report = (request: string) => {
		requests.push(request);
	};
	const wrapped: Transport = {
		...methods,
		collect: (request, signal) => {
			report(request.mode);
			return methods.collect(request, signal);
		},
		send: (request, signal) => {
			report(request.mode);
			return methods.send(request, signal);
		},
		stream: (request, signal) => {
			report('stream');
			return methods.stream(request, signal);
		},
		live: async (request, signal) => {
			report('live');
			const handle = await open(request, signal);
			return {
				messages: handle.messages,
				kill: () => {
					report('kill');
					return handle.kill();
				},
			};
		},
		...(interactive && {
			begin: async (signal) => {
				report('begin');
				const handle = await begin(signal);
				return {
					send: (request, signal) => {
						report('scoped');
						return handle.send(request, signal);
					},
					commit: () => {
						report('commit');
						return handle.commit();
					},
					cancel: () => {
						report('cancel');
						return handle.cancel();
					},
				};
			},
		}),
	};
	return { client: makeWith(wrapped), requests };
};

const values = Array.from({ length: 8 }, (_, value) => value);
const members = values.map((value) => bind(read, { reading: value }));

describe('execution request budgets', () => {
	it('sends one request for run', async () => {
		const { client, requests } = counted();
		expect(await Effect.runPromise(client.run(read, { reading: 1 }))).toBe(
			1,
		);
		expect(requests).toEqual(['query']);
	});

	it('sends one atomic request for eight transaction statements', async () => {
		const { client, requests } = counted();
		expect(await Effect.runPromise(client.transaction(members))).toEqual(
			values,
		);
		expect(requests).toEqual(['atomic']);
	});

	it.each([
		'statement',
		'transaction',
	] as const)('sends one query request for eight pipeline members in %s mode', async (each) => {
		const { client, requests } = counted();
		const results = await Effect.runPromise(
			client.pipeline(members, { each }),
		);
		expect(results).toEqual(
			values.map((success) =>
				expect.objectContaining({ _tag: 'Success', success }),
			),
		);
		expect(requests).toEqual(['query']);
	});

	it('sends one streamed request and emits the rows', async () => {
		const { client, requests } = counted();
		expect(
			await Effect.runPromise(Stream.runCollect(client.stream(readMany))),
		).toEqual([1, 2]);
		expect(requests).toEqual(['stream']);
	});

	it.each([
		{
			label: 'non-atomic',
			interactive: true,
			atomic: false,
			requests: Array.from({ length: 10 }, () => 'query'),
		},
		{
			label: 'interactive atomic',
			interactive: true,
			atomic: true,
			requests: [
				'begin',
				...Array.from({ length: 10 }, () => 'scoped'),
				'commit',
			],
		},
		{
			label: 'single-request atomic fallback',
			interactive: false,
			atomic: true,
			requests: ['atomic'],
		},
	])('sends the budgeted $label requests for 5,000 bulk rows', async (scenario) => {
		const { client, requests } = counted(scenario.interactive);
		const rows = Array.from({ length: 5_000 }, (_, n) => ({ n }));
		const result = await Effect.runPromise(
			client.bulk(
				write,
				{ rows },
				{
					over: 'rows',
					chunk: 500,
					atomic: scenario.atomic,
				},
			),
		);
		expect(result).toMatchObject({ rows: 5_000, chunks: 10 });
		expect(requests).toEqual(scenario.requests);
	});

	it('sends k + 2 requests for k scoped statements', async () => {
		const { client, requests } = counted();
		expect(
			await Effect.runPromise(
				client.withTransaction((tx) =>
					Effect.forEach(values, (value) =>
						tx.run(read, { reading: value }),
					),
				),
			),
		).toEqual(values);
		expect(requests).toEqual([
			'begin',
			...values.map(() => 'scoped'),
			'commit',
		]);
	});

	it('registers the scoped live query once and sends exactly one kill at scope exit', async () => {
		const { client, requests } = counted();
		await Effect.runPromise(
			Effect.scoped(
				Effect.gen(function* () {
					yield* client.liveScoped(live);
					expect(requests).toEqual(['live']);
				}),
			),
		);
		expect(requests).toEqual(['live', 'kill']);
	});
});
