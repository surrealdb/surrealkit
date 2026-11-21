import { Effect, Schema } from 'effect';
import { afterEach, expect, it, vi } from 'vitest';

import { bind, makeWith } from '../src/client/client.js';
import * as tracing from '../src/client/internal/trace.js';
import type { Transport } from '../src/client/internal/transport.js';
import { make } from '../src/statement/index.js';

afterEach(() => vi.restoreAllMocks());

const collecting = (...results: ReadonlyArray<unknown>) => {
	const collect = vi.fn<Transport['collect']>().mockResolvedValue(results);
	const client = makeWith({
		collect,
		send: () => Promise.reject(new Error('expected collect')),
		stream: () => {
			throw new Error('expected collect');
		},
	});
	return { client, collect };
};

const recordTimings = () => {
	const record = vi.fn();
	vi.spyOn(tracing, 'recording').mockImplementation((_kind, _name, use) =>
		use({
			encode: (effect) => effect,
			wait: (effect) => effect,
			decode: (effect) => effect,
			server: (name, duration) =>
				Effect.sync(() => record('server', name, duration)),
			rows: (name, count) =>
				Effect.sync(() => record('rows', name, count)),
		}),
	);
	return record;
};

const scalar = make({
	name: 'read, value',
	sql: '1;',
	params: {},
	result: Schema.Number,
});

it('records the original name of a scalar run without a server duration', async () => {
	const record = recordTimings();
	const { client } = collecting(1);

	await expect(Effect.runPromise(client.run(scalar))).resolves.toBe(1);
	expect(record.mock.calls).toEqual([['server', 'read, value', undefined]]);
});

it('records each transaction part in order, including duplicate names and scalars', async () => {
	const record = recordTimings();
	const list = make({
		name: 'read, value',
		sql: '[1, 2];',
		params: {},
		result: Schema.Array(Schema.Number),
	});
	const { client } = collecting(1, [1, 2], 1);

	await expect(
		Effect.runPromise(
			client.transaction([bind(scalar), bind(list), bind(scalar)]),
		),
	).resolves.toEqual([1, [1, 2], 1]);
	expect(record.mock.calls).toEqual([
		['server', 'read, value', undefined],
		['server', 'read, value', undefined],
		['rows', 'read, value', 2],
		['server', 'read, value', undefined],
	]);
});

it('validates renamed bindings under their original statement and binding names', async () => {
	const read = (name: string) =>
		make({
			name,
			sql: '$item;',
			params: { item: Schema.Unknown },
			result: Schema.Unknown,
		});
	const invalid = { nested: new Map([['key', 'value']]) };
	const entries = [
		bind(read('first'), { item: 1 }),
		bind(read('second, value'), { item: invalid }),
	] as const;
	const { client, collect } = collecting(1, 2);

	await expect(
		Effect.runPromise(client.transaction(entries)),
	).rejects.toMatchObject({
		_tag: 'StatementEncodeError',
		statement: 'second, value',
		binding: 'item',
		path: ['nested'],
		message: /^\$item\.nested /,
	});
	expect(collect).not.toHaveBeenCalled();
	await expect(
		Effect.runPromise(
			client.transaction(entries, { unsafeSkipValueCheck: true }),
		),
	).resolves.toEqual([1, 2]);
	expect(collect.mock.calls[0]?.[0]).toMatchObject({
		sql: '$item__1;\n$item__2;',
		bindings: { item__1: 1, item__2: invalid },
	});
});

it('keeps scope and codec failures ahead of the encoded value check', async () => {
	const read = make({
		name: 'read item',
		sql: '$item;',
		params: { item: Schema.Unknown },
		result: Schema.Unknown,
	});
	const readInteger = make({
		name: 'read integer',
		sql: '$n;',
		params: { n: Schema.Number.check(Schema.isInt()) },
		result: Schema.Number,
	});
	const nested = make({
		name: 'nested transaction',
		sql: 'BEGIN; NONE; COMMIT;',
		params: {},
		result: Schema.Undefined,
		slots: 3,
		valueSlot: 2,
		transactional: true,
		writes: false,
	});
	const badValue = bind(read, { item: new Map() });
	const { client, collect } = collecting();

	for (const options of [undefined, { unsafeSkipValueCheck: true }]) {
		await expect(
			Effect.runPromise(
				client.transaction([badValue, bind(nested)], options),
			),
		).rejects.toMatchObject({
			_tag: 'StatementScopeError',
			statement: 'nested transaction',
		});
		await expect(
			Effect.runPromise(
				client.transaction(
					[badValue, bind(readInteger, { n: 1.5 })],
					options,
				),
			),
		).rejects.toMatchObject({
			_tag: 'StatementEncodeError',
			statement: 'read integer',
			binding: 'n',
		});
	}
	expect(collect).not.toHaveBeenCalled();
});
