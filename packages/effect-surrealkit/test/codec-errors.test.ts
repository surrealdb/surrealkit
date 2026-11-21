import { inspect } from 'node:util';
import { Effect, Schema, Stream, Tracer } from 'effect';
import { expect, it } from 'vitest';

import {
	StatementDecodeError,
	StatementEncodeError,
	StatementWriteOutcomeError,
} from '../src/client/index.js';
import { live, make } from '../src/statement/index.js';
import { events, fake } from '../src/testing/index.js';

const confidential = 'synthetic-confidential-codec-value';
const diagnostic = 'synthetic-custom-diagnostic';
const rejected = Schema.String.check(
	Schema.makeFilter((value) =>
		value === confidential
			? { path: [value], issue: `${diagnostic} ${value}` }
			: undefined,
	),
);
const row = Schema.Struct({ payload: rejected });
const input = { payload: confidential };

const failure = async <E>(effect: Effect.Effect<unknown, E>) => {
	const spans: Array<Tracer.Span> = [];
	const tracer = Tracer.make({
		span: (options) => {
			const span = new Tracer.NativeSpan(options);
			spans.push(span);
			return span;
		},
	});
	const error = await Effect.runPromise(
		Effect.flip(effect.pipe(Effect.withTracer(tracer))),
	);
	const exposed = [
		String(error),
		JSON.stringify(error),
		inspect(error),
		inspect(
			spans.map((span) => [span.attributes, span.status]),
			{
				depth: null,
			},
		),
	].join('\n');
	expect(exposed).not.toContain(confidential);
	expect(exposed).not.toContain(diagnostic);
	return error;
};

it('reports an unreadable binding at its path without echoing the thrown error', async () => {
	const statement = make({
		name: 'unreadable binding',
		sql: 'RETURN $entry;',
		params: { entry: Schema.Unknown },
		result: Schema.Unknown,
		writes: false,
	});
	const client = fake({});
	const error = await failure(
		client.run(statement, {
			entry: {
				get field(): never {
					throw new Error(confidential);
				},
			},
		}),
	);
	expect(error).toBeInstanceOf(StatementEncodeError);
	expect(error).toMatchObject({
		statement: 'unreadable binding',
		binding: 'entry',
		path: ['field'],
	});
	expect(error.message).toContain('$entry[*]');
	expect(client.calls).toHaveLength(0);
});

it('reports a failed binding codec by its binding and declared field before sending', async () => {
	const statement = make({
		name: 'bind payload',
		sql: 'RETURN $entry;',
		params: { entry: row },
		result: Schema.Unknown,
		writes: false,
	});
	const client = fake({});
	const error = await failure(client.run(statement, { entry: input }));
	expect(error).toBeInstanceOf(StatementEncodeError);
	expect(error).toMatchObject({
		statement: 'bind payload',
		binding: 'entry',
	});
	expect(error.message).toContain('payload');
	expect(client.calls).toHaveLength(0);
});

it('reports a failed result codec in its committed write outcome', async () => {
	const statement = make({
		name: 'write payload',
		sql: 'LET $stored = CREATE ONLY item SET payload = "public"; RETURN $stored;',
		params: {},
		defines: ['stored'],
		slots: 2,
		result: row,
		writes: true,
	});
	const client = fake({ 'write payload': input });
	const error = await failure(client.run(statement));
	expect(error).toBeInstanceOf(StatementWriteOutcomeError);
	expect(error).toMatchObject({
		request: 'write payload',
		cause: {
			_tag: 'StatementDecodeError',
			statement: 'write payload',
			slot: 2,
			committed: true,
		},
	});
	expect(error.message).toContain('payload');
	expect(error.message).toContain('writes are committed');
});

it('reports a failed streamed row codec by its statement and result slot', async () => {
	const statement = make({
		name: 'stream payload',
		sql: 'SELECT payload FROM item;',
		params: {},
		result: Schema.Array(row),
		writes: false,
	});
	const client = fake({ 'stream payload': [input] });
	const error = await failure(Stream.runCollect(client.stream(statement)));
	expect(error).toBeInstanceOf(StatementDecodeError);
	expect(error).toMatchObject({
		statement: 'stream payload',
		slot: 1,
		committed: false,
	});
	expect(error.message).toContain('payload');
});

it.each([
	'id',
	'value',
] as const)('reports a failed live %s codec and kills the subscription', async (field) => {
	const statement = live({
		name: 'item',
		id: rejected,
		select: row,
	}).build('watch payload');
	const client = fake({
		'watch payload': events([
			{
				action: 'CREATE',
				id: field === 'id' ? confidential : 'public-id',
				value: field === 'value' ? input : { payload: 'public' },
			},
		]),
	});
	const error = await failure(Stream.runCollect(client.live(statement)));
	expect(error).toBeInstanceOf(StatementDecodeError);
	expect(error).toMatchObject({
		statement: 'watch payload',
		slot: 1,
		committed: false,
	});
	expect(error.message).toContain(`live event ${field}`);
	expect(client.calls.at(-1)?.operation).toBe('kill');
});

it('omits object keys from data while preserving declared nested paths', async () => {
	const statement = make({
		name: 'read shape',
		sql: 'RETURN {};',
		params: {},
		result: Schema.Struct({
			record: Schema.Record(Schema.String, Schema.Number),
		}),
		writes: false,
	});
	const client = fake({
		'read shape': { record: { [confidential]: diagnostic } },
	});
	const error = await failure(client.run(statement));
	expect(error).toBeInstanceOf(StatementDecodeError);
	expect(error.message).toContain('record');
});
