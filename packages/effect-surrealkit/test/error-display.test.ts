import { inspect } from 'node:util';

import { Effect, Schema, Stream } from 'effect';
import { Features, UnavailableFeatureError } from 'surrealdb';
import { expect, it } from 'vitest';

import { makeWith } from '../src/client/client.js';
import {
	StatementConflict,
	StatementEncodeError,
	StatementRequestError,
	StatementThrown,
	StatementUnexpectedThrow,
	StatementUnsupported,
	TransactionOutcomeUnknown,
} from '../src/client/index.js';
import { make } from '../src/statement/index.js';
import { scriptedTransport } from '../src/testing/fake.js';
import { failure, fake, thrown } from '../src/testing/index.js';

const marker = 'synthetic-private-request-data';
const read = make({
	name: 'read',
	sql: 'RETURN $entry;',
	params: { entry: Schema.Unknown },
	result: Schema.Array(Schema.Number),
	writes: false,
});

const rejected = <E>(effect: Effect.Effect<unknown, E>) =>
	Effect.runPromise(Effect.flip(effect));

const expectSafeDisplay = (error: Error) => {
	for (const text of [
		error.message,
		String(error),
		error.stack,
		inspect(error, { depth: null }),
		JSON.stringify(error),
	]) {
		expect(text).not.toContain(marker);
	}
};

it('keeps the raw SDK failure accessible without rendering its message', async () => {
	const error = await rejected(
		fake({ read: failure(`Conversion rejected ${marker}`) }).run(read, {
			entry: marker,
		}),
	);
	expect(error).toBeInstanceOf(StatementRequestError);
	expect(error).toHaveProperty(
		'cause.message',
		`Conversion rejected ${marker}`,
	);
	expectSafeDisplay(error);
});

it('keeps undeclared THROW text for deliberate access and displays declared text', async () => {
	const error = await rejected(
		fake({ read: thrown(marker) }).run(read, { entry: marker }),
	);
	expect(error).toBeInstanceOf(StatementUnexpectedThrow);
	expect(error).toHaveProperty('reason', marker);
	expect(error).toHaveProperty(
		'cause.message',
		`An error occurred: ${marker}`,
	);
	expectSafeDisplay(error);

	const guarded = make({
		...read,
		sql: "THROW 'fixed guard';",
		throws: ['fixed guard'],
	});
	const declared = await rejected(
		fake({ read: thrown('fixed guard') }).run(guarded, { entry: marker }),
	);
	expect(declared).toBeInstanceOf(StatementThrown);
	expect(declared.message).toBe('fixed guard');
});

it('renders a transaction conflict without exposing the engine message', async () => {
	const cause = Object.assign(
		new Error(`Transaction conflict on ${marker}`),
		{
			isTransactionConflict: true,
		},
	);
	const client = makeWith({
		...scriptedTransport({}).transport,
		collect: async () => {
			throw cause;
		},
	});
	const error = await rejected(client.run(read, { entry: 0 }));
	expect(error).toBeInstanceOf(StatementConflict);
	expect(error).toHaveProperty('cause', cause);
	expectSafeDisplay(error);
});

it('renders unavailable streaming without exposing the reported SDK version', async () => {
	const cause = new UnavailableFeatureError(Features.QueryStreaming, marker);
	const client = makeWith({
		...scriptedTransport({}).transport,
		stream: () => {
			throw cause;
		},
	});
	const error = await rejected(
		Stream.runCollect(client.stream(read, { entry: 0 })),
	);
	expect(error).toBeInstanceOf(StatementUnsupported);
	expect(error).toMatchObject({
		statement: 'read',
		feature: 'stream',
		cause,
	});
	expectSafeDisplay(error);
});

it('keeps the raw cause of an unacknowledged commit out of the display', async () => {
	const client = fake({ commit: failure(marker) });
	const error = await rejected(client.withTransaction(() => Effect.void));
	expect(error).toBeInstanceOf(TransactionOutcomeUnknown);
	expect(error.message).toContain('Read back the data before retrying');
	expect(error).toHaveProperty('cause.message', marker);
	expectSafeDisplay(error);
});

it('keeps the raw preflight path without rendering a data key or constructor name', async () => {
	const dynamic = { [marker]: class {} };
	const entry = { [marker]: [new dynamic[marker]()] };
	const client = fake({});
	const error = await rejected(client.run(read, { entry }));
	expect(error).toBeInstanceOf(StatementEncodeError);
	expect(error).toMatchObject({ binding: 'entry', path: [marker, 0] });
	expect(error.message).toContain('$entry');
	expect(client.calls).toHaveLength(0);
	expectSafeDisplay(error);
});
