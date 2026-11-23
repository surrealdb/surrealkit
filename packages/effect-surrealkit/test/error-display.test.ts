import { inspect } from 'node:util';

import { Effect, Schema, Stream } from 'effect';
import { Features, UnavailableFeatureError } from 'surrealdb';
import { describe, expect, it } from 'vitest';

import { makeWith } from '../src/client/client.js';
import {
	StatementConflict,
	StatementEncodeError,
	StatementIndexConflict,
	StatementRecordExists,
	StatementRequestError,
	StatementThrown,
	StatementUnexpectedThrow,
	StatementUnsupported,
	TransactionOutcomeUnknown,
} from '../src/client/index.js';
import { make } from '../src/statement/index.js';
import { scriptedTransport } from '../src/testing/fake.js';
import {
	failure,
	fake,
	indexConflict,
	recordExists,
	thrown,
} from '../src/testing/index.js';

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

describe('constraint errors', () => {
	const write = make({
		name: 'write',
		sql: 'CREATE person CONTENT $entry RETURN NONE;',
		params: { entry: Schema.Unknown },
		result: Schema.Tuple([]),
		writes: true,
		uniqueIndexes: ['person_email', 'person_handle'],
	});
	/** The engine's error for a duplicate value of `index`, holding the marker. */
	const duplicate = (index: string, prefix = '') =>
		Object.assign(
			new Error(
				`${prefix}Database index \`${index}\` already contains '${marker}', with record \`person:${marker}\``,
			),
			{ kind: 'Internal' },
		);
	const failingWith = (cause: unknown) =>
		makeWith({
			...scriptedTransport({}).transport,
			collect: async () => {
				throw cause;
			},
		});

	it('renders a duplicate on a declared unique index by its name only', async () => {
		const cause = duplicate('person_handle');
		const error = await rejected(
			failingWith(cause).run(write, { entry: marker }),
		);
		expect(error).toBeInstanceOf(StatementIndexConflict);
		expect(error).toMatchObject({
			statement: 'write',
			index: 'person_handle',
			cause,
		});
		expect(error.message).toBe(
			'`write` would duplicate a value of the unique index `person_handle`',
		);
		expect(JSON.parse(JSON.stringify(error))).toStrictEqual({
			_tag: 'StatementIndexConflict',
			statement: 'write',
			index: 'person_handle',
			message:
				'`write` would duplicate a value of the unique index `person_handle`',
		});
		expectSafeDisplay(error);
	});

	it('renders a record that exists without its id', async () => {
		const cause = Object.assign(
			new Error(`Database record \`person:${marker}\` already exists`),
			{
				kind: 'AlreadyExists',
				details: {
					kind: 'Record',
					details: { id: `person:${marker}` },
				},
			},
		);
		const error = await rejected(
			failingWith(cause).run(write, { entry: marker }),
		);
		expect(error).toBeInstanceOf(StatementRecordExists);
		expect(error).toMatchObject({ statement: 'write', cause });
		expect(JSON.parse(JSON.stringify(error))).toStrictEqual({
			_tag: 'StatementRecordExists',
			statement: 'write',
			message: '`write` tried to create a record whose id already exists',
		});
		expectSafeDisplay(error);
	});

	it('keeps every other shape a request error: an undeclared index, a nested event, another kind, an AlreadyExists that is not a record', async () => {
		for (const cause of [
			duplicate('person_name'),
			duplicate('person'),
			duplicate('person_email_2'),
			duplicate('person_email', 'Error while processing event copy: '),
			Object.assign(duplicate('person_email'), { kind: undefined }),
			Object.assign(
				new Error(
					`Database index \`person_email\` already contains '${marker}'`,
				),
				{ kind: 'Internal' },
			),
			Object.assign(new Error(`The table '${marker}' already exists`), {
				kind: 'AlreadyExists',
				details: { kind: 'Table', details: { name: marker } },
			}),
			Object.assign(new Error('Database record already exists'), {
				kind: 'AlreadyExists',
			}),
		]) {
			const error = await rejected(
				failingWith(cause).run(write, { entry: marker }),
			);
			expect(error, cause.message).toBeInstanceOf(StatementRequestError);
			expectSafeDisplay(error);
		}
	});

	it('a THROW of the same text is a thrown message, not a conflict', async () => {
		const error = await rejected(
			failingWith(
				Object.assign(duplicate('person_email'), { kind: 'Thrown' }),
			).run(write, { entry: marker }),
		);
		expect(error).toBeInstanceOf(StatementUnexpectedThrow);
	});

	it('a statement that declares no unique index never reports one', async () => {
		const error = await rejected(
			failingWith(duplicate('person_email')).run(read, { entry: marker }),
		);
		expect(error).toBeInstanceOf(StatementRequestError);
	});

	it('the scripted client reports both, as the engine does', async () => {
		const conflicting = await rejected(
			fake({ write: indexConflict('person_email') }).run(write, {
				entry: 1,
			}),
		);
		expect(conflicting).toBeInstanceOf(StatementIndexConflict);
		expect(conflicting).toMatchObject({ index: 'person_email' });
		expectSafeDisplay(conflicting);
		const undeclared = await rejected(
			fake({ write: indexConflict('person_name') }).run(write, {
				entry: 1,
			}),
		);
		expect(undeclared).toBeInstanceOf(StatementRequestError);
		const existing = await rejected(
			fake({ write: recordExists() }).run(write, { entry: 1 }),
		);
		expect(existing).toBeInstanceOf(StatementRecordExists);
		expectSafeDisplay(existing);
	});
});

describe('declaring unique indexes', () => {
	it('make keeps them sorted and once, and leaves the field out when there are none', () => {
		const declared = make({
			sql: 'CREATE person CONTENT $entry;',
			params: { entry: Schema.Unknown },
			result: Schema.Unknown,
			writes: true,
			uniqueIndexes: ['b', 'a', 'b'],
		});
		expect(declared.uniqueIndexes).toStrictEqual(['a', 'b']);
		expect('uniqueIndexes' in read).toBe(false);
		expect(
			'uniqueIndexes' in
				make({ ...read, writes: true, uniqueIndexes: [] }),
		).toBe(false);
	});

	it('make refuses them on a statement declared not to write, and refuses an empty name', () => {
		expect(() =>
			make({ ...read, uniqueIndexes: ['person_email'] }),
		).toThrow(
			'statement `read` is declared not to write, so it cannot duplicate a value of a unique index; remove `uniqueIndexes` or declare `writes`',
		);
		expect(() =>
			make({ ...read, writes: true, uniqueIndexes: [''] }),
		).toThrow('statement `read` declares a unique index with no name');
	});
});
