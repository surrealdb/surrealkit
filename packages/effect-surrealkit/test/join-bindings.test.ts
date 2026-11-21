import { Effect, Schema } from 'effect';
import { BoundQuery } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
	bind,
	type Client,
	make as makeClient,
	StatementScopeError,
} from '../src/client/index.js';
import { join } from '../src/client/internal/join.js';
import { make } from '../src/statement/index.js';
import { fake } from '../src/testing/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
} from './engine.js';

const reader = (name: string, key: string) =>
	make({
		name,
		sql: `$${key};`,
		params: { [key]: Schema.Number },
		result: Schema.Number,
	});

const options = { mode: 'query', refuseTransactional: false } as const;
const bindingNames = ['__proto__', 'constructor', 'toString', 'ordinary'];

const compareClientModes = async (client: Client, key: string) => {
	const statement = reader('reader', key);
	const bindings = { [key]: 7 };
	expect(await Effect.runPromise(client.run(statement, bindings))).toBe(7);
	const atomic = client.transaction([bind(statement, bindings)]);
	if (key === '__proto__') {
		const error = await Effect.runPromise(Effect.flip(atomic));
		expect(error).toBeInstanceOf(StatementScopeError);
		expect(error).toMatchObject({ statement: 'reader', binding: key });
	} else {
		expect(await Effect.runPromise(atomic)).toStrictEqual([7]);
	}
};

describe('joining encoded bindings', () => {
	it.each(
		bindingNames,
	)('keeps equal own $%s bindings in the joined request and each part', async (key) => {
		const bindings = { [key]: 7 };
		const joined = await Effect.runPromise(
			join(
				[
					{ statement: reader('first', key), bindings },
					{ statement: reader('second', key), bindings },
				],
				options,
			),
		);
		expect(Object.entries(joined.bindings)).toEqual([[key, 7]]);
		expect(joined.parts.map((part) => Object.entries(part.sent))).toEqual([
			[[key, key]],
			[[key, key]],
		]);
	});

	it.each([
		'__proto__',
		'ordinary',
	])('refuses different values for the same $%s binding', async (key) => {
		const error = await Effect.runPromise(
			Effect.flip(
				join(
					[
						{
							statement: reader('first', key),
							bindings: { [key]: 7 },
						},
						{
							statement: reader('second', key),
							bindings: { [key]: 8 },
						},
					],
					options,
				),
			),
		);
		expect(error).toBeInstanceOf(StatementScopeError);
		expect(error).toMatchObject({ statement: 'second', binding: key });
	});
});

it.each(bindingNames)('handles $%s on a scripted client', async (key) => {
	const client = fake({
		reader: (bindings: Readonly<Record<string, unknown>>) => bindings[key],
	});
	await compareClientModes(client, key);
	expect(client.calls).toHaveLength(key === '__proto__' ? 1 : 2);
});

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] joined binding engine suite skipped: ${engineSkipReason}`,
	);
}

describe.skipIf(!engineConfigured)(
	'joined bindings on a disposable engine',
	() => {
		let engine: Engine;
		beforeAll(async () => {
			engine = await startEngine();
		});
		afterAll(async () => {
			await engine?.stop();
		});

		it.each(
			bindingNames,
		)('compares plain query and SDK atomic handling of $%s', async (key) => {
			const bindings = { [key]: 7 };
			const joined = await Effect.runPromise(
				join([{ statement: reader('reader', key), bindings }], options),
			);
			expect(
				await engine.db.query(joined.sql, joined.bindings),
			).toStrictEqual([7]);
			expect(
				await engine.db.transaction([
					new BoundQuery(joined.sql, bindings),
				]),
			).toStrictEqual([key === '__proto__' ? undefined : 7]);
			await compareClientModes(makeClient(engine.db), key);
		});
	},
);
