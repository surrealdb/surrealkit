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
	expect(
		await Effect.runPromise(
			client.transaction([
				bind(statement, bindings),
				bind(statement, { [key]: 8 }),
			]),
		),
	).toStrictEqual([7, 8]);
};

describe('joining encoded bindings', () => {
	it.each(
		bindingNames,
	)('renames repeat bindings of $%s and keeps original part keys', async (key) => {
		for (const second of [7, 8]) {
			const joined = await Effect.runPromise(
				join(
					[
						{
							statement: reader('first', key),
							bindings: { [key]: 7 },
						},
						{
							statement: reader('second', key),
							bindings: { [key]: second },
						},
					],
					options,
				),
			);
			expect(Object.entries(joined.bindings)).toEqual([
				[`${key}__1`, 7],
				[`${key}__2`, second],
			]);
			expect(
				joined.parts.map((part) => Object.entries(part.sent)),
			).toEqual([[[key, `${key}__1`]], [[key, `${key}__2`]]]);
			expect(
				joined.parts.map((part) => Object.entries(part.bindings)),
			).toEqual([[[key, 7]], [[key, second]]]);
		}
	});
});

it.each(bindingNames)('handles $%s on a scripted client', async (key) => {
	const client = fake({
		reader: (bindings: Readonly<Record<string, unknown>>) => bindings[key],
	});
	await compareClientModes(client, key);
	expect(client.calls).toHaveLength(key === '__proto__' ? 3 : 4);
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
