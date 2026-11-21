import { Arbitrary, Effect, Result, Schema } from 'effect';
import { expect, it } from 'vitest';

import { bind, StatementScopeError } from '../src/client/index.js';
import { join, joinWith } from '../src/client/internal/join.js';
import { renameNames } from '../src/statement/rename.js';
import { make } from '../src/statement/statement.js';
import { fake } from '../src/testing/index.js';

const read = (binding: string) =>
	make({
		name: binding,
		sql: `$${binding};`,
		params: { [binding]: Schema.Number },
		result: Schema.Number,
	});
const query = { mode: 'query', refuseTransactional: false } as const;
const atomic = { mode: 'atomic', refuseTransactional: true } as const;

it('changes only parameter tokens, preserving every byte of literals and comments', () => {
	const names = { id: 'id__1' };
	const opaque = [
		"'$id'",
		'"$id"',
		"'a\\'$id'",
		'"a\\"$id"',
		...['r', 'd', 'u', 's', 'f', 'b'].flatMap((prefix) => [
			`${prefix}'$id'`,
			`${prefix}"$id"`,
		]),
		'`$id`',
		'⟨$id⟩',
		'/[$id]\\//',
	].join(', ');
	const comment = '-- $id\n// $id\n# $id\n/* $id */';
	const source = `${comment}\n[${opaque}, $id.field, $id[0], $id::name, $id2, $id / 2, $\`id\`, $⟨id⟩];`;
	expect(renameNames(source, names)).toBe(
		`${comment}\n[${opaque}, $id__1.field, $id__1[0], $id__1::name, $id2, $id__1 / 2, $id__1, $id__1];`,
	);
});

it('renames declarations, typed closure parameters, and references inside scopes', () => {
	const sql = 'FOR $id IN [$id] { LET $id = $id; }; |$id: int| $id;';
	expect(renameNames(sql, { id: 'id__3' })).toBe(
		'FOR $id__3 IN [$id__3] { LET $id__3 = $id__3; }; |$id__3: int| $id__3;',
	);
});

it.each([
	"$id; '$id",
	'$id; /* $id',
	'$id; `id',
	'$id; ⟨id',
	'$id; §',
	'function($id) { return $id; };',
])('refuses to guess when it cannot tokenize %s', (sql) =>
	expect(renameNames(sql, { id: 'id__1' })).toBeUndefined());

it('extends a suffix past locals and quoted variables anywhere in the list', async () => {
	const first = read('id');
	const reserve = make({
		sql: '{ LET $id__1 = 7; $id__1; }; $⟨id__1_⟩;',
		params: {},
		result: Schema.Unknown,
		slots: 2,
	});
	const joined = await Effect.runPromise(
		join(
			[
				{ statement: first, bindings: { id: 1 } },
				{ statement: first, bindings: { id: 2 } },
				{ statement: reserve, bindings: {} },
			],
			query,
		),
	);
	expect(joined.sql).toBe(`$id__1__;\n$id__2;\n${reserve.sql}`);
	expect(joined.bindings).toEqual({ id__1__: 1, id__2: 2 });
	expect(joined.parts.map(({ sent }) => sent)).toEqual([
		{ id: 'id__1__' },
		{ id: 'id__2' },
		{},
	]);
});

it('refuses engine-set collisions before evaluating codecs', async () => {
	const local = make({
		name: 'engine name',
		sql: 'LET $after = 1; $after;',
		defines: ['after'],
		params: {},
		slots: 2,
		result: Schema.Number,
	});
	const encoded = Effect.die('codec must not run');
	const failure = await Effect.runPromise(
		Effect.flip(
			joinWith(
				[
					{ statement: local, bindings: encoded },
					{ statement: local, bindings: encoded },
				],
				atomic,
			),
		),
	);
	expect(failure).toBeInstanceOf(StatementScopeError);
	expect(failure).toMatchObject({
		statement: 'engine name',
		binding: 'after',
	});
});

it('refuses unsafe text only when it needs a rename plan', async () => {
	const uncertain = make({
		name: 'uncertain',
		sql: "$id; 'unterminated",
		params: { id: Schema.Number },
		result: Schema.Number,
	});
	const entry = { statement: uncertain, bindings: { id: 7 } };
	expect((await Effect.runPromise(join([entry], query))).sql).toBe(
		uncertain.sql,
	);
	const failure = await Effect.runPromise(
		Effect.flip(join([entry, entry], query)),
	);
	expect(failure).toBeInstanceOf(StatementScopeError);
	expect(failure).toMatchObject({
		statement: 'uncertain',
		message: expect.stringContaining('cannot tokenize'),
	});
});

it('always builds identical SQL and sent names for the same descriptors with different inputs', async () => {
	const choices = ['a', 'b', 'a__1', 'a__1_', 'constructor'];
	const cases = Arbitrary.schema(
		Schema.Array(
			Schema.Struct({
				position: Schema.Int,
				first: Schema.Int,
				second: Schema.Int,
			}),
		),
	);
	const report = await Effect.runPromise(
		Arbitrary.checkEffect(
			cases,
			(items) =>
				Effect.gen(function* () {
					const rows = items.map(({ position, first, second }) => {
						const name =
							choices[Math.abs(position) % choices.length] ?? 'a';
						return { name, statement: read(name), first, second };
					});
					const separate = (key: 'first' | 'second') =>
						join(
							rows.map((row) => ({
								statement: row.statement,
								bindings: { [row.name]: row[key] },
							})),
							query,
						);
					const a = yield* separate('first');
					const b = yield* separate('second');
					const names = (request: typeof a) =>
						request.parts.map(({ sent }) => Object.values(sent));
					return (
						a.sql === b.sql &&
						JSON.stringify(names(a)) === JSON.stringify(names(b)) &&
						Object.keys(a.bindings).length === rows.length &&
						Object.keys(b.bindings).length === rows.length
					);
				}),
			{ runs: 100, seed: 20261120 },
		),
	);
	expect(Arbitrary.formatCheckFailure(report) ?? 'Passed').toBe('Passed');
});

it('keeps original fake bindings when the same statement runs twice in a transaction or pipeline', async () => {
	const client = fake({
		id: (values: Readonly<Record<string, unknown>>) => values.id,
	});
	const reader = read('id');
	const statements = [
		bind(reader, { id: 7 }),
		bind(reader, { id: 8 }),
	] as const;
	expect(await Effect.runPromise(client.transaction(statements))).toEqual([
		7, 8,
	]);
	const outcomes = await Effect.runPromise(client.pipeline(statements));
	expect(outcomes).toEqual([Result.succeed(7), Result.succeed(8)]);
	expect(
		client.calls.map(({ bindings }) => Object.entries(bindings)),
	).toEqual([[['id', 7]], [['id', 8]], [['id', 7]], [['id', 8]]]);
});
