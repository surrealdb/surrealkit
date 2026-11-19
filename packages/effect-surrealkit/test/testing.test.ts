/**
 * `effect-surrealkit/testing`: the memory database (always the embedded
 * engine), and record users, index expectations and the scripted client
 * against the suite's engine, so they run on both engine lanes.
 */
import { Effect, Layer, Schedule, Schema } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
	type Bound,
	bind,
	make as makeClient,
	run,
	StatementConflict,
	StatementDecodeError,
	StatementEncodeError,
	StatementRequestError,
	StatementThrown,
	SurrealClient,
	transaction,
} from '../src/client/index.js';
import {
	type AnyStatement,
	and,
	eq,
	gte,
	make,
	param,
	part,
	readAs,
	select,
	sequence,
} from '../src/statement/index.js';
import {
	conflict,
	expectIndex,
	expectNoIndex,
	explain,
	failure,
	fake,
	IndexExpectationError,
	layerFrom,
	loadSchema,
	memoryLayer,
	recordUser,
	result,
	type TestDatabase,
	thrown,
} from '../src/testing/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
} from './engine.js';

const PersonId = SurrealSchema.RecordIdFor('person');
const PersonTable = {
	name: 'person',
	schemafull: true,
	id: PersonId,
	select: Schema.Struct({
		id: PersonId,
		email: SurrealSchema.String,
		name: SurrealSchema.String,
		age: SurrealSchema.Int,
	}),
} as const;

const schema = `
	DEFINE TABLE person SCHEMAFULL
		PERMISSIONS FOR select WHERE $auth = NONE OR age >= 18 FOR create, update, delete NONE;
	DEFINE FIELD email ON person TYPE string
		PERMISSIONS FOR select WHERE $auth = NONE;
	DEFINE FIELD name ON person TYPE string;
	DEFINE FIELD age ON person TYPE int;
	DEFINE INDEX person_email ON person FIELDS email UNIQUE;
	DEFINE INDEX person_age ON person FIELDS age;
	DEFINE TABLE account SCHEMAFULL PERMISSIONS FOR select WHERE id = $auth.id;
	DEFINE FIELD name ON account TYPE string;
	DEFINE ACCESS user ON DATABASE TYPE RECORD
		SIGNUP (CREATE account SET name = $name)
		SIGNIN (SELECT * FROM account WHERE name = $name);
	CREATE person:ann SET email = 'ann@example.com', name = 'Ann', age = 30;
	CREATE person:kit SET email = 'kit@example.com', name = 'Kit', age = 12;
`;

const everyone = select(PersonTable)
	.project((row) => ({ name: row.name }))
	.build('everyone');
const byEmail = select(PersonTable)
	.where((row) => eq(row.email, param('email', SurrealSchema.String)))
	.build('by email');
const byName = select(PersonTable)
	.where((row) => eq(row.name, param('name', SurrealSchema.String)))
	.build('by name');

const failureOf = <A, E>(effect: Effect.Effect<A, E>) =>
	Effect.runPromise(Effect.flip(effect));

describe('memoryLayer', () => {
	it('gives a SurrealClient over an in-memory database with its schema loaded', async () => {
		const names = await Effect.runPromise(
			Effect.gen(function* () {
				yield* loadSchema(
					"CREATE person:lee SET email = 'lee@example.com', name = 'Lee', age = 41;",
				);
				return yield* run(everyone);
			}).pipe(Effect.provide(memoryLayer({ schema }))),
		);
		expect(names.map(({ name }) => name).sort()).toEqual([
			'Ann',
			'Kit',
			'Lee',
		]);
	});

	it('is a database of its own each time', async () => {
		const count = await Effect.runPromise(
			run(everyone).pipe(
				Effect.provide(memoryLayer({ schema: 'DEFINE TABLE person;' })),
			),
		);
		expect(count).toEqual([]);
	});

	it('reports schema text the engine refuses as a TestingError', async () => {
		const error = await failureOf(
			Effect.scoped(
				Layer.build(memoryLayer({ schema: 'DEFINE TABLE;' })),
			),
		);
		expect(error._tag).toBe('TestingError');
		expect(error.message).toMatch(/^the schema text failed: /u);
	});
});

describe.skipIf(!engineConfigured)('against the engine', () => {
	let engine: Engine;
	let database: Layer.Layer<TestDatabase | SurrealClient>;

	beforeAll(async () => {
		engine = await startEngine();
		await engine.db.query(schema);
		database = layerFrom(engine.db, {
			namespace: engine.namespace,
			database: engine.namespace,
		});
	});
	afterAll(async () => {
		await engine?.stop();
	});

	const runHere = <A, E>(
		effect: Effect.Effect<A, E, TestDatabase | SurrealClient>,
	) => Effect.runPromise(effect.pipe(Effect.provide(database)));

	describe('recordUser', () => {
		it('signs a record user up and runs statements as that session', async () => {
			// What the session may read: adults only, and never the email.
			const Visible = readAs(
				PersonTable,
				Schema.Struct({
					id: PersonId,
					name: SurrealSchema.String,
					email: Schema.optionalKey(SurrealSchema.String),
					age: SurrealSchema.Int,
				}),
			);
			const visible = select(Visible).build('visible people');
			const { asRoot, asUser } = await runHere(
				Effect.scoped(
					Effect.gen(function* () {
						const user = yield* recordUser({
							access: 'user',
							variables: { name: 'dee' },
						});
						return {
							asRoot: yield* run(visible),
							asUser: yield* user.client.run(visible),
						};
					}),
				),
			);
			expect(asRoot.map(({ name }) => name).sort()).toEqual([
				'Ann',
				'Kit',
			]);
			expect(asRoot.every(({ email }) => email !== undefined)).toBe(true);
			expect(asUser).toEqual([
				{ id: expect.anything(), name: 'Ann', age: 30 },
			]);
		});

		it('reports an access method the engine refuses as a TestingError', async () => {
			const error = await Effect.runPromise(
				Effect.flip(
					Effect.scoped(recordUser({ access: 'nobody' })).pipe(
						Effect.provide(database),
					),
				),
			);
			expect(error._tag).toBe('TestingError');
			expect(error.message).toMatch(/through `nobody`/u);
		});
	});

	describe('EXPLAIN', () => {
		// What `SELECT … EXPLAIN` answers for each builder shape, on this engine.
		it.each([
			[
				'= on a unique index',
				byEmail,
				{ email: 'ann@example.com' },
				['person_email'],
			],
			[
				'a range, with ORDER BY and LIMIT',
				select(PersonTable)
					.where((row) =>
						gte(row.age, param('minimum', SurrealSchema.Int)),
					)
					.orderBy((row) => row.age)
					.limit(1)
					.build('oldest first'),
				{ minimum: 18 },
				['person_age'],
			],
			[
				'a projection',
				select(PersonTable)
					.where((row) =>
						eq(row.email, param('email', SurrealSchema.String)),
					)
					.project((row) => ({ id: row.id, name: row.name }))
					.build('projected'),
				{ email: 'ann@example.com' },
				['person_email'],
			],
			[
				'SELECT VALUE … FROM ONLY',
				select(PersonTable)
					.where((row) =>
						eq(row.email, param('email', SurrealSchema.String)),
					)
					.value((row) => row.name)
					.only()
					.build('one name'),
				{ email: 'ann@example.com' },
				['person_email'],
			],
			[
				'an indexed and an unindexed condition',
				select(PersonTable)
					.where((row) =>
						and(
							eq(row.email, param('email', SurrealSchema.String)),
							eq(row.name, param('name', SurrealSchema.String)),
						),
					)
					.build('both'),
				{ email: 'ann@example.com', name: 'Ann' },
				['person_email'],
			],
			['a table scan', byName, { name: 'Ann' }, []],
		] satisfies ReadonlyArray<
			readonly [
				string,
				AnyStatement,
				Readonly<Record<string, unknown>>,
				ReadonlyArray<string>,
			]
		>)('%s', async (_, statement, input, indexes) => {
			const planned = await runHere(
				explain(statement as AnyStatement, input as never),
			);
			expect(planned.indexes).toEqual(indexes);
			// One root node: `SelectProject`, `ProjectValue` or, for ONLY,
			// `UnwrapExactlyOne` around one.
			expect(planned.plan).toEqual(
				expect.objectContaining({ operator: expect.any(String) }),
			);
		});

		it('expectIndex and expectNoIndex read the plan', async () => {
			await runHere(
				Effect.all([
					expectIndex(
						byEmail,
						{ email: 'ann@example.com' },
						'person_email',
					),
					expectNoIndex(
						byEmail,
						{ email: 'ann@example.com' },
						'person_age',
					),
					expectNoIndex(byName, { name: 'Ann' }, 'person_email'),
				]),
			);
			const missed = await Effect.runPromise(
				Effect.flip(
					expectIndex(byName, { name: 'Ann' }, 'person_email').pipe(
						Effect.provide(database),
					),
				),
			);
			expect(missed).toBeInstanceOf(IndexExpectationError);
			expect(missed).toMatchObject({
				statement: 'by name',
				index: 'person_email',
				expected: 'used',
				used: [],
				message:
					'`by name` does not read the index `person_email`; its plan reads no index',
			});
			const used = await Effect.runPromise(
				Effect.flip(
					expectNoIndex(byEmail, { email: 'x' }, 'person_email').pipe(
						Effect.provide(database),
					),
				),
			);
			expect(used).toMatchObject({
				expected: 'unused',
				used: ['person_email'],
				message: '`by email` reads the index `person_email`',
			});
		});

		it('takes only a single SELECT', async () => {
			const write = make({
				name: 'write',
				sql: 'CREATE person:x SET name = "x";',
				params: {},
				result: Schema.Unknown,
			});
			const exit = await Effect.runPromiseExit(
				expectIndex(write, {}, 'person_email').pipe(
					Effect.provide(database),
				),
			);
			expect(String(exit)).toMatch(/`write` is not a single SELECT/u);
		});
	});

	describe('the scripted client refuses what the SDK refuses', () => {
		const one = make({
			name: 'one',
			sql: 'RETURN 1;',
			params: {},
			result: SurrealSchema.Int,
		});
		const two = make({
			name: 'two',
			sql: 'RETURN 2;',
			params: {},
			result: SurrealSchema.Int,
		});
		const own = make({
			name: 'own',
			sql: 'BEGIN; RETURN 3; COMMIT;',
			params: {},
			slots: 3,
			result: SurrealSchema.Int,
		});

		it.each([
			['a RETURN followed by another statement', [bind(one), bind(two)]],
			['a BEGIN it did not declare', [bind(own)]],
		] satisfies ReadonlyArray<
			readonly [string, ReadonlyArray<Bound>]
		>)('%s', async (_, statements: ReadonlyArray<Bound>) => {
			const scripted = fake({ one: 1, two: 2, own: 3 });
			const real = await failureOf(
				makeClient(engine.db).transaction(statements),
			);
			const fakeError = await failureOf(scripted.transaction(statements));
			expect(real).toBeInstanceOf(StatementRequestError);
			expect(fakeError).toBeInstanceOf(StatementRequestError);
			expect(fakeError.message).toBe(real.message);
			expect(scripted.calls).toEqual([]);
		});
	});
});

describe('fake', () => {
	const guarded = sequence(
		part({
			sql: "IF $name = NONE { THROW 'No name' };",
			free: ['name'],
			throws: ['No name'],
		}),
		part({
			sql: 'SELECT VALUE name FROM person WHERE name = $name;',
			free: ['name'],
		}),
	).define({
		name: 'guarded',
		params: { name: SurrealSchema.String },
		result: Schema.Array(SurrealSchema.String),
	});

	it('answers each statement by name and records its encoded bindings', async () => {
		const client = fake({
			'by email': [],
			guarded: (bindings: Readonly<Record<string, unknown>>) => [
				String(bindings.name),
			],
		});
		const [rows, names] = await Effect.runPromise(
			Effect.all([
				client.run(byEmail, { email: 'a@example.com' }),
				client.run(guarded, { name: 'Ann' }),
			]),
		);
		expect(rows).toEqual([]);
		expect(names).toEqual(['Ann']);
		expect(client.calls).toEqual([
			{ statement: 'by email', bindings: { email: 'a@example.com' } },
			{ statement: 'guarded', bindings: { name: 'Ann' } },
		]);
	});

	it('decodes a canned row with the statement codec', async () => {
		const client = fake({
			everyone: [{ name: 'Ann' }, { name: 'Kit', extra: 1 }],
		});
		const error = await failureOf(client.run(everyone));
		expect(error).toBeInstanceOf(StatementDecodeError);
	});

	it('refuses a binding its codec refuses, sending nothing', async () => {
		const client = fake({ 'by email': [] });
		const error = await failureOf(client.run(byEmail, { email: '\uD800' }));
		expect(error).toBeInstanceOf(StatementEncodeError);
		expect(client.calls).toEqual([]);
	});

	it('raises a THROW, a conflict and a failure as the engine would', async () => {
		let attempts = 0;
		const client = fake({
			guarded: thrown('No name'),
			everyone: () => {
				attempts += 1;
				return attempts < 3 ? conflict() : result([{ name: 'Ann' }]);
			},
			'by name': failure('Parse error'),
		});
		const thrownError = await failureOf(client.run(guarded, { name: 'x' }));
		expect(thrownError).toBeInstanceOf(StatementThrown);
		expect(thrownError).toMatchObject({
			statement: 'guarded',
			reason: 'No name',
		});
		expect(
			await Effect.runPromise(
				client.run(everyone, {}, { retry: Schedule.recurs(3) }),
			),
		).toEqual([{ name: 'Ann' }]);
		expect(attempts).toBe(3);
		const conflicted = await failureOf(
			fake({ everyone: conflict() }).run(everyone),
		);
		expect(conflicted).toBeInstanceOf(StatementConflict);
		const failed = await failureOf(client.run(byName, { name: 'x' }));
		expect(failed).toBeInstanceOf(StatementRequestError);
		expect(failed.message).toBe('Parse error');
	});

	it('fails a statement the script does not name', async () => {
		const error = await failureOf(fake({}).run(everyone));
		expect(error).toBeInstanceOf(StatementRequestError);
		expect(error.message).toBe(
			'the scripted client has no reply for `everyone`',
		);
	});

	it('runs a transaction atomically, with the scope rules of the real client', async () => {
		const client = fake({ everyone: [{ name: 'Ann' }], guarded: ['Ann'] });
		const [people, names] = await Effect.runPromise(
			transaction([bind(everyone), bind(guarded, { name: 'Ann' })]).pipe(
				Effect.provide(Layer.succeed(SurrealClient, client)),
			),
		);
		expect(people).toEqual([{ name: 'Ann' }]);
		expect(names).toEqual(['Ann']);
		expect(client.calls.map((call) => call.transaction)).toEqual([
			'transaction(everyone, guarded)',
			'transaction(everyone, guarded)',
		]);
		const clash = await failureOf(
			client.transaction([
				bind(guarded, { name: 'Ann' }),
				bind(byName, { name: 'Kit' }),
			]),
		);
		expect(clash._tag).toBe('StatementScopeError');
	});
});
