/**
 * Statements through a real engine: the corpus schema is applied to a
 * disposable database, and the statements are built from the golden modules
 * the emitter generated for that same corpus. Every result is decoded
 * strictly by the codec its statement derived.
 */
import { readFileSync } from 'node:fs';

import { Arbitrary, Effect, Schedule, Schema } from 'effect';
import { DateTime, RecordId } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
	bind,
	type Client,
	make as makeClient,
	StatementConflict,
	StatementDecodeError,
	StatementEmptyResult,
	StatementEncodeError,
	StatementScopeError,
	StatementThrown,
	StatementUnexpectedThrow,
} from '../src/client/index.js';
import * as SurrealSchema from '../src/index.js';
import {
	and,
	at,
	call,
	cast,
	contains,
	create,
	deleteFrom,
	eq,
	gte,
	inside,
	isNone,
	literal,
	make,
	not,
	or,
	param,
	part,
	readAs,
	relate,
	select,
	sequence,
	sqlOf,
	update,
} from '../src/statement/index.js';
import { corpusPath, readJson } from './documents.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';
import {
	fn_double,
	fn_greet,
	MemberOfCreate,
	MemberOfTable,
	PersonCreate,
	PersonId,
	PersonMerge,
	type PersonRow,
	PersonTable,
	TeamCreate,
	TeamId,
	TeamTable,
} from './golden/corpus/index.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] statement engine suite skipped: ${engineSkipReason}`,
	);
}

interface CorpusModule {
	readonly path: string;
	readonly engines?: ReadonlyArray<string>;
	readonly experimental?: ReadonlyArray<string>;
}

/** Applies the corpus modules meant for this engine, as its manifest says. */
const applyCorpus = async (engine: Engine): Promise<void> => {
	const minor = /(\d+\.\d+)\./u.exec(engine.version)?.[1] ?? '';
	const { modules } = readJson(corpusPath('corpus.json')) as unknown as {
		readonly modules: ReadonlyArray<CorpusModule>;
	};
	for (const module of modules) {
		if (module.experimental !== undefined) continue;
		if (module.engines !== undefined && !module.engines.includes(minor)) {
			continue;
		}
		await engine.db.query(readFileSync(corpusPath(module.path), 'utf8'));
	}
};

const ok = <A, E>(effect: Effect.Effect<A, E>): Promise<A> =>
	within(Effect.runPromise(effect), 20_000);
const failure = <A, E>(effect: Effect.Effect<A, E>): Promise<E> =>
	within(Effect.runPromise(Effect.flip(effect)), 20_000);

const person = (
	name: string,
	extra: Partial<PersonCreate> = {},
): PersonCreate => ({
	name,
	email: `${name.toLowerCase()}@example.com`,
	bio: null,
	score: 50,
	address: { city: 'Leeds' },
	...extra,
});

const createPerson = create(PersonTable)
	.content(param('person', PersonCreate))
	.build();
const byId = select(PersonTable, param('id', PersonId)).only().build();
const byEmail = select(PersonTable)
	.where((row) => eq(row.email, param('email', SurrealSchema.String)))
	.only()
	.build();

describe.skipIf(!engineConfigured)('statements on a disposable engine', () => {
	let engine: Engine;
	let client: Client;
	let ann: PersonRow;
	let bob: PersonRow;
	let cy: PersonRow;

	beforeAll(async () => {
		engine = await startEngine();
		await applyCorpus(engine);
		await engine.db.query(
			'DEFINE TABLE tally SCHEMAFULL; DEFINE FIELD n ON tally TYPE int; CREATE tally:one SET n = 0;',
		);
		client = makeClient(engine.db);
		ann = await ok(
			client.run(createPerson, {
				person: person('Ann', {
					age: 30,
					nickname: 'an',
					pins: ['a', 'b'],
					phones: [{ kind: 'home', number: '1' }],
				}),
			}),
		);
		bob = await ok(
			client.run(createPerson, {
				person: person('Bob', { address: { city: 'York' } }),
			}),
		);
		cy = await ok(
			client.run(createPerson, { person: person('Cy', { age: 41 }) }),
		);
	});

	afterAll(async () => {
		await engine?.stop();
	});

	it('creates a row and reads it back through the generated schema', async () => {
		expect(ann.id.table.name).toBe('person');
		expect(ann).toMatchObject({
			name: 'Ann',
			age: 30,
			nickname: 'an',
			role: 'member',
			address: { city: 'Leeds' },
		});
		// NONE is an absent key in a stored row.
		expect('age' in bob).toBe(false);
		expect(await ok(client.run(byId, { id: ann.id }))).toEqual(ann);
		expect(
			await ok(client.run(byEmail, { email: 'bob@example.com' })),
		).toEqual(bob);
		expect(
			await ok(client.run(byEmail, { email: 'nobody@example.com' })),
		).toBeUndefined();
	});

	it('filters, sorts and pages with bound values', async () => {
		const page = select(PersonTable)
			.where((row) => gte(row.age, param('minimum', SurrealSchema.Int)))
			.orderBy((row) => row.name, 'DESC')
			.limit(param('rows', SurrealSchema.Int))
			.start(param('skip', SurrealSchema.Int))
			.value((row) => row.name)
			.build();
		expect(
			await ok(client.run(page, { minimum: 18, rows: 5, skip: 0 })),
		).toEqual(['Cy', 'Ann']);
		expect(
			await ok(client.run(page, { minimum: 18, rows: 1, skip: 1 })),
		).toEqual(['Ann']);
		expect(
			await ok(client.run(page, { minimum: 99, rows: 5, skip: 0 })),
		).toEqual([]);
	});

	it('orders by the stored field the callback selected, whatever is projected', async () => {
		// Ann 30, Bob no age, Cy 41: the projection leaves `age` out.
		const byAge = select(PersonTable)
			.orderBy((row) => row.age, 'DESC')
			.project((row) => ({ name: row.name }))
			.build();
		expect(await ok(client.run(byAge))).toEqual([
			{ name: 'Cy' },
			{ name: 'Ann' },
			{ name: 'Bob' },
		]);
		const withAge = select(PersonTable)
			.orderBy((row) => row.age, 'DESC')
			.project((row) => ({ name: row.name, age: row.age }))
			.build();
		expect((await ok(client.run(withAge))).map((row) => row.name)).toEqual([
			'Cy',
			'Ann',
			'Bob',
		]);
		// Nested fields and array elements are paths too.
		const byCity = select(PersonTable)
			.orderBy((row) => row.address.city, 'DESC')
			.orderBy((row) => at(row.pins, 0))
			.orderBy((row) => row.name)
			.value((row) => row.name)
			.build();
		// York before Leeds; then Cy, who has no pins (NONE sorts first),
		// before Ann.
		expect(await ok(client.run(byCity))).toEqual(['Bob', 'Cy', 'Ann']);
		// An alias of the same name over another field would sort by the
		// alias instead: it is refused when the statement is built.
		expect(() =>
			select(PersonTable)
				.orderBy((row) => row.age)
				.project((row) => ({ age: row.name }))
				.build(),
		).toThrow(/alias `age`/);
	});

	it('projects expressions, NONE included', async () => {
		const listing = select(PersonTable)
			.orderBy((row) => row.name)
			.project((row) => ({
				name: row.name,
				nickname: row.nickname,
				city: row.address.city,
				key: cast('string', row.id),
				phone: at(row.phones, 0),
				greeting: call(
					fn_greet,
					row.name,
					param('greeting', SurrealSchema.String),
				),
				doubled: call(fn_double, 21),
			}))
			.build();
		const rows = await ok(client.run(listing, { greeting: 'hi' }));
		expect(rows).toEqual([
			{
				name: 'Ann',
				nickname: 'an',
				city: 'Leeds',
				key: `person:${String(ann.id.id)}`,
				phone: { kind: 'home', number: '1' },
				greeting: 'hi Ann',
				doubled: 42,
			},
			{
				name: 'Bob',
				nickname: undefined,
				city: 'York',
				key: `person:${String(bob.id.id)}`,
				phone: undefined,
				greeting: 'hi Bob',
				doubled: 42,
			},
			{
				name: 'Cy',
				nickname: undefined,
				city: 'Leeds',
				key: `person:${String(cy.id.id)}`,
				phone: undefined,
				greeting: 'hi Cy',
				doubled: 42,
			},
		]);
	});

	it('evaluates conditions as SurrealDB does', async () => {
		const matching = (
			condition: Parameters<
				ReturnType<typeof select<typeof PersonTable>>['where']
			>[0],
		) =>
			ok(
				client.run(
					select(PersonTable)
						.where(condition)
						.orderBy((row) => row.name)
						.value((row) => row.name)
						.build(),
				),
			);
		expect(await matching((row) => isNone(row.age))).toEqual(['Bob']);
		expect(await matching((row) => not(isNone(row.age)))).toEqual([
			'Ann',
			'Cy',
		]);
		expect(
			await matching((row) => inside(row.name, ['Ann', 'Zed'])),
		).toEqual(['Ann']);
		expect(await matching((row) => contains(row.pins, 'b'))).toEqual([
			'Ann',
		]);
		expect(
			await matching((row) =>
				and(
					eq(row.role, 'member'),
					or(eq(row.address.city, 'York'), gte(row.age, 40)),
				),
			),
		).toEqual(['Bob', 'Cy']);
		expect(await matching((row) => eq(row.bio, null))).toHaveLength(3);
	});

	it('compares record ids, bound or inlined through their codec', async () => {
		const named = (id: ReturnType<typeof param<'id', typeof PersonId>>) =>
			select(PersonTable)
				.where((row) => eq(row.id, id))
				.value((row) => row.name)
				.build();
		expect(
			await ok(client.run(named(param('id', PersonId)), { id: ann.id })),
		).toEqual(['Ann']);
		const inlined = select(PersonTable)
			.where((row) => eq(row.id, literal(bob.id, PersonId)))
			.value((row) => row.name)
			.build();
		expect(inlined.sql).toContain('id = r"person:');
		expect(await ok(client.run(inlined))).toEqual(['Bob']);
	});

	it('nests a query as an expression', async () => {
		const peers = select(PersonTable)
			.where((row) => eq(row.name, 'Bob'))
			.project((row) => ({
				name: row.name,
				inLeeds: select(PersonTable)
					.where((peer) =>
						eq(
							peer.address.city,
							param('city', SurrealSchema.String),
						),
					)
					.orderBy((peer) => peer.name)
					.value((peer) => peer.name)
					.toExpr(),
			}))
			.build();
		expect(await ok(client.run(peers, { city: 'Leeds' }))).toEqual([
			{ name: 'Bob', inLeeds: ['Ann', 'Cy'] },
		]);
	});

	it('updates one record by id, or NONE when it is not there', async () => {
		const mergeOne = update(PersonTable, param('id', PersonId))
			.merge(param('changes', PersonMerge))
			.build();
		const changed = await ok(
			client.run(mergeOne, {
				id: cy.id,
				// `undefined` is NONE: it removes an `option<T>` field.
				changes: { age: 42, nickname: 'cyrus' },
			}),
		);
		expect(changed).toMatchObject({
			name: 'Cy',
			age: 42,
			nickname: 'cyrus',
		});
		const cleared = await ok(
			client.run(mergeOne, {
				id: cy.id,
				changes: { nickname: undefined },
			}),
		);
		expect(cleared !== undefined && 'nickname' in cleared).toBe(false);
		expect(cleared?.age).toBe(42);

		const missing = Schema.decodeUnknownSync(PersonId)(
			new RecordId('person', 'nobody'),
		);
		expect(
			await ok(
				client.run(mergeOne, { id: missing, changes: { age: 1 } }),
			),
		).toBeUndefined();
		expect(await ok(client.run(byId, { id: missing }))).toBeUndefined();
	});

	it('updates every match and returns what was asked for', async () => {
		const promote = update(PersonTable)
			.merge(param('changes', PersonMerge))
			.where((row) =>
				eq(row.address.city, param('city', SurrealSchema.String)),
			)
			.returning((row) => ({ name: row.name, role: row.role }))
			.build();
		const promoted = await ok(
			client.run(promote, { city: 'York', changes: { role: 'admin' } }),
		);
		expect(promoted).toEqual([{ name: 'Bob', role: 'admin' }]);

		const quietly = update(PersonTable)
			.merge(param('changes', PersonMerge))
			.where((row) => eq(row.name, 'Bob'))
			.returning('none')
			.build();
		expect(
			await ok(client.run(quietly, { changes: { role: 'member' } })),
		).toEqual([]);
		expect((await ok(client.run(byId, { id: bob.id })))?.role).toBe(
			'member',
		);
	});

	it('relates two records and deletes them', async () => {
		const team = await ok(
			client.run(
				create(TeamTable).content(param('team', TeamCreate)).build(),
				{ team: { name: 'core' } },
			),
		);
		const join = relate(
			MemberOfTable,
			param('person', PersonId),
			param('team', TeamId),
		)
			.content(param('membership', MemberOfCreate))
			.build();
		const edge = await ok(
			client.run(join, {
				person: ann.id,
				team: team.id,
				membership: { role: 'lead' },
			}),
		);
		expect(edge.in.equals(ann.id)).toBe(true);
		expect(edge.out.equals(team.id)).toBe(true);
		expect(edge.role).toBe('lead');

		const removeEdge = deleteFrom(MemberOfTable)
			.where((row) => eq(row.in, param('person', PersonId)))
			.returning('before')
			.build();
		expect(await ok(client.run(removeEdge, { person: ann.id }))).toEqual([
			edge,
		]);
		expect(await ok(client.run(removeEdge, { person: ann.id }))).toEqual(
			[],
		);

		const removeTeam = deleteFrom(TeamTable, param('id', TeamId))
			.returning('before')
			.build();
		expect(await ok(client.run(removeTeam, { id: team.id }))).toMatchObject(
			{
				name: 'core',
			},
		);
		expect(
			await ok(client.run(removeTeam, { id: team.id })),
		).toBeUndefined();
		expect(
			await ok(
				client.run(deleteFrom(TeamTable, param('id', TeamId)).build(), {
					id: team.id,
				}),
			),
		).toBeUndefined();
	});

	const findByEmail = sequence(
		part({
			sql: 'LET $person = (SELECT VALUE id FROM ONLY person WHERE email = $boundEmail LIMIT 1);',
			free: ['boundEmail'],
			defines: ['person'],
		}),
		part({
			sql: "IF $person = NONE { THROW 'No such person' };",
			free: ['person'],
			throws: ['No such person'],
		}),
		part({ sql: 'RETURN $person;', free: ['person'] }),
	).define({
		name: 'find by email',
		params: { boundEmail: SurrealSchema.String },
		result: PersonId,
	});

	it('runs authored SurrealQL and maps its THROW to a typed error', async () => {
		const found = await ok(
			client.run(findByEmail, { boundEmail: 'ann@example.com' }),
		);
		expect(found.equals(ann.id)).toBe(true);

		const thrown = await failure(
			client.run(findByEmail, { boundEmail: 'nobody@example.com' }),
		);
		expect(thrown).toBeInstanceOf(StatementThrown);
		expect(thrown).toMatchObject({
			statement: 'find by email',
			reason: 'No such person',
		});

		const undeclared = make({
			sql: "THROW 'Unexpected';",
			params: {},
			result: Schema.Undefined,
		});
		const unexpected = await failure(client.run(undeclared));
		expect(unexpected).toBeInstanceOf(StatementUnexpectedThrow);
		expect(unexpected).toMatchObject({ reason: 'Unexpected' });
	});

	it('applies a transaction entirely or not at all', async () => {
		const createTeam = create(TeamTable)
			.content(param('team', TeamCreate))
			.build();
		const teams = select(TeamTable)
			.where((row) => eq(row.name, param('name', SurrealSchema.String)))
			.value((row) => row.name)
			.build();

		const [team, found] = await ok(
			client.transaction([
				bind(createTeam, { team: { name: 'kept' } }),
				bind(findByEmail, { boundEmail: 'ann@example.com' }),
			]),
		);
		expect(team.name).toBe('kept');
		expect(found.equals(ann.id)).toBe(true);
		expect(await ok(client.run(teams, { name: 'kept' }))).toEqual(['kept']);

		const thrown = await failure(
			client.transaction([
				bind(createTeam, { team: { name: 'ghost' } }),
				bind(findByEmail, { boundEmail: 'nobody@example.com' }),
			]),
		);
		expect(thrown).toBeInstanceOf(StatementThrown);
		// The statement that declares the message, not the transaction.
		expect(thrown).toMatchObject({ statement: 'find by email' });
		expect(await ok(client.run(teams, { name: 'ghost' }))).toEqual([]);
	});

	it('joins statements without a terminator of their own', async () => {
		const one = make({
			sql: 'RETURN 1',
			params: {},
			result: SurrealSchema.Int,
		});
		const two = make({ sql: '2', params: {}, result: SurrealSchema.Int });
		expect(await ok(client.transaction([bind(two), bind(one)]))).toEqual([
			2, 1,
		]);
	});

	it('refuses a binding another statement of the transaction defines', async () => {
		const letter = sequence(
			part({ sql: 'LET $n = 1;', defines: ['n'] }),
			part({ sql: '$n;', free: ['n'] }),
		).define({ name: 'letter', params: {}, result: SurrealSchema.Int });
		const reader = make({
			name: 'reader',
			sql: '$n;',
			params: { n: SurrealSchema.Int },
			result: SurrealSchema.Int,
		});
		expect(await ok(client.run(letter))).toBe(1);
		expect(await ok(client.run(reader, { n: 2 }))).toBe(2);
		const error = await failure(
			client.transaction([bind(letter), bind(reader, { n: 2 })]),
		);
		expect(error).toBeInstanceOf(StatementScopeError);
		expect(error).toMatchObject({ statement: 'reader', binding: 'n' });
	});

	it('refuses a statement with its own BEGIN … COMMIT inside a transaction', async () => {
		const own = sequence(
			part({
				sql: 'BEGIN; LET $x = 2; COMMIT;',
				defines: ['x'],
				slots: 3,
			}),
		).define({ name: 'own', params: {}, result: Schema.Undefined });
		// It runs on its own (the COMMIT is the last result, NONE).
		expect(await ok(client.run(own))).toBeUndefined();
		const error = await failure(client.transaction([bind(own)]));
		expect(error).toBeInstanceOf(StatementScopeError);
		expect(error).toMatchObject({ statement: 'own' });
	});

	it('reports a transaction conflict, which a retry resolves', async () => {
		const bump = make({
			name: 'bump tally',
			sql: 'LET $current = (SELECT VALUE n FROM ONLY tally:one); SLEEP 20ms; UPDATE tally:one SET n = $current + 1 RETURN NONE;',
			params: {},
			result: Schema.Tuple([]),
			slots: 3,
			defines: ['current'],
		});
		const read = make({
			sql: 'SELECT VALUE n FROM ONLY tally:one;',
			params: {},
			result: SurrealSchema.Int,
		});
		const attempt = client.transaction([bind(bump)]);
		const outcomes = await ok(
			Effect.all(
				Array.from({ length: 6 }, () => Effect.result(attempt)),
				{ concurrency: 'unbounded' },
			),
		);
		const failures = outcomes.flatMap((outcome) =>
			outcome._tag === 'Failure' ? [outcome.failure] : [],
		);
		expect(failures.length).toBeGreaterThan(0);
		for (const error of failures) {
			expect(error).toBeInstanceOf(StatementConflict);
		}
		const before = await ok(client.run(read));
		await ok(
			Effect.all(
				Array.from({ length: 4 }, () =>
					attempt.pipe(
						Effect.retry({
							while: (error) =>
								error instanceof StatementConflict,
						}),
					),
				),
				{ concurrency: 'unbounded' },
			),
		);
		expect(await ok(client.run(read))).toBe(before + 4);
		// The same, with the client's own retry: only conflicts are resent.
		await ok(
			Effect.all(
				Array.from({ length: 4 }, () =>
					client.transaction([bind(bump)], {
						retry: Schedule.forever,
					}),
				),
				{ concurrency: 'unbounded' },
			),
		);
		expect(await ok(client.run(read))).toBe(before + 8);
	});

	it('never sends a value its codec refuses', async () => {
		const older = select(PersonTable)
			.where((row) => gte(row.age, param('minimum', SurrealSchema.Int)))
			.build();
		const error = await failure(client.run(older, { minimum: 2 ** 60 }));
		expect(error).toBeInstanceOf(StatementEncodeError);
		expect(error).toMatchObject({ binding: 'minimum' });
	});

	it('refuses rows that are not what the statement declares', async () => {
		// A row schema that leaves out stored fields does not describe the
		// rows `SELECT *` returns: project the fields instead.
		const Narrow = Schema.Struct({
			id: PersonId,
			name: SurrealSchema.String,
		});
		const narrow = readAs(PersonTable, Narrow);
		const error = await failure(client.run(select(narrow).build()));
		expect(error).toBeInstanceOf(StatementDecodeError);
		expect(
			await ok(
				client.run(
					select(narrow)
						.where((row) => eq(row.name, 'Ann'))
						.project((row) => ({ id: row.id, name: row.name }))
						.build(),
				),
			),
		).toEqual([{ id: ann.id, name: 'Ann' }]);
	});

	it('stops waiting when the effect is interrupted', async () => {
		const slow = make({
			sql: 'SLEEP 10s; RETURN 1;',
			params: {},
			result: SurrealSchema.Int,
			slots: 2,
		});
		const started = Date.now();
		const outcome = await ok(
			client.run(slow).pipe(Effect.timeoutOption('200 millis')),
		);
		expect(outcome._tag).toBe('None');
		expect(Date.now() - started).toBeLessThan(5000);
	});

	it('writes a datetime constant the engine reads as the same instant as the binding', async () => {
		// Finding 6: instants before 1970 with a fractional second.
		const instants = Arbitrary.all([
			Arbitrary.schema(
				Schema.BigInt.check(
					Schema.isBetweenBigInt({
						minimum: SurrealSchema.DATETIME_MIN_SECONDS,
						maximum: -1n,
					}),
				),
			),
			Arbitrary.schema(
				Schema.Number.check(
					Schema.isInt(),
					Schema.isBetween({ minimum: 1, maximum: 999_999_999 }),
				),
			),
		]);
		const result = await within(
			Effect.runPromise(
				Arbitrary.checkEffect(
					instants,
					([seconds, nanos]) =>
						Effect.gen(function* () {
							const instant = new DateTime([
								seconds,
								BigInt(nanos),
							]);
							const same = make({
								sql: `RETURN ${sqlOf(literal(instant, SurrealSchema.DateTime))} = $instant;`,
								params: { instant: SurrealSchema.DateTime },
								result: Schema.Boolean,
							});
							return yield* client.run(same, { instant });
						}),
					{ runs: 100, seed: 20261119 },
				),
			),
			60_000,
		);
		expect(Arbitrary.formatCheckFailure(result) ?? 'Passed').toBe('Passed');
	});

	it('reads any table name as a name in every subject position', async () => {
		// Finding 7: `from` and `ONLY` were left bare, and failed to parse.
		const awkward = [
			'from',
			'ONLY',
			'only',
			'select',
			'none',
			'NONE',
			'value',
			'odd.name',
			'odd-name',
			'ünïcödé',
			'1st',
			'tick`name',
			'angle⟩name',
			'back\\slash',
		];
		const node = (id: string) => new RecordId('node', id);
		await engine.db.query('CREATE node:a; CREATE node:b;');
		for (const name of awkward) {
			const Id = SurrealSchema.RecordIdFor(name);
			const Row = Schema.Struct({
				id: Id,
				v: SurrealSchema.Int,
				w: Schema.optionalKey(SurrealSchema.Int),
			});
			const Data = Schema.Struct({ v: SurrealSchema.Int });
			const table = {
				name,
				id: Id,
				select: Row,
				create: Data,
				merge: Schema.Struct({ w: SurrealSchema.Int }),
			};
			const made = await ok(
				client.run(create(table).content(param('data', Data)).build(), {
					data: { v: 1 },
				}),
			);
			expect(made.id.table.name).toBe(name);
			await ok(
				client.run(
					create(table)
						.content(param('data', Data))
						.returning('none')
						.build(),
					{ data: { v: 2 } },
				),
			);
			const all = select(table)
				.orderBy((row) => row.v)
				.value((row) => row.v)
				.build();
			expect(await ok(client.run(all))).toEqual([1, 2]);
			const one = select(table)
				.where((row) => eq(row.v, 1))
				.only()
				.build();
			expect((await ok(client.run(one)))?.id.equals(made.id)).toBe(true);
			const changed = update(table)
				.merge(param('changes', table.merge))
				.where((row) => eq(row.v, 1))
				.build();
			expect(
				await ok(client.run(changed, { changes: { w: 3 } })),
			).toMatchObject([{ v: 1, w: 3 }]);
			const removed = deleteFrom(table)
				.where((row) => eq(row.v, 2))
				.returning('before')
				.build();
			expect(await ok(client.run(removed))).toMatchObject([{ v: 2 }]);
			await ok(client.run(deleteFrom(table).build()));

			const NodeId = SurrealSchema.RecordIdFor('node');
			const edge = {
				name,
				id: Id,
				select: Schema.Struct({
					id: Id,
					in: NodeId,
					out: NodeId,
					v: SurrealSchema.Int,
				}),
				create: Data,
				edge: { table: name, in: [NodeId], out: [NodeId] },
			};
			const linked = await ok(
				client.run(
					relate(edge, param('from', NodeId), param('to', NodeId))
						.content(param('data', Data))
						.build(),
					{ from: node('a'), to: node('b'), data: { v: 4 } },
				),
			);
			expect(linked.id.table.name).toBe(name);
			expect(linked.in.equals(node('a'))).toBe(true);
		}
	});

	it('reports a create or relate a record user may not make as an empty result, and leaves an update by condition alone', async () => {
		// `person` lets a record user create only itself; `member_of`
		// declares no permissions, so a record user may not write it.
		await engine.db.query(`
			DEFINE TABLE account SCHEMAFULL PERMISSIONS FULL;
			DEFINE FIELD name ON account TYPE string;
			DEFINE ACCESS member ON DATABASE TYPE RECORD
				SIGNUP (CREATE account SET name = $name)
				SIGNIN (SELECT * FROM account WHERE name = $name);
		`);
		const session = await within(engine.db.newSession());
		try {
			const scope = {
				namespace: engine.namespace,
				database: engine.namespace,
			};
			await within(session.use(scope));
			await within(
				session.signup({
					...scope,
					access: 'member',
					variables: { name: 'dee' },
				}),
			);
			const user = makeClient(session);

			const created = await failure(
				user.run(createPerson, { person: person('Dee') }),
			);
			expect(created).toBeInstanceOf(StatementEmptyResult);
			expect(created).toMatchObject({ statement: 'create person' });
			expect(created.message).toMatch(/may lack permission/);

			const team = await ok(
				client.run(
					create(TeamTable)
						.content(param('team', TeamCreate))
						.build(),
					{ team: { name: 'guarded' } },
				),
			);
			const join = relate(
				MemberOfTable,
				param('person', PersonId),
				param('team', TeamId),
			)
				.content(param('membership', MemberOfCreate))
				.build();
			const related = await failure(
				user.run(join, {
					person: ann.id,
					team: team.id,
					membership: { role: 'lead' },
				}),
			);
			expect(related).toBeInstanceOf(StatementEmptyResult);

			// An update by condition may match nothing: still a success.
			const rename = update(PersonTable)
				.merge(param('changes', PersonMerge))
				.where((row) => eq(row.name, 'Ann'))
				.build();
			expect(
				await ok(user.run(rename, { changes: { nickname: 'x' } })),
			).toEqual([]);

			// Nothing was written.
			expect(
				await ok(client.run(byEmail, { email: 'dee@example.com' })),
			).toBeUndefined();
			expect(await ok(client.run(byId, { id: ann.id }))).toMatchObject({
				nickname: 'an',
			});
		} finally {
			await session.closeSession();
		}
	});

	it('says a write that did not decode is committed', async () => {
		const Wrong = Schema.Struct({ id: PersonId, name: SurrealSchema.Int });
		const wrong = create(readAs(PersonTable, Wrong))
			.content(param('person', PersonCreate))
			.returning((row) => ({ id: row.id, name: row.name }))
			.build();
		const error = await failure(
			client.run(wrong, { person: person('Eve') }),
		);
		expect(error).toBeInstanceOf(StatementDecodeError);
		expect(error).toMatchObject({ committed: true });
		expect(error.message).toMatch(/writes are committed/);
		expect(
			await ok(client.run(byEmail, { email: 'eve@example.com' })),
		).toMatchObject({ name: 'Eve' });
	});

	it('reads an unbound parameter as NONE, which is why bindings are checked', async () => {
		// The engine does not report a parameter nothing bound: a misspelt
		// binding would quietly change what a statement does.
		const unbound = make({
			sql: 'RETURN $neverBound = NONE;',
			params: {},
			result: Schema.Boolean,
		});
		expect(await ok(client.run(unbound))).toBe(true);
	});
});
