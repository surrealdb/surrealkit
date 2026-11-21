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
	StatementWriteOutcomeError,
} from '../src/client/index.js';
import * as SurrealSchema from '../src/index.js';
import {
	and,
	arrayGroup,
	at,
	call,
	cast,
	contains,
	count,
	create,
	deleteFrom,
	eq,
	gte,
	type IdRange,
	idRange,
	insert,
	insertRelation,
	inside,
	isNone,
	literal,
	make,
	max,
	mean,
	median,
	min,
	not,
	or,
	param,
	part,
	readAs,
	recordId,
	relate,
	select,
	sequence,
	sqlOf,
	sum,
	timeMin,
	update,
	upsert,
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
	IdArrayTable,
	IdIntTable,
	IdObjectTable,
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

	it.each([
		'single',
		'transaction',
	])('reconciles a successful %s write after decoding fails', async (mode) => {
		const record = new RecordId('tally', mode);
		await engine.db.query('CREATE $record SET n = 0', { record });
		const write = make({
			name: 'increment',
			sql: 'UPDATE ONLY $record SET n += 1 RETURN VALUE n;',
			params: { record: SurrealSchema.RecordIdAny },
			result: Schema.String,
		});
		const error = await failure(
			mode === 'single'
				? client.run(write, { record }).pipe(Effect.asVoid)
				: client
						.transaction([bind(write, { record })])
						.pipe(Effect.asVoid),
		);
		expect(error).toBeInstanceOf(StatementWriteOutcomeError);
		expect(error).toMatchObject({
			cause: { statement: 'increment', slot: 1 },
		});
		expect(
			await engine.db.query('SELECT VALUE n FROM ONLY $record', {
				record,
			}),
		).toEqual([1]);
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
		part({ sql: '$person;', free: ['person'] }),
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

	it('returns the value before COMMIT of a script that manages its transaction', async () => {
		await engine.db.query(
			'DEFINE TABLE slot_entry SCHEMAFULL; DEFINE FIELD n ON slot_entry TYPE int;',
		);
		const record = sequence(
			part({ sql: 'BEGIN TRANSACTION;' }),
			part({
				sql: "IF $n < 0 { THROW 'Negative' };",
				free: ['n'],
				throws: ['Negative'],
			}),
			part({
				sql: 'CREATE ONLY slot_entry SET n = $n RETURN VALUE n;',
				free: ['n'],
			}),
			part({ sql: 'COMMIT TRANSACTION;' }),
		).define({
			name: 'record',
			params: { n: SurrealSchema.Int },
			result: SurrealSchema.Int,
		});
		expect(record.valueSlot).toBe(3);
		const stored = () =>
			engine.db.query('SELECT VALUE n FROM slot_entry ORDER BY n');

		expect(await ok(client.run(record, { n: 5 }))).toBe(5);
		expect(await stored()).toEqual([[5]]);

		const thrown = await failure(client.run(record, { n: -1 }));
		expect(thrown).toBeInstanceOf(StatementThrown);
		expect(thrown).toMatchObject({
			statement: 'record',
			reason: 'Negative',
		});
		expect(await stored()).toEqual([[5]]);
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

	it('isolates a bound name from another statement’s top-level LET', async () => {
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
		expect(
			await ok(
				client.transaction([bind(letter), bind(reader, { n: 2 })]),
			),
		).toEqual([1, 2]);
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
		expect(error).toBeInstanceOf(StatementWriteOutcomeError);
		const cause = (error as StatementWriteOutcomeError).cause;
		expect(cause).toBeInstanceOf(StatementDecodeError);
		expect(cause).toMatchObject({ committed: true });
		expect(cause.message).toMatch(/writes are committed/);
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

	describe('record ids from their key parts, and id ranges', () => {
		const at = (iso: string) => new DateTime(iso);
		const intId = recordId(IdIntTable, param('n', SurrealSchema.Int));
		const createInt = create(IdIntTable, intId).build('create id_int');
		const readInt = select(IdIntTable, intId).only().build('read id_int');
		const deleteInt = deleteFrom(IdIntTable, intId)
			.returning('before')
			.build('delete id_int');
		const cityAt = recordId(IdArrayTable, [
			param('city', SurrealSchema.String),
			param('at', SurrealSchema.DateTime),
		]);
		const regionN = recordId(IdObjectTable, {
			n: param('n', SurrealSchema.Int),
			region: param('region', SurrealSchema.String),
		});

		beforeAll(async () => {
			for (const n of [1, 2, 3, 4, 5]) {
				await ok(client.run(createInt, { n }));
			}
			const createCity = create(IdArrayTable, cityAt).build();
			for (const [city, iso] of [
				['leeds', '2024-01-01T00:00:00Z'],
				['leeds', '2024-01-02T00:00:00Z'],
				['leeds', '2024-01-03T00:00:00Z'],
				['york', '2024-01-02T00:00:00Z'],
			] as const) {
				await ok(client.run(createCity, { city, at: at(iso) }));
			}
		});

		it('creates, reads and deletes a record by a key built from a binding', async () => {
			const read = await ok(client.run(readInt, { n: 3 }));
			expect(read?.id.id).toBe(3);
			expect(read?.id.table.name).toBe('id_int');
			expect(await ok(client.run(readInt, { n: 99 }))).toBe(undefined);
			const created = await ok(client.run(createInt, { n: 6 }));
			expect(created.id.id).toBe(6);
			expect((await ok(client.run(deleteInt, { n: 6 })))?.id.id).toBe(6);
			expect(await ok(client.run(readInt, { n: 6 }))).toBe(undefined);
		});

		it('reads by an array key and an object key, part by part', async () => {
			const row = await ok(
				client.run(select(IdArrayTable, cityAt).only().build(), {
					city: 'leeds',
					at: at('2024-01-02T00:00:00Z'),
				}),
			);
			expect(row?.id.id[0]).toBe('leeds');
			const object = create(IdObjectTable, regionN).build();
			const made = await ok(client.run(object, { n: 7, region: 'eu' }));
			expect(made.id.id).toEqual({ n: 7, region: 'eu' });
			const updated = await ok(
				client.run(
					update(IdObjectTable, regionN)
						.merge(param('changes', IdObjectTable.merge))
						.build(),
					{ n: 7, region: 'eu', changes: {} },
				),
			);
			expect(updated?.id.id).toEqual({ n: 7, region: 'eu' });
		});

		it('relates records by keys built from bindings', async () => {
			const keyed = create(
				PersonTable,
				recordId(PersonTable, param('key', SurrealSchema.String)),
			)
				.content(param('person', PersonCreate))
				.build();
			await ok(client.run(keyed, { key: 'dot', person: person('Dot') }));
			await ok(
				client.run(
					create(TeamTable, recordId(TeamTable, 'keyed'))
						.content(param('team', TeamCreate))
						.build(),
					{ team: { name: 'Keyed' } },
				),
			);
			const edge = await ok(
				client.run(
					relate(
						MemberOfTable,
						recordId(
							PersonTable,
							param('key', SurrealSchema.String),
						),
						recordId(
							TeamTable,
							param('team', SurrealSchema.String),
						),
					)
						.content(param('membership', MemberOfCreate))
						.build(),
					{ key: 'dot', team: 'keyed', membership: {} },
				),
			);
			expect([edge.in.id, edge.out.id]).toEqual(['dot', 'keyed']);
		});

		/** The keys of the rows a range of `id_int` reads, in key order. */
		const keysOf = async (
			range: IdRange<'id_int'>,
			input: Readonly<Record<string, number>>,
		): Promise<ReadonlyArray<number>> => {
			const statement = select(IdIntTable, range)
				.orderBy((row) => row.id)
				.build();
			const rows = await ok(client.run(statement, input));
			return rows.map((row) => row.id.id);
		};

		it('reads the records a range spans: both bounds, an open side, an excluded start', async () => {
			const low = param('low', SurrealSchema.Int);
			const high = param('high', SurrealSchema.Int);
			expect(
				await keysOf(idRange(IdIntTable, { from: low, to: high }), {
					low: 2,
					high: 4,
				}),
			).toEqual([2, 3]);
			expect(
				await keysOf(
					idRange(IdIntTable, { from: low, through: high }),
					{
						low: 2,
						high: 4,
					},
				),
			).toEqual([2, 3, 4]);
			expect(
				await keysOf(idRange(IdIntTable, { after: low }), { low: 3 }),
			).toEqual([4, 5]);
			expect(
				await keysOf(idRange(IdIntTable, { through: high }), {
					high: 2,
				}),
			).toEqual([1, 2]);
			expect(await keysOf(idRange(IdIntTable, {}), {})).toEqual([
				1, 2, 3, 4, 5,
			]);
		});

		it('reads an array-key range within one leading part, as many rows or with ONLY one', async () => {
			const week = select(
				IdArrayTable,
				idRange(IdArrayTable, {
					from: [
						param('city', SurrealSchema.String),
						param('start', SurrealSchema.DateTime),
					],
					through: [
						param('city', SurrealSchema.String),
						param('end', SurrealSchema.DateTime),
					],
				}),
			);
			const input = {
				city: 'leeds',
				start: at('2024-01-02T00:00:00Z'),
				end: at('2024-01-03T00:00:00Z'),
			};
			const rows = await ok(client.run(week.build(), input));
			expect(rows.map((row) => row.id.id[1].toString()).sort()).toEqual([
				at('2024-01-02T00:00:00Z').toString(),
				at('2024-01-03T00:00:00Z').toString(),
			]);
			expect(rows.every((row) => row.id.id[0] === 'leeds')).toBe(true);
			const one = await ok(client.run(week.only().build(), input));
			expect(one?.id.id[0]).toBe('leeds');
		});
	});

	describe('UPSERT, INSERT and every RETURN mode', () => {
		const Upserted = Schema.Struct({
			address: PersonCreate.fields.address,
			bio: PersonCreate.fields.bio,
			email: PersonCreate.fields.email,
			name: PersonCreate.fields.name,
			score: PersonCreate.fields.score,
		});
		const key = recordId(PersonTable, param('key', SurrealSchema.String));
		const upserted = (name: string) => ({
			address: { city: 'Leeds' },
			bio: null,
			email: `${name.toLowerCase()}@upsert.example.com`,
			name,
			score: 10,
		});

		it('UPSERT of a record creates it, then changes it, in every RETURN mode', async () => {
			const one = upsert(PersonTable, key).merge(
				param('changes', Upserted),
			);
			const created = await ok(
				client.run(one.build(), {
					key: 'up1',
					changes: upserted('Up'),
				}),
			);
			expect(created).toMatchObject({ name: 'Up', score: 10 });
			const before = await ok(
				client.run(one.returning('before').build(), {
					key: 'up1',
					changes: { ...upserted('Up'), score: 20 },
				}),
			);
			expect(before).toMatchObject({ score: 10 });
			expect(
				await ok(
					client.run(one.returning('before').build(), {
						key: 'up2',
						changes: upserted('Up2'),
					}),
				),
			).toBe(undefined);
			// A MERGE of some fields of an existing record (the types ask for
			// what a create needs, so it is cast): with RETURN VALUE NONE it
			// passes on SurrealDB 3.3, which refuses it with RETURN NONE.
			const partial = upsert(PersonTable, key)
				.merge(param('changes', PersonMerge) as never)
				.returning('none')
				.build();
			expect(
				await ok(
					client.run(partial, {
						key: 'up1',
						changes: { score: 30 },
					} as never),
				),
			).toBe(undefined);
			expect(
				(
					await ok(
						client.run(select(PersonTable, key).only().build(), {
							key: 'up1',
						}),
					)
				)?.score,
			).toBe(30);
			const diff = await ok(
				client.run(one.returning('diff').build(), {
					key: 'up1',
					changes: { ...upserted('Up'), score: 40 },
				}),
			);
			// Fields the corpus sets on every write (`touched_at`) change too.
			expect(diff).toContainEqual({
				op: 'replace',
				path: '/score',
				value: 40,
			});
			expect(
				await ok(
					client.run(one.returningValue((row) => row.score).build(), {
						key: 'up1',
						changes: { ...upserted('Up'), score: 41 },
					}),
				),
			).toBe(41);
		});

		it('UPSERT of a table changes every match, or creates one record', async () => {
			const byEmailUpsert = upsert(PersonTable)
				.merge(param('changes', Upserted))
				.where((row) =>
					eq(row.email, param('email', SurrealSchema.String)),
				)
				.returningValue((row) => row.name)
				.build();
			const fresh = upserted('Fresh');
			expect(
				await ok(
					client.run(byEmailUpsert, {
						changes: fresh,
						email: fresh.email,
					}),
				),
			).toEqual(['Fresh']);
			expect(
				await ok(
					client.run(byEmailUpsert, {
						changes: { ...fresh, name: 'Fresher' },
						email: fresh.email,
					}),
				),
			).toEqual(['Fresher']);
		});

		it('INSERT one row or many; ON DUPLICATE KEY UPDATE from $input; BEFORE is NONE for a new row', async () => {
			const insertOne = insert(PersonTable, param('person', PersonCreate))
				.returningValue((row) => row.name)
				.build();
			expect(
				await ok(client.run(insertOne, { person: person('Ivy') })),
			).toEqual(['Ivy']);
			const many = insert(
				PersonTable,
				param('people', Schema.Array(PersonCreate)),
			).build();
			const rows = await ok(
				client.run(many, { people: [person('Jo'), person('Kai')] }),
			);
			expect(rows.map((row) => row.name)).toEqual(['Jo', 'Kai']);
			const duplicate = insert(
				PersonTable,
				param('people', Schema.Array(PersonCreate)),
			)
				.onDuplicate((input) => ({ score: input.score }))
				.returning('before')
				.build();
			const before = await ok(
				client.run(duplicate, {
					people: [{ ...person('Ivy'), score: 77 }, person('Lou')],
				}),
			);
			expect(before[0]).toMatchObject({ name: 'Ivy', score: 50 });
			expect(before[1]).toBe(undefined);
			expect(
				await ok(client.run(byEmail, { email: 'ivy@example.com' })),
			).toMatchObject({ score: 77 });
		});

		it('INSERT RELATION takes edges with their endpoints', async () => {
			const team = await ok(
				client.run(
					create(TeamTable)
						.content(param('team', TeamCreate))
						.build(),
					{ team: { name: 'Inserted' } },
				),
			);
			const edges = insertRelation(
				MemberOfTable,
				param(
					'edges',
					Schema.Array(
						Schema.Struct({
							in: PersonId,
							out: TeamId,
							...MemberOfCreate.fields,
						}),
					),
				),
			).build();
			const made = await ok(
				client.run(edges, {
					edges: [
						{ in: ann.id, out: team.id },
						{ in: bob.id, out: team.id },
					],
				}),
			);
			expect(made.map((edge) => edge.in)).toEqual([ann.id, bob.id]);
		});

		it('DIFF decodes as JSON Patch operations, on UPDATE, CREATE and DELETE', async () => {
			const created = await ok(
				client.run(
					create(PersonTable)
						.content(param('person', PersonCreate))
						.returning('diff')
						.build(),
					{ person: person('Dia', { nickname: 'd' }) },
				),
			);
			expect(created).toHaveLength(1);
			expect(created[0]).toMatchObject({ op: 'replace', path: '' });
			const id = (created[0] as unknown as { value: { id: PersonId } })
				.value.id;
			const changed = await ok(
				client.run(
					update(PersonTable, param('id', PersonId))
						.merge(param('patch', PersonMerge))
						.returning('diff')
						.build(),
					{
						id,
						patch: { nickname: undefined, age: 3, name: 'Diana' },
					},
				),
			);
			expect(changed).toContainEqual({
				op: 'add',
				path: '/age',
				value: 3,
			});
			expect(changed).toContainEqual({ op: 'remove', path: '/nickname' });
			expect(changed?.find((op) => op.path === '/name')?.op).toBe(
				'change',
			);
			const deleted = await ok(
				client.run(
					deleteFrom(PersonTable, param('id', PersonId))
						.returning('diff')
						.build(),
					{ id },
				),
			);
			expect(deleted).toEqual([
				{ op: 'replace', path: '', value: undefined },
			]);
		});

		it('DELETE returns NONE after, and the deleted record through $before', async () => {
			const remove = (name: string) =>
				ok(client.run(createPerson, { person: person(name) }));
			const gone = await remove('Gus');
			const after = await ok(
				client.run(
					deleteFrom(PersonTable)
						.where((row) => eq(row.id, param('id', PersonId)))
						.returning('after')
						.build(),
					{ id: gone.id },
				),
			);
			expect(after).toEqual([undefined]);
			const kept = await remove('Hal');
			expect(
				await ok(
					client.run(
						deleteFrom(PersonTable, param('id', PersonId))
							.returning((row) => ({
								name: row.name,
								email: row.email,
							}))
							.build(),
						{ id: kept.id },
					),
				),
			).toEqual({ name: 'Hal', email: 'hal@example.com' });
			const last = await remove('Ida');
			expect(
				await ok(
					client.run(
						deleteFrom(PersonTable, param('id', PersonId))
							.returningValue((row) => row.name)
							.build(),
						{ id: last.id },
					),
				),
			).toBe('Ida');
		});

		it('RETURN VALUE on CREATE and RELATE', async () => {
			const name = await ok(
				client.run(
					create(PersonTable)
						.content(param('person', PersonCreate))
						.returningValue((row) => row.name)
						.build(),
					{ person: person('Val') },
				),
			);
			expect(name).toBe('Val');
			const team = await ok(
				client.run(
					create(TeamTable)
						.content(param('team', TeamCreate))
						.build(),
					{ team: { name: 'Valued' } },
				),
			);
			const edgeIn = await ok(
				client.run(
					relate(
						MemberOfTable,
						param('p', PersonId),
						param('t', TeamId),
					)
						.content(param('m', MemberOfCreate))
						.returningValue((row) => row.in)
						.build(),
					{ p: cy.id, t: team.id, m: {} },
				),
			);
			expect(edgeIn).toEqual(cy.id);
		});
	});

	describe('GROUP BY and GROUP ALL', () => {
		it('groups by keys with typed aggregates, decoded strictly', async () => {
			const statement = select(PersonTable)
				.groupBy((row) => ({ city: row.address.city }))
				.aggregate((row) => ({
					people: count(),
					total: sum(row.score),
					average: mean(row.score),
					oldest: max(row.age),
					names: arrayGroup(row.name),
					first: timeMin(row.created_at),
				}))
				.orderBy('city')
				.build();
			const rows = await ok(client.run(statement));
			const york = rows.find((row) => row.city === 'York');
			expect(york).toMatchObject({ people: expect.any(Number) });
			expect(york?.names).toContain('Bob');
			const leeds = rows.find((row) => row.city === 'Leeds');
			expect(leeds?.names).toEqual(expect.arrayContaining(['Ann', 'Cy']));
			expect(typeof leeds?.oldest).toBe('number');
			expect(leeds?.first).toBeInstanceOf(DateTime);
			expect(rows.map((row) => row.city)).toEqual(
				[...rows.map((row) => row.city)].sort(),
			);
		});

		it('GROUP ALL over no record is one row of empty aggregates', async () => {
			const statement = select(PersonTable)
				.where((row) =>
					eq(row.email, param('email', SurrealSchema.String)),
				)
				.groupAll()
				.aggregate((row) => ({
					people: count(),
					total: sum(row.score),
					least: min(row.score),
					median: median(row.score),
					first: timeMin(row.created_at),
				}))
				.build();
			expect(
				await ok(
					client.run(statement, { email: 'nobody@example.com' }),
				),
			).toEqual([
				{
					people: 0,
					total: 0,
					least: Number.POSITIVE_INFINITY,
					median: undefined,
					first: undefined,
				},
			]);
			const [all] = await ok(
				client.run(statement, { email: 'ann@example.com' }),
			);
			expect(all).toMatchObject({
				people: 1,
				total: 50,
				least: 50,
				median: 50,
			});
		});
	});

	describe('SPLIT and OMIT', () => {
		it('SPLIT reads one row per element, and an empty array as itself, decoded strictly', async () => {
			const rows = await ok(
				client.run(
					select(PersonTable)
						.where((row) => inside(row.name, ['Ann', 'Bob']))
						.split('pins')
						.project((row) => ({ name: row.name, pins: row.pins }))
						.orderBy((row) => row.name)
						.build(),
				),
			);
			// Ann was created with pins ['a', 'b'], Bob with none.
			expect(rows.filter((row) => row.name === 'Ann')).toEqual([
				{ name: 'Ann', pins: 'a' },
				{ name: 'Ann', pins: 'b' },
			]);
			expect(rows.filter((row) => row.name === 'Bob')).toEqual([
				{ name: 'Bob', pins: [] },
			]);
		});

		it('SPLIT of the stored row splits an array of objects', async () => {
			const rows = await ok(
				client.run(
					select(PersonTable)
						.where((row) => eq(row.name, 'Ann'))
						.split('phones')
						.build(),
				),
			);
			expect(rows.map((row) => row.phones)).toEqual([
				{ kind: 'home', number: '1' },
			]);
		});

		it('OMIT leaves fields out of the decoded row', async () => {
			const [row] = await ok(
				client.run(
					select(PersonTable)
						.where((row) => eq(row.name, 'Ann'))
						.omit('ssn', 'notes', 'nickname')
						.build(),
				),
			);
			expect(row).toMatchObject({ name: 'Ann' });
			expect(row && 'nickname' in row).toBe(false);
		});
	});
});
