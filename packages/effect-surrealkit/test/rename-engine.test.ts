import { readFileSync } from 'node:fs';

import { Effect, Result, Schema } from 'effect';
import { DateTime, ExpressionError, RecordId } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
	bind,
	type Client,
	type InputOf,
	make as makeClient,
	StatementRequestError,
	StatementThrown,
} from '../src/client/index.js';
import * as S from '../src/index.js';
import {
	at,
	block,
	call,
	cast,
	count,
	eq,
	gte,
	idRange,
	literal,
	make,
	match,
	median,
	min,
	param,
	part,
	recordId,
	select,
	sequence,
	sqlOf,
	sum,
	timeMin,
} from '../src/statement/index.js';
import type {
	Codec,
	Input,
	Params,
	Statement,
} from '../src/statement/statement.js';
import { personAndTeams } from './blocks.js';
import { corpusPath, readJson } from './documents.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
} from './engine.js';
import {
	DocTable,
	fn_double,
	fn_greet,
	IdArrayTable,
	IdIntTable,
	PersonId,
	PersonTable,
	TeamTable,
} from './golden/corpus/index.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] rename engine suite skipped: ${engineSkipReason}`,
	);
}

const findByEmail = (terminal: '$person;' | 'RETURN $person;') =>
	sequence(
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
		part({ sql: terminal, free: ['person'] }),
	).define({
		name: 'find by email',
		params: { boundEmail: S.String },
		result: PersonId,
	});
const readPerson = findByEmail('$person;');
const legacy = findByEmail('RETURN $person;');
const annEmail = { boundEmail: 'ann@example.com' };
const bobEmail = { boundEmail: 'bob@example.com' };
const marker = make({
	name: 'mark',
	sql: 'CREATE ONLY rename_marker:one SET n = 1 RETURN VALUE n;',
	params: {},
	result: S.Int,
	writes: true,
});
const author = (name: string) =>
	Schema.decodeUnknownSync(PersonId)(new RecordId('person', name));
const ann = author('ann');
const bob = author('bob');

const cases = (() => {
	// The parameterized reads from statement-engine.test.ts. The corpus uses
	// these builders with identical expressions; the rows here have fixed IDs.
	const byId = select(PersonTable, param('id', PersonId)).only().build();
	const byEmail = select(PersonTable)
		.where((row) => eq(row.email, param('email', S.String)))
		.only()
		.build();
	const page = select(PersonTable)
		.where((row) => gte(row.age, param('minimum', S.Int)))
		.orderBy((row) => row.name, 'DESC')
		.limit(param('rows', S.Int))
		.start(param('skip', S.Int))
		.value((row) => row.name)
		.build();
	const listing = select(PersonTable)
		.orderBy((row) => row.name)
		.project((row) => ({
			name: row.name,
			nickname: row.nickname,
			city: row.address.city,
			key: cast('string', row.id),
			phone: at(row.phones, 0),
			greeting: call(fn_greet, row.name, param('greeting', S.String)),
			doubled: call(fn_double, 21),
		}))
		.build();
	const named = select(PersonTable)
		.where((row) => eq(row.id, param('id', PersonId)))
		.value((row) => row.name)
		.build();
	const peers = select(PersonTable)
		.where((row) => eq(row.name, 'Bob'))
		.project((row) => ({
			name: row.name,
			inLeeds: select(PersonTable)
				.where((peer) => eq(peer.address.city, param('city', S.String)))
				.orderBy((peer) => peer.name)
				.value((peer) => peer.name)
				.toExpr(),
		}))
		.build();
	const teams = select(TeamTable)
		.where((row) => eq(row.name, param('name', S.String)))
		.value((row) => row.name)
		.build();
	const reader = make({
		name: 'reader',
		sql: '$n;',
		params: { n: S.Int },
		result: S.Int,
	});
	const older = select(PersonTable)
		.where((row) => gte(row.age, param('minimum', S.Int)))
		.build();
	const readInt = select(IdIntTable, recordId(IdIntTable, param('n', S.Int)))
		.only()
		.build();
	const readArray = select(
		IdArrayTable,
		recordId(IdArrayTable, [
			param('city', S.String),
			param('at', S.DateTime),
		]),
	)
		.only()
		.build();
	const range = (shape: 'fromTo' | 'fromThrough' | 'after' | 'through') => {
		const low = param('low', S.Int);
		const high = param('high', S.Int);
		const bounds = {
			fromTo: { from: low, to: high },
			fromThrough: { from: low, through: high },
			after: { after: low },
			through: { through: high },
		};
		return select(IdIntTable, idRange(IdIntTable, bounds[shape]))
			.orderBy((row) => row.id)
			.build();
	};
	const week = select(
		IdArrayTable,
		idRange(IdArrayTable, {
			from: [param('city', S.String), param('start', S.DateTime)],
			through: [param('city', S.String), param('end', S.DateTime)],
		}),
	);
	const grouping = select(PersonTable)
		.where((row) => eq(row.email, param('email', S.String)))
		.groupAll()
		.aggregate((row) => ({
			people: count(),
			total: sum(row.score),
			least: min(row.score),
			median: median(row.score),
			first: timeMin(row.created_at),
		}))
		.build();
	const fixture =
		<P extends Params, R extends Codec, T extends string>(
			statement: Statement<P, R, T>,
			...inputs: readonly [Input<P>, Input<P>]
		) =>
		async (client: Client) => {
			// Each concrete case has a nonempty parameter map. InputOf's
			// conditional tuple leaves that fact out of this generic helper.
			const one = (value: Input<P>) => [value] as unknown as InputOf<P>;
			const expected = await Promise.all(
				inputs.map((value) =>
					Effect.runPromise(client.run(statement, ...one(value))),
				),
			);
			const actual = await Effect.runPromise(
				client.transaction([
					bind(statement, ...one(inputs[0])),
					bind(statement, ...one(inputs[1])),
				]),
			);
			expect(actual).toEqual(expected);
		};
	const jan1 = new DateTime('2024-01-01T00:00:00Z');
	const jan2 = new DateTime('2024-01-02T00:00:00Z');
	return {
		'by record id': fixture(byId, { id: ann }, { id: bob }),
		'by email': fixture(
			byEmail,
			{ email: annEmail.boundEmail },
			{ email: bobEmail.boundEmail },
		),
		'filter, limit and start': fixture(
			page,
			{ minimum: 18, rows: 5, skip: 0 },
			{ minimum: 25, rows: 1, skip: 0 },
		),
		'projected function call': fixture(
			listing,
			{ greeting: 'hi' },
			{ greeting: 'hello' },
		),
		'record id predicate': fixture(named, { id: ann }, { id: bob }),
		'nested projection': fixture(
			peers,
			{ city: 'Leeds' },
			{ city: 'York' },
		),
		'composable authored lookup': fixture(readPerson, annEmail, bobEmail),
		'team name predicate': fixture(
			teams,
			{ name: 'Red' },
			{ name: 'Blue' },
		),
		'plain parameter reader': fixture(reader, { n: 1 }, { n: 2 }),
		'age predicate': fixture(older, { minimum: 25 }, { minimum: 40 }),
		'integer record key': fixture(readInt, { n: 1 }, { n: 2 }),
		'array record key': fixture(
			readArray,
			{ city: 'leeds', at: jan1 },
			{ city: 'leeds', at: jan2 },
		),
		'excluded upper endpoint': fixture(
			range('fromTo'),
			{ low: 1, high: 2 },
			{ low: 2, high: 3 },
		),
		'included upper endpoint': fixture(
			range('fromThrough'),
			{ low: 1, high: 2 },
			{ low: 2, high: 3 },
		),
		'open end after a lower bound': fixture(
			range('after'),
			{ low: 1 },
			{ low: 2 },
		),
		'open start through an upper bound': fixture(
			range('through'),
			{ high: 1 },
			{ high: 2 },
		),
		'array key range': fixture(
			week.build(),
			{ city: 'leeds', start: jan1, end: jan2 },
			{ city: 'leeds', start: jan2, end: jan2 },
		),
		'array key range with ONLY': fixture(
			week.only().build(),
			{ city: 'leeds', start: jan1, end: jan1 },
			{ city: 'leeds', start: jan2, end: jan2 },
		),
		'aggregate selection': fixture(
			grouping,
			{ email: annEmail.boundEmail },
			{ email: 'none@example.com' },
		),
		'graph traversal wrapped in a block': fixture(
			personAndTeams,
			{ who: ann },
			{ who: bob },
		),
		'block with locals': fixture(
			block()
				.let('local', () => param('n', S.Int))
				.return(($) => $.local)
				.build(),
			{ n: 2 },
			{ n: 3 },
		),
		'full-text match': fixture(
			select(DocTable)
				.where(() =>
					match(DocTable, 'body', param('terms', S.String), {
						ref: 1,
					}),
				)
				.value((row) => row.title)
				.build(),
			{ terms: 'fox' },
			{ terms: 'dog' },
		),
		'inner FOR shadow': fixture(
			make({
				sql: "{ LET $outer = $id; FOR $id IN [2, 3] { IF $id > 3 { THROW 'wrong local' }; }; [$outer, $id]; };",
				params: { id: S.Int },
				result: Schema.Array(S.Int),
				writes: false,
			}),
			{ id: 7 },
			{ id: 9 },
		),
	};
})();

// Names also make the V2.1 corpus inventory and V2.2/V2.3 cases visible in
// test reports. Each case compares two separately executed statements.
const corpusCases = Object.entries(cases);

describe.skipIf(!engineConfigured)(
	'repeated statements on a disposable engine',
	() => {
		let engine: Engine;
		let client: Client;
		beforeAll(async () => {
			engine = await startEngine();
			client = makeClient(engine.db);
			const minor = /(\d+\.\d+)\./u.exec(engine.version)?.[1] ?? '';
			const { modules } = readJson(
				corpusPath('corpus.json'),
			) as unknown as {
				readonly modules: ReadonlyArray<{
					readonly path: string;
					readonly engines?: ReadonlyArray<string>;
					readonly experimental?: ReadonlyArray<string>;
				}>;
			};
			for (const module of modules) {
				if (
					module.experimental === undefined &&
					(module.engines === undefined ||
						module.engines.includes(minor))
				) {
					await engine.db.query(
						readFileSync(corpusPath(module.path), 'utf8'),
					);
				}
			}
			await engine.db.query(`
			DEFINE TABLE rename_marker SCHEMALESS;
			CREATE person:ann CONTENT { name: 'Ann', email: 'ann@example.com', bio: NULL, score: 50f, age: 30, address: { city: 'Leeds' } };
			CREATE person:bob CONTENT { name: 'Bob', email: 'bob@example.com', bio: NULL, score: 40f, age: 45, address: { city: 'York' }, manager: person:ann };
			CREATE team:red SET name = 'Red'; CREATE team:blue SET name = 'Blue';
			RELATE person:ann->member_of->team:red; RELATE person:bob->member_of->team:blue;
			CREATE id_int:1; CREATE id_int:2; CREATE id_int:3;
			CREATE id_array:['leeds', d'2024-01-01T00:00:00Z']; CREATE id_array:['leeds', d'2024-01-02T00:00:00Z'];
			CREATE doc:fox SET title = 'fox', body = 'the quick fox', code = 'fox', owner = person:ann, embedding = [1.0, 0.0, 0.0, 0.0];
			CREATE doc:dog SET title = 'dog', body = 'the quick dog', code = 'dog', owner = person:bob, embedding = [0.9, 0.1, 0.0, 0.0];
		`);
		});
		afterAll(async () => {
			await engine?.stop();
		});

		it.each(
			corpusCases,
		)('matches individual results for %s', async (_name, runCase) => {
			await runCase(client);
		});

		it.each([
			'statement',
			'transaction',
		] as const)('repeats a bound statement in a %s pipeline', async (each) => {
			const outcomes = await Effect.runPromise(
				client.pipeline(
					[bind(readPerson, annEmail), bind(readPerson, bobEmail)],
					{ each },
				),
			);
			expect(outcomes).toEqual([
				Result.succeed(ann),
				Result.succeed(bob),
			]);
		});

		it('keeps the legacy standalone RETURN and the SDK early-return refusal', async () => {
			expect(legacy.sql).toMatch(/RETURN \$person;$/u);
			expect(
				await Effect.runPromise(client.run(legacy, annEmail)),
			).toEqual(ann);
			expect(
				await Effect.runPromise(client.run(legacy, bobEmail)),
			).toEqual(bob);
			const error = await Effect.runPromise(
				Effect.flip(
					client.transaction([
						bind(legacy, annEmail),
						bind(legacy, bobEmail),
					]),
				),
			);
			expect(error).toBeInstanceOf(StatementRequestError);
			expect(error).toHaveProperty('cause', expect.any(ExpressionError));
			expect(error.message).toMatch(
				/contains a RETURN statement which is followed by another statement/u,
			);
		});

		it('continues to refuse the corpus datetime comparison with top-level RETURN', async () => {
			const instant = new DateTime('1969-12-31T23:59:59.5Z');
			const distinct = new DateTime('1969-12-31T23:59:59.6Z');
			const comparison = make({
				sql: `RETURN ${sqlOf(literal(instant, S.DateTime))} = $instant;`,
				params: { instant: S.DateTime },
				result: Schema.Boolean,
			});
			expect(
				await Effect.runPromise(client.run(comparison, { instant })),
			).toBe(true);
			expect(
				await Effect.runPromise(
					client.run(comparison, { instant: distinct }),
				),
			).toBe(false);
			const error = await Effect.runPromise(
				Effect.flip(
					client.transaction([
						bind(comparison, { instant }),
						bind(comparison, { instant: distinct }),
					]),
				),
			);
			expect(error).toBeInstanceOf(StatementRequestError);
			expect(error.message).toMatch(
				/contains a RETURN statement which is followed by another statement/u,
			);
		});

		it('commits a successful repeated lookup and rolls back the earlier write on a typed guard', async () => {
			const readMarker = () =>
				engine.db.query('SELECT VALUE id FROM ONLY rename_marker:one;');
			const bad = await Effect.runPromise(
				Effect.flip(
					client.transaction([
						bind(marker),
						bind(readPerson, annEmail),
						bind(readPerson, { boundEmail: 'missing@example.com' }),
					]),
				),
			);
			expect(bad).toBeInstanceOf(StatementThrown);
			expect(bad).toMatchObject({
				statement: 'find by email',
				reason: 'No such person',
			});
			expect(await readMarker()).toEqual([undefined]);
			expect(
				await Effect.runPromise(
					client.transaction([
						bind(marker),
						bind(readPerson, annEmail),
						bind(readPerson, bobEmail),
					]),
				),
			).toEqual([1, ann, bob]);
			expect(await readMarker()).toEqual([
				new RecordId('rename_marker', 'one'),
			]);
		});
	},
);
