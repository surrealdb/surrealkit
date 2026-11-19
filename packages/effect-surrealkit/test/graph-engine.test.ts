/**
 * Record links, graph steps, recursive paths and FETCH through a real
 * engine: the corpus schema is applied to a disposable database, the
 * statements are built from the golden modules and store registry the
 * emitter generated for that same corpus, and every result is decoded
 * strictly by the codec its statement derived.
 */
import { readFileSync } from 'node:fs';

import { Effect } from 'effect';
import { RecordId } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { bind, type Client, make as makeClient } from '../src/client/index.js';
import * as SurrealSchema from '../src/index.js';
import {
	both,
	collect,
	contains,
	destructure,
	edges,
	eq,
	follow,
	incoming,
	outgoing,
	param,
	recurse,
	rows,
	select,
	shortest,
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
	DocTable,
	MemberOfTable,
	PersonId,
	PersonTable,
	store,
	TeamTable,
} from './golden/corpus/index.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] graph engine suite skipped: ${engineSkipReason}`,
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

const id = (table: string, key: string) => new RecordId(table, key);
const ann = id('person', 'ann');
const bob = id('person', 'bob');
const cy = id('person', 'cy');
const dan = id('person', 'dan');
const red = id('team', 'red');

const keys = (values: ReadonlyArray<RecordId> | undefined) =>
	values?.map((value) => `${value.table.name}:${String(value.id)}`).sort();

const person = (key: string, name: string, extra = '') =>
	`CREATE person:${key} SET name = '${name}', email = '${key}@example.com', bio = NULL, score = 50, address = { city: 'Leeds' }${extra};`;

describe.skipIf(!engineConfigured)(
	'graph statements on a disposable engine',
	() => {
		let engine: Engine;
		let client: Client;
		const ok = <A, E>(effect: Effect.Effect<A, E>): Promise<A> =>
			within(Effect.runPromise(effect), 20_000);

		beforeAll(async () => {
			engine = await startEngine();
			await applyCorpus(engine);
			await engine.db.query(
				[
					person('ann', 'Ann'),
					person(
						'bob',
						'Bob',
						', manager = person:ann, watchers = [person:ann, person:cy]',
					),
					person(
						'cy',
						'Cy',
						', manager = person:ghost, watchers = [person:ghost, person:ann]',
					),
					person('dan', 'Dan', ', manager = person:bob'),
					"CREATE team:red SET name = 'Red';",
					"CREATE org:acme SET name = 'Acme';",
					"RELATE person:ann->member_of->team:red SET role = 'lead';",
					"RELATE person:ann->member_of->org:acme SET role = 'member';",
					"RELATE person:bob->member_of->team:red SET role = 'member';",
					'RELATE person:ann->knows->person:bob;',
					'RELATE person:ann->knows->person:cy;',
					'RELATE person:bob->knows->person:dan;',
					'RELATE person:cy->knows->person:dan;',
					'RELATE person:dan->follows->person:ghost;',
					"CREATE doc:one SET title = 'One', body = 'b', code = 'c1', embedding = [0.1, 0.2, 0.3, 0.4], owner = person:bob;",
				].join('\n'),
			);
			client = makeClient(engine.db);
		});

		afterAll(async () => {
			await engine?.stop();
		});

		describe('record links', () => {
			it('follow a link in a condition and a projection, NONE for a missing or absent target', async () => {
				const statement = select(PersonTable)
					.project((row) => ({
						id: row.id,
						boss: follow(store, row.manager).name,
						bossCity: follow(store, row.manager).address.city,
					}))
					.orderBy((row) => row.id)
					.build();
				expect(await ok(client.run(statement))).toEqual([
					{ id: ann, boss: undefined, bossCity: undefined },
					{ id: bob, boss: 'Ann', bossCity: 'Leeds' },
					{ id: cy, boss: undefined, bossCity: undefined },
					{ id: dan, boss: 'Bob', bossCity: 'Leeds' },
				]);
				const owned = select(DocTable)
					.where((row) =>
						eq(
							follow(store, row.owner).name,
							param('owner', SurrealSchema.String),
						),
					)
					.value(
						(row) =>
							follow(store, follow(store, row.owner).manager)
								.name,
					)
					.build();
				expect(await ok(client.run(owned, { owner: 'Bob' }))).toEqual([
					'Ann',
				]);
				expect(await ok(client.run(owned, { owner: 'Cy' }))).toEqual(
					[],
				);
			});

			it('read a list of links as a list, NONE for each missing record', async () => {
				const statement = select(PersonTable, param('who', PersonId))
					.only()
					.value((row) => follow(store, row.watchers).name)
					.build();
				expect(
					await ok(client.run(statement, { who: cy as PersonId })),
				).toEqual([undefined, 'Ann']);
			});

			it('follow a link to several tables through the fields they share', async () => {
				const statement = select(MemberOfTable)
					.project((row) => ({
						to: follow(store, row.out).id,
						name: follow(store, row.out).name,
					}))
					.orderBy((row) => row.role)
					.build();
				const result = await ok(client.run(statement));
				expect(result.map((row) => row.name).sort()).toEqual([
					'Acme',
					'Red',
					'Red',
				]);
				expect(keys(result.map((row) => row.to as RecordId))).toEqual([
					'org:acme',
					'team:red',
					'team:red',
				]);
			});

			it('materialise and destructure a link, NONE for a missing target', async () => {
				const statement = select(PersonTable)
					.project((row) => ({
						id: row.id,
						boss: rows(store, row.manager),
						card: destructure(store, row.manager, (boss) => ({
							name: boss.name,
						})),
					}))
					.orderBy((row) => row.id)
					.build();
				const result = await ok(client.run(statement));
				expect(result.map((row) => row.boss?.name)).toEqual([
					undefined,
					'Ann',
					undefined,
					'Bob',
				]);
				expect(result.map((row) => row.card)).toEqual([
					undefined,
					{ name: 'Ann' },
					undefined,
					{ name: 'Bob' },
				]);
			});
		});

		describe('graph steps', () => {
			const from = (key: RecordId) => ({ who: key as PersonId });

			it('yield the records on the other side, each direction', async () => {
				const statement = select(PersonTable, param('who', PersonId))
					.only()
					.project((row) => ({
						teams: outgoing(store, row.id, 'member_of'),
						friends: outgoing(store, row.id, 'knows'),
						around: both(store, row.id, 'knows'),
					}))
					.build();
				const result = await ok(client.run(statement, from(bob)));
				expect(keys(result?.teams)).toEqual(['team:red']);
				expect(keys(result?.friends)).toEqual(['person:dan']);
				// `<->` yields both endpoints of each edge: bob once per edge.
				expect(keys(result?.around)).toEqual([
					'person:ann',
					'person:bob',
					'person:bob',
					'person:dan',
				]);
				const members = select(TeamTable)
					.value((row) => incoming(store, row.id, 'member_of'))
					.build();
				expect((await ok(client.run(members))).map(keys)).toEqual([
					['person:ann', 'person:bob'],
				]);
			});

			it('chain into one flat list, duplicates kept', async () => {
				const statement = select(PersonTable, param('who', PersonId))
					.only()
					.value((row) =>
						outgoing(
							store,
							outgoing(store, row.id, 'knows'),
							'knows',
						),
					)
					.build();
				expect(
					keys(await ok(client.run(statement, from(ann)))),
				).toEqual(['person:dan', 'person:dan']);
			});

			it('filter the edges, read them, and pick fields of the records', async () => {
				const statement = select(PersonTable, param('who', PersonId))
					.only()
					.project((row) => ({
						leads: outgoing(store, row.id, 'member_of', {
							where: (edge) =>
								eq(
									edge.role,
									param('role', MemberOfTable.fields.role),
								),
						}),
						roles: follow(
							store,
							edges(outgoing(store, row.id, 'member_of')),
						).role,
						names: follow(
							store,
							outgoing(store, row.id, 'member_of'),
						).name,
						cards: destructure(
							store,
							outgoing(store, row.id, 'member_of'),
							(target) => ({ id: target.id, title: target.name }),
						),
						rows: rows(store, outgoing(store, row.id, 'member_of')),
					}))
					.build();
				const result = await ok(
					client.run(statement, {
						who: ann as PersonId,
						role: 'lead',
					}),
				);
				expect(keys(result?.leads)).toEqual(['team:red']);
				expect([...(result?.roles ?? [])].sort()).toEqual([
					'lead',
					'member',
				]);
				expect([...(result?.names ?? [])].sort()).toEqual([
					'Acme',
					'Red',
				]);
				expect(result?.cards.map((card) => card.title).sort()).toEqual([
					'Acme',
					'Red',
				]);
				expect(
					result?.rows.map((target) => target.name).sort(),
				).toEqual(['Acme', 'Red']);
			});

			it('step from a link and in a condition; NONE from an absent link', async () => {
				const statement = select(PersonTable)
					.where((row) =>
						contains(
							outgoing(store, row.id, 'member_of'),
							param('team', TeamTable.id),
						),
					)
					.project((row) => ({
						id: row.id,
						bossTeams: outgoing(store, row.manager, 'member_of'),
					}))
					.orderBy((row) => row.id)
					.build();
				const result = await ok(
					client.run(statement, { team: red as never }),
				);
				expect(result.map((row) => keys([row.id]))).toEqual([
					['person:ann'],
					['person:bob'],
				]);
				expect(result[0]?.bossTeams).toBeUndefined();
				expect(keys(result[1]?.bossTeams)).toEqual([
					'org:acme',
					'team:red',
				]);
			});

			it('read NONE for an edge of a relation that is not enforced to a missing record', async () => {
				const statement = select(PersonTable, param('who', PersonId))
					.only()
					.project((row) => ({
						ids: outgoing(store, row.id, 'follows'),
						rows: rows(store, outgoing(store, row.id, 'follows')),
					}))
					.build();
				const result = await ok(client.run(statement, from(dan)));
				expect(keys(result?.ids)).toEqual(['person:ghost']);
				expect(result?.rows).toEqual([undefined]);
			});
		});

		describe('recursive paths', () => {
			const knows = select(PersonTable, param('who', PersonId))
				.only()
				.project((row) => ({
					depth: recurse(row.id, { min: 1, max: 3 }, (node) =>
						outgoing(store, node, 'knows'),
					),
					exact: recurse(row.id, { min: 1, max: 1 }, (node) =>
						outgoing(store, node, 'knows'),
					),
					all: collect(row.id, (node) =>
						outgoing(store, node, 'knows'),
					),
					self: collect(
						row.id,
						(node) => outgoing(store, node, 'knows'),
						{
							inclusive: true,
						},
					),
					path: shortest(row.id, param('to', PersonId), (node) =>
						outgoing(store, node, 'knows'),
					),
					names: follow(
						store,
						collect(row.id, (node) =>
							outgoing(store, node, 'knows'),
						),
					).name,
				}))
				.build();

			it('walk a bounded depth range, collect and find a shortest path', async () => {
				const result = await ok(
					client.run(knows, {
						who: ann as PersonId,
						to: dan as PersonId,
					}),
				);
				// The walk stops where it reaches no record: depth 2 of at most 3.
				expect(keys(result?.depth)).toEqual([
					'person:dan',
					'person:dan',
				]);
				expect(keys(result?.exact)).toEqual([
					'person:bob',
					'person:cy',
				]);
				expect(keys(result?.all)).toEqual([
					'person:bob',
					'person:cy',
					'person:dan',
				]);
				expect(keys(result?.self)).toEqual([
					'person:ann',
					'person:bob',
					'person:cy',
					'person:dan',
				]);
				expect(result?.path?.length).toBe(2);
				expect(keys(result?.path?.slice(1))).toEqual(['person:dan']);
				expect([...(result?.names ?? [])].sort()).toEqual([
					'Bob',
					'Cy',
					'Dan',
				]);
			});

			it('answer NONE for a shortest path that does not exist', async () => {
				const result = await ok(
					client.run(knows, {
						who: dan as PersonId,
						to: ann as PersonId,
					}),
				);
				expect(result?.path).toBeUndefined();
				expect(result?.depth).toEqual([]);
			});
		});

		describe('FETCH', () => {
			it('replaces a link with its row, absent for a missing or absent target', async () => {
				const statement = select(PersonTable)
					.fetch(store, 'manager')
					.orderBy((row) => row.id)
					.build();
				const result = await ok(client.run(statement));
				expect(result.map((row) => row.manager?.name)).toEqual([
					undefined,
					'Ann',
					undefined,
					'Bob',
				]);
				expect(result.map((row) => 'manager' in row)).toEqual([
					false,
					true,
					false,
					true,
				]);
			});

			it('replaces a list of links, NONE for each missing record', async () => {
				const statement = select(PersonTable, param('who', PersonId))
					.fetch(store, 'watchers')
					.build();
				const [row] = await ok(
					client.run(statement, { who: cy as PersonId }),
				);
				expect(row?.watchers.map((watcher) => watcher?.name)).toEqual([
					undefined,
					'Ann',
				]);
			});

			it('fetches after OMIT, and a record read FOR UPDATE', async () => {
				const omitted = select(PersonTable, param('who', PersonId))
					.omit('email', 'bio')
					.fetch(store, 'manager')
					.build();
				const [row] = await ok(
					client.run(omitted, { who: bob as PersonId }),
				);
				expect(row?.manager?.name).toBe('Ann');
				expect(row !== undefined && 'email' in row).toBe(false);
				const locked = select(PersonTable, param('who', PersonId))
					.forUpdate()
					.fetch(store, 'manager')
					.build();
				expect(locked.sql).toBe(
					'SELECT * FROM $who FETCH manager FOR UPDATE;',
				);
				const [read] = await ok(
					client.transaction([
						bind(locked, { who: dan as PersonId }),
					]),
				);
				expect(read[0]?.manager?.name).toBe('Bob');
			});

			it('fetches through a fetched record, and a link to several tables', async () => {
				const statement = select(DocTable)
					.fetch(store, 'owner.manager')
					.build();
				const [doc] = await ok(client.run(statement));
				expect(doc?.owner?.name).toBe('Bob');
				expect(doc?.owner?.manager?.name).toBe('Ann');
				const union = select(MemberOfTable)
					.project((row) => ({ out: row.out, role: row.role }))
					.fetch(store, 'out')
					.build();
				const result = await ok(client.run(union));
				expect(result.map((row) => row.out?.name).sort()).toEqual([
					'Acme',
					'Red',
					'Red',
				]);
				expect(
					keys(result.map((row) => row.out?.id as RecordId)),
				).toEqual(['org:acme', 'team:red', 'team:red']);
			});
		});
	},
);
