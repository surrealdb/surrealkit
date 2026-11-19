/**
 * Traversal shapes real code writes, as statements run on a disposable
 * engine and decoded strictly: chained steps read through a link, a step
 * whose edge is filtered by its far endpoint, and nested projections over
 * two-hop incoming steps that hold two-hop incoming steps of their own.
 * Each also starts from a record that does not exist, which reaches
 * nothing. The schema is this file's own; its descriptors are written as
 * the emitter writes them.
 */
import { Effect, Schema } from 'effect';
import { RecordId } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { type Client, make as makeClient } from '../src/client/index.js';
import * as SurrealSchema from '../src/index.js';
import {
	destructure,
	edges,
	eq,
	evaluate,
	follow,
	incoming,
	ne,
	outgoing,
	param,
	type Result,
	select,
	selectFrom,
} from '../src/statement/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';

const PId = SurrealSchema.RecordIdFor('tr_p');
const GId = SurrealSchema.RecordIdFor('tr_g');
const DId = SurrealSchema.RecordIdFor('tr_d');

type Codec = Schema.Codec<unknown, unknown, never, never>;

const node = <
	const N extends string,
	I extends Codec,
	const F extends Schema.Struct.Fields,
>(
	name: N,
	id: I,
	fields: F,
) => {
	const select = Schema.Struct({ id, ...fields });
	return { name, schemafull: true, id, select, visible: select } as const;
};

const relation = <
	const N extends string,
	I extends Codec,
	O extends Codec,
	const E extends boolean,
>(
	name: N,
	from: I,
	to: O,
	enforced: E,
) => {
	const id = SurrealSchema.RecordIdFor(name);
	const edge = { table: name, in: [from], out: [to], enforced } as const;
	const select = Schema.Struct({ id, in: from, out: to });
	return {
		edge,
		table: {
			name,
			schemafull: true,
			id,
			select,
			visible: select,
			edge,
		} as const,
	};
};

const P = node('tr_p', PId, { name: SurrealSchema.String });
const G = node('tr_g', GId, { name: SurrealSchema.String });
const D = node('tr_d', DId, { title: SurrealSchema.String, author: PId });
const member = relation('tr_member', PId, GId, true);
const owns = relation('tr_owns', GId, DId, true);
const follows = relation('tr_follows', PId, PId, true);
const wrote = relation('tr_wrote', PId, DId, true);
const tags = relation('tr_tags', DId, GId, false);
const invited = relation('tr_invited', GId, PId, false);
const mentors = relation('tr_mentors', PId, PId, false);

const store = {
	tables: {
		tr_p: P,
		tr_g: G,
		tr_d: D,
		tr_member: member.table,
		tr_owns: owns.table,
		tr_follows: follows.table,
		tr_wrote: wrote.table,
		tr_tags: tags.table,
		tr_invited: invited.table,
		tr_mentors: mentors.table,
	},
	edges: {
		tr_member: member.edge,
		tr_owns: owns.edge,
		tr_follows: follows.edge,
		tr_wrote: wrote.edge,
		tr_tags: tags.edge,
		tr_invited: invited.edge,
		tr_mentors: mentors.edge,
	},
} as const;

const schema = `
	DEFINE TABLE tr_p SCHEMAFULL;
	DEFINE FIELD name ON tr_p TYPE string;
	DEFINE TABLE tr_g SCHEMAFULL;
	DEFINE FIELD name ON tr_g TYPE string;
	DEFINE TABLE tr_d SCHEMAFULL;
	DEFINE FIELD title ON tr_d TYPE string;
	DEFINE FIELD author ON tr_d TYPE record<tr_p>;
	DEFINE TABLE tr_member TYPE RELATION IN tr_p OUT tr_g ENFORCED;
	DEFINE TABLE tr_owns TYPE RELATION IN tr_g OUT tr_d ENFORCED;
	DEFINE TABLE tr_follows TYPE RELATION IN tr_p OUT tr_p ENFORCED;
	DEFINE TABLE tr_wrote TYPE RELATION IN tr_p OUT tr_d ENFORCED;
	DEFINE TABLE tr_tags TYPE RELATION IN tr_d OUT tr_g;
	DEFINE TABLE tr_invited TYPE RELATION IN tr_g OUT tr_p;
	DEFINE TABLE tr_mentors TYPE RELATION IN tr_p OUT tr_p;
	CREATE tr_p:ann SET name = 'Ann';
	CREATE tr_p:bob SET name = 'Bob';
	CREATE tr_p:cy SET name = 'Cy';
	CREATE tr_p:dee SET name = 'Dee';
	CREATE tr_p:eve SET name = 'Eve';
	CREATE tr_g:red SET name = 'Red';
	CREATE tr_g:blue SET name = 'Blue';
	CREATE tr_d:one SET title = 'One', author = tr_p:ann;
	CREATE tr_d:two SET title = 'Two', author = tr_p:bob;
	RELATE tr_p:ann->tr_member->tr_g:red;
	RELATE tr_p:bob->tr_member->tr_g:red;
	RELATE tr_p:cy->tr_member->tr_g:blue;
	RELATE tr_g:red->tr_owns->tr_d:one;
	RELATE tr_g:blue->tr_owns->tr_d:two;
	RELATE tr_p:bob->tr_follows->tr_p:ann;
	RELATE tr_p:cy->tr_follows->tr_p:ann;
	RELATE tr_p:dee->tr_follows->tr_p:bob;
	RELATE tr_p:eve->tr_follows->tr_p:cy;
	RELATE tr_p:ann->tr_wrote->tr_d:one;
	RELATE tr_p:bob->tr_wrote->tr_d:two;
	RELATE tr_d:one->tr_tags->tr_g:blue;
	RELATE tr_d:two->tr_tags->tr_g:blue;
	RELATE tr_g:red->tr_invited->tr_p:dee;
	RELATE tr_g:blue->tr_invited->tr_p:bob;
	RELATE tr_g:blue->tr_invited->tr_p:eve;
	RELATE tr_p:dee->tr_mentors->tr_p:ann;
	RELATE tr_p:eve->tr_mentors->tr_p:bob;
`;

const p = (key: string) => new RecordId('tr_p', key);
const g = (key: string) => new RecordId('tr_g', key);
const d = (key: string) => new RecordId('tr_d', key);

/** Rows of `{ name, … }`, ordered by name: the engine promises no order. */
const byName = <R extends { readonly name: string } | undefined>(
	list: ReadonlyArray<R> | undefined,
): ReadonlyArray<R> =>
	[...(list ?? [])].sort((x, y) =>
		(x?.name ?? '').localeCompare(y?.name ?? ''),
	);

/** Fails to compile unless `A` and `B` are the same type. */
type Same<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;
const same = <A, B>(_: Same<A, B>): void => undefined;

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] traversal engine suite skipped: ${engineSkipReason}`,
	);
}

describe.skipIf(!engineConfigured)('traversal shapes on an engine', () => {
	let engine: Engine;
	let client: Client;
	const ok = <A, E>(effect: Effect.Effect<A, E>): Promise<A> =>
		within(Effect.runPromise(effect), 20_000);

	beforeAll(async () => {
		engine = await startEngine();
		await within(engine.db.query(schema), 20_000);
		client = makeClient(engine.db);
	});
	afterAll(async () => {
		await engine?.stop();
	});

	it('(i) two chained outgoing steps from a bound record, then a field through a link of each record reached', async () => {
		const statement = evaluate(
			follow(
				store,
				follow(
					store,
					outgoing(
						store,
						outgoing(store, param('who', PId), 'tr_member'),
						'tr_owns',
					),
				).author,
			).name,
			'authors of the documents of my groups',
			{ writes: false },
		);
		expect(statement.sql).toBe(
			'RETURN $who->tr_member->?->tr_owns->?.author.name;',
		);
		expect(await ok(client.run(statement, { who: p('bob') }))).toEqual([
			'Ann',
		]);
		expect(await ok(client.run(statement, { who: p('cy') }))).toEqual([
			'Bob',
		]);
		expect(await ok(client.run(statement, { who: p('ghost') }))).toEqual(
			[],
		);
	});

	it('(ii) a step whose edge is filtered by its far endpoint being a second bound record', async () => {
		const statement = evaluate(
			outgoing(store, param('who', PId), 'tr_follows', {
				where: (edge) => eq(edge.out, param('whom', PId)),
			}),
			'whether I follow them',
			{ writes: false },
		);
		expect(statement.sql).toBe(
			'RETURN $who->(tr_follows WHERE out = $whom)->?;',
		);
		const run = (who: RecordId, whom: RecordId) =>
			ok(client.run(statement, { who, whom } as never));
		expect(await run(p('bob'), p('ann'))).toEqual([p('ann')]);
		expect(await run(p('bob'), p('cy'))).toEqual([]);
		expect(await run(p('ghost'), p('ann'))).toEqual([]);
	});

	it('(iii) a nested projection over a two-hop incoming step, holding a two-hop incoming step of its own', async () => {
		// The members of the groups that own a document, and for each the
		// followers of their followers.
		const statement = evaluate(
			destructure(
				store,
				incoming(
					store,
					incoming(store, param('doc', DId), 'tr_owns'),
					'tr_member',
				),
				(member) => ({
					name: member.name,
					reach: destructure(
						store,
						incoming(
							store,
							incoming(store, member.id, 'tr_follows'),
							'tr_follows',
						),
						(follower) => ({ name: follower.name }),
					),
				}),
			),
			'members and the followers of their followers',
			{ writes: false },
		);
		expect(statement.sql).toBe(
			'RETURN $doc<-tr_owns<-?<-tr_member<-?.{ name: name, reach: id<-tr_follows<-?<-tr_follows<-?.{ name: name } };',
		);
		// Over ENFORCED relations every record reached is a row.
		same<
			Result<typeof statement>,
			ReadonlyArray<{
				readonly name: string;
				readonly reach: ReadonlyArray<{ readonly name: string }>;
			}>
		>(true);
		const result = await ok(client.run(statement, { doc: d('one') }));
		expect(
			byName(result).map((row) => ({ ...row, reach: byName(row.reach) })),
		).toEqual([
			{ name: 'Ann', reach: [{ name: 'Dee' }, { name: 'Eve' }] },
			{ name: 'Bob', reach: [] },
		]);
		expect(await ok(client.run(statement, { doc: d('ghost') }))).toEqual(
			[],
		);
	});

	it('(iv) the same shape through other relations: followers of authors, and the members of the groups that invited each', async () => {
		const statement = evaluate(
			destructure(
				store,
				incoming(
					store,
					incoming(store, param('doc', DId), 'tr_wrote'),
					'tr_follows',
				),
				(follower) => ({
					name: follower.name,
					hosts: destructure(
						store,
						incoming(
							store,
							incoming(store, follower.id, 'tr_invited'),
							'tr_member',
						),
						(host) => ({ name: host.name, id: host.id }),
					),
				}),
			),
			'followers of the author and who invited them',
			{ writes: false },
		);
		expect(statement.sql).toBe(
			'RETURN $doc<-tr_wrote<-?<-tr_follows<-?.{ name: name, hosts: id<-tr_invited<-?<-tr_member<-?.{ name: name, id: id } };',
		);
		const result = await ok(client.run(statement, { doc: d('one') }));
		expect(
			byName(result).map((row) => ({ ...row, hosts: byName(row.hosts) })),
		).toEqual([
			{ name: 'Bob', hosts: [{ name: 'Cy', id: p('cy') }] },
			{ name: 'Cy', hosts: [] },
		]);
		expect(await ok(client.run(statement, { doc: d('ghost') }))).toEqual(
			[],
		);
	});

	it('(v) the same shape ending at another table: authors of tagged documents, and the groups that invited their mentors', async () => {
		const statement = evaluate(
			destructure(
				store,
				incoming(
					store,
					incoming(store, param('group', GId), 'tr_tags'),
					'tr_wrote',
				),
				(author) => ({
					name: author.name,
					groups: destructure(
						store,
						incoming(
							store,
							incoming(store, author.id, 'tr_mentors'),
							'tr_invited',
						),
						(group) => ({ name: group.name }),
					),
				}),
			),
			'authors and the groups that invited their mentors',
			{ writes: false },
		);
		expect(statement.sql).toBe(
			'RETURN $group<-tr_tags<-?<-tr_wrote<-?.{ name: name, groups: id<-tr_mentors<-?<-tr_invited<-?.{ name: name } };',
		);
		// A relation that is not enforced may lead to a missing record.
		same<
			Result<typeof statement>[number]['groups'],
			ReadonlyArray<{ readonly name: string } | undefined>
		>(true);
		const result = await ok(client.run(statement, { group: g('blue') }));
		expect(
			byName(result).map((row) => ({
				...row,
				groups: byName(row.groups),
			})),
		).toEqual([
			{ name: 'Ann', groups: [{ name: 'Red' }] },
			{ name: 'Bob', groups: [{ name: 'Blue' }] },
		]);
		expect(await ok(client.run(statement, { group: g('ghost') }))).toEqual(
			[],
		);
	});

	describe('the records a step reaches, as rows and filtered', () => {
		it('selectFrom filters, orders, limits and projects them, and reads none from a missing record', async () => {
			const statement = selectFrom(
				store,
				incoming(store, param('who', PId), 'tr_follows'),
			)
				.where((fan) =>
					ne(fan.name, param('not', SurrealSchema.String)),
				)
				.orderBy((fan) => fan.name, 'DESC')
				.limit(1)
				.project((fan) => ({ id: fan.id, name: fan.name }))
				.build('a follower', { writes: false });
			expect(statement.sql).toBe(
				'SELECT id AS id, name AS name FROM $who<-tr_follows<-? WHERE name != $not ORDER BY name DESC LIMIT 1;',
			);
			const run = (who: RecordId, not: string) =>
				ok(client.run(statement, { who, not } as never));
			expect(await run(p('ann'), 'Nobody')).toEqual([
				{ id: p('cy'), name: 'Cy' },
			]);
			expect(await run(p('ann'), 'Cy')).toEqual([
				{ id: p('bob'), name: 'Bob' },
			]);
			expect(await run(p('ghost'), 'Cy')).toEqual([]);
		});

		it('selectFrom reads chained steps, the edges of a step and one row with ONLY', async () => {
			const reach = selectFrom(
				store,
				incoming(
					store,
					incoming(store, param('who', PId), 'tr_follows'),
					'tr_follows',
				),
			)
				.orderBy((fan) => fan.name)
				.value((fan) => fan.name)
				.build();
			expect(await ok(client.run(reach, { who: p('ann') }))).toEqual([
				'Dee',
				'Eve',
			]);
			const memberships = selectFrom(
				store,
				edges(outgoing(store, param('who', PId), 'tr_member')),
			)
				.value((edge) => edge.out)
				.build();
			expect(
				await ok(client.run(memberships, { who: p('ann') })),
			).toEqual([g('red')]);
			const first = selectFrom(
				store,
				incoming(store, param('who', PId), 'tr_follows'),
			)
				.orderBy((fan) => fan.name)
				.only()
				.value((fan) => fan.name)
				.build();
			expect(await ok(client.run(first, { who: p('ann') }))).toBe('Bob');
			expect(
				await ok(client.run(first, { who: p('ghost') })),
			).toBeUndefined();
		});

		it('selectFrom nests as a subquery from the outer row', async () => {
			const statement = select(P)
				.project((row) => ({
					name: row.name,
					fans: selectFrom(
						store,
						incoming(store, row.id, 'tr_follows'),
					)
						.orderBy((fan) => fan.name)
						.value((fan) => fan.name)
						.toExpr(),
				}))
				.orderBy((row) => row.name)
				.build();
			expect(await ok(client.run(statement))).toEqual([
				{ name: 'Ann', fans: ['Bob', 'Cy'] },
				{ name: 'Bob', fans: ['Dee'] },
				{ name: 'Cy', fans: ['Eve'] },
				{ name: 'Dee', fans: [] },
				{ name: 'Eve', fans: [] },
			]);
		});

		it('whereTarget keeps the records that satisfy it', async () => {
			const statement = evaluate(
				outgoing(store, param('who', PId), 'tr_member', {
					whereTarget: (group) =>
						eq(group.name, param('name', SurrealSchema.String)),
				}),
				'my group of that name',
				{ writes: false },
			);
			expect(statement.sql).toBe(
				'RETURN $who->tr_member->(? WHERE name = $name);',
			);
			const run = (who: RecordId, name: string) =>
				ok(client.run(statement, { who, name } as never));
			expect(await run(p('ann'), 'Red')).toEqual([g('red')]);
			expect(await run(p('cy'), 'Red')).toEqual([]);
			expect(await run(p('ghost'), 'Red')).toEqual([]);
		});
	});
});
