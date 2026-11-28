/**
 * The records a step reaches as rows (`selectFrom`) and a filter on them
 * (`whereTarget`), without an engine: the SurrealQL each renders, the
 * codecs it derives and what is refused. `test/traversal-engine.test.ts`
 * runs them, and `test/conformance.test.ts` pins what the engines do.
 */
import { describe, expect, it } from 'vitest';

import * as SurrealSchema from '../src/index.js';
import {
	both,
	collect,
	destructure,
	edges,
	eq,
	follow,
	gte,
	incoming,
	ne,
	outgoing,
	param,
	parent,
	readAs,
	select,
	selectFrom,
} from '../src/statement/index.js';
import { PersonId, PersonTable, store, TeamId } from './golden/corpus/index.js';

/** A builder with its types switched off, to reach its run-time checks. */
const untyped = <F>(f: F) =>
	f as unknown as (...args: ReadonlyArray<unknown>) => unknown;

const who = param('who', PersonId);

describe('selectFrom: the records a step reaches, as rows', () => {
	it('filters, orders, limits and projects them', () => {
		const statement = selectFrom(store, outgoing(store, who, 'knows'))
			.where((peer) => gte(peer.age, param('age', SurrealSchema.Int)))
			.orderBy((peer) => peer.name, 'DESC')
			.limit(2)
			.start(1)
			.project((peer) => ({ id: peer.id, name: peer.name }))
			.build('peers');
		expect(statement.sql).toBe(
			'SELECT id AS id, name AS name FROM $who->knows->? WHERE age >= $age ORDER BY name DESC LIMIT 2 START 1;',
		);
		expect(Object.keys(statement.params).sort()).toEqual(['age', 'who']);
	});

	it('reads one row with ONLY, which adds LIMIT 1', () => {
		expect(
			selectFrom(
				store,
				incoming(store, param('team', TeamId), 'member_of'),
			)
				.only()
				.build().sql,
		).toBe('SELECT * FROM ONLY $team<-member_of<-? LIMIT 1;');
	});

	it('takes chained and filtered steps, the edges of a step and a recursive path', () => {
		expect(
			selectFrom(
				store,
				outgoing(store, outgoing(store, who, 'knows'), 'knows', {
					where: (edge) => ne(edge.out, who),
				}),
			).build().sql,
		).toBe('SELECT * FROM $who->knows->?->(knows WHERE out != $who)->?;');
		expect(
			selectFrom(store, edges(outgoing(store, who, 'member_of')))
				.where((edge) => eq(edge.role, 'lead'))
				.project((edge) => ({ to: edge.out }))
				.build().sql,
		).toBe('SELECT out AS to FROM $who->member_of WHERE role = s"lead";');
		expect(
			selectFrom(
				store,
				collect(who, (node) => outgoing(store, node, 'knows')),
			)
				.value((peer) => peer.name)
				.build().sql,
		).toBe('SELECT VALUE name FROM $who.{..+collect}(->knows->?);');
	});

	it('nests as a subquery reading the outer row, which the engines read from that row', () => {
		const statement = select(PersonTable)
			.project((row) => ({
				adults: selectFrom(store, outgoing(store, row.id, 'knows'))
					.where((peer) => gte(peer.age, 18))
					.value((peer) => peer.name)
					.toExpr(),
			}))
			.build();
		expect(statement.sql).toBe(
			'SELECT (SELECT VALUE name FROM id->knows->? WHERE age >= 18) AS adults FROM ⟨person⟩;',
		);
		// Built on its own, the step from a row reads a row it has none of.
		let step: unknown;
		select(PersonTable).where((row) => {
			step = outgoing(store, row.id, 'knows');
			return eq(row.name, 'x');
		});
		expect(() => selectFrom(store, step as never).build('alone')).toThrow(
			'`id` is a field of the row of a select of `person`, and statement `alone` has no row to read it from.',
		);
	});

	it('reads through the store view the step reads through', () => {
		expect(() =>
			selectFrom(store, outgoing(readAs(store), who, 'knows')).build(
				'mixed',
			),
		).toThrow(/statement `mixed` reads its table as a privileged reader/);
		const session = readAs(store);
		expect(
			selectFrom(session, outgoing(session, who, 'knows')).build().sql,
		).toBe('SELECT * FROM $who->knows->?;');
	});

	it('refuses what is not one table’s records', () => {
		expect(() =>
			untyped(selectFrom)(store, outgoing(store, who, 'member_of')),
		).toThrow(
			'the records of this step are of several tables (`org`, `team`), and a select reads the rows of one',
		);
		expect(() => untyped(selectFrom)(store, who)).toThrow(
			'a select reads the records of a graph step or a recursive path',
		);
	});
});

describe('whereTarget: a filter on the records a step reaches', () => {
	it('renders after the step, with or without an edge filter, in each direction', () => {
		expect(
			select(PersonTable)
				.project((row) => ({
					leads: outgoing(store, row.id, 'member_of', {
						where: (edge) => eq(edge.role, 'lead'),
						whereTarget: (team) =>
							eq(team.name, param('name', SurrealSchema.String)),
					}),
					fans: incoming(store, row.id, 'knows', {
						whereTarget: (fan) => gte(fan.age, 18),
					}),
					around: both(store, row.id, 'knows', {
						whereTarget: (peer) => ne(peer.id, who),
					}),
				}))
				.build().sql,
		).toBe(
			'SELECT id->(member_of WHERE role = s"lead")->(? WHERE name = $name) AS leads, id<-knows<-(? WHERE age >= 18) AS fans, id<->knows<->(? WHERE id != $who) AS around FROM ⟨person⟩;',
		);
	});

	it('is followed, destructured and chained from', () => {
		const adults = outgoing(store, who, 'knows', {
			whereTarget: (peer) => gte(peer.age, 18),
		});
		expect(
			select(PersonTable)
				.project(() => ({
					names: follow(store, adults).name,
					picked: destructure(store, adults, (peer) => ({
						n: peer.name,
					})),
					next: outgoing(store, adults, 'knows'),
				}))
				.build().sql,
		).toBe(
			'SELECT $who->knows->(? WHERE age >= 18).name AS names, $who->knows->(? WHERE age >= 18).{ n: name } AS picked, $who->knows->(? WHERE age >= 18)->knows->? AS next FROM ⟨person⟩;',
		);
	});

	it('refuses a field of another row, parent(), and the edges of the step', () => {
		expect(() =>
			select(PersonTable).project((row) => ({
				x: outgoing(store, row.id, 'knows', {
					whereTarget: (peer) => eq(peer.name, row.name),
				}),
			})),
		).toThrow(
			/`name` is a field of the row of a select of `person`, but in a step's `whereTarget` a field path reads the record the step reaches \(`person`\)/,
		);
		expect(() =>
			select(PersonTable).project((row) => ({
				x: outgoing(store, row.id, 'knows', {
					whereTarget: (peer) => eq(peer.name, parent(row.name)),
				}),
			})),
		).toThrow(/`parent\(row\.name\)` is refused in a step's `whereTarget`/);
		expect(() =>
			edges(
				outgoing(store, who, 'knows', {
					whereTarget: (peer) => gte(peer.age, 18),
				}),
			),
		).toThrow(
			/the edges of a step with a `whereTarget` filter would include the edges to the records it leaves out/,
		);
	});

	it('reads the fields every table on that side has', () => {
		const step = outgoing(store, who, 'member_of', {
			whereTarget: (target) => {
				// `members` is a field of `team` only, `name` of both.
				expect(Object.keys(target)).not.toContain('members');
				return eq(target.name, 'Red');
			},
		});
		expect(follow(store, step).name).toBeDefined();
		expect(
			select(PersonTable)
				.value(() => step)
				.build().sql,
		).toBe(
			'SELECT VALUE $who->member_of->(? WHERE name = s"Red") FROM ⟨person⟩;',
		);
	});
});
