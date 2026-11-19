/**
 * Record links, graph steps, recursive paths and FETCH without an engine:
 * the SurrealQL each renders, the bindings it collects, the codecs it
 * derives and what is refused when it is built. `test/graph-engine.test.ts`
 * runs the same shapes against the corpus schema.
 */
import { Schema } from 'effect';
import { RecordId } from 'surrealdb';
import { describe, expect, it } from 'vitest';

import * as SurrealSchema from '../src/index.js';
import {
	both,
	codecOf,
	collect,
	destructure,
	edges,
	eq,
	follow,
	incoming,
	isTraversal,
	outgoing,
	param,
	paramsOf,
	recurse,
	rowOf,
	rows,
	select,
	shortest,
	sqlOf,
} from '../src/statement/index.js';
import {
	DocTable,
	MemberOf,
	MemberOfTable,
	Person,
	PersonId,
	PersonTable,
	store,
	TeamTable,
} from './golden/corpus/index.js';

const person = rowOf(Person);
const membership = rowOf(MemberOf);

/** A builder with its types switched off, to reach its run-time checks. */
const untyped = <F>(f: F) =>
	f as unknown as (...args: ReadonlyArray<unknown>) => unknown;

const decode = (codec: unknown, value: unknown): unknown =>
	Schema.decodeUnknownSync(codec as Schema.Codec<unknown, unknown>)(value, {
		onExcessProperty: 'error',
	});

const ann = new RecordId('person', 'ann');
const red = new RecordId('team', 'red');
const acme = new RecordId('org', 'acme');

describe('record links', () => {
	it('follow a link to its target fields', () => {
		const manager = follow(store, person.manager);
		expect(sqlOf(manager.name)).toBe('manager.name');
		expect(sqlOf(manager.address.city)).toBe('manager.address.city');
		expect(sqlOf(manager['odd.name'])).toBe('manager.⟨odd.name⟩');
		expect(Object.keys(manager).sort()).toEqual(
			Object.keys(Person.fields).sort(),
		);
		expect(sqlOf(follow(store, manager.team).name)).toBe(
			'manager.team.name',
		);
	});

	it('read every field through a link as possibly NONE', () => {
		const owner = follow(store, person.manager);
		expect(decode(codecOf(owner.name), undefined)).toBeUndefined();
		expect(decode(codecOf(owner.name), 'Ann')).toBe('Ann');
		expect(() => decode(codecOf(owner.name), 5)).toThrow();
		expect(decode(codecOf(owner.address.city), undefined)).toBeUndefined();
	});

	it('read a list of links as a list, NONE for each missing record', () => {
		const watchers = follow(store, person.watchers);
		expect(sqlOf(watchers.name)).toBe('watchers.name');
		expect(decode(codecOf(watchers.name), [undefined, 'Ann'])).toEqual([
			undefined,
			'Ann',
		]);
	});

	it('expose the fields every target of a union link has', () => {
		const out = follow(store, membership.out);
		expect(Object.keys(out).sort()).toEqual(['id', 'name']);
		expect(decode(codecOf(out.id), red)).toBe(red);
		expect(decode(codecOf(out.id), acme)).toBe(acme);
		expect(() =>
			decode(codecOf(out.id), new RecordId('person', 'x')),
		).toThrow();
	});

	it('carry the bindings of the link expression', () => {
		const linked = follow(store, param('who', PersonId));
		expect(sqlOf(linked.name)).toBe('$who.name');
		expect(Object.keys(paramsOf(linked.name))).toEqual(['who']);
	});

	it('refuse what is not a link, or links to any table', () => {
		expect(() => untyped(follow)(store, person.name)).toThrow(
			/is not a record link/,
		);
		expect(() =>
			untyped(follow)(store, param('any', SurrealSchema.RecordIdAny)),
		).toThrow(/any table/);
	});

	it('materialise and destructure link targets', () => {
		expect(sqlOf(rows(store, person.manager))).toBe('manager.*');
		const picked = destructure(store, person.manager, (target) => ({
			n: target.name,
			city: target.address.city,
		}));
		expect(sqlOf(picked)).toBe('manager.{ n: name, city: address.city }');
		expect(decode(codecOf(picked), undefined)).toBeUndefined();
		expect(decode(codecOf(picked), { n: 'Ann', city: 'Leeds' })).toEqual({
			n: 'Ann',
			city: 'Leeds',
		});
	});

	it('take a projection under a select', () => {
		const statement = select(PersonTable)
			.where((row) =>
				eq(
					follow(store, row.manager).name,
					param('boss', SurrealSchema.String),
				),
			)
			.project((row) => ({ boss: follow(store, row.manager).name }))
			.build();
		expect(statement.sql).toBe(
			'SELECT manager.name AS boss FROM ⟨person⟩ WHERE manager.name = $boss;',
		);
		expect(statement.writes).toBeUndefined();
	});
});

describe('the README example', () => {
	it('renders as documented', () => {
		const byOwner = select(DocTable)
			.where((row) =>
				eq(
					follow(store, row.owner).name,
					param('owner', SurrealSchema.String),
				),
			)
			.project((row) => ({
				title: row.title,
				teams: outgoing(store, row.owner, 'member_of', {
					where: (edge) =>
						eq(edge.role, param('role', MemberOfTable.fields.role)),
				}),
				roles: follow(
					store,
					edges(outgoing(store, row.owner, 'member_of')),
				).role,
				reach: follow(
					store,
					collect(row.owner, (node) =>
						outgoing(store, node, 'knows'),
					),
				).name,
			}))
			.build();
		expect(byOwner.sql).toBe(
			'SELECT title AS title, owner->(member_of WHERE role = $role)->? AS teams, owner->member_of.role AS roles, owner.{..+collect}(->knows->?).name AS reach FROM ⟨doc⟩ WHERE owner.name = $owner;',
		);
		expect(
			select(PersonTable).fetch(store, 'manager', 'watchers').build().sql,
		).toBe('SELECT * FROM ⟨person⟩ FETCH manager, watchers;');
	});
});

describe('graph steps', () => {
	it('render each direction from the row', () => {
		expect(sqlOf(outgoing(store, person.id, 'member_of'))).toBe(
			'id->member_of->?',
		);
		expect(
			sqlOf(incoming(store, rowOf(TeamTable.select).id, 'member_of')),
		).toBe('id<-member_of<-?');
		expect(sqlOf(both(store, person.id, 'knows'))).toBe('id<->knows<->?');
	});

	it('yield the records on the other side, as a list', () => {
		const teams = outgoing(store, person.id, 'member_of');
		expect(isTraversal(teams)).toBe(true);
		expect(decode(codecOf(teams), [red, acme])).toEqual([red, acme]);
		expect(() => decode(codecOf(teams), [ann])).toThrow();
		expect(decode(codecOf(both(store, person.id, 'knows')), [ann])).toEqual(
			[ann],
		);
	});

	it('chain into one flat list', () => {
		const friends = outgoing(store, person.id, 'knows');
		const second = outgoing(store, friends, 'knows');
		expect(sqlOf(second)).toBe('id->knows->?->knows->?');
		expect(decode(codecOf(second), [ann, ann])).toEqual([ann, ann]);
		expect(sqlOf(outgoing(store, friends, 'member_of'))).toBe(
			'id->knows->?->member_of->?',
		);
	});

	it('start from a link, a binding or the row', () => {
		expect(sqlOf(outgoing(store, person.manager, 'knows'))).toBe(
			'manager->knows->?',
		);
		const fromBinding = outgoing(store, param('who', PersonId), 'knows');
		expect(sqlOf(fromBinding)).toBe('$who->knows->?');
		expect(Object.keys(paramsOf(fromBinding))).toEqual(['who']);
		// From an optional link the list itself may be NONE.
		expect(
			decode(
				codecOf(outgoing(store, person.manager, 'knows')),
				undefined,
			),
		).toBeUndefined();
	});

	it('filter the edges they walk', () => {
		const leads = outgoing(store, person.id, 'member_of', {
			where: (edge) => eq(edge.role, param('role', MemberOf.fields.role)),
		});
		expect(sqlOf(leads)).toBe('id->(member_of WHERE role = $role)->?');
		expect(Object.keys(paramsOf(leads))).toEqual(['role']);
	});

	it('read the edges and their fields', () => {
		const step = outgoing(store, person.id, 'member_of', {
			where: (edge) => eq(edge.role, 'lead'),
		});
		const walked = edges(step);
		expect(sqlOf(walked)).toBe('id->(member_of WHERE role = s"lead")');
		expect(sqlOf(follow(store, walked).role)).toBe(
			'id->(member_of WHERE role = s"lead").role',
		);
		expect(decode(codecOf(follow(store, walked).role), ['lead'])).toEqual([
			'lead',
		]);
	});

	it('materialise and destructure the records they reach', () => {
		const teams = outgoing(store, person.id, 'member_of');
		expect(sqlOf(rows(store, teams))).toBe('id->member_of->?.*');
		expect(
			decode(codecOf(rows(store, teams)), [
				{ id: red, name: 'Red', members: [] },
				{ id: acme, name: 'Acme' },
			]),
		).toEqual([
			{ id: red, name: 'Red', members: [] },
			{ id: acme, name: 'Acme' },
		]);
		const named = destructure(store, teams, (team) => ({
			id: team.id,
			title: team.name,
			members: incoming(store, team.id, 'member_of'),
		}));
		expect(sqlOf(named)).toBe(
			'id->member_of->?.{ id: id, title: name, members: id<-member_of<-? }',
		);
		expect(
			decode(codecOf(named), [{ id: red, title: 'Red', members: [ann] }]),
		).toEqual([{ id: red, title: 'Red', members: [ann] }]);
		// The engine drops what the session may not read, so an enforced
		// relation's list holds no NONE; one that is not enforced may.
		expect(() => decode(codecOf(named), [undefined])).toThrow();
		expect(
			decode(
				codecOf(rows(store, outgoing(store, person.id, 'follows'))),
				[undefined],
			),
		).toEqual([undefined]);
	});

	it('render in a projection', () => {
		const statement = select(PersonTable)
			.project((row) => ({
				teams: outgoing(store, row.id, 'member_of'),
				roles: follow(
					store,
					edges(outgoing(store, row.id, 'member_of')),
				).role,
			}))
			.build();
		expect(statement.sql).toBe(
			'SELECT id->member_of->? AS teams, id->member_of.role AS roles FROM ⟨person⟩;',
		);
	});

	it('refuse a start the relation does not accept', () => {
		const team = rowOf(TeamTable.select);
		expect(() => untyped(outgoing)(store, team.id, 'member_of')).toThrow(
			'`member_of` has no edges out of `team`',
		);
		expect(() => untyped(incoming)(store, person.id, 'member_of')).toThrow(
			'`member_of` has no edges into `person`',
		);
		expect(() =>
			untyped(outgoing)(store, person.watchers, 'knows'),
		).toThrow(/one list per record/);
		expect(() =>
			untyped(outgoing)(
				store,
				collect(person.id, (node) => outgoing(store, node, 'knows')),
				'knows',
			),
		).toThrow(/one list per record/);
		expect(() => untyped(outgoing)(store, person.name, 'knows')).toThrow(
			/not a record id/,
		);
		expect(() => untyped(outgoing)(store, person.id, 'nope')).toThrow(
			'`nope` is not a relation of this store',
		);
	});
});

describe('recursive paths', () => {
	const knows = (node: unknown) => untyped(outgoing)(store, node, 'knows');

	it('render a bounded depth range', () => {
		const reached = recurse(person.id, { min: 1, max: 3 }, (node) =>
			outgoing(store, node, 'knows'),
		);
		expect(sqlOf(reached)).toBe('id.{1..3}(->knows->?)');
		expect(decode(codecOf(reached), [ann])).toEqual([ann]);
	});

	it('render collect, with its options', () => {
		expect(
			sqlOf(collect(person.id, (node) => outgoing(store, node, 'knows'))),
		).toBe('id.{..+collect}(->knows->?)');
		expect(
			sqlOf(
				collect(person.id, (node) => outgoing(store, node, 'knows'), {
					min: 2,
					max: 4,
					inclusive: true,
				}),
			),
		).toBe('id.{2..4+collect+inclusive}(->knows->?)');
		expect(
			sqlOf(
				collect(person.id, (node) => outgoing(store, node, 'knows'), {
					min: 2,
				}),
			),
		).toBe('id.{2..+collect}(->knows->?)');
		expect(
			sqlOf(
				collect(person.id, (node) => outgoing(store, node, 'knows'), {
					max: 2,
				}),
			),
		).toBe('id.{1..2+collect}(->knows->?)');
	});

	it('render a shortest path to a binding, which may be NONE', () => {
		const path = shortest(person.id, param('to', PersonId), (node) =>
			outgoing(store, node, 'knows'),
		);
		expect(sqlOf(path)).toBe('id.{..+shortest=$to}(->knows->?)');
		expect(Object.keys(paramsOf(path))).toEqual(['to']);
		expect(decode(codecOf(path), undefined)).toBeUndefined();
		expect(
			sqlOf(
				shortest(
					person.id,
					param('to', PersonId),
					(node) => outgoing(store, node, 'knows'),
					{
						inclusive: true,
					},
				),
			),
		).toBe('id.{..+shortest=$to+inclusive}(->knows->?)');
	});

	it('repeat a filtered step and read what it reaches', () => {
		const reached = collect(param('from', PersonId), (node) =>
			outgoing(store, node, 'follows', {
				where: (edge) => eq(edge.in, param('owner', PersonId)),
			}),
		);
		expect(sqlOf(reached)).toBe(
			'$from.{..+collect}(->(follows WHERE in = $owner)->?)',
		);
		expect(Object.keys(paramsOf(reached)).sort()).toEqual([
			'from',
			'owner',
		]);
		expect(sqlOf(follow(store, reached).name)).toBe(
			'$from.{..+collect}(->(follows WHERE in = $owner)->?).name',
		);
	});

	it('refuse depths the engine refuses or answers NONE for', () => {
		expect(() =>
			untyped(recurse)(person.id, { min: 0, max: 2 }, knows),
		).toThrow(
			'the least depth is 0; a recursion depth is an integer from 1 to 256',
		);
		expect(() =>
			untyped(recurse)(person.id, { min: 1, max: 257 }, knows),
		).toThrow(/the greatest depth is 257/);
		expect(() =>
			untyped(recurse)(person.id, { min: 3, max: 2 }, knows),
		).toThrow(/greater than the greatest/);
		expect(() =>
			untyped(recurse)(person.id, { min: 1.5, max: 2 }, knows),
		).toThrow(/integer/);
	});

	it('refuse a step that does not lead back, or is not a step', () => {
		expect(() =>
			untyped(recurse)(person.id, { min: 1, max: 2 }, (node: unknown) =>
				untyped(outgoing)(store, node, 'member_of'),
			),
		).toThrow(/leads back to the table/);
		expect(() =>
			untyped(recurse)(person.id, { min: 1, max: 2 }, () => person.id),
		).toThrow(/repeats a graph step/);
		expect(() =>
			untyped(recurse)(person.watchers, { min: 1, max: 2 }, knows),
		).toThrow(/one list per record|one record/);
	});
});

describe('FETCH', () => {
	const PalId = SurrealSchema.RecordIdFor('pal').pipe(Schema.brand('t/pal'));
	const Pal = Schema.Struct({
		id: PalId,
		n: SurrealSchema.Int,
		next: Schema.optionalKey(PalId),
		friends: Schema.Array(PalId),
		box: Schema.Struct({ pal: PalId }),
	});
	const PalTable = { name: 'pal', id: PalId, select: Pal } as const;
	const pals = { tables: { pal: PalTable }, edges: {} } as const;
	const a = new RecordId('pal', 'a');
	const b = new RecordId('pal', 'b');
	const leaf = { id: b, n: 2, friends: [], box: { pal: a } };

	it('renders after LIMIT and START, each path quoted', () => {
		expect(
			select(PersonTable)
				.limit(2)
				.start(1)
				.fetch(store, 'manager', 'watchers')
				.build().sql,
		).toBe(
			'SELECT * FROM ⟨person⟩ LIMIT 2 START 1 FETCH manager, watchers;',
		);
		expect(
			select(PalTable).fetch(pals, 'next.next', 'box.pal').build().sql,
		).toBe('SELECT * FROM ⟨pal⟩ FETCH next.next, box.pal;');
	});

	it('replaces a link with its row, which may be absent', () => {
		const statement = select(PalTable).fetch(pals, 'next').build();
		expect(
			decode(statement.result, [
				{ id: a, n: 1, next: leaf, friends: [], box: { pal: b } },
				{ id: b, n: 2, next: undefined, friends: [], box: { pal: a } },
			]),
		).toEqual([
			{ id: a, n: 1, next: leaf, friends: [], box: { pal: b } },
			{ id: b, n: 2, friends: [], box: { pal: a } },
		]);
		expect(() =>
			decode(statement.result, [{ ...leaf, next: b }]),
		).toThrow();
		expect(statement.writes).toBe(false);
	});

	it('replaces a list of links, each record possibly NONE, and a nested link', () => {
		const statement = select(PalTable)
			.fetch(pals, 'friends')
			.fetch(pals, 'box.pal')
			.build();
		expect(
			decode(statement.result, [
				{ id: a, n: 1, friends: [undefined, leaf], box: { pal: leaf } },
			]),
		).toEqual([
			{ id: a, n: 1, friends: [undefined, leaf], box: { pal: leaf } },
		]);
	});

	it('fetches through a fetched record', () => {
		const statement = select(PalTable).fetch(pals, 'next.next').build();
		const deep = { ...leaf, next: { ...leaf, id: a } };
		expect(
			decode(statement.result, [
				{ id: a, n: 1, next: deep, friends: [], box: { pal: b } },
			]),
		).toEqual([{ id: a, n: 1, next: deep, friends: [], box: { pal: b } }]);
	});

	it('names the aliases of a projection', () => {
		const statement = select(PalTable)
			.project((row) => ({ boss: row.next, n: row.n }))
			.fetch(pals, 'boss')
			.only()
			.build();
		expect(statement.sql).toBe(
			'SELECT next AS boss, n AS n FROM ONLY ⟨pal⟩ LIMIT 1 FETCH boss;',
		);
		expect(decode(statement.result, { boss: leaf, n: 1 })).toEqual({
			boss: leaf,
			n: 1,
		});
		expect(decode(statement.result, { boss: undefined, n: 1 })).toEqual({
			boss: undefined,
			n: 1,
		});
	});

	it('refuses a projection after it, SELECT VALUE, and what is not a link', () => {
		expect(() =>
			untyped(select(PalTable).fetch(pals, 'next').project)(
				(row: { readonly n: unknown }) => ({ n: row.n }),
			),
		).toThrow(/call `project` before `fetch`/);
		expect(() =>
			untyped(select(PalTable).value((row) => row.next).fetch)(
				pals,
				'next',
			),
		).toThrow(/SELECT VALUE/);
		expect(() => untyped(select(PalTable).fetch)(pals, 'n')).toThrow(
			'`n` is not a record link, so FETCH has nothing to replace',
		);
		expect(() => untyped(select(PalTable).fetch)(pals, 'nope')).toThrow(
			'`nope` is not a field of what the select returns',
		);
		expect(() => untyped(select(PalTable).fetch)(pals, 'next.')).toThrow(
			/not a FETCH path/,
		);
	});

	it('fetches after OMIT, from the row without the omitted fields, and before FOR UPDATE', () => {
		const omitted = select(PalTable)
			.omit('box')
			.fetch(pals, 'next')
			.build();
		expect(omitted.sql).toBe('SELECT * OMIT box FROM ⟨pal⟩ FETCH next;');
		expect(
			decode(omitted.result, [{ id: a, n: 1, next: leaf, friends: [] }]),
		).toEqual([{ id: a, n: 1, next: leaf, friends: [] }]);
		expect(
			select(PersonTable, param('who', PersonId))
				.forUpdate()
				.fetch(store, 'manager')
				.build().sql,
		).toBe('SELECT * FROM $who FETCH manager FOR UPDATE;');
	});

	it('refuses OMIT, SPLIT and grouping after it', () => {
		const fetched = select(PalTable).fetch(pals, 'next');
		expect(() => untyped(fetched.omit)('box')).toThrow(
			/call `omit` before `fetch`/,
		);
		expect(() => untyped(fetched.split)('friends')).toThrow(
			/call `split` before `fetch`/,
		);
		expect(() => untyped(fetched.groupAll)()).toThrow(
			/group a SELECT before projecting, ordering, limiting, fetching it/,
		);
	});

	it('fetches a union link to the row of whichever table it names', () => {
		const statement = select(store.tables.member_of)
			.project((row) => ({ out: row.out }))
			.fetch(store, 'out')
			.build();
		expect(
			decode(statement.result, [
				{ out: { id: red, name: 'Red', members: [] } },
				{ out: { id: acme, name: 'Acme' } },
			]),
		).toEqual([
			{ out: { id: red, name: 'Red', members: [] } },
			{ out: { id: acme, name: 'Acme' } },
		]);
	});
});
