/**
 * Positive type fixtures for record links, graph steps, recursive paths
 * and FETCH: what they derive from the golden corpus modules and their
 * store registry. Compiled by `tsc -p test/types`.
 */
import * as SurrealSchema from 'effect-surrealkit';

import {
	both,
	collect,
	contains,
	destructure,
	edges,
	eq,
	follow,
	type Input,
	incoming,
	ne,
	outgoing,
	param,
	parent,
	type Result,
	recurse,
	rows,
	select,
	shortest,
} from '../../src/statement/index.js';
import {
	type DocRow,
	DocTable,
	MemberOfTable,
	type OrgId,
	type OrgRow,
	PersonId,
	type PersonRow,
	PersonTable,
	store,
	type TeamId,
	type TeamRow,
	TeamTable,
} from '../golden/corpus/index.js';

/** Fails to compile unless `A` and `B` are the same type. */
type Same<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;
const same = <A, B>(_: Same<A, B>): void => undefined;

// ---- record links: the target's fields, each possibly NONE.

export const byOwner = select(DocTable)
	.where((row) =>
		eq(follow(store, row.owner).name, param('owner', SurrealSchema.String)),
	)
	.project((row) => ({
		title: row.title,
		owner: follow(store, row.owner).name,
		city: follow(store, row.owner).address.city,
		boss: follow(store, follow(store, row.owner).manager).email,
	}))
	.build();
same<
	Result<typeof byOwner>,
	ReadonlyArray<{
		readonly title: string;
		readonly owner: string | undefined;
		readonly city: string | undefined;
		readonly boss: string | undefined;
	}>
>(true);
same<Input<(typeof byOwner)['params']>, { readonly owner: string }>(true);

// A list of links reads as a list; a union link exposes the common fields.
export const watched = select(PersonTable)
	.project((row) => ({ names: follow(store, row.watchers).name }))
	.build();
same<
	Result<typeof watched>,
	ReadonlyArray<{ readonly names: ReadonlyArray<string | undefined> }>
>(true);

export const memberships = select(MemberOfTable)
	.project((row) => ({
		target: follow(store, row.out).id,
		name: follow(store, row.out).name,
	}))
	.build();
same<
	Result<typeof memberships>,
	ReadonlyArray<{
		readonly target: OrgId | TeamId | undefined;
		readonly name: string | undefined;
	}>
>(true);

export const managers = select(PersonTable)
	.project((row) => ({
		manager: rows(store, row.manager),
		card: destructure(store, row.manager, (boss) => ({
			name: boss.name,
			nickname: boss.nickname,
		})),
	}))
	.build();
same<
	Result<typeof managers>,
	ReadonlyArray<{
		readonly manager: PersonRow | undefined;
		readonly card:
			| { readonly name: string; readonly nickname: string | undefined }
			| undefined;
	}>
>(true);

// ---- graph steps: lists of the records on the other side.

export const teams = select(PersonTable)
	.project((row) => ({
		teams: outgoing(store, row.id, 'member_of'),
		leads: outgoing(store, row.id, 'member_of', {
			where: (edge) =>
				eq(edge.role, param('role', MemberOfTable.fields.role)),
		}),
		roles: follow(store, edges(outgoing(store, row.id, 'member_of'))).role,
		names: follow(store, outgoing(store, row.id, 'member_of')).name,
		friends: rows(store, outgoing(store, row.id, 'knows')),
		reach: outgoing(store, outgoing(store, row.id, 'knows'), 'member_of'),
		around: both(store, row.id, 'knows'),
		cards: destructure(
			store,
			outgoing(store, row.id, 'member_of'),
			(team) => ({
				id: team.id,
				title: team.name,
			}),
		),
		followed: rows(store, outgoing(store, row.id, 'follows')),
	}))
	.build();
same<
	Result<typeof teams>,
	ReadonlyArray<{
		readonly teams: ReadonlyArray<OrgId | TeamId>;
		readonly leads: ReadonlyArray<OrgId | TeamId>;
		readonly roles: ReadonlyArray<'lead' | 'member'>;
		readonly names: ReadonlyArray<string>;
		readonly friends: ReadonlyArray<PersonRow>;
		readonly reach: ReadonlyArray<OrgId | TeamId>;
		readonly around: ReadonlyArray<PersonId>;
		readonly cards: ReadonlyArray<{
			readonly id: OrgId | TeamId;
			readonly title: string;
		}>;
		// `follows` is not enforced, so an edge can point at a missing record.
		readonly followed: ReadonlyArray<PersonRow | undefined>;
	}>
>(true);
same<Input<(typeof teams)['params']>, { readonly role: 'lead' | 'member' }>(
	true,
);

export const members = select(TeamTable)
	.value((row) => incoming(store, row.id, 'member_of'))
	.build();
same<Result<typeof members>, ReadonlyArray<ReadonlyArray<PersonId>>>(true);

// From an optional link the list may be NONE; a step works in conditions.
export const managed = select(PersonTable)
	.where((row) =>
		contains(
			outgoing(store, row.id, 'member_of'),
			param('team', TeamTable.id),
		),
	)
	.value((row) => outgoing(store, row.manager, 'knows'))
	.build();
same<
	Result<typeof managed>,
	ReadonlyArray<ReadonlyArray<PersonId> | undefined>
>(true);

// ---- recursive paths

export const reached = select(PersonTable, param('from', PersonId))
	.only()
	.project((row) => ({
		depth: recurse(row.id, { min: 1, max: 3 }, (node) =>
			outgoing(store, node, 'knows'),
		),
		all: collect(row.id, (node) => outgoing(store, node, 'knows'), {
			inclusive: true,
		}),
		path: shortest(row.id, param('to', PersonId), (node) =>
			outgoing(store, node, 'knows'),
		),
		names: follow(
			store,
			collect(row.id, (node) => outgoing(store, node, 'knows')),
		).name,
	}))
	.build();
same<
	Result<typeof reached>,
	| {
			readonly depth: ReadonlyArray<PersonId>;
			readonly all: ReadonlyArray<PersonId>;
			readonly path: ReadonlyArray<PersonId> | undefined;
			readonly names: ReadonlyArray<string>;
	  }
	| undefined
>(true);
same<
	Input<(typeof reached)['params']>,
	{ readonly from: PersonId; readonly to: PersonId }
>(true);

// ---- FETCH: a link becomes its row, possibly absent.

export const fetched = select(PersonTable).fetch(store, 'manager').build();
type FetchedPerson = Result<typeof fetched>[number];
same<FetchedPerson['manager'], PersonRow | undefined>(true);
same<FetchedPerson['name'], string>(true);

export const fetchedList = select(PersonTable)
	.fetch(store, 'watchers', 'team')
	.build();
type FetchedList = Result<typeof fetchedList>[number];
same<FetchedList['watchers'], ReadonlyArray<PersonRow | undefined>>(true);
same<FetchedList['team'], TeamRow | undefined>(true);

export const fetchedDeep = select(DocTable)
	.fetch(store, 'owner.manager')
	.build();
type FetchedDoc = Result<typeof fetchedDeep>[number];
same<NonNullable<FetchedDoc['owner']>['manager'], PersonRow | undefined>(true);
same<Omit<FetchedDoc, 'owner'>, Omit<DocRow, 'owner'>>(true);

export const fetchedUnion = select(MemberOfTable)
	.project((row) => ({ out: row.out, role: row.role }))
	.fetch(store, 'out')
	.build();
same<Result<typeof fetchedUnion>[number]['out'], OrgRow | TeamRow | undefined>(
	true,
);

// ---- parent(row.field): the select's row, read from a step's `where`.

export const notManaged = select(PersonTable)
	.project((row) => ({
		peers: outgoing(store, row.id, 'knows', {
			where: (edge) =>
				ne(follow(store, edge.out).manager, parent(row.id)),
		}),
		boss: outgoing(store, row.id, 'knows', {
			where: (edge) => eq(edge.out, parent(row.manager)),
		}),
	}))
	.build();
same<Result<typeof notManaged>[number]['peers'], ReadonlyArray<PersonId>>(true);
