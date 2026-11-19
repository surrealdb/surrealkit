/**
 * Negative type fixtures for record links, graph steps, recursive paths
 * and FETCH: each `@ts-expect-error` must be an actual error, and
 * `test/diagnostics.test.ts` pins the first lines of its diagnostic.
 */
import {
	collect,
	destructure,
	edges,
	eq,
	follow,
	incoming,
	outgoing,
	param,
	recurse,
	rowOf,
	select,
	shortest,
} from '../../src/statement/index.js';
import {
	DocTable,
	Kinds,
	MemberOfTable,
	Person,
	PersonTable,
	store,
	TeamId,
	TeamTable,
} from '../golden/corpus/index.js';

const person = rowOf(Person);
const kinds = rowOf(Kinds);

// ---- record links

export const rejectedLinks = () => {
	// @ts-expect-error [link/not-a-link] a string field links to nothing
	follow(store, person.name);
	// @ts-expect-error [link/any-table] `record` can point at any table
	follow(store, kinds.k_record_any);
	select(DocTable).project((row) => ({
		// @ts-expect-error [link/no-such-field] the target has no such field
		x: follow(store, row.owner).nope,
	}));
	select(MemberOfTable).project((row) => ({
		// @ts-expect-error [link/union-uncommon-field] only `team` has `members`
		x: follow(store, row.out).members,
	}));
	select(PersonTable).project((row) => ({
		// @ts-expect-error [link/destructure-constant] a destructured field is an expression
		x: destructure(store, row.manager, () => ({ n: 1 })),
	}));
};

// ---- graph steps

export const rejectedSteps = () => {
	select(TeamTable).project((row) => ({
		// @ts-expect-error [step/out-of-wrong-table] `member_of` goes out of `person` only
		x: outgoing(store, row.id, 'member_of'),
	}));
	select(PersonTable).project((row) => ({
		// @ts-expect-error [step/into-wrong-table] `member_of` leads into `org` and `team` only
		x: incoming(store, row.id, 'member_of'),
	}));
	select(PersonTable).project((row) => ({
		// @ts-expect-error [step/not-a-relation] `team` is a table, not a relation
		x: outgoing(store, row.id, 'team'),
	}));
	select(PersonTable).project((row) => ({
		// @ts-expect-error [step/from-list-of-links] a list of links yields one list per link
		x: outgoing(store, row.watchers, 'knows'),
	}));
	select(PersonTable).project((row) => ({
		x: outgoing(
			store,
			// @ts-expect-error [step/after-recursion] after a recursive path SurrealDB nests the lists
			collect(row.id, (node) => outgoing(store, node, 'knows')),
			'member_of',
		),
	}));
	select(PersonTable).project((row) => ({
		// @ts-expect-error [step/from-non-id] a step starts from a record id
		x: outgoing(store, row.name, 'knows'),
	}));
	select(PersonTable).project((row) => ({
		x: outgoing(store, row.id, 'member_of', {
			// @ts-expect-error [step/filter-no-such-field] the edge has no such field
			where: (edge) => eq(edge.nope, 1),
		}),
	}));
	select(PersonTable).project((row) => ({
		x: outgoing(store, row.id, 'member_of', {
			// @ts-expect-error [step/filter-wrong-type] `role` is a literal union
			where: (edge) => eq(edge.role, 5),
		}),
	}));
	select(PersonTable).project((row) => ({
		x: edges(
			// @ts-expect-error [step/edges-of-recursion] a recursive path walks no edges of its own
			collect(row.id, (node) => outgoing(store, node, 'knows')),
		),
	}));
};

// ---- recursive paths

export const rejectedRecursion = () => {
	select(PersonTable).project((row) => ({
		x: recurse(row.id, { min: 1, max: 2 }, (node) =>
			// @ts-expect-error [recurse/not-back-to-start] `member_of` leads away from `person`
			outgoing(store, node, 'member_of'),
		),
	}));
	select(PersonTable).project((row) => ({
		// @ts-expect-error [recurse/not-a-step] a recursive path repeats a graph step
		x: collect(row.id, () => row.id),
	}));
	select(PersonTable).project((row) => ({
		x: collect(
			// @ts-expect-error [recurse/from-a-list] recursion starts from one record
			outgoing(store, row.id, 'knows'),
			(node) => outgoing(store, node, 'knows'),
		),
	}));
	select(PersonTable).project((row) => ({
		x: shortest(
			row.id,
			// @ts-expect-error [recurse/shortest-to-other-table] a path of `knows` never reaches a team
			param('to', TeamId),
			(node) => outgoing(store, node, 'knows'),
		),
	}));
};
