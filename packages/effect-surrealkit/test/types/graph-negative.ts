/**
 * Negative type fixtures for record links, graph steps, recursive paths
 * and FETCH: each `@ts-expect-error` must be an actual error, and
 * `test/diagnostics.test.ts` pins the first lines of its diagnostic.
 */
import { run } from '../../src/client/index.js';
import {
	collect,
	destructure,
	edges,
	eq,
	follow,
	incoming,
	outgoing,
	param,
	parent,
	recurse,
	rowOf,
	select,
	selectFrom,
	shortest,
} from '../../src/statement/index.js';
import {
	DocTable,
	Kinds,
	KindsTable,
	MemberOfTable,
	Person,
	PersonId,
	PersonTable,
	store,
	TeamId,
	TeamTable,
} from '../golden/corpus/index.js';
import { evaluated, projected } from './graph-origins.js';

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
		x: outgoing(store, row.id, 'knows', {
			// @ts-expect-error [step/parent-not-a-path] `parent` reads a field path of the select's row
			where: (edge) => eq(edge.out, parent(param('who', PersonId))),
		}),
	}));
	select(PersonTable).project((row) => ({
		x: edges(
			// @ts-expect-error [step/edges-of-recursion] a recursive path walks no edges of its own
			collect(row.id, (node) => outgoing(store, node, 'knows')),
		),
	}));
};

// ---- the records a step reaches

export const rejectedReached = () => {
	selectFrom(
		store,
		// @ts-expect-error [subject/several-tables] `member_of` leads to `org` and `team`
		outgoing(store, param('who', PersonId), 'member_of'),
	);
	// @ts-expect-error [subject/not-a-traversal] a select reads the records of a step
	selectFrom(store, param('who', PersonId));
	outgoing(store, param('who', PersonId), 'knows', {
		// @ts-expect-error [step/target-filter-no-such-field] the records reached have no such field
		whereTarget: (peer) => eq(peer.nope, 1),
	});
	outgoing(store, param('who', PersonId), 'member_of', {
		// @ts-expect-error [step/target-filter-uncommon-field] only `team` has `members`
		whereTarget: (target) => eq(target.members, 1),
	});
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

// ---- FETCH

export const rejectedFetches = () => {
	// @ts-expect-error [fetch/not-a-link] `name` is a string
	select(PersonTable).fetch(store, 'name');
	// @ts-expect-error [fetch/no-such-field] the row has no such field
	select(PersonTable).fetch(store, 'nope');
	// @ts-expect-error [fetch/any-table] `record` can point at any table
	select(KindsTable).fetch(store, 'k_record_any');
	// @ts-expect-error [fetch/nested-not-a-link] `manager.name` is a string of the fetched record
	select(PersonTable).fetch(store, 'manager.name');
	select(PersonTable)
		.project((row) => ({ boss: row.manager }))
		// @ts-expect-error [fetch/projection-has-no-such-alias] FETCH names the projection's aliases
		.fetch(store, 'manager');
	select(PersonTable)
		.fetch(store, 'manager')
		// @ts-expect-error [fetch/project-after] a projection comes before FETCH
		.project((row) => ({ name: row.name }));
	select(PersonTable)
		.fetch(store, 'manager')
		.where((row) => eq(row.name, 'Ann'))
		// @ts-expect-error [fetch/value-after] SELECT VALUE takes no FETCH, before or after a condition
		.value((row) => row.name);
	// @ts-expect-error [fetch/group-after] grouping comes before FETCH
	select(PersonTable).fetch(store, 'manager').groupAll();
};

// ---- a statement from a step that starts at a binding asks for it

export const rejectedOrigins = (who: PersonId) => {
	// @ts-expect-error [origin/evaluated-outgoing-missing] the step starts at `$who`
	run(evaluated.outgoing, {});
	// @ts-expect-error [origin/evaluated-outgoing-wrong-type] `$who` is a person id
	run(evaluated.outgoing, { who: 'person:ann' });
	run(evaluated.outgoing, { who });
	// @ts-expect-error [origin/evaluated-incoming-missing] the step starts at `$who`
	run(evaluated.incoming, {});
	// @ts-expect-error [origin/evaluated-incoming-wrong-type] `$who` is a person id
	run(evaluated.incoming, { who: 'person:ann' });
	run(evaluated.incoming, { who });
	// @ts-expect-error [origin/evaluated-nested-missing] the step starts at `$who`
	run(evaluated.nested, {});
	// @ts-expect-error [origin/evaluated-nested-wrong-type] `$who` is a person id
	run(evaluated.nested, { who: 'person:ann' });
	run(evaluated.nested, { who });
	// @ts-expect-error [origin/evaluated-follow-missing] the step starts at `$who`
	run(evaluated.follow, {});
	// @ts-expect-error [origin/evaluated-follow-wrong-type] `$who` is a person id
	run(evaluated.follow, { who: 'person:ann' });
	run(evaluated.follow, { who });
	// @ts-expect-error [origin/evaluated-followTwice-missing] the step starts at `$who`
	run(evaluated.followTwice, {});
	// @ts-expect-error [origin/evaluated-followTwice-wrong-type] `$who` is a person id
	run(evaluated.followTwice, { who: 'person:ann' });
	run(evaluated.followTwice, { who });
	// @ts-expect-error [origin/evaluated-destructure-missing] the step starts at `$who`
	run(evaluated.destructure, {});
	// @ts-expect-error [origin/evaluated-destructure-wrong-type] `$who` is a person id
	run(evaluated.destructure, { who: 'person:ann' });
	run(evaluated.destructure, { who });
	// @ts-expect-error [origin/evaluated-destructureStep-missing] the step starts at `$who`
	run(evaluated.destructureStep, {});
	// @ts-expect-error [origin/evaluated-destructureStep-wrong-type] `$who` is a person id
	run(evaluated.destructureStep, { who: 'person:ann' });
	run(evaluated.destructureStep, { who });
	// @ts-expect-error [origin/evaluated-edges-missing] the step starts at `$who`
	run(evaluated.edges, {});
	// @ts-expect-error [origin/evaluated-edges-wrong-type] `$who` is a person id
	run(evaluated.edges, { who: 'person:ann' });
	run(evaluated.edges, { who });
	// @ts-expect-error [origin/evaluated-rows-missing] the step starts at `$who`
	run(evaluated.rows, {});
	// @ts-expect-error [origin/evaluated-rows-wrong-type] `$who` is a person id
	run(evaluated.rows, { who: 'person:ann' });
	run(evaluated.rows, { who });
	// @ts-expect-error [origin/evaluated-collect-missing] the step starts at `$who`
	run(evaluated.collect, {});
	// @ts-expect-error [origin/evaluated-collect-wrong-type] `$who` is a person id
	run(evaluated.collect, { who: 'person:ann' });
	run(evaluated.collect, { who });
	// @ts-expect-error [origin/projected-outgoing-missing] the step starts at `$who`
	run(projected.outgoing, {});
	// @ts-expect-error [origin/projected-outgoing-wrong-type] `$who` is a person id
	run(projected.outgoing, { who: 'person:ann' });
	run(projected.outgoing, { who });
	// @ts-expect-error [origin/projected-incoming-missing] the step starts at `$who`
	run(projected.incoming, {});
	// @ts-expect-error [origin/projected-incoming-wrong-type] `$who` is a person id
	run(projected.incoming, { who: 'person:ann' });
	run(projected.incoming, { who });
	// @ts-expect-error [origin/projected-nested-missing] the step starts at `$who`
	run(projected.nested, {});
	// @ts-expect-error [origin/projected-nested-wrong-type] `$who` is a person id
	run(projected.nested, { who: 'person:ann' });
	run(projected.nested, { who });
	// @ts-expect-error [origin/projected-follow-missing] the step starts at `$who`
	run(projected.follow, {});
	// @ts-expect-error [origin/projected-follow-wrong-type] `$who` is a person id
	run(projected.follow, { who: 'person:ann' });
	run(projected.follow, { who });
	// @ts-expect-error [origin/projected-followTwice-missing] the step starts at `$who`
	run(projected.followTwice, {});
	// @ts-expect-error [origin/projected-followTwice-wrong-type] `$who` is a person id
	run(projected.followTwice, { who: 'person:ann' });
	run(projected.followTwice, { who });
	// @ts-expect-error [origin/projected-destructure-missing] the step starts at `$who`
	run(projected.destructure, {});
	// @ts-expect-error [origin/projected-destructure-wrong-type] `$who` is a person id
	run(projected.destructure, { who: 'person:ann' });
	run(projected.destructure, { who });
	// @ts-expect-error [origin/projected-destructureStep-missing] the step starts at `$who`
	run(projected.destructureStep, {});
	// @ts-expect-error [origin/projected-destructureStep-wrong-type] `$who` is a person id
	run(projected.destructureStep, { who: 'person:ann' });
	run(projected.destructureStep, { who });
	// @ts-expect-error [origin/projected-selectFrom-missing] the step starts at `$who`
	run(projected.selectFrom, {});
	// @ts-expect-error [origin/projected-selectFrom-wrong-type] `$who` is a person id
	run(projected.selectFrom, { who: 'person:ann' });
	run(projected.selectFrom, { who });
};
