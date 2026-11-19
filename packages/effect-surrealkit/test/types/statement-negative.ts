/**
 * Negative type fixtures for `effect-surrealkit/statement` and
 * `effect-surrealkit/client`: each `@ts-expect-error` must be an actual
 * error, or `tsc -p test/types` fails.
 */
import * as SurrealSchema from 'effect-surrealkit';

import { bind, run, transaction } from '../../src/client/index.js';
import {
	and,
	at,
	call,
	contains,
	create,
	eq,
	gt,
	inside,
	not,
	param,
	part,
	relate,
	select,
	sequence,
	update,
} from '../../src/statement/index.js';
import {
	fn_greet,
	MemberOfTable,
	PersonCreate,
	PersonId,
	PersonMerge,
	PersonTable,
	TeamId,
} from '../golden/example/index.js';

// ---- expressions

export const rejectedConditions = () => {
	select(PersonTable).where((row) =>
		// @ts-expect-error a string column never compares with a number
		eq(row.name, 5),
	);
	select(PersonTable).where((row) =>
		// @ts-expect-error an int column never compares with a string binding
		gt(row.age, param('minimum', SurrealSchema.String)),
	);
	select(PersonTable).where((row) =>
		// @ts-expect-error `role` is a literal union, and this is not a member
		eq(row.role, 'guest'),
	);
	select(PersonTable).where((row) =>
		// @ts-expect-error a person id never compares with a team id
		eq(row.manager, param('team', TeamId)),
	);
	// @ts-expect-error the table has no such field
	select(PersonTable).where((row) => eq(row.nope, 1));
	// @ts-expect-error `address` has no such child
	select(PersonTable).where((row) => eq(row.address.street, 'x'));
	// @ts-expect-error a condition is a boolean expression, not a string field
	select(PersonTable).where((row) => row.name);
	// @ts-expect-error `and` takes boolean expressions
	select(PersonTable).where((row) => and(row.name, eq(row.role, 'admin')));
	// @ts-expect-error `not` takes a boolean expression
	select(PersonTable).where((row) => not(row.email));
	select(PersonTable).where((row) =>
		// @ts-expect-error the elements of this list are not strings
		inside(row.name, [1, 2]),
	);
	select(PersonTable).where((row) =>
		// @ts-expect-error `tags` holds strings
		contains(row.tags, 1),
	);
	select(PersonTable).project((row) => ({
		// @ts-expect-error a string has no elements to index
		first: at(row.name, 0),
	}));
	// @ts-expect-error LIMIT takes a number
	select(PersonTable).limit(param('rows', SurrealSchema.String));
};

export const rejectedCalls = () => {
	select(PersonTable).project((row) => ({
		// @ts-expect-error fn::greet takes a string first
		greeting: call(fn_greet, row.age),
	}));
	// @ts-expect-error fn::greet needs its first argument
	call(fn_greet);
	// @ts-expect-error fn::greet takes at most three arguments
	call(fn_greet, 'a', 'b', 1, 2);
};

// ---- sources and writes

export const rejectedTargets = () => {
	// @ts-expect-error a team id is not a record of `person`
	select(PersonTable, param('id', TeamId));
	// @ts-expect-error a team id is not a record of `person`
	update(PersonTable, param('id', TeamId));
	create(PersonTable).content(
		// @ts-expect-error a merge value lacks the fields CREATE requires
		param('person', PersonMerge),
	);
	update(PersonTable).content(
		// @ts-expect-error a merge value lacks the fields CONTENT requires
		param('changes', PersonMerge),
	);
	create(PersonTable).content(
		// @ts-expect-error a string is not a person
		param('person', SurrealSchema.String),
	);
	// @ts-expect-error CREATE has no RETURN BEFORE
	create(PersonTable).returning('before');
};

export const rejectedRelations = () => {
	// @ts-expect-error `person` is not a relation
	relate(PersonTable, param('from', PersonId), param('to', PersonId));
	relate(
		MemberOfTable,
		// @ts-expect-error a team is not an `in` endpoint of member_of
		param('from', TeamId),
		param('to', TeamId),
	);
	relate(
		MemberOfTable,
		param('from', PersonId),
		// @ts-expect-error a person is not an `out` endpoint of member_of
		param('to', PersonId),
	);
	relate(MemberOfTable, param('from', PersonId), param('to', TeamId)).content(
		// @ts-expect-error a person is not the edge's content
		param('data', PersonCreate),
	);
};

// ---- authored SurrealQL

const bindWrite = part({
	sql: 'LET $producer = $boundProducer;',
	free: ['boundProducer'],
	defines: ['producer'],
});
const requireOwner = part({
	sql: "IF fn::can_access($producer) != true { THROW 'Not the owner' };",
	free: ['producer'],
	throws: ['Not the owner'],
});

export const rejectedSequences = () => {
	sequence(bindWrite, requireOwner).define({
		// @ts-expect-error `$boundProducer` has no codec
		params: {},
		result: SurrealSchema.String,
	});
	sequence(bindWrite, requireOwner).define({
		params: {
			boundProducer: SurrealSchema.String,
			// @ts-expect-error nothing reads `$producer` from the caller: bindWrite defines it
			producer: SurrealSchema.String,
		},
		result: SurrealSchema.String,
	});
	// Without bindWrite the guard reads `$producer` from the caller instead.
	sequence(requireOwner).define({
		// @ts-expect-error the contract of the longer sequence no longer fits
		params: { boundProducer: SurrealSchema.String },
		result: SurrealSchema.String,
	});
};

// ---- client

const byEmail = select(PersonTable)
	.where((row) => eq(row.email, param('email', SurrealSchema.String)))
	.build();

export const rejectedRuns = () => {
	// @ts-expect-error the statement needs its input
	run(byEmail);
	// @ts-expect-error `email` is a string
	run(byEmail, { email: 1 });
	// @ts-expect-error the binding is called `email`
	run(byEmail, { mail: 'a@example.com' });
	// @ts-expect-error a transaction takes bound statements
	transaction([byEmail]);
	// @ts-expect-error the statement needs its input
	transaction([bind(byEmail)]);
};
