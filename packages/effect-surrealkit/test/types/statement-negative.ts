/**
 * Negative type fixtures for `effect-surrealkit/statement` and
 * `effect-surrealkit/client`: each `@ts-expect-error` must be an actual
 * error, or `tsc -p test/types` fails.
 */
import { Schema } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';

import { bind, run, transaction } from '../../src/client/index.js';
import {
	and,
	at,
	call,
	cast,
	contains,
	create,
	eq,
	gt,
	inside,
	literal,
	make,
	not,
	param,
	part,
	readAs,
	relate,
	select,
	sequence,
	update,
} from '../../src/statement/index.js';
import * as Corpus from '../golden/corpus/index.js';
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
		// @ts-expect-error [condition/string-field-with-number] a string column never compares with a number
		eq(row.name, 5),
	);
	select(PersonTable).where((row) =>
		// @ts-expect-error [condition/int-field-with-string-binding] an int column never compares with a string binding
		gt(row.age, param('minimum', SurrealSchema.String)),
	);
	select(PersonTable).where((row) =>
		// @ts-expect-error [condition/literal-union-non-member] `role` is a literal union, and this is not a member
		eq(row.role, 'guest'),
	);
	select(PersonTable).where((row) =>
		// @ts-expect-error [condition/person-id-with-team-id] a person id never compares with a team id
		eq(row.manager, param('team', TeamId)),
	);
	// @ts-expect-error [condition/no-such-field] the table has no such field
	select(PersonTable).where((row) => eq(row.nope, 1));
	// @ts-expect-error [condition/no-such-child] `address` has no such child
	select(PersonTable).where((row) => eq(row.address.street, 'x'));
	// @ts-expect-error [condition/not-boolean] a condition is a boolean expression, not a string field
	select(PersonTable).where((row) => row.name);
	// @ts-expect-error [condition/and-non-boolean] `and` takes boolean expressions
	select(PersonTable).where((row) => and(row.name, eq(row.role, 'admin')));
	// @ts-expect-error [condition/not-non-boolean] `not` takes a boolean expression
	select(PersonTable).where((row) => not(row.email));
	select(PersonTable).where((row) =>
		// @ts-expect-error [condition/inside-wrong-elements] the elements of this list are not strings
		inside(row.name, [1, 2]),
	);
	select(PersonTable).where((row) =>
		// @ts-expect-error [condition/contains-wrong-element] `tags` holds strings
		contains(row.tags, 1),
	);
	select(PersonTable).project((row) => ({
		// @ts-expect-error [expr/index-non-array] a string has no elements to index
		first: at(row.name, 0),
	}));
	// @ts-expect-error [select/limit-non-number] LIMIT takes a number
	select(PersonTable).limit(param('rows', SurrealSchema.String));
};

// ORDER BY takes field paths only: the engine parses nothing else there.
export const rejectedOrders = () => {
	// @ts-expect-error [order/cast] a cast is not a field path
	select(PersonTable).orderBy((row) => cast('string', row.age));
	// @ts-expect-error [order/constant] a constant is not a field path
	select(PersonTable).orderBy(() => literal(1));
	// @ts-expect-error [order/call] a function call is not a field path
	select(PersonTable).orderBy((row) => call(fn_greet, row.name));
	// @ts-expect-error [order/comparison] a comparison is not a field path
	select(PersonTable).orderBy((row) => eq(row.age, 1));
	// @ts-expect-error [order/param] a parameter is not a field path
	select(PersonTable).orderBy(() => param('x', SurrealSchema.Int));
	select(PersonTable).orderBy(() =>
		// @ts-expect-error [order/bound-array-element] an element of a bound array is not a field path
		at(param('xs', Schema.Array(SurrealSchema.Int)), 0),
	);
};

export const rejectedProjections = () => {
	select(PersonTable).project(() => ({
		// @ts-expect-error [project/alias-string] a projected field is an expression; inline a constant with `literal`
		label: 'person',
	}));
	select(PersonTable).project((row) => ({
		name: row.name,
		// @ts-expect-error [project/alias-number] a projected field is an expression; inline a constant with `literal`
		one: 1,
	}));
};

export const rejectedCalls = () => {
	select(PersonTable).project((row) => ({
		// @ts-expect-error [call/wrong-argument] fn::greet takes a string first
		greeting: call(fn_greet, row.age),
	}));
	// @ts-expect-error [call/missing-argument] fn::greet needs its first argument
	call(fn_greet);
	// @ts-expect-error [call/too-many-arguments] fn::greet takes at most three arguments
	call(fn_greet, 'a', 'b', 1, 2);
};

// ---- sources and writes

export const rejectedTargets = () => {
	// @ts-expect-error [select/id-of-other-table] a team id is not a record of `person`
	select(PersonTable, param('id', TeamId));
	// @ts-expect-error [update/id-of-other-table] a team id is not a record of `person`
	update(PersonTable, param('id', TeamId));
	create(PersonTable).content(
		// @ts-expect-error [create/merge-value-missing-fields] a merge value lacks the fields CREATE requires
		param('person', PersonMerge),
	);
	update(PersonTable).content(
		// @ts-expect-error [content/merge-value-missing-fields] a merge value lacks the fields CONTENT requires
		param('changes', PersonMerge),
	);
	create(PersonTable).content(
		// @ts-expect-error [create/not-an-object] a string is not a person
		param('person', SurrealSchema.String),
	);
	// Finding 9: a field the table does not have, which the engine refuses.
	update(PersonTable).merge(
		// @ts-expect-error [merge/extra-field] `bogus` is not a field person accepts in MERGE
		param(
			'changes',
			Schema.Struct({
				...PersonMerge.fields,
				bogus: SurrealSchema.String,
			}),
		),
	);
	update(PersonTable, param('id', PersonId)).content(
		// @ts-expect-error [content/extra-field] `bogus` is not a field person accepts in CONTENT
		param(
			'content',
			Schema.Struct({
				...PersonTable.content.fields,
				bogus: SurrealSchema.Int,
			}),
		),
	);
	create(PersonTable).content(
		// @ts-expect-error [create/extra-field] `bogus` is not a field person accepts in CREATE
		param(
			'person',
			Schema.Struct({ ...PersonCreate.fields, bogus: SurrealSchema.Int }),
		),
	);
	create(PersonTable).content(
		// @ts-expect-error [create/missing-field] CREATE requires `name`
		param(
			'person',
			Schema.Struct({
				address: PersonCreate.fields.address,
				bio: PersonCreate.fields.bio,
				email: PersonCreate.fields.email,
				score: PersonCreate.fields.score,
			}),
		),
	);
	update(PersonTable).merge(
		// @ts-expect-error [merge/element-missing-field] a phone needs its `kind`
		param(
			'changes',
			Schema.Struct({ phones: Schema.Array(Schema.Struct({})) }),
		),
	);
	// An undeclared key inside a declared child object or an array element.
	update(Corpus.PersonTable).merge(
		// @ts-expect-error [merge/nested-extra-field] `settings` declares no `bogus`
		param(
			'changes',
			Schema.Struct({
				settings: Schema.Struct({
					theme: Corpus.PersonMerge.fields.settings.schema.fields
						.theme,
					bogus: SurrealSchema.Int,
				}),
			}),
		),
	);
	create(Corpus.PersonTable).content(
		// @ts-expect-error [create/depth-two-extra-field] `settings.notify` declares no `bogus`
		param(
			'person',
			Schema.Struct({
				...Corpus.PersonCreate.fields,
				settings: Schema.Struct({
					notify: Schema.Struct({
						email: Schema.Boolean,
						bogus: SurrealSchema.Int,
					}),
				}),
			}),
		),
	);
	update(PersonTable).merge(
		// @ts-expect-error [merge/element-extra-field] a phone declares no `bogus`
		param(
			'changes',
			Schema.Struct({
				phones: Schema.Array(
					Schema.Struct({
						kind: Schema.Literals(['home', 'mobile', 'work']),
						bogus: SurrealSchema.Int,
					}),
				),
			}),
		),
	);
	// @ts-expect-error [create/return-before] CREATE has no RETURN BEFORE
	create(PersonTable).returning('before');
};

export const rejectedRelations = () => {
	// @ts-expect-error [relate/not-a-relation] `person` is not a relation
	relate(PersonTable, param('from', PersonId), param('to', PersonId));
	relate(
		MemberOfTable,
		// @ts-expect-error [relate/wrong-in-endpoint] a team is not an `in` endpoint of member_of
		param('from', TeamId),
		param('to', TeamId),
	);
	relate(
		MemberOfTable,
		param('from', PersonId),
		// @ts-expect-error [relate/wrong-out-endpoint] a person is not an `out` endpoint of member_of
		param('to', PersonId),
	);
	relate(MemberOfTable, param('from', PersonId), param('to', TeamId)).content(
		// @ts-expect-error [relate/wrong-content] a person is not the edge's content
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
		// @ts-expect-error [sequence/missing-binding] `$boundProducer` has no codec
		params: {},
		result: SurrealSchema.String,
	});
	sequence(bindWrite, requireOwner).define({
		params: {
			boundProducer: SurrealSchema.String,
			// @ts-expect-error [sequence/extra-binding] nothing reads `$producer` from the caller: bindWrite defines it
			producer: SurrealSchema.String,
		},
		result: SurrealSchema.String,
	});
	// Without bindWrite the guard reads `$producer` from the caller instead.
	sequence(requireOwner).define({
		// @ts-expect-error [sequence/contract-of-other-sequence] the contract of the longer sequence no longer fits
		params: { boundProducer: SurrealSchema.String },
		result: SurrealSchema.String,
	});
};

// One write flag: `writes: false` is what says a statement does not write.
export const rejectedFlags = () => {
	make({
		sql: 'RETURN 1;',
		params: {},
		result: SurrealSchema.Int,
		// @ts-expect-error [flag/make-read-only] there is no `readOnly`; declare `writes: false`
		readOnly: true,
	});
	sequence(part({ sql: 'RETURN 1;' })).define({
		params: {},
		result: SurrealSchema.Int,
		// @ts-expect-error [flag/define-read-only] there is no `readOnly`; declare `writes: false`
		readOnly: true,
	});
	// @ts-expect-error [flag/build-read-only] there is no `readOnly`; declare `writes: false`
	select(PersonTable).build('read', { readOnly: true });
};

// ---- client

const byEmail = select(PersonTable)
	.where((row) => eq(row.email, param('email', SurrealSchema.String)))
	.build();
const byEmailAndName = select(PersonTable)
	.where((row) => eq(row.email, param('email', SurrealSchema.String)))
	.where((row) => eq(row.name, param('name', SurrealSchema.String)))
	.build();

export const rejectedRuns = () => {
	// @ts-expect-error [run/missing-input] the statement needs its input
	run(byEmail);
	// @ts-expect-error [run/wrong-type] `email` is a string
	run(byEmail, { email: 1 });
	// @ts-expect-error [run/misspelt-binding] the binding is called `email`
	run(byEmail, { mail: 'a@example.com' });
	// @ts-expect-error [run/missing-value] `name` has no value
	run(byEmailAndName, { email: 'a@example.com' });
	// @ts-expect-error [run/extra-value] the statement reads no `extra`
	run(byEmail, { email: 'a@example.com', extra: 1 });
	// @ts-expect-error [transaction/unbound-statement] a transaction takes bound statements
	transaction([byEmail]);
	// @ts-expect-error [bind/missing-input] the statement needs its input
	transaction([bind(byEmail)]);
	// @ts-expect-error [bind/missing-value] `name` has no value
	bind(byEmailAndName, { email: 'a@example.com' });
	// @ts-expect-error [bind/extra-value] the statement reads no `extra`
	bind(byEmail, { email: 'a@example.com', extra: 1 });
	// @ts-expect-error [bind/wrong-type] `email` is a string
	bind(byEmail, { email: 1 });
	// Finding 9: a bound statement is made by `bind`, which types its input.
	// @ts-expect-error [bound/hand-built] a hand-built object is not a bound statement
	transaction([{ statement: byEmail, input: {} }]);
	// @ts-expect-error [bound/hand-built-wrong-value] a hand-built object is not a bound statement
	transaction([{ statement: byEmail, input: { email: 42 } }]);
};

export const rejectedReadView = () => {
	// @ts-expect-error [read-view/unknown-stored-field] session views narrow the actual schemafull table.
	readAs(PersonTable, Schema.Struct({ emial: Schema.String }));
	// @ts-expect-error [read-view/wrong-record-table] the native encoding of the person id cannot address team.
	readAs(PersonTable, Schema.Struct({ id: TeamId }));
	const NoteId = SurrealSchema.RecordIdFor('note');
	const handWritten = {
		name: 'note',
		id: NoteId,
		select: Schema.Struct({ id: NoteId }),
	} as const;
	// @ts-expect-error [read-view/no-visible-row] a hand-written descriptor has no generated `visible` row to read through
	readAs(handWritten);
};
