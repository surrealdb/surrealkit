/**
 * Using the generated contracts. Nothing here connects to a database: this
 * file exists to be type-checked (`npm run typecheck`), and every
 * `@ts-expect-error` line is a mistake the generated types catch at compile
 * time. With a connection, encode inputs with the generated schemas before
 * handing them to the SDK, and decode what it returns with them.
 */
import { Schema } from 'effect';
import { NoneAsAbsent, refine } from 'effect-surrealkit';
import { DateTime, RecordId } from 'surrealdb';

import {
	fn_greet,
	MemberOfEdge,
	Person,
	PersonCreate,
	PersonId,
	PersonMerge,
	PersonTable,
	TeamId,
} from './generated/surreal/index.js';

// Record ids are checked by table and branded per store: a team id is not a
// person id, and neither is a bare SDK RecordId until it has been decoded.
export const ann = Schema.decodeUnknownSync(PersonId)(
	new RecordId('person', 'ann'),
);
export const core = Schema.decodeUnknownSync(TeamId)(
	new RecordId('team', 'core'),
);
// @ts-expect-error a team id where a person id is expected
export const wrongTable: PersonId = core;

// CREATE: required fields must be present; engine-supplied ones may be omitted.
export const create: PersonCreate = {
	name: 'Ann',
	email: 'Ann@Example.com',
	bio: null,
	tags: new Set(['admin']),
};
// @ts-expect-error `email` is required on create (its VALUE reads the input)
export const missingEmail: PersonCreate = { name: 'Ann', bio: null };
export const createEncoded = Schema.encodeSync(PersonCreate)(create);

// MERGE: omission leaves a field unchanged, `undefined` (NONE) removes an
// optional one, and readonly or computed fields cannot be sent at all.
export const merge: PersonMerge = { nickname: undefined, role: 'admin' };
export const readonlyInMerge: PersonMerge = {
	// @ts-expect-error `created_at` is READONLY: excluded from merge
	created_at: new DateTime(),
};
export const computedInMerge: PersonMerge = {
	// @ts-expect-error `display` is COMPUTED: never written
	display: 'Ann <ann@example.com>',
};

// SELECT: a stored row as the SDK returns it, NONE read as an absent key.
export const decodeRow = (row: unknown): Person =>
	Schema.decodeUnknownSync(NoneAsAbsent(Person))(row);

// Relations carry their endpoints for typed RELATE arguments.
export const relate = (
	from: (typeof MemberOfEdge.in)[number]['Type'],
	to: (typeof MemberOfEdge.out)[number]['Type'],
) => ({ from, to });
export const membership = relate(ann, core);
// @ts-expect-error the edge goes from person to team, not the other way
export const backwards = relate(core, ann);

// Functions: argument tuples follow the trailing-optional rule.
export const greetArgs: (typeof fn_greet.args)['Type'] = ['Ann'];
// @ts-expect-error `fn::greet` takes a string first
export const badGreet: (typeof fn_greet.args)['Type'] = [42];

// Refinements apply to the base field of every operation at once.
export const Refined = refine(PersonTable, {
	name: (name) => name.check(Schema.isMaxLength(80)),
});
export const refinedCreate = Schema.decodeUnknownSync(Refined.create)({
	name: 'Ann',
	email: 'ann@example.com',
	bio: null,
});
