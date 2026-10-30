/**
 * Positive type fixtures: generated contracts accept what SurrealDB accepts.
 * Compiled by `tsc -p test/types` (strict, exactOptionalPropertyTypes).
 */
import { DateTime as EffectDateTime, Schema } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';
import { DateTime, RecordId } from 'surrealdb';
import type * as Corpus from '../golden/corpus/index.js';
import {
	type fn_greet,
	type fn_optional_first,
	type MemberOfCreate,
	type MemberOfEdge,
	type MemberOfTable,
	OrgId,
	type OrgOrTeamTagged,
	type Person,
	type PersonContent,
	type PersonCreate,
	PersonId,
	type PersonMerge,
	PersonTable,
	TeamId,
} from '../golden/example/index.js';
import type * as Big from '../golden/example-bigint/index.js';

// Branded ids come from decoding SDK values.
export const personId: PersonId = Schema.decodeUnknownSync(PersonId)(
	new RecordId('person', 'tobie'),
);
export const teamId: TeamId = Schema.decodeUnknownSync(TeamId)(
	new RecordId('team', 'core'),
);
export const orgId: OrgId = Schema.decodeUnknownSync(OrgId)(
	new RecordId('org', 'acme'),
);

// Create: required fields only, then every optional one.
export const create: PersonCreate = {
	address: { city: 'Leeds' },
	bio: null,
	email: 'a@example.com',
	name: 'Ann',
	score: 50,
};
export const createFull: PersonCreate = {
	...create,
	address: { city: 'Leeds', country: 'UK' }, // FLEXIBLE: undeclared keys allowed
	age: 30,
	nickname: 'an',
	created_at: new DateTime(),
	role: 'admin',
	tags: new Set(['a', 'b']),
	phones: [{ kind: 'home' }],
	manager: personId,
};

// Merge: omission keeps, undefined removes (option<T>) or resets (DEFAULT ALWAYS).
export const merge: PersonMerge = {
	nickname: undefined,
	updated_at: undefined,
	name: 'Bea',
};
export const mergeNested: PersonMerge = { address: {} };

// Content: full replacement of the writable fields.
export const content: PersonContent = {
	address: { city: 'York' },
	bio: 'b',
	email: 'b@example.com',
	name: 'Bea',
	phones: [],
	role: 'member',
	score: 1,
	tags: new Set(),
};

// Relations: endpoints and edge content.
export const from: SurrealSchema.EdgeIn<typeof MemberOfEdge> = personId;
export const toTeam: SurrealSchema.EdgeOut<typeof MemberOfEdge> = teamId;
export const toOrg: SurrealSchema.EdgeOut<typeof MemberOfEdge> = orgId;
export const edgeContent: MemberOfCreate = { role: 'lead' };
export const memberOfIn: (typeof MemberOfTable.select.Type)['in'] = personId;

// Functions: trailing optional args may be omitted; a non-trailing one may not.
export const greet1: SurrealSchema.FunctionArgs<typeof fn_greet> = ['Ann'];
export const greet2: SurrealSchema.FunctionArgs<typeof fn_greet> = [
	'Ann',
	'hi',
];
export const greet3: SurrealSchema.FunctionArgs<typeof fn_greet> = [
	'Ann',
	undefined,
	3,
];
export const optionalFirst: SurrealSchema.FunctionArgs<
	typeof fn_optional_first
> = [undefined, 2];
export const greeting: SurrealSchema.FunctionResult<typeof fn_greet> = 'hi Ann';

// record<a | b> as a tagged union.
export const owner: OrgOrTeamTagged = { _tag: 'team', id: teamId };
export const ownerName = (value: OrgOrTeamTagged): string => {
	switch (value._tag) {
		case 'org':
			return `org ${value.id.id}`;
		case 'team':
			return `team ${value.id.id}`;
	}
};

// Refinements apply to every variant; brands narrow the type everywhere.
export const People = SurrealSchema.refine(PersonTable, {
	name: (s) => s.check(Schema.isMinLength(2)),
	email: (s) => s.pipe(Schema.brand('Email')),
});
export type Email = (typeof People.select.Type)['email'];
export const email = Schema.decodeUnknownSync(People.create)(create).email;
export const emailInMerge: Email | undefined = Schema.decodeUnknownSync(
	People.merge,
)({}).email;
export const PeopleScores = SurrealSchema.replaceField(
	PersonTable,
	'score',
	Schema.Literal(1),
);
export const score: (typeof PeopleScores.create.Type)['score'] = 1;

// Rows decode from SDK values.
export const decodePerson = (row: unknown): Person =>
	Schema.decodeUnknownSync(SurrealSchema.NoneAsAbsent(PersonTable.select))(
		row,
	);

// The bigint / effect-datetime profile.
export const bigCreate: Big.PersonCreate = {
	address: { city: 'Leeds' },
	bio: null,
	email: 'a@example.com',
	name: 'Ann',
	score: 50,
	age: SurrealSchema.int64(30n),
	created_at: EffectDateTime.makeUnsafe(0),
};
export const patch: Big.PersonPatch = {
	op: 'replace',
	path: '/name',
	value: 'Bea',
};
export const removal: Big.PersonPatch = { op: 'remove', path: '/nickname' };

// The derived corpus: compound and constrained record keys.
export const objectKey: (typeof Corpus.IdObjectId.Type)['id'] = {
	n: 1,
	region: 'eu',
};
export const tupleKey: (typeof Corpus.IdArrayId.Type)['id'] = [
	'a',
	new DateTime(),
];
export const unconstrained: SurrealSchema.EdgeIn<typeof Corpus.LikesEdge> =
	teamId;
