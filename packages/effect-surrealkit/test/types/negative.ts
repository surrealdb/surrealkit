/**
 * Negative type fixtures: each `@ts-expect-error` must be an actual error,
 * or `tsc -p test/types` fails ("Unused '@ts-expect-error' directive").
 */
import { Schema } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';
import { DateTime, RecordId } from 'surrealdb';

import type * as Corpus from '../golden/corpus/index.js';
import * as CorpusValues from '../golden/corpus/index.js';
import {
	type fn_greet,
	type fn_optional_first,
	type MemberOfCreate,
	type MemberOfEdge,
	type OrgOrTeamTagged,
	type PersonContent,
	type PersonCreate,
	type PersonId,
	type PersonMerge,
	PersonTable,
	type TeamId,
} from '../golden/example/index.js';

declare const personId: PersonId;
declare const teamId: TeamId;
declare const corpusTeamId: Corpus.TeamId;
declare const create: PersonCreate;
declare const content: PersonContent;

// ---- wrong-table ids

// @ts-expect-error a team id is not a person id
export const teamAsPerson: PersonId = teamId;
// @ts-expect-error an unbranded SDK id is not a PersonId; decode it first
export const unbranded: PersonId = new RecordId('person', 'x');
// @ts-expect-error record<org | team> does not accept a person id
export const personAsOwner: OrgOrTeamTagged['id'] = personId;
export const badKey: (typeof Corpus.IdObjectId.Type)['id'] = {
	// @ts-expect-error a key of the wrong shape for id_object
	n: 'one',
	region: 'eu',
};
// @ts-expect-error int-keyed table ids carry numbers, not strings
export const badIntKey: (typeof Corpus.IdIntId.Type)['id'] = 'x';
// Brands are per store: the corpus `team` and the example `team` stay distinct
// only by brand string, which is the same store here, so this one is allowed:
export const sameStore: TeamId = corpusTeamId;

// ---- wrong relation endpoints

// @ts-expect-error member_of starts at a person
export const wrongIn: SurrealSchema.EdgeIn<typeof MemberOfEdge> = teamId;
// @ts-expect-error member_of ends at a team or an org
export const wrongOut: SurrealSchema.EdgeOut<typeof MemberOfEdge> = personId;
// @ts-expect-error endpoints are RELATE arguments, not edge content
export const endpointInContent: MemberOfCreate = { in: personId };

// ---- readonly / computed fields in writes

// @ts-expect-error created_at is READONLY: not part of Merge
export const readonlyMerge: PersonMerge = { created_at: new DateTime() };
export const readonlyContent: PersonContent = {
	...content,
	// @ts-expect-error created_at is READONLY: not part of Content
	created_at: new DateTime(),
};
// @ts-expect-error display is COMPUTED: not part of Merge
export const computedMerge: PersonMerge = { display: 'x' };
// @ts-expect-error display is COMPUTED: not part of Create
export const computedCreate: PersonCreate = { ...create, display: 'x' };
// @ts-expect-error the id is an operation argument, not Create content
export const idInCreate: PersonCreate = { ...create, id: personId };
// @ts-expect-error NONE (undefined) is rejected for a field that is not optional
export const noneForRequired: PersonMerge = { name: undefined };
// @ts-expect-error NONE is rejected for a plain DEFAULT (not re-applied on MERGE)
export const noneForDefault: PersonMerge = { role: undefined };

// ---- missing required Create / Content fields

// @ts-expect-error email and name are required on create
export const missingOnCreate: PersonCreate = {
	address: { city: 'x' },
	bio: null,
	score: 1,
};
// @ts-expect-error a plain DEFAULT is not re-applied by CONTENT, so role is required there
export const missingOnContent: PersonContent = {
	address: { city: 'x' },
	bio: null,
	email: 'e',
	name: 'n',
	phones: [],
	score: 1,
	tags: new Set(),
};
export const extraInClosed: Corpus.PersonCreate['settings'] = {
	theme: 'dark',
	// @ts-expect-error a closed object rejects undeclared keys
	extra: 1,
};

// ---- wrong function argument tuples

// @ts-expect-error fn::greet takes a string first
export const wrongArg: SurrealSchema.FunctionArgs<typeof fn_greet> = [42];
// @ts-expect-error fn::greet takes at most three arguments
export const tooMany: SurrealSchema.FunctionArgs<typeof fn_greet> = [
	'a',
	'b',
	1,
	2,
];
// @ts-expect-error fn::greet requires its first argument
export const tooFew: SurrealSchema.FunctionArgs<typeof fn_greet> = [];
// @ts-expect-error fn::optional_first's optional first argument is not trailing, so it cannot be omitted
export const notOmittable: SurrealSchema.FunctionArgs<
	typeof fn_optional_first
> = [2];

// ---- refinement hooks

// @ts-expect-error unknown keys are errors
SurrealSchema.refine(PersonTable, { nope: (s) => s });
// @ts-expect-error a refinement cannot change the type; use replaceField
SurrealSchema.refine(PersonTable, { name: () => Schema.Number });
// @ts-expect-error replaceField only takes shared fields
SurrealSchema.replaceField(PersonTable, 'address', Schema.String);
// @ts-expect-error a refinement cannot change a field's presence
SurrealSchema.refine(PersonTable, { name: (s) => Schema.optionalKey(s) });
// @ts-expect-error nor can a replacement: presence comes from the table
SurrealSchema.replaceField(PersonTable, 'name', Schema.optional(Schema.String));

// ---- tagged record ids

// @ts-expect-error the tag must match the id's table
export const badTag: OrgOrTeamTagged = { _tag: 'org', id: teamId };

// ---- the native and JSON codec worlds are not interchangeable

declare const ledgerJson: Corpus.LedgerJsonEncoded;
declare const ledgerNative: Corpus.LedgerEncoded;
// @ts-expect-error a JSON row carries text ids and decimals, not SDK values
export const jsonAsNative: Corpus.LedgerEncoded = ledgerJson;
// @ts-expect-error an SDK row is not what HTTP /sql renders
export const nativeAsJson: Corpus.LedgerJsonEncoded = ledgerNative;
export const decodeNativeAsJson = Schema.decodeSync(
	CorpusValues.LedgerFromJson,
);
// @ts-expect-error decoding JSON takes the JSON encoding, not an SDK row
decodeNativeAsJson(ledgerNative);
// @ts-expect-error the JSON field schemas are not native shared fields
export const jsonFieldAsNative: typeof CorpusValues.LedgerFields.amount =
	CorpusValues.LedgerJsonFields.amount;
