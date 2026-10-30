/** Runtime behaviour of the golden generated modules. */
import { DateTime as EffectDateTime, Exit, Schema } from 'effect';
import { DateTime, RecordId } from 'surrealdb';
import { describe, expect, it } from 'vitest';

import * as SurrealSchema from '../src/index.js';
import {
	fn_double,
	fn_greet,
	fn_optional_first,
	MemberOfEdge,
	manifest,
	OrgOrTeamId,
	OrgOrTeamTagged,
	PersonCreate,
	PersonId,
	PersonMerge,
	PersonRow,
	PersonTable,
	TeamId,
} from './golden/example/index.js';
import * as Big from './golden/example-bigint/index.js';

const strict = { onExcessProperty: 'error' } as const;
const decodes = (schema: Schema.Top, value: unknown) =>
	Exit.isSuccess(
		Schema.decodeUnknownExit(schema as Schema.Codec<unknown, unknown>)(
			value,
			strict,
		),
	);

const create = {
	address: { city: 'Leeds' },
	bio: null,
	email: 'a@example.com',
	name: 'Ann',
	score: 50,
};

describe('generated table modules', () => {
	it('decode a stored row the SDK returned, NONE as absent', () => {
		const row = Schema.decodeUnknownSync(PersonRow)({
			id: new RecordId('person', 'ann'),
			...create,
			nickname: undefined,
			created_at: new DateTime(),
			phones: [],
			role: 'member',
			tags: new Set(),
			touched_at: new DateTime(),
			updated_at: new DateTime(),
		});
		expect('nickname' in row).toBe(false);
		expect(row.id.table.name).toBe('person');
	});

	it('reject a record id of another table where a table id is expected', () => {
		expect(decodes(PersonId, new RecordId('team', 'x'))).toBe(false);
		expect(decodes(OrgOrTeamId, new RecordId('team', 'x'))).toBe(true);
		expect(decodes(OrgOrTeamId, new RecordId('person', 'x'))).toBe(false);
	});

	it('check recognised assertions client-side', () => {
		expect(decodes(PersonCreate, create)).toBe(true);
		expect(decodes(PersonCreate, { ...create, name: '' })).toBe(false); // string::len >= 1
		expect(decodes(PersonCreate, { ...create, score: 100 })).toBe(false); // < 100f
		expect(decodes(PersonCreate, { ...create, age: 151 })).toBe(false); // <= 150
		expect(decodes(PersonCreate, { ...create, role: 'guest' })).toBe(false); // INSIDE [...]
		// string::len counts code points, as SurrealDB does
		expect(
			decodes(PersonCreate, { ...create, name: '😀'.repeat(80) }),
		).toBe(true);
	});

	it('exclude readonly and computed fields from writes at runtime too', () => {
		expect(decodes(PersonMerge, { created_at: new DateTime() })).toBe(
			false,
		);
		expect(decodes(PersonCreate, { ...create, display: 'x' })).toBe(false);
	});

	it('expose per-field presence in the descriptor', () => {
		expect(PersonTable.presence.updated_at).toEqual({
			select: 'required',
			create: 'optional',
			merge: 'resettable',
			content: 'optional',
		});
		expect(PersonTable.relations).toEqual({ in: ['member_of'], out: [] });
	});
});

describe('refine and replaceField', () => {
	const People = SurrealSchema.refine(PersonTable, {
		nickname: (s) => s.check(Schema.isMinLength(3)),
	});

	it("apply a refinement to every variant, keeping each variant's presence", () => {
		expect(decodes(People.create, { ...create, nickname: 'ab' })).toBe(
			false,
		);
		expect(decodes(People.merge, { nickname: 'ab' })).toBe(false);
		expect(
			decodes(People.content, {
				...create,
				phones: [],
				role: 'member',
				tags: new Set(),
				nickname: 'ab',
			}),
		).toBe(false);
		expect(
			decodes(People.select, {
				id: new RecordId('person', 'a'),
				nickname: 'ab',
			}),
		).toBe(false);
		// presence is preserved: merge can still remove it, create can still omit it
		expect(decodes(People.merge, { nickname: undefined })).toBe(true);
		expect(decodes(People.create, create)).toBe(true);
		// the original descriptor is untouched
		expect(decodes(PersonTable.merge, { nickname: 'ab' })).toBe(true);
	});

	it('replace a field outright when asked to', () => {
		const Scored = SurrealSchema.replaceField(
			PersonTable,
			'score',
			Schema.Literal(1),
		);
		expect(decodes(Scored.create, { ...create, score: 1 })).toBe(true);
		expect(decodes(Scored.create, { ...create, score: 2 })).toBe(false);
	});

	it('refuse keys that are not shared fields', () => {
		expect(() =>
			SurrealSchema.refine(PersonTable, {
				address: (s: Schema.Top) => s,
			} as never),
		).toThrow(/`address` is not a shared field/);
	});

	it('refuse a schema that brings its own presence', () => {
		expect(() =>
			SurrealSchema.refine(PersonTable, {
				name: (s: Schema.Top) => Schema.optionalKey(s),
			} as never),
		).toThrow(/`name` of table `person` is optional/);
		expect(() =>
			SurrealSchema.replaceField(
				PersonTable,
				'name',
				Schema.optional(Schema.String) as never,
			),
		).toThrow(/is optional/);
	});
});

describe('relations, record unions and functions', () => {
	it('edge endpoints decode only the declared tables', () => {
		const [personId] = MemberOfEdge.in;
		const [orgId, teamId] = MemberOfEdge.out;
		expect(decodes(personId, new RecordId('person', 'a'))).toBe(true);
		expect(decodes(orgId, new RecordId('org', 'a'))).toBe(true);
		expect(decodes(teamId, new RecordId('person', 'a'))).toBe(false);
	});

	it('record<org | team> has a tagged application representation', () => {
		const id = Schema.decodeUnknownSync(TeamId)(
			new RecordId('team', 'core'),
		);
		const tagged = Schema.decodeUnknownSync(OrgOrTeamTagged)(id);
		expect(tagged).toEqual({ _tag: 'team', id });
		expect(Schema.encodeSync(OrgOrTeamTagged)(tagged)).toBe(id);
	});

	it('function argument tuples follow the trailing-optional rule', () => {
		expect(decodes(fn_greet.args, ['Ann'])).toBe(true);
		expect(decodes(fn_greet.args, ['Ann', undefined, 2])).toBe(true);
		expect(decodes(fn_greet.args, [1])).toBe(false);
		expect(decodes(fn_optional_first.args, [2])).toBe(false);
		expect(decodes(fn_optional_first.args, [undefined, 2])).toBe(true);
		expect(fn_double.returns).toBe(Schema.Unknown);
	});

	it('the manifest records what the modules came from', () => {
		expect(manifest).toMatchObject({
			store: 'corpus',
			semanticDigest:
				'sha256:9bb3a0a15eb772d737b5dd170653655b97e4833995b53d272207288b37ee0f37',
			emitter: { name: 'effect-surrealkit' },
			profile: { int: 'safe-number', datetime: 'sdk' },
			peers: { effect: '4.0.6', surrealdb: '2.1.1' },
			source: {
				kind: 'compile',
				engine: '3.3.0',
				adapter: 'structure-3.3',
			},
		});
	});
});

describe('the bigint / effect-datetime profile', () => {
	it('decodes ints as Int64 and datetimes as Effect DateTime.Utc', () => {
		const value = Schema.decodeUnknownSync(Big.PersonCreate)({
			...create,
			age: 42,
			created_at: new DateTime('2024-01-02T03:04:05.123456789Z'),
		});
		expect(value.age).toBe(42n);
		expect(
			value.created_at && EffectDateTime.toEpochMillis(value.created_at),
		).toBe(Date.UTC(2024, 0, 2, 3, 4, 5, 123));
	});

	it('emits JSON Patch unions behind the flag', () => {
		expect(
			decodes(Big.PersonPatch, {
				op: 'replace',
				path: '/name',
				value: 'Bea',
			}),
		).toBe(true);
		expect(
			decodes(Big.PersonPatch, { op: 'remove', path: '/nickname' }),
		).toBe(true);
		expect(decodes(Big.PersonPatch, { op: 'remove', path: '/name' })).toBe(
			false,
		);
		expect(
			decodes(Big.PersonPatch, {
				op: 'replace',
				path: '/created_at',
				value: 1,
			}),
		).toBe(false);
	});
});
