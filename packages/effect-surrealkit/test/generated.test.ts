/** Runtime behaviour of the golden generated modules. */
import Ajv2020 from 'ajv/dist/2020.js';
import { DateTime as EffectDateTime, Exit, Schema } from 'effect';
import { DateTime, RecordId } from 'surrealdb';
import { describe, expect, it } from 'vitest';

import * as SurrealSchema from '../src/index.js';
import {
	AnythingFromJson,
	AnythingRow,
	DocTable as CorpusDocTable,
	PersonMerge as CorpusPersonMerge,
	PersonTable as CorpusPersonTable,
	KindsFields,
	KindsJsonFields,
	SinkContent,
	SinkCreate,
	SinkMerge,
	SinkTable,
} from './golden/corpus/index.js';
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
import { Member, MemberTable, MemberVisible } from './golden/members/index.js';

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

const encodes = (schema: Schema.Top, value: unknown) =>
	Exit.isSuccess(
		Schema.encodeUnknownExit(schema as Schema.Codec<unknown, unknown>)(
			value,
			strict,
		),
	);

describe('a closed object with no fields is exactly {}', () => {
	// `sink` is SCHEMAFULL with only its id: SurrealDB refuses any key in its
	// content (`Found field 'stray', but no such field exists`).
	const inputs = [
		['SinkCreate', SinkCreate],
		['SinkMerge', SinkMerge],
		['SinkContent', SinkContent],
		['EmptyObject', SurrealSchema.EmptyObject],
	] as const;

	it.each(inputs)('%s accepts {} in both directions', (_, schema) => {
		expect(decodes(schema, {})).toBe(true);
		expect(encodes(schema, {})).toBe(true);
		expect(decodes(schema, Object.create(null))).toBe(true);
		expect(Schema.encodeSync(schema)({})).toEqual({});
	});

	it.each(
		inputs,
	)('%s refuses a stray key, a string, a number and an array in both directions', (_, schema) => {
		for (const value of [{ stray: 1 }, 'x', 1, [], [1], new Date(0)]) {
			expect(decodes(schema, value), JSON.stringify(value)).toBe(false);
			expect(encodes(schema, value), JSON.stringify(value)).toBe(false);
			// Also without `onExcessProperty: "error"`.
			expect(
				Exit.isSuccess(
					Schema.encodeUnknownExit(
						schema as Schema.Codec<unknown, unknown>,
					)(value),
				),
			).toBe(false);
		}
	});

	it('Schema.Struct({}) is what it replaces: it passes every non-nullish value through', () => {
		const bare = Schema.Struct({});
		for (const value of [{ stray: 1 }, 'x', []]) {
			expect(encodes(bare, value)).toBe(true);
		}
	});

	it('is the descriptor’s operation schema, and refine keeps it', () => {
		expect(SinkTable.create).toBe(SurrealSchema.EmptyObject);
		expect(SinkTable.merge).toBe(SurrealSchema.EmptyObject);
		expect(SinkTable.content).toBe(SurrealSchema.EmptyObject);
		const refined = SurrealSchema.refine(SinkTable, {});
		expect(refined.create).toBe(SurrealSchema.EmptyObject);
		expect(decodes(refined.create, { stray: 1 })).toBe(false);
		// Its fields are none, as before: a `Schema.Struct`.
		expect(SinkTable.create.fields).toEqual({});
	});

	it('has the JSON Schema of an exactly empty object', () => {
		const { schema } = Schema.toJsonSchemaDocument(
			SurrealSchema.EmptyObject,
		);
		expect(schema).toMatchObject({
			allOf: [{ type: 'object', additionalProperties: false }],
		});
		const validate = new Ajv2020({ strict: false }).compile(schema);
		expect(validate({})).toBe(true);
		for (const value of [{ stray: 1 }, 'x', [], 1, null]) {
			expect(validate(value), JSON.stringify(value)).toBe(false);
		}
	});

	it('keeps the type {}: a caller that passes {} compiles', () => {
		const value: typeof SinkCreate.Type = {};
		expect(Schema.decodeUnknownSync(SinkCreate)(value)).toEqual({});
	});
});

describe('generated table modules', () => {
	it('encodes NONE in a flexible MERGE object while keeping stored array items strict', () => {
		const patch = {
			address: { zip: undefined, extra: { deep: undefined } },
		} satisfies typeof CorpusPersonMerge.Type;
		expect(Schema.encodeSync(CorpusPersonMerge)(patch)).toEqual(patch);
		expect(
			decodes(CorpusPersonMerge, { address: { extra: [undefined] } }),
		).toBe(false);
		expect(decodes(SurrealSchema.SurrealValue, { deep: undefined })).toBe(
			false,
		);
	});

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

	it('decode open row fields according to the representation', () => {
		const id = new RecordId('anything', 'ann');
		const datetime = new DateTime('2024-01-01T00:00:00Z');
		expect(decodes(AnythingRow, { id, extra: { datetime } })).toBe(true);
		expect(decodes(AnythingRow, { id, extra: Symbol() })).toBe(false);
		expect(
			decodes(AnythingFromJson, {
				id: 'anything:ann',
				extra: { datetime: datetime.toString() },
			}),
		).toBe(true);
		expect(
			decodes(AnythingFromJson, {
				id: 'anything:ann',
				extra: { datetime },
			}),
		).toBe(false);
	});

	it('distinguish NONE in an any field from values in open collections', () => {
		expect(decodes(KindsFields.k_any, undefined)).toBe(true);
		expect(decodes(KindsFields.k_array, [undefined])).toBe(false);
		expect(decodes(KindsFields.k_set, new Set([undefined]))).toBe(false);
		expect(decodes(KindsJsonFields.k_any, undefined)).toBe(false);
		expect(decodes(KindsJsonFields.k_array, [null])).toBe(true);
		expect(decodes(KindsJsonFields.k_set, [null])).toBe(true);
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

	it('expose each index and each field permission in the descriptor', () => {
		expect(PersonTable.indexes).toEqual({
			uniq_email: { type: 'unique', columns: [['email']] },
		});
		expect(CorpusDocTable.indexes.idx_owner_status).toEqual({
			type: 'standard',
			columns: [['owner'], ['status']],
		});
		expect(CorpusDocTable.indexes.ft_body).toEqual({
			type: 'fulltext',
			columns: [['body']],
			fulltext: { analyzer: 'simple', highlights: true },
		});
		expect(CorpusDocTable.indexes.cnt_open).toEqual({
			type: 'count',
			columns: [],
			where: "status = 'open'",
		});
		expect(PersonTable.fieldPermissions.ssn).toEqual({
			select: '$auth.admin = true',
			create: true,
			update: false,
		});
		expect(Object.keys(PersonTable.fieldPermissions)).toEqual(
			Object.keys(PersonTable.presence).filter((key) => key !== 'id'),
		);
		expect(CorpusPersonTable.fieldPermissions.address.fields?.city).toEqual(
			{ select: true, create: true, update: true },
		);
	});

	it('read a row without the fields a permission can hide through the visible row, refinements included', () => {
		const seen = {
			id: new RecordId('member', 'ann'),
			handle: 'ann',
			region: 'north',
			number: 1,
			profile: { bio: 'b' },
		};
		expect(decodes(Member, seen)).toBe(false);
		expect(decodes(MemberVisible, seen)).toBe(true);
		expect(MemberTable.visible).toBe(MemberVisible);
		expect(PersonTable.visible).toBe(PersonTable.select);
		const Refined = SurrealSchema.refine(MemberTable, {
			email: (s) => s.check(Schema.isMinLength(3)),
		});
		expect(decodes(Refined.visible, seen)).toBe(true);
		expect(decodes(Refined.visible, { ...seen, email: 'a' })).toBe(false);
		expect(decodes(Refined.visible, { ...seen, email: 'a@b' })).toBe(true);
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
