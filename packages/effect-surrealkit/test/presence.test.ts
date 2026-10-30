/**
 * The field-presence matrix (schema document v2, section 7) checked cell by
 * cell against a disposable engine, using the modules generated for the
 * corpus's evidence tables (test/golden/presence). For every cell the
 * generated schema and the engine must agree; the two documented
 * conservative cells (COMPUTED supplied on create, VALUE with a fallback) are
 * pinned explicitly.
 */
import { Exit, Schema } from 'effect';
import { DateTime, RecordId, type Surreal } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import * as SurrealSchema from '../src/index.js';
import { presenceDocument } from './documents.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';
import { captureStructure, presenceDefinitions } from './fixtures/presence.js';
import * as Corpus from './golden/corpus/index.js';
import { E, EContent, EMerge } from './golden/presence/e.js';
import { Ip, IpContent, IpCreate, IpMerge } from './golden/presence/ip.js';
import { M, MContent, MCreate, MMerge } from './golden/presence/m.js';
import {
	Oa,
	OaContent,
	OaCreate,
	OaMerge,
	OaTable,
} from './golden/presence/oa.js';
import {
	Up,
	UpContent,
	UpCreate,
	UpMerge,
	UpTable,
} from './golden/presence/up.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] presence suite skipped: ${engineSkipReason}`,
	);
}

const strict = { onExcessProperty: 'error' } as const;

const accepted = <T, E>(schema: Schema.Codec<T, E>, value: unknown): E => {
	const exit = Schema.encodeUnknownExit(schema)(value, strict);
	if (Exit.isFailure(exit))
		throw new Error(
			`schema rejected ${JSON.stringify(value)}: ${String(exit.cause)}`,
		);
	return exit.value;
};

const rejected = <T, E>(schema: Schema.Codec<T, E>, value: unknown): void => {
	expect(
		Exit.isFailure(Schema.encodeUnknownExit(schema)(value, strict)),
	).toBe(true);
};

describe.skipIf(!engineConfigured)(
	'field-presence matrix against a disposable engine',
	() => {
		let engine: Engine;
		let db: Surreal;
		const one = async <T = Record<string, unknown>>(
			sql: string,
			vars?: Record<string, unknown>,
		): Promise<T> => {
			const results = await within(db.query<unknown[]>(sql, vars));
			const last = results[results.length - 1];
			return (Array.isArray(last) ? last[0] : last) as T;
		};
		const fails = async (
			sql: string,
			vars: Record<string, unknown>,
			pattern: RegExp,
		) => {
			await expect(within(db.query(sql, vars))).rejects.toThrow(pattern);
		};
		const m1 = new RecordId('m', 1);
		const selectRow = async (id: RecordId) =>
			Schema.decodeUnknownSync(SurrealSchema.NoneAsAbsent(M))(
				await one('SELECT * FROM ONLY $id', { id }),
			);

		beforeAll(async () => {
			engine = await startEngine();
			db = engine.db;
			for (const statement of presenceDefinitions())
				await within(db.query(statement));
		});
		afterAll(async () => {
			await engine?.stop();
		});

		it('the engine stores the evidence tables as the generated modules assume', async () => {
			// The golden modules were generated from the document `surrealkit
			// compile` produced for these tables; the engine under test must hold
			// the same clauses for every field.
			type Field = Record<string, unknown> & { path: string[] };
			type Table = { name: string; fields: Field[] };
			const clauses = (field: Record<string, unknown>) => ({
				readonly: field.readonly === true,
				flexible: field.flexible === true,
				computed: field.computed ?? null,
				value: field.value ?? null,
				default: field.default ?? null,
				always: field.default_always === true,
			});
			const expected = (presenceDocument().tables as Table[]).map(
				(t) => ({
					name: t.name,
					fields: t.fields.map((f) => ({
						name: f.path.join('.'),
						...clauses({
							readonly: f.readonly,
							flexible: f.flexible,
							computed: (f.computed as { expr?: string })?.expr,
							value: (f.value as { expr?: string })?.expr,
							default: (f.default as { expr?: string })?.expr,
							default_always:
								(f.default as { mode?: string })?.mode ===
								'always',
						}),
					})),
				}),
			);
			const captured = await captureStructure(db);
			const live = Object.entries(captured.tables)
				.map(([name, info]) => ({
					name,
					fields: (
						(info as { fields: Array<Record<string, unknown>> })
							.fields ?? []
					)
						.filter((f) => f.name !== 'id')
						.map((f) => ({ name: f.name as string, ...clauses(f) }))
						.sort((a, b) => (a.name < b.name ? -1 : 1)),
				}))
				.sort((a, b) => (a.name < b.name ? -1 : 1));
			expect(live).toEqual(expected);
		});

		describe('create', () => {
			it('TYPE T, T | null and VALUE reading $value are required; the rest is supplied by the engine', async () => {
				const content = accepted(MCreate, {
					req: 'x',
					nul: null,
					vin: 'ABC',
					vfall: 'Q',
				});
				await one('CREATE $id CONTENT $content', { id: m1, content });
				const row = await selectRow(m1);
				expect(row).toMatchObject({
					dflt: 'd',
					dalw: 'a',
					optd: 'od',
					ro: 'r',
					comp: 'x!',
					vin: 'abc',
				});
				expect(row.vout.toString()).toBe('2020-01-01T00:00:00.000Z');
				expect('opt' in row).toBe(false);
			});

			it('T | null: NONE is not null', async () => {
				rejected(MCreate, { req: 'x', vin: 'a', vfall: 'q' });
				await fails(
					'CREATE m:2 CONTENT $content',
					{ content: { req: 'x', vin: 'a' } },
					/Expected `string \| null` but found `NONE`/,
				);
			});

			it('VALUE reading $value without a fallback is required', async () => {
				rejected(MCreate, { req: 'x', nul: null, vfall: 'q' });
				await fails(
					'CREATE m:3 CONTENT $content',
					{ content: { req: 'x', nul: null } },
					/string::lowercase/,
				);
			});

			it('VALUE with a fallback: conservatively required (the engine would accept omission)', async () => {
				rejected(MCreate, { req: 'x', nul: null, vin: 'a' });
				const row = await one(
					'CREATE m:31 CONTENT $content RETURN vfall',
					{ content: { req: 'x', nul: null, vin: 'a' } },
				);
				expect(row).toEqual({ vfall: 'x' });
			});

			it('COMPUTED is excluded (the engine silently ignores a supplied value)', async () => {
				rejected(MCreate, {
					req: 'x',
					nul: null,
					vin: 'a',
					vfall: 'q',
					comp: 'zzz',
				});
				const row = await one(
					'CREATE m:5 CONTENT $content RETURN comp',
					{ content: { req: 'x', nul: null, vin: 'a', comp: 'zzz' } },
				);
				expect(row).toEqual({ comp: 'x!' });
			});

			it('VALUE ignoring $value is optional (a supplied value is replaced)', async () => {
				const content = accepted(MCreate, {
					req: 'x',
					nul: null,
					vin: 'a',
					vfall: 'q',
					vout: new DateTime('1999-01-01T00:00:00Z'),
				});
				const row = await one<{ vout: DateTime }>(
					'CREATE m:4 CONTENT $content RETURN vout',
					{ content },
				);
				expect(row.vout.toString()).toBe('2020-01-01T00:00:00.000Z');
			});

			it('READONLY may be supplied on create', async () => {
				const content = accepted(MCreate, {
					req: 'x',
					nul: null,
					vin: 'a',
					vfall: 'q',
					ro: 'given',
					roopt: 'p',
				});
				expect(
					await one('CREATE m:7 CONTENT $content RETURN ro', {
						content,
					}),
				).toEqual({ ro: 'given' });
			});

			it('schemafull rows and closed objects are exact; FLEXIBLE objects are open', async () => {
				rejected(MCreate, {
					req: 'x',
					nul: null,
					vin: 'a',
					vfall: 'q',
					nope: 1,
				});
				await fails(
					'CREATE m:8 CONTENT $content',
					{ content: { req: 'x', nul: null, vin: 'a', nope: 1 } },
					/no such field exists/,
				);
				rejected(MCreate, {
					req: 'x',
					nul: null,
					vin: 'a',
					vfall: 'q',
					obj: { a: 1, extra: 2 },
				});
				await fails(
					'CREATE m:9 CONTENT $content',
					{
						content: {
							req: 'x',
							nul: null,
							vin: 'a',
							obj: { a: 1, extra: 2 },
						},
					},
					/obj\.extra/,
				);
				const content = accepted(MCreate, {
					req: 'x',
					nul: null,
					vin: 'a',
					vfall: 'q',
					flex: { a: 1, extra: { deep: true } },
				});
				expect(
					await one('CREATE m:10 CONTENT $content RETURN flex', {
						content,
					}),
				).toEqual({ flex: { a: 1, extra: { deep: true } } });
			});
		});

		describe('objects without declared children', () => {
			// What 3.2.4 and 3.3.x do, and what the corpus's generated `kinds`
			// fields (SCHEMAFULL: `k_object` plain, `k_object_flexible`) say.
			const closed = Corpus.KindsFields.k_object;
			const open = Corpus.KindsFields.k_object_flexible;
			beforeAll(async () => {
				await within(
					db.query(`
						DEFINE TABLE oc SCHEMAFULL;
						DEFINE FIELD plain ON oc TYPE option<object>;
						DEFINE FIELD flex ON oc TYPE option<object> FLEXIBLE;
						DEFINE FIELD items ON oc TYPE option<array<object>>;
						DEFINE FIELD parent ON oc TYPE option<object> FLEXIBLE;
						DEFINE FIELD parent.inner ON oc TYPE option<object>;
						DEFINE TABLE ol SCHEMALESS;
						DEFINE FIELD plain ON ol TYPE object;
					`),
				);
			});
			const stored = (sql: string, content: Record<string, unknown>) =>
				one(`${sql} RETURN AFTER`, { content });

			it('SCHEMAFULL refuses every key of an object that is not FLEXIBLE: only {} is stored', async () => {
				expect(
					await stored('CREATE oc:empty CONTENT $content', {
						plain: {},
					}),
				).toMatchObject({
					plain: {},
				});
				accepted(closed, {});
				for (const [content, key] of [
					[{ plain: { x: 1 } }, 'plain\\.x'],
					[{ items: [{ x: 1 }] }, 'items\\[0\\]\\.x'],
					// A FLEXIBLE parent does not open a declared child.
					[{ parent: { inner: { x: 1 } } }, 'parent\\.inner\\.x'],
				] as const) {
					await fails(
						'CREATE oc CONTENT $content',
						{ content },
						new RegExp(key),
					);
				}
				await fails(
					'UPDATE oc:empty MERGE $content',
					{ content: { plain: { m: 1 } } },
					/plain\.m/,
				);
				rejected(closed, { x: 1 });
			});

			it('FLEXIBLE keeps undeclared keys, nested ones included', async () => {
				expect(
					await stored('CREATE oc:flex CONTENT $content', {
						flex: { x: 1, y: { z: 2 } },
					}),
				).toMatchObject({ flex: { x: 1, y: { z: 2 } } });
				accepted(open, { x: 1, y: { z: 2 } });
			});

			it('SCHEMALESS keeps the keys of a plain object field', async () => {
				expect(
					await stored('CREATE ol:a CONTENT $content', {
						plain: { x: 1 },
					}),
				).toMatchObject({ plain: { x: 1 } });
			});
		});

		describe('merge', () => {
			it('TYPE T and a plain DEFAULT reject NONE', async () => {
				rejected(MMerge, { req: undefined });
				await fails(
					'UPDATE $id MERGE { req: NONE }',
					{ id: m1 },
					/Expected `string` but found `NONE`/,
				);
				rejected(MMerge, { dflt: undefined });
				await fails(
					'UPDATE $id MERGE { dflt: NONE }',
					{ id: m1 },
					/Expected `string` but found `NONE`/,
				);
			});

			it('DEFAULT ALWAYS is resettable: undefined re-applies the default', async () => {
				await one('UPDATE $id MERGE { dalw: "changed" }', { id: m1 });
				const patch = accepted(MMerge, { dalw: undefined });
				expect(
					await one('UPDATE $id MERGE $patch RETURN dalw', {
						id: m1,
						patch,
					}),
				).toEqual({ dalw: 'a' });
			});

			it('option<T> is removable: undefined removes it', async () => {
				expect(
					await one('UPDATE $id MERGE $patch RETURN opt', {
						id: m1,
						patch: accepted(MMerge, { opt: 'o' }),
					}),
				).toEqual({ opt: 'o' });
				const patch = accepted(MMerge, { opt: undefined });
				await one('UPDATE $id MERGE $patch', { id: m1, patch });
				expect('opt' in (await selectRow(m1))).toBe(false);
			});

			it('READONLY is excluded: a changed value or NONE is rejected', async () => {
				rejected(MMerge, { ro: 'other' });
				await fails(
					'UPDATE $id MERGE { ro: "other" }',
					{ id: m1 },
					/readonly/,
				);
				rejected(MMerge, { roopt: undefined });
				await fails('UPDATE m:7 MERGE { roopt: NONE }', {}, /readonly/);
			});

			it('omitting a VALUE field keeps the stored value', async () => {
				const patch = accepted(MMerge, { opt: 'z' });
				expect(
					await one('UPDATE $id MERGE $patch RETURN vin', {
						id: m1,
						patch,
					}),
				).toEqual({ vin: 'abc' });
			});
		});

		describe('content', () => {
			it('a plain DEFAULT is required (not re-applied)', async () => {
				rejected(MContent, {
					req: 'y',
					nul: null,
					vin: 'Q',
					vfall: 'q',
				});
				await fails(
					'UPDATE $id CONTENT $content',
					{ id: m1, content: { req: 'y', nul: null, vin: 'Q' } },
					/`dflt`/,
				);
			});

			it('DEFAULT ALWAYS, READONLY, option<T> and producer VALUE may be omitted', async () => {
				const content = accepted(MContent, {
					req: 'y',
					nul: null,
					vin: 'Q',
					vfall: 'q',
					dflt: 'd',
				});
				const row = await one<Record<string, unknown>>(
					'UPDATE $id CONTENT $content RETURN dalw, ro, optd, vout',
					{ id: m1, content },
				);
				expect(row.dalw).toBe('a');
				expect(row.ro).toBe('r');
				expect(row.optd).toBeUndefined();
				expect(String(row.vout)).toBe('2020-01-01T00:00:00.000Z');
			});

			it('VALUE reading $value is required', async () => {
				rejected(MContent, {
					req: 'y',
					nul: null,
					vfall: 'q',
					dflt: 'd',
				});
				await fails(
					'UPDATE $id CONTENT $content',
					{ id: m1, content: { req: 'y', nul: null, dflt: 'd' } },
					/string::lowercase/,
				);
			});

			it('READONLY and id are excluded', async () => {
				rejected(MContent, {
					req: 'y',
					nul: null,
					vin: 'Q',
					vfall: 'q',
					dflt: 'd',
					ro: 'changed',
				});
				await fails(
					'UPDATE $id CONTENT $content',
					{
						id: m1,
						content: {
							req: 'y',
							nul: null,
							vin: 'Q',
							dflt: 'd',
							ro: 'changed',
						},
					},
					/readonly/,
				);
				rejected(MContent, {
					req: 'y',
					nul: null,
					vin: 'Q',
					vfall: 'q',
					dflt: 'd',
					id: new RecordId('m', 'other'),
				});
				await fails(
					'UPDATE $id CONTENT $content',
					{
						id: m1,
						content: {
							id: new RecordId('m', 'other'),
							req: 'y',
							nul: null,
							vin: 'Q',
							dflt: 'd',
						},
					},
					/specific record has been specified/,
				);
			});

			it('the stored row still decodes as a select row', async () => {
				expect((await selectRow(m1)).req).toBe('y');
			});
		});

		describe('relations', () => {
			it('in/out are kept on CONTENT omission and excluded from MERGE', async () => {
				await one('CREATE p:a, p:b');
				await one('RELATE p:a->e:1->p:b CONTENT { w: 1 }');
				const content = accepted(EContent, { w: 2 });
				const updated = await one('UPDATE e:1 CONTENT $content', {
					content,
				});
				const row = Schema.decodeUnknownSync(
					SurrealSchema.NoneAsAbsent(E),
				)(updated);
				expect(row.in.id).toBe('a');
				expect(row.out.id).toBe('b');
				expect(row.w).toBe(2);
				rejected(EMerge, { in: new RecordId('p', 'b') });
				expect(
					await one('UPDATE e:1 MERGE { in: p:b } RETURN in'),
				).toEqual({ in: new RecordId('p', 'a') });
			});
		});

		describe('implicit parents (contract section 6)', () => {
			const ip2 = new RecordId('ip', 2);
			const selectIp = async (id: RecordId) =>
				Schema.decodeUnknownSync(SurrealSchema.NoneAsAbsent(Ip))(
					await one('SELECT * FROM ONLY $id', { id }),
				);

			it('create: required when a child is required, otherwise optional; always selected', async () => {
				rejected(IpCreate, {});
				await fails(
					'CREATE ip:1 CONTENT $content',
					{ content: {} },
					/field `need.x` .* Expected `string` but found `NONE`/,
				);
				const content = accepted(IpCreate, { need: { x: 'v' } });
				await one('CREATE $id CONTENT $content', { id: ip2, content });
				expect(await selectIp(ip2)).toMatchObject({
					need: { x: 'v' },
					pref: {},
				});
			});

			it('merge: NONE resets an all-optional parent to {} and is rejected when a child is required', async () => {
				await one('UPDATE $id MERGE $patch', {
					id: ip2,
					patch: accepted(IpMerge, { pref: { locale: 'en' } }),
				});
				expect((await selectIp(ip2)).pref).toEqual({ locale: 'en' });
				await one('UPDATE $id MERGE $patch', {
					id: ip2,
					patch: accepted(IpMerge, { pref: undefined }),
				});
				expect((await selectIp(ip2)).pref).toEqual({});
				rejected(IpMerge, { need: undefined });
				await fails(
					'UPDATE $id MERGE $patch',
					{ id: ip2, patch: { need: undefined } },
					/field `need.x` .* Expected `string` but found `NONE`/,
				);
			});

			it('content: an omitted all-optional parent comes back as {}', async () => {
				const content = accepted(IpContent, { need: { x: 'w' } });
				await one('UPDATE $id CONTENT $content', { id: ip2, content });
				expect(await selectIp(ip2)).toMatchObject({
					need: { x: 'w' },
					pref: {},
				});
				rejected(IpContent, { pref: {} });
			});
		});

		describe('TYPE option<T> DEFAULT ALWAYS', () => {
			const oa1 = new RecordId('oa', 1);
			const selectOa = async () =>
				Schema.decodeUnknownSync(SurrealSchema.NoneAsAbsent(Oa))(
					await one('SELECT * FROM ONLY $id', { id: oa1 }),
				);

			it('is resettable in merge, not removable: NONE re-applies the default', async () => {
				expect(OaTable.presence.x).toEqual({
					select: 'optional',
					create: 'optional',
					merge: 'resettable',
					content: 'optional',
				});
				await one('CREATE $id CONTENT $content', {
					id: oa1,
					content: accepted(OaCreate, {}),
				});
				expect((await selectOa()).x).toBe('z');
				await one('UPDATE $id MERGE $patch', {
					id: oa1,
					patch: accepted(OaMerge, { x: 'y' }),
				});
				expect((await selectOa()).x).toBe('y');
				await one('UPDATE $id MERGE $patch', {
					id: oa1,
					patch: accepted(OaMerge, { x: undefined }),
				});
				expect((await selectOa()).x).toBe('z');
			});

			it('content: omission re-applies the default', async () => {
				await one('UPDATE $id MERGE { x: "y" }', { id: oa1 });
				await one('UPDATE $id CONTENT $content', {
					id: oa1,
					content: accepted(OaContent, {}),
				});
				expect((await selectOa()).x).toBe('z');
			});
		});

		describe('a parent without TYPE whose child is required', () => {
			const up4 = new RecordId('up', 4);
			const selectUp = async () =>
				Schema.decodeUnknownSync(SurrealSchema.NoneAsAbsent(Up))(
					await one('SELECT * FROM ONLY $id', { id: up4 }),
				);

			it('is a required object closed over its children', async () => {
				expect(UpTable.presence.obj).toEqual({
					select: 'required',
					create: 'required',
					merge: 'optional',
					content: 'required',
				});
				rejected(UpCreate, {});
				await fails(
					'CREATE up:1 CONTENT $content',
					{ content: {} },
					/field `obj.a` .* Expected `int` but found `NONE`/,
				);
				rejected(UpCreate, { obj: { a: 1, b: 2 } });
				await fails(
					'CREATE up:3 CONTENT $content',
					{ content: { obj: { a: 1, b: 2 } } },
					/Found field 'obj.b', but no such field exists/,
				);
				rejected(UpCreate, { obj: 5 });
				await one('CREATE $id CONTENT $content', {
					id: up4,
					content: accepted(UpCreate, { obj: { a: 1 } }),
				});
				expect((await selectUp()).obj).toEqual({ a: 1 });
			});

			it('merge: merged recursively, NONE rejected', async () => {
				await one('UPDATE $id MERGE $patch', {
					id: up4,
					patch: accepted(UpMerge, { obj: { a: 2 } }),
				});
				expect((await selectUp()).obj).toEqual({ a: 2 });
				rejected(UpMerge, { obj: undefined });
				await fails(
					'UPDATE $id MERGE $patch',
					{ id: up4, patch: { obj: undefined } },
					/field `obj.a` .* Expected `int` but found `NONE`/,
				);
			});

			it('content: required', async () => {
				rejected(UpContent, {});
				await fails(
					'UPDATE $id CONTENT $content',
					{ id: up4, content: {} },
					/field `obj.a` .* Expected `int` but found `NONE`/,
				);
				await one('UPDATE $id CONTENT $content', {
					id: up4,
					content: accepted(UpContent, { obj: { a: 3 } }),
				});
				expect((await selectUp()).obj).toEqual({ a: 3 });
			});
		});
	},
);
