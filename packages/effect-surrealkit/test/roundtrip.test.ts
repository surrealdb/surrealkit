/**
 * Round-trip properties of the generated corpus modules against a disposable
 * engine prepared by `surrealkit test --exec` (see scripts/roundtrip.mjs):
 * create → select → merge → select, relate, and call, with values generated
 * by `Arbitrary.schema` from the generated schemas and checked with
 * `Arbitrary.checkEffect`, which must report `Passed`.
 *
 * Generation starts from each generated Create/Merge schema. Where the
 * engine has semantics a client schema cannot carry (`string::is_email`,
 * an unrecognised ASSERT, a VALUE clause, duration ranges, open FLEXIBLE
 * content typed `unknown`), the generator is narrowed for that field, the
 * way an application would `refine` it; encoding always goes through the
 * generated schema itself, so a generated value it rejects fails the test.
 *
 * Skipped unless SURREALKIT_EXEC_ENDPOINT is set, i.e. outside
 * `surrealkit test --exec`.
 */
import { inspect } from 'node:util';

import { Arbitrary, Effect, Schema } from 'effect';
import { Duration, equals, escapeIdent, RecordId, Surreal } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import * as S from '../src/index.js';
import { compiledPath, readJson } from './documents.js';
import * as Corpus from './golden/corpus/index.js';
import * as Big from './golden/ints-bigint/index.js';

const env = process.env;
const prepared = env.SURREALKIT_EXEC_ENDPOINT !== undefined;
const RUNS = Number(env.SURREALKIT_ROUNDTRIP_RUNS ?? 50);
const SEED = 20261025;

if (!prepared) {
	console.warn(
		'[effect-surrealkit] round-trip suite skipped: run it through `surrealkit test --exec` (bun run test:roundtrip)',
	);
}

type Row = Record<string, unknown>;

/** A plain object (not an SDK value class, array or Set); generated records have a null prototype. */
const isPlain = (value: unknown): value is Row => {
	if (typeof value !== 'object' || value === null) return false;
	const proto = Object.getPrototypeOf(value);
	return proto === Object.prototype || proto === null;
};

/**
 * What `UPDATE … MERGE` stores: nested objects merge, `undefined` (NONE)
 * removes, anything else replaces.
 */
const merged = (base: unknown, patch: unknown): unknown => {
	if (!isPlain(patch) || !isPlain(base)) return patch;
	const out: Row = { ...base };
	for (const [key, value] of Object.entries(patch)) {
		if (value === undefined) delete out[key];
		else out[key] = merged(base[key], value);
	}
	return out;
};

const fail = (message: string): never => {
	throw new Error(message);
};

/** One table's round trip: how to generate, which fields store what was sent. */
interface TableCase {
	readonly table: string;
	/** Generator schemas: subsets of the generated Create and Merge Types. */
	readonly createGen: Schema.Top;
	readonly mergeGen: Schema.Top;
	/** The generated schemas the values are encoded and decoded through. */
	readonly create: Schema.Top;
	readonly merge: Schema.Top;
	readonly select: Schema.Top;
	/** The generated JSON-world row: the same row read over HTTP `/sql` must decode to the same value. */
	readonly fromJson: Schema.Top;
	/** Fields stored exactly as sent (no VALUE rewriting, not computed). */
	readonly stored: ReadonlyArray<string>;
	/** Fields whose NONE re-applies a default (merge: resettable). */
	readonly resettable?: ReadonlyArray<string>;
	/** A record id per evaluation. */
	readonly id: (n: number) => RecordId;
	/** Turns a generated value into the Create/Merge Type (for narrowed generators). */
	readonly prepare?: (value: Row) => Row;
}

describe.skipIf(!prepared)(
	'generated corpus modules against a prepared engine',
	() => {
		let db: Surreal;
		let counter = 0;

		const query = async <T = unknown>(
			sql: string,
			vars?: Row,
		): Promise<T> => {
			const results = await db.query<unknown[]>(sql, vars);
			return results[results.length - 1] as T;
		};

		beforeAll(async () => {
			const endpoint =
				env.SURREALKIT_EXEC_ENDPOINT ?? fail('no endpoint');
			const url = new URL(endpoint);
			if (url.pathname === '/' || url.pathname === '')
				url.pathname = '/rpc';
			db = new Surreal();
			await db.connect(url.toString());
			// Owner of the suite database only, never a server-wide token.
			await db.authenticate(
				env.SURREALKIT_EXEC_TOKEN ?? fail('no database token'),
			);
			await db.use({
				namespace:
					env.SURREALKIT_EXEC_NAMESPACE ?? fail('no namespace'),
				database: env.SURREALKIT_EXEC_DATABASE ?? fail('no database'),
			});
		});
		afterAll(async () => {
			await db?.close();
		});

		it('runs against the schema the modules were generated from', () => {
			// SurrealKit introspected the prepared database; compile of the same
			// files gave the document at SURREALKIT_ROUNDTRIP_DOCUMENT.
			const portable = readJson(
				env.SURREALKIT_ROUNDTRIP_DOCUMENT ??
					fail('no compiled document'),
			) as { semanticDigest: string; tables: Array<{ name: string }> };
			expect(env.SURREALKIT_EXEC_SCHEMA_DIGEST).toBe(
				portable.semanticDigest,
			);
			// The golden modules come from the whole corpus; every table the
			// database has is identical there.
			const whole = readJson(compiledPath('corpus')) as typeof portable;
			for (const table of portable.tables) {
				expect(whole.tables.find((t) => t.name === table.name)).toEqual(
					table,
				);
			}
		});

		const property = async <A>(
			arbitrary: Arbitrary.Arbitrary<A>,
			check: (value: A) => Promise<boolean>,
		) => {
			const result = await Effect.runPromise(
				Arbitrary.checkEffect(
					arbitrary,
					(value) => Effect.promise(() => check(value)),
					{ runs: RUNS, seed: SEED },
				),
			);
			expect(Arbitrary.formatCheckFailure(result) ?? 'Passed').toBe(
				'Passed',
			);
			expect(result._tag).toBe('Passed');
		};

		const equalField = (
			select: Schema.Top,
			field: string,
			a: unknown,
			b: unknown,
		): boolean => {
			const fields = (
				select as unknown as { fields: Record<string, Schema.Top> }
			).fields;
			const schema = fields[field] ?? fail(`no field ${field}`);
			return Schema.toEquivalence(schema as Schema.Codec<unknown>)(a, b);
		};

		/** Decodes a selected row; a row the generated select schema rejects fails the property. */
		const selectRow = async (
			select: Schema.Top,
			id: RecordId,
		): Promise<Row> => {
			const raw = await query('SELECT * FROM ONLY $id', { id });
			return Schema.decodeUnknownSync(
				S.NoneAsAbsent(select as Schema.Codec<Row>),
			)(raw, { onExcessProperty: 'error' });
		};

		/** The same row as SurrealDB's HTTP `/sql` renders it, decoded through the JSON world. */
		const selectJson = async (
			fromJson: Schema.Top,
			id: RecordId,
			onExcessProperty: 'error' | 'ignore' = 'error',
		): Promise<Row> => {
			const endpoint = new URL(
				env.SURREALKIT_EXEC_ENDPOINT ?? fail('no endpoint'),
			);
			endpoint.protocol =
				endpoint.protocol === 'wss:' ? 'https:' : 'http:';
			endpoint.pathname = '/sql';
			const response = await fetch(endpoint, {
				method: 'POST',
				headers: {
					Accept: 'application/json',
					Authorization: `Bearer ${env.SURREALKIT_EXEC_TOKEN ?? fail('no database token')}`,
					'surreal-ns':
						env.SURREALKIT_EXEC_NAMESPACE ?? fail('no namespace'),
					'surreal-db':
						env.SURREALKIT_EXEC_DATABASE ?? fail('no database'),
				},
				body: `SELECT * FROM ${escapeIdent(id.table.name)}`,
			});
			const [statement] = (await response.json()) as Array<{
				readonly status: string;
				readonly result: unknown;
			}>;
			if (statement?.status !== 'OK' || !Array.isArray(statement.result))
				throw new Error(`HTTP /sql: ${inspect(statement)}`);
			const decodeId = Schema.decodeUnknownSync(S.RecordIdAnyFromJson);
			const rows = statement.result.filter((json) =>
				equals(decodeId((json as { readonly id: unknown }).id), id),
			);
			if (rows.length !== 1)
				throw new Error(
					`HTTP /sql: ${rows.length} rows of ${inspect(statement.result)} have the id ${inspect(id)}`,
				);
			return Schema.decodeUnknownSync(fromJson as Schema.Codec<Row>)(
				rows[0],
				{ onExcessProperty },
			);
		};

		const sameStored = (
			c: TableCase,
			row: Row,
			expected: Row,
			label: string,
		): boolean => {
			for (const field of c.stored) {
				if (!(field in expected)) continue;
				if (!equalField(c.select, field, row[field], expected[field])) {
					throw new Error(
						`${c.table}.${field} after ${label}: sent ${inspect(expected[field])}, stored ${inspect(row[field])}`,
					);
				}
			}
			return true;
		};

		const tableRoundTrip = (c: TableCase) =>
			property(
				Arbitrary.all([
					Arbitrary.schema(c.createGen),
					Arbitrary.schema(c.mergeGen),
				]),
				async ([createGenerated, mergeGenerated]) => {
					const prepare = c.prepare ?? ((value: Row) => value);
					const createValue = prepare(createGenerated as Row);
					const mergeValue = prepare(mergeGenerated as Row);
					const id = c.id(++counter);
					const encodeCreate = Schema.encodeUnknownSync(
						c.create as Schema.Codec<unknown, unknown>,
					);
					const encodeMerge = Schema.encodeUnknownSync(
						c.merge as Schema.Codec<unknown, unknown>,
					);
					try {
						await query('CREATE $id CONTENT $content', {
							id,
							content: encodeCreate(createValue),
						});
						const first = await selectRow(c.select, id);
						expect(first.id).toBeInstanceOf(RecordId);
						sameStored(c, first, createValue as Row, 'create');
						const overJson = await selectJson(c.fromJson, id);
						if (
							!Schema.toEquivalence(
								c.select as Schema.Codec<Row>,
							)(first, overJson)
						) {
							throw new Error(
								`${c.table}: the JSON row differs from the SDK row\n${inspect(overJson)}\n${inspect(first)}`,
							);
						}

						await query('UPDATE $id MERGE $patch', {
							id,
							patch: encodeMerge(mergeValue),
						});
						const second = await selectRow(c.select, id);
						const patch = mergeValue as Row;
						const expected: Row = {};
						for (const field of c.stored) {
							if (!(field in patch)) {
								if (field in first)
									expected[field] = first[field];
								continue;
							}
							if (patch[field] === undefined) {
								// NONE: removed, or a resettable default re-applied.
								if (c.resettable?.includes(field)) continue;
								if (field in second) {
									throw new Error(
										`${c.table}.${field}: NONE did not remove it`,
									);
								}
								continue;
							}
							expected[field] = merged(
								first[field],
								patch[field],
							);
						}
						return sameStored(c, second, expected, 'merge');
					} finally {
						await query('DELETE $id', { id });
					}
				},
			);

		// ---- generators narrowed where the engine has semantics the schema lacks

		const lower = Schema.String.check(
			Schema.isPattern(/^[a-z][a-z0-9]{0,11}$/),
		);
		const email = Schema.String.check(
			Schema.isPattern(/^[a-z][a-z0-9]{0,11}@example\.com$/),
		);
		/** `pattern`: ASSERT $value = NONE OR $value = /^[a-z]+$/ (unrecognised, so server-side). */
		const letters = Schema.String.check(Schema.isPattern(/^[a-z]{1,12}$/));
		const upper = Schema.String.check(Schema.isPattern(/^[A-Z]{1,8}$/));
		/** JSON-safe values for content typed `unknown` (FLEXIBLE extras, untyped fields). */
		const plainValue = Schema.Union([
			S.String,
			Schema.Boolean,
			S.Int,
			Schema.Array(S.String),
		]);
		const extras = Schema.Record(
			Schema.String.check(Schema.isPattern(/^x_[a-z]{1,6}$/)),
			plainValue,
		);
		/** `session_ttl`: ASSERT $value >= 1m AND $value <= 1d (a server-side check), as seconds. */
		const ttlSeconds = Schema.Number.check(
			Schema.isInt(),
			Schema.isBetween({ minimum: 60, maximum: 86_400 }),
		);
		const withTtl = (value: Row): Row =>
			typeof value.session_ttl === 'number'
				? {
						...value,
						session_ttl: new Duration(`${value.session_ttl}s`),
					}
				: value;

		const P = Corpus.PersonFields;
		const personAddress = (mode: 'create' | 'merge') =>
			Schema.StructWithRest(
				Schema.Struct({
					city:
						mode === 'create' ? P.name : Schema.optionalKey(P.name),
					geo:
						mode === 'create'
							? Schema.optionalKey(S.GeometryPoint)
							: Schema.optional(S.GeometryPoint),
					zip:
						mode === 'create'
							? Schema.optionalKey(S.String)
							: Schema.optional(S.String),
				}),
				[extras],
			);
		const personOverrides = (mode: 'create' | 'merge') => {
			const optional =
				mode === 'create' ? Schema.optionalKey : Schema.optional;
			return {
				address:
					mode === 'create'
						? personAddress(mode)
						: Schema.optionalKey(personAddress(mode)),
				email: mode === 'create' ? email : Schema.optionalKey(email),
				handle: optional(lower),
				shout: Schema.optionalKey(upper),
				pattern: optional(letters),
				notes: optional(plainValue),
				session_ttl: Schema.optionalKey(ttlSeconds),
			};
		};

		const PersonCreateGen = Schema.Struct({
			...Corpus.PersonCreate.fields,
			...personOverrides('create'),
		});
		const PersonMergeGen = Schema.Struct({
			...Corpus.PersonMerge.fields,
			...personOverrides('merge'),
		});

		const personStored = [
			'address',
			'age',
			'bio',
			'buddy',
			'created_at',
			'email',
			'flagged',
			'handle',
			'manager',
			'mentor',
			'motto',
			'name',
			'nickname',
			'notes',
			'odd.name',
			'org',
			'pattern',
			'phones',
			'pins',
			'prefs',
			'role',
			'score',
			'session_ttl',
			'settings',
			'shout',
			'ssn',
			'tags',
			'team',
			'tier',
			'updated_at',
			'watchers',
		];

		it('person: create → select → merge → select (NONE/NULL/absent, FLEXIBLE, sets, ns datetimes)', async () => {
			await tableRoundTrip({
				table: 'person',
				createGen: PersonCreateGen,
				mergeGen: PersonMergeGen,
				create: Corpus.PersonCreate,
				merge: Corpus.PersonMerge,
				select: Corpus.Person,
				fromJson: Corpus.PersonFromJson,
				stored: personStored,
				// `prefs` is an implicit parent: NONE resets it to {} (contract section 6).
				resettable: ['handle', 'prefs', 'updated_at'],
				id: (n) => new RecordId('person', `rt${n}`),
				prepare: withTtl,
			});
		});

		it('team: open FLEXIBLE object, removable by NONE', async () => {
			const meta = extras;
			await tableRoundTrip({
				table: 'team',
				createGen: Schema.Struct({
					...Corpus.TeamCreate.fields,
					meta: Schema.optionalKey(meta),
				}),
				mergeGen: Schema.Struct({
					...Corpus.TeamMerge.fields,
					meta: Schema.optional(meta),
				}),
				create: Corpus.TeamCreate,
				merge: Corpus.TeamMerge,
				select: Corpus.Team,
				fromJson: Corpus.TeamFromJson,
				stored: ['meta', 'name'],
				id: (n) => new RecordId('team', `rt${n}`),
			});
		});

		it('ledger: decimals stay exact', async () => {
			await tableRoundTrip({
				table: 'ledger',
				createGen: Corpus.LedgerCreate,
				mergeGen: Corpus.LedgerMerge,
				create: Corpus.LedgerCreate,
				merge: Corpus.LedgerMerge,
				select: Corpus.Ledger,
				fromJson: Corpus.LedgerFromJson,
				stored: ['amount'],
				id: (n) => new RecordId('ledger', `rt${n}`),
			});
		});

		it('audit_log: optional record links and strings', async () => {
			await tableRoundTrip({
				table: 'audit_log',
				createGen: Corpus.AuditLogCreate,
				mergeGen: Corpus.AuditLogMerge,
				create: Corpus.AuditLogCreate,
				merge: Corpus.AuditLogMerge,
				select: Corpus.AuditLog,
				fromJson: Corpus.AuditLogFromJson,
				stored: ['doc', 'note'],
				id: (n) => new RecordId('audit_log', `rt${n}`),
			});
		});

		// ---- record ids: every key kind, compound ones and int64 extremes

		const idRoundTrip = async (id: Schema.Top, select: Schema.Top) => {
			const decodeId = Schema.encodeUnknownSync(
				id as Schema.Codec<unknown, unknown>,
			);
			await property(Arbitrary.schema(id), async (value) => {
				const recordId = decodeId(value) as RecordId;
				try {
					await query('CREATE $id', { id: recordId });
					const row = await selectRow(select, recordId);
					const equivalent = Schema.toEquivalence(
						id as Schema.Codec<unknown>,
					);
					// The same id as SurrealQL text over HTTP /sql.
					const overJson = await selectJson(
						Schema.Struct({
							id: S.RecordIdFromJsonAs(
								id as Schema.Codec<unknown, RecordId>,
							),
						}),
						recordId,
						'ignore',
					);
					return (
						equivalent(row.id, value) &&
						equivalent(overJson.id, value)
					);
				} finally {
					await query('DELETE $id', { id: recordId });
				}
			});
		};

		for (const [name, id, select] of [
			['id_int', Corpus.IdIntId, Corpus.IdInt],
			['id_string', Corpus.IdStringId, Corpus.IdString],
			['id_uuid', Corpus.IdUuidId, Corpus.IdUuid],
			[
				'id_array ([string, datetime], nanoseconds)',
				Corpus.IdArrayId,
				Corpus.IdArray,
			],
			['id_object ({ region, n })', Corpus.IdObjectId, Corpus.IdObject],
			['id_union (int | string)', Corpus.IdUnionId, Corpus.IdUnion],
			['id_int, bigint profile', Big.IdIntId, Big.IdInt],
			['id_union, bigint profile', Big.IdUnionId, Big.IdUnion],
		] as const) {
			it(`record ids: ${name}`, async () => {
				await idRoundTrip(id, select);
			});
		}

		it('record ids: int64 extremes, bigint profile', async () => {
			const exact = [
				-(2n ** 63n),
				-(2n ** 63n) + 1n,
				-(2n ** 53n) - 2n,
				-(2n ** 53n) + 1n,
				-1n,
				0n,
				2n ** 53n - 1n,
				2n ** 53n,
				2n ** 53n + 1n,
				2n ** 63n - 1n,
			];
			// surrealdb.js 2.1.1 decodes -(2^53 + 1) as the number -2^53, so a stored
			// -2^53 or -(2^53 + 1) cannot be told apart on the way back: Int64 refuses
			// to guess, loudly.
			const ambiguous = [-(2n ** 53n), -(2n ** 53n) - 1n];
			for (const key of [...exact, ...ambiguous]) {
				const id = Schema.encodeSync(Big.IdIntId)(
					Schema.decodeUnknownSync(Big.IdIntId)(
						new RecordId('id_int', key),
					),
				);
				try {
					await query('CREATE $id', { id });
					if (ambiguous.includes(key)) {
						await expect(selectRow(Big.IdInt, id)).rejects.toThrow(
							/exact value is unknown/,
						);
						// The stored key itself is exact: the engine compares it as an int.
						expect(
							await query('RETURN record::id($id) = $key', {
								id,
								key,
							}),
						).toBe(true);
					} else {
						const row = await selectRow(Big.IdInt, id);
						expect((row.id as RecordId).id).toBe(key);
					}
				} finally {
					await query('DELETE $id', { id });
				}
			}
		});

		// ---- relate

		it('member_of: relate(person, team | org) with edge content', async () => {
			const ends = Arbitrary.all([
				Arbitrary.schema(Schema.Literals(['team', 'org'])),
				Arbitrary.schema(Corpus.MemberOfCreate),
				Arbitrary.schema(Corpus.MemberOfMerge),
			]);
			await property(ends, async ([outTable, content, patch]) => {
				const n = ++counter;
				const from = new RecordId('person', `rel${n}`);
				const to = new RecordId(outTable, `rel${n}`);
				const edge = new RecordId('member_of', `rel${n}`);
				try {
					await query(
						'CREATE $id CONTENT { name: "A", email: "a@example.com", score: 1, bio: NULL, address: { city: "Leeds" } }',
						{ id: from },
					);
					await query('CREATE $id CONTENT { name: "T" }', { id: to });
					await query('RELATE $from->$edge->$to CONTENT $content', {
						from,
						edge,
						to,
						content: Schema.encodeSync(Corpus.MemberOfCreate)(
							content,
						),
					});
					const first = await selectRow(Corpus.MemberOf, edge);
					const decodedIn = first.in as RecordId;
					const decodedOut = first.out as RecordId;
					expect(decodedIn.equals(from)).toBe(true);
					expect(decodedOut.equals(to)).toBe(true);
					// The endpoint schema the edge descriptor declares accepts them.
					expect(
						Corpus.MemberOfEdge.out.some((schema) =>
							Schema.is(schema)(decodedOut),
						),
					).toBe(true);
					if (content.role !== undefined)
						expect(first.role).toBe(content.role);
					if (content.since !== undefined)
						expect(
							equalField(
								Corpus.MemberOf,
								'since',
								first.since,
								content.since,
							),
						).toBe(true);
					await query('UPDATE $id MERGE $patch', {
						id: edge,
						patch: Schema.encodeSync(Corpus.MemberOfMerge)(patch),
					});
					const second = await selectRow(Corpus.MemberOf, edge);
					expect(second.role).toBe(patch.role ?? first.role);
					expect(
						equalField(
							Corpus.MemberOf,
							'since',
							second.since,
							first.since,
						),
					).toBe(true);
					return true;
				} finally {
					await query('DELETE $edge; DELETE $from; DELETE $to', {
						edge,
						from,
						to,
					});
				}
			});
		});

		// ---- call

		const call = async (
			fn: {
				readonly name: string;
				readonly args: Schema.Top;
				readonly returns: Schema.Top;
			},
			args: ReadonlyArray<unknown>,
		): Promise<unknown> => {
			const encoded = Schema.encodeUnknownSync(
				fn.args as Schema.Codec<unknown, ReadonlyArray<unknown>>,
			)(args);
			const vars: Row = {};
			const names = encoded.map((value, i) => {
				vars[`a${i}`] = value;
				return `$a${i}`;
			});
			const raw = await query(
				`RETURN ${fn.name}(${names.join(', ')})`,
				vars,
			);
			return Schema.decodeUnknownSync(
				fn.returns as Schema.Codec<unknown>,
			)(raw);
		};

		it('fn::greet: trailing optional arguments may be omitted', async () => {
			await property(
				Arbitrary.schema(Corpus.fn_greet.args),
				async (args) => {
					const [name, greeting] = args;
					return (
						(await call(Corpus.fn_greet, args)) ===
						`${greeting ?? 'hello'} ${name}`
					);
				},
			);
		});

		it('fn::optional_first: int64 through a call, bigint profile, a leading NONE', async () => {
			await property(
				Arbitrary.schema(Big.fn_optional_first.args),
				async (args) => {
					const result = await call(Big.fn_optional_first, args);
					return result === args[1];
				},
			);
		});

		it('fn::double: an undeclared return is unknown and still the engine value', async () => {
			await property(
				Arbitrary.schema(Corpus.fn_double.args),
				async (args) => {
					const result = await call(Corpus.fn_double, args);
					return (
						BigInt(result as number | bigint) ===
						BigInt(args[0]) * 2n
					);
				},
			);
		});
	},
);
