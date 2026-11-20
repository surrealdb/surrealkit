/**
 * What the engine does with a NONE binding assigned to a field with
 * `SET field = $value`, for each field declaration and each statement in
 * which the analyzer records an assignment (`CREATE`, `UPDATE` and
 * `UPSERT`, the last both creating a record and updating one). These are
 * the facts the compile-time NONE refusal rests on: a codec that can hold
 * NONE is refused only for a field that rejects NONE in every one of them.
 */
import { Effect, Schema } from 'effect';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { make as makeClient } from '../src/client/index.js';
import * as SurrealSchema from '../src/index.js';
import { sequence } from '../src/statement/index.js';

import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';
import { presence } from './golden/statements-presence/index.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] NONE assignment engine suite skipped: ${engineSkipReason}`,
	);
}

/** One field declaration: the clauses after `DEFINE FIELD f ON <table>`. */
interface Declaration {
	readonly name: string;
	/** `DEFINE TABLE` clauses; SCHEMAFULL unless stated. */
	readonly table?: string;
	readonly field: string;
	/**
	 * What the engine does, in the order of {@link statements}: `rejected`
	 * in all four, or each outcome.
	 */
	readonly outcome:
		| 'rejected'
		| readonly [Outcome, Outcome, Outcome, Outcome];
	/**
	 * A value the field holds, for the record an update starts from; empty
	 * when the record is created without one.
	 */
	readonly seed: string;
}

/**
 * `rejected` (the engine refused NONE for this field), or `accepted: <the
 * field's value afterwards as a string>`, `NONE` when it is absent.
 */
type Outcome = string;

const declarations: ReadonlyArray<Declaration> = [
	{ name: 'string', field: 'TYPE string', outcome: 'rejected', seed: "'b'" },
	{
		name: 'option<string>',
		field: 'TYPE option<string>',
		outcome: [
			'accepted: NONE',
			'accepted: NONE',
			'accepted: NONE',
			'accepted: NONE',
		],
		seed: "'b'",
	},
	{
		name: 'string | none',
		field: 'TYPE string | none',
		outcome: [
			'accepted: NONE',
			'accepted: NONE',
			'accepted: NONE',
			'accepted: NONE',
		],
		seed: "'b'",
	},
	{
		name: 'string | null',
		field: 'TYPE string | null',
		outcome: 'rejected',
		seed: "'b'",
	},
	{
		name: 'no TYPE',
		field: '',
		outcome: [
			'accepted: NONE',
			'accepted: NONE',
			'accepted: NONE',
			'accepted: NONE',
		],
		seed: "'b'",
	},
	{
		name: 'string, SCHEMALESS',
		table: 'SCHEMALESS',
		field: 'TYPE string',
		outcome: 'rejected',
		seed: "'b'",
	},
	{
		name: 'string DEFAULT',
		field: "TYPE string DEFAULT 'a'",
		outcome: ['accepted: a', 'rejected', 'accepted: a', 'rejected'],
		seed: "'b'",
	},
	{
		name: 'string DEFAULT ALWAYS',
		field: "TYPE string DEFAULT ALWAYS 'a'",
		outcome: ['accepted: a', 'accepted: a', 'accepted: a', 'accepted: a'],
		seed: "'b'",
	},
	{
		name: 'option<string> DEFAULT',
		field: "TYPE option<string> DEFAULT 'a'",
		outcome: [
			'accepted: a',
			'accepted: NONE',
			'accepted: a',
			'accepted: NONE',
		],
		seed: "'b'",
	},
	{
		name: 'string VALUE reading $value',
		field: 'TYPE string VALUE string::lowercase($value)',
		outcome: 'rejected',
		seed: "'b'",
	},
	{
		name: 'string VALUE falling back',
		field: "TYPE string VALUE $value ?? 'a'",
		outcome: ['accepted: a', 'accepted: a', 'accepted: a', 'accepted: a'],
		seed: "'b'",
	},
	{
		name: 'string VALUE ignoring $value',
		field: "TYPE string VALUE 'a'",
		outcome: ['accepted: a', 'accepted: a', 'accepted: a', 'accepted: a'],
		seed: "'b'",
	},
	{
		name: 'string READONLY',
		field: 'TYPE string READONLY',
		outcome: 'rejected',
		seed: "'b'",
	},
	{
		name: 'string ASSERT',
		field: 'TYPE string ASSERT $value != NONE',
		outcome: 'rejected',
		seed: "'b'",
	},
	{ name: 'bool', field: 'TYPE bool', outcome: 'rejected', seed: 'true' },
	{ name: 'int', field: 'TYPE int', outcome: 'rejected', seed: '1' },
	{ name: 'float', field: 'TYPE float', outcome: 'rejected', seed: '1f' },
	{ name: 'number', field: 'TYPE number', outcome: 'rejected', seed: '1' },
	{
		name: 'decimal',
		field: 'TYPE decimal',
		outcome: 'rejected',
		seed: '1dec',
	},
	{
		name: 'datetime',
		field: 'TYPE datetime',
		outcome: 'rejected',
		seed: 'time::now()',
	},
	{
		name: 'duration',
		field: 'TYPE duration',
		outcome: 'rejected',
		seed: '1s',
	},
	{
		name: 'uuid',
		field: 'TYPE uuid',
		outcome: 'rejected',
		seed: 'rand::uuid()',
	},
	{
		name: 'record<other>',
		field: 'TYPE record<other>',
		outcome: 'rejected',
		seed: 'other:a',
	},
	{
		name: 'bytes',
		field: 'TYPE bytes',
		outcome: 'rejected',
		seed: "<bytes>'b'",
	},
	{ name: 'object', field: 'TYPE object', outcome: 'rejected', seed: '{}' },
	{
		name: 'array<string>',
		field: 'TYPE array<string>',
		outcome: 'rejected',
		seed: "['b']",
	},
	{
		name: 'set<string>',
		field: 'TYPE set<string>',
		outcome: 'rejected',
		seed: "<set>['b']",
	},
	{
		name: "'a' | 'b'",
		field: "TYPE 'a' | 'b'",
		outcome: 'rejected',
		seed: "'b'",
	},
	{
		name: 'int | string',
		field: 'TYPE int | string',
		outcome: 'rejected',
		seed: "'b'",
	},
	{
		name: 'geometry<point>',
		field: 'TYPE geometry<point>',
		outcome: 'rejected',
		seed: '(1, 2)',
	},
	{
		name: '[string]',
		field: 'TYPE [string]',
		outcome: 'rejected',
		seed: "['b']",
	},
	{
		name: '{ a: string }',
		field: 'TYPE { a: string }',
		outcome: 'rejected',
		seed: "{ a: 'b' }",
	},
	{ name: 'range', field: 'TYPE range', outcome: 'rejected', seed: '1..2' },
	{ name: 'null', field: 'TYPE null', outcome: 'rejected', seed: 'NULL' },
	{
		name: 'record<other> REFERENCE',
		field: 'TYPE record<other> REFERENCE',
		outcome: 'rejected',
		seed: 'other:a',
	},
	{
		name: 'string COMPUTED',
		field: "TYPE string COMPUTED 'a'",
		outcome: ['accepted: a', 'accepted: a', 'accepted: a', 'accepted: a'],
		seed: '',
	},
	{
		name: 'string, DROP table',
		table: 'DROP SCHEMAFULL',
		field: 'TYPE string',
		outcome: ['rejected', 'accepted: NONE', 'rejected', 'rejected'],
		seed: "'b'",
	},
];

/** The statements the analyzer records an assignment in. */
const statements = {
	CREATE: (t: string) => `CREATE ${t}:new SET f = $none;`,
	UPDATE: (t: string) => `UPDATE ${t}:old SET f = $none;`,
	'UPSERT (new)': (t: string) => `UPSERT ${t}:fresh SET f = $none;`,
	'UPSERT (existing)': (t: string) => `UPSERT ${t}:old SET f = $none;`,
} as const;

const kinds = Object.keys(statements) as ReadonlyArray<keyof typeof statements>;

/** Each declaration's outcomes by statement. */
const expected = Object.fromEntries(
	declarations.map((declaration) => [
		declaration.name,
		Object.fromEntries(
			kinds.map((kind, index) => [
				kind,
				declaration.outcome === 'rejected'
					? 'rejected'
					: declaration.outcome[index],
			]),
		),
	]),
);

describe.skipIf(!engineConfigured)('a NONE binding assigned to a field', () => {
	let engine: Engine;

	beforeAll(async () => {
		engine = await startEngine();
		await within(
			Promise.resolve(
				engine.db.query(
					'DEFINE TABLE other SCHEMALESS; CREATE other:a;',
				),
			),
		);
	});
	afterAll(async () => {
		await engine?.stop();
	});

	/** Runs `sql`; the error's message when the engine rejects it. */
	const attempt = async (
		sql: string,
		bindings?: Record<string, unknown>,
	): Promise<string | undefined> => {
		try {
			await within(Promise.resolve(engine.db.query(sql, bindings)));
			return undefined;
		} catch (error) {
			return error instanceof Error ? error.message : String(error);
		}
	};

	it('passes NONE for an undefined binding', async () => {
		const [none] = await within(
			Promise.resolve(
				engine.db.query<[boolean]>('RETURN $none IS NONE;', {
					none: undefined,
				}),
			),
		);
		expect(none).toBe(true);
	});

	it('rejects NONE exactly where each declaration says', async () => {
		const found: Record<string, Record<string, Outcome>> = {};
		for (const [index, declaration] of declarations.entries()) {
			const outcomes: Record<string, Outcome> = {};
			for (const [kind, statement] of Object.entries(statements)) {
				const t = `t${index}_${kind.replace(/\W/gu, '')}`;
				const defined = await attempt(
					`DEFINE TABLE ${t} ${declaration.table ?? 'SCHEMAFULL'}; DEFINE FIELD f ON ${t} ${declaration.field};`,
				);
				if (defined !== undefined) {
					outcomes[kind] = `not definable: ${defined}`;
					continue;
				}
				const seeded = await attempt(
					declaration.seed === ''
						? `CREATE ${t}:old;`
						: `CREATE ${t}:old SET f = ${declaration.seed};`,
				);
				if (seeded !== undefined) {
					outcomes[kind] = `not seeded: ${seeded}`;
					continue;
				}
				const failed = await attempt(statement(t), { none: undefined });
				if (failed !== undefined) {
					// Every rejection is about the NONE: the kind or READONLY.
					outcomes[kind] = /found `NONE`|field is readonly/u.test(
						failed,
					)
						? 'rejected'
						: `rejected otherwise: ${failed}`;
					continue;
				}
				const id = statement(t).split(' ')[1]?.split(':')[1] as string;
				const [value] = await within(
					Promise.resolve(
						engine.db.query<[unknown]>(
							`RETURN <string> (SELECT VALUE f FROM ONLY ${t}:${id});`,
						),
					),
				);
				outcomes[kind] = `accepted: ${String(value)}`;
			}
			found[declaration.name] = outcomes;
		}
		expect(found).toEqual(expected);
	});
});

describe.skipIf(!engineConfigured)(
	'a generated part assigning an optional field',
	() => {
		let engine: Engine;

		beforeAll(async () => {
			engine = await startEngine();
			// The `m` fields of the field-presence corpus the part reads.
			await within(
				Promise.resolve(
					engine.db.query(
						"DEFINE TABLE m SCHEMAFULL; DEFINE FIELD req ON m TYPE string; DEFINE FIELD opt ON m TYPE option<string>; CREATE m:one SET req = 'x';",
					),
				),
			);
		});
		afterAll(async () => {
			await engine?.stop();
		});

		it('runs with a narrow codec that can hold NONE', async () => {
			const client = makeClient(engine.db);
			const assign = sequence(presence.assign).define({
				name: 'presence.assign',
				params: {
					opt: Schema.UndefinedOr(Schema.Literal('a')),
					req: SurrealSchema.String,
				},
				result: Schema.Unknown,
			});
			const opt = async () => {
				const [value] = await within(
					Promise.resolve(
						engine.db.query<[unknown]>(
							'RETURN <string> (SELECT VALUE opt FROM ONLY m:one);',
						),
					),
				);
				return value;
			};
			await within(
				Effect.runPromise(client.run(assign, { opt: 'a', req: 'y' })),
			);
			expect(await opt()).toBe('a');
			await within(
				Effect.runPromise(
					client.run(assign, { opt: undefined, req: 'y' }),
				),
			);
			expect(await opt()).toBe('NONE');
		});
	},
);
