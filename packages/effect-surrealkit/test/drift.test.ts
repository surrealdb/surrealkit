/**
 * Schema drift on a real engine: `schemaDrift` from the testing subpath
 * compares a store's declarations with the live database, and a record an
 * ENFORCED relation reaches that reads NONE fails to decode with a message
 * that says the live schema may differ. The descriptors are this file's
 * own, written as the emitter writes them; `test/conformance.test.ts` pins
 * what `INFO … STRUCTURE` returns on both engines.
 */
import { Effect, type Layer, Schema } from 'effect';
import { RecordId } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { StatementDecodeError, SurrealClient } from '../src/client/index.js';
import * as SurrealSchema from '../src/index.js';
import {
	evaluate,
	follow,
	outgoing,
	param,
	rows,
} from '../src/statement/index.js';
import {
	layerFrom,
	type SchemaDifference,
	schemaDrift,
	type TestDatabase,
} from '../src/testing/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';
import { store as corpusStore } from './golden/corpus/index.js';

const PId = SurrealSchema.RecordIdFor('dr_p');
const GId = SurrealSchema.RecordIdFor('dr_g');
const LinkId = SurrealSchema.RecordIdFor('dr_link');
const LooseId = SurrealSchema.RecordIdFor('dr_loose');

const Person = Schema.Struct({
	id: PId,
	name: SurrealSchema.String,
	email: SurrealSchema.String,
	tags: Schema.Array(SurrealSchema.String),
});
const PersonTable = {
	name: 'dr_p',
	type: 'normal',
	schemafull: true,
	id: PId,
	select: Person,
	visible: Person,
	indexes: {
		dr_email: { type: 'unique', columns: [['email']] },
		dr_name_email: { type: 'standard', columns: [['name'], ['email']] },
		dr_tags: { type: 'standard', columns: [['tags', '*']] },
		dr_count: { type: 'count', columns: [] },
	},
} as const;
const Group = Schema.Struct({ id: GId, name: SurrealSchema.String });
const GroupTable = {
	name: 'dr_g',
	type: 'normal',
	schemafull: true,
	id: GId,
	select: Group,
	visible: Group,
} as const;
const LinkEdge = {
	table: 'dr_link',
	in: [PId],
	out: [GId],
	enforced: true,
} as const;
const Link = Schema.Struct({ id: LinkId, in: PId, out: GId });
const LinkTable = {
	name: 'dr_link',
	type: 'relation',
	schemafull: false,
	id: LinkId,
	select: Link,
	visible: Link,
	edge: LinkEdge,
} as const;
// Declared ENFORCED; the live database defines it without.
const LooseEdge = {
	table: 'dr_loose',
	in: [PId],
	out: [PId],
	enforced: true,
} as const;
const Loose = Schema.Struct({ id: LooseId, in: PId, out: PId });
const LooseTable = {
	name: 'dr_loose',
	type: 'relation',
	schemafull: false,
	id: LooseId,
	select: Loose,
	visible: Loose,
	edge: LooseEdge,
} as const;

// A generated store registry is what the check takes.
export const corpus: Parameters<typeof schemaDrift>[0] = corpusStore;

const matching = {
	tables: { dr_p: PersonTable, dr_g: GroupTable, dr_link: LinkTable },
	edges: { dr_link: LinkEdge },
} as const;

const drifted = {
	tables: {
		dr_p: PersonTable,
		dr_g: GroupTable,
		dr_loose: LooseTable,
	},
	edges: { dr_loose: LooseEdge },
} as const;

const schema = `
	DEFINE TABLE dr_p TYPE NORMAL SCHEMAFULL;
	DEFINE FIELD name ON dr_p TYPE string;
	DEFINE FIELD email ON dr_p TYPE string;
	DEFINE FIELD tags ON dr_p TYPE array<string> DEFAULT [];
	DEFINE INDEX dr_email ON dr_p FIELDS email UNIQUE;
	DEFINE INDEX dr_name_email ON dr_p FIELDS name, email;
	DEFINE INDEX dr_tags ON dr_p FIELDS tags.*;
	DEFINE INDEX dr_count ON dr_p COUNT;
	DEFINE TABLE dr_g TYPE NORMAL SCHEMAFULL;
	DEFINE FIELD name ON dr_g TYPE string;
	DEFINE TABLE dr_link TYPE RELATION IN dr_p OUT dr_g ENFORCED;
	DEFINE TABLE dr_loose TYPE RELATION IN dr_p OUT dr_p;
	CREATE dr_p:ann SET name = 'Ann', email = 'ann@example.com';
	RELATE dr_p:ann->dr_loose->dr_p:ghost;
`;

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] drift engine suite skipped: ${engineSkipReason}`,
	);
}

describe.skipIf(!engineConfigured)('schema drift on an engine', () => {
	let engine: Engine;
	let database: Layer.Layer<TestDatabase | SurrealClient>;

	beforeAll(async () => {
		engine = await startEngine();
		await within(engine.db.query(schema), 20_000);
		database = layerFrom(engine.db, {
			namespace: engine.namespace,
			database: engine.namespace,
		});
	});
	afterAll(async () => {
		await engine?.stop();
	});

	const drift = (store: Parameters<typeof schemaDrift>[0]) =>
		Effect.runPromise(schemaDrift(store).pipe(Effect.provide(database)));

	/** What running `effect` with the suite's client fails with. */
	const failure = <A, E>(
		effect: (client: SurrealClient['Service']) => Effect.Effect<A, E>,
	): Promise<E> =>
		Effect.runPromise(
			Effect.flip(
				Effect.flatMap(Effect.service(SurrealClient), effect),
			).pipe(Effect.provide(database)),
		);

	const only = (
		differences: ReadonlyArray<SchemaDifference>,
	): ReadonlyArray<Omit<SchemaDifference, 'message'>> =>
		differences.map(({ message: _, ...rest }) => rest);

	it('finds no difference where the declarations match', async () => {
		expect(await drift(matching)).toEqual([]);
	});

	it('finds a table that is missing or of another kind', async () => {
		const differences = await drift({
			tables: {
				dr_nosuch: { ...GroupTable, name: 'dr_nosuch' },
				dr_g: { ...GroupTable, type: 'relation' },
			},
		});
		expect(only(differences)).toEqual([
			{
				kind: 'missing-table',
				table: 'dr_nosuch',
				declared: 'normal',
				live: '(none)',
			},
			{
				kind: 'table-kind',
				table: 'dr_g',
				declared: 'relation',
				live: 'normal',
			},
		]);
		expect(differences[0]?.message).toBe(
			'`dr_nosuch` is declared and not defined',
		);
	});

	it('finds a relation whose ENFORCED flag or endpoint tables differ', async () => {
		const differences = await drift({
			tables: {
				dr_loose: LooseTable,
				dr_link: {
					...LinkTable,
					edge: { ...LinkEdge, in: [GId], out: [GId, PId] },
				},
			},
		});
		expect(only(differences)).toEqual([
			{
				kind: 'relation-enforced',
				table: 'dr_loose',
				declared: 'ENFORCED',
				live: 'not ENFORCED',
			},
			{
				kind: 'relation-endpoints',
				table: 'dr_link',
				declared: 'IN dr_g',
				live: 'IN dr_p',
			},
			{
				kind: 'relation-endpoints',
				table: 'dr_link',
				declared: 'OUT dr_g | dr_p',
				live: 'OUT dr_g',
			},
		]);
	});

	it('finds an index that is missing, of another kind or over other fields', async () => {
		const differences = await drift({
			tables: {
				dr_p: {
					...PersonTable,
					indexes: {
						dr_email: { type: 'standard', columns: [['email']] },
						dr_name_email: {
							type: 'standard',
							columns: [['email'], ['name']],
						},
						dr_tags: { type: 'standard', columns: [['tags', '*']] },
						dr_nosuch: { type: 'unique', columns: [['name']] },
					},
				},
			},
		});
		expect(only(differences)).toEqual([
			{
				kind: 'index-kind',
				table: 'dr_p',
				index: 'dr_email',
				declared: 'standard',
				live: 'unique',
			},
			{
				kind: 'index-fields',
				table: 'dr_p',
				index: 'dr_name_email',
				declared: 'email, name',
				live: 'name, email',
			},
			{
				kind: 'missing-index',
				table: 'dr_p',
				index: 'dr_nosuch',
				declared: 'unique on name',
				live: '(none)',
			},
		]);
	});

	it('a record an ENFORCED relation reaches that reads NONE fails to decode, saying the live schema may differ', async () => {
		const names = evaluate(
			follow(drifted, outgoing(drifted, param('who', PId), 'dr_loose'))
				.name,
			'whom I reach',
			{ writes: false },
		);
		const reached = evaluate(
			rows(drifted, outgoing(drifted, param('who', PId), 'dr_loose')),
			'the records I reach',
			{ writes: false },
		);
		const ann = new RecordId('dr_p', 'ann');
		for (const error of [
			await failure((client) => client.run(names, { who: ann })),
			await failure((client) => client.run(reached, { who: ann })),
		]) {
			expect(error).toBeInstanceOf(StatementDecodeError);
			expect(error.message).toMatch(
				/a record reached through `dr_loose` is NONE, but the generated descriptors declare `dr_loose` ENFORCED.*the live schema may differ from the generated descriptors/s,
			);
		}
		expect(only(await drift(drifted))).toEqual([
			{
				kind: 'relation-enforced',
				table: 'dr_loose',
				declared: 'ENFORCED',
				live: 'not ENFORCED',
			},
		]);
	});
});
