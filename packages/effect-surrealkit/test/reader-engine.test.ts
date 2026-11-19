/**
 * The reader as a property of a store view, through a real engine and a
 * record user signed up with the testing subpath's `recordUser`: a field
 * permission hides `secret` of `rd_item` and `hush` of the `rd_link` edge,
 * and the session may not read `rd_vault` at all. The descriptors are
 * written here as the emitter writes them, a `select` row for a privileged
 * reader and a `visible` row in which a hidden field is optional.
 *
 * Built from the privileged descriptors and store, a link, a step, a
 * nested projection and a FETCH each type the hidden field as present, and
 * decoding what the session gets fails; built from `readAs(table)` and
 * `readAs(store)`, every table on the way reads through its `visible` row
 * and the same statements decode. A statement that mixes the two readers
 * is refused when it is built.
 */
import { Effect, type Layer, Schema } from 'effect';
import { RecordId } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
	StatementDecodeError,
	type SurrealClient,
} from '../src/client/index.js';
import * as SurrealSchema from '../src/index.js';
import {
	collect,
	destructure,
	edges,
	follow,
	outgoing,
	param,
	type Result,
	readAs,
	rows,
	select,
} from '../src/statement/index.js';
import {
	layerFrom,
	recordUser,
	type TestDatabase,
} from '../src/testing/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';
import { store as corpus, PersonTable } from './golden/corpus/index.js';

const ItemId = SurrealSchema.RecordIdFor('rd_item');
const VaultId = SurrealSchema.RecordIdFor('rd_vault');
const LinkId = SurrealSchema.RecordIdFor('rd_link');

const ItemTable = {
	name: 'rd_item',
	schemafull: true,
	id: ItemId,
	select: Schema.Struct({
		id: ItemId,
		label: SurrealSchema.String,
		secret: SurrealSchema.String,
		next: Schema.optionalKey(ItemId),
	}),
	visible: Schema.Struct({
		id: ItemId,
		label: SurrealSchema.String,
		secret: Schema.optionalKey(SurrealSchema.String),
		next: Schema.optionalKey(ItemId),
	}),
} as const;

const Vault = Schema.Struct({
	id: VaultId,
	label: SurrealSchema.String,
	secret: SurrealSchema.String,
});
const VaultTable = {
	name: 'rd_vault',
	schemafull: true,
	id: VaultId,
	select: Vault,
	visible: Vault,
} as const;

const LinkEdge = {
	table: 'rd_link',
	in: [ItemId],
	out: [ItemId, VaultId],
	enforced: true,
} as const;
const LinkTable = {
	name: 'rd_link',
	schemafull: true,
	id: LinkId,
	select: Schema.Struct({
		id: LinkId,
		in: ItemId,
		out: Schema.Union([ItemId, VaultId]),
		note: SurrealSchema.String,
		hush: SurrealSchema.String,
	}),
	visible: Schema.Struct({
		id: LinkId,
		in: ItemId,
		out: Schema.Union([ItemId, VaultId]),
		note: SurrealSchema.String,
		hush: Schema.optionalKey(SurrealSchema.String),
	}),
	edge: LinkEdge,
} as const;

const store = {
	tables: { rd_item: ItemTable, rd_vault: VaultTable, rd_link: LinkTable },
	edges: { rd_link: LinkEdge },
} as const;
const session = readAs(store);
const Item = readAs(ItemTable);

const a = new RecordId('rd_item', 'a');
const b = new RecordId('rd_item', 'b');
const who = param('who', ItemId);

const schema = `
	DEFINE TABLE rd_user SCHEMAFULL PERMISSIONS FOR select FULL FOR create, update, delete NONE;
	DEFINE FIELD name ON rd_user TYPE string;
	DEFINE ACCESS rd_access ON DATABASE TYPE RECORD
		SIGNUP (CREATE rd_user SET name = $name)
		SIGNIN (SELECT * FROM rd_user WHERE name = $name);
	DEFINE TABLE rd_item SCHEMAFULL PERMISSIONS FOR select FULL FOR create, update, delete NONE;
	DEFINE FIELD label ON rd_item TYPE string;
	DEFINE FIELD secret ON rd_item TYPE string PERMISSIONS FOR select NONE;
	DEFINE FIELD next ON rd_item TYPE option<record<rd_item>>;
	DEFINE TABLE rd_vault SCHEMAFULL PERMISSIONS NONE;
	DEFINE FIELD label ON rd_vault TYPE string;
	DEFINE FIELD secret ON rd_vault TYPE string;
	DEFINE TABLE rd_link TYPE RELATION IN rd_item OUT rd_item | rd_vault ENFORCED SCHEMAFULL
		PERMISSIONS FOR select FULL FOR create, update, delete NONE;
	DEFINE FIELD note ON rd_link TYPE string;
	DEFINE FIELD hush ON rd_link TYPE string PERMISSIONS FOR select NONE;
	CREATE rd_item:a SET label = 'A', secret = 'sa', next = rd_item:b;
	CREATE rd_item:b SET label = 'B', secret = 'sb';
	CREATE rd_vault:v SET label = 'V', secret = 'sv';
	RELATE rd_item:a->rd_link->rd_item:b SET note = 'ab', hush = 'h1';
	RELATE rd_item:a->rd_link->rd_vault:v SET note = 'av', hush = 'h2';
`;

/** Fails to compile unless `A` and `B` are the same type. */
type Same<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;
const same = <A, B>(_: Same<A, B>): void => undefined;

// A hidden field read through a step of the store view may be absent.
const reached = select(Item, who)
	.only()
	.value(
		(row) => follow(session, outgoing(session, row.id, 'rd_link')).secret,
	)
	.build();
same<Result<typeof reached>, ReadonlyArray<string | undefined> | undefined>(
	true,
);
const privileged = select(ItemTable, who)
	.only()
	.value((row) => follow(store, outgoing(store, row.id, 'rd_link')).secret)
	.build();
same<Result<typeof privileged>, ReadonlyArray<string> | undefined>(true);

describe('a statement that mixes readers', () => {
	it('is refused when built: a privileged subject with a restricted store view, and the reverse', () => {
		expect(() =>
			select(ItemTable)
				.value((row) => rows(session, row.next))
				.build('mixed'),
		).toThrow(
			'statement `mixed` reads its table as a privileged reader, but a link, step or FETCH in it reads records through a restricted store view (`readAs(store)`)',
		);
		expect(() =>
			select(Item)
				.value((row) => outgoing(store, row.id, 'rd_link'))
				.build('mixed'),
		).toThrow(
			'statement `mixed` reads its table through `readAs`, but a link, step or FETCH in it reads records through the privileged store',
		);
		expect(() =>
			select(Item, who)
				.project((row) => ({ next: row.next }))
				.fetch(store, 'next')
				.build('mixed'),
		).toThrow(/through the privileged store/);
		expect(() =>
			select(ItemTable)
				.value((row) => follow(session, row.next).label)
				.toExpr(),
		).toThrow(
			/the subquery of `rd_item` reads its table as a privileged reader/,
		);
		// A recursive path reads through the store of the step it repeats.
		expect(() =>
			select(PersonTable)
				.value((row) =>
					collect(row.id, (node) =>
						outgoing(readAs(corpus), node, 'knows'),
					),
				)
				.build('reach'),
		).toThrow(
			/statement `reach` reads its table as a privileged reader, but a link, step or FETCH in it reads records through a restricted store view/,
		);
		// Each select of a statement answers for its own reader.
		expect(
			select(ItemTable)
				.project((row) => ({
					label: row.label,
					seen: select(Item)
						.value((item) => follow(session, item.next).label)
						.toExpr(),
				}))
				.build().sql,
		).toBe(
			'SELECT label AS label, (SELECT VALUE next.label FROM ⟨rd_item⟩) AS seen FROM ⟨rd_item⟩;',
		);
	});

	it('reads every table of a store view through its visible row', () => {
		expect(session.tables.rd_item.select).toBe(ItemTable.visible);
		expect(session.tables.rd_link.select).toBe(LinkTable.visible);
		expect(session.edges).toBe(store.edges);
	});
});

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] reader engine suite skipped: ${engineSkipReason}`,
	);
}

describe.skipIf(!engineConfigured)(
	'a restricted store view on an engine',
	() => {
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

		/** Runs `effect` as a fresh record user of `rd_access`. */
		const asUser = <A, E>(
			effect: (client: SurrealClient['Service']) => Effect.Effect<A, E>,
		) =>
			Effect.runPromise(
				Effect.scoped(
					Effect.gen(function* () {
						const user = yield* recordUser({
							access: 'rd_access',
							variables: {
								name: `u${Date.now()}${Math.random()}`,
							},
						});
						return yield* effect(user.client);
					}),
				).pipe(Effect.provide(database)),
			);

		/** The privileged statement fails to decode what the session gets. */
		const refused = async (
			statement: Parameters<SurrealClient['Service']['run']>[0],
		) => {
			const error = await asUser((client) =>
				Effect.flip(
					client.run(statement as never, { who: a } as never),
				),
			);
			expect(error).toBeInstanceOf(StatementDecodeError);
		};

		it('a link: the target row', async () => {
			await refused(
				select(ItemTable, who)
					.only()
					.value((row) => rows(store, row.next))
					.build('privileged', { writes: false }),
			);
			const statement = select(Item, who)
				.only()
				.value((row) => rows(session, row.next))
				.build();
			expect(
				await asUser((client) => client.run(statement, { who: a })),
			).toEqual({ id: b, label: 'B' });
		});

		it('a step: a field of the records it reaches', async () => {
			await refused(
				select(ItemTable, who)
					.only()
					.value(
						(row) =>
							follow(store, outgoing(store, row.id, 'rd_link'))
								.secret,
					)
					.build('privileged', { writes: false }),
			);
			const statement = select(Item, who)
				.only()
				.project((row) => ({
					labels: follow(
						session,
						outgoing(session, row.id, 'rd_link'),
					).label,
					secrets: follow(
						session,
						outgoing(session, row.id, 'rd_link'),
					).secret,
				}))
				.build();
			expect(
				await asUser((client) => client.run(statement, { who: a })),
			).toEqual({ labels: ['B'], secrets: [undefined] });
		});

		it('a nested projection over the records a step reaches', async () => {
			await refused(
				select(ItemTable, who)
					.only()
					.value((row) =>
						destructure(
							store,
							outgoing(store, row.id, 'rd_link'),
							(item) => ({
								label: item.label,
								secret: item.secret,
							}),
						),
					)
					.build('privileged', { writes: false }),
			);
			const statement = select(Item, who)
				.only()
				.value((row) =>
					destructure(
						session,
						outgoing(session, row.id, 'rd_link'),
						(item) => ({
							label: item.label,
							secret: item.secret,
						}),
					),
				)
				.build();
			expect(
				await asUser((client) => client.run(statement, { who: a })),
			).toEqual([{ label: 'B', secret: undefined }]);
		});

		it('a FETCH of a link', async () => {
			await refused(
				select(ItemTable, who)
					.only()
					.project((row) => ({ next: row.next }))
					.fetch(store, 'next')
					.build('privileged', { writes: false }),
			);
			const statement = select(Item, who)
				.only()
				.project((row) => ({ next: row.next }))
				.fetch(session, 'next')
				.build();
			expect(
				await asUser((client) => client.run(statement, { who: a })),
			).toEqual({ next: { id: b, label: 'B' } });
		});

		it('the edges of a step and the records it reaches can differ in length; pair them from the edge', async () => {
			const statement = select(Item, who)
				.only()
				.project((row) => ({
					edges: edges(outgoing(session, row.id, 'rd_link')),
					records: outgoing(session, row.id, 'rd_link'),
					paired: destructure(
						session,
						edges(outgoing(session, row.id, 'rd_link')),
						(edge) => ({
							note: edge.note,
							hush: edge.hush,
							to: follow(session, edge.out).label,
						}),
					),
				}))
				.build();
			const result = await asUser((client) =>
				client.run(statement, { who: a }),
			);
			// The session reads both edges, and only the record it may read.
			expect(result?.edges).toHaveLength(2);
			expect(result?.records).toEqual([b]);
			expect(
				[...(result?.paired ?? [])].sort((x, y) =>
					x.note.localeCompare(y.note),
				),
			).toEqual([
				{ note: 'ab', hush: undefined, to: 'B' },
				{ note: 'av', hush: undefined, to: undefined },
			]);
		});
	},
);
