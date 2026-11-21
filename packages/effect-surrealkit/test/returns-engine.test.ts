/**
 * Every `RETURN` mode of `update`, `upsert`, `deleteFrom` and
 * `insertRelation` on whichever engine the run uses (see `engine.ts`): a
 * record that exists and one that does not, a condition that matches and
 * one that matches nothing, an edge to a record that exists and to one that
 * does not. Each result is decoded strictly by the codec its statement
 * derived, is annotated with the type its statement declares, and is
 * compared whole; the stored records are read back to show what the
 * statement did. SurrealDB 3.3 and 4.0 answer all of it alike.
 */
import { Effect, Schema } from 'effect';
import { RecordId } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
	type Client,
	make as makeClient,
	StatementRequestError,
} from '../src/client/index.js';
import * as SurrealSchema from '../src/index.js';
import {
	type Diff,
	deleteFrom,
	eq,
	insertRelation,
	param,
	update,
	upsert,
} from '../src/statement/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';
import {
	Holds,
	type Item,
	ItemCreate,
	ItemId,
	ItemMerge,
	ItemTable,
	returnsSchema,
	Sees,
} from './returns.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] RETURN mode engine suite skipped: ${engineSkipReason}`,
	);
}

type HoldsRow = typeof Holds.row.Type;

describe.skipIf(!engineConfigured)('RETURN modes on the engine', () => {
	let engine: Engine;
	let client: Client;

	const ok = <A, E>(effect: Effect.Effect<A, E>): Promise<A> =>
		within(Effect.runPromise(effect), 20_000);
	const failure = <A, E>(effect: Effect.Effect<A, E>): Promise<E> =>
		within(Effect.runPromise(Effect.flip(effect)), 20_000);

	const item = (key: string) => new RecordId('rt_item', key) as ItemId;
	const owner = new RecordId('rt_owner', 'o');
	/** Stores an item and answers its id. */
	const seed = async (
		key: string,
		data: { readonly name: string; readonly qty: number; note?: string },
	): Promise<ItemId> => {
		const id = item(key);
		await engine.db.query('CREATE $id CONTENT $data', { id, data });
		return id;
	};
	/** The stored record as the engine holds it, or `undefined`. */
	const stored = async (id: RecordId): Promise<unknown> => {
		const [row] = await engine.db.query<[unknown]>(
			'SELECT * FROM ONLY $id',
			{ id },
		);
		return row;
	};
	const countOf = async (table: string, where = 'true'): Promise<number> => {
		const [rows] = await engine.db.query<[ReadonlyArray<unknown>]>(
			`SELECT * FROM ${table} WHERE ${where}`,
		);
		return rows.length;
	};

	beforeAll(async () => {
		engine = await startEngine();
		client = makeClient(engine.db);
		await engine.db.query(returnsSchema);
	});
	afterAll(async () => {
		await engine?.stop();
	});

	const id = param('id', ItemId);
	const named = param('name', SurrealSchema.String);

	describe('UPDATE', () => {
		const one = update(ItemTable, id).merge(param('patch', ItemMerge));
		const many = update(ItemTable)
			.merge(param('patch', ItemMerge))
			.where((row) => eq(row.name, named));

		it('of one record returns what each mode asks for', async () => {
			const there = await seed('upd', { name: 'Upd', qty: 1 });
			const none: undefined = await ok(
				client.run(one.returning('none').build(), {
					id: there,
					patch: { qty: 2 },
				}),
			);
			expect(none).toBe(undefined);
			const after: Item | undefined = await ok(
				client.run(one.returning('after').build(), {
					id: there,
					patch: { qty: 3 },
				}),
			);
			expect(after).toEqual({ id: there, name: 'Upd', qty: 3 });
			const before: Item | undefined = await ok(
				client.run(one.returning('before').build(), {
					id: there,
					patch: { qty: 4, note: 'n' },
				}),
			);
			expect(before).toEqual({ id: there, name: 'Upd', qty: 3 });
			const diff: Diff | undefined = await ok(
				client.run(one.returning('diff').build(), {
					id: there,
					patch: { qty: 5, note: undefined },
				}),
			);
			expect(diff).toEqual([
				{ op: 'remove', path: '/note' },
				{ op: 'replace', path: '/qty', value: 5 },
			]);
			// A patch that changes nothing is the empty list, not NONE.
			expect(
				await ok(
					client.run(one.returning('diff').build(), {
						id: there,
						patch: { qty: 5 },
					}),
				),
			).toEqual([]);
			const fields = await ok(
				client.run(
					one
						.returning((row) => ({ n: row.name, note: row.note }))
						.build(),
					{ id: there, patch: { qty: 6 } },
				),
			);
			// A projected NONE keeps its key, holding `undefined`.
			expect(fields).toStrictEqual({ n: 'Upd', note: undefined });
			// The value of a record that is there can itself be NONE.
			const note: string | undefined = await ok(
				client.run(one.returningValue((row) => row.note).build(), {
					id: there,
					patch: { qty: 7 },
				}),
			);
			expect(note).toBe(undefined);
			const qty: number | undefined = await ok(
				client.run(one.returningValue((row) => row.qty).build(), {
					id: there,
					patch: { qty: 8 },
				}),
			);
			expect(qty).toBe(8);
			expect(await stored(there)).toEqual({
				id: there,
				name: 'Upd',
				qty: 8,
			});
		});

		it('of a record that does not exist is NONE in every mode, and creates nothing', async () => {
			const missing = item('upd-missing');
			const input = { id: missing, patch: { qty: 2 } };
			expect(
				await ok(client.run(one.returning('none').build(), input)),
			).toBe(undefined);
			expect(
				await ok(client.run(one.returning('after').build(), input)),
			).toBe(undefined);
			expect(
				await ok(client.run(one.returning('before').build(), input)),
			).toBe(undefined);
			expect(
				await ok(client.run(one.returning('diff').build(), input)),
			).toBe(undefined);
			expect(
				await ok(
					client.run(
						one.returning((row) => ({ n: row.name })).build(),
						input,
					),
				),
			).toBe(undefined);
			expect(
				await ok(
					client.run(
						one.returningValue((row) => row.qty).build(),
						input,
					),
				),
			).toBe(undefined);
			expect(await stored(missing)).toBe(undefined);
		});

		it('by a condition returns one result per changed record', async () => {
			const first = await seed('updm1', { name: 'UpdMany', qty: 1 });
			const second = await seed('updm2', {
				name: 'UpdMany',
				qty: 1,
				note: 'n',
			});
			const input = (qty: number) => ({
				name: 'UpdMany',
				patch: { qty },
			});
			const none: readonly [] = await ok(
				client.run(many.returning('none').build(), input(2)),
			);
			expect(none).toEqual([]);
			expect(
				await countOf('rt_item', "name = 'UpdMany' AND qty = 2"),
			).toBe(2);
			const after: ReadonlyArray<Item> = await ok(
				client.run(many.returning('after').build(), input(3)),
			);
			expect(after).toEqual([
				{ id: first, name: 'UpdMany', qty: 3 },
				{ id: second, name: 'UpdMany', qty: 3, note: 'n' },
			]);
			const before: ReadonlyArray<Item> = await ok(
				client.run(many.returning('before').build(), input(4)),
			);
			expect(before.map((row) => row.qty)).toEqual([3, 3]);
			const diff: ReadonlyArray<Diff> = await ok(
				client.run(many.returning('diff').build(), input(5)),
			);
			expect(diff).toEqual([
				[{ op: 'replace', path: '/qty', value: 5 }],
				[{ op: 'replace', path: '/qty', value: 5 }],
			]);
			expect(
				await ok(
					client.run(
						many
							.returning((row) => ({
								q: row.qty,
								note: row.note,
							}))
							.build(),
						input(6),
					),
				),
			).toStrictEqual([
				{ q: 6, note: undefined },
				{ q: 6, note: 'n' },
			]);
			const notes: ReadonlyArray<string | undefined> = await ok(
				client.run(
					many.returningValue((row) => row.note).build(),
					input(7),
				),
			);
			expect(notes).toEqual([undefined, 'n']);
		});

		it('by a condition that matches nothing is the empty list in every mode', async () => {
			const input = { name: 'no such item', patch: { qty: 9 } };
			for (const run of [
				() => client.run(many.returning('none').build(), input),
				() => client.run(many.returning('after').build(), input),
				() => client.run(many.returning('before').build(), input),
				() => client.run(many.returning('diff').build(), input),
				() =>
					client.run(
						many.returning((row) => ({ q: row.qty })).build(),
						input,
					),
				() =>
					client.run(
						many.returningValue((row) => row.qty).build(),
						input,
					),
			]) {
				expect(await ok<unknown, unknown>(run())).toEqual([]);
			}
		});
	});

	describe('UPSERT', () => {
		const one = upsert(ItemTable, id).merge(param('data', ItemCreate));
		const many = upsert(ItemTable)
			.merge(param('data', ItemCreate))
			.where((row) => eq(row.name, named));

		it('of a record that does not exist creates it, in every mode', async () => {
			const data = { name: 'Ups', qty: 1 };
			const none: undefined = await ok(
				client.run(one.returning('none').build(), {
					id: item('ups-none'),
					data,
				}),
			);
			expect(none).toBe(undefined);
			expect(await stored(item('ups-none'))).toEqual({
				id: item('ups-none'),
				...data,
			});
			const after: Item = await ok(
				client.run(one.returning('after').build(), {
					id: item('ups-after'),
					data,
				}),
			);
			expect(after).toEqual({ id: item('ups-after'), ...data });
			// A record the statement made had no state before it.
			const before: Item | undefined = await ok(
				client.run(one.returning('before').build(), {
					id: item('ups-before'),
					data,
				}),
			);
			expect(before).toBe(undefined);
			expect(await stored(item('ups-before'))).toEqual({
				id: item('ups-before'),
				...data,
			});
			const diff: Diff = await ok(
				client.run(one.returning('diff').build(), {
					id: item('ups-diff'),
					data,
				}),
			);
			expect(diff).toEqual([
				{
					op: 'replace',
					path: '',
					value: { id: item('ups-diff'), ...data },
				},
			]);
			expect(
				await ok(
					client.run(
						one
							.returning((row) => ({
								n: row.name,
								note: row.note,
							}))
							.build(),
						{ id: item('ups-fields'), data },
					),
				),
			).toStrictEqual({ n: 'Ups', note: undefined });
			const note: string | undefined = await ok(
				client.run(one.returningValue((row) => row.note).build(), {
					id: item('ups-value'),
					data,
				}),
			);
			expect(note).toBe(undefined);
			expect(await stored(item('ups-value'))).toEqual({
				id: item('ups-value'),
				...data,
			});
		});

		it('of a record that exists changes it, in every mode', async () => {
			const there = await seed('ups', { name: 'Ups', qty: 1, note: 'n' });
			const data = (qty: number) => ({ name: 'Ups', qty });
			expect(
				await ok(
					client.run(one.returning('none').build(), {
						id: there,
						data: data(2),
					}),
				),
			).toBe(undefined);
			// MERGE keeps the fields the data does not name.
			expect(
				await ok(
					client.run(one.returning('after').build(), {
						id: there,
						data: data(3),
					}),
				),
			).toEqual({ id: there, name: 'Ups', qty: 3, note: 'n' });
			expect(
				await ok(
					client.run(one.returning('before').build(), {
						id: there,
						data: data(4),
					}),
				),
			).toEqual({ id: there, name: 'Ups', qty: 3, note: 'n' });
			expect(
				await ok(
					client.run(one.returning('diff').build(), {
						id: there,
						data: data(5),
					}),
				),
			).toEqual([{ op: 'replace', path: '/qty', value: 5 }]);
			// An upsert that changes nothing has the empty patch.
			expect(
				await ok(
					client.run(one.returning('diff').build(), {
						id: there,
						data: data(5),
					}),
				),
			).toEqual([]);
			expect(
				await ok(
					client.run(
						one
							.returning((row) => ({
								q: row.qty,
								note: row.note,
							}))
							.build(),
						{ id: there, data: data(6) },
					),
				),
			).toStrictEqual({ q: 6, note: 'n' });
			expect(
				await ok(
					client.run(one.returningValue((row) => row.qty).build(), {
						id: there,
						data: data(7),
					}),
				),
			).toBe(7);
			// CONTENT replaces the record: the note is gone.
			const replaced: Item = await ok(
				client.run(
					upsert(ItemTable, id)
						.content(param('data', ItemCreate))
						.build(),
					{ id: there, data: data(8) },
				),
			);
			expect(replaced).toEqual({ id: there, name: 'Ups', qty: 8 });
		});

		it('by a condition that matches nothing creates one record, in every mode', async () => {
			const data = (name: string) => ({ name, qty: 1 });
			const input = (name: string) => ({ name, data: data(name) });
			const none: readonly [] = await ok(
				client.run(many.returning('none').build(), input('UpsNone')),
			);
			expect(none).toEqual([]);
			expect(await countOf('rt_item', "name = 'UpsNone'")).toBe(1);
			const after: ReadonlyArray<Item> = await ok(
				client.run(many.returning('after').build(), input('UpsAfter')),
			);
			expect(after).toHaveLength(1);
			expect(after[0]).toMatchObject(data('UpsAfter'));
			const before: ReadonlyArray<Item | undefined> = await ok(
				client.run(
					many.returning('before').build(),
					input('UpsBefore'),
				),
			);
			expect(before).toEqual([undefined]);
			expect(await countOf('rt_item', "name = 'UpsBefore'")).toBe(1);
			const diff: ReadonlyArray<Diff> = await ok(
				client.run(many.returning('diff').build(), input('UpsDiff')),
			);
			expect(diff).toHaveLength(1);
			expect(diff[0]).toHaveLength(1);
			expect(diff[0]?.[0]).toMatchObject({
				op: 'replace',
				path: '',
				value: data('UpsDiff'),
			});
			expect(
				await ok(
					client.run(
						many
							.returning((row) => ({
								n: row.name,
								note: row.note,
							}))
							.build(),
						input('UpsFields'),
					),
				),
			).toStrictEqual([{ n: 'UpsFields', note: undefined }]);
			const notes: ReadonlyArray<string | undefined> = await ok(
				client.run(
					many.returningValue((row) => row.note).build(),
					input('UpsValue'),
				),
			);
			expect(notes).toEqual([undefined]);
		});

		it('by a condition changes every match, in every mode', async () => {
			const first = await seed('upsm1', { name: 'UpsMany', qty: 1 });
			const second = await seed('upsm2', {
				name: 'UpsMany',
				qty: 1,
				note: 'n',
			});
			const input = (qty: number) => ({
				name: 'UpsMany',
				data: { name: 'UpsMany', qty },
			});
			expect(
				await ok(client.run(many.returning('none').build(), input(2))),
			).toEqual([]);
			// Asking for nothing changes the two records and creates none.
			expect(
				await countOf('rt_item', "name = 'UpsMany' AND qty = 2"),
			).toBe(2);
			expect(await countOf('rt_item', "name = 'UpsMany'")).toBe(2);
			expect(
				await ok(client.run(many.returning('after').build(), input(3))),
			).toEqual([
				{ id: first, name: 'UpsMany', qty: 3 },
				{ id: second, name: 'UpsMany', qty: 3, note: 'n' },
			]);
			expect(
				await ok(
					client.run(many.returning('before').build(), input(4)),
				),
			).toEqual([
				{ id: first, name: 'UpsMany', qty: 3 },
				{ id: second, name: 'UpsMany', qty: 3, note: 'n' },
			]);
			expect(
				await ok(client.run(many.returning('diff').build(), input(5))),
			).toEqual([
				[{ op: 'replace', path: '/qty', value: 5 }],
				[{ op: 'replace', path: '/qty', value: 5 }],
			]);
			expect(
				await ok(
					client.run(
						many.returningValue((row) => row.qty).build(),
						input(6),
					),
				),
			).toEqual([6, 6]);
			expect(await countOf('rt_item', "name = 'UpsMany'")).toBe(2);
		});
	});

	describe('DELETE', () => {
		const one = deleteFrom(ItemTable, id);
		const many = deleteFrom(ItemTable).where((row) => eq(row.name, named));

		it('of one record returns what each mode asks for, and deletes it', async () => {
			const data = { name: 'Del', qty: 1 };
			const fresh = (key: string) => seed(key, data);
			const none: undefined = await ok(
				client.run(one.returning('none').build(), {
					id: await fresh('del-none'),
				}),
			);
			expect(none).toBe(undefined);
			expect(await stored(item('del-none'))).toBe(undefined);
			// A deleted record has no state after the statement.
			const after: undefined = await ok(
				client.run(one.returning('after').build(), {
					id: await fresh('del-after'),
				}),
			);
			expect(after).toBe(undefined);
			expect(await stored(item('del-after'))).toBe(undefined);
			const before: Item | undefined = await ok(
				client.run(one.returning('before').build(), {
					id: await fresh('del-before'),
				}),
			);
			expect(before).toEqual({ id: item('del-before'), ...data });
			const diff: Diff | undefined = await ok(
				client.run(one.returning('diff').build(), {
					id: await fresh('del-diff'),
				}),
			);
			expect(diff).toEqual([
				{ op: 'replace', path: '', value: undefined },
			]);
			expect(
				await ok(
					client.run(
						one
							.returning((row) => ({
								n: row.name,
								note: row.note,
							}))
							.build(),
						{ id: await fresh('del-fields') },
					),
				),
			).toStrictEqual({ n: 'Del', note: undefined });
			// The value of a deleted record can itself be NONE.
			const note: string | undefined = await ok(
				client.run(one.returningValue((row) => row.note).build(), {
					id: await fresh('del-value'),
				}),
			);
			expect(note).toBe(undefined);
			expect(await stored(item('del-value'))).toBe(undefined);
			expect(
				await ok(
					client.run(one.returningValue((row) => row.qty).build(), {
						id: await fresh('del-qty'),
					}),
				),
			).toBe(1);
		});

		it('of a record that does not exist is NONE in every mode', async () => {
			const input = { id: item('del-missing') };
			for (const run of [
				() => client.run(one.returning('none').build(), input),
				() => client.run(one.returning('after').build(), input),
				() => client.run(one.returning('before').build(), input),
				() => client.run(one.returning('diff').build(), input),
				() =>
					client.run(
						one.returning((row) => ({ n: row.name })).build(),
						input,
					),
				() =>
					client.run(
						one.returningValue((row) => row.name).build(),
						input,
					),
			]) {
				expect(await ok<unknown, unknown>(run())).toBe(undefined);
			}
		});

		it('by a condition returns one result per deleted record', async () => {
			const pair = async () => {
				await seed('delm1', { name: 'DelMany', qty: 1 });
				await seed('delm2', { name: 'DelMany', qty: 2, note: 'n' });
			};
			const input = { name: 'DelMany' };
			await pair();
			const none: readonly [] = await ok(
				client.run(many.returning('none').build(), input),
			);
			expect(none).toEqual([]);
			expect(await countOf('rt_item', "name = 'DelMany'")).toBe(0);
			await pair();
			const after: ReadonlyArray<undefined> = await ok(
				client.run(many.returning('after').build(), input),
			);
			expect(after).toEqual([undefined, undefined]);
			await pair();
			const before: ReadonlyArray<Item> = await ok(
				client.run(many.returning('before').build(), input),
			);
			expect(before).toEqual([
				{ id: item('delm1'), name: 'DelMany', qty: 1 },
				{ id: item('delm2'), name: 'DelMany', qty: 2, note: 'n' },
			]);
			await pair();
			const diff: ReadonlyArray<Diff> = await ok(
				client.run(many.returning('diff').build(), input),
			);
			expect(diff).toEqual([
				[{ op: 'replace', path: '', value: undefined }],
				[{ op: 'replace', path: '', value: undefined }],
			]);
			await pair();
			expect(
				await ok(
					client.run(
						many
							.returning((row) => ({
								q: row.qty,
								note: row.note,
							}))
							.build(),
						input,
					),
				),
			).toStrictEqual([
				{ q: 1, note: undefined },
				{ q: 2, note: 'n' },
			]);
			await pair();
			const notes: ReadonlyArray<string | undefined> = await ok(
				client.run(
					many.returningValue((row) => row.note).build(),
					input,
				),
			);
			expect(notes).toEqual([undefined, 'n']);
			expect(await countOf('rt_item', "name = 'DelMany'")).toBe(0);
		});

		it('by a condition that matches nothing is the empty list in every mode', async () => {
			const input = { name: 'no such item' };
			for (const run of [
				() => client.run(many.returning('none').build(), input),
				() => client.run(many.returning('after').build(), input),
				() => client.run(many.returning('before').build(), input),
				() => client.run(many.returning('diff').build(), input),
				() =>
					client.run(
						many.returning((row) => ({ q: row.qty })).build(),
						input,
					),
				() =>
					client.run(
						many.returningValue((row) => row.qty).build(),
						input,
					),
			]) {
				expect(await ok<unknown, unknown>(run())).toEqual([]);
			}
		});
	});

	describe('INSERT RELATION', () => {
		const edges = param('edges', Schema.Array(Holds.insertRow));
		const into = insertRelation(Holds.table, edges);
		const holds = (key: string) => new RecordId('rt_holds', key);
		let a: ItemId;
		let b: ItemId;

		beforeAll(async () => {
			a = await seed('rel-a', { name: 'RelA', qty: 1 });
			b = await seed('rel-b', { name: 'RelB', qty: 1 });
		});

		const pair = (first: string, second: string) => ({
			edges: [
				{ id: holds(first), in: owner, out: a, weight: 1 },
				{ id: holds(second), in: owner, out: b, weight: 2 },
			],
		});
		const rows = (first: string, second: string) => [
			{ id: holds(first), in: owner, out: a, weight: 1 },
			{ id: holds(second), in: owner, out: b, weight: 2 },
		];

		it('returns one result per inserted edge, in every mode', async () => {
			const none: readonly [] = await ok(
				client.run(into.returning('none').build(), pair('n1', 'n2')),
			);
			expect(none).toEqual([]);
			expect(await stored(holds('n2'))).toEqual(rows('n1', 'n2')[1]);
			const after: ReadonlyArray<HoldsRow> = await ok(
				client.run(into.returning('after').build(), pair('a1', 'a2')),
			);
			expect(after).toEqual(rows('a1', 'a2'));
			// An edge the statement made had no state before it.
			const before: ReadonlyArray<HoldsRow | undefined> = await ok(
				client.run(into.returning('before').build(), pair('b1', 'b2')),
			);
			expect(before).toEqual([undefined, undefined]);
			expect(await stored(holds('b1'))).toEqual(rows('b1', 'b2')[0]);
			const diff: ReadonlyArray<Diff> = await ok(
				client.run(into.returning('diff').build(), pair('d1', 'd2')),
			);
			expect(diff).toEqual(
				rows('d1', 'd2').map((value) => [
					{ op: 'replace', path: '', value },
				]),
			);
			expect(
				await ok(
					client.run(
						into
							.returning((row) => ({
								w: row.weight,
								item: row.out,
							}))
							.build(),
						pair('f1', 'f2'),
					),
				),
			).toEqual([
				{ w: 1, item: a },
				{ w: 2, item: b },
			]);
			const outs: ReadonlyArray<ItemId> = await ok(
				client.run(
					into.returningValue((row) => row.out).build(),
					pair('v1', 'v2'),
				),
			);
			expect(outs).toEqual([a, b]);
		});

		it('takes one edge that is not in a list, and still returns a list', async () => {
			const single = insertRelation(
				Holds.table,
				param('edge', Holds.insertRow),
			);
			const made: ReadonlyArray<HoldsRow> = await ok(
				client.run(single.build(), {
					edge: { id: holds('one'), in: owner, out: a, weight: 3 },
				}),
			);
			expect(made).toEqual([
				{ id: holds('one'), in: owner, out: a, weight: 3 },
			]);
			// An edge without an id is given one.
			const [anonymous] = await ok(
				client.run(single.build(), {
					edge: { in: owner, out: b, weight: 4 },
				}),
			);
			expect(anonymous).toMatchObject({ in: owner, out: b, weight: 4 });
			expect(anonymous?.id.table.name).toBe('rt_holds');
		});

		it('of no edge is the empty list in every mode', async () => {
			const input = { edges: [] };
			for (const run of [
				() => client.run(into.returning('none').build(), input),
				() => client.run(into.returning('after').build(), input),
				() => client.run(into.returning('before').build(), input),
				() => client.run(into.returning('diff').build(), input),
				() =>
					client.run(
						into.returning((row) => ({ w: row.weight })).build(),
						input,
					),
				() =>
					client.run(
						into.returningValue((row) => row.out).build(),
						input,
					),
			]) {
				expect(await ok<unknown, unknown>(run())).toEqual([]);
			}
		});

		it('ON DUPLICATE KEY UPDATE returns the edge it changed as it was before, and its patch', async () => {
			await ok(client.run(into.build(), pair('dup1', 'dup2')));
			const again = into.onDuplicate((input) => ({
				weight: input.weight,
			}));
			const before = await ok(
				client.run(again.returning('before').build(), {
					edges: [
						{ id: holds('dup1'), in: owner, out: a, weight: 7 },
						{ id: holds('dup3'), in: owner, out: a, weight: 5 },
					],
				}),
			);
			expect(before).toEqual([rows('dup1', 'dup2')[0], undefined]);
			expect(await stored(holds('dup1'))).toMatchObject({ weight: 7 });
			expect(
				await ok(
					client.run(again.returning('diff').build(), {
						edges: [
							{ id: holds('dup1'), in: owner, out: a, weight: 8 },
						],
					}),
				),
			).toEqual([[{ op: 'replace', path: '/weight', value: 8 }]]);
		});

		it('refuses an edge of an enforced relation to a record that does not exist, and inserts none of the list', async () => {
			const error = await failure(
				client.run(into.build(), {
					edges: [
						{ id: holds('g1'), in: owner, out: a, weight: 1 },
						{
							id: holds('g2'),
							in: owner,
							out: item('rel-ghost'),
							weight: 1,
						},
					],
				}),
			);
			expect(error).toBeInstanceOf(StatementRequestError);
			expect(error.message).toBe('SurrealDB request failed');
			// SurrealDB 3.3 and 4.0 quote the id differently.
			expect(error).toHaveProperty(
				'cause.message',
				expect.stringMatching(/rel-ghost.*does not exist/u),
			);
			expect(await stored(holds('g1'))).toBe(undefined);
			expect(await stored(holds('g2'))).toBe(undefined);
		});

		it('inserts an edge of a relation that is not enforced to a record that does not exist', async () => {
			const sees = (key: string) => new RecordId('rt_sees', key);
			const ghost = item('rel-ghost');
			const loose = insertRelation(
				Sees.table,
				param('edges', Schema.Array(Sees.insertRow)),
			);
			const edge = (key: string) => ({
				edges: [{ id: sees(key), in: owner, out: ghost, weight: 1 }],
			});
			expect(await ok(client.run(loose.build(), edge('s1')))).toEqual([
				{ id: sees('s1'), in: owner, out: ghost, weight: 1 },
			]);
			expect(
				await ok(
					client.run(loose.returning('before').build(), edge('s2')),
				),
			).toEqual([undefined]);
			expect(
				await ok(
					client.run(
						loose.returningValue((row) => row.out).build(),
						edge('s3'),
					),
				),
			).toEqual([ghost]);
			expect(
				await ok(
					client.run(loose.returning('none').build(), edge('s4')),
				),
			).toEqual([]);
			expect(await countOf('rt_sees')).toBe(4);
		});
	});
});
