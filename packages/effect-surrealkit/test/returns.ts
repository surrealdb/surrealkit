/**
 * A small schema for running every `RETURN` mode of every write: a table of
 * items with an optional field, and two relations from an owner to an item,
 * one `ENFORCED` (an endpoint must exist) and one not. No field has a value
 * the engine computes, so each test asserts the whole decoded result.
 * `returnsSchema` defines them.
 */
import { Schema } from 'effect';

import * as SurrealSchema from '../src/index.js';

const excluded = {
	select: 'required',
	create: 'excluded',
	merge: 'excluded',
	content: 'excluded',
} as const;
const required = {
	select: 'required',
	create: 'required',
	merge: 'optional',
	content: 'required',
} as const;
const removable = {
	select: 'optional',
	create: 'optional',
	merge: 'removable',
	content: 'optional',
} as const;
const full = { select: true, create: true, update: true } as const;
const closed = {
	select: false,
	create: false,
	update: false,
	delete: false,
} as const;

export const ItemId = SurrealSchema.RecordIdFor('rt_item');
export type ItemId = typeof ItemId.Type;
const ItemFields = {
	name: SurrealSchema.String,
	qty: SurrealSchema.Int,
	note: SurrealSchema.String,
} as const;
export const Item = Schema.Struct({
	id: ItemId,
	name: ItemFields.name,
	qty: ItemFields.qty,
	note: Schema.optionalKey(ItemFields.note),
});
export type Item = typeof Item.Type;
/** What a `CREATE`, a `CONTENT` and an `UPSERT` take. */
export const ItemCreate = Schema.Struct({
	name: ItemFields.name,
	qty: ItemFields.qty,
	note: Schema.optionalKey(ItemFields.note),
});
export const ItemMerge = Schema.Struct({
	name: Schema.optionalKey(ItemFields.name),
	qty: Schema.optionalKey(ItemFields.qty),
	note: Schema.optional(ItemFields.note),
});
export const ItemTable = SurrealSchema.table({
	name: 'rt_item',
	type: 'normal',
	schemafull: true,
	drop: false,
	id: ItemId,
	select: Item,
	visible: Item,
	create: ItemCreate,
	merge: ItemMerge,
	content: ItemCreate,
	fields: ItemFields,
	presence: {
		id: excluded,
		name: required,
		qty: required,
		note: removable,
	},
	relations: { in: [], out: ['rt_holds', 'rt_sees'] },
	permissions: closed,
	fieldPermissions: { name: full, qty: full, note: full },
	indexes: {},
});

export const OwnerId = SurrealSchema.RecordIdFor('rt_owner');
export type OwnerId = typeof OwnerId.Type;

const relation = <const Name extends string, const Enforced extends boolean>(
	name: Name,
	enforced: Enforced,
) => {
	const id = SurrealSchema.RecordIdFor(name);
	const fields = { weight: SurrealSchema.Int } as const;
	const row = Schema.Struct({
		id,
		in: OwnerId,
		out: ItemId,
		weight: fields.weight,
	});
	const write = Schema.Struct({ weight: fields.weight });
	const edge = SurrealSchema.edge({
		table: name,
		in: [OwnerId],
		out: [ItemId],
		enforced,
		lightweight: false,
	});
	const table = SurrealSchema.table({
		name,
		type: 'relation',
		schemafull: true,
		drop: false,
		id,
		select: row,
		visible: row,
		create: write,
		merge: Schema.Struct({ weight: Schema.optionalKey(fields.weight) }),
		content: write,
		fields,
		presence: {
			id: excluded,
			in: excluded,
			out: excluded,
			weight: required,
		},
		relations: { in: [], out: [] },
		permissions: closed,
		fieldPermissions: { weight: full },
		indexes: {},
		edge,
	});
	/** One edge of an `INSERT RELATION`: its endpoints, and an id or none. */
	const insertRow = Schema.Struct({
		id: Schema.optionalKey(id),
		in: OwnerId,
		out: ItemId,
		weight: fields.weight,
	});
	return { id, row, table, insertRow };
};

/** `ENFORCED`: SurrealDB refuses an edge to a record that does not exist. */
export const Holds = relation('rt_holds', true);
/** Not enforced: an edge may point at a record that does not exist. */
export const Sees = relation('rt_sees', false);

export const returnsSchema = `
	DEFINE TABLE rt_item SCHEMAFULL;
	DEFINE FIELD name ON rt_item TYPE string;
	DEFINE FIELD qty ON rt_item TYPE int;
	DEFINE FIELD note ON rt_item TYPE option<string>;
	DEFINE TABLE rt_owner SCHEMAFULL;
	DEFINE FIELD name ON rt_owner TYPE string;
	DEFINE TABLE rt_holds TYPE RELATION IN rt_owner OUT rt_item ENFORCED SCHEMAFULL;
	DEFINE FIELD weight ON rt_holds TYPE int;
	DEFINE TABLE rt_sees TYPE RELATION IN rt_owner OUT rt_item SCHEMAFULL;
	DEFINE FIELD weight ON rt_sees TYPE int;
	CREATE rt_owner:o CONTENT { name: 'O' };
`;
