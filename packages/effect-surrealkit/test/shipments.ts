/**
 * A `shipment` table whose unique indexes run through object fields of
 * several shapes: `label` is an optional struct, which a lookup reads
 * through; `event` is a union of structs and `parcel` a nullable struct,
 * whose fields the row exposes no path to, so a lookup through them is
 * refused.
 */
import { Schema } from 'effect';

import * as SurrealSchema from '../src/index.js';

const id = SurrealSchema.RecordIdFor('shipment');
const sender = SurrealSchema.String;
const label = Schema.Struct({ text: SurrealSchema.String });
const event = Schema.Union([
	Schema.Struct({ id: SurrealSchema.String, kind: Schema.Literal('packed') }),
	Schema.Struct({
		id: SurrealSchema.String,
		kind: Schema.Literal('sent'),
		carrier: SurrealSchema.String,
	}),
]);
const parcel = Schema.NullOr(Schema.Struct({ code: SurrealSchema.String }));

const row = Schema.Struct({
	id,
	sender,
	label: Schema.optionalKey(label),
	event,
	parcel,
});
const write = Schema.Struct({
	sender,
	label: Schema.optionalKey(label),
	event,
	parcel,
});

const required = {
	select: 'required',
	create: 'required',
	merge: 'optional',
	content: 'required',
} as const;

export const ShipmentTable = SurrealSchema.table({
	name: 'shipment',
	type: 'normal',
	schemafull: true,
	drop: false,
	id,
	select: row,
	visible: row,
	create: write,
	merge: write,
	content: write,
	fields: { sender, label, event, parcel },
	presence: {
		id: {
			select: 'required',
			create: 'excluded',
			merge: 'excluded',
			content: 'excluded',
		},
		sender: required,
		label: {
			select: 'optional',
			create: 'optional',
			merge: 'removable',
			content: 'optional',
		},
		event: required,
		parcel: required,
	},
	relations: { in: [], out: [] },
	permissions: { select: false, create: false, update: false, delete: false },
	indexes: {
		shipment_label: { type: 'unique', columns: [['label', 'text']] },
		shipment_event: {
			type: 'unique',
			columns: [['sender'], ['event', 'id']],
		},
		shipment_parcel: { type: 'unique', columns: [['parcel', 'code']] },
	} as {
		readonly shipment_label: {
			readonly type: 'unique';
			readonly columns: readonly [readonly ['label', 'text']];
		};
		readonly shipment_event: {
			readonly type: 'unique';
			readonly columns: readonly [
				readonly ['sender'],
				readonly ['event', 'id'],
			];
		};
		readonly shipment_parcel: {
			readonly type: 'unique';
			readonly columns: readonly [readonly ['parcel', 'code']];
		};
	},
});
