import { Schema, Stream } from 'effect';

import type { Client } from '../../src/client/index.js';
import { Int, RecordIdFor } from '../../src/index.js';
import { make, param, updateEach } from '../../src/statement/index.js';

const scalar = make({
	name: 'scalar',
	sql: '$single;',
	params: { single: Schema.String },
	result: Schema.String,
});
const list = make({
	name: 'list',
	sql: '$rows;',
	params: { rows: Schema.Array(Int), label: Schema.String },
	result: Schema.Array(Int),
});
const id = RecordIdFor('bulk_row');
const Row = {
	name: 'bulk_row',
	id,
	select: Schema.Struct({ id, n: Int }),
	merge: Schema.Struct({ n: Schema.optionalKey(Int) }),
} as const;
const badRows = Schema.Array(
	Schema.Struct({ id, set: Schema.Struct({ n: Int, absent: Int }) }),
);
const wrongId = Schema.Array(
	Schema.Struct({
		id: RecordIdFor('other_row'),
		set: Schema.Struct({ n: Int }),
	}),
);

const badStream = { rows: Stream.make('wrong'), label: 'all' };

export const forbidden = (client: Client): void => {
	// @ts-expect-error [bulk-over-kind] over must name the list binding
	client.bulk(list, { rows: [1], label: 'all' }, { over: 'label' });
	// @ts-expect-error [bulk-list-required] there is no list binding to chunk
	client.bulk(scalar, { single: 'single' }, { over: 'single' });
	// @ts-expect-error [bulk-row-kind] each iterable item must match the list element
	client.bulk(list, { rows: ['wrong'], label: 'all' }, { over: 'rows' });
	// @ts-expect-error [bulk-stream-kind] each stream item must match the list element
	client.bulk(list, badStream, { over: 'rows' });
	// @ts-expect-error [bulk-other-binding] changing the row source does not omit other required bindings
	client.bulk(list, { rows: [1] }, { over: 'rows' });
	// @ts-expect-error [bulk-merge-field] updateEach refuses an unknown set field
	updateEach(Row, param('rows', badRows));
	// @ts-expect-error [bulk-record-id] updateEach requires ids from its own table
	updateEach(Row, param('rows', wrongId));
};
