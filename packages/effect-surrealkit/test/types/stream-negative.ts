import { Schema } from 'effect';

import { type Client, stream } from '../../src/client/index.js';
import { make, param, select } from '../../src/statement/index.js';
import { PersonId, PersonTable } from '../golden/example/index.js';

const one = select(PersonTable, param('id', PersonId)).only().build();
const fixed = make({
	sql: '[1];',
	params: {},
	result: Schema.Tuple([Schema.Number]),
	writes: false,
});
const optional = make({
	sql: 'NONE;',
	params: {},
	result: Schema.UndefinedOr(Schema.Array(Schema.Number)),
	writes: false,
});

export const invalidStreams = (
	client: Client,
	id: typeof PersonId.Type,
): void => {
	// @ts-expect-error [stream/result-must-be-list-of-rows-only] a stream needs a list of rows, and only returns at most one row
	client.stream(one, { id });
	// @ts-expect-error [stream/result-must-be-list-of-rows-tuple] a stream needs a plain array schema, not a tuple
	stream(fixed);
	// @ts-expect-error [stream/result-must-be-list-of-rows-optional] a stream needs a plain array schema, not an optional list
	stream(optional);
};
