import { Schedule, Schema } from 'effect';

import { bind, type TransactionClient } from '../../src/client/index.js';
import { make } from '../../src/statement/index.js';

const scalar = make({
	name: 'scalar',
	sql: '1;',
	params: {},
	result: Schema.Number,
});
const list = make({
	name: 'list',
	sql: '$values;',
	params: { values: Schema.Array(Schema.Number) },
	result: Schema.Array(Schema.Number),
});
const retry = Schedule.recurs(1);

export const forbidden = (tx: TransactionClient) => {
	// @ts-expect-error [interactive-no-pipeline] a transaction only offers run, transaction and bulk
	tx.pipeline([bind(scalar)]);
	// @ts-expect-error [interactive-no-stream] streaming a transaction is scoped out
	tx.stream(list, { values: [1] });
	// @ts-expect-error [interactive-no-live] live queries are scoped out
	tx.live(list);
	// @ts-expect-error [interactive-no-live-scoped] scoped live queries are scoped out
	tx.liveScoped(list);
	// @ts-expect-error [interactive-no-savepoint] transactions cannot be nested
	tx.withTransaction();
	// @ts-expect-error [interactive-no-run-retry] retries can only replay the enclosing transaction
	tx.run(scalar, undefined, { retry });
	// @ts-expect-error [interactive-no-join-retry] joined statements cannot retry separately
	tx.transaction([bind(scalar)], { retry });
	// @ts-expect-error [interactive-no-bulk-retry] bulk chunks cannot retry inside a transaction
	tx.bulk(list, { values: [1] }, { over: 'values', retry });
	// @ts-expect-error [interactive-no-bulk-atomic] bulk is already atomic inside a transaction
	tx.bulk(list, { values: [1] }, { over: 'values', atomic: false });
};
