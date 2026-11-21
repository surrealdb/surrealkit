import { Effect, Schedule, Schema } from 'effect';

import {
	bind,
	type Client,
	run,
	type StatementError,
	type SurrealClient,
	type TransactionError,
	withTransaction,
} from '../../src/client/index.js';
import { make } from '../../src/statement/index.js';

const scalar = make({
	name: 'scalar',
	sql: '$value;',
	params: { value: Schema.Number },
	result: Schema.Number,
});
const list = make({
	name: 'list',
	sql: '$values;',
	params: { values: Schema.Array(Schema.Number) },
	result: Schema.Array(Schema.Number),
});

export const interactive = (
	client: Client,
): Effect.Effect<
	readonly [number, number, ReadonlyArray<ReadonlyArray<number>>],
	| StatementError<never>
	| TransactionError
	| import('../../src/client/index.js').BulkWriteError<never>
> =>
	client.withTransaction(
		(tx) =>
			Effect.gen(function* () {
				const first = yield* tx.run(
					scalar,
					{ value: 1 },
					{ unsafeSkipValueCheck: true },
				);
				const [second] = yield* tx.transaction(
					[bind(scalar, { value: 2 })],
					{ unsafeSkipValueCheck: false },
				);
				const bulk = yield* tx.bulk(
					list,
					{ values: [1, 2] },
					{ over: 'values' },
				);
				return [first, second, bulk.results] as const;
			}),
		{ retry: Schedule.recurs(1) },
	);

export const usesContext: Effect.Effect<
	number,
	StatementError<never> | TransactionError,
	SurrealClient
> = withTransaction(run(scalar, { value: 1 }), { retry: Schedule.recurs(1) });
