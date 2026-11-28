import { type Effect, type Result, Schema } from 'effect';

import {
	bind,
	type Client,
	type StatementError,
} from '../../src/client/index.js';
import { make } from '../../src/statement/index.js';

declare const client: Client;
const lookup = make({
	sql: '$id;',
	params: { id: Schema.Number },
	result: Schema.Number,
});
const label = make({
	sql: '$id;',
	params: { id: Schema.String },
	result: Schema.String,
});
const statements = [
	bind(lookup, { id: 1 }),
	bind(label, { id: 'two' }),
	bind(lookup, { id: 3 }),
] as const;

export const repeatedTransaction: Effect.Effect<
	readonly [number, string, number],
	StatementError<never>
> = client.transaction(statements);
export const repeatedPipeline: Effect.Effect<
	readonly [
		Result.Result<number, StatementError<never>>,
		Result.Result<string, StatementError<never>>,
		Result.Result<number, StatementError<never>>,
	],
	StatementError<never>
> = client.pipeline(statements);
