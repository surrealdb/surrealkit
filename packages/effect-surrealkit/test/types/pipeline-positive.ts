import { type Effect, type Result, Schema } from 'effect';

import {
	bind,
	type Client,
	pipeline,
	type StatementEncodeError,
	type StatementError,
	type StatementRequestError,
	type StatementScopeError,
	type SurrealClient,
} from '../../src/client/index.js';
import { make } from '../../src/statement/index.js';

type Same<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;
const same = <A, B>(_: Same<A, B>): void => undefined;

const read = make({
	sql: '1;',
	params: {},
	result: Schema.Number,
	writes: false,
});
const search = make({
	sql: 'IF $term = "" { THROW "empty"; }; [$term];',
	params: { term: Schema.String },
	result: Schema.Array(Schema.String),
	throws: ['empty'],
	slots: 2,
	writes: false,
});
const flag = make({
	sql: 'true;',
	params: {},
	result: Schema.Boolean,
	writes: false,
});

type Outcomes = readonly [
	Result.Result<number, StatementError<never>>,
	Result.Result<ReadonlyArray<string>, StatementError<'empty'>>,
	Result.Result<boolean, StatementError<never>>,
];
type RequestFailure =
	| StatementEncodeError
	| StatementScopeError
	| StatementRequestError;

export const exactPipelineTuple = (client: Client): void => {
	const outcomes = client.pipeline([
		bind(read),
		bind(search, { term: 'a' }),
		bind(flag),
	]);
	same<typeof outcomes, Effect.Effect<Outcomes, RequestFailure>>(true);
	const withContext = pipeline(
		[bind(read), bind(search, { term: 'a' }), bind(flag)],
		{ each: 'transaction' },
	);
	same<
		typeof withContext,
		Effect.Effect<Outcomes, RequestFailure, SurrealClient>
	>(true);
	const empty = client.pipeline([]);
	same<typeof empty, Effect.Effect<readonly [], RequestFailure>>(true);
};
