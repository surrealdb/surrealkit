/**
 * Runs `effect-surrealkit/statement` values against SurrealDB through the
 * JavaScript SDK, as an Effect service.
 */

export type {
	BulkOptions,
	BulkProgress,
	BulkSummary,
	ListBindingsOf,
} from './bulk.js';
export {
	type Bound,
	type BoundStatements,
	bind,
	type Client,
	type ClientOptions,
	type InputOf,
	layer,
	make,
	pipeline,
	type Queryable,
	type Results,
	type RunArgs,
	type RunOptions,
	run,
	SurrealClient,
	stream,
	type ThrownByAny,
	transaction,
	type UniqueIndexesOfAny,
	watchQuery,
	withTransaction,
} from './client.js';
export {
	BulkWriteError,
	LiveQueryClosed,
	StatementConflict,
	StatementDecodeError,
	StatementEmptyResult,
	StatementEncodeError,
	type StatementError,
	StatementIndexConflict,
	StatementRecordExists,
	StatementRequestError,
	StatementScopeError,
	StatementThrown,
	StatementUnexpectedThrow,
	StatementUnsupported,
	StatementWriteOutcomeError,
	TransactionOutcomeUnknown,
	WatchQueryClosed,
} from './errors.js';
export type {
	TransactionClient,
	TransactionError,
	TransactionOptions,
} from './interactive.js';
export { metrics } from './internal/trace.js';
export type { LiveError, LiveEvent } from './live.js';
export type { PipelineOptions, PipelineResults } from './pipeline.js';
export { type RequestTiming, type StatementTiming, timed } from './timing.js';
export {
	type BoundLive,
	bindLive,
	type WatchQuery,
	type WatchQueryError,
	type WatchQueryOptions,
	type WatchQueryState,
} from './watch-query.js';
