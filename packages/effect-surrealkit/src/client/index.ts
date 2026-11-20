/**
 * Runs `effect-surrealkit/statement` values against SurrealDB through the
 * JavaScript SDK, as an Effect service.
 */
export {
	type Bound,
	type BoundStatements,
	bind,
	type Client,
	type InputOf,
	layer,
	make,
	type Queryable,
	type Results,
	type RunArgs,
	type RunOptions,
	run,
	SurrealClient,
	type ThrownByAny,
	transaction,
} from './client.js';
export {
	StatementConflict,
	StatementDecodeError,
	StatementEmptyResult,
	StatementEncodeError,
	type StatementError,
	StatementRequestError,
	StatementScopeError,
	StatementThrown,
	StatementUnexpectedThrow,
	StatementWriteOutcomeError,
} from './errors.js';
export { metrics } from './internal/trace.js';
